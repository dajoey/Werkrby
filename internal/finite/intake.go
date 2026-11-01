package finite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/agent0ai/spynel/internal/fsx"
	"gopkg.in/yaml.v3"
)

const maxBriefBytes = 1024 * 1024

var briefIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,63}$`)

type briefFrontMatter struct {
	ID              string      `yaml:"id"`
	Title           string      `yaml:"title"`
	Risk            string      `yaml:"risk"`
	ReviewRequired  *bool       `yaml:"review_required"`
	MaxRuns         *int        `yaml:"max_runs"`
	MaxFailures     *int        `yaml:"max_failures"`
	MaxReviewCycles *int        `yaml:"max_review_cycles"`
	Staff           string      `yaml:"staff"`
	ReviewStaff     string      `yaml:"review_staff"`
	Fallbacks       []briefRung `yaml:"fallbacks"`
	ReviewFallbacks []briefRung `yaml:"review_fallbacks"`
}

// briefRung is one fallback chain rung in the intake representation.
type briefRung struct {
	Seat string `yaml:"seat"`
	Gate string `yaml:"gate,omitempty"`
}

// parseBriefRungs validates fallback rungs against their phase staff seat and
// returns the engine representation, or nil when the brief carries none.
func parseBriefRungs(rungs []briefRung, primary, field string) ([]SeatRung, error) {
	if len(rungs) == 0 {
		return nil, nil
	}
	if primary == "" {
		return nil, fmt.Errorf("finite: %s requires a staff seat", field)
	}
	if len(rungs) > maxFallbackRungs {
		return nil, fmt.Errorf("finite: %s exceeds %d rungs", field, maxFallbackRungs)
	}
	parsed := make([]SeatRung, 0, len(rungs))
	seen := make(map[string]bool, len(rungs)+1)
	seen[primary] = true
	for _, rung := range rungs {
		seat := strings.TrimSpace(rung.Seat)
		if !briefIDPattern.MatchString(seat) {
			return nil, fmt.Errorf("finite: invalid fallback seat %q", rung.Seat)
		}
		gate := strings.TrimSpace(rung.Gate)
		if gate != "" && gate != SeatGateBalance && gate != SeatGatePaidOK {
			return nil, fmt.Errorf("finite: invalid fallback gate %q", rung.Gate)
		}
		if seen[seat] {
			return nil, fmt.Errorf("finite: duplicate fallback seat %q", seat)
		}
		seen[seat] = true
		parsed = append(parsed, SeatRung{Seat: seat, Gate: gate})
	}
	return parsed, nil
}

// ParseBrief decodes the strict Markdown intake representation.
func ParseBrief(data []byte) (Brief, error) {
	if len(data) == 0 || len(data) > maxBriefBytes || !utf8.Valid(data) || bytes.IndexByte(data, 0) >= 0 {
		return Brief{}, fmt.Errorf("finite: brief must be valid UTF-8 within %d bytes", maxBriefBytes)
	}
	text := strings.ReplaceAll(string(data), "\r\n", "\n")
	lines := strings.Split(text, "\n")
	if len(lines) < 3 || lines[0] != "---" {
		return Brief{}, fmt.Errorf("finite: brief requires YAML front matter")
	}
	closing := -1
	for i := 1; i < len(lines); i++ {
		if lines[i] == "---" {
			closing = i
			break
		}
	}
	if closing < 0 {
		return Brief{}, fmt.Errorf("finite: unterminated brief front matter")
	}
	var front briefFrontMatter
	decoder := yaml.NewDecoder(strings.NewReader(strings.Join(lines[1:closing], "\n")))
	decoder.KnownFields(true)
	if err := decoder.Decode(&front); err != nil {
		return Brief{}, fmt.Errorf("finite: decode brief: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return Brief{}, fmt.Errorf("finite: brief front matter must contain one document")
	}
	front.ID = strings.TrimSpace(front.ID)
	front.Title = strings.TrimSpace(front.Title)
	front.Risk = strings.TrimSpace(front.Risk)
	if !briefIDPattern.MatchString(front.ID) {
		return Brief{}, fmt.Errorf("finite: invalid task id")
	}
	if front.Title == "" || len(front.Title) > 200 || strings.IndexFunc(front.Title, unicode.IsControl) >= 0 {
		return Brief{}, fmt.Errorf("finite: invalid task title")
	}
	if front.Risk != "low" && front.Risk != "medium" && front.Risk != "high" {
		return Brief{}, fmt.Errorf("finite: invalid task risk")
	}
	if front.ReviewRequired == nil {
		return Brief{}, fmt.Errorf("finite: review_required is required")
	}
	for _, staff := range []string{front.Staff, front.ReviewStaff} {
		if staff != "" && !briefIDPattern.MatchString(staff) {
			return Brief{}, fmt.Errorf("finite: invalid staff name %q", staff)
		}
	}
	brief := Brief{ID: front.ID, Title: front.Title, Risk: front.Risk, ReviewRequired: *front.ReviewRequired, MaxRuns: 12, MaxFailures: 3, MaxReviewCycles: 3,
		Staff: front.Staff, ReviewStaff: front.ReviewStaff}
	fallbacks, err := parseBriefRungs(front.Fallbacks, front.Staff, "fallbacks")
	if err != nil {
		return Brief{}, err
	}
	reviewFallbacks, err := parseBriefRungs(front.ReviewFallbacks, front.ReviewStaff, "review_fallbacks")
	if err != nil {
		return Brief{}, err
	}
	brief.Fallbacks, brief.ReviewFallbacks = fallbacks, reviewFallbacks
	if front.MaxRuns != nil {
		brief.MaxRuns = *front.MaxRuns
	}
	if front.MaxFailures != nil {
		brief.MaxFailures = *front.MaxFailures
	}
	if front.MaxReviewCycles != nil {
		brief.MaxReviewCycles = *front.MaxReviewCycles
	}
	if brief.MaxRuns <= 0 || brief.MaxFailures <= 0 || brief.MaxReviewCycles <= 0 {
		return Brief{}, fmt.Errorf("finite: run policy limits must be positive")
	}
	brief.Body = strings.Join(lines[closing+1:], "\n")
	if strings.TrimSpace(brief.Body) == "" {
		return Brief{}, fmt.Errorf("finite: brief Markdown body is required")
	}
	return brief, nil
}

// IntakeBrief publishes an immutable brief and creates its initial task.
func (s *Store) IntakeBrief(ctx context.Context, sourcePath string) (task Task, err error) {
	info, err := os.Lstat(sourcePath)
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxBriefBytes {
		return Task{}, fmt.Errorf("finite: intake source must be a bounded regular file")
	}
	data, err := os.ReadFile(sourcePath)
	if err != nil {
		return Task{}, fmt.Errorf("finite: read brief: %w", err)
	}
	brief, err := ParseBrief(data)
	if err != nil {
		return Task{}, err
	}
	// Publication, the task commit and failure cleanup form one unit so an
	// orphan reused by one intake cannot be removed by another's cleanup.
	s.intakeMu.Lock()
	defer s.intakeMu.Unlock()
	briefPath := filepath.Join(s.cfg.BriefDir(), brief.ID+".md")
	if err := s.publishBrief(ctx, brief.ID, briefPath, data); err != nil {
		return Task{}, err
	}
	defer func() {
		if err == nil {
			return
		}
		// A commit-unknown error can follow a task row that did land; keep
		// the brief unless the task row is proven absent.
		if _, getErr := s.GetTask(context.WithoutCancel(ctx), brief.ID); errors.Is(getErr, ErrNotFound) {
			_ = os.Remove(briefPath)
		}
	}()
	now := s.now().UTC()
	hash := fmt.Sprintf("%x", sha256.Sum256(data))
	task = Task{
		ID: brief.ID, Title: brief.Title, State: StateTodo, Risk: brief.Risk,
		ReviewRequired: brief.ReviewRequired, MaxRuns: brief.MaxRuns,
		MaxFailures: brief.MaxFailures, MaxReviewCycles: brief.MaxReviewCycles,
		BriefPath: briefPath, BriefSHA256: hash,
		ProjectionPath: filepath.Join(s.cfg.ProjectionDir(), brief.ID+".md"),
		Version:        0, CreatedAt: now, UpdatedAt: now,
	}
	err = s.withImmediateTx(ctx, func(tx txRunner) error {
		_, insertErr := tx.ExecContext(ctx, `INSERT INTO tasks(
			id,title,state,risk,review_required,max_runs,max_failures,max_review_cycles,
			brief_path,brief_sha256,projection_path,version,created_at,updated_at
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			task.ID, task.Title, task.State, task.Risk, boolInt(task.ReviewRequired),
			task.MaxRuns, task.MaxFailures, task.MaxReviewCycles, task.BriefPath,
			task.BriefSHA256, task.ProjectionPath, task.Version, stamp(now), stamp(now))
		if insertErr != nil {
			return fmt.Errorf("finite: create task: %w", insertErr)
		}
		return nil
	})
	if err != nil {
		return Task{}, err
	}
	return task, nil
}

// publishBrief creates the immutable brief copy. A brief left by a crash
// between publication and the task commit is reused when its bytes are
// identical and no task row exists; any other existing brief is refused.
func (s *Store) publishBrief(ctx context.Context, id, briefPath string, data []byte) error {
	createErr := fsx.AtomicCreateFile(briefPath, data, 0o600)
	if createErr == nil {
		return nil
	}
	refused := fmt.Errorf("finite: publish immutable brief: %w", createErr)
	if !errors.Is(createErr, os.ErrExist) {
		return refused
	}
	if _, err := s.GetTask(ctx, id); err == nil {
		return fmt.Errorf("finite: task %s already exists: %w", id, createErr)
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	info, err := os.Lstat(briefPath)
	if err != nil || !info.Mode().IsRegular() || info.Size() != int64(len(data)) {
		return refused
	}
	existing, err := os.ReadFile(briefPath)
	if err != nil || !bytes.Equal(existing, data) {
		return refused
	}
	return nil
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
