package orchestrator

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/agent0ai/spynel/internal/fsx"
	"gopkg.in/yaml.v3"
)

type Document struct {
	FrontMatter map[string]any
	Body        string
}

// TaskPolicy is the fail-safe typed view of task lifecycle policy. Missing or
// malformed metadata always requires review; ambiguity can never grant a
// direct completion.
type TaskPolicy struct {
	ReviewRequired bool
}

func TaskPolicyFromDocument(document Document) (TaskPolicy, error) {
	value, ok := document.FrontMatter["review_required"]
	if !ok || value == nil {
		return TaskPolicy{ReviewRequired: true}, nil
	}
	required, ok := value.(bool)
	if !ok {
		return TaskPolicy{ReviewRequired: true}, fmt.Errorf("review_required must be a boolean, got %T", value)
	}
	return TaskPolicy{ReviewRequired: required}, nil
}

func ReadDocument(path string) (Document, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Document{}, err
	}
	return ParseDocument(data)
}

func ParseDocument(data []byte) (Document, error) {
	normalized := strings.ReplaceAll(string(data), "\r\n", "\n")
	if !strings.HasPrefix(normalized, "---\n") {
		return Document{}, errors.New("markdown document must begin with YAML front matter")
	}
	end := strings.Index(normalized[4:], "\n---\n")
	if end < 0 {
		return Document{}, errors.New("markdown front matter is not terminated")
	}
	front := normalized[4 : 4+end]
	body := normalized[4+end+5:]
	metadata := map[string]any{}
	if err := yaml.Unmarshal([]byte(front), &metadata); err != nil {
		sanitized := sanitizeFrontMatter(front)
		if retryErr := yaml.Unmarshal([]byte(sanitized), &metadata); retryErr == nil {
			return Document{FrontMatter: metadata, Body: body}, nil
		}
		return Document{}, err
	}
	return Document{FrontMatter: metadata, Body: body}, nil
}

// sanitizeFrontMatter fixes common LLM authoring mistakes where natural prose
// containing colons is emitted as an unquoted YAML scalar (for example,
// `waiting_for: Joey's decision (questions 1-4: picks stand)` or
// `outcome: Progress: completed...`). These violate YAML's mapping key rules
// and cause "mapping values are not allowed in this context".
func sanitizeFrontMatter(front string) string {
	lines := strings.Split(front, "\n")
	var result []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			result = append(result, line)
			continue
		}
		idx := strings.Index(line, ":")
		if idx < 0 {
			result = append(result, line)
			continue
		}
		prefix := line[:idx+1]
		keyPart := strings.TrimSpace(prefix[:len(prefix)-1])
		if strings.HasPrefix(keyPart, "- ") {
			keyPart = strings.TrimSpace(keyPart[2:])
		}
		if strings.ContainsAny(keyPart, " \t\"'") {
			result = append(result, line)
			continue
		}
		valWithSpaces := line[idx+1:]
		valTrimmed := strings.TrimSpace(valWithSpaces)
		if valTrimmed == "" {
			result = append(result, line)
			continue
		}
		if strings.HasPrefix(valTrimmed, "\"") || strings.HasPrefix(valTrimmed, "'") ||
			strings.HasPrefix(valTrimmed, "|") || strings.HasPrefix(valTrimmed, ">") ||
			strings.HasPrefix(valTrimmed, "[") || strings.HasPrefix(valTrimmed, "{") {
			result = append(result, line)
			continue
		}
		if strings.Contains(valTrimmed, ": ") || strings.HasSuffix(valTrimmed, ":") ||
			strings.HasPrefix(valTrimmed, "@") || strings.HasPrefix(valTrimmed, "`") {
			spaceLen := len(valWithSpaces) - len(strings.TrimLeft(valWithSpaces, " \t"))
			spacing := valWithSpaces[:spaceLen]
			if spacing == "" {
				spacing = " "
			}
			result = append(result, prefix+spacing+strconv.Quote(valTrimmed))
			continue
		}
		result = append(result, line)
	}
	return strings.Join(result, "\n")
}

func (d Document) Bytes() ([]byte, error) {
	front, err := yaml.Marshal(d.FrontMatter)
	if err != nil {
		return nil, err
	}
	var result bytes.Buffer
	result.WriteString("---\n")
	result.Write(front)
	result.WriteString("---\n")
	result.WriteString(d.Body)
	if !strings.HasSuffix(d.Body, "\n") {
		result.WriteByte('\n')
	}
	return result.Bytes(), nil
}

func WriteDocument(path string, document Document) error {
	data, err := document.Bytes()
	if err != nil {
		return err
	}
	return fsx.AtomicWriteFile(path, data, 0o600)
}

var workflowStatusDirectories = map[string]bool{
	"todo": true, "working": true, "review": true, "reviewing": true,
	"waiting": true, "done": true, "failed": true, "cancelled": true,
	"proposed": true, "planning": true, "active": true, "abandoned": true,
}

// providerTurnLockPath is stable while a document moves between sibling
// workflow status directories. Claims, runtime transitions, and provider-turn
// reservations must all use this lock so a replacement cannot recreate the
// source side of a concurrent move.
func providerTurnLockPath(documentPath string) string {
	directory := filepath.Dir(filepath.Clean(documentPath))
	lockDirectory := directory
	if workflowStatusDirectories[filepath.Base(directory)] {
		lockDirectory = filepath.Dir(directory)
	}
	return filepath.Join(lockDirectory, "."+filepath.Base(documentPath)+".provider-turn.lock")
}

func ClaimDocument(source, target, status string, now time.Time) (Document, error) {
	return claimDocument(source, target, status, "attempt", now)
}

func claimDocument(source, target, status, attemptField string, now time.Time) (Document, error) {
	return claimDocumentWithWriter(source, target, status, attemptField, now, WriteDocument)
}

func claimDocumentWithWriter(source, target, status, attemptField string, now time.Time, write func(string, Document) error) (Document, error) {
	lock, err := lockProviderTurn(source)
	if err != nil {
		return Document{}, err
	}
	defer unlockProviderTurn(lock)
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		return Document{}, err
	}
	if _, err := os.Stat(target); err == nil {
		return Document{}, fmt.Errorf("claim target already exists: %s", target)
	}
	if err := os.Rename(source, target); err != nil {
		return Document{}, err
	}
	document, err := ReadDocument(target)
	if err != nil {
		_ = os.Rename(target, source)
		return Document{}, err
	}
	document.FrontMatter["status"] = status
	document.FrontMatter["updated_at"] = now.UTC().Format(time.RFC3339)
	if first, ok := frontMatterTime(document.FrontMatter["first_assigned_at"]); !ok || first.After(now) {
		document.FrontMatter["first_assigned_at"] = now.UTC().Format(time.RFC3339)
	}
	attempt, credit := claimAttempt(document.FrontMatter, attemptField)
	document.FrontMatter[attemptField] = attempt
	switch credit {
	case claimCreditHonoured:
		appendProgress(&document, now, resumeCreditNote(attempt))
	case claimCreditExhausted:
		appendProgress(&document, now, resumeCreditExhaustedNote(attempt))
	}
	if err := write(target, document); err != nil {
		return Document{}, err
	}
	return document, nil
}

// ReserveProviderTurn defines the durable iteration boundary. The increment
// and first-assignment repair are committed under a cross-process sibling lock
// immediately before provider delivery. A crash after reservation retains the
// count; recovery reserves a new iteration and can never reuse the old one.
func ReserveProviderTurn(path string, now time.Time) (time.Time, int, error) {
	return reserveProviderTurn(path, now, nil)
}

func reserveProviderTurn(path string, now time.Time, afterRead func()) (time.Time, int, error) {
	lock, err := lockProviderTurn(path)
	if err != nil {
		return time.Time{}, 0, err
	}
	defer unlockProviderTurn(lock)
	document, err := ReadDocument(path)
	if err != nil {
		return time.Time{}, 0, err
	}
	if afterRead != nil {
		afterRead()
	}
	first, ok := frontMatterTime(document.FrontMatter["first_assigned_at"])
	if !ok || first.After(now) {
		first = now.UTC()
		document.FrontMatter["first_assigned_at"] = first.Format(time.RFC3339)
	}
	iterations := frontMatterNonNegativeInt(document.FrontMatter["provider_iterations"]) + 1
	document.FrontMatter["provider_iterations"] = iterations
	document.FrontMatter["updated_at"] = now.UTC().Format(time.RFC3339)
	if err := WriteDocument(path, document); err != nil {
		return time.Time{}, 0, err
	}
	// A provider agent moves its own claimed Markdown file directly and cannot
	// participate in Spynel's advisory lock. If that move landed after our read
	// but before the atomic replacement, the replacement recreated the old path.
	// Merge only the durable timing fields into the moved copy and remove the
	// stale source before admitting provider delivery.
	if movedPath, moved, err := movedWorkflowDocument(path, document); err != nil {
		return time.Time{}, 0, err
	} else if moved {
		movedDocument, err := ReadDocument(movedPath)
		if err != nil {
			return time.Time{}, 0, err
		}
		movedFirst, movedFirstOK := frontMatterTime(movedDocument.FrontMatter["first_assigned_at"])
		if !movedFirstOK || first.Before(movedFirst) {
			movedDocument.FrontMatter["first_assigned_at"] = first.Format(time.RFC3339)
		}
		if frontMatterNonNegativeInt(movedDocument.FrontMatter["provider_iterations"]) < iterations {
			movedDocument.FrontMatter["provider_iterations"] = iterations
		}
		if err := WriteDocument(movedPath, movedDocument); err != nil {
			return time.Time{}, 0, err
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return time.Time{}, 0, err
		}
	}
	return first, iterations, nil
}

func movedWorkflowDocument(path string, reserved Document) (string, bool, error) {
	directory := filepath.Dir(filepath.Clean(path))
	if !workflowStatusDirectories[filepath.Base(directory)] {
		return "", false, nil
	}
	base := filepath.Dir(directory)
	name := filepath.Base(path)
	id := documentID(reserved)
	var match string
	for status := range workflowStatusDirectories {
		candidate := filepath.Join(base, status, name)
		if filepath.Clean(candidate) == filepath.Clean(path) {
			continue
		}
		candidateDocument, err := ReadDocument(candidate)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return "", false, err
		}
		if candidateID := documentID(candidateDocument); id != "" && candidateID != id {
			continue
		}
		if match != "" {
			return "", false, fmt.Errorf("multiple moved workflow documents found for %s", path)
		}
		match = candidate
	}
	return match, match != "", nil
}

func DurableTiming(document Document) (time.Time, int, bool) {
	first, ok := frontMatterTime(document.FrontMatter["first_assigned_at"])
	if !ok {
		return time.Time{}, 0, false
	}
	return first, frontMatterNonNegativeInt(document.FrontMatter["provider_iterations"]), true
}

func frontMatterTime(value any) (time.Time, bool) {
	switch typed := value.(type) {
	case string:
		parsed, err := time.Parse(time.RFC3339, typed)
		return parsed, err == nil
	case time.Time:
		return typed, !typed.IsZero()
	default:
		return time.Time{}, false
	}
}

func frontMatterNonNegativeInt(value any) int {
	var result int
	switch typed := value.(type) {
	case int:
		result = typed
	case int64:
		result = int(typed)
	case uint64:
		if typed <= uint64(^uint(0)>>1) {
			result = int(typed)
		}
	case float64:
		if typed == float64(int(typed)) {
			result = int(typed)
		}
	}
	if result < 0 {
		return 0
	}
	return result
}

func DocumentDue(path string, now time.Time) (bool, error) {
	return documentDueForPhase(path, now, "")
}

func documentDueForPhase(path string, now time.Time, phase string) (bool, error) {
	document, err := ReadDocument(path)
	if err != nil {
		return false, err
	}
	return documentDue(document, now, phase), nil
}

// documentDue is the parse-free half of documentDueForPhase: the claim loop
// already read the document, so eligibility must not trigger a second read.
func documentDue(document Document, now time.Time, phase string) bool {
	fields := []string{"not_before", "next_dispatch_at", "next_review_at"}
	switch phase {
	case phaseTaskImplementation, phaseGoalPlanning:
		fields = []string{"not_before", "next_dispatch_at"}
	case phaseTaskReview, phaseGoalReview:
		// Review is queued only after the preceding phase has established
		// eligibility. Reapplying dispatch dates or a goal checkpoint here can
		// turn a durable queue transition into an unintended second delay.
		fields = nil
	}
	for _, field := range fields {
		var when time.Time
		switch value := document.FrontMatter[field].(type) {
		case string:
			when, _ = time.Parse(time.RFC3339, value)
		case time.Time:
			when = value
		}
		if when.IsZero() {
			continue
		}
		if when.After(now) {
			return false
		}
	}
	return true
}

// documentIsGoalDerived reports whether a document belongs to a goal: a task
// the goal planner (or one of the goal's workers) filed carries a goal_id.
// Goal-derived documents claim ahead of standalone ones (Joey, 2026-10-03:
// the current goal is paramount); a blank or non-string goal_id is standalone.
func documentIsGoalDerived(document Document) bool {
	id, _ := document.FrontMatter["goal_id"].(string)
	return strings.TrimSpace(id) != ""
}

// repairLane is the front-matter lane value that lets a task claim the
// reserved repair capacity beyond orchestrator.max_parallel.
const repairLane = "repair"

// documentIsRepair reports whether a document is stamped `lane: repair`:
// a task the repair lane (a Helm card about a silent, stalled or quota-walled
// worker) may claim the reserved slot while stuck workers hold every normal
// slot (2026-10-15). Any other lane value, including a blank one, is a
// normal task and never touches the reserve.
func documentIsRepair(document Document) bool {
	lane, _ := document.FrontMatter["lane"].(string)
	return strings.TrimSpace(lane) == repairLane
}

// documentCreatedAt is the claim-ordering timestamp, taken from the front
// matter the caller already parsed; it never re-reads the file. A missing or
// unparseable value reports !ok so the document sorts last instead of first.
func documentCreatedAt(document Document) (time.Time, bool) {
	switch value := document.FrontMatter["created_at"].(type) {
	case string:
		parsed, err := time.Parse(time.RFC3339, value)
		if err != nil || parsed.IsZero() {
			return time.Time{}, false
		}
		return parsed, true
	case time.Time:
		if value.IsZero() {
			return time.Time{}, false
		}
		return value, true
	}
	return time.Time{}, false
}
