package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/roster"
	"github.com/agent0ai/spynel/internal/workspace"
)

// The routine reviewer is resolved against the implementer's model family at selection time, so
// the same-family bounce (a full review session spent for no verdict) cannot start: fixture A -
// a Gemini author's routine review never starts on the Gemini routine reviewer (a different-family
// seat when the fallback chain reaches one, otherwise the review waits unclaimed with no attempt
// spent); fixture B - a GLM author's review is unchanged.
func TestRosterSeatResolvesReviewFamilyAtSelection(t *testing.T) {
	stateDir := t.TempDir()
	body := `
staff:
  analyst: {runner: agy, headless: true, args: ["--model", "gemini-3.8-flash-high"], fallback: flash-backup}
  flash-backup: {runner: pi, args: ["--model", "zai/glm-5.3-flash", "--thinking", "high"]}
  geminibig: {runner: agy, headless: true, args: ["--model", "gemini-4-argon-high"]}
  glmwriter: {runner: pi, args: ["--model", "zai/glm-5.3", "--thinking", "max"]}
roles:
  task_review: analyst
  task_review_high_risk: glmwriter
`
	if err := os.WriteFile(roster.Path(stateDir), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	staffing, err := roster.Load(stateDir)
	if err != nil || staffing == nil {
		t.Fatalf("roster load: %v", err)
	}
	record := func(state, task, seat string) {
		t.Helper()
		if err := roster.RecordImplementer(state, task, seat, time.Now()); err != nil {
			t.Fatal(err)
		}
	}

	// Fixture A1: a Gemini author's routine review takes the different-family seat the
	// fallback chain reaches, not the Gemini routine reviewer.
	record(stateDir, "tasks-a1.md", "geminibig")
	seat, ok, hold := rosterSeat(staffing, stateDir, phaseTaskReview, map[string]any{}, "tasks-a1.md", nil)
	if !ok || seat != "flash-backup" || hold != "" {
		t.Errorf("fixture A1: review of a Gemini author picked %q (ok=%t, hold=%q), want flash-backup", seat, ok, hold)
	}

	// Fixture A2: when every reachable seat shares the author's family the review is held
	// unclaimed, not started on the same family.
	alone := t.TempDir()
	bodyAlone := `
staff:
  analyst: {runner: agy, headless: true, args: ["--model", "gemini-3.8-flash-high"]}
  geminibig: {runner: agy, headless: true, args: ["--model", "gemini-4-argon-high"]}
  glmwriter: {runner: pi, args: ["--model", "zai/glm-5.3", "--thinking", "max"]}
roles:
  task_review: analyst
  task_review_high_risk: glmwriter
`
	if err := os.WriteFile(roster.Path(alone), []byte(bodyAlone), 0o600); err != nil {
		t.Fatal(err)
	}
	staffingAlone, err := roster.Load(alone)
	if err != nil || staffingAlone == nil {
		t.Fatalf("roster load: %v", err)
	}
	record(alone, "tasks-a2.md", "geminibig")
	seat, ok, hold = rosterSeat(staffingAlone, alone, phaseTaskReview, map[string]any{}, "tasks-a2.md", nil)
	if !ok || hold == "" {
		t.Errorf("fixture A2: same-family review not held (seat=%q, ok=%t, hold=%q)", seat, ok, hold)
	}

	// Fixture B: a GLM author's routine review is unchanged - the Gemini reviewer takes it.
	record(stateDir, "tasks-b.md", "glmwriter")
	seat, ok, hold = rosterSeat(staffing, stateDir, phaseTaskReview, map[string]any{}, "tasks-b.md", nil)
	if !ok || seat != "analyst" || hold != "" {
		t.Errorf("fixture B: review of a GLM author picked %q (ok=%t, hold=%q), want analyst unchanged", seat, ok, hold)
	}

	// The risk-high tier keeps its own ladder: never held, never walked.
	record(stateDir, "tasks-hi.md", "geminibig")
	seat, ok, hold = rosterSeat(staffing, stateDir, phaseTaskReview, map[string]any{"risk": "high"}, "tasks-hi.md", nil)
	if !ok || seat != "glmwriter" || hold != "" {
		t.Errorf("risk high: picked %q (ok=%t, hold=%q), want glmwriter without a family hold", seat, ok, hold)
	}
}

func TestReviewFamilyHoldLeavesSameFamilyReviewUnclaimed(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(config.PathForRoot(root))
	state := cfg.StatePath()
	body := `
staff:
  analyst: {runner: agy, headless: true, args: ["--model", "gemini-3.8-flash-high"]}
  geminibig: {runner: agy, headless: true, args: ["--model", "gemini-4-argon-high"]}
  glmwriter: {runner: pi, args: ["--model", "zai/glm-5.3", "--thinking", "max"]}
roles:
  task_review: analyst
`
	if err := os.WriteFile(roster.Path(state), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	tasks := workflowRoutes()[0]
	base := filepath.Dir(cfg.Resolve(tasks.Source))
	newTask := func(title string) string {
		t.Helper()
		path, err := Create(cfg, "tasks", title, "")
		if err != nil {
			t.Fatal(err)
		}
		return filepath.Base(path)
	}
	gem := newTask("written on gemini")
	glm := newTask("written on glm")
	if err := roster.RecordImplementer(state, gem, "geminibig", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := roster.RecordImplementer(state, glm, "glmwriter", time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{gem, glm} {
		if err := moveDocument(filepath.Join(base, "todo", name), filepath.Join(base, "review", name), "review", time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}

	manager := New(cfg, newFakeRecipient(), extensions.Runner{Directory: filepath.Join(root, "missing")})
	t.Cleanup(manager.Wait) // claimed work dispatches in the background; let it finish before TempDir cleanup
	if err := manager.scanPhaseQueue(context.Background(), tasks, filepath.Join(base, "review"), filepath.Join(base, "reviewing"), phaseTaskReview); err != nil {
		t.Fatal(err)
	}
	exists := func(dir, name string) bool {
		_, err := os.Stat(filepath.Join(base, dir, name))
		return err == nil
	}

	// Fixture A: the Gemini author's review waits unclaimed, no attempt spent.
	if !exists("review", gem) || exists("reviewing", gem) {
		t.Error("a routine review of a Gemini author with no reachable different-family reviewer seat was claimed")
	}
	if document, err := ReadDocument(filepath.Join(base, "review", gem)); err == nil && numberValue(document.FrontMatter["review_attempt"]) != 0 {
		t.Errorf("a family-held review's attempt changed: %v", document.FrontMatter["review_attempt"])
	}
	// Fixture B: the GLM author's review claims as before.
	if exists("review", glm) || !exists("reviewing", glm) {
		t.Error("a routine review of a GLM author was not claimed as before")
	}
}
