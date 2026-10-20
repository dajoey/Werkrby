package orchestrator

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/roster"
	"github.com/agent0ai/spynel/internal/workspace"
)

// writeLegacyImplementerRecords leaves the implementer record the retired family rule kept
// (runtime/roster-implemented.json) naming who wrote each task, as live state dirs still carry it.
func writeLegacyImplementerRecords(t *testing.T, stateDir string, seats map[string]string) {
	t.Helper()
	records := map[string]map[string]any{}
	for task, seat := range seats {
		records[task] = map[string]any{"seat": seat, "at": time.Now().UTC()}
	}
	data, err := json.Marshal(records)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(stateDir, "runtime"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stateDir, "runtime", "roster-implemented.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// A review runs on the seat its role names, whoever wrote the work: there is no model-family
// rule. A Gemini-written task is reviewed by the Gemini routine reviewer, a lone same-model seat
// is never held, and the risk-high tier keeps its own seat.
func TestReviewRunsOnTheRoleSeatWhoeverWroteIt(t *testing.T) {
	stateDir := t.TempDir()
	body := `
staff:
  analyst: {runner: agy, headless: true, args: ["--model", "gemini-3.8-flash-high"], fallback: flash-backup}
  flash-backup: {runner: pi, args: ["--model", "zai/glm-5.3-flash", "--thinking", "high"]}
  geminibig: {runner: agy, headless: true, args: ["--model", "gemini-4-argon-high"]}
  glmwriter: {runner: pi, args: ["--model", "zai/glm-5.3", "--thinking", "max"]}
roles:
  task_review: analyst
  task_review_medium: analyst
  task_review_high_risk: glmwriter
role_alternates:
  task_review_medium: [flash-backup]
`
	if err := os.WriteFile(roster.Path(stateDir), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	staffing, err := roster.Load(stateDir)
	if err != nil || staffing == nil {
		t.Fatalf("roster load (a leftover role_alternates key must still load): %v", err)
	}
	// Every reviewed task was written on the Gemini seat, the same model as the routine reviewer.
	writeLegacyImplementerRecords(t, stateDir, map[string]string{
		"tasks-routine.md": "geminibig", "tasks-medium.md": "geminibig", "tasks-high.md": "geminibig",
	})
	for _, c := range []struct{ name, risk, want string }{
		{"routine", "", "analyst"},
		{"medium", "medium", "analyst"},
		{"high", "high", "glmwriter"},
	} {
		frontMatter := map[string]any{}
		if c.risk != "" {
			frontMatter["risk"] = c.risk
		}
		seat, ok := rosterSeat(staffing, stateDir, phaseTaskReview, frontMatter, "tasks-"+c.name+".md", nil)
		if !ok || seat != c.want {
			t.Errorf("%s review: picked %q (ok=%t), want %q", c.name, seat, ok, c.want)
		}
	}
}

// A queued review is claimed whatever model wrote the work: nothing holds it for a reviewer of
// another family.
func TestReviewIsClaimedWhateverModelWroteIt(t *testing.T) {
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
	// analyst is the only Gemini reviewer and has no fallback: the retired rule held this review.
	writeLegacyImplementerRecords(t, state, map[string]string{gem: "geminibig", glm: "glmwriter"})
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
	for _, name := range []string{gem, glm} {
		if _, err := os.Stat(filepath.Join(base, "reviewing", name)); err != nil {
			t.Errorf("review of %s was not claimed: %v", name, err)
		}
	}
}
