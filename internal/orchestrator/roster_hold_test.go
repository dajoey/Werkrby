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

// A seat on a pace hold (runtime/roster-holds.json) keeps its queued work unclaimed: a review,
// an escalating retry and goal planning that would start on a held seat all wait in place with
// no attempt spent, while work for an unheld seat is claimed as before.
func TestRosterHoldLeavesHeldWorkUnclaimed(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(config.PathForRoot(root))
	state := cfg.StatePath()
	body := `
staff:
  implementer: {runner: opencode}
  ops: {runner: agy}
  architect: {runner: pi}
  flash-backup: {runner: pi}
  thinker: {runner: pi}
roles:
  task_implementation: implementer
  task_review: flash-backup
  goal_planning: thinker
escalation: {after_attempt: 2, staff: architect}
`
	if err := os.WriteFile(roster.Path(state), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	writeHolds := func(staff ...string) {
		t.Helper()
		entries := map[string]map[string]string{}
		for _, name := range staff {
			entries[name] = map[string]string{"until": time.Now().Add(time.Hour).UTC().Format(time.RFC3339), "reason": "zai-pace"}
		}
		data, _ := json.Marshal(map[string]any{"updated_at": time.Now().UTC().Format(time.RFC3339), "staff": entries})
		if err := os.MkdirAll(filepath.Join(state, "runtime"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(state, "runtime", "roster-holds.json"), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeHolds("flash-backup", "architect", "thinker")

	tasks := workflowRoutes()[0]
	base := filepath.Dir(cfg.Resolve(tasks.Source))
	newTask := func(title, extra string) string {
		t.Helper()
		path, err := Create(cfg, "tasks", title, "")
		if err != nil {
			t.Fatal(err)
		}
		document, err := ReadDocument(path)
		if err != nil {
			t.Fatal(err)
		}
		document.FrontMatter["staff"] = "ops"
		if extra != "" {
			document.FrontMatter["attempt"] = 2
		}
		if err := WriteDocument(path, document); err != nil {
			t.Fatal(err)
		}
		return filepath.Base(path)
	}
	escalating := newTask("ops retry that escalates", "attempt2")
	plain := newTask("ops first try", "")
	reviewName := newTask("written by agy", "")
	if err := moveDocument(filepath.Join(base, "todo", reviewName), filepath.Join(base, "review", reviewName), "review", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	manager := New(cfg, newFakeRecipient(), extensions.Runner{Directory: filepath.Join(root, "missing")})
	t.Cleanup(manager.Wait) // claimed work dispatches in the background; let it finish before TempDir cleanup
	scan := func(source, claimed, phase string) {
		t.Helper()
		if err := manager.scanPhaseQueue(context.Background(), tasks, filepath.Join(base, source), filepath.Join(base, claimed), phase); err != nil {
			t.Fatal(err)
		}
	}
	exists := func(dir, name string) bool {
		_, err := os.Stat(filepath.Join(base, dir, name))
		return err == nil
	}

	scan("review", "reviewing", phaseTaskReview)
	if !exists("review", reviewName) || exists("reviewing", reviewName) {
		t.Error("a review whose seat (flash-backup) is held was claimed")
	}
	scan("todo", "working", phaseTaskImplementation)
	if !exists("todo", escalating) {
		t.Error("an attempt-3 retry that escalates onto held architect was claimed")
	}
	if document, err := ReadDocument(filepath.Join(base, "todo", escalating)); err == nil && numberValue(document.FrontMatter["attempt"]) != 2 {
		t.Errorf("a held task's attempt changed: %v", document.FrontMatter["attempt"])
	}
	if exists("todo", plain) {
		t.Fatal("an ops task on an unheld seat was not claimed")
	}

	goals := workflowRoutes()[1]
	goalPath, err := Create(cfg, "goals", "paced goal", "")
	if err != nil {
		t.Fatal(err)
	}
	goalDir := filepath.Dir(goalPath)
	if err := manager.scanPhaseQueue(context.Background(), goals, goalDir, cfg.Resolve(goals.Working), phaseGoalPlanning); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(goalPath); err != nil {
		t.Error("goal planning on held thinker was claimed")
	}

	writeHolds() // the hold lifts
	scan("review", "reviewing", phaseTaskReview)
	if exists("review", reviewName) {
		t.Fatal("the review stayed queued after the hold lifted")
	}
}
