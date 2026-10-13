package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// breakFrontMatter keeps every key, including goal_id, but makes the YAML unreadable.
func breakFrontMatter(t *testing.T, path string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	broken := strings.Replace(string(raw), "---\n", "---\nbroken: [unclosed\n", 1)
	if err := os.WriteFile(path, []byte(broken), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadDocument(path); err == nil {
		t.Fatal("test setup: corrupted round task still parses")
	}
}

func roundTaskPaths(t *testing.T, manager *Manager, goal string) []linkedTask {
	t.Helper()
	document, err := ReadDocument(goal)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := manager.goalRoundTasks(document)
	if err != nil || len(tasks) == 0 {
		t.Fatalf("round tasks = %#v, %v", tasks, err)
	}
	return tasks
}

// An unreadable round task must cost only a skipped scan for its goal. Before the fix
// linkedRoundTasks dropped it silently, validateGoalActivation saw fewer linked tasks than
// round_task_ids and advanceActiveGoals moved the active goal back to proposed/.
func TestUnparseableRoundTaskDoesNotBounceActiveGoal(t *testing.T) {
	cfg, _, manager := workflowTestManager(t)
	goal := writeActiveGoalRound(t, cfg, "all_round_tasks_settled", time.Now().Add(7*24*time.Hour), "todo", "todo")
	goalBase := filepath.Dir(filepath.Dir(goal))

	// Control: with every round task readable the goal stays active.
	if err := manager.advanceActiveGoals(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(goal); err != nil {
		t.Fatalf("control: goal left active/: %v", err)
	}

	tasks := roundTaskPaths(t, manager, goal)
	breakFrontMatter(t, tasks[0].Path)
	if err := manager.advanceActiveGoals(); err != nil {
		t.Fatalf("one unreadable round task aborted the goal scan: %v", err)
	}
	if _, err := os.Stat(goal); err != nil {
		t.Fatalf("goal left active/ because a round task was unreadable: %v", err)
	}
	if _, err := os.Stat(filepath.Join(goalBase, "proposed", filepath.Base(goal))); err == nil {
		t.Fatal("goal was bounced to proposed/")
	}
	if _, err := os.Stat(m1RepairPath(manager, tasks[0].Path)); err != nil {
		t.Fatalf("no repair task filed for the unreadable round task: %v", err)
	}
}

// A corrupt document that is not linked to the goal must not hold the goal back.
func TestUnparseableUnrelatedTaskDoesNotHoldActiveGoal(t *testing.T) {
	cfg, _, manager := workflowTestManager(t)
	goal := writeActiveGoalRound(t, cfg, "all_round_tasks_settled", time.Now().Add(7*24*time.Hour), "done")
	stray := filepath.Join(cfg.StatePath("tasks", "done"), "tasks-stray-corrupt.md")
	writeScratchDocument(t, stray, "broken: [unclosed\n")
	if err := manager.advanceActiveGoals(); err != nil {
		t.Fatal(err)
	}
	review := filepath.Join(filepath.Dir(filepath.Dir(goal)), "review", filepath.Base(goal))
	if _, err := os.Stat(review); err != nil {
		t.Fatalf("an unrelated corrupt task held the settled goal back: %v", err)
	}
}

func m1RepairPath(manager *Manager, broken string) string {
	return manager.Config.StatePath("tasks", "todo", unparseableRepairID(broken)+".md")
}

// The whole scan, not only advanceActiveGoals, keeps the goal active and still files one repair.
func TestScanOnceKeepsGoalWithUnreadableRoundTaskActive(t *testing.T) {
	cfg, _, manager := workflowTestManager(t)
	goal := writeActiveGoalRound(t, cfg, "all_round_tasks_settled", time.Now().Add(7*24*time.Hour), "done", "done")
	tasks := roundTaskPaths(t, manager, goal)
	breakFrontMatter(t, tasks[0].Path)
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatalf("ScanOnce aborted: %v", err)
	}
	manager.Wait()
	if _, err := os.Stat(goal); err != nil {
		t.Fatalf("goal left active/ after a full scan: %v", err)
	}
	if _, err := os.Stat(m1RepairPath(manager, tasks[0].Path)); err != nil {
		if len(liveRepairTasks(t, cfg)) != 1 {
			t.Fatalf("expected exactly one repair task: %v", err)
		}
	}
}
