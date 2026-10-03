package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Goal work outranks standalone work. Joey, 2026-10-03: "its current goal is
// PARAMOUNT". Until now the claim loop knew only created_at, so a task the
// goal spawned later (stamped "now") queued behind every older standalone task,
// and the goal's own planning/review phases were scanned after ALL task
// implementation, so with a full todo/ they never got a slot. These tests pin
// the rule: goal-derived documents (a goal_id in front matter) and the goals
// route claim first; inside each class the oldest-first order is unchanged.

func writeGoalDerivedFixture(t *testing.T, dir, name, createdAt string) string {
	t.Helper()
	front := map[string]any{
		"id": "fixture-" + name, "title": "fixture " + name, "status": "todo",
		"created_at": createdAt, "updated_at": "2026-10-03T15:00:00Z", "attempt": 0,
		"review_required": true, "goal_id": "goals-fixture", "goal_round": 1,
	}
	path := filepath.Join(dir, name)
	if err := WriteDocument(path, Document{FrontMatter: front, Body: "# fixture\n\n## Progress\n\n- Fixture document.\n"}); err != nil {
		t.Fatal(err)
	}
	return path
}

// A goal-derived task filed LATER than a standalone one still takes the only
// free slot. This is the hole a hand-stamped created_at could not close: a task
// spawned by the goal after the stamping carries "now" and went to the back.
func TestGoalDerivedTaskClaimsAheadOfOlderStandaloneTask(t *testing.T) {
	cfg, fake, manager, finish := claimOrderTestManager(t)
	route := workflowRoutes()[0]
	todoDir := cfg.Resolve(route.Source)
	writeClaimFixture(t, todoDir, "tasks-standalone-old.md", "2026-09-01T00:00:00Z")
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-new.md", "2026-10-03T18:30:00Z")

	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	claimOrderAssertSingleClaim(t, cfg, fake, finish, "tasks-goal-new.md")
}

// Inside the goal class the order is still oldest-first (then filename).
func TestGoalDerivedTasksKeepOldestFirstAmongThemselves(t *testing.T) {
	cfg, fake, manager, finish := claimOrderTestManager(t)
	route := workflowRoutes()[0]
	todoDir := cfg.Resolve(route.Source)
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-aaa-newer.md", "2026-10-03T18:00:00Z")
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-zzz-older.md", "2026-10-03T17:00:00Z")

	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	claimOrderAssertSingleClaim(t, cfg, fake, finish, "tasks-goal-zzz-older.md")
}

// With no goal work queued, a standalone task still claims (nothing starves).
func TestStandaloneTaskStillClaimsWhenNoGoalWorkIsQueued(t *testing.T) {
	cfg, fake, manager, finish := claimOrderTestManager(t)
	route := workflowRoutes()[0]
	todoDir := cfg.Resolve(route.Source)
	writeClaimFixture(t, todoDir, "tasks-standalone-a.md", "2026-10-03T15:00:00Z")
	writeClaimFixture(t, todoDir, "tasks-standalone-b.md", "2026-10-03T16:00:00Z")

	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	claimOrderAssertSingleClaim(t, cfg, fake, finish, "tasks-standalone-a.md")
}

// The goal's own planning phase takes a free slot ahead of standalone task
// implementation. Observed live 2026-10-03: an invalid goal returned to
// goals/proposed sat there while 30+ tasks kept every slot busy, because the
// tasks route was scanned first and canAdmitClaim() cut the goals scan off.
func TestGoalPlanningClaimsBeforeStandaloneTaskImplementation(t *testing.T) {
	cfg, fake, manager, finish := claimOrderTestManager(t)
	taskRoute := workflowRoutes()[0]
	goalRoute := workflowRoutes()[1]
	writeClaimFixture(t, cfg.Resolve(taskRoute.Source), "tasks-standalone.md", "2026-09-01T00:00:00Z")
	goal, err := Create(cfg, "goals", "goal planning must not starve", "")
	if err != nil {
		t.Fatal(err)
	}
	goalName := filepath.Base(goal)
	goalBase := filepath.Dir(cfg.Resolve(goalRoute.Source))

	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(goalBase, "planning", goalName)); err != nil {
		t.Fatalf("the goal did not take the only free slot for planning: %v", err)
	}
	taskBase := filepath.Dir(cfg.Resolve(taskRoute.Source))
	if _, err := os.Stat(filepath.Join(taskBase, "todo", "tasks-standalone.md")); err != nil {
		t.Fatalf("a standalone task was claimed ahead of goal planning: %v", err)
	}
	finish()
	if fake.calls != 1 {
		t.Fatalf("provider dispatches = %d, want exactly the goal planning claim", fake.calls)
	}
}

// Goal review (the step that closes a goal) takes a free slot ahead of a
// standalone task's review.
func TestGoalReviewClaimsBeforeStandaloneTaskReview(t *testing.T) {
	cfg, fake, manager, finish := claimOrderTestManager(t)
	taskRoute := workflowRoutes()[0]
	goalRoute := workflowRoutes()[1]
	taskBase := filepath.Dir(cfg.Resolve(taskRoute.Source))
	goalBase := filepath.Dir(cfg.Resolve(goalRoute.Source))

	standalone := writeClaimFixture(t, cfg.Resolve(taskRoute.Source), "tasks-standalone-review.md", "2026-09-01T00:00:00Z")
	if err := moveDocument(standalone, filepath.Join(taskBase, "review", filepath.Base(standalone)), "review", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	goal, err := Create(cfg, "goals", "goal review must not starve", "")
	if err != nil {
		t.Fatal(err)
	}
	goalName := filepath.Base(goal)
	if err := moveDocument(goal, filepath.Join(goalBase, "review", goalName), "review", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(goalBase, "reviewing", goalName)); err != nil {
		t.Fatalf("the goal review did not take the only free slot: %v", err)
	}
	if _, err := os.Stat(filepath.Join(taskBase, "review", "tasks-standalone-review.md")); err != nil {
		t.Fatalf("a standalone task review was claimed ahead of the goal review: %v", err)
	}
	finish()
	if fake.calls != 1 {
		t.Fatalf("provider dispatches = %d, want exactly the goal review claim", fake.calls)
	}
}

// Priority means the top item takes capacity first AND everything below it
// yields. Claiming goal work first (above) is only half of that: with two free
// slots a standalone task still started beside the goal's step. While goal work
// is queued or in flight, no standalone document is claimed; tasks already
// running finish (no pre-emption). Goal work parked in waiting/ does not hold.

func twoSlotManager(t *testing.T) (*Manager, func(), string, string) {
	t.Helper()
	cfg, _, manager, finish := claimOrderTestManager(t)
	manager.capacityMu.Lock()
	manager.capacityLimit = 2
	manager.capacityMu.Unlock()
	taskRoute := workflowRoutes()[0]
	return manager, finish, cfg.Resolve(taskRoute.Source), filepath.Dir(cfg.Resolve(taskRoute.Source))
}

func TestStandaloneWaitsWhileGoalTaskIsInFlight(t *testing.T) {
	manager, finish, todoDir, base := twoSlotManager(t)
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-running.md", "2026-10-03T17:00:00Z")
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(base, "working", "tasks-goal-running.md")); err != nil {
		t.Fatalf("goal task was not claimed: %v", err)
	}
	// A free slot exists (limit 2, one in flight) and a standalone task arrives.
	writeClaimFixture(t, todoDir, "tasks-standalone-late.md", "2026-09-01T00:00:00Z")
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(base, "todo", "tasks-standalone-late.md")); err != nil {
		t.Fatalf("a standalone task started while goal work was in flight: %v", err)
	}
	finish()
}

func TestStandaloneWaitsWhileGoalIsWaitingToBePlanned(t *testing.T) {
	cfg, fake, manager, finish := claimOrderTestManager(t)
	manager.capacityMu.Lock()
	manager.capacityLimit = 2
	manager.capacityMu.Unlock()
	taskRoute := workflowRoutes()[0]
	goalRoute := workflowRoutes()[1]
	writeClaimFixture(t, cfg.Resolve(taskRoute.Source), "tasks-standalone.md", "2026-09-01T00:00:00Z")
	goal, err := Create(cfg, "goals", "goal planning holds standalone work", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(cfg.Resolve(goalRoute.Source)), "planning", filepath.Base(goal))); err != nil {
		t.Fatalf("goal was not claimed for planning: %v", err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(cfg.Resolve(taskRoute.Source)), "todo", "tasks-standalone.md")); err != nil {
		t.Fatalf("a standalone task started while the goal was being planned: %v", err)
	}
	finish()
	if fake.calls != 1 {
		t.Fatalf("provider dispatches = %d, want only the goal planning claim", fake.calls)
	}
}

// Goal work that is parked (waiting/) does not hold the board: a sleeping goal
// task must not freeze every other task for hours.
func TestStandaloneRunsWhileGoalTaskIsParked(t *testing.T) {
	manager, finish, todoDir, base := twoSlotManager(t)
	parked := writeGoalDerivedFixture(t, todoDir, "tasks-goal-parked.md", "2026-10-03T17:00:00Z")
	if err := moveDocument(parked, filepath.Join(base, "waiting", filepath.Base(parked)), "waiting", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	writeClaimFixture(t, todoDir, "tasks-standalone.md", "2026-10-03T15:00:00Z")
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(base, "working", "tasks-standalone.md")); err != nil {
		t.Fatalf("a parked goal task froze standalone work: %v", err)
	}
	finish()
}
