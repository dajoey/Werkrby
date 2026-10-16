package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	rosterpkg "github.com/agent0ai/spynel/internal/roster"
)

// Goal work claims first, so a goal that files more tasks than it settles used to hold every slot
// while normal operations (standalone tasks) waited. The ops lane keeps slots for them while
// they wait. These tests pin that rule through ScanOnce, the live claim path, and the other half:
// with no ops work waiting, goal work still fills every slot.

func setOpsLane(manager *Manager, lane config.OpsLane) {
	manager.runtimeConfigMu.Lock()
	manager.runtimeConfig.Orchestrator.OpsLane = lane
	manager.runtimeConfigMu.Unlock()
}

func opsLaneOn(reserved int) config.OpsLane {
	return config.OpsLane{Enabled: true, ReservedSlots: reserved}
}

// writeOpsFixture is a standalone task; notBefore, when set, keeps it from being due.
func writeOpsFixture(t *testing.T, dir, name, createdAt, notBefore string) string {
	t.Helper()
	path := writeClaimFixture(t, dir, name, createdAt)
	if notBefore == "" {
		return path
	}
	document, err := ReadDocument(path)
	if err != nil {
		t.Fatal(err)
	}
	document.FrontMatter["not_before"] = notBefore
	if err := WriteDocument(path, document); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestOpsLaneGoalWorkFillsEverySlotWhenNoOpsTaskWaits(t *testing.T) {
	manager, finish, todoDir, base := slots(t, 4)
	setOpsLane(manager, opsLaneOn(2))
	for i, name := range []string{"tasks-goal-a.md", "tasks-goal-b.md", "tasks-goal-c.md", "tasks-goal-d.md"} {
		writeGoalDerivedFixture(t, todoDir, name, "2026-10-15T10:0"+string(rune('0'+i))+":00Z")
	}
	scan(t, manager)
	for _, name := range []string{"tasks-goal-a.md", "tasks-goal-b.md", "tasks-goal-c.md", "tasks-goal-d.md"} {
		assertWorking(t, base, name, true)
	}
	finish()
}

func TestOpsLaneKeepsItsSlotsForWaitingOpsTasks(t *testing.T) {
	manager, finish, todoDir, base := slots(t, 4)
	setOpsLane(manager, opsLaneOn(2))
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-a.md", "2026-10-15T10:00:00Z")
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-b.md", "2026-10-15T10:01:00Z")
	scan(t, manager)
	assertWorking(t, base, "tasks-goal-a.md", true)
	assertWorking(t, base, "tasks-goal-b.md", true)

	// The goal keeps filing; two ops tasks arrive behind it. Goal order says goal first; the lane
	// says the last two slots belong to the tasks that are waiting.
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-c.md", "2026-10-15T11:00:00Z")
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-d.md", "2026-10-15T11:01:00Z")
	writeClaimFixture(t, todoDir, "tasks-ops-a.md", "2026-10-15T12:00:00Z")
	writeClaimFixture(t, todoDir, "tasks-ops-b.md", "2026-10-15T12:01:00Z")
	scan(t, manager)
	assertWorking(t, base, "tasks-ops-a.md", true)
	assertWorking(t, base, "tasks-ops-b.md", true)
	assertWorking(t, base, "tasks-goal-c.md", false)
	assertWorking(t, base, "tasks-goal-d.md", false)
	finish()
}

// The lane takes only what it is short of: one ops task already running means one slot is kept,
// not two, and a goal task may still use the other.
func TestOpsLaneKeepsOnlyTheSlotsItIsShortOf(t *testing.T) {
	manager, finish, todoDir, base := slots(t, 4)
	setOpsLane(manager, opsLaneOn(2))
	writeClaimFixture(t, todoDir, "tasks-ops-a.md", "2026-10-15T09:00:00Z")
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-a.md", "2026-10-15T10:00:00Z")
	scan(t, manager)
	assertWorking(t, base, "tasks-ops-a.md", true)
	assertWorking(t, base, "tasks-goal-a.md", true)

	writeGoalDerivedFixture(t, todoDir, "tasks-goal-b.md", "2026-10-15T11:00:00Z")
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-c.md", "2026-10-15T11:01:00Z")
	writeClaimFixture(t, todoDir, "tasks-ops-b.md", "2026-10-15T12:00:00Z")
	scan(t, manager)
	assertWorking(t, base, "tasks-goal-b.md", true)
	assertWorking(t, base, "tasks-ops-b.md", true)
	assertWorking(t, base, "tasks-goal-c.md", false)
	finish()
}

func TestOpsLaneDisabledLeavesGoalWorkFirst(t *testing.T) {
	manager, finish, todoDir, base := slots(t, 2)
	setOpsLane(manager, config.OpsLane{})
	writeClaimFixture(t, todoDir, "tasks-ops-a.md", "2026-10-15T09:00:00Z")
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-a.md", "2026-10-15T10:00:00Z")
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-b.md", "2026-10-15T10:01:00Z")
	scan(t, manager)
	assertWorking(t, base, "tasks-goal-a.md", true)
	assertWorking(t, base, "tasks-goal-b.md", true)
	assertWorking(t, base, "tasks-ops-a.md", false)
	finish()
}

// An ops task that cannot be claimed yet holds nothing back: reserving slots for it would leave
// them idle.
func TestOpsLaneIgnoresAnOpsTaskThatIsNotDue(t *testing.T) {
	manager, finish, todoDir, base := slots(t, 3)
	setOpsLane(manager, opsLaneOn(2))
	writeOpsFixture(t, todoDir, "tasks-ops-later.md", "2026-10-15T09:00:00Z", "2099-01-01T00:00:00Z")
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-a.md", "2026-10-15T10:00:00Z")
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-b.md", "2026-10-15T10:01:00Z")
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-c.md", "2026-10-15T10:02:00Z")
	scan(t, manager)
	assertWorking(t, base, "tasks-goal-a.md", true)
	assertWorking(t, base, "tasks-goal-b.md", true)
	assertWorking(t, base, "tasks-goal-c.md", true)
	assertWorking(t, base, "tasks-ops-later.md", false)
	finish()
}

// A tiny max_parallel never lets the lane take the last slot from goal work.
func TestOpsLaneNeverTakesTheOnlySlot(t *testing.T) {
	manager, finish, todoDir, base := slots(t, 1)
	setOpsLane(manager, opsLaneOn(2))
	writeClaimFixture(t, todoDir, "tasks-ops-a.md", "2026-10-15T09:00:00Z")
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-a.md", "2026-10-15T10:00:00Z")
	scan(t, manager)
	assertWorking(t, base, "tasks-goal-a.md", true)
	assertWorking(t, base, "tasks-ops-a.md", false)
	finish()
}

// A goal planning round is goal work too: it waits for the lane's slots.
func TestOpsLaneHoldsAGoalPlanningRoundForWaitingOpsTasks(t *testing.T) {
	manager, finish, todoDir, base := slots(t, 2)
	setOpsLane(manager, opsLaneOn(1))
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-a.md", "2026-10-15T10:00:00Z")
	scan(t, manager)
	assertWorking(t, base, "tasks-goal-a.md", true)

	writeClaimFixture(t, todoDir, "tasks-ops-a.md", "2026-10-15T12:00:00Z")
	if _, err := Create(manager.Config, "goals", "a goal that needs a planning round", ""); err != nil {
		t.Fatal(err)
	}
	scan(t, manager)
	assertWorking(t, base, "tasks-ops-a.md", true)
	leases, err := manager.loadLeases()
	if err != nil {
		t.Fatal(err)
	}
	for _, lease := range leases {
		if lease.Route == "goals" {
			t.Fatalf("the goal was claimed (%s) although the last slot belongs to the waiting ops task", lease.File)
		}
	}
	finish()
}

// Goal work waiting for a slot pauses a running standalone task (Joey, 2026-10-10). An ops task
// inside the lane's reserve is not that task.
func TestOpsLaneWithinItsReserveIsNotPausedForGoalWork(t *testing.T) {
	manager, finish, todoDir, base := slots(t, 2)
	setOpsLane(manager, opsLaneOn(1))
	probe := newPreemptProbe(manager)
	writeClaimFixture(t, todoDir, "tasks-ops-a.md", "2026-10-15T09:00:00Z")
	scan(t, manager)
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-a.md", "2026-10-15T10:00:00Z")
	scan(t, manager)
	assertWorking(t, base, "tasks-ops-a.md", true)
	assertWorking(t, base, "tasks-goal-a.md", true)

	writeGoalDerivedFixture(t, todoDir, "tasks-goal-late.md", "2026-10-15T11:00:00Z")
	scan(t, manager)
	probe.expectNoCall(t, "the only ops task is inside the lane's reserve")
	assertWorking(t, base, "tasks-goal-late.md", false)
	finish()
}

// Above its reserve the lane is ordinary standalone work again and can be paused.
func TestOpsLaneAboveItsReserveIsStillPausedForGoalWork(t *testing.T) {
	manager, finish, todoDir, _ := slots(t, 3)
	setOpsLane(manager, opsLaneOn(1))
	probe := newPreemptProbe(manager)
	writeClaimFixture(t, todoDir, "tasks-ops-a.md", "2026-10-15T09:00:00Z")
	writeClaimFixture(t, todoDir, "tasks-ops-b.md", "2026-10-15T09:01:00Z")
	scan(t, manager)
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-a.md", "2026-10-15T10:00:00Z")
	scan(t, manager)
	sessionA := leaseSessionFor(t, manager, "tasks-ops-a.md")
	sessionB := leaseSessionFor(t, manager, "tasks-ops-b.md")

	writeGoalDerivedFixture(t, todoDir, "tasks-goal-late.md", "2026-10-15T11:00:00Z")
	scan(t, manager)
	if got := probe.expectCall(t); got != sessionA && got != sessionB {
		t.Fatalf("paused %q, want one of the two ops tasks (%q, %q)", got, sessionA, sessionB)
	}
	finish()
}

func TestOpsLaneRecordsTheClassOnTheLease(t *testing.T) {
	manager, finish, todoDir, _ := slots(t, 3)
	setOpsLane(manager, opsLaneOn(1))
	writeClaimFixture(t, todoDir, "tasks-ops-a.md", "2026-10-15T09:00:00Z")
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-a.md", "2026-10-15T10:00:00Z")
	scan(t, manager)
	leases, err := manager.loadLeases()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, lease := range leases {
		got[filepath.Base(lease.File)] = lease.SlotClass
	}
	if got["tasks-ops-a.md"] != slotClassOps || got["tasks-goal-a.md"] != slotClassGoal {
		t.Fatalf("lease classes = %v, want ops for the standalone task and goal for the goal task", got)
	}
	finish()
}

// A same-named copy refiled into todo/ while the original runs is skipped by the claim loop, so it
// is not "waiting": holding goal work for it would leave a slot idle for as long as the original runs.
func TestOpsLaneIgnoresACopyOfARunningOpsTask(t *testing.T) {
	manager, finish, todoDir, base := slots(t, 4)
	setOpsLane(manager, opsLaneOn(2))
	writeClaimFixture(t, todoDir, "tasks-ops-a.md", "2026-10-15T09:00:00Z")
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-a.md", "2026-10-15T10:00:00Z")
	scan(t, manager)
	assertWorking(t, base, "tasks-ops-a.md", true)
	assertWorking(t, base, "tasks-goal-a.md", true)

	writeClaimFixture(t, todoDir, "tasks-ops-a.md", "2026-10-15T09:00:00Z")
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-b.md", "2026-10-15T11:00:00Z")
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-c.md", "2026-10-15T11:01:00Z")
	scan(t, manager)
	assertWorking(t, base, "tasks-goal-b.md", true)
	assertWorking(t, base, "tasks-goal-c.md", true)
	finish()
}

// An ops task waiting for its review counts as waiting too.
func TestOpsLaneCountsAnOpsTaskWaitingForReview(t *testing.T) {
	manager, finish, todoDir, base := slots(t, 2)
	setOpsLane(manager, opsLaneOn(1))
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-a.md", "2026-10-15T10:00:00Z")
	scan(t, manager)
	assertWorking(t, base, "tasks-goal-a.md", true)

	reviewDir := filepath.Join(base, "review")
	if err := os.MkdirAll(reviewDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeClaimFixture(t, reviewDir, "tasks-ops-review.md", "2026-10-15T09:00:00Z")
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-b.md", "2026-10-15T11:00:00Z")
	manager.refreshOpsWaiting(manager.runtimeSnapshot())
	if got := manager.opsWaitingCount.Load(); got != 1 {
		t.Fatalf("waiting ops count = %d, want 1: an ops task in review/ was not counted as waiting", got)
	}
	scan(t, manager)
	assertWorking(t, base, "tasks-goal-b.md", false)
	finish()
}

// A lease whose document can no longer be read is of unknown class: it is not counted as ops.
func TestOpsLaneUnreadableLeaseIsNotOps(t *testing.T) {
	manager, finish, _, _ := slots(t, 2)
	lease := Lease{ID: "old", Route: "tasks", File: filepath.Join(t.TempDir(), "gone.md")}
	if got := manager.leaseSlotClass(lease); got != "" {
		t.Fatalf("class of an unreadable lease = %q, want unknown", got)
	}
	finish()
}

// A goal review that ends in planning while ops tasks are waiting and the lane is short starts no
// session and spends no attempt; once the ops task is gone it starts as a fresh round.
func TestOpsLaneDefersGoalPlanningStartForWaitingOps(t *testing.T) {
	fx := newGoalHoldFixture(t)
	fx.manager.capacityLimit = 2
	setOpsLane(fx.manager, opsLaneOn(1))
	todoDir := fx.cfg.Resolve(workflowRoutes()[0].Source)
	opsPath := writeClaimFixture(t, todoDir, "tasks-ops-a.md", "2026-10-15T09:00:00Z")
	fx.manager.setInflight("a-running-goal-task", true)
	fx.manager.refreshOpsWaiting(fx.manager.runtimeSnapshot())
	name := fx.finishedGoalReview(t)

	for i := 0; i < 2; i++ {
		if err := fx.manager.reconcileTransitions(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := fx.manager.recoverOrphanClaims(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	fx.manager.Wait()
	if got := fx.sessions(); got != 0 {
		t.Fatalf("a planning session started while ops work waits for the lane: %d", got)
	}
	if got := fx.planningAttempt(t, "planning", name); got != 1 {
		t.Fatalf("planning_attempt changed under the hold: %d", got)
	}
	document, _ := ReadDocument(filepath.Join(fx.goalsBase, "planning", name))
	if marked, _ := document.FrontMatter["planning_hold_deferred"].(bool); !marked {
		t.Error("the deferral was not marked")
	}

	if err := os.Remove(opsPath); err != nil {
		t.Fatal(err)
	}
	fx.manager.refreshOpsWaiting(fx.manager.runtimeSnapshot())
	if err := fx.manager.recoverOrphanClaims(context.Background()); err != nil {
		t.Fatal(err)
	}
	fx.manager.Wait()
	if got := fx.sessions(); got != 1 {
		t.Fatalf("planning did not start after the ops task left: %d sessions", got)
	}
	if got := fx.planningAttempt(t, "planning", name); got != 2 {
		t.Fatalf("planning_attempt = %d after the deferred start, want 2", got)
	}
}

// A goal due to resume planning from waiting/ stays put while the ops lane is short.
func TestOpsLaneKeepsAWokenGoalWaitingForWaitingOps(t *testing.T) {
	fx := newGoalHoldFixture(t)
	fx.manager.capacityLimit = 2
	setOpsLane(fx.manager, opsLaneOn(1))
	todoDir := fx.cfg.Resolve(workflowRoutes()[0].Source)
	opsPath := writeClaimFixture(t, todoDir, "tasks-ops-a.md", "2026-10-15T09:00:00Z")
	fx.manager.setInflight("a-running-goal-task", true)
	fx.manager.refreshOpsWaiting(fx.manager.runtimeSnapshot())
	path, err := Create(fx.cfg, "goals", "woken goal", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(path)
	editFrontMatter(t, path, func(fm map[string]any) {
		fm["planning_attempt"] = 1
		fm["resume_status"] = "planning"
		fm["wake_at"] = time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	})
	waiting := filepath.Join(fx.goalsBase, "waiting", name)
	if err := moveDocument(path, waiting, "waiting", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(waiting)

	if err := fx.manager.wakeWaitingDocuments(context.Background()); err != nil {
		t.Fatal(err)
	}
	fx.manager.Wait()
	after, err := os.ReadFile(waiting)
	if err != nil {
		t.Fatalf("the held goal left waiting/: %v", err)
	}
	if string(before) != string(after) || fx.sessions() != 0 {
		t.Fatal("a woken goal started or was rewritten while the ops lane is short")
	}

	if err := os.Remove(opsPath); err != nil {
		t.Fatal(err)
	}
	fx.manager.refreshOpsWaiting(fx.manager.runtimeSnapshot())
	if err := fx.manager.wakeWaitingDocuments(context.Background()); err != nil {
		t.Fatal(err)
	}
	fx.manager.Wait()
	if got := fx.sessions(); got != 1 {
		t.Fatalf("planning did not start once the ops task left: %d sessions", got)
	}
}

func assertOpsWaiting(t *testing.T, manager *Manager, want int64) {
	t.Helper()
	manager.refreshOpsWaiting(manager.runtimeSnapshot())
	if got := manager.opsWaitingCount.Load(); got != want {
		t.Fatalf("waiting ops count = %d, want %d", got, want)
	}
}

// The reserve ends the moment the last waiting ops task is claimed: with one ops task waiting the
// lane is short one slot, not two, so a goal task that sorts ahead of it takes the other slot in
// the same scan instead of idling it for one interval.
func TestOpsLaneHoldsOnlyAsManySlotsAsOpsTasksWait(t *testing.T) {
	manager, finish, todoDir, base := slots(t, 3)
	setOpsLane(manager, opsLaneOn(2))
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-a.md", "2026-10-15T10:00:00Z")
	scan(t, manager)
	assertWorking(t, base, "tasks-goal-a.md", true)

	writeClaimFixture(t, todoDir, "tasks-ops-a.md", "2026-10-15T12:00:00Z")
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-b.md", "2026-10-15T11:00:00Z")
	scan(t, manager)
	assertWorking(t, base, "tasks-ops-a.md", true)
	assertWorking(t, base, "tasks-goal-b.md", true)
	finish()
}

// A waiting ops task claimed earlier in the scan (its review, which scans before implementation)
// lowers the count, so goal work later in the same scan is not held for a task that is running.
func TestOpsLaneClaimLowersTheWaitingCountWithinTheScan(t *testing.T) {
	manager, finish, todoDir, base := slots(t, 3)
	setOpsLane(manager, opsLaneOn(2))
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-a.md", "2026-10-15T10:00:00Z")
	scan(t, manager)
	assertWorking(t, base, "tasks-goal-a.md", true)

	reviewDir := filepath.Join(base, "review")
	if err := os.MkdirAll(reviewDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeClaimFixture(t, reviewDir, "tasks-ops-review.md", "2026-10-15T09:00:00Z")
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-b.md", "2026-10-15T11:00:00Z")
	scan(t, manager)
	if _, err := os.Stat(filepath.Join(base, "reviewing", "tasks-ops-review.md")); err != nil {
		t.Fatalf("the ops review was not claimed: %v", err)
	}
	assertWorking(t, base, "tasks-goal-b.md", true)
	assertOpsWaiting(t, manager, 0)
	finish()
}

// With the work window open the lane still counts and releases the same way.
func TestOpsLaneWorksInsideAnOpenWorkWindow(t *testing.T) {
	manager, finish, todoDir, base := slots(t, 4)
	setWorkWindow(manager, workWindowFixture(), inWindow(t))
	setOpsLane(manager, opsLaneOn(2))
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-a.md", "2026-10-15T10:00:00Z")
	scan(t, manager)
	assertWorking(t, base, "tasks-goal-a.md", true)

	writeGoalDerivedFixture(t, todoDir, "tasks-goal-b.md", "2026-10-15T11:00:00Z")
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-c.md", "2026-10-15T11:01:00Z")
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-d.md", "2026-10-15T11:02:00Z")
	writeClaimFixture(t, todoDir, "tasks-ops-a.md", "2026-10-15T12:00:00Z")
	assertOpsWaiting(t, manager, 1)
	scan(t, manager)
	assertWorking(t, base, "tasks-ops-a.md", true)
	assertWorking(t, base, "tasks-goal-b.md", true)
	assertWorking(t, base, "tasks-goal-c.md", false)
	assertWorking(t, base, "tasks-goal-d.md", false)
	assertOpsWaiting(t, manager, 0)
	finish()
}

// A waiting ops task whose seat is on a pace hold cannot be claimed, so it is not waiting and goal
// work is not held for it. The held ops task is a review (its own seat); the goal tasks implement.
func TestOpsLaneIgnoresARosterHeldOpsTask(t *testing.T) {
	fx := newGoalHoldFixture(t)
	fx.manager.capacityLimit = 3
	setOpsLane(fx.manager, opsLaneOn(2))
	roster := `
staff:
  implementer: {runner: opencode}
  reviewer: {runner: pi}
roles:
  task_implementation: implementer
  task_review: reviewer
`
	if err := os.WriteFile(rosterpkg.Path(fx.cfg.StatePath()), []byte(roster), 0o600); err != nil {
		t.Fatal(err)
	}
	todoDir := fx.cfg.Resolve(workflowRoutes()[0].Source)
	base := filepath.Dir(todoDir)
	reviewDir := filepath.Join(base, "review")
	if err := os.MkdirAll(reviewDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeClaimFixture(t, reviewDir, "tasks-ops-review.md", "2026-10-15T09:00:00Z")
	assertOpsWaiting(t, fx.manager, 1)

	fx.writeHolds("reviewer")
	assertOpsWaiting(t, fx.manager, 0)
	for i, name := range []string{"tasks-goal-a.md", "tasks-goal-b.md", "tasks-goal-c.md"} {
		writeGoalDerivedFixture(t, todoDir, name, "2026-10-15T10:0"+string(rune('0'+i))+":00Z")
	}
	if err := fx.manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"tasks-goal-a.md", "tasks-goal-b.md", "tasks-goal-c.md"} {
		assertWorking(t, base, name, true)
	}
	if _, err := os.Stat(filepath.Join(reviewDir, "tasks-ops-review.md")); err != nil {
		t.Fatalf("the held review left review/: %v", err)
	}
}

// A lane: repair task has its own reserved capacity: it is not ops work waiting for a slot, so it
// neither holds goal work back nor shrinks the slots goal tasks may fill.
func TestOpsLaneIgnoresARepairTask(t *testing.T) {
	manager, finish, todoDir, base := slots(t, 3)
	setOpsLane(manager, opsLaneOn(2))
	path := writeClaimFixture(t, todoDir, "tasks-repair-a.md", "2026-10-15T09:00:00Z")
	document, err := ReadDocument(path)
	if err != nil {
		t.Fatal(err)
	}
	document.FrontMatter["lane"] = "repair"
	if err := WriteDocument(path, document); err != nil {
		t.Fatal(err)
	}
	for i, name := range []string{"tasks-goal-a.md", "tasks-goal-b.md", "tasks-goal-c.md"} {
		writeGoalDerivedFixture(t, todoDir, name, "2026-10-15T10:0"+string(rune('0'+i))+":00Z")
	}
	assertOpsWaiting(t, manager, 0)
	scan(t, manager)
	for _, name := range []string{"tasks-goal-a.md", "tasks-goal-b.md", "tasks-goal-c.md"} {
		assertWorking(t, base, name, true)
	}
	finish()
}
