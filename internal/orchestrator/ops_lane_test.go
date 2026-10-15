package orchestrator

import (
	"path/filepath"
	"testing"

	"github.com/agent0ai/spynel/internal/config"
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
