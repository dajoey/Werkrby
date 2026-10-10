package orchestrator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Goal work is the top priority, and that means two things at once (Joey,
// 2026-10-10): it claims first, and when it is queued with every slot full a
// running standalone task is paused so the goal work takes the slot. It does
// not mean standalone work stops while slots are idle. The pause is the
// operator-stop path with a different landing: the standalone task goes back to
// todo/ with its attempt unspent and runs again when a slot is free.

// preemptProbe records which sessions the manager asked to pause.
type preemptProbe struct {
	calls chan string
	err   error
}

func newPreemptProbe(manager *Manager) *preemptProbe {
	probe := &preemptProbe{calls: make(chan string, 8)}
	manager.PreemptJob = func(_ context.Context, sessionKey string) error {
		probe.calls <- sessionKey
		return probe.err
	}
	return probe
}

func (p *preemptProbe) expectCall(t *testing.T) string {
	t.Helper()
	select {
	case key := <-p.calls:
		return key
	case <-time.After(3 * time.Second):
		t.Fatal("the manager never asked to pause a standalone task")
		return ""
	}
}

func (p *preemptProbe) expectNoCall(t *testing.T, why string) {
	t.Helper()
	select {
	case key := <-p.calls:
		t.Fatalf("a task was paused (%s): %s", key, why)
	case <-time.After(300 * time.Millisecond):
	}
}

func leaseSessionFor(t *testing.T, manager *Manager, docName string) string {
	t.Helper()
	leases, err := manager.loadLeases()
	if err != nil {
		t.Fatal(err)
	}
	for _, lease := range leases {
		if filepath.Base(lease.File) == docName {
			return lease.SessionKey
		}
	}
	t.Fatalf("no lease for %s in %#v", docName, leases)
	return ""
}

func oneSlotWithStandaloneRunning(t *testing.T) (*Manager, func(), string, string, string) {
	t.Helper()
	cfg, _, manager, finish := claimOrderTestManager(t)
	taskRoute := workflowRoutes()[0]
	todoDir := cfg.Resolve(taskRoute.Source)
	base := filepath.Dir(todoDir)
	writeClaimFixture(t, todoDir, "tasks-standalone-running.md", "2026-09-01T00:00:00Z")
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(base, "working", "tasks-standalone-running.md")); err != nil {
		t.Fatalf("standalone task was not claimed: %v", err)
	}
	session := leaseSessionFor(t, manager, "tasks-standalone-running.md")
	return manager, finish, todoDir, base, session
}

func TestGoalTaskPausesARunningStandaloneTaskWhenEverySlotIsFull(t *testing.T) {
	manager, finish, todoDir, base, session := oneSlotWithStandaloneRunning(t)
	probe := newPreemptProbe(manager)
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-late.md", "2026-10-10T17:00:00Z")
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := probe.expectCall(t); got != session {
		t.Fatalf("paused session %q, want the standalone task's %q", got, session)
	}
	if _, err := os.Stat(filepath.Join(base, "todo", "tasks-goal-late.md")); err != nil {
		t.Fatalf("the goal task cannot be claimed before the slot frees: %v", err)
	}
	finish()
}

func TestGoalPlanningPausesARunningStandaloneTaskWhenEverySlotIsFull(t *testing.T) {
	manager, finish, _, _, session := oneSlotWithStandaloneRunning(t)
	probe := newPreemptProbe(manager)
	if _, err := Create(manager.Config, "goals", "a goal that needs a slot to be planned", ""); err != nil {
		t.Fatal(err)
	}
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := probe.expectCall(t); got != session {
		t.Fatalf("paused session %q, want the standalone task's %q", got, session)
	}
	finish()
}

// One pause per waiting slot need: until the paused job has actually left, the
// scan that runs every few seconds must not pause a second task for the same
// goal work.
func TestAPauseInProgressIsNotRepeated(t *testing.T) {
	manager, finish, todoDir, _, _ := oneSlotWithStandaloneRunning(t)
	probe := newPreemptProbe(manager)
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-late.md", "2026-10-10T17:00:00Z")
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	probe.expectCall(t)
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	probe.expectNoCall(t, "the first pause had not finished yet")
	finish()
}

// A pause the harness refused must not strand the goal work: the next scan asks
// again.
func TestARefusedPauseIsRetriedOnTheNextScan(t *testing.T) {
	manager, finish, todoDir, _, _ := oneSlotWithStandaloneRunning(t)
	probe := newPreemptProbe(manager)
	probe.err = errors.New("the provider did not accept the interrupt request")
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-late.md", "2026-10-10T17:00:00Z")
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	probe.expectCall(t)
	// The goroutine clears its record once the stop fails; give it a moment.
	time.Sleep(200 * time.Millisecond)
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	probe.expectCall(t)
	finish()
}

func TestNothingIsPausedWhileASlotIsFree(t *testing.T) {
	manager, finish, todoDir, base := twoSlotManager(t)
	writeClaimFixture(t, todoDir, "tasks-standalone-running.md", "2026-09-01T00:00:00Z")
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	probe := newPreemptProbe(manager)
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-late.md", "2026-10-10T17:00:00Z")
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	probe.expectNoCall(t, "the goal task fit in the free slot")
	for _, name := range []string{"tasks-standalone-running.md", "tasks-goal-late.md"} {
		if _, err := os.Stat(filepath.Join(base, "working", name)); err != nil {
			t.Fatalf("%s should be running: %v", name, err)
		}
	}
	finish()
}

func TestGoalWorkIsNeverPausedForOtherGoalWork(t *testing.T) {
	cfg, _, manager, finish := claimOrderTestManager(t)
	todoDir := cfg.Resolve(workflowRoutes()[0].Source)
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-first.md", "2026-10-10T16:00:00Z")
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	probe := newPreemptProbe(manager)
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-second.md", "2026-10-10T17:00:00Z")
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	probe.expectNoCall(t, "both tasks belong to the goal")
	finish()
}

func TestNothingIsPausedForStandaloneWork(t *testing.T) {
	manager, finish, todoDir, _, _ := oneSlotWithStandaloneRunning(t)
	probe := newPreemptProbe(manager)
	writeClaimFixture(t, todoDir, "tasks-standalone-waiting.md", "2026-08-01T00:00:00Z")
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	probe.expectNoCall(t, "standalone work does not outrank standalone work")
	finish()
}

// The paused task lands in todo/ (not waiting/ for twelve hours, which is where
// an operator stop sends it), unclaimed attempt intact, no timer.
func TestAPausedStandaloneTaskReturnsToTodoWithItsAttemptUnspent(t *testing.T) {
	manager, lease, doc, _ := stoppedTaskFixture(t)
	if !manager.markPreemption(lease.ID) {
		t.Fatal("a preemption record could not be made for an idle manager")
	}
	manager.MarkControlCancellation("sess-stopped")

	manager.finishRuntimeJob(lease.ID)
	manager.recordError(cancelledContext(), lease, context.Canceled)

	if fileExists(doc) {
		t.Fatal("the paused task is still in working/: it would be re-adopted as a stalled claim")
	}
	todo := strings.Replace(doc, filepath.Join("tasks", "working"), filepath.Join("tasks", "todo"), 1)
	paused, err := ReadDocument(todo)
	if err != nil {
		t.Fatalf("the paused task was not returned to todo/: %v", err)
	}
	if paused.FrontMatter["status"] != "todo" {
		t.Fatalf("status = %v, want todo", paused.FrontMatter["status"])
	}
	if _, has := paused.FrontMatter["wake_at"]; has {
		t.Fatalf("a paused task must not carry a timer: %#v", paused.FrontMatter)
	}
	if credit, _ := paused.FrontMatter[resumeCreditField].(bool); !credit {
		t.Fatalf("a pause must not spend an attempt: %#v", paused.FrontMatter)
	}
	if !strings.Contains(paused.Body, "goal work") || strings.Contains(paused.Body, "stopped by an operator") {
		t.Fatalf("progress note wrong: %s", paused.Body)
	}
	if leases, _ := manager.loadLeases(); len(leases) != 0 {
		t.Fatalf("lease still exists, so Spynel would adopt the claim again: %#v", leases)
	}
	if manager.preemptionPending() || manager.operatorStopPending(lease.ID) || manager.isControlCancelled(lease.ID) {
		t.Fatal("the pause record was not cleared")
	}
}

// An operator's own Stop keeps its meaning: parked in waiting/ for twelve hours.
func TestAnOperatorStopIsNotTurnedIntoAPause(t *testing.T) {
	manager, lease, doc, waiting := stoppedTaskFixture(t)
	manager.MarkControlCancellation("sess-stopped")
	manager.finishRuntimeJob(lease.ID)
	manager.recordError(cancelledContext(), lease, context.Canceled)
	if fileExists(doc) || !fileExists(waiting) {
		t.Fatal("an operator stop must still park the task in waiting/")
	}
}
