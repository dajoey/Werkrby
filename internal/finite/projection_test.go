package finite

import (
	"context"
	"errors"
	"os"
	"strings"
	"syscall"
	"testing"
)

func TestFiniteProjectionRecovery_R5(t *testing.T) {
	s := newTestStore(t)
	task := intakeTestTask(t, s, "task-projection", false)
	run := dispatchTestRun(t, s, task, PhaseImplementation)
	settled, err := s.SettleRun(context.Background(), SettlementRequest{
		RunID: run.ID, ExpectedTaskVersion: task.Version + 1, ExecutionOutcome: RunOK,
		Result: validTestResult(run, ResultDone),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ProjectTask(context.Background(), task.ID); err != nil {
		t.Fatalf("initial projection: %v", err)
	}
	before, readErr := os.ReadFile(task.ProjectionPath)
	if readErr != nil {
		t.Errorf("projection was not generated: %v", readErr)
		return
	}
	if !strings.Contains(string(before), "state: done") || !strings.Contains(string(before), "version: 2") {
		t.Errorf("projection does not reflect SQLite: %s", before)
	}

	originalWriter := atomicWriteFile
	atomicWriteFile = func(path string, data []byte, mode os.FileMode) error { return syscall.ENOSPC }
	err = s.ProjectTask(context.Background(), task.ID)
	atomicWriteFile = originalWriter
	if !errors.Is(err, syscall.ENOSPC) {
		t.Errorf("projection fault error = %v", err)
		return
	}
	afterFault, err := os.ReadFile(task.ProjectionPath)
	if err != nil || string(afterFault) != string(before) {
		t.Errorf("fault left torn projection: %v", err)
	}
	if err := os.WriteFile(task.ProjectionPath, []byte("torn"), 0o600); err != nil {
		t.Fatal(err)
	}
	runsBefore := countRows(t, s, `SELECT COUNT(*) FROM runs`)
	eventsBefore := countRows(t, s, `SELECT COUNT(*) FROM task_events`)
	outboxBefore := countRows(t, s, `SELECT COUNT(*) FROM outbox`)
	if err := s.ProjectTask(context.Background(), task.ID); err != nil {
		t.Fatalf("regenerate projection: %v", err)
	}
	regenerated, err := os.ReadFile(task.ProjectionPath)
	if err != nil || string(regenerated) != string(before) {
		t.Errorf("regeneration was not exact: %v", err)
	}
	if countRows(t, s, `SELECT COUNT(*) FROM runs`) != runsBefore || countRows(t, s, `SELECT COUNT(*) FROM task_events`) != eventsBefore || countRows(t, s, `SELECT COUNT(*) FROM outbox`) != outboxBefore {
		t.Errorf("projection changed workflow records")
	}
	current, _ := s.GetTask(context.Background(), task.ID)
	if current.State != settled.State || current.Version != settled.Version {
		t.Errorf("projection rolled back settlement: %+v", current)
	}
	if err := s.CheckInvariants(context.Background()); err != nil {
		t.Errorf("invariants after projection recovery: %v", err)
	}
}
