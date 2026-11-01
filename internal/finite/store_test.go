package finite

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := OpenStore(Config{Root: t.TempDir(), WorkerDir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	s.now = func() time.Time { return time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC) }
	return s
}

func writeTestBrief(t *testing.T, id string, reviewRequired bool) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), id+".md")
	data := fmt.Sprintf("---\nid: %s\ntitle: Test task %s\nrisk: low\nreview_required: %t\n---\n\nImplement one finite objective.\n", id, id, reviewRequired)
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatalf("write brief: %v", err)
	}
	return path
}

func intakeTestTask(t *testing.T, s *Store, id string, reviewRequired bool) Task {
	t.Helper()
	task, err := s.IntakeBrief(context.Background(), writeTestBrief(t, id, reviewRequired))
	if err != nil {
		t.Fatalf("intake brief: %v", err)
	}
	return task
}

func dispatchTestRun(t *testing.T, s *Store, task Task, phase Phase) Run {
	t.Helper()
	run, err := s.Dispatch(context.Background(), DispatchRequest{
		TaskID: task.ID, ExpectedVersion: task.Version, Phase: phase,
		Seat: "seat-a", SessionKey: "finite:test", LaunchReason: "initial", PromptHash: "abc",
	}, func(context.Context, Run) error { return nil })
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	return run
}

func validTestResult(run Run, outcome ResultOutcome) *Result {
	return &Result{
		SchemaVersion: 1, RunID: run.ID, TaskID: run.TaskID, Phase: run.Phase,
		Outcome: outcome, Progress: "Completed and verified the finite objective.",
		Summary: &Summary{Outcome: "Complete", Evidence: "unit evidence", Uncertainty: "none material"},
	}
}

func countRows(t *testing.T, s *Store, query string, args ...any) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return n
}

func TestFiniteDispatchSettlementAtomicity(t *testing.T) {
	t.Run("dispatch_commits_before_launch_and_settles_once", func(t *testing.T) {
		s := newTestStore(t)
		task := intakeTestTask(t, s, "task-atomic", false)
		launched := false
		run, err := s.Dispatch(context.Background(), DispatchRequest{
			TaskID: task.ID, ExpectedVersion: task.Version, Phase: PhaseImplementation,
			Seat: "seat-a", SessionKey: "finite:atomic", LaunchReason: "initial", PromptHash: "hash",
		}, func(ctx context.Context, launchedRun Run) error {
			launched = true
			current, err := s.GetTask(ctx, task.ID)
			if err != nil {
				return err
			}
			if current.State != StateWorking || current.Version != task.Version+1 {
				return fmt.Errorf("launch observed uncommitted task: %+v", current)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("dispatch: %v", err)
		}
		if !launched {
			t.Errorf("launcher was not called after dispatch")
			return
		}
		if run.ID == 0 || run.TaskVersion != task.Version {
			t.Fatalf("invalid dispatched run: %+v", run)
		}
		result := validTestResult(run, ResultDone)
		exit := 1
		req := SettlementRequest{RunID: run.ID, ExpectedTaskVersion: task.Version + 1, ExecutionOutcome: RunFail, ExitCode: &exit, Result: result}
		settled, err := s.SettleRun(context.Background(), req)
		if err != nil {
			t.Fatalf("settle: %v", err)
		}
		if settled.State != StateDone || settled.Version != task.Version+2 {
			t.Fatalf("valid result did not win and settle: %+v", settled)
		}
		persistedRun, err := s.GetRun(context.Background(), run.ID)
		if err != nil || persistedRun.Outcome != RunOK {
			t.Fatalf("run outcome = %+v, %v", persistedRun, err)
		}
		if got := countRows(t, s, `SELECT COUNT(*) FROM run_results WHERE run_id=?`, run.ID); got != 1 {
			t.Errorf("result rows = %d", got)
		}
		if got := countRows(t, s, `SELECT COUNT(*) FROM task_events WHERE run_id=? AND kind='settled'`, run.ID); got != 1 {
			t.Errorf("settled events = %d", got)
		}
		if got := countRows(t, s, `SELECT COUNT(*) FROM outbox`); got != 1 {
			t.Errorf("outbox rows = %d", got)
		}
		beforeEvents := countRows(t, s, `SELECT COUNT(*) FROM task_events`)
		if _, err := s.SettleRun(context.Background(), req); err != nil {
			t.Fatalf("identical replay: %v", err)
		}
		if got := countRows(t, s, `SELECT COUNT(*) FROM task_events`); got != beforeEvents {
			t.Errorf("replay inserted events: %d -> %d", beforeEvents, got)
		}
		conflict := req
		conflict.Result = nil
		conflict.ExecutionOutcome = RunTimeout
		if _, err := s.SettleRun(context.Background(), conflict); !errors.Is(err, ErrSettlementConflict) {
			t.Errorf("conflicting replay error = %v", err)
		}
	})

	t.Run("version_fence_rolls_back_entire_settlement", func(t *testing.T) {
		s := newTestStore(t)
		task := intakeTestTask(t, s, "task-fence", false)
		run := dispatchTestRun(t, s, task, PhaseImplementation)
		_, err := s.SettleRun(context.Background(), SettlementRequest{
			RunID: run.ID, ExpectedTaskVersion: task.Version + 99,
			ExecutionOutcome: RunOK, Result: validTestResult(run, ResultDone),
		})
		if !errors.Is(err, ErrVersionConflict) {
			t.Errorf("settlement version error = %v", err)
			return
		}
		current, _ := s.GetTask(context.Background(), task.ID)
		open, _ := s.GetRun(context.Background(), run.ID)
		if current.State != StateWorking || current.Version != task.Version+1 || open.EndedAt != nil {
			t.Errorf("partial settlement after fence: task=%+v run=%+v", current, open)
		}
		if got := countRows(t, s, `SELECT COUNT(*) FROM run_results`); got != 0 {
			t.Errorf("orphan result rows = %d", got)
		}
	})

	t.Run("launcher_failure_is_after_durable_open_run", func(t *testing.T) {
		s := newTestStore(t)
		task := intakeTestTask(t, s, "task-launch-failure", false)
		launchErr := errors.New("failpoint: start")
		run, err := s.Dispatch(context.Background(), DispatchRequest{
			TaskID: task.ID, ExpectedVersion: task.Version, Phase: PhaseImplementation,
			Seat: "seat-a", SessionKey: "finite:failure", LaunchReason: "initial",
		}, func(context.Context, Run) error { return launchErr })
		if !errors.Is(err, launchErr) {
			t.Errorf("launch error = %v", err)
			return
		}
		persisted, getErr := s.GetRun(context.Background(), run.ID)
		if getErr != nil || persisted.EndedAt != nil {
			t.Errorf("open run did not survive launch failure: %+v, %v", persisted, getErr)
		}
	})
}
