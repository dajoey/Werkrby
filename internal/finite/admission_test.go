package finite

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestFiniteAdmissionWaitCancelAndVersionFences(t *testing.T) {
	reset := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	task := Task{ID: "task-admit", State: StateTodo, MaxRuns: 12, MaxFailures: 3, MaxReviewCycles: 3}
	decision := Admit(AdmissionInput{Task: task, Phase: PhaseImplementation, Seats: []Seat{{Name: "seat-b", Available: true}, {Name: "seat-a", Available: true}}})
	if decision.Action != AdmissionDispatch || decision.Seat != "seat-a" {
		t.Errorf("admission = %+v", decision)
	}
	cancelled := task
	cancelled.CancelRequested = true
	if got := Admit(AdmissionInput{Task: cancelled, Phase: PhaseImplementation, Seats: []Seat{{Name: "seat-a", Available: true}}}); got.Action != AdmissionWait {
		t.Errorf("stop-pending admission = %+v", got)
	}
	if got := Admit(AdmissionInput{Task: task, Phase: PhaseImplementation, Accounting: Accounting{Attempts: 12}, Seats: []Seat{{Name: "seat-a", Available: true}}}); got.Action != AdmissionFail {
		t.Errorf("cap admission = %+v", got)
	}
	waiting := Admit(AdmissionInput{Task: task, Phase: PhaseImplementation, Seats: []Seat{{Name: "seat-b", QuotaReset: &reset}, {Name: "seat-a"}}})
	if waiting.Action != AdmissionWait || waiting.WakeAt == nil || !waiting.WakeAt.Equal(reset) {
		t.Errorf("quota admission = %+v", waiting)
	}

	t.Run("time_wait_wake_is_atomic_and_joey_ignores_clock", func(t *testing.T) {
		s := newTestStore(t)
		task := intakeTestTask(t, s, "task-wait", false)
		run := dispatchTestRun(t, s, task, PhaseImplementation)
		result := validTestResult(run, ResultWait)
		wake := s.now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
		result.Wait = &Wait{Kind: WaitTime, WakeAt: wake, Reason: "Deployment window"}
		waitingTask, err := s.SettleRun(context.Background(), SettlementRequest{RunID: run.ID, ExpectedTaskVersion: task.Version + 1, ExecutionOutcome: RunOK, Result: result})
		if err != nil {
			t.Fatal(err)
		}
		if waitingTask.State != StateWaiting || waitingTask.ResumeState != StateTodo || waitingTask.WakeAt == nil {
			t.Errorf("wait settlement = %+v", waitingTask)
			return
		}
		if _, err := s.WakeTime(context.Background(), task.ID, waitingTask.Version); !errors.Is(err, ErrStateConflict) {
			t.Errorf("early wake error = %v", err)
		}
		s.now = func() time.Time { return time.Date(2026, 10, 31, 13, 0, 0, 0, time.UTC) }
		awake, err := s.WakeTime(context.Background(), task.ID, waitingTask.Version)
		if err != nil || awake.State != StateTodo || awake.WaitKind != "" || awake.WakeAt != nil {
			t.Errorf("wake = %+v, %v", awake, err)
			return
		}
		if _, err := s.WakeTime(context.Background(), task.ID, waitingTask.Version); !errors.Is(err, ErrVersionConflict) {
			t.Errorf("duplicate wake error = %v", err)
		}

		joeyTask := intakeTestTask(t, s, "task-joey", false)
		joeyRun := dispatchTestRun(t, s, joeyTask, PhaseImplementation)
		joeyResult := validTestResult(joeyRun, ResultWait)
		joeyResult.Wait = &Wait{Kind: WaitJoey, Reason: "Need a decision", Ask: json.RawMessage(`{"question":"Continue?","options":[{"id":"yes","label":"Yes"},{"id":"no","label":"No"}]}`)}
		joeyWaiting, err := s.SettleRun(context.Background(), SettlementRequest{RunID: joeyRun.ID, ExpectedTaskVersion: joeyTask.Version + 1, ExecutionOutcome: RunOK, Result: joeyResult})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.WakeTime(context.Background(), joeyTask.ID, joeyWaiting.Version); !errors.Is(err, ErrStateConflict) {
			t.Errorf("clock woke Joey wait: %v", err)
		}
		if _, err := s.AnswerJoey(context.Background(), joeyTask.ID, joeyWaiting.Version, "invalid"); err == nil {
			t.Errorf("unknown Joey answer was accepted")
		}
		answered, err := s.AnswerJoey(context.Background(), joeyTask.ID, joeyWaiting.Version, "yes")
		if err != nil || answered.State != StateTodo || answered.JoeyAskJSON != "" {
			t.Errorf("Joey answer = %+v, %v", answered, err)
		}
	})

	t.Run("stop_fence_beats_valid_result_and_charges_nothing", func(t *testing.T) {
		s := newTestStore(t)
		task := intakeTestTask(t, s, "task-stop", false)
		run := dispatchTestRun(t, s, task, PhaseImplementation)
		stopped, err := s.RequestStop(context.Background(), task.ID, task.Version+1)
		if err != nil || !stopped.CancelRequested || stopped.State != StateWorking {
			t.Errorf("stop fence = %+v, %v", stopped, err)
			return
		}
		settled, err := s.SettleRun(context.Background(), SettlementRequest{
			RunID: run.ID, ExpectedTaskVersion: stopped.Version, ExecutionOutcome: RunOK,
			Result: validTestResult(run, ResultDone),
		})
		if err != nil || settled.State != StateCancelled {
			t.Errorf("stop settlement = %+v, %v", settled, err)
			return
		}
		persisted, _ := s.GetRun(context.Background(), run.ID)
		accounting, _ := s.DerivedAccounting(context.Background(), task.ID, PhaseImplementation)
		if persisted.Outcome != RunInterrupted || accounting.Attempts != 0 {
			t.Errorf("stop accounting run=%+v accounting=%+v", persisted, accounting)
		}
	})

	t.Run("dispatch_rechecks_stale_version_and_open_run", func(t *testing.T) {
		s := newTestStore(t)
		task := intakeTestTask(t, s, "task-dispatch-fence", false)
		_ = dispatchTestRun(t, s, task, PhaseImplementation)
		called := false
		_, err := s.Dispatch(context.Background(), DispatchRequest{TaskID: task.ID, ExpectedVersion: task.Version, Phase: PhaseImplementation, Seat: "seat", SessionKey: "finite:fence", LaunchReason: "retry"}, func(context.Context, Run) error {
			called = true
			return nil
		})
		if !errors.Is(err, ErrVersionConflict) || called {
			t.Errorf("stale dispatch error=%v launched=%t", err, called)
		}
	})
}
