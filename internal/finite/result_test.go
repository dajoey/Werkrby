package finite

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFiniteResultValidationReplayAndAccounting_X1_X3(t *testing.T) {
	run := Run{ID: 42, TaskID: "task-result", Phase: PhaseImplementation, RunDir: t.TempDir()}
	valid := fmt.Sprintf(`{"schema_version":1,"run_id":42,"task_id":"task-result","phase":"implementation","outcome":"review","progress":"%s","summary":{"outcome":"Done","evidence":"artifacts/evidence.txt","uncertainty":"None"}}`, "verified")
	decoded, err := DecodeResult(run, []byte(valid))
	if err != nil || decoded.RunID != run.ID || decoded.Outcome != ResultReview {
		t.Errorf("valid result rejected: %+v, %v", decoded, err)
	}

	invalid := map[string]string{
		"malformed":         `{`,
		"unknown_top":       `{"schema_version":1,"run_id":42,"task_id":"task-result","phase":"implementation","outcome":"todo","progress":"x","front_matter":{"state":"done"}}`,
		"unknown_nested":    `{"schema_version":1,"run_id":42,"task_id":"task-result","phase":"implementation","outcome":"todo","progress":"x","summary":{"outcome":"x","evidence":"y","uncertainty":"z","status":"done"}}`,
		"wrong_run":         `{"schema_version":1,"run_id":41,"task_id":"task-result","phase":"implementation","outcome":"todo","progress":"x"}`,
		"wrong_task":        `{"schema_version":1,"run_id":42,"task_id":"other","phase":"implementation","outcome":"todo","progress":"x"}`,
		"wrong_phase":       `{"schema_version":1,"run_id":42,"task_id":"task-result","phase":"review","outcome":"accepted","progress":"x"}`,
		"phase_outcome":     `{"schema_version":1,"run_id":42,"task_id":"task-result","phase":"implementation","outcome":"accepted","progress":"x"}`,
		"schema_version":    `{"schema_version":2,"run_id":42,"task_id":"task-result","phase":"implementation","outcome":"todo","progress":"x"}`,
		"missing_wait":      `{"schema_version":1,"run_id":42,"task_id":"task-result","phase":"implementation","outcome":"wait","progress":"x"}`,
		"unknown_ask":       `{"schema_version":1,"run_id":42,"task_id":"task-result","phase":"implementation","outcome":"wait","progress":"x","wait":{"kind":"joey","reason":"Need answer","ask":{"question":"Continue?","options":[{"id":"yes","label":"Yes"}],"answer":"yes"}}}`,
		"trailing_json":     valid + `{}`,
		"oversize_progress": fmt.Sprintf(`{"schema_version":1,"run_id":42,"task_id":"task-result","phase":"implementation","outcome":"todo","progress":%q}`, strings.Repeat("x", MaxProgressRunes+1)),
	}
	for name, data := range invalid {
		t.Run("reject_"+name, func(t *testing.T) {
			if _, err := DecodeResult(run, []byte(data)); !errors.Is(err, ErrInvalidResult) {
				t.Errorf("error = %v", err)
			}
		})
	}

	t.Run("missing_oversized_and_non_regular_files", func(t *testing.T) {
		if _, err := ReadResult(run); !errors.Is(err, ErrInvalidResult) {
			t.Errorf("missing result error = %v", err)
		}
		path := filepath.Join(run.RunDir, "result.json")
		if err := os.WriteFile(path, []byte(strings.Repeat(" ", MaxResultBytes+1)), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadResult(run); !errors.Is(err, ErrInvalidResult) {
			t.Errorf("oversized result error = %v", err)
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadResult(run); !errors.Is(err, ErrInvalidResult) {
			t.Errorf("directory result error = %v", err)
		}
	})

	t.Run("invalid_result_charges_without_credit_or_refund", func(t *testing.T) {
		s := newTestStore(t)
		task := intakeTestTask(t, s, "task-invalid", false)
		run := dispatchTestRun(t, s, task, PhaseImplementation)
		settled, err := s.SettleRun(context.Background(), SettlementRequest{
			RunID: run.ID, ExpectedTaskVersion: task.Version + 1, ExecutionOutcome: RunOK, ResultError: ErrInvalidResult,
		})
		if err != nil {
			t.Fatalf("settle invalid result: %v", err)
		}
		if settled.State != StateTodo {
			t.Errorf("invalid result state = %s", settled.State)
			return
		}
		accounting, err := s.DerivedAccounting(context.Background(), task.ID, PhaseImplementation)
		if err != nil || accounting.Attempts != 1 || accounting.Failures != 1 {
			t.Errorf("invalid result accounting = %+v, %v", accounting, err)
		}
		persisted, _ := s.GetRun(context.Background(), run.ID)
		if persisted.Error != "invalid_result" {
			t.Errorf("invalid result run error = %q", persisted.Error)
		}
	})

	t.Run("failure_timeout_and_late_result_use_new_run_boundaries", func(t *testing.T) {
		s := newTestStore(t)
		task := intakeTestTask(t, s, "task-retry", false)
		first := dispatchTestRun(t, s, task, PhaseImplementation)
		firstSettlement := SettlementRequest{RunID: first.ID, ExpectedTaskVersion: task.Version + 1, ExecutionOutcome: RunFail, Error: "provider failed"}
		requeued, err := s.SettleRun(context.Background(), firstSettlement)
		if err != nil {
			t.Fatal(err)
		}
		second := dispatchTestRun(t, s, requeued, PhaseImplementation)
		if second.ID == first.ID {
			t.Fatalf("retry reused run ID %d", first.ID)
			return
		}
		late := validTestResult(first, ResultDone)
		lateRequest := firstSettlement
		lateRequest.Result = late
		if _, err := s.SettleRun(context.Background(), lateRequest); !errors.Is(err, ErrSettlementConflict) {
			t.Errorf("conflicting late result error = %v", err)
		}
		if _, err := s.SettleRun(context.Background(), firstSettlement); err != nil {
			t.Errorf("identical late replay was not harmless: %v", err)
		}
		current, _ := s.GetTask(context.Background(), task.ID)
		open, _ := s.GetRun(context.Background(), second.ID)
		if current.State != StateWorking || open.EndedAt != nil {
			t.Errorf("late result mutated successor: task=%+v run=%+v", current, open)
		}
		timedOut, err := s.SettleRun(context.Background(), SettlementRequest{RunID: second.ID, ExpectedTaskVersion: current.Version, ExecutionOutcome: RunTimeout})
		if err != nil || timedOut.State != StateTodo {
			t.Errorf("timeout settlement = %+v, %v", timedOut, err)
		}
		accounting, _ := s.DerivedAccounting(context.Background(), task.ID, PhaseImplementation)
		if accounting.Attempts != 2 || accounting.Failures != 2 {
			t.Errorf("failure/timeout accounting = %+v", accounting)
		}
	})

	t.Run("cap_failure_is_terminal_and_uncharged_lost_is_not_attempt", func(t *testing.T) {
		s := newTestStore(t)
		task := intakeTestTask(t, s, "task-cap", false)
		if _, err := s.db.Exec(`UPDATE tasks SET max_runs=1,max_failures=1 WHERE id=?`, task.ID); err != nil {
			t.Fatal(err)
		}
		task.MaxRuns, task.MaxFailures = 1, 1
		run := dispatchTestRun(t, s, task, PhaseImplementation)
		settled, err := s.SettleRun(context.Background(), SettlementRequest{RunID: run.ID, ExpectedTaskVersion: task.Version + 1, ExecutionOutcome: RunTimeout})
		if err != nil || settled.State != StateFailed {
			t.Errorf("cap settlement = %+v, %v", settled, err)
			return
		}
		accounting, _ := s.DerivedAccounting(context.Background(), task.ID, PhaseImplementation)
		if accounting.Attempts != 1 || accounting.Failures != 1 {
			t.Errorf("cap accounting = %+v", accounting)
		}
	})
}
