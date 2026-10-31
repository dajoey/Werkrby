package finite

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// SettlementFailpoint identifies one statement boundary in SettleRun.
type SettlementFailpoint string

const (
	SettlementAfterRunResultsInsert      SettlementFailpoint = "after_run_results_insert"
	SettlementAfterRunUpdate             SettlementFailpoint = "after_run_update"
	SettlementAfterUsageInsert           SettlementFailpoint = "after_usage_insert"
	SettlementAfterTaskUpdate            SettlementFailpoint = "after_task_update"
	SettlementAfterSettlementEventInsert SettlementFailpoint = "after_settlement_event_insert"
	SettlementAfterOutboxInsert          SettlementFailpoint = "after_outbox_insert"
	SettlementBeforeCommit               SettlementFailpoint = "before_commit"
	SettlementAfterCommit                SettlementFailpoint = "after_commit"
)

// registerSettlementFailpoint is package-private so only same-package tests can arm it.
func registerSettlementFailpoint(point SettlementFailpoint, hook func() error) func() {
	return func() {}
}

func runSettlementFailpoint(point SettlementFailpoint) error { return nil }

// SettleRun applies one terminal settlement and replays it idempotently.
func (s *Store) SettleRun(ctx context.Context, req SettlementRequest) (Task, error) {
	fingerprint, err := settlementFingerprint(req)
	if err != nil {
		return Task{}, err
	}
	var settled Task
	err = s.withImmediateTx(ctx, func(tx txRunner) error {
		run, err := getRunTx(ctx, tx, req.RunID)
		if err != nil {
			return err
		}
		task, err := getTaskTx(ctx, tx, run.TaskID)
		if err != nil {
			return err
		}
		if run.EndedAt != nil {
			var payload string
			err := tx.QueryRowContext(ctx, `SELECT payload_json FROM task_events WHERE run_id=? AND kind='settled'`, run.ID).Scan(&payload)
			if err != nil {
				return fmt.Errorf("finite: settled run lacks its event: %w", err)
			}
			var audit struct {
				RequestSHA256 string `json:"request_sha256"`
			}
			if json.Unmarshal([]byte(payload), &audit) != nil || audit.RequestSHA256 != fingerprint {
				return fmt.Errorf("%w: run %d", ErrSettlementConflict, run.ID)
			}
			settled = task
			return nil
		}
		if task.Version != req.ExpectedTaskVersion {
			return fmt.Errorf("%w: expected %d, current %d", ErrVersionConflict, req.ExpectedTaskVersion, task.Version)
		}
		expectedState := StateWorking
		if run.Phase == PhaseReview {
			expectedState = StateReviewing
		}
		if task.State != expectedState {
			return fmt.Errorf("%w: run phase %s with task state %s", ErrStateConflict, run.Phase, task.State)
		}

		result := req.Result
		var resultJSON []byte
		resultInvalid := req.ResultError != nil
		if result != nil {
			if err := ValidateResult(run, *result); err != nil {
				result = nil
				resultInvalid = true
			} else {
				if result.Wait != nil && result.Wait.Kind == WaitJoey {
					ask, askErr := decodeJoeyAsk(result.Wait.Ask)
					if askErr != nil {
						result = nil
						resultInvalid = true
					} else {
						result.Wait.Ask, _ = json.Marshal(ask)
					}
				}
				if result != nil {
					resultJSON, _ = json.Marshal(result)
				}
			}
		}
		outcome := req.ExecutionOutcome
		if outcome == "" {
			outcome = RunOK
		}
		if task.CancelRequested {
			outcome = RunInterrupted
		} else if result != nil {
			outcome = RunOK
		}
		if !validRunOutcome(outcome) {
			return fmt.Errorf("finite: invalid execution outcome %q", outcome)
		}
		if outcome == RunWall && req.WallWakeAt == nil {
			return fmt.Errorf("finite: quota wall requires a provider reset timestamp")
		}
		if req.Usage != nil {
			if err := validateNormalizedUsage(*req.Usage); err != nil {
				return err
			}
		}

		runError := req.Error
		if task.CancelRequested {
			runError = "cancel_requested"
			result = nil
			resultJSON = nil
		} else if outcome == RunOK && result == nil {
			runError = "invalid_result"
		} else if runError == "" && resultInvalid {
			runError = "invalid_result"
		}
		now := s.now().UTC()
		if result != nil {
			if _, err := tx.ExecContext(ctx, `INSERT INTO run_results(run_id,task_id,phase,outcome,result_json) VALUES(?,?,?,?,?)`,
				run.ID, task.ID, run.Phase, result.Outcome, string(resultJSON)); err != nil {
				return fmt.Errorf("finite: insert run result: %w", err)
			}
		}
		update, err := tx.ExecContext(ctx, `UPDATE runs SET ended_at=?,exit_code=?,outcome=?,error=? WHERE id=? AND ended_at IS NULL`,
			stamp(now), req.ExitCode, outcome, nullString(runError), run.ID)
		if err != nil {
			return err
		}
		if affected, _ := update.RowsAffected(); affected != 1 {
			return fmt.Errorf("%w: run ended concurrently", ErrSettlementConflict)
		}
		if req.Usage != nil {
			usage := cloneUsage(*req.Usage)
			if _, err := tx.ExecContext(ctx, `INSERT INTO run_usage(run_id,fresh_input_tokens,cache_read_tokens,cache_write_tokens,
				output_tokens,reasoning_tokens,api_calls,tool_calls,source,collected_at) VALUES(?,?,?,?,?,?,?,?,?,?)`,
				run.ID, usage.FreshInputTokens, usage.CacheReadTokens, usage.CacheWriteTokens, usage.OutputTokens,
				usage.ReasoningTokens, usage.APICalls, usage.ToolCalls, usage.Source, stamp(usage.CollectedAt)); err != nil {
				return fmt.Errorf("finite: insert run usage: %w", err)
			}
		}
		accounting, err := derivedAccounting(ctx, tx, task.ID, run.Phase)
		if err != nil {
			return err
		}
		next := settlementState(task, run, outcome, result, accounting)
		newVersion := task.Version + 1
		if err := updateSettlementTask(ctx, tx, task, next, newVersion, now, result, req.WallWakeAt, runError); err != nil {
			return err
		}
		settled = task
		settled.State = next
		settled.Version = newVersion
		settled.UpdatedAt = now
		applySettlementWait(&settled, run, outcome, result, req.WallWakeAt, runError)
		auditPayload, _ := json.Marshal(map[string]any{
			"request_sha256": fingerprint, "execution_outcome": outcome,
			"result_outcome": resultOutcome(result), "error": runError,
		})
		eventID := fmt.Sprintf("run:%d:settled", run.ID)
		if err := insertTaskEvent(ctx, tx, eventID, task.ID, &run.ID, "settled", task.State,
			next, newVersion, auditPayload, stamp(now)); err != nil {
			return fmt.Errorf("finite: insert settlement event: %w", err)
		}
		if terminalState(next) {
			if err := insertTerminalOutbox(ctx, tx, eventID, task.ID, run.ID, next, newVersion, now); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return Task{}, err
	}
	return settled, nil
}

func settlementFingerprint(req SettlementRequest) (string, error) {
	var result json.RawMessage
	if req.Result != nil {
		encoded, err := json.Marshal(req.Result)
		if err != nil {
			return "", err
		}
		result = encoded
	}
	var usage json.RawMessage
	if req.Usage != nil {
		encoded, err := json.Marshal(cloneUsage(*req.Usage))
		if err != nil {
			return "", err
		}
		usage = encoded
	}
	value := struct {
		Outcome    RunOutcome      `json:"outcome"`
		ExitCode   *int            `json:"exit_code"`
		Result     json.RawMessage `json:"result"`
		Invalid    bool            `json:"invalid"`
		Usage      json.RawMessage `json:"usage"`
		WallWakeAt *time.Time      `json:"wall_wake_at"`
		Error      string          `json:"error"`
	}{req.ExecutionOutcome, req.ExitCode, result, req.ResultError != nil, usage, req.WallWakeAt, req.Error}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(encoded)), nil
}

func validRunOutcome(outcome RunOutcome) bool {
	return outcome == RunOK || outcome == RunFail || outcome == RunTimeout || outcome == RunLost || outcome == RunInterrupted || outcome == RunWall
}

func resultOutcome(result *Result) ResultOutcome {
	if result == nil {
		return ""
	}
	return result.Outcome
}

func phaseQueue(phase Phase) State {
	if phase == PhaseReview {
		return StateReview
	}
	return StateTodo
}

func settlementState(task Task, run Run, outcome RunOutcome, result *Result, accounting Accounting) State {
	if task.CancelRequested {
		return StateCancelled
	}
	if outcome == RunWall {
		return StateWaiting
	}
	if outcome == RunLost || outcome == RunInterrupted {
		return phaseQueue(run.Phase)
	}
	capReached := accounting.Attempts >= task.MaxRuns || accounting.Failures >= task.MaxFailures || accounting.ReviewCycles >= task.MaxReviewCycles
	if result == nil {
		if capReached {
			return StateFailed
		}
		return phaseQueue(run.Phase)
	}
	switch result.Outcome {
	case ResultDone:
		if task.ReviewRequired {
			return StateReview
		}
		return StateDone
	case ResultReview:
		return StateReview
	case ResultAccepted:
		return StateDone
	case ResultFailed:
		return StateFailed
	case ResultWait:
		return StateWaiting
	case ResultTodo, ResultRejected:
		if capReached {
			return StateFailed
		}
		return phaseQueue(run.Phase)
	default:
		return StateFailed
	}
}

func updateSettlementTask(ctx context.Context, tx txRunner, task Task, next State, version int64, now time.Time, result *Result, wallWake *time.Time, runError string) error {
	var resume, kind, wake, waiting, ask any
	if next == StateWaiting {
		if task.State == StateReviewing {
			resume = StateReview
		} else {
			resume = StateTodo
		}
		waiting = runError
		if result != nil && result.Outcome == ResultWait {
			waiting = result.Wait.Reason
			kind = result.Wait.Kind
			if result.Wait.Kind == WaitTime {
				parsed, _ := time.Parse(time.RFC3339Nano, result.Wait.WakeAt)
				wake = stamp(parsed)
			} else {
				ask = string(result.Wait.Ask)
			}
		} else {
			kind = WaitTime
			wake = stamp(*wallWake)
			if strings.TrimSpace(runError) == "" {
				waiting = "provider quota wall"
			}
		}
	}
	update, err := tx.ExecContext(ctx, `UPDATE tasks SET state=?,version=?,updated_at=?,resume_state=?,wait_kind=?,wake_at=?,waiting_for=?,joey_ask_json=?
		WHERE id=? AND version=? AND state=?`, next, version, stamp(now), resume, kind, wake, waiting, ask, task.ID, task.Version, task.State)
	if err != nil {
		return err
	}
	if affected, _ := update.RowsAffected(); affected != 1 {
		return fmt.Errorf("%w: settlement task update", ErrVersionConflict)
	}
	return nil
}

func applySettlementWait(task *Task, run Run, outcome RunOutcome, result *Result, wallWake *time.Time, runError string) {
	task.ResumeState = ""
	task.WaitKind = ""
	task.WakeAt = nil
	task.WaitingFor = ""
	task.JoeyAskJSON = ""
	if task.State != StateWaiting {
		return
	}
	task.ResumeState = phaseQueue(run.Phase)
	task.WaitingFor = runError
	if result != nil && result.Outcome == ResultWait {
		task.WaitKind = result.Wait.Kind
		task.WaitingFor = result.Wait.Reason
		if result.Wait.Kind == WaitTime {
			parsed, _ := time.Parse(time.RFC3339Nano, result.Wait.WakeAt)
			task.WakeAt = &parsed
		} else {
			task.JoeyAskJSON = string(result.Wait.Ask)
		}
		return
	}
	task.WaitKind = WaitTime
	task.WakeAt = wallWake
	if task.WaitingFor == "" {
		task.WaitingFor = "provider quota wall"
	}
}

func terminalState(state State) bool {
	return state == StateDone || state == StateFailed || state == StateCancelled
}

func insertTerminalOutbox(ctx context.Context, tx txRunner, eventID, taskID string, runID int64, state State, version int64, now time.Time) error {
	deliveryID := fmt.Sprintf("task:%s:v%d:default", taskID, version)
	payload, _ := json.Marshal(map[string]any{"task_id": taskID, "run_id": runID, "state": state, "version": version})
	_, err := tx.ExecContext(ctx, `INSERT INTO outbox(delivery_id,event_id,channel,kind,payload_json,created_at,available_at)
		VALUES(?,?,?,?,?,?,?)`, deliveryID, eventID, "default", "task_terminal", string(payload), stamp(now), stamp(now))
	return err
}

// RequestStop commits the durable cancellation fence.
func (s *Store) RequestStop(ctx context.Context, taskID string, expectedVersion int64) (Task, error) {
	var stopped Task
	err := s.withImmediateTx(ctx, func(tx txRunner) error {
		task, err := getTaskTx(ctx, tx, taskID)
		if err != nil {
			return err
		}
		if task.Version != expectedVersion {
			return fmt.Errorf("%w: stop", ErrVersionConflict)
		}
		if terminalState(task.State) {
			stopped = task
			return nil
		}
		now := s.now().UTC()
		next := task.State
		if task.State != StateWorking && task.State != StateReviewing {
			next = StateCancelled
		}
		newVersion := task.Version + 1
		update, err := tx.ExecContext(ctx, `UPDATE tasks SET cancel_requested=1,state=?,version=?,updated_at=?,
			resume_state=NULL,wait_kind=NULL,wake_at=NULL,waiting_for=NULL,joey_ask_json=NULL
			WHERE id=? AND version=?`, next, newVersion, stamp(now), task.ID, task.Version)
		if err != nil {
			return err
		}
		if affected, _ := update.RowsAffected(); affected != 1 {
			return fmt.Errorf("%w: stop fence", ErrVersionConflict)
		}
		eventID := fmt.Sprintf("task:%s:v%d:stop_requested", task.ID, newVersion)
		if err := insertTaskEvent(ctx, tx, eventID, task.ID, nil, "stop_requested", task.State, next, newVersion, []byte(`{}`), stamp(now)); err != nil {
			return err
		}
		if next == StateCancelled {
			if err := insertTerminalOutbox(ctx, tx, eventID, task.ID, 0, next, newVersion, now); err != nil {
				return err
			}
		}
		stopped = task
		stopped.CancelRequested = true
		stopped.State = next
		stopped.Version = newVersion
		stopped.UpdatedAt = now
		stopped.ResumeState = ""
		stopped.WaitKind = ""
		stopped.WakeAt = nil
		stopped.WaitingFor = ""
		stopped.JoeyAskJSON = ""
		return nil
	})
	return stopped, err
}

// WakeTime performs a due time-only, task-only wait transition.
func (s *Store) WakeTime(ctx context.Context, taskID string, expectedVersion int64) (Task, error) {
	return s.completeWait(ctx, taskID, expectedVersion, "", false)
}

// AnswerJoey performs a matching-answer, task-only wait transition.
func (s *Store) AnswerJoey(ctx context.Context, taskID string, expectedVersion int64, optionID string) (Task, error) {
	return s.completeWait(ctx, taskID, expectedVersion, optionID, true)
}

func (s *Store) completeWait(ctx context.Context, taskID string, expectedVersion int64, optionID string, joey bool) (Task, error) {
	var completed Task
	err := s.withImmediateTx(ctx, func(tx txRunner) error {
		task, err := getTaskTx(ctx, tx, taskID)
		if err != nil {
			return err
		}
		if task.Version != expectedVersion {
			return fmt.Errorf("%w: wait completion", ErrVersionConflict)
		}
		if task.State != StateWaiting || task.CancelRequested {
			return fmt.Errorf("%w: task is not completing a wait", ErrStateConflict)
		}
		if joey {
			if task.WaitKind != WaitJoey {
				return fmt.Errorf("%w: not a Joey wait", ErrStateConflict)
			}
			ask, err := decodeJoeyAsk([]byte(task.JoeyAskJSON))
			if err != nil {
				return err
			}
			matched := false
			for _, option := range ask.Options {
				matched = matched || option.ID == optionID
			}
			if !matched {
				return fmt.Errorf("finite: answer is not an offered option")
			}
		} else {
			if task.WaitKind != WaitTime || task.WakeAt == nil || s.now().UTC().Before(task.WakeAt.UTC()) {
				return fmt.Errorf("%w: time wait is not due", ErrStateConflict)
			}
		}
		now := s.now().UTC()
		next := task.ResumeState
		newVersion := task.Version + 1
		update, err := tx.ExecContext(ctx, `UPDATE tasks SET state=?,version=?,updated_at=?,resume_state=NULL,wait_kind=NULL,
			wake_at=NULL,waiting_for=NULL,joey_ask_json=NULL WHERE id=? AND version=? AND state='waiting'`,
			next, newVersion, stamp(now), task.ID, task.Version)
		if err != nil {
			return err
		}
		if affected, _ := update.RowsAffected(); affected != 1 {
			return fmt.Errorf("%w: wait update", ErrVersionConflict)
		}
		kind := "woke"
		if joey {
			kind = "answered"
		}
		payload, _ := json.Marshal(map[string]any{"option_id": optionID})
		eventID := fmt.Sprintf("task:%s:v%d:%s", task.ID, newVersion, kind)
		if err := insertTaskEvent(ctx, tx, eventID, task.ID, nil, kind, task.State, next, newVersion, payload, stamp(now)); err != nil {
			return err
		}
		completed = task
		completed.State = next
		completed.Version = newVersion
		completed.UpdatedAt = now
		completed.ResumeState = ""
		completed.WaitKind = ""
		completed.WakeAt = nil
		completed.WaitingFor = ""
		completed.JoeyAskJSON = ""
		return nil
	})
	return completed, err
}
