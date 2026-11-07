package finite

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
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

var settlementTestFailpoint struct {
	sync.RWMutex
	point SettlementFailpoint
	hook  func() error
}

// registerSettlementFailpoint is package-private so only same-package tests can arm it.
func registerSettlementFailpoint(point SettlementFailpoint, hook func() error) func() {
	settlementTestFailpoint.Lock()
	settlementTestFailpoint.point = point
	settlementTestFailpoint.hook = hook
	settlementTestFailpoint.Unlock()
	return func() {
		settlementTestFailpoint.Lock()
		settlementTestFailpoint.point = ""
		settlementTestFailpoint.hook = nil
		settlementTestFailpoint.Unlock()
	}
}

func runSettlementFailpoint(point SettlementFailpoint) error {
	settlementTestFailpoint.RLock()
	hook := settlementTestFailpoint.hook
	if settlementTestFailpoint.point != point {
		hook = nil
	}
	settlementTestFailpoint.RUnlock()
	if hook != nil {
		if err := hook(); err != nil {
			return fmt.Errorf("finite: settlement failpoint %s: %w", point, err)
		}
	}
	return nil
}

// SettleRun applies one terminal settlement and replays it idempotently.
func (s *Store) SettleRun(ctx context.Context, req SettlementRequest) (Task, error) {
	req = canonicalSettlementRequest(req)
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
					// A valid ask is already canonical; see canonicalSettlementRequest.
					if _, askErr := decodeJoeyAsk(result.Wait.Ask); askErr != nil {
						result = nil
						resultInvalid = true
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
			if err := runSettlementFailpoint(SettlementAfterRunResultsInsert); err != nil {
				return err
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
		if err := runSettlementFailpoint(SettlementAfterRunUpdate); err != nil {
			return err
		}
		if req.Usage != nil {
			usage := cloneUsage(*req.Usage)
			if _, err := tx.ExecContext(ctx, `INSERT INTO run_usage(run_id,fresh_input_tokens,cache_read_tokens,cache_write_tokens,
				output_tokens,reasoning_tokens,api_calls,tool_calls,source,collected_at) VALUES(?,?,?,?,?,?,?,?,?,?)`,
				run.ID, usage.FreshInputTokens, usage.CacheReadTokens, usage.CacheWriteTokens, usage.OutputTokens,
				usage.ReasoningTokens, usage.APICalls, usage.ToolCalls, usage.Source, stamp(usage.CollectedAt)); err != nil {
				return fmt.Errorf("finite: insert run usage: %w", err)
			}
			if err := runSettlementFailpoint(SettlementAfterUsageInsert); err != nil {
				return err
			}
		}
		accounting, err := derivedAccounting(ctx, tx, task.ID, run.Phase)
		if err != nil {
			return err
		}
		next, failureReason := settlementState(task, run, outcome, result, accounting, now)
		newVersion := task.Version + 1
		if err := updateSettlementTask(ctx, tx, task, next, newVersion, now, result, settlementWallWake(req), runError); err != nil {
			return err
		}
		if err := runSettlementFailpoint(SettlementAfterTaskUpdate); err != nil {
			return err
		}
		settled = task
		settled.State = next
		settled.Version = newVersion
		settled.UpdatedAt = now
		applySettlementWait(&settled, run, outcome, result, settlementWallWake(req), runError)
		audit := map[string]any{
			"request_sha256": fingerprint, "execution_outcome": outcome,
			"result_outcome": resultOutcome(result), "error": runError,
		}
		if outcome == RunWall {
			// The fallback walk reads the walled seat and the wall deadline
			// from this payload; no schema column carries them. wall_source
			// and pool say how the wake was derived; a payload without a
			// wall_source is a legacy row, re-read by the legacy rules.
			audit["seat"] = run.Seat
			if wake := settlementWallWake(req); wake != nil {
				audit["wall_wake_at"] = wake.UTC().Format(time.RFC3339Nano)
			}
			if req.WallSource != "" {
				audit["wall_source"] = req.WallSource
			}
			if req.WallPool != "" {
				audit["pool"] = req.WallPool
			}
		}
		if failureReason != "" {
			audit["failure_reason"] = failureReason
		}
		if result != nil {
			// A bounded digest so later runs of the task can be told what this
			// run concluded; the full result stays in the run directory.
			audit["phase"] = run.Phase
			audit["progress"] = clipRunes(result.Progress, historyProgressRunes)
			if len(result.Findings) != 0 {
				findings := make([]string, 0, len(result.Findings))
				for _, finding := range result.Findings {
					findings = append(findings, clipRunes(finding, historyFindingRunes))
				}
				audit["findings"] = findings
			}
		}
		auditPayload, _ := json.Marshal(audit)
		eventID := fmt.Sprintf("run:%d:settled", run.ID)
		if err := insertTaskEvent(ctx, tx, eventID, task.ID, &run.ID, "settled", task.State,
			next, newVersion, auditPayload, stamp(now)); err != nil {
			return fmt.Errorf("finite: insert settlement event: %w", err)
		}
		if err := runSettlementFailpoint(SettlementAfterSettlementEventInsert); err != nil {
			return err
		}
		if terminalState(next) {
			if err := insertTerminalOutbox(ctx, tx, eventID, task.ID, run.ID, next, newVersion, now); err != nil {
				return err
			}
			if err := runSettlementFailpoint(SettlementAfterOutboxInsert); err != nil {
				return err
			}
		}
		if err := runSettlementFailpoint(SettlementBeforeCommit); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return Task{}, err
	}
	if err := runSettlementFailpoint(SettlementAfterCommit); err != nil {
		return Task{}, err
	}
	return settled, nil
}

// canonicalSettlementRequest returns req with a valid Joey ask in canonical
// JSON form. The result and wait are copied so the caller's request is never
// rewritten, and the fingerprint is taken over this form so a replay whose ask
// differs only in key order or spacing matches the settled request.
func canonicalSettlementRequest(req SettlementRequest) SettlementRequest {
	if req.Result == nil || req.Result.Wait == nil || req.Result.Wait.Kind != WaitJoey {
		return req
	}
	ask, err := decodeJoeyAsk(req.Result.Wait.Ask)
	if err != nil {
		return req
	}
	canonical, err := json.Marshal(ask)
	if err != nil {
		return req
	}
	result := *req.Result
	wait := *result.Wait
	wait.Ask = canonical
	result.Wait = &wait
	req.Result = &result
	return req
}

// settlementWallWake returns the wake the settlement writes: the engine's
// resolved value when one came with the request, else the harness value as
// the request carried it.
func settlementWallWake(req SettlementRequest) *time.Time {
	if req.ResolvedWallWakeAt != nil {
		return req.ResolvedWallWakeAt
	}
	return req.WallWakeAt
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

// wallStreakBudget bounds how long one unbroken run of provider walls may
// hold a task waiting. A wall spends nothing and never consumes the run
// budget; a provider condition that outlasts this span is a stuck
// provider, not a wall, and the task fails so it is visible and rerunnable.
const wallStreakBudget = 48 * time.Hour

// settlementState chooses the next task state and, when a run-policy cap is
// what ends the task, names that cap.
func settlementState(task Task, run Run, outcome RunOutcome, result *Result, accounting Accounting, now time.Time) (State, string) {
	if task.CancelRequested {
		return StateCancelled, ""
	}
	// Lost and interrupted runs are uncharged but still count toward
	// MaxRuns, so a cause that repeats forever still ends in a failed
	// task. A wall is different: the provider refused the turn at the door
	// (quota reset, empty balance) and nothing was spent, so it is bounded
	// by how long the wall streak has lasted, not by the run budget.
	runCapReached := accounting.Runs >= task.MaxRuns
	if outcome == RunWall {
		if accounting.WallStreakStart != nil && now.Sub(*accounting.WallStreakStart) > wallStreakBudget {
			return StateFailed, "wall_span"
		}
		return StateWaiting, ""
	}
	if outcome == RunLost || outcome == RunInterrupted {
		if runCapReached {
			return StateFailed, "max_runs"
		}
		return phaseQueue(run.Phase), ""
	}
	capReason := ""
	switch {
	case runCapReached || accounting.Attempts >= task.MaxRuns:
		capReason = "max_runs"
	case accounting.Failures >= task.MaxFailures:
		capReason = "max_failures"
	case accounting.ReviewCycles >= task.MaxReviewCycles:
		capReason = "max_review_cycles"
	}
	if result == nil {
		if capReason != "" {
			return StateFailed, capReason
		}
		return phaseQueue(run.Phase), ""
	}
	switch result.Outcome {
	case ResultDone:
		if task.ReviewRequired {
			return StateReview, ""
		}
		return StateDone, ""
	case ResultReview:
		return StateReview, ""
	case ResultAccepted:
		return StateDone, ""
	case ResultFailed:
		return StateFailed, ""
	case ResultWait:
		return StateWaiting, ""
	case ResultTodo, ResultRejected:
		if capReason != "" {
			return StateFailed, capReason
		}
		if result.Outcome == ResultRejected {
			return StateTodo, ""
		}
		return phaseQueue(run.Phase), ""
	default:
		return StateFailed, ""
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

// FailTask records a deterministic terminal admission failure without a run.
func (s *Store) FailTask(ctx context.Context, taskID string, expectedVersion int64, reason string) (Task, error) {
	reason = strings.TrimSpace(reason)
	if reason == "" || len(reason) > 256 {
		return Task{}, fmt.Errorf("finite: admission failure reason is empty or oversized")
	}
	var failed Task
	err := s.withImmediateTx(ctx, func(tx txRunner) error {
		task, err := getTaskTx(ctx, tx, taskID)
		if err != nil {
			return err
		}
		if task.Version != expectedVersion {
			return fmt.Errorf("%w: admission failure", ErrVersionConflict)
		}
		if task.CancelRequested || (task.State != StateTodo && task.State != StateReview) {
			return fmt.Errorf("%w: admission failure from %s", ErrStateConflict, task.State)
		}
		var open int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE task_id=? AND ended_at IS NULL`, task.ID).Scan(&open); err != nil {
			return err
		}
		if open != 0 {
			return fmt.Errorf("%w: admission failure with an open run", ErrStateConflict)
		}
		now := s.now().UTC()
		newVersion := task.Version + 1
		update, err := tx.ExecContext(ctx, `UPDATE tasks SET state='failed',version=?,updated_at=?,
			resume_state=NULL,wait_kind=NULL,wake_at=NULL,waiting_for=NULL,joey_ask_json=NULL
			WHERE id=? AND version=? AND state=?`, newVersion, stamp(now), task.ID, task.Version, task.State)
		if err != nil {
			return err
		}
		if affected, _ := update.RowsAffected(); affected != 1 {
			return fmt.Errorf("%w: admission failure update", ErrVersionConflict)
		}
		eventID := fmt.Sprintf("task:%s:v%d:admission_failed", task.ID, newVersion)
		payload, _ := json.Marshal(map[string]any{"reason": reason})
		if err := insertTaskEvent(ctx, tx, eventID, task.ID, nil, "admission_failed", task.State, StateFailed, newVersion, payload, stamp(now)); err != nil {
			return err
		}
		if err := insertTerminalOutbox(ctx, tx, eventID, task.ID, 0, StateFailed, newVersion, now); err != nil {
			return err
		}
		failed = task
		failed.State = StateFailed
		failed.Version = newVersion
		failed.UpdatedAt = now
		failed.ResumeState = ""
		failed.WaitKind = ""
		failed.WakeAt = nil
		failed.WaitingFor = ""
		failed.JoeyAskJSON = ""
		return nil
	})
	return failed, err
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
	return s.completeWait(ctx, taskID, expectedVersion, "", "", false)
}

// WakeFallback completes an undischarged quota-wall time wait early because a
// fallback seat can run the work now. Like WakeTime it is task-only and
// version-fenced; unlike it the deadline has not passed, so the store itself
// demands the wall evidence: the task's latest settled run must be a wall.
func (s *Store) WakeFallback(ctx context.Context, taskID string, expectedVersion int64) (Task, error) {
	var completed Task
	err := s.withImmediateTx(ctx, func(tx txRunner) error {
		task, err := getTaskTx(ctx, tx, taskID)
		if err != nil {
			return err
		}
		if task.Version != expectedVersion {
			return fmt.Errorf("%w: fallback wake", ErrVersionConflict)
		}
		if task.State != StateWaiting || task.CancelRequested || task.WaitKind != WaitTime || task.WakeAt == nil {
			return fmt.Errorf("%w: task is not completing a quota-wall wait", ErrStateConflict)
		}
		if !s.now().UTC().Before(task.WakeAt.UTC()) {
			return fmt.Errorf("%w: quota-wall wait is already due", ErrStateConflict)
		}
		var outcome RunOutcome
		if err := tx.QueryRowContext(ctx,
			`SELECT outcome FROM runs WHERE task_id=? AND ended_at IS NOT NULL ORDER BY id DESC LIMIT 1`, taskID).Scan(&outcome); err != nil || outcome != RunWall {
			return fmt.Errorf("%w: latest run is not a quota wall", ErrStateConflict)
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
			return fmt.Errorf("%w: fallback wake update", ErrVersionConflict)
		}
		payload, _ := json.Marshal(map[string]any{"reason": "fallback"})
		eventID := fmt.Sprintf("task:%s:v%d:woke", task.ID, newVersion)
		if err := insertTaskEvent(ctx, tx, eventID, task.ID, nil, "woke", task.State, next, newVersion, payload, stamp(now)); err != nil {
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

// AnswerJoey performs a matching-answer, task-only wait transition. The note
// is the free text Joey attached to his tap; empty when there was none. It is
// recorded verbatim on the answered event so every later run of the task
// reads it: the note is his instruction and overrides the option label when
// they conflict.
func (s *Store) AnswerJoey(ctx context.Context, taskID string, expectedVersion int64, optionID, note string) (Task, error) {
	return s.completeWait(ctx, taskID, expectedVersion, optionID, note, true)
}

func (s *Store) completeWait(ctx context.Context, taskID string, expectedVersion int64, optionID string, note string, joey bool) (Task, error) {
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
		answer := map[string]any{"option_id": optionID}
		if joey {
			if ask, err := decodeJoeyAsk([]byte(task.JoeyAskJSON)); err == nil {
				answer["question"] = ask.Question
				for _, option := range ask.Options {
					if option.ID == optionID {
						answer["label"] = option.Label
					}
				}
			}
			if trimmed := strings.TrimSpace(note); trimmed != "" {
				answer["note"] = trimmed
			}
		}
		payload, _ := json.Marshal(answer)
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
