package finite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// recoverOpenRuns reconciles each open run independently.
func (e *Engine) recoverOpenRuns(ctx context.Context) error {
	rows, err := e.store.db.QueryContext(ctx, runSelect+` WHERE ended_at IS NULL ORDER BY id`)
	if err != nil {
		return err
	}
	var runs []Run
	for rows.Next() {
		run, scanErr := scanRun(rows)
		if scanErr != nil {
			_ = rows.Close()
			return scanErr
		}
		runs = append(runs, run)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	var errs []error
	for _, run := range runs {
		if err := e.recoverRun(ctx, run); err != nil {
			errs = append(errs, fmt.Errorf("finite: recover run %d: %w", run.ID, err))
		}
	}
	return errors.Join(errs...)
}

func (e *Engine) recoverRun(ctx context.Context, run Run) error {
	task, err := e.store.GetTask(ctx, run.TaskID)
	if err != nil {
		return err
	}
	if task.CancelRequested {
		if run.ProcessToken != "" {
			liveness, stopErr := e.stopRun(ctx, run.ID, run.ProcessToken, e.opts.StopGrace)
			if stopErr != nil {
				return stopErr
			}
			if liveness != LivenessAbsent {
				return ErrProcessUnknown
			}
		}
		// The stopped turn may still have spent tokens. Record valid usage;
		// unreadable usage must not hold a stop open, so it stays unreported.
		usage, usageErr := readRecoveredUsage(run)
		if usageErr != nil {
			usage = nil
		}
		_, err := e.settleRecovered(ctx, SettlementRequest{
			RunID: run.ID, ExpectedTaskVersion: task.Version, ExecutionOutcome: RunInterrupted, Usage: usage,
		})
		return err
	}
	if run.ProcessToken == "" {
		// The launch path persists a token before any supervisor can exist.
		_, err := e.settleRecovered(ctx, SettlementRequest{
			RunID: run.ID, ExpectedTaskVersion: task.Version, ExecutionOutcome: RunLost, Error: "owner exited before supervisor start",
		})
		return err
	}

	result, resultErr, resultPresent := readRecoveredResult(run)
	observation, err := e.inspectRun(ctx, run.ID, run.ProcessToken)
	if err != nil {
		return err
	}
	switch observation.Liveness {
	case LivenessUnknown:
		return ErrProcessUnknown
	case LivenessLive:
		if observation.Supervisor == nil {
			return ErrDetachedProvider
		}
		identity := *observation.Supervisor
		if run.SupervisorPID != nil && *run.SupervisorPID != identity.PID {
			return ErrProcessIdentityMismatch
		}
		persisted, err := readEngineOwnerIdentity(run)
		if err != nil {
			return err
		}
		if persisted.PID != 0 && !persisted.Equal(identity) {
			return ErrProcessIdentityMismatch
		}
		adopted, err := AdoptRun(ctx, identity)
		if err != nil {
			return err
		}
		if err := e.persistStarted(ctx, run, identity); err != nil {
			return err
		}
		if result == nil || resultErr != nil {
			e.watchAdopted(run.ID, adopted)
			return nil
		}
		execution, err := readRecoveredExecution(run)
		if err != nil {
			return err
		}
		if execution == nil {
			e.watchAdopted(run.ID, adopted)
			return nil
		}
		// A completed provider turn can finish between adoption and this scan.
		// Wait for actual boundary absence now instead of depending on another
		// scheduler pass to settle the original run, but only for a bounded
		// time: the scan holds scanMu. A boundary that outlives the bound
		// stays open and monitored, and a later scan settles it.
		waitCtx, cancelWait := context.WithTimeout(ctx, adoptedSettleWait(e.opts.StopGrace))
		err = adopted.Wait(waitCtx)
		cancelWait()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
				e.watchAdopted(run.ID, adopted)
				return nil
			}
			return err
		}
	case LivenessAbsent:
		// Settlement is permitted only after the whole run boundary is absent.
	default:
		return ErrProcessUnknown
	}

	// A fast worker may publish its result between the first result check and
	// the absence scan. Re-read only after reliable absence so that a complete
	// result cannot be misclassified as a missing result.
	result, resultErr, resultPresent = readRecoveredResult(run)
	execution, err := readRecoveredExecution(run)
	if err != nil {
		return err
	}
	usage, err := readRecoveredUsage(run)
	if err != nil {
		return err
	}
	request := SettlementRequest{
		RunID: run.ID, ExpectedTaskVersion: task.Version, ExecutionOutcome: RunLost,
		Result: result, ResultError: resultErr, Usage: usage, Error: "provider boundary is absent without a result",
	}
	if execution != nil {
		request.ExecutionOutcome = execution.Outcome
		request.ExitCode = execution.ExitCode
		request.WallWakeAt = execution.WallWakeAt
		request.Error = execution.Error
	} else if resultPresent {
		request.ExecutionOutcome = RunOK
		request.Error = ""
	} else {
		started, err := providerTurnStarted(run)
		if err != nil {
			return err
		}
		if started {
			// The provider turn began and its boundary died without a result or
			// execution record: that turn was spent, so charge it as a failure
			// and let MaxFailures bound the retries.
			request.ExecutionOutcome = RunFail
			request.Error = "supervisor exited during the provider turn without a result"
		}
	}
	_, err = e.settleRecovered(ctx, request)
	return err
}

// providerTurnStarted reports whether the run directory shows the supervisor
// began its provider turn: the start fence or any recorded provider event.
func providerTurnStarted(run Run) (bool, error) {
	if _, err := os.Lstat(filepath.Join(run.RunDir, "supervisor-started.json")); err == nil {
		return true, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	info, err := os.Lstat(filepath.Join(run.RunDir, "events.jsonl"))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return info.Size() > 0, nil
}

func (e *Engine) settleRecovered(ctx context.Context, request SettlementRequest) (Task, error) {
	if e.opts.hooks.beforeSettlement != nil {
		e.opts.hooks.beforeSettlement(request)
	}
	task, err := e.store.SettleRun(ctx, request)
	if err != nil {
		return Task{}, err
	}
	if e.opts.hooks.afterSettlement != nil {
		e.opts.hooks.afterSettlement(request)
	}
	if err := e.store.ProjectTask(ctx, task.ID); err != nil {
		return task, err
	}
	e.signalWake()
	return task, nil
}

func (e *Engine) inspectRun(ctx context.Context, runID int64, token string) (RunProcesses, error) {
	if e.opts.hooks.inspectRun != nil {
		return e.opts.hooks.inspectRun(ctx, runID, token)
	}
	return e.inspectRunProcesses(ctx, runID, token)
}

func (e *Engine) stopRun(ctx context.Context, runID int64, token string, grace time.Duration) (Liveness, error) {
	if e.opts.hooks.stopRun != nil {
		return e.opts.hooks.stopRun(ctx, runID, token, grace)
	}
	return e.stopRunProcesses(ctx, runID, token, grace)
}

func readRecoveredResult(run Run) (*Result, error, bool) {
	_, err := os.Lstat(filepath.Join(run.RunDir, "result.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, false
	}
	if err != nil {
		return nil, err, true
	}
	result, err := ReadResult(run)
	if err != nil {
		return nil, err, true
	}
	return &result, nil, true
}

type engineExecutionRecord struct {
	SchemaVersion int        `json:"schema_version"`
	RunID         int64      `json:"run_id"`
	Outcome       RunOutcome `json:"outcome"`
	Error         string     `json:"error,omitempty"`
	ExitCode      *int       `json:"exit_code,omitempty"`
	WallWakeAt    *time.Time `json:"wall_wake_at,omitempty"`
	CompletedAt   time.Time  `json:"completed_at,omitempty"`
}

func readRecoveredExecution(run Run) (*engineExecutionRecord, error) {
	execution, found, err := readEngineExecutionFile(filepath.Join(run.RunDir, "execution.json"), run.ID)
	if err != nil {
		return nil, err
	}
	if found {
		return execution, nil
	}
	completion, found, err := readEngineExecutionFile(filepath.Join(run.RunDir, "completion.json"), run.ID)
	if err != nil || !found {
		return nil, err
	}
	return completion, nil
}

func readEngineExecutionFile(path string, runID int64) (*engineExecutionRecord, bool, error) {
	data, err := readSupervisorBoundedFile(path, 64*1024)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	var record engineExecutionRecord
	if err := decodeEngineJSON(data, &record); err != nil {
		return nil, true, err
	}
	if record.SchemaVersion != 1 || record.RunID != runID || !validRunOutcome(record.Outcome) {
		return nil, true, fmt.Errorf("finite: invalid execution artifact binding or outcome")
	}
	if record.Outcome == RunWall && record.WallWakeAt == nil {
		return nil, true, fmt.Errorf("finite: wall execution artifact lacks reset time")
	}
	return &record, true, nil
}

type engineUsageRecord struct {
	RunID            int64     `json:"run_id"`
	FreshInputTokens *int64    `json:"fresh_input_tokens,omitempty"`
	CacheReadTokens  *int64    `json:"cache_read_tokens,omitempty"`
	CacheWriteTokens *int64    `json:"cache_write_tokens,omitempty"`
	OutputTokens     *int64    `json:"output_tokens,omitempty"`
	ReasoningTokens  *int64    `json:"reasoning_tokens,omitempty"`
	APICalls         *int64    `json:"api_calls,omitempty"`
	ToolCalls        *int64    `json:"tool_calls,omitempty"`
	Source           string    `json:"source"`
	CollectedAt      time.Time `json:"collected_at"`
}

func readRecoveredUsage(run Run) (*Usage, error) {
	data, err := readSupervisorBoundedFile(filepath.Join(run.RunDir, "usage.json"), 64*1024)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var record engineUsageRecord
	if err := decodeEngineJSON(data, &record); err != nil {
		return nil, err
	}
	if record.RunID != run.ID {
		return nil, fmt.Errorf("finite: usage run binding mismatch")
	}
	usage := &Usage{
		FreshInputTokens: record.FreshInputTokens, CacheReadTokens: record.CacheReadTokens,
		CacheWriteTokens: record.CacheWriteTokens, OutputTokens: record.OutputTokens,
		ReasoningTokens: record.ReasoningTokens, APICalls: record.APICalls, ToolCalls: record.ToolCalls,
		Source: record.Source, CollectedAt: record.CollectedAt,
	}
	if err := validateNormalizedUsage(*usage); err != nil {
		return nil, err
	}
	return usage, nil
}

func decodeEngineJSON(data []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fmt.Errorf("finite: trailing JSON artifact data")
	}
	return nil
}

// adoptedSettleMargin is the slack beyond the stop grace that a scan waits
// for an adopted boundary whose turn has already published its artifacts.
const adoptedSettleMargin = 2 * time.Second

// adoptedSettleWait bounds the in-scan wait for an adopted boundary's
// absence. The scan holds scanMu, so the wait must never be unbounded.
func adoptedSettleWait(stopGrace time.Duration) time.Duration {
	if stopGrace <= 0 {
		return 30 * time.Second
	}
	return stopGrace + adoptedSettleMargin
}
