package finite

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Dispatch atomically reserves a run before invoking its launcher.
func (s *Store) Dispatch(ctx context.Context, req DispatchRequest, launch LaunchFunc) (Run, error) {
	if strings.TrimSpace(req.Seat) == "" || strings.TrimSpace(req.SessionKey) == "" {
		return Run{}, fmt.Errorf("finite: dispatch seat and session are required")
	}
	var dispatched Run
	err := s.withImmediateTx(ctx, func(tx txRunner) error {
		task, err := getTaskTx(ctx, tx, req.TaskID)
		if err != nil {
			return err
		}
		if task.Version != req.ExpectedVersion {
			return fmt.Errorf("%w: expected %d, current %d", ErrVersionConflict, req.ExpectedVersion, task.Version)
		}
		if task.CancelRequested {
			return fmt.Errorf("%w: cancellation is fenced", ErrStateConflict)
		}
		nextState := StateWorking
		if req.Phase == PhaseReview {
			nextState = StateReviewing
			if task.State != StateReview {
				return fmt.Errorf("%w: review dispatch from %s", ErrStateConflict, task.State)
			}
		} else if req.Phase == PhaseImplementation {
			if task.State != StateTodo {
				return fmt.Errorf("%w: implementation dispatch from %s", ErrStateConflict, task.State)
			}
		} else {
			return fmt.Errorf("%w: unknown dispatch phase", ErrStateConflict)
		}
		var open int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE task_id=? AND ended_at IS NULL`, task.ID).Scan(&open); err != nil {
			return err
		}
		if open != 0 {
			return fmt.Errorf("%w: task already has an open run", ErrStateConflict)
		}
		now := s.now().UTC()
		result, err := tx.ExecContext(ctx, `INSERT INTO runs(
			task_id,phase,task_version,seat,session_key,launch_reason,prompt_hash,run_dir,created_at
		) VALUES(?,?,?,?,?,?,?,?,?)`, task.ID, req.Phase, task.Version, req.Seat,
			req.SessionKey, req.LaunchReason, nullString(req.PromptHash), "", stamp(now))
		if err != nil {
			return fmt.Errorf("finite: insert run: %w", err)
		}
		runID, err := result.LastInsertId()
		if err != nil {
			return err
		}
		runDir := filepath.Join(s.cfg.RunsDir(), fmt.Sprintf("%d", runID))
		if _, err := tx.ExecContext(ctx, `UPDATE runs SET run_dir=? WHERE id=?`, runDir, runID); err != nil {
			return err
		}
		update, err := tx.ExecContext(ctx, `UPDATE tasks SET state=?,version=version+1,updated_at=?
			WHERE id=? AND version=? AND state=? AND cancel_requested=0`, nextState, stamp(now), task.ID, task.Version, task.State)
		if err != nil {
			return err
		}
		if affected, _ := update.RowsAffected(); affected != 1 {
			return fmt.Errorf("%w: dispatch task update", ErrVersionConflict)
		}
		dispatched = Run{ID: runID, TaskID: task.ID, Phase: req.Phase, TaskVersion: task.Version,
			Seat: req.Seat, SessionKey: req.SessionKey, LaunchReason: req.LaunchReason,
			PromptHash: req.PromptHash, RunDir: runDir, CreatedAt: now}
		payload, _ := json.Marshal(map[string]any{"phase": req.Phase, "prompt_hash": req.PromptHash})
		return insertTaskEvent(ctx, tx, fmt.Sprintf("run:%d:dispatched", runID), task.ID, &runID,
			"dispatched", task.State, nextState, task.Version+1, payload, stamp(now))
	})
	if err != nil {
		return Run{}, err
	}
	if err := os.MkdirAll(dispatched.RunDir, 0o700); err != nil {
		return dispatched, fmt.Errorf("finite: create run directory: %w", err)
	}
	if launch != nil {
		if err := launch(ctx, dispatched); err != nil {
			return dispatched, fmt.Errorf("finite: launch run %d: %w", dispatched.ID, err)
		}
	}
	return dispatched, nil
}

func insertTaskEvent(ctx context.Context, tx txRunner, eventID, taskID string, runID *int64,
	kind string, from State, to State, version int64, payload []byte, created string) error {
	if !json.Valid(payload) {
		return fmt.Errorf("finite: invalid event payload")
	}
	var run any
	if runID != nil {
		run = *runID
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO task_events(
		event_id,task_id,run_id,kind,from_state,to_state,task_version,payload_json,created_at
	) VALUES(?,?,?,?,?,?,?,?,?)`, eventID, taskID, run, kind, nullString(string(from)), to, version, string(payload), created)
	return err
}
