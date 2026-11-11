package finite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/agent0ai/spynel/internal/fsx"
)

type engineSessionRecord struct {
	SchemaVersion int    `json:"schema_version"`
	RunID         int64  `json:"run_id"`
	TaskID        string `json:"task_id"`
	Phase         Phase  `json:"phase"`
	SessionKey    string `json:"session_key"`
	ProcessToken  string `json:"process_token"`
	BundleHash    string `json:"bundle_hash"`
	FullBundle    bool   `json:"full_bundle"`
}

type engineOwnerStartRecord struct {
	SchemaVersion int             `json:"schema_version"`
	Identity      ProcessIdentity `json:"identity"`
	StartedAt     time.Time       `json:"started_at"`
}

func (e *Engine) prepareRun(ctx context.Context, run Run, token string, prompt Prompt) error {
	if err := validateRunBinding(run.ID, token); err != nil {
		return err
	}
	err := e.store.withImmediateTx(ctx, func(tx txRunner) error {
		task, err := getTaskTx(ctx, tx, run.TaskID)
		if err != nil {
			return err
		}
		if task.CancelRequested {
			return errStopPending
		}
		result, err := tx.ExecContext(ctx, `UPDATE runs SET process_token=?,prompt_hash=?
			WHERE id=? AND ended_at IS NULL AND process_token IS NULL`, token, prompt.BundleHash, run.ID)
		if err != nil {
			return err
		}
		if affected, _ := result.RowsAffected(); affected != 1 {
			return fmt.Errorf("finite: run %d process identity was already persisted or settled", run.ID)
		}
		return nil
	})
	if err != nil {
		return err
	}
	record, _ := json.Marshal(engineSessionRecord{
		SchemaVersion: 1, RunID: run.ID, TaskID: run.TaskID, Phase: run.Phase,
		SessionKey: run.SessionKey, ProcessToken: token, BundleHash: prompt.BundleHash, FullBundle: prompt.FullBundle,
	})
	if err := fsx.AtomicCreateFile(filepath.Join(run.RunDir, "session.json"), record, 0o600); err != nil {
		return fmt.Errorf("finite: persist run session identity: %w", err)
	}
	return nil
}

func (e *Engine) persistStarted(ctx context.Context, run Run, identity ProcessIdentity) error {
	if identity.RunID != run.ID || identity.Token != run.ProcessToken || identity.PID <= 0 || identity.Start == "" {
		return ErrProcessIdentityMismatch
	}
	started := time.Now().UTC()
	startRecord := engineOwnerStartRecord{SchemaVersion: 1, Identity: identity, StartedAt: started}
	data, _ := json.Marshal(startRecord)
	path := filepath.Join(run.RunDir, "owner-identity.json")
	if existing, err := os.ReadFile(path); err == nil {
		var persisted engineOwnerStartRecord
		if json.Unmarshal(existing, &persisted) != nil || persisted.SchemaVersion != 1 || !persisted.Identity.Equal(identity) {
			return ErrProcessIdentityMismatch
		}
	} else if !os.IsNotExist(err) {
		return err
	} else if err := fsx.AtomicCreateFile(path, data, 0o600); err != nil {
		return err
	}
	return e.store.withImmediateTx(ctx, func(tx txRunner) error {
		result, err := tx.ExecContext(ctx, `UPDATE runs SET supervisor_pid=?,started_at=COALESCE(started_at,?)
			WHERE id=? AND process_token=? AND ended_at IS NULL`, identity.PID, stamp(started), run.ID, run.ProcessToken)
		if err != nil {
			return err
		}
		if affected, _ := result.RowsAffected(); affected != 1 {
			return fmt.Errorf("%w: persist supervisor identity for run %d", ErrSettlementConflict, run.ID)
		}
		return nil
	})
}

func readEngineOwnerIdentity(run Run) (ProcessIdentity, error) {
	data, err := os.ReadFile(filepath.Join(run.RunDir, "owner-identity.json"))
	if errors.Is(err, os.ErrNotExist) {
		return ProcessIdentity{}, nil
	}
	if err != nil {
		return ProcessIdentity{}, err
	}
	var record engineOwnerStartRecord
	if err := json.Unmarshal(data, &record); err != nil || record.SchemaVersion != 1 {
		return ProcessIdentity{}, ErrProcessIdentityMismatch
	}
	if record.Identity.RunID != run.ID || record.Identity.Token != run.ProcessToken {
		return ProcessIdentity{}, ErrProcessIdentityMismatch
	}
	return record.Identity, nil
}
