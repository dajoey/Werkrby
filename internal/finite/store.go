package finite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/ncruces/go-sqlite3/driver"
)

var (
	ErrNotFound           = errors.New("finite: not found")
	ErrVersionConflict    = errors.New("finite: task version conflict")
	ErrStateConflict      = errors.New("finite: task state conflict")
	ErrSettlementConflict = errors.New("finite: conflicting settlement")
	ErrInvalidResult      = errors.New("finite: invalid result")
)

// Store owns the current-schema SQLite workflow authority.
type Store struct {
	db  *sql.DB
	cfg Config
	now func() time.Time
}

type txRunner interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type rowScanner interface {
	Scan(dest ...any) error
}

// OpenStore opens an isolated current-schema store.
func OpenStore(cfg Config) (*Store, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	root, _ := filepath.Abs(cfg.Root)
	worker, _ := filepath.Abs(cfg.WorkerDir)
	cfg.Root = filepath.Clean(root)
	cfg.WorkerDir = filepath.Clean(worker)
	if err := os.MkdirAll(cfg.Root, 0o700); err != nil {
		return nil, fmt.Errorf("finite: create state root: %w", err)
	}
	if err := validateFiniteWAL(cfg.DatabasePath() + "-wal"); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite3", storeSQLiteDSN(cfg))
	if err != nil {
		return nil, fmt.Errorf("finite: open SQLite: %w", err)
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db, cfg: cfg, now: time.Now}
	closeOnError := func(err error) (*Store, error) {
		_ = db.Close()
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return closeOnError(fmt.Errorf("finite: connect SQLite: %w", err))
	}
	var version, tables int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return closeOnError(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`).Scan(&tables); err != nil {
		return closeOnError(err)
	}
	if version == 0 && tables == 0 {
		if err := s.withImmediateTx(context.Background(), func(tx txRunner) error {
			if _, err := tx.ExecContext(context.Background(), currentSchema); err != nil {
				return err
			}
			_, err := tx.ExecContext(context.Background(), fmt.Sprintf(`PRAGMA user_version=%d`, schemaVersion))
			return err
		}); err != nil {
			return closeOnError(fmt.Errorf("finite: initialize current schema: %w", err))
		}
	} else if version != schemaVersion {
		return closeOnError(fmt.Errorf("finite: schema version %d is not current version %d", version, schemaVersion))
	}
	for _, table := range []string{"tasks", "runs", "run_results", "run_usage", "task_events", "outbox"} {
		var found int
		if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&found); err != nil || found != 1 {
			return closeOnError(fmt.Errorf("finite: current schema lacks %s", table))
		}
	}
	var journal string
	var synchronous, foreignKeys int
	if err := db.QueryRow(`PRAGMA journal_mode`).Scan(&journal); err != nil {
		return closeOnError(err)
	}
	if err := db.QueryRow(`PRAGMA synchronous`).Scan(&synchronous); err != nil {
		return closeOnError(err)
	}
	if err := db.QueryRow(`PRAGMA foreign_keys`).Scan(&foreignKeys); err != nil {
		return closeOnError(err)
	}
	if !strings.EqualFold(journal, "wal") || synchronous != 2 || foreignKeys != 1 {
		return closeOnError(fmt.Errorf("finite: SQLite durability prerequisites were not enabled"))
	}
	if err := s.CheckInvariants(context.Background()); err != nil {
		return closeOnError(fmt.Errorf("finite: validate SQLite on open: %w", err))
	}
	return s, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) withImmediateTx(ctx context.Context, fn func(tx txRunner) error) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("finite: begin immediate transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := runStoreTransactionFault(storeTransactionBeforeCommit); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("finite: commit transaction: %w", err)
	}
	return runStoreTransactionFault(storeTransactionAfterCommit)
}

const taskSelect = `SELECT id,title,state,risk,review_required,cancel_requested,max_runs,max_failures,max_review_cycles,
	brief_path,brief_sha256,projection_path,resume_state,wait_kind,wake_at,waiting_for,joey_ask_json,
	version,created_at,updated_at FROM tasks`

func scanTask(scanner rowScanner) (Task, error) {
	var task Task
	var review, cancel int
	var resume, waitKind, wake, waitingFor, ask sql.NullString
	var created, updated string
	err := scanner.Scan(&task.ID, &task.Title, &task.State, &task.Risk, &review, &cancel,
		&task.MaxRuns, &task.MaxFailures, &task.MaxReviewCycles, &task.BriefPath,
		&task.BriefSHA256, &task.ProjectionPath, &resume, &waitKind, &wake,
		&waitingFor, &ask, &task.Version, &created, &updated)
	if err != nil {
		return Task{}, err
	}
	task.ReviewRequired = review != 0
	task.CancelRequested = cancel != 0
	task.ResumeState = State(resume.String)
	task.WaitKind = WaitKind(waitKind.String)
	task.WaitingFor = waitingFor.String
	task.JoeyAskJSON = ask.String
	if wake.Valid {
		parsed, err := time.Parse(time.RFC3339Nano, wake.String)
		if err != nil {
			return Task{}, fmt.Errorf("finite: invalid wake timestamp: %w", err)
		}
		task.WakeAt = &parsed
	}
	if task.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil {
		return Task{}, err
	}
	if task.UpdatedAt, err = time.Parse(time.RFC3339Nano, updated); err != nil {
		return Task{}, err
	}
	return task, nil
}

const runSelect = `SELECT id,task_id,phase,task_version,seat,session_key,launch_reason,prompt_hash,run_dir,
	supervisor_pid,process_token,created_at,started_at,ended_at,exit_code,outcome,error FROM runs`

func scanRun(scanner rowScanner) (Run, error) {
	var run Run
	var prompt, token, started, ended, outcome, runError sql.NullString
	var pid, exit sql.NullInt64
	var created string
	err := scanner.Scan(&run.ID, &run.TaskID, &run.Phase, &run.TaskVersion, &run.Seat,
		&run.SessionKey, &run.LaunchReason, &prompt, &run.RunDir, &pid, &token,
		&created, &started, &ended, &exit, &outcome, &runError)
	if err != nil {
		return Run{}, err
	}
	run.PromptHash = prompt.String
	run.ProcessToken = token.String
	run.Error = runError.String
	run.Outcome = RunOutcome(outcome.String)
	if pid.Valid {
		value := int(pid.Int64)
		run.SupervisorPID = &value
	}
	if exit.Valid {
		value := int(exit.Int64)
		run.ExitCode = &value
	}
	if run.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil {
		return Run{}, err
	}
	if started.Valid {
		value, err := time.Parse(time.RFC3339Nano, started.String)
		if err != nil {
			return Run{}, err
		}
		run.StartedAt = &value
	}
	if ended.Valid {
		value, err := time.Parse(time.RFC3339Nano, ended.String)
		if err != nil {
			return Run{}, err
		}
		run.EndedAt = &value
	}
	return run, nil
}

func (s *Store) GetTask(ctx context.Context, id string) (Task, error) {
	task, err := scanTask(s.db.QueryRowContext(ctx, taskSelect+` WHERE id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Task{}, fmt.Errorf("%w: task %s", ErrNotFound, id)
	}
	return task, err
}

func getTaskTx(ctx context.Context, tx txRunner, id string) (Task, error) {
	task, err := scanTask(tx.QueryRowContext(ctx, taskSelect+` WHERE id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Task{}, fmt.Errorf("%w: task %s", ErrNotFound, id)
	}
	return task, err
}

func (s *Store) GetRun(ctx context.Context, id int64) (Run, error) {
	run, err := scanRun(s.db.QueryRowContext(ctx, runSelect+` WHERE id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Run{}, fmt.Errorf("%w: run %d", ErrNotFound, id)
	}
	return run, err
}

func getRunTx(ctx context.Context, tx txRunner, id int64) (Run, error) {
	run, err := scanRun(tx.QueryRowContext(ctx, runSelect+` WHERE id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Run{}, fmt.Errorf("%w: run %d", ErrNotFound, id)
	}
	return run, err
}

func (s *Store) ListRuns(ctx context.Context, taskID string) ([]Run, error) {
	rows, err := s.db.QueryContext(ctx, runSelect+` WHERE task_id=? ORDER BY id`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var runs []Run
	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

func (s *Store) DerivedAccounting(ctx context.Context, taskID string, phase Phase) (Accounting, error) {
	return derivedAccounting(ctx, s.db, taskID, phase)
}

func derivedAccounting(ctx context.Context, runner txRunner, taskID string, phase Phase) (Accounting, error) {
	var accounting Accounting
	err := runner.QueryRowContext(ctx, `SELECT
		COALESCE(SUM(CASE WHEN outcome IN ('ok','fail','timeout') THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN outcome IN ('fail','timeout') OR
			(outcome='ok' AND NOT EXISTS (SELECT 1 FROM run_results rr WHERE rr.run_id=runs.id))
			THEN 1 ELSE 0 END),0),
		COUNT(*)
		FROM runs WHERE task_id=? AND phase=?`, taskID, phase).Scan(&accounting.Attempts, &accounting.Failures, &accounting.Runs)
	if err != nil {
		return Accounting{}, err
	}
	if err := runner.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_results WHERE task_id=? AND phase='review' AND outcome='rejected'`, taskID).Scan(&accounting.ReviewCycles); err != nil {
		return Accounting{}, err
	}
	return accounting, nil
}

func (s *Store) GetUsage(ctx context.Context, runID int64) (Usage, error) {
	var usage Usage
	var fresh, cacheRead, cacheWrite, output, reasoning, apiCalls, toolCalls sql.NullInt64
	var collected string
	err := s.db.QueryRowContext(ctx, `SELECT fresh_input_tokens,cache_read_tokens,cache_write_tokens,output_tokens,
		reasoning_tokens,api_calls,tool_calls,source,collected_at FROM run_usage WHERE run_id=?`, runID).Scan(
		&fresh, &cacheRead, &cacheWrite, &output, &reasoning, &apiCalls, &toolCalls, &usage.Source, &collected)
	if errors.Is(err, sql.ErrNoRows) {
		return Usage{}, fmt.Errorf("%w: usage for run %d", ErrNotFound, runID)
	}
	if err != nil {
		return Usage{}, err
	}
	usage.FreshInputTokens = nullInt64Pointer(fresh)
	usage.CacheReadTokens = nullInt64Pointer(cacheRead)
	usage.CacheWriteTokens = nullInt64Pointer(cacheWrite)
	usage.OutputTokens = nullInt64Pointer(output)
	usage.ReasoningTokens = nullInt64Pointer(reasoning)
	usage.APICalls = nullInt64Pointer(apiCalls)
	usage.ToolCalls = nullInt64Pointer(toolCalls)
	usage.CollectedAt, err = time.Parse(time.RFC3339Nano, collected)
	return usage, err
}

func nullInt64Pointer(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	result := value.Int64
	return &result
}

func (s *Store) ListOutbox(ctx context.Context) ([]OutboxEntry, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT delivery_id,event_id,channel,kind,payload_json,created_at,available_at,
		send_count,delivered_at,last_error FROM outbox ORDER BY created_at,delivery_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var entries []OutboxEntry
	for rows.Next() {
		var entry OutboxEntry
		var created, available string
		var delivered, lastError sql.NullString
		if err := rows.Scan(&entry.DeliveryID, &entry.EventID, &entry.Channel, &entry.Kind,
			&entry.PayloadJSON, &created, &available, &entry.SendCount, &delivered, &lastError); err != nil {
			return nil, err
		}
		var err error
		if entry.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil {
			return nil, err
		}
		if entry.AvailableAt, err = time.Parse(time.RFC3339Nano, available); err != nil {
			return nil, err
		}
		if delivered.Valid {
			value, err := time.Parse(time.RFC3339Nano, delivered.String)
			if err != nil {
				return nil, err
			}
			entry.DeliveredAt = &value
		}
		entry.LastError = lastError.String
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

// CheckInvariants verifies database and active/open-run consistency.
func (s *Store) CheckInvariants(ctx context.Context) error {
	var integrity string
	if err := s.db.QueryRowContext(ctx, `PRAGMA integrity_check`).Scan(&integrity); err != nil || integrity != "ok" {
		return fmt.Errorf("finite: SQLite integrity = %q, %v", integrity, err)
	}
	rows, err := s.db.QueryContext(ctx, `PRAGMA foreign_key_check`)
	if err != nil {
		return err
	}
	if rows.Next() {
		rows.Close()
		return fmt.Errorf("finite: foreign key check failed")
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	rows, err = s.db.QueryContext(ctx, `SELECT t.id,t.state,COUNT(r.id),
		COALESCE(SUM(CASE WHEN r.phase='implementation' THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN r.phase='review' THEN 1 ELSE 0 END),0)
		FROM tasks t LEFT JOIN runs r ON r.task_id=t.id AND r.ended_at IS NULL
		GROUP BY t.id,t.state`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var state State
		var open, implementation, review int
		if err := rows.Scan(&id, &state, &open, &implementation, &review); err != nil {
			return err
		}
		valid := (state == StateWorking && open == 1 && implementation == 1) ||
			(state == StateReviewing && open == 1 && review == 1) ||
			(state != StateWorking && state != StateReviewing && open == 0)
		if !valid {
			return fmt.Errorf("finite: task %s state %s has %d open runs", id, state, open)
		}
	}
	return rows.Err()
}

func stamp(value time.Time) string { return value.UTC().Format(time.RFC3339Nano) }

func nullString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
