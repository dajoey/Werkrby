package finite

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/agent0ai/spynel/internal/harness"
)

var (
	errEngineClosed = errors.New("finite: engine is closed")
	errStopPending  = errors.New("finite: stop fence is pending")
)

// EngineOptions supplies the owner-side provider and scheduling dependencies.
type EngineOptions struct {
	Harness      harness.HarnessConfig
	Seats        func(context.Context) ([]Seat, error)
	Executable   string
	PollInterval time.Duration
	StopGrace    time.Duration
	Domains      []string
	OnError      func(error)
	hooks        engineHooks
}

type engineHooks struct {
	afterDispatchCommit func(Run)
	beforeStart         func(Run)
	afterSpawn          func(Run, ProcessIdentity)
	afterPIDPersist     func(Run, ProcessIdentity)
	beforeSettlement    func(SettlementRequest)
	afterSettlement     func(SettlementRequest)
	beforeWake          func(Task)
	afterWake           func(Task)
	inspectRun          func(context.Context, int64, string) (RunProcesses, error)
	stopRun             func(context.Context, int64, string, time.Duration) (Liveness, error)
}

// Engine is the single SQLite-writing owner for one disposable workspace.
type Engine struct {
	cfg    Config
	opts   EngineOptions
	store  *Store
	lock   *WorkspaceLock
	ctx    context.Context
	cancel context.CancelFunc
	wake   chan struct{}

	scanMu  sync.Mutex
	closeMu sync.Mutex
	closed  bool

	monitorMu sync.Mutex
	monitors  map[int64]struct{}
}

// NewEngine acquires the owner lock before opening SQLite.
func NewEngine(cfg Config, opts EngineOptions) (*Engine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	root, err := filepath.Abs(cfg.Root)
	if err != nil {
		return nil, err
	}
	worker, err := filepath.Abs(cfg.WorkerDir)
	if err != nil {
		return nil, err
	}
	cfg.Root = filepath.Clean(root)
	cfg.WorkerDir = filepath.Clean(worker)
	lock, err := AcquireWorkspaceLock(cfg.Root)
	if err != nil {
		return nil, err
	}
	store, err := OpenStore(cfg)
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	if opts.Harness.Cwd == "" {
		opts.Harness.Cwd = cfg.WorkerDir
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Engine{
		cfg: cfg, opts: opts, store: store, lock: lock, ctx: ctx, cancel: cancel,
		wake: make(chan struct{}, 1), monitors: make(map[int64]struct{}),
	}, nil
}

// ScanOnce performs one owner scheduling and recovery pass.
func (e *Engine) ScanOnce(ctx context.Context) error {
	e.scanMu.Lock()
	defer e.scanMu.Unlock()
	if err := e.checkOpen(ctx); err != nil {
		return err
	}
	if !SupportsProcessBoundary() {
		return ErrUnsupportedPlatform
	}
	var errs []error
	if err := e.recoverOpenRuns(ctx); err != nil {
		errs = append(errs, err)
	}
	tasks, err := e.listTasks(ctx)
	if err != nil {
		return errors.Join(append(errs, err)...)
	}
	for index := range tasks {
		task := tasks[index]
		launchReason := ""
		if task.State == StateWaiting && task.WaitKind == WaitTime && task.WakeAt != nil && !e.store.now().UTC().Before(task.WakeAt.UTC()) {
			if e.opts.hooks.beforeWake != nil {
				e.opts.hooks.beforeWake(task)
			}
			updated, wakeErr := e.store.WakeTime(ctx, task.ID, task.Version)
			if wakeErr != nil {
				errs = append(errs, wakeErr)
				continue
			}
			if e.opts.hooks.afterWake != nil {
				e.opts.hooks.afterWake(updated)
			}
			task = updated
			tasks[index] = task
			launchReason = "wake"
		}
		if task.State != StateTodo && task.State != StateReview {
			continue
		}
		if err := e.dispatchTask(ctx, task, launchReason); err != nil && !errors.Is(err, errStopPending) {
			errs = append(errs, err)
		}
	}
	for _, task := range tasks {
		if err := e.store.ProjectTask(ctx, task.ID); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (e *Engine) checkOpen(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.closeMu.Lock()
	defer e.closeMu.Unlock()
	if e.closed {
		return errEngineClosed
	}
	return nil
}

func (e *Engine) listTasks(ctx context.Context) ([]Task, error) {
	rows, err := e.store.db.QueryContext(ctx, taskSelect+` ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []Task
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

func (e *Engine) dispatchTask(ctx context.Context, task Task, launchReason string) error {
	if task.CancelRequested {
		return nil
	}
	phase := PhaseImplementation
	if task.State == StateReview {
		phase = PhaseReview
	}
	accounting, err := e.store.DerivedAccounting(ctx, task.ID, phase)
	if err != nil {
		return err
	}
	if e.opts.Seats == nil {
		return fmt.Errorf("finite: no seat provider configured")
	}
	seats, err := e.opts.Seats(ctx)
	if err != nil {
		return fmt.Errorf("finite: inspect provider seats: %w", err)
	}
	decision := Admit(AdmissionInput{Task: task, Phase: phase, Accounting: accounting, Seats: seats})
	if decision.Action == AdmissionFail {
		_, err := e.store.FailTask(ctx, task.ID, task.Version, decision.Reason)
		return err
	}
	if decision.Action != AdmissionDispatch {
		return nil
	}
	brief, err := os.ReadFile(task.BriefPath)
	if err != nil {
		return fmt.Errorf("finite: read immutable brief for dispatch: %w", err)
	}
	if len(brief) == 0 || len(brief) > maxBriefBytes {
		return fmt.Errorf("finite: immutable brief size changed outside intake")
	}
	if digest := fmt.Sprintf("%x", sha256.Sum256(brief)); digest != task.BriefSHA256 {
		return fmt.Errorf("finite: immutable brief SHA-256 mismatch")
	}
	sessionKey := fmt.Sprintf("finite:%s:%s", task.ID, phase)
	dynamic := fmt.Sprintf("TASK BRIEF\ntitle: %s\n\n%s", task.Title, brief)
	history, err := e.store.TaskHistory(ctx, task.ID)
	if err != nil {
		return fmt.Errorf("finite: read task history for dispatch: %w", err)
	}
	if history != "" {
		dynamic += "\n\nTASK HISTORY (earlier runs of this task, oldest first; act on Joey's answers and fix every review finding)\n" + history + "\n"
	}
	promptRequest := PromptRequest{
		TaskID: task.ID, RunID: 1, Phase: phase, Domains: e.opts.Domains,
		Dynamic: dynamic,
	}
	preliminary, err := BuildPrompt(promptRequest)
	if err != nil {
		return err
	}
	token, err := NewProcessToken()
	if err != nil {
		return err
	}
	if launchReason == "" {
		launchReason = "initial"
		if phase == PhaseReview {
			launchReason = "review"
		} else {
			runs, listErr := e.store.ListRuns(ctx, task.ID)
			if listErr != nil {
				return listErr
			}
			if len(runs) != 0 {
				launchReason = "retry"
			}
		}
	}
	_, err = e.store.Dispatch(ctx, DispatchRequest{
		TaskID: task.ID, ExpectedVersion: task.Version, Phase: phase, Seat: decision.Seat,
		SessionKey: sessionKey, LaunchReason: launchReason, PromptHash: preliminary.BundleHash,
	}, func(launchCtx context.Context, run Run) error {
		if e.opts.hooks.afterDispatchCommit != nil {
			e.opts.hooks.afterDispatchCommit(run)
		}
		promptRequest.RunID = run.ID
		promptRequest.RunDir = run.RunDir
		prompt, buildErr := BuildPrompt(promptRequest)
		if buildErr != nil {
			return buildErr
		}
		if prompt.BundleHash != preliminary.BundleHash {
			return fmt.Errorf("finite: prompt bundle changed during dispatch")
		}
		if err := e.prepareRun(launchCtx, run, token, prompt); err != nil {
			return err
		}
		run.ProcessToken = token
		run.PromptHash = prompt.BundleHash
		if e.opts.hooks.beforeStart != nil {
			e.opts.hooks.beforeStart(run)
		}
		current, getErr := e.store.GetTask(launchCtx, task.ID)
		if getErr != nil {
			return getErr
		}
		if current.CancelRequested {
			return errStopPending
		}
		handle, startErr := StartSupervisor(launchCtx, SupervisorSpec{
			RunID: run.ID, RunDir: run.RunDir, ProcessToken: token, Executable: e.opts.Executable,
			SessionKey: sessionKey, Prompt: prompt.Text, Harness: e.opts.Harness,
		})
		if startErr != nil {
			return startErr
		}
		if e.opts.hooks.afterSpawn != nil {
			e.opts.hooks.afterSpawn(run, handle.Identity)
		}
		if err := e.persistStarted(launchCtx, run, handle.Identity); err != nil {
			e.watchSupervisor(run.ID, handle)
			return err
		}
		if e.opts.hooks.afterPIDPersist != nil {
			e.opts.hooks.afterPIDPersist(run, handle.Identity)
		}
		e.watchSupervisor(run.ID, handle)
		return nil
	})
	if errors.Is(err, errStopPending) {
		e.signalWake()
	}
	return err
}

func (e *Engine) watchSupervisor(runID int64, handle *SupervisorHandle) {
	if !e.beginMonitor(runID) {
		return
	}
	go func() {
		defer e.endMonitor(runID)
		_ = handle.Wait()
		e.signalWake()
	}()
}

func (e *Engine) watchAdopted(runID int64, adopted *AdoptedRun) {
	if !e.beginMonitor(runID) {
		return
	}
	go func() {
		defer e.endMonitor(runID)
		_ = adopted.Wait(e.ctx)
		e.signalWake()
	}()
}

func (e *Engine) beginMonitor(runID int64) bool {
	e.monitorMu.Lock()
	defer e.monitorMu.Unlock()
	if _, exists := e.monitors[runID]; exists {
		return false
	}
	e.monitors[runID] = struct{}{}
	return true
}

func (e *Engine) endMonitor(runID int64) {
	e.monitorMu.Lock()
	delete(e.monitors, runID)
	e.monitorMu.Unlock()
}

func (e *Engine) signalWake() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

// Run performs bounded scans until cancellation.
func (e *Engine) Run(ctx context.Context) error {
	interval := e.opts.PollInterval
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	for {
		if err := e.ScanOnce(ctx); err != nil && !errors.Is(err, errEngineClosed) && e.opts.OnError != nil {
			e.opts.OnError(err)
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-e.ctx.Done():
			timer.Stop()
			return nil
		case <-e.wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// RequestStop commits the durable stop fence and projects that transition.
func (e *Engine) RequestStop(ctx context.Context, taskID string, expectedVersion int64) (Task, error) {
	e.scanMu.Lock()
	defer e.scanMu.Unlock()
	if err := e.checkOpen(ctx); err != nil {
		return Task{}, err
	}
	task, err := e.store.RequestStop(ctx, taskID, expectedVersion)
	if err == nil {
		err = e.store.ProjectTask(ctx, taskID)
		e.signalWake()
	}
	return task, err
}

// WakeTime completes a due time wait without dispatching in the transaction.
func (e *Engine) WakeTime(ctx context.Context, taskID string, expectedVersion int64) (Task, error) {
	e.scanMu.Lock()
	defer e.scanMu.Unlock()
	if err := e.checkOpen(ctx); err != nil {
		return Task{}, err
	}
	task, err := e.store.WakeTime(ctx, taskID, expectedVersion)
	if err == nil {
		err = e.store.ProjectTask(ctx, taskID)
		e.signalWake()
	}
	return task, err
}

// AnswerJoey completes a matching human-answer wait.
func (e *Engine) AnswerJoey(ctx context.Context, taskID string, expectedVersion int64, optionID string) (Task, error) {
	e.scanMu.Lock()
	defer e.scanMu.Unlock()
	if err := e.checkOpen(ctx); err != nil {
		return Task{}, err
	}
	task, err := e.store.AnswerJoey(ctx, taskID, expectedVersion, optionID)
	if err == nil {
		err = e.store.ProjectTask(ctx, taskID)
		e.signalWake()
	}
	return task, err
}

// Close stops owner-side work, closes SQLite, and releases the owner lock.
func (e *Engine) Close() error {
	if e == nil {
		return nil
	}
	e.closeMu.Lock()
	if e.closed {
		e.closeMu.Unlock()
		return nil
	}
	e.closed = true
	e.cancel()
	e.closeMu.Unlock()
	e.scanMu.Lock()
	defer e.scanMu.Unlock()
	return errors.Join(e.store.Close(), e.lock.Close())
}
