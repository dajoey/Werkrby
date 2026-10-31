package finite

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/agent0ai/spynel/internal/harness"
)

var errEngineClosed = errors.New("finite: engine is closed")

// EngineOptions supplies the owner-side provider and scheduling dependencies.
type EngineOptions struct {
	Harness     harness.HarnessConfig
	Seats       func(context.Context) ([]Seat, error)
	Executable  string
	PollInterval time.Duration
	StopGrace   time.Duration
	Domains     []string
	OnError     func(error)
	hooks       engineHooks
}

type engineHooks struct {
	afterDispatchCommit func(Run)
	beforeStart         func(Run)
	afterSpawn          func(Run, ProcessIdentity)
	afterPIDPersist     func(Run, ProcessIdentity)
	beforeSettlement    func(SettlementRequest)
	afterSettlement     func(SettlementRequest)
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
}

// NewEngine acquires the owner lock before opening SQLite.
func NewEngine(cfg Config, opts EngineOptions) (*Engine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	lock, err := AcquireWorkspaceLock(cfg.Root)
	if err != nil {
		return nil, err
	}
	store, err := OpenStore(cfg)
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Engine{cfg: cfg, opts: opts, store: store, lock: lock, ctx: ctx, cancel: cancel, wake: make(chan struct{}, 1)}, nil
}

// ScanOnce performs one owner scheduling and recovery pass.
func (e *Engine) ScanOnce(ctx context.Context) error {
	e.scanMu.Lock()
	defer e.scanMu.Unlock()
	e.closeMu.Lock()
	closed := e.closed
	e.closeMu.Unlock()
	if closed {
		return errEngineClosed
	}
	return nil
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
	task, err := e.store.RequestStop(ctx, taskID, expectedVersion)
	if err == nil {
		err = e.store.ProjectTask(ctx, taskID)
	}
	return task, err
}

// WakeTime completes a due time wait without dispatching in the transaction.
func (e *Engine) WakeTime(ctx context.Context, taskID string, expectedVersion int64) (Task, error) {
	task, err := e.store.WakeTime(ctx, taskID, expectedVersion)
	if err == nil {
		err = e.store.ProjectTask(ctx, taskID)
	}
	return task, err
}

// AnswerJoey completes a matching human-answer wait.
func (e *Engine) AnswerJoey(ctx context.Context, taskID string, expectedVersion int64, optionID string) (Task, error) {
	task, err := e.store.AnswerJoey(ctx, taskID, expectedVersion, optionID)
	if err == nil {
		err = e.store.ProjectTask(ctx, taskID)
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
	return errors.Join(e.store.Close(), e.lock.Close())
}
