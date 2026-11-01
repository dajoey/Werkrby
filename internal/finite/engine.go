package finite

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	MaxOpenRuns  int
	OnError      func(error)
	// RosterSeats reports that Seats come from a fallback roster, so
	// admission walks each task's staff chain instead of taking any seat.
	RosterSeats bool
	hooks       engineHooks
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
		if launchReason == "" && e.opts.RosterSeats && task.State == StateWaiting && task.WaitKind == WaitTime &&
			task.WakeAt != nil && e.store.now().UTC().Before(task.WakeAt.UTC()) &&
			e.latestEndedRunIsWall(ctx, task.ID) {
			// A quota wall is not a reason to sleep while a fallback seat can
			// run the work: wake early, but never above the open-run cap.
			room := true
			if e.opts.MaxOpenRuns > 0 {
				open, countErr := e.store.CountOpenRuns(ctx)
				if countErr != nil {
					errs = append(errs, countErr)
					room = false
				} else {
					room = open < e.opts.MaxOpenRuns
				}
			}
			if room {
				updated, woken, wakeErr := e.tryFallbackWake(ctx, task)
				if wakeErr != nil {
					errs = append(errs, wakeErr)
				} else if woken {
					task = updated
					tasks[index] = task
					launchReason = "wake"
				}
			}
		}
		if task.State != StateTodo && task.State != StateReview {
			continue
		}
		if e.opts.MaxOpenRuns > 0 {
			open, countErr := e.store.CountOpenRuns(ctx)
			if countErr != nil {
				errs = append(errs, countErr)
				break
			}
			if open >= e.opts.MaxOpenRuns {
				continue // queued until a run ends; woken tasks stay todo
			}
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
	// The brief is proven immutable before any admission work, so a tampered
	// brief fails its task once whatever the seats say.
	brief, parsedBrief, err := e.verifiedBrief(ctx, task)
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
	startSeat, walledPools, err := e.walkInputs(ctx, task, phase, parsedBrief, seats)
	if err != nil {
		return err
	}
	decision := Admit(AdmissionInput{Task: task, Phase: phase, Accounting: accounting, Seats: seats,
		RosterSeats: e.opts.RosterSeats, StartSeat: startSeat, WalledPools: walledPools})
	if decision.Action == AdmissionFail {
		_, err := e.store.FailTask(ctx, task.ID, task.Version, decision.Reason)
		return err
	}
	if decision.Action != AdmissionDispatch {
		return nil
	}
	seatModel := ""
	if e.opts.RosterSeats {
		seatModel = decision.Seat
	}
	runHarness := harnessForPhase(e.opts.Harness, parsedBrief, phase, seatModel)
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
			SessionKey: sessionKey, Prompt: prompt.Text, Harness: runHarness,
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

// briefChangedReason is the terminal failure recorded when a committed brief
// no longer matches the bytes hashed at intake.
const briefChangedReason = "immutable brief changed after intake"

// verifiedBrief reads the immutable brief, proves it still hashes to the
// intake digest, and parses it. A mismatch fails the task once via
// failChangedBrief; a parse failure is an engine error naming the cause.
func (e *Engine) verifiedBrief(ctx context.Context, task Task) ([]byte, Brief, error) {
	brief, readErr := os.ReadFile(task.BriefPath)
	var digest string
	if readErr == nil {
		digest = fmt.Sprintf("%x", sha256.Sum256(brief))
	}
	if readErr != nil || len(brief) == 0 || len(brief) > maxBriefBytes || digest != task.BriefSHA256 {
		return nil, Brief{}, e.failChangedBrief(ctx, task, readErr, digest)
	}
	parsed, err := ParseBrief(brief)
	if err != nil {
		return nil, Brief{}, fmt.Errorf("finite: parse immutable brief for dispatch: %w", err)
	}
	return brief, parsed, nil
}

// walkInputs derives the roster-walk origin and the pools this task's own
// quota walls still wall. Slot mode (no roster) returns zero values and keeps
// today's any-seat admission.
func (e *Engine) walkInputs(ctx context.Context, task Task, phase Phase, brief Brief, seats []Seat) (string, map[string]struct{}, error) {
	if !e.opts.RosterSeats {
		return "", nil, nil
	}
	start := brief.Staff
	if phase == PhaseReview && brief.ReviewStaff != "" {
		start = brief.ReviewStaff
	}
	if start == "" {
		start = strings.TrimPrefix(e.opts.Harness.Model, "@")
	}
	deadlines, err := e.store.wallDeadlinesBySeat(ctx, task.ID)
	if err != nil {
		return "", nil, err
	}
	walled := make(map[string]struct{})
	if len(deadlines) != 0 {
		now := e.store.now().UTC()
		poolOf := make(map[string]string, len(seats))
		for _, seat := range seats {
			poolOf[seat.Name] = seat.Pool
		}
		for seatName, deadline := range deadlines {
			pool := poolOf[seatName]
			if pool == "" || !deadline.After(now) {
				continue // unknown seat, unmetered, or the wall has lifted
			}
			walled[pool] = struct{}{}
		}
	}
	return start, walled, nil
}

// latestEndedRunIsWall reports whether the task's most recent settled run
// ended in a provider quota wall.
func (e *Engine) latestEndedRunIsWall(ctx context.Context, taskID string) bool {
	var outcome RunOutcome
	err := e.store.db.QueryRowContext(ctx,
		`SELECT outcome FROM runs WHERE task_id=? AND ended_at IS NOT NULL ORDER BY id DESC LIMIT 1`, taskID).Scan(&outcome)
	return err == nil && outcome == RunWall
}

// tryFallbackWake completes a quota-wall wait early when the fallback walk
// finds a seat that can run the work now. Errors never wake; the next scan
// retries, and a brief problem surfaces when the wait is due.
func (e *Engine) tryFallbackWake(ctx context.Context, task Task) (Task, bool, error) {
	probe := task
	probe.State = task.ResumeState
	if probe.State != StateTodo && probe.State != StateReview {
		return Task{}, false, nil
	}
	phase := PhaseImplementation
	if probe.State == StateReview {
		phase = PhaseReview
	}
	_, parsedBrief, err := e.verifiedBrief(ctx, task)
	if err != nil {
		return Task{}, false, nil
	}
	seats, err := e.opts.Seats(ctx)
	if err != nil {
		return Task{}, false, nil
	}
	startSeat, walledPools, err := e.walkInputs(ctx, task, phase, parsedBrief, seats)
	if err != nil {
		return Task{}, false, nil
	}
	accounting, err := e.store.DerivedAccounting(ctx, task.ID, phase)
	if err != nil {
		return Task{}, false, nil
	}
	decision := Admit(AdmissionInput{Task: probe, Phase: phase, Accounting: accounting, Seats: seats,
		RosterSeats: e.opts.RosterSeats, StartSeat: startSeat, WalledPools: walledPools})
	if decision.Action != AdmissionDispatch {
		return Task{}, false, nil
	}
	if e.opts.hooks.beforeWake != nil {
		e.opts.hooks.beforeWake(task)
	}
	updated, err := e.store.WakeFallback(ctx, task.ID, task.Version)
	if err != nil {
		// A version or state conflict is a race with another writer; the
		// next scan re-evaluates the wait.
		return Task{}, false, nil
	}
	if e.opts.hooks.afterWake != nil {
		e.opts.hooks.afterWake(updated)
	}
	return updated, true, nil
}

// failChangedBrief settles a task whose immutable brief no longer matches the
// hash recorded at intake. Refusing to dispatch would wedge the task in its
// queue while every scan logs an anonymous error, so the task is failed once —
// the bridge then moves its doc to failed/ — and the returned error names the
// task so the serve log shows which work stopped.
func (e *Engine) failChangedBrief(ctx context.Context, task Task, readErr error, digest string) error {
	detail := fmt.Sprintf("sha256 %s, want %s", digest, task.BriefSHA256)
	if readErr != nil {
		detail = fmt.Sprintf("unreadable: %v", readErr)
	}
	if _, err := e.store.FailTask(ctx, task.ID, task.Version, briefChangedReason); err != nil {
		return fmt.Errorf("finite: task %s: %s (%s): fail task: %w", task.ID, briefChangedReason, detail, err)
	}
	return fmt.Errorf("finite: task %s: %s (%s); task failed once", task.ID, briefChangedReason, detail)
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
