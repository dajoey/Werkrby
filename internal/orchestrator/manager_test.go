package orchestrator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/workspace"
)

type fakeHarness struct {
	mu         sync.Mutex
	calls      int
	keys       []string
	prompts    []string
	threads    map[string]string
	active     map[string]bool
	beforeEmit func()
	events     []core.Event
	sendErrs   []error // consumed one per Send call; exhausted or nil entry = success
	// Real harnesses emit their own terminal event immediately before returning
	// a send error: an error for a refused turn, a final for a turn that
	// completed and then failed on the way back.
	sendErrFinal bool
}

func newFakeRecipient() *fakeHarness {
	return &fakeHarness{threads: map[string]string{}, active: map[string]bool{}}
}

func (f *fakeHarness) Start(context.Context) error { return nil }
func (f *fakeHarness) Close() error                { return nil }
func (f *fakeHarness) ResetSession(key string) error {
	delete(f.threads, key)
	return nil
}
func (f *fakeHarness) ThreadID(key string) string { return f.threads[key] }
func (f *fakeHarness) IsActive(key string) bool   { return f.active[key] }
func (f *fakeHarness) Interrupt(_ context.Context, key string) (bool, error) {
	if !f.active[key] {
		return false, nil
	}
	f.active[key] = false
	return true, nil
}
func (f *fakeHarness) Send(_ context.Context, key, prompt string, emit core.Emit) (string, bool, error) {
	f.mu.Lock()
	f.calls++
	f.keys = append(f.keys, key)
	f.prompts = append(f.prompts, prompt)
	var sendErr error
	if len(f.sendErrs) > 0 {
		sendErr = f.sendErrs[0]
		f.sendErrs = f.sendErrs[1:]
	}
	thread := f.threads[key]
	if thread == "" {
		thread = "thread-" + key
		f.threads[key] = thread
	}
	f.mu.Unlock()
	if sendErr != nil {
		if emit != nil {
			event := core.Event{Kind: core.EventError, Text: sendErr.Error(), ThreadID: thread, Done: true}
			if f.sendErrFinal {
				final := "done"
				event = core.Event{Kind: core.EventFinal, Text: final, FinalText: &final, ThreadID: thread, Done: true}
			}
			emit(event)
		}
		return "", false, sendErr
	}
	if f.beforeEmit != nil && !strings.HasPrefix(key, "orchestrator:notification:") {
		f.beforeEmit()
	}
	if f.events != nil {
		for _, event := range f.events {
			event.ThreadID = thread
			emit(event)
		}
	} else {
		emit(core.Event{Kind: core.EventFinal, Text: "done", ThreadID: thread, Done: true})
	}
	return thread, false, nil
}

func TestLiveParallelLimitRaisesPromptlyAndLowersWithoutCancelling(t *testing.T) {
	cfg := config.Default()
	cfg.Orchestrator.MaxParallel = 1
	manager := New(cfg, newFakeRecipient(), extensions.Runner{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if !manager.acquireCapacity(ctx, false) {
		t.Fatal("initial capacity was unavailable")
	}
	acquired := make(chan struct{})
	go func() {
		if manager.acquireCapacity(ctx, false) {
			close(acquired)
		}
	}()
	select {
	case <-acquired:
		t.Fatal("limit one admitted overlapping work")
	case <-time.After(25 * time.Millisecond):
	}
	cfg.Orchestrator.MaxParallel = 2
	manager.ApplyRuntimeConfig(cfg)
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("raised limit did not admit waiting work promptly")
	}
	cfg.Orchestrator.MaxParallel = 1
	manager.ApplyRuntimeConfig(cfg)
	third := make(chan struct{})
	go func() {
		if manager.acquireCapacity(ctx, false) {
			close(third)
		}
	}()
	manager.releaseCapacity(false)
	select {
	case <-third:
		t.Fatal("lowered limit admitted work while active count remained at the bound")
	case <-time.After(25 * time.Millisecond):
	}
	manager.releaseCapacity(false)
	select {
	case <-third:
	case <-time.After(time.Second):
		t.Fatal("waiting work was not admitted after active work drained below the new bound")
	}
	manager.releaseCapacity(false)
}

func TestLiveScanIntervalResetsRunningSchedulerFromAcceptedChange(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Orchestrator.IntervalSec = 3600
	manager := New(cfg, newFakeRecipient(), extensions.Runner{})
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- manager.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-runDone
	})
	deadline := time.Now().Add(time.Second)
	for {
		manager.scanTimerMu.Lock()
		running := manager.scanTimer != nil
		manager.scanTimerMu.Unlock()
		if running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("route scheduler did not start")
		}
		time.Sleep(time.Millisecond)
	}
	cfg.Orchestrator.IntervalSec = 2
	before := time.Now()
	manager.ApplyRuntimeConfig(cfg)
	after := time.Now()
	manager.scanTimerMu.Lock()
	next := manager.scanNext
	manager.scanTimerMu.Unlock()
	if next.Before(before.Add(2*time.Second)) || next.After(after.Add(2*time.Second)) {
		t.Fatalf("next scan deadline %s was not measured from acceptance window [%s, %s]", next, before.Add(2*time.Second), after.Add(2*time.Second))
	}
	if got := manager.scanInterval(); got != 2*time.Second {
		t.Fatalf("live scan interval = %s", got)
	}
}

func TestImplementationDoneMoveIsReconciledIntoIndependentReview(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(config.PathForRoot(root))
	task, err := Create(cfg, "tasks", "move and release", "")
	if err != nil {
		t.Fatal(err)
	}
	working := filepath.Join(cfg.Resolve(workflowRoutes()[0].Working), filepath.Base(task))
	done := filepath.Join(filepath.Dir(cfg.Resolve(workflowRoutes()[0].Source)), "done", filepath.Base(task))
	fake := newFakeRecipient()
	var manager *Manager
	var moved sync.Once
	fake.beforeEmit = func() {
		moved.Do(func() {
			if err := os.Rename(working, done); err != nil {
				t.Errorf("move completed task: %v", err)
			}
			if err := manager.recoverStale(context.Background()); err != nil {
				t.Errorf("recover after completed move: %v", err)
			}
		})
	}
	manager = New(cfg, fake, extensions.Runner{Directory: filepath.Join(root, "missing")})
	started := 0
	finished := 0
	var attempts []int
	manager.JobStarted = func(lease Lease, description string, _ time.Time, _ int, implementationAttempts int) (int, error) {
		if lease.SessionKey == "" || description == "" {
			t.Errorf("empty orchestrator job metadata: %q %q", lease.SessionKey, description)
		}
		attempts = append(attempts, implementationAttempts)
		started++
		return 7, nil
	}
	manager.JobFinished = func(id int) {
		if id != 7 {
			t.Errorf("finished job ID = %d, want 7", id)
		}
		finished++
	}
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	leases, err := manager.loadLeases()
	if err != nil || len(leases) != 1 || leases[0].Phase != phaseTaskReview {
		t.Fatalf("task did not enter review phase: %#v, %v", leases, err)
	}
	reviewing := filepath.Join(filepath.Dir(cfg.Resolve(workflowRoutes()[0].Source)), "reviewing", filepath.Base(task))
	if _, err := os.Stat(reviewing); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(done); !os.IsNotExist(err) {
		t.Fatalf("unreviewed task remained done: %v", err)
	}
	if started != 2 || finished != 1 {
		t.Fatalf("job lifecycle callbacks = %d started, %d finished", started, finished)
	}
	if !reflect.DeepEqual(attempts, []int{1, 1}) {
		t.Fatalf("implementation-attempt snapshots = %v, want [1 1]", attempts)
	}
	claimed, err := ReadDocument(reviewing)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.FrontMatter["attempt"] != 1 || claimed.FrontMatter["review_attempt"] != 1 {
		t.Fatalf("review claim counters = %#v, want one implementation and one review", claimed.FrontMatter)
	}
}

func TestProviderCompletionRemainsAwaitingTransitionUntilDurableMove(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(config.PathForRoot(root))
	task, err := Create(cfg, "tasks", "await durable move", "")
	if err != nil {
		t.Fatal(err)
	}
	fake := newFakeRecipient()
	fake.events = []core.Event{
		{Kind: core.EventStatus, Execution: &core.ExecutionStatus{State: "reconnecting", ReconnectAttempt: 1}},
		{Kind: core.EventStatus, Execution: &core.ExecutionStatus{State: "running"}},
		{Kind: core.EventFinal, Text: "done", Done: true, Execution: &core.ExecutionStatus{State: "finishing"}},
	}
	manager := New(cfg, fake, extensions.Runner{Directory: filepath.Join(root, "missing")})
	started, finished := 0, 0
	var states []string
	var providerStates []string
	manager.JobStarted = func(lease Lease, _ string, _ time.Time, _, _ int) (int, error) {
		if lease.DocumentType == "notification" {
			return 12, nil
		}
		started++
		return 11, nil
	}
	manager.JobUpdated = func(_ int, lease Lease) { states = append(states, lease.State) }
	manager.JobExecutionUpdated = func(_ int, status core.ExecutionStatus) { providerStates = append(providerStates, status.State) }
	manager.JobFinished = func(id int) {
		if id == 12 {
			return
		}
		if id != 11 {
			t.Errorf("finished ID=%d", id)
		}
		finished++
	}
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	working := filepath.Join(cfg.Resolve(workflowRoutes()[0].Working), filepath.Base(task))
	if started != 1 || finished != 0 || len(states) == 0 || states[len(states)-1] != "awaiting_transition" || !reflect.DeepEqual(providerStates, []string{"reconnecting", "running"}) {
		t.Fatalf("before transition: started=%d finished=%d lease=%v provider=%v", started, finished, states, providerStates)
	}
	waiting := filepath.Join(filepath.Dir(cfg.Resolve(workflowRoutes()[0].Source)), "waiting", filepath.Base(task))
	if err := moveDocument(working, waiting, "waiting", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if finished != 1 {
		t.Fatalf("durable reconciliation finished jobs %d times", finished)
	}
}

func TestClaimLeasePreventsDuplicatesAndStaleLeaseRecovers(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	task, err := Create(cfg, "tasks", "verify lease behavior", "")
	if err != nil {
		t.Fatal(err)
	}
	fake := newFakeRecipient()
	manager := New(cfg, fake, extensions.Runner{Directory: filepath.Join(root, "missing")})
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	working := filepath.Join(cfg.Resolve(workflowRoutes()[0].Working), filepath.Base(task))
	if _, err := os.Stat(working); err != nil {
		t.Fatalf("task was not claimed into working: %v", err)
	}
	if fake.calls != 1 {
		t.Fatalf("dispatch calls = %d, want 1", fake.calls)
	}
	document, err := ReadDocument(working)
	if err != nil {
		t.Fatal(err)
	}
	firstAssigned, iterations, ok := DurableTiming(document)
	if !ok || firstAssigned.IsZero() || iterations != 1 {
		t.Fatalf("initial durable timing = %s %d %t", firstAssigned, iterations, ok)
	}
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if fake.calls != 1 {
		t.Fatalf("leased task was duplicated: %d calls", fake.calls)
	}
	leases, err := manager.loadLeases()
	if err != nil || len(leases) != 1 {
		t.Fatalf("leases = %#v, %v", leases, err)
	}
	lease := leases[0]
	lease.HeartbeatAt = time.Now().Add(-time.Hour)
	if err := manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	fake.active[lease.SessionKey] = true
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if fake.calls != 1 {
		t.Fatalf("active harness session received duplicate stale recovery: %d calls", fake.calls)
	}
	fake.active[lease.SessionKey] = false
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if fake.calls != 2 {
		t.Fatalf("stale task was not recovered: %d calls", fake.calls)
	}
	document, err = ReadDocument(working)
	if err != nil {
		t.Fatal(err)
	}
	recoveredFirst, iterations, ok := DurableTiming(document)
	if !ok || !recoveredFirst.Equal(firstAssigned) || iterations != 2 {
		t.Fatalf("recovered durable timing = %s %d %t", recoveredFirst, iterations, ok)
	}
	recovered, err := manager.loadLease(lease.ID)
	if err != nil || recovered.RecoveryCount != 1 || recovered.State != "awaiting_transition" {
		t.Fatalf("recovered lease = %#v, %v", recovered, err)
	}
}

func TestRecoveryDispatchStateTracksProviderLifecycle(t *testing.T) {
	for _, test := range []struct {
		name, priorState, wantState, wantError string
		events                                 []core.Event
	}{
		{name: "admission", priorState: "error", wantState: "processing", events: []core.Event{}},
		{name: "running", priorState: "awaiting_transition", wantState: "processing", events: []core.Event{
			{Kind: core.EventStatus, Execution: &core.ExecutionStatus{State: "running"}},
			{Kind: core.EventDelta, Text: "resumed work"},
		}},
		{name: "readmitted", priorState: "recovering", wantState: "processing", events: []core.Event{}},
		{name: "completed", priorState: "processing", wantState: "awaiting_transition", events: []core.Event{
			{Kind: core.EventFinal, Done: true},
		}},
		{name: "failed", priorState: "error", wantState: "awaiting_transition", wantError: "new failure", events: []core.Event{
			{Kind: core.EventError, Text: "new failure", Done: true},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			if err := workspace.Init(root, false); err != nil {
				t.Fatal(err)
			}
			cfg, err := config.Load(config.PathForRoot(root))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Create(cfg, "tasks", "recovery state", ""); err != nil {
				t.Fatal(err)
			}
			fake := newFakeRecipient()
			manager := New(cfg, fake, extensions.Runner{})
			if err := manager.ScanOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			manager.Wait()
			leases, err := manager.loadLeases()
			if err != nil || len(leases) != 1 {
				t.Fatalf("leases = %#v, %v", leases, err)
			}
			lease := leases[0]
			lease.State, lease.LastError = test.priorState, "previous failure"
			if err := manager.saveLease(lease); err != nil {
				t.Fatal(err)
			}
			fake.events = test.events
			manager.JobStarted = func(current Lease, _ string, _ time.Time, _, _ int) (int, error) {
				if current.State != "recovering" || current.LastError != "" {
					t.Errorf("recovery admission retained previous state: %#v", current)
				}
				return 1, nil
			}
			var updates []Lease
			manager.JobUpdated = func(_ int, current Lease) { updates = append(updates, current) }
			manager.dispatch(context.Background(), workflowRoutes()[0], lease, true)
			manager.Wait()
			current, err := manager.loadLease(lease.ID)
			if err != nil || current.State != test.wantState || current.LastError != test.wantError || current.RecoveryCount != 1 {
				t.Fatalf("recovered lease = %#v, %v", current, err)
			}
			if len(updates) == 0 || !reflect.DeepEqual(updates[len(updates)-1], current) {
				t.Fatalf("job updates did not publish the persisted lease: %#v", updates)
			}
			if test.name == "running" {
				for _, update := range updates {
					if update.State != "processing" {
						t.Fatalf("provider activity retained recovery state: %#v", update)
					}
				}
			}
		})
	}
}

func TestFutureDispatchTimeDefersGoalPlanning(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(config.PathForRoot(root))
	goal, err := Create(cfg, "goals", "review this later", "")
	if err != nil {
		t.Fatal(err)
	}
	document, err := ReadDocument(goal)
	if err != nil {
		t.Fatal(err)
	}
	document.FrontMatter["next_dispatch_at"] = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	if err := WriteDocument(goal, document); err != nil {
		t.Fatal(err)
	}
	fake := newFakeRecipient()
	manager := New(cfg, fake, extensions.Runner{Directory: filepath.Join(root, "missing")})
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if fake.calls != 0 {
		t.Fatalf("future goal dispatched %d times", fake.calls)
	}
	if _, err := os.Stat(goal); err != nil {
		t.Fatalf("deferred goal moved unexpectedly: %v", err)
	}
}

func TestFutureCheckpointDoesNotDeferImplementationOrPlanning(t *testing.T) {
	for _, route := range []string{"tasks", "goals"} {
		t.Run(route, func(t *testing.T) {
			root := t.TempDir()
			if err := workspace.Init(root, false); err != nil {
				t.Fatal(err)
			}
			cfg, _ := config.Load(config.PathForRoot(root))
			path, err := Create(cfg, route, "checkpoint belongs to active goals", "")
			if err != nil {
				t.Fatal(err)
			}
			document, err := ReadDocument(path)
			if err != nil {
				t.Fatal(err)
			}
			document.FrontMatter["next_review_at"] = time.Now().Add(7 * 24 * time.Hour).UTC().Format(time.RFC3339)
			if err := WriteDocument(path, document); err != nil {
				t.Fatal(err)
			}
			fake := newFakeRecipient()
			manager := New(cfg, fake, extensions.Runner{Directory: filepath.Join(root, "missing")})
			if err := manager.ScanOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			manager.Wait()
			if fake.calls != 1 {
				t.Fatalf("%s dispatches = %d, want 1", route, fake.calls)
			}
		})
	}
}

func TestReviewTransitionAcceptRejectAndSelfReviewGuard(t *testing.T) {
	for _, test := range []struct {
		name, target, want string
		sameThread         bool
	}{{"accept", "done", "done", false}, {"reject", "todo", "todo", false}, {"self-review", "done", "todo", true}, {"park accepted on human confirmation", "waiting", "waiting", false}, {"failed is not a review outcome", "failed", "todo", false}, {"cancel during review honours operator cancel", "cancelled", "cancelled", false}} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			if err := workspace.Init(root, false); err != nil {
				t.Fatal(err)
			}
			cfg, _ := config.Load(config.PathForRoot(root))
			route := workflowRoutes()[0]
			reviewDir := filepath.Join(filepath.Dir(cfg.Resolve(route.Source)), "review")
			path := filepath.Join(filepath.Dir(reviewDir), "reviewing", "review.md")
			doc := Document{FrontMatter: map[string]any{"id": "review-id", "title": "review", "status": "reviewing", "created_at": time.Now().UTC().Format(time.RFC3339), "updated_at": time.Now().UTC().Format(time.RFC3339), "review_attempt": 1, "notify": map[string]any{"enabled": false}}, Body: "# review\n"}
			if test.target == "waiting" {
				doc.FrontMatter["joey_ask"] = map[string]any{"kind": "choice", "prompt": "Confirm in-game grade?"}
			}
			if err := WriteDocument(path, doc); err != nil {
				t.Fatal(err)
			}
			thread := "review-thread"
			implementer := "implementation-thread"
			if test.sameThread {
				implementer = thread
			}
			manager := New(cfg, newFakeRecipient(), extensions.Runner{Directory: filepath.Join(root, "missing")})
			lease := Lease{ID: "lease", Route: "tasks", File: path, SessionKey: "review-session", Phase: phaseTaskReview, ClaimAttempt: 1, ThreadID: thread, ImplementerThread: implementer, StartedAt: time.Now(), HeartbeatAt: time.Now()}
			if err := manager.saveLease(lease); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(filepath.Dir(cfg.Resolve(route.Source)), test.target, filepath.Base(path))
			if err := os.Rename(path, target); err != nil {
				t.Fatal(err)
			}
			if err := manager.reconcileTransitions(context.Background()); err != nil {
				t.Fatal(err)
			}
			want := filepath.Join(filepath.Dir(cfg.Resolve(route.Source)), test.want, filepath.Base(path))
			if _, err := os.Stat(want); err != nil {
				t.Fatalf("wanted %s: %v", want, err)
			}
			if manager.leaseExists(lease.ID) {
				t.Fatal("terminal review retained lease")
			}
		})
	}
}

func TestCancelTaskDuringReviewDoesNotSpendAttemptOrRedispatch(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(config.PathForRoot(root))
	route := workflowRoutes()[0]
	reviewDir := filepath.Join(filepath.Dir(cfg.Resolve(route.Source)), "review")
	path := filepath.Join(filepath.Dir(reviewDir), "reviewing", "review.md")
	doc := Document{FrontMatter: map[string]any{
		"id":             "review-id",
		"title":          "review",
		"status":         "reviewing",
		"created_at":     time.Now().UTC().Format(time.RFC3339),
		"updated_at":     time.Now().UTC().Format(time.RFC3339),
		"attempt":        1,
		"review_attempt": 1,
		"notify":         map[string]any{"enabled": false},
	}, Body: "# review\n"}
	if err := WriteDocument(path, doc); err != nil {
		t.Fatal(err)
	}
	fake := newFakeRecipient()
	manager := New(cfg, fake, extensions.Runner{Directory: filepath.Join(root, "missing")})
	lease := Lease{
		ID: "lease", Route: "tasks", File: path, SessionKey: "review-session",
		Phase: phaseTaskReview, ClaimAttempt: 1, ThreadID: "review-thread",
		ImplementerThread: "implementation-thread", StartedAt: time.Now(), HeartbeatAt: time.Now(),
	}
	if err := manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}

	// Operator moves the task from reviewing/ to cancelled/
	cancelledTarget := filepath.Join(filepath.Dir(cfg.Resolve(route.Source)), "cancelled", filepath.Base(path))
	if err := os.Rename(path, cancelledTarget); err != nil {
		t.Fatal(err)
	}

	// Reconcile transitions
	if err := manager.reconcileTransitions(context.Background()); err != nil {
		t.Fatal(err)
	}

	// It must stay in cancelled/, NOT be redirected to todo/
	todoPath := filepath.Join(filepath.Dir(cfg.Resolve(route.Source)), "todo", filepath.Base(path))
	if _, err := os.Stat(todoPath); err == nil {
		t.Fatal("task was incorrectly redirected to todo/ after operator cancel during review")
	}
	if _, err := os.Stat(cancelledTarget); err != nil {
		t.Fatalf("task was not found in cancelled/: %v", err)
	}

	// Lease must be cleaned up
	if manager.leaseExists(lease.ID) {
		t.Fatal("cancelled review retained lease")
	}

	// Attempt must not be incremented
	cancelledDoc, err := ReadDocument(cancelledTarget)
	if err != nil {
		t.Fatal(err)
	}
	if numberValue(cancelledDoc.FrontMatter["attempt"]) != 1 {
		t.Fatalf("attempt was incremented to %v, want 1", cancelledDoc.FrontMatter["attempt"])
	}

	// ScanOnce must not re-dispatch or claim the cancelled task
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fake.calls != 0 {
		t.Fatalf("cancelled task was re-dispatched: %d calls", fake.calls)
	}
}

func TestReviewDispatchUsesFreshAttemptSessionAcrossRestart(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(config.PathForRoot(root))
	route := workflowRoutes()[0]
	path := filepath.Join(filepath.Dir(cfg.Resolve(route.Source)), "review", "retry.md")
	if err := WriteDocument(path, Document{FrontMatter: map[string]any{"id": "retry", "title": "retry", "status": "review", "created_at": time.Now().UTC().Format(time.RFC3339), "updated_at": time.Now().UTC().Format(time.RFC3339)}, Body: "# retry\n"}); err != nil {
		t.Fatal(err)
	}
	first := New(cfg, newFakeRecipient(), extensions.Runner{Directory: filepath.Join(root, "missing")})
	if err := first.scanPhaseQueue(context.Background(), route, filepath.Dir(path), filepath.Join(filepath.Dir(filepath.Dir(path)), "reviewing"), phaseTaskReview); err != nil {
		t.Fatal(err)
	}
	first.Wait()
	leases, _ := first.loadLeases()
	if len(leases) != 1 || !strings.HasSuffix(leases[0].SessionKey, ":1") {
		t.Fatalf("first review lease = %#v", leases)
	}
	restarted := New(cfg, newFakeRecipient(), extensions.Runner{Directory: filepath.Join(root, "missing")})
	if err := restarted.scanPhaseQueue(context.Background(), route, filepath.Dir(path), filepath.Join(filepath.Dir(filepath.Dir(path)), "reviewing"), phaseTaskReview); err != nil {
		t.Fatal(err)
	}
	restarted.Wait()
	leases, _ = restarted.loadLeases()
	if len(leases) != 1 {
		t.Fatalf("restart duplicated review: %#v", leases)
	}
}

func TestControlReservationsPersistAndRefreshRegisteredDurableJob(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, ".werkrby", "tasks", "working", "control.md")
	assigned := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	if err := WriteDocument(path, Document{FrontMatter: map[string]any{
		"id": "control", "status": "working", "first_assigned_at": assigned.Format(time.RFC3339), "provider_iterations": 1,
	}, Body: "# Control\n"}); err != nil {
		t.Fatal(err)
	}
	manager := New(cfg, newFakeRecipient(), extensions.Runner{})
	lease := Lease{ID: "control-lease", OwnerID: manager.ownerID, Route: "tasks", File: path, SessionKey: "orchestrator:control", State: "processing", Phase: phaseTaskImplementation, StartedAt: assigned, HeartbeatAt: assigned}
	if err := manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	manager.setRuntimeJob(lease.ID, 7)
	var observed []int
	manager.JobTimingUpdated = func(id int, first time.Time, iterations int) {
		if id != 7 || !first.Equal(assigned) {
			t.Errorf("timing update = id %d first %s", id, first)
		}
		observed = append(observed, iterations)
	}
	for range 2 {
		if !manager.ReserveControlProviderTurn(lease, "control") {
			t.Fatal("control provider-turn reservation failed")
		}
	}
	document, err := ReadDocument(path)
	if err != nil {
		t.Fatal(err)
	}
	first, iterations, ok := DurableTiming(document)
	if !ok || !first.Equal(assigned) || iterations != 3 || !reflect.DeepEqual(observed, []int{2, 3}) {
		t.Fatalf("control timing = %s %d %t, updates %#v", first, iterations, ok, observed)
	}
}

func TestPrepareControlContinuationRevalidatesLeaseOwnerAndDurableState(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(config.PathForRoot(root))
	target := newFakeRecipient()
	manager := New(cfg, target, extensions.Runner{})
	path := filepath.Join(cfg.StatePath("tasks", "working"), "control.md")
	if err := WriteDocument(path, Document{FrontMatter: map[string]any{"id": "control", "status": "working"}, Body: "# Control\n"}); err != nil {
		t.Fatal(err)
	}
	lease := Lease{ID: "control-lease", OwnerID: manager.ownerID, SessionKey: "orchestrator:control", File: path, Route: "tasks", Phase: phaseTaskImplementation, State: "awaiting_transition", StartedAt: time.Now().UTC()}
	if err := manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	manager.setRuntimeJob(lease.ID, 1)
	target.active[lease.SessionKey] = true
	if !manager.PrepareControlContinuation(lease, "control") {
		t.Fatal("valid continuation was refused")
	}
	current, err := manager.loadLease(lease.ID)
	if err != nil || current.State != "processing" {
		t.Fatalf("continued lease = %#v, %v", current, err)
	}
	firstReservation := manager.MarkControlCancellation(lease.SessionKey)
	secondReservation := manager.MarkControlCancellation(lease.SessionKey)
	if manager.PrepareControlContinuation(lease, "control") {
		t.Fatal("cancelled control was allowed to continue")
	}
	manager.RestoreControlCancellation(firstReservation)
	if manager.PrepareControlContinuation(lease, "control") {
		t.Fatal("one rollback cleared another cancellation reservation")
	}
	manager.RestoreControlCancellation(secondReservation)
	if !manager.ControlStillValid(lease, "control") {
		t.Fatal("all rejected cancellation reservations did not restore control")
	}
	if err := WriteDocument(path, Document{FrontMatter: map[string]any{"id": "replacement", "status": "working"}, Body: "# Replacement\n"}); err != nil {
		t.Fatal(err)
	}
	if manager.ControlStillValid(lease, "control") {
		t.Fatal("replacement document identity was accepted")
	}
	if err := WriteDocument(path, Document{FrontMatter: map[string]any{"id": "control", "status": "reviewing"}, Body: "# Wrong phase\n"}); err != nil {
		t.Fatal(err)
	}
	if manager.ControlStillValid(lease, "control") {
		t.Fatal("phase/status mismatch was accepted")
	}
	if err := WriteDocument(path, Document{FrontMatter: map[string]any{"id": "control", "status": "working"}, Body: "# Control\n"}); err != nil {
		t.Fatal(err)
	}
	changedOwner := lease
	changedOwner.OwnerID = "stale-owner"
	if manager.PrepareControlContinuation(changedOwner, "control") {
		t.Fatal("changed owner was allowed to continue")
	}
	if err := os.Rename(path, filepath.Join(cfg.StatePath("tasks", "waiting"), filepath.Base(path))); err != nil {
		t.Fatal(err)
	}
	if manager.PrepareControlContinuation(lease, "control") {
		t.Fatal("moved durable task was allowed to continue")
	}
}

func TestTransportOnlyDoneStatusDoesNotEndOrchestratorProviderTurn(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(config.PathForRoot(root))
	if _, err := Create(cfg, "tasks", "retain native control emitter", ""); err != nil {
		t.Fatal(err)
	}
	target := newFakeRecipient()
	target.events = []core.Event{{Kind: core.EventStatus, Done: true, Text: "transport emitter retained"}}
	manager := New(cfg, target, extensions.Runner{})
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	leases, err := manager.loadLeases()
	if err != nil || len(leases) != 1 {
		t.Fatalf("leases = %#v, %v", leases, err)
	}
	if leases[0].State != "processing" {
		t.Fatalf("transport-only done changed lease state to %q", leases[0].State)
	}
}

func TestAutomaticCleanupRunsOnlyForPrimaryAndUsesLiveRetention(t *testing.T) {
	cfg := config.Default()
	manager := New(cfg, newFakeRecipient(), extensions.Runner{})
	ticks := make(chan time.Time, 3)
	manager.cleanupTicks = ticks
	called := make(chan int, 1)
	manager.Cleanup = func(_ context.Context, days int) (string, error) {
		called <- days
		return "complete", nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { manager.runAutomaticCleanup(ctx); close(done) }()
	ticks <- time.Now()
	select {
	case days := <-called:
		t.Fatalf("secondary ran automatic cleanup with %d days", days)
	case <-time.After(20 * time.Millisecond):
	}
	manager.SetPrimaryOwned(true)
	next := cfg
	next.Workspace.CleanupRetentionDays = 45
	manager.ApplyRuntimeConfig(next)
	ticks <- time.Now()
	select {
	case days := <-called:
		if days != 45 {
			t.Fatalf("automatic cleanup retention = %d, want 45", days)
		}
	case <-time.After(time.Second):
		t.Fatal("primary did not run automatic cleanup")
	}
	cancel()
	<-done
}

func TestTaskNotificationDecisionCoversTerminalAndActionableWaitingTransitions(t *testing.T) {
	now := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		status string
		wakeAt string
		want   bool
	}{
		{name: "done", status: "done", want: true},
		{name: "failed", status: "failed", want: true},
		{name: "cancelled", status: "cancelled", want: true},
		{name: "unscheduled waiting", status: "waiting", want: true},
		{name: "malformed waiting schedule", status: "waiting", wakeAt: "not-a-time", want: true},
		{name: "due waiting schedule", status: "waiting", wakeAt: now.Format(time.RFC3339), want: true},
		{name: "future waiting schedule", status: "waiting", wakeAt: now.Add(time.Minute).Format(time.RFC3339), want: false},
		{name: "working", status: "working", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document := Document{FrontMatter: map[string]any{}}
			if test.wakeAt != "" {
				document.FrontMatter["wake_at"] = test.wakeAt
			}
			if got := requiresTaskNotificationDecision(document, test.status, now); got != test.want {
				t.Fatalf("requiresTaskNotificationDecision(%q, wake_at=%q) = %v, want %v", test.status, test.wakeAt, got, test.want)
			}
		})
	}
}

func TestResolveTargetModel(t *testing.T) {
	dir := t.TempDir()
	taskPath := filepath.Join(dir, "task.md")
	content := []byte("---\nid: test-task\nagent: hermes\nstatus: working\n---\n# Test Task\n")
	if err := os.WriteFile(taskPath, content, 0600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	cfg := config.Config{
		Harness: config.Harness{
			Name:           "herdr",
			Model:          "opencode",
			DeveloperModel: "opencode",
			ReviewerModel:  "kimi",
		},
	}
	manager := New(cfg, newFakeRecipient(), extensions.Runner{})

	// 1. Implementation phase uses task frontmatter "agent: hermes"
	leaseWorking := Lease{File: taskPath, Phase: "working"}
	if got := manager.resolveTargetModel(workflowRoute{Name: "tasks"}, leaseWorking); got != "hermes" {
		t.Errorf("expected target model 'hermes', got %q", got)
	}

	// 2. Review phase automatically uses ReviewerModel ("kimi")
	leaseReview := Lease{File: taskPath, Phase: phaseTaskReview}
	if got := manager.resolveTargetModel(workflowRoute{Name: "tasks"}, leaseReview); got != "kimi" {
		t.Errorf("expected target model 'kimi' for review phase, got %q", got)
	}

	// 3. Task without agent frontmatter falls back to DeveloperModel ("opencode")
	taskNoAgent := filepath.Join(dir, "task-no-agent.md")
	if err := os.WriteFile(taskNoAgent, []byte("---\nid: test-2\nstatus: working\n---\n"), 0600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	leaseNoAgent := Lease{File: taskNoAgent, Phase: "working"}
	if got := manager.resolveTargetModel(workflowRoute{Name: "tasks"}, leaseNoAgent); got != "opencode" {
		t.Errorf("expected target model 'opencode', got %q", got)
	}
}

func TestTransientProviderErrorRetriesInPlace(t *testing.T) {
	transient := errors.New("herdr prompt failed: Error from provider (Console Go): Upstream request failed: [service_overloaded] The backend is temporarily overloaded. Please retry. (out: service_overloaded)")
	deadRunner := errors.New("herdr prompt failed: signal: killed (out: signal: killed)")
	for name, test := range map[string]struct {
		errs      []error
		final     bool
		wantCalls int
		wantState string
		wantError string
	}{
		// One transient refusal: retried once in place, second send succeeds,
		// the lease reaches the normal terminal path with no recorded error.
		// The harness emits its own terminal error beside the refusal, exactly
		// as herdr does, and that must not veto the retry.
		"transient then success": {errs: []error{transient}, wantCalls: 2, wantState: "awaiting_transition", wantError: ""},
		// An undelivered prompt is retried once in place on the same worker
		"prompt_not_delivered then success": {errs: []error{errors.New("prompt_not_delivered: worker sj-t-a1-test never received prompt, please retry")}, wantCalls: 2, wantState: "awaiting_transition", wantError: ""},
		// A turn that produced its final response and then failed on the way
		// back has already done its work: reconciliation owns it, not a retry.
		"transient after a completed turn": {errs: []error{transient}, final: true, wantCalls: 1, wantState: "error", wantError: transient.Error()},
		// The refusal persists past the retry: the ordinary error path runs,
		// exactly one in-place retry was spent, no third attempt.
		"transient twice": {errs: []error{transient, transient}, wantCalls: 2, wantState: "error", wantError: transient.Error()},
		// A dead runner is never retried in place: it fails over at once.
		"dead runner": {errs: []error{deadRunner}, wantCalls: 1, wantState: "error", wantError: deadRunner.Error()},
		// A quota refusal must not retry in place; it belongs to failover.
		"quota refusal": {errs: []error{errors.New("herdr prompt failed: 429 You've reached your weekly usage limit")}, wantCalls: 1, wantState: "error", wantError: "429"},
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			if err := workspace.Init(root, false); err != nil {
				t.Fatal(err)
			}
			cfg, err := config.Load(config.PathForRoot(root))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Create(cfg, "tasks", "transient retry", ""); err != nil {
				t.Fatal(err)
			}
			fake := newFakeRecipient()
			fake.sendErrs = test.errs
			fake.sendErrFinal = test.final
			manager := New(cfg, fake, extensions.Runner{})
			if err := manager.ScanOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			manager.Wait()
			if fake.calls != test.wantCalls {
				t.Fatalf("harness calls = %d, want %d", fake.calls, test.wantCalls)
			}
			leases, err := manager.loadLeases()
			if err != nil {
				t.Fatal(err)
			}
			if name == "quota refusal" {
				if len(leases) != 0 {
					t.Fatalf("quota refusal lease survived: %#v", leases)
				}
				entries, err := os.ReadDir(cfg.StatePath("tasks", "waiting"))
				if err != nil || len(entries) != 1 {
					t.Fatalf("expected 1 task in waiting/, got entries=%v, err=%v", entries, err)
				}
				return
			}
			if len(leases) != 1 {
				t.Fatalf("leases = %#v, %v", leases, err)
			}
			current := leases[0]
			if current.State != test.wantState {
				t.Fatalf("lease state = %q, want %q", current.State, test.wantState)
			}
			if test.wantError != "" && !strings.Contains(current.LastError, test.wantError) {
				t.Fatalf("lease error = %q, want it to contain %q", current.LastError, test.wantError)
			}
			if test.wantError == "" && current.LastError != "" {
				t.Fatalf("lease error = %q, want none", current.LastError)
			}
			// recordError fires captureAutopsyAndAlert on a detached goroutine
			// that writes err-*.json under .werkrby/diagnostics; wait for it so it
			// cannot race t.TempDir cleanup.
			if test.wantError != "" {
				diag := cfg.StatePath("diagnostics")
				deadline := time.Now().Add(2 * time.Second)
				for {
					matches, _ := filepath.Glob(filepath.Join(diag, "err-*.json"))
					if len(matches) > 0 || time.Now().After(deadline) {
						break
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
		})
	}
}

// waitDispatchDiagnostics polls for the err-*.json autopsy that the detached
// captureAutopsyAndAlert goroutine writes, so a dispatch-error test neither
// races t.TempDir cleanup nor asserts before the goroutine reached the alerter
// step.
func waitDispatchDiagnostics(t *testing.T, diagDir string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		matches, _ := filepath.Glob(filepath.Join(diagDir, "err-*.json"))
		if len(matches) > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no dispatch-error diagnostics file appeared under %s", diagDir)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// installFakePython3 puts a recording python3 first on PATH and returns the
// marker file every invocation appends its argv to.
func installFakePython3(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	marker := filepath.Join(dir, "python3-invocations.log")
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" >> '" + marker + "'\n"
	if err := os.WriteFile(filepath.Join(dir, "python3"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return marker
}

// TestDispatchErrorUnderGoTestNeverExecsLiveAlerter guards the production
// alert lane: a Go test that provokes a dispatch error must write its
// diagnostics autopsy but must never exec the live telemetry alerter, so
// fixture tasks stay out of the review log, the dedup file and the ntfy topic.
func TestDispatchErrorUnderGoTestNeverExecsLiveAlerter(t *testing.T) {
	marker := installFakePython3(t)
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Create(cfg, "tasks", "alerter guard fixture", ""); err != nil {
		t.Fatal(err)
	}
	fake := newFakeRecipient()
	fake.sendErrs = []error{errors.New("boom: fixture dispatch error")}
	manager := New(cfg, fake, extensions.Runner{})
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if fake.calls != 1 {
		t.Fatalf("harness calls = %d, want 1", fake.calls)
	}
	waitDispatchDiagnostics(t, cfg.StatePath("diagnostics"))
	// captureAutopsyAndAlert execs the alerter immediately after writing the
	// autopsy, so if it were going to run it has run by now. Give the goroutine
	// a bounded grace window and fail if the fake python3 was ever invoked.
	deadline := time.Now().Add(750 * time.Millisecond)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(marker); err == nil {
			t.Fatalf("live alerter was exec'd under go test; fake python3 argv: %s", data)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestDispatchErrorStillAlertsServiceAlerterWithLeaseArgs pins the service
// alert path: a test that opts back in by pointing telemetryAlerterPath at its
// own recording script must see the exact production argv built from the
// lease, so the running binary keeps alerting with the fixture's task and
// runner exactly as before.
func TestDispatchErrorStillAlertsServiceAlerterWithLeaseArgs(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "alerter-argv.log")
	scriptPath := filepath.Join(dir, "fake-recording-alerter.py")
	// The service execs `python3 <alerterPath> ...`, so the recording fixture
	// must be valid Python, and it records one argv element per line.
	script := "import sys\nwith open('" + marker + "', 'a') as f:\n    f.write('\\n'.join(sys.argv[1:]) + '\\n')\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	previous := telemetryAlerterPath
	telemetryAlerterPath = scriptPath
	defer func() { telemetryAlerterPath = previous }()

	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Create(cfg, "tasks", "alerter service path fixture", ""); err != nil {
		t.Fatal(err)
	}
	fake := newFakeRecipient()
	fake.sendErrs = []error{errors.New("boom: fixture dispatch error")}
	manager := New(cfg, fake, extensions.Runner{})
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()

	leases, err := manager.loadLeases()
	if err != nil {
		t.Fatal(err)
	}
	if len(leases) != 1 {
		t.Fatalf("leases = %#v, want 1", leases)
	}
	lease := leases[0]

	// The recording script runs on the same detached goroutine; its argv file
	// appearing is the completion signal.
	deadline := time.Now().Add(2 * time.Second)
	var lines []string
	for {
		data, readErr := os.ReadFile(marker)
		if readErr == nil {
			lines = strings.Split(strings.TrimSpace(string(data)), "\n")
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("service-path alerter script was never invoked")
		}
		time.Sleep(5 * time.Millisecond)
	}

	type pair struct{ flag, value string }
	for _, want := range []pair{
		{"--title", "Runner Dispatch Error"},
		{"--component", "werkrby-orchestrator"},
		{"--severity", "P1"},
		{"--task-id", filepath.Base(lease.File)},
		{"--runner", lease.SessionKey},
	} {
		found := false
		for i := 0; i+1 < len(lines); i++ {
			if lines[i] == want.flag {
				if lines[i+1] != want.value {
					t.Fatalf("alerter %s = %q, want %q (argv: %v)", want.flag, lines[i+1], want.value, lines)
				}
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("alerter argv missing %s (argv: %v)", want.flag, lines)
		}
	}
	for i := 0; i+1 < len(lines); i++ {
		if lines[i] == "--error" && strings.Contains(lines[i+1], "boom: fixture dispatch error") {
			return
		}
	}
	t.Fatalf("alerter argv missing the dispatch error text (argv: %v)", lines)
}
