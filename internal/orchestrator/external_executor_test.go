package orchestrator

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/extensions"
)

// A task document whose front matter says `executor: finite` is executed by the separate
// werkrby-finite engine. The legacy orchestrator leaves it alone in every status folder: no claim,
// lease, dispatch, recovery, wake, notification, retention move or front-matter write, while an
// ordinary task beside it keeps today's behaviour.

func finiteTaskBase(cfg config.Config) string {
	return filepath.Dir(cfg.Resolve(workflowRoutes()[0].Source))
}

func writeFiniteFixture(t *testing.T, dir, name string, extra map[string]any) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	front := map[string]any{
		"id": strings.TrimSuffix(name, ".md"), "title": "finite " + name, "status": filepath.Base(dir),
		"created_at": "2026-10-03T14:00:00Z", "updated_at": "2026-10-03T15:00:00Z", "attempt": 0,
		"review_required": false, "executor": "finite", "finite_task_id": "ft-" + strings.TrimSuffix(name, ".md"),
		"notify": map[string]any{"enabled": true, "origin": "tui/local", "on": []any{"done", "failed", "cancelled", "waiting"}},
	}
	for key, value := range extra {
		front[key] = value
	}
	path := filepath.Join(dir, name)
	if err := WriteDocument(path, Document{FrontMatter: front, Body: "# finite\n\n## Progress\n\n- Bridge handed this task to werkrby-finite.\n"}); err != nil {
		t.Fatal(err)
	}
	return path
}

func readFixtureBytes(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

func assertFiniteUntouched(t *testing.T, path string, before []byte) {
	t.Helper()
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("finite document left %s: %v", path, err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("finite document %s was rewritten by the legacy orchestrator:\n--- before\n%s\n--- after\n%s", path, before, after)
	}
}

func assertNoLeaseFor(t *testing.T, manager *Manager, name string) {
	t.Helper()
	leases, err := manager.loadLeases()
	if err != nil {
		t.Fatal(err)
	}
	for _, lease := range leases {
		if filepath.Base(lease.File) == name || (lease.SourceFile != "" && filepath.Base(lease.SourceFile) == name) {
			t.Fatalf("legacy lease %s (state %s) covers finite document %s", lease.ID, lease.State, name)
		}
	}
}

func assertNotDispatched(t *testing.T, fake *fakeHarness, name string) {
	t.Helper()
	fake.mu.Lock()
	defer fake.mu.Unlock()
	id := strings.TrimSuffix(name, ".md")
	for index, claimed := range fake.claimed {
		if filepath.Base(claimed) == name {
			t.Fatalf("provider dispatch %d named finite document %s", index, name)
		}
	}
	for index, key := range fake.keys {
		if strings.Contains(key, id) || strings.Contains(fake.prompts[index], name) {
			t.Fatalf("provider turn %d (%s) concerned finite document %s", index, key, name)
		}
	}
}

func TestFiniteExecutorTodoTaskIsNeverClaimedWhileOrdinaryTaskIs(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	base := finiteTaskBase(cfg)
	todo := filepath.Join(base, "todo")
	// Older than the ordinary task, so it would be claimed first if it were eligible.
	finite := writeFiniteFixture(t, todo, "tasks-finite-todo.md", nil)
	before := readFixtureBytes(t, finite)
	writeClaimFixture(t, todo, "tasks-ordinary.md", "2026-10-03T15:00:00Z")

	scanAndWait(t, manager)
	scanAndWait(t, manager)

	assertFiniteUntouched(t, finite, before)
	assertNoLeaseFor(t, manager, "tasks-finite-todo.md")
	assertNotDispatched(t, fake, "tasks-finite-todo.md")
	if _, err := os.Stat(filepath.Join(base, "working", "tasks-ordinary.md")); err != nil {
		t.Fatalf("ordinary task beside the finite one was not claimed: %v", err)
	}
	if fake.calls == 0 {
		t.Fatal("ordinary task was claimed but never dispatched")
	}
}

func TestFiniteExecutorClaimedDocumentsWithoutLeaseAreNotRecovered(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	base := finiteTaskBase(cfg)
	working := writeFiniteFixture(t, filepath.Join(base, "working"), "tasks-finite-working.md", nil)
	workingBefore := readFixtureBytes(t, working)
	reviewing := writeFiniteFixture(t, filepath.Join(base, "reviewing"), "tasks-finite-reviewing.md", nil)
	reviewingBefore := readFixtureBytes(t, reviewing)
	// Control: an ordinary claimed document without a lease still gets a recovery lease.
	orphan, err := Create(cfg, "tasks", "recover an interrupted claim", "")
	if err != nil {
		t.Fatal(err)
	}
	orphanWorking := filepath.Join(base, "working", filepath.Base(orphan))
	if _, err := ClaimDocument(orphan, orphanWorking, "working", time.Now()); err != nil {
		t.Fatal(err)
	}

	scanAndWait(t, manager)
	scanAndWait(t, manager)

	assertFiniteUntouched(t, working, workingBefore)
	assertFiniteUntouched(t, reviewing, reviewingBefore)
	assertNoLeaseFor(t, manager, "tasks-finite-working.md")
	assertNoLeaseFor(t, manager, "tasks-finite-reviewing.md")
	assertNotDispatched(t, fake, "tasks-finite-working.md")
	assertNotDispatched(t, fake, "tasks-finite-reviewing.md")
	if !manager.hasLeaseForFile(orphanWorking) {
		t.Fatal("ordinary orphaned claim beside the finite ones received no recovery lease")
	}
}

func TestFiniteExecutorWaitingTaskIsNotWoken(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	base := finiteTaskBase(cfg)
	waiting := filepath.Join(base, "waiting")
	due := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	finite := writeFiniteFixture(t, waiting, "tasks-finite-waiting.md", map[string]any{"wake_at": due})
	before := readFixtureBytes(t, finite)
	ordinary := writeFiniteFixture(t, waiting, "tasks-ordinary-waiting.md", map[string]any{"wake_at": due})
	document, err := ReadDocument(ordinary)
	if err != nil {
		t.Fatal(err)
	}
	delete(document.FrontMatter, "executor")
	delete(document.FrontMatter, "finite_task_id")
	if err := WriteDocument(ordinary, document); err != nil {
		t.Fatal(err)
	}

	scanAndWait(t, manager)

	assertFiniteUntouched(t, finite, before)
	assertNoLeaseFor(t, manager, "tasks-finite-waiting.md")
	assertNotDispatched(t, fake, "tasks-finite-waiting.md")
	if _, err := os.Stat(ordinary); err == nil {
		t.Fatal("ordinary due waiting task beside the finite one was not woken")
	}
}

// A legacy lease whose document the bridge has since marked finite and settled: reconcile must not
// validate, redirect, run hooks for, or start a notification turn about the finite document.
func TestFiniteExecutorSettledDocumentUnderLeaseRaisesNoTransitionOrNotification(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	cfg.Orchestrator.TaskNotifications = config.TaskNotificationsAlways
	manager.ApplyRuntimeConfig(cfg)
	base := finiteTaskBase(cfg)
	name := "tasks-finite-settled.md"
	for _, status := range []string{"done", "failed", "waiting"} {
		done := writeFiniteFixture(t, filepath.Join(base, status), name, map[string]any{
			"completion_summary": map[string]any{"verdict": "completed", "outcome": "finite engine finished"},
		})
		before := readFixtureBytes(t, done)
		now := time.Now().UTC()
		key := leaseID("tasks:"+phaseTaskImplementation, strings.TrimSuffix(name, ".md"))
		if err := manager.saveLease(Lease{
			ID: key, ClaimID: key, DocumentType: "task", Route: "tasks", OwnerID: manager.ownerID,
			File: filepath.Join(base, "working", name), SessionKey: "orchestrator:tasks:" + key,
			State: "processing", Phase: phaseTaskImplementation, ClaimAttempt: 1, StartedAt: now, HeartbeatAt: now,
		}); err != nil {
			t.Fatal(err)
		}

		scanAndWait(t, manager)

		assertFiniteUntouched(t, done, before)
		assertNoLeaseFor(t, manager, name)
		assertNotDispatched(t, fake, name)
		if fake.calls != 0 {
			t.Fatalf("%s: finite document settle started %d provider turn(s): %v", status, fake.calls, fake.keys)
		}
		if err := os.Remove(done); err != nil {
			t.Fatal(err)
		}
	}
}

// A legacy lease on a finite document still at its claimed path: neither the stale recovery nor
// the awaiting-transition requeue may act on it.
func TestFiniteExecutorStaleLeaseIsDroppedWithoutRecovery(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	base := finiteTaskBase(cfg)
	for index, state := range []string{"processing", "awaiting_transition", "error"} {
		name := "tasks-finite-stale-" + state + ".md"
		working := writeFiniteFixture(t, filepath.Join(base, "working"), name, nil)
		before := readFixtureBytes(t, working)
		old := time.Now().Add(-3 * time.Hour).UTC()
		key := leaseID("tasks:"+phaseTaskImplementation, strings.TrimSuffix(name, ".md"))
		if err := manager.saveLease(Lease{
			ID: key, ClaimID: key, DocumentType: "task", Route: "tasks", OwnerID: manager.ownerID,
			File: working, SessionKey: "orchestrator:tasks:" + key, State: state, Phase: phaseTaskImplementation,
			ClaimAttempt: 1, RecoveryCount: index * quickAwaitingTransitionRecoveries, StartedAt: old, HeartbeatAt: old,
		}); err != nil {
			t.Fatal(err)
		}

		scanAndWait(t, manager)
		scanAndWait(t, manager)

		assertFiniteUntouched(t, working, before)
		assertNoLeaseFor(t, manager, name)
		assertNotDispatched(t, fake, name)
	}
	if fake.calls != 0 {
		t.Fatalf("stale finite leases started %d provider turn(s): %v", fake.calls, fake.keys)
	}
}

// A claim interrupted after its lease was journaled, whose source the bridge then marked finite.
func TestFiniteExecutorInterruptedClaimIsNotResumed(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	base := finiteTaskBase(cfg)
	name := "tasks-finite-interrupted.md"
	source := writeFiniteFixture(t, filepath.Join(base, "todo"), name, nil)
	before := readFixtureBytes(t, source)
	now := time.Now().UTC()
	key := leaseID("tasks:"+phaseTaskImplementation, strings.TrimSuffix(name, ".md"))
	if err := manager.saveLease(Lease{
		ID: key, ClaimID: key, DocumentType: "task", Route: "tasks", OwnerID: manager.ownerID,
		File: filepath.Join(base, "working", name), SourceFile: source, SessionKey: "orchestrator:tasks:" + key,
		State: "claiming", Phase: phaseTaskImplementation, ClaimAttempt: 1, StartedAt: now, HeartbeatAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	scanAndWait(t, manager)
	scanAndWait(t, manager)

	assertFiniteUntouched(t, source, before)
	assertNoLeaseFor(t, manager, name)
	assertNotDispatched(t, fake, name)
}

// bridgeTake reproduces the bridge's take of a todo document: it holds the document's
// provider-turn lock, writes `executor: finite` into the copy in todo/ and, when move is set,
// renames it to working/. It returns once the lock is held; done closes after the release, and the
// bytes the bridge left behind are reported on it.
func bridgeTake(t *testing.T, source string, move bool) (string, <-chan []byte) {
	t.Helper()
	lock, err := lockProviderTurn(source)
	if err != nil {
		t.Fatal(err)
	}
	final := source
	if move {
		final = filepath.Join(filepath.Dir(filepath.Dir(source)), "working", filepath.Base(source))
	}
	written := make(chan []byte, 1)
	go func() {
		defer unlockProviderTurn(lock)
		// Give the legacy claim time to block on the held lock.
		time.Sleep(50 * time.Millisecond)
		document, err := ReadDocument(source)
		if err != nil {
			t.Errorf("bridge read: %v", err)
			written <- nil
			return
		}
		document.FrontMatter["executor"] = "finite"
		document.FrontMatter["finite_task_id"] = "ft-" + filepath.Base(source)
		if move {
			document.FrontMatter["status"] = "working"
		}
		if err := WriteDocument(source, document); err != nil {
			t.Errorf("bridge write: %v", err)
		}
		if move {
			if err := os.Rename(source, final); err != nil {
				t.Errorf("bridge move: %v", err)
			}
		}
		data, _ := os.ReadFile(final)
		written <- data
	}()
	return final, written
}

func finiteClaimRaceManager(t *testing.T, move bool) {
	t.Helper()
	cfg, fake, manager := workflowTestManager(t)
	base := finiteTaskBase(cfg)
	todo := filepath.Join(base, "todo")
	name := "tasks-bridge-taken.md"
	raced := writeClaimFixture(t, todo, name, "2026-10-03T14:00:00Z")
	writeClaimFixture(t, todo, "tasks-ordinary.md", "2026-10-03T15:00:00Z")
	var (
		logMu sync.Mutex
		logs  []string
	)
	manager.Log = func(message string) {
		logMu.Lock()
		logs = append(logs, message)
		logMu.Unlock()
	}
	var final string
	var written <-chan []byte
	manager.claimDocument = func(source, target, status, attemptField string, now time.Time) (Document, error) {
		if filepath.Base(source) == name && written == nil {
			// The scan already read the document (it was ordinary then) and journaled the
			// claiming lease; the bridge now holds the lock the claim is about to take.
			final, written = bridgeTake(t, raced, move)
		}
		return claimDocument(source, target, status, attemptField, now)
	}

	scanAndWait(t, manager)
	if written == nil {
		t.Fatal("test setup: the scan never tried to claim the raced document")
	}
	left := <-written
	if len(left) == 0 {
		t.Fatal("test setup: the bridge left no document")
	}
	// A second scan must not adopt, resume or recover what the bridge took.
	scanAndWait(t, manager)

	assertFiniteUntouched(t, final, left)
	assertNoLeaseFor(t, manager, name)
	assertNotDispatched(t, fake, name)
	if document, err := ReadDocument(final); err != nil || numberValue(document.FrontMatter["attempt"]) != 0 {
		t.Fatalf("raced claim spent an attempt: %#v, %v", document.FrontMatter["attempt"], err)
	}
	if !move {
		if _, err := os.Stat(filepath.Join(base, "working", name)); err == nil {
			t.Fatal("the legacy claim moved a document the bridge had marked finite")
		}
	}
	if _, err := os.Stat(filepath.Join(base, "working", "tasks-ordinary.md")); err != nil {
		t.Fatalf("ordinary task beside the raced one was not claimed: %v", err)
	}
	logMu.Lock()
	defer logMu.Unlock()
	for _, message := range logs {
		if strings.HasPrefix(message, "claim ") || strings.Contains(message, "recovering claimed document without lease: "+final) {
			t.Fatalf("the abandoned claim was logged as a claim error or recovery: %q", message)
		}
	}
}

// The bridge takes the document while the claim waits on the provider-turn lock: when the claim
// gets the lock, its source has moved to working/ (the claim's own target path).
func TestFiniteExecutorClaimAbortsWhenBridgeMovedSourceUnderLock(t *testing.T) {
	finiteClaimRaceManager(t, true)
}

// The bridge marked the source finite after the scan read it, and the source is still in todo/.
func TestFiniteExecutorClaimAbortsWhenSourceMarkedFiniteUnderLock(t *testing.T) {
	finiteClaimRaceManager(t, false)
}

// Goals keep reading a finite child's folder: in working/ it is in progress (the goal stays active
// and the orphan path leaves it alone), and once the bridge settles it the round is settled.
func TestFiniteExecutorGoalRoundCountsFiniteChildByFolder(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	goal := writeActiveGoalRound(t, cfg, "all_round_tasks_settled", time.Now().Add(7*24*time.Hour), "working", "done")
	tasks := roundTaskPaths(t, manager, goal)
	var child string
	for _, task := range tasks {
		if task.Status == "working" {
			child = task.Path
		}
	}
	if child == "" {
		t.Fatal("test setup: no working round task")
	}
	document, err := ReadDocument(child)
	if err != nil {
		t.Fatal(err)
	}
	document.FrontMatter["executor"] = "finite"
	document.FrontMatter["finite_task_id"] = "ft-child"
	if err := WriteDocument(child, document); err != nil {
		t.Fatal(err)
	}
	before := readFixtureBytes(t, child)

	scanAndWait(t, manager)

	assertFiniteUntouched(t, child, before)
	assertNoLeaseFor(t, manager, filepath.Base(child))
	assertNotDispatched(t, fake, filepath.Base(child))
	if _, err := os.Stat(goal); err != nil {
		t.Fatalf("goal left active/ while its finite child was still working: %v", err)
	}

	// The bridge settles the child: completion summary, then done/.
	document.FrontMatter["completion_summary"] = map[string]any{"verdict": "completed", "outcome": "finite engine finished"}
	document.FrontMatter["status"] = "done"
	settled := filepath.Join(finiteTaskBase(cfg), "done", filepath.Base(child))
	if err := WriteDocument(settled, document); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(child); err != nil {
		t.Fatal(err)
	}
	if err := manager.advanceActiveGoals(); err != nil {
		t.Fatal(err)
	}
	review := filepath.Join(filepath.Dir(filepath.Dir(goal)), "review", filepath.Base(goal))
	if _, err := os.Stat(review); err != nil {
		t.Fatalf("goal round with its finite child settled was not queued for review: %v", err)
	}
	round := roundTaskPaths(t, manager, review)
	for _, task := range round {
		if filepath.Base(task.Path) == filepath.Base(child) && task.Status != "done" {
			t.Fatalf("goal read the finite child as %q, want done", task.Status)
		}
	}
}

func TestFiniteExecutorTerminalTaskIsNotArchived(t *testing.T) {
	cfg, _, manager := workflowTestManager(t)
	base := finiteTaskBase(cfg)
	old := map[string]any{"updated_at": "2026-01-01T00:00:00Z"}
	finite := writeFiniteFixture(t, filepath.Join(base, "done"), "tasks-finite-old.md", old)
	before := readFixtureBytes(t, finite)
	ordinary := writeFiniteFixture(t, filepath.Join(base, "done"), "tasks-ordinary-old.md", old)
	document, err := ReadDocument(ordinary)
	if err != nil {
		t.Fatal(err)
	}
	delete(document.FrontMatter, "executor")
	if err := WriteDocument(ordinary, document); err != nil {
		t.Fatal(err)
	}

	archived, _, failed := manager.ArchiveTerminalTasks(time.Now())

	assertFiniteUntouched(t, finite, before)
	if archived != 1 || failed != 0 {
		t.Fatalf("archived=%d failed=%d, want only the ordinary task archived", archived, failed)
	}
	if _, err := os.Stat(filepath.Join(base, "archive", "tasks-ordinary-old.md")); err != nil {
		t.Fatalf("ordinary terminal task beside the finite one was not archived: %v", err)
	}
}

func TestFiniteExecutorOperatorFinalizeRunsNoNotification(t *testing.T) {
	cfg, _, _ := workflowTestManager(t)
	cfg.Orchestrator.TaskNotifications = config.TaskNotificationsAlways
	target := &notificationActionHarness{}
	manager := New(cfg, target, extensions.Runner{Directory: filepath.Join(cfg.StatePath(), "missing")})
	path := writeFiniteFixture(t, filepath.Join(finiteTaskBase(cfg), "done"), "tasks-finite-operator.md", nil)
	before := readFixtureBytes(t, path)

	manager.FinalizeOperatorTaskTransition(context.Background(), path, "tasks-finite-operator", "t-web", "done")
	manager.Wait()

	assertFiniteUntouched(t, path, before)
	if target.calls.Load() != 0 {
		t.Fatalf("operator finalize of a finite task started %d notification turn(s)", target.calls.Load())
	}
}

// A finite task in todo/ is never claimed, so it is not ops work waiting for a slot either.
func TestFiniteExecutorTaskDoesNotHoldTheOpsLane(t *testing.T) {
	manager, finish, todoDir, base := slots(t, 3)
	setOpsLane(manager, opsLaneOn(2))
	writeFiniteFixture(t, todoDir, "tasks-finite-ops.md", map[string]any{"created_at": "2026-10-15T09:00:00Z"})
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-a.md", "2026-10-15T10:00:00Z")
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-b.md", "2026-10-15T10:01:00Z")
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-c.md", "2026-10-15T10:02:00Z")
	scan(t, manager)
	assertWorking(t, base, "tasks-goal-a.md", true)
	assertWorking(t, base, "tasks-goal-b.md", true)
	assertWorking(t, base, "tasks-goal-c.md", true)
	assertWorking(t, base, "tasks-finite-ops.md", false)
	finish()
}

func TestFiniteExecutorHeartbeatPromptLeavesFiniteTasksAlone(t *testing.T) {
	manager := newHeartbeatManager(t, &heartbeatHarness{})
	prompt, err := manager.semanticHeartbeatPrompt("heartbeat-test", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(prompt, "`executor: finite`") {
		t.Fatalf("heartbeat prompt does not tell the agent to leave finite-executed tasks alone: %q", prompt)
	}
}

func TestExternallyExecutedIsExactCaseSensitiveString(t *testing.T) {
	for _, tc := range []struct {
		front map[string]any
		want  bool
	}{
		{map[string]any{"executor": "finite"}, true},
		{map[string]any{"executor": "Finite"}, false},
		{map[string]any{"executor": "FINITE"}, false},
		{map[string]any{"executor": " finite"}, false},
		{map[string]any{"executor": "legacy"}, false},
		{map[string]any{"executor": true}, false},
		{map[string]any{"executor": nil}, false},
		{map[string]any{"finite_task_id": "ft-1"}, false},
		{map[string]any{}, false},
		{nil, false},
	} {
		if got := externallyExecuted(Document{FrontMatter: tc.front}); got != tc.want {
			t.Errorf("externallyExecuted(%#v) = %t, want %t", tc.front, got, tc.want)
		}
	}
	// YAML front matter as the bridge writes it.
	document, err := ParseDocument([]byte("---\nid: t1\nexecutor: finite\nfinite_task_id: ft-1\n---\n# t1\n"))
	if err != nil || !externallyExecuted(document) {
		t.Fatalf("bridge-written front matter not recognised: %v", err)
	}
	// A document that cannot be read keeps today's behaviour.
	if externallyExecutedAt(filepath.Join(t.TempDir(), "missing.md")) {
		t.Fatal("a missing document was treated as finite-executed")
	}
	broken := filepath.Join(t.TempDir(), "broken.md")
	if err := os.WriteFile(broken, []byte("---\nexecutor: finite\nbroken: [unclosed\n---\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if externallyExecutedAt(broken) {
		t.Fatal("an unparseable document was treated as finite-executed")
	}
}

// While the bridge's switch file exists, a task the old runner never started is the bridge's to
// take: the old runner leaves it in todo/ however the timing falls. Started work, and everything
// once the switch file is gone, behaves as before.
func TestFreshTaskIsLeftToTheBridgeWhileItsSwitchFileExists(t *testing.T) {
	cfg, _, manager := workflowTestManager(t)
	base := finiteTaskBase(cfg)
	todo := filepath.Join(base, "todo")
	flag := filepath.Join(t.TempDir(), "bridge.on")
	if err := os.WriteFile(flag, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	manager.runtimeConfigMu.Lock()
	manager.runtimeConfig.Orchestrator.ExternalExecutorFlag = flag
	manager.runtimeConfigMu.Unlock()

	fresh := writeClaimFixture(t, todo, "tasks-fresh.md", "2026-10-03T14:00:00Z") // attempt: 0, as producers write it
	started := writeClaimFixture(t, todo, "tasks-started.md", "2026-10-03T15:00:00Z")
	doc, err := ReadDocument(started)
	if err != nil {
		t.Fatal(err)
	}
	doc.FrontMatter["attempt"] = 1
	doc.FrontMatter["first_assigned_at"] = "2026-10-03T15:01:00Z"
	if err := WriteDocument(started, doc); err != nil {
		t.Fatal(err)
	}
	before := readFixtureBytes(t, fresh)

	scanAndWait(t, manager)
	scanAndWait(t, manager)

	assertFiniteUntouched(t, fresh, before)
	assertNoLeaseFor(t, manager, "tasks-fresh.md")
	if _, err := os.Stat(filepath.Join(base, "working", "tasks-started.md")); err != nil {
		t.Fatalf("already-started task was not claimed while the switch file exists: %v", err)
	}

	if err := os.Remove(flag); err != nil {
		t.Fatal(err)
	}
	scanAndWait(t, manager)
	if _, err := os.Stat(filepath.Join(base, "working", "tasks-fresh.md")); err != nil {
		t.Fatalf("with the switch file gone the old runner did not claim the new task: %v", err)
	}
}
