package orchestrator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/harness"
	"github.com/agent0ai/spynel/internal/workspace"
)

// blockedTaskFixture is a working task whose lease has already spent two recoveries: one more
// counted error moves it to failed/.
func blockedTaskFixture(t *testing.T, hermesBlocked int) (*Manager, Lease, string, string) {
	t.Helper()
	root, cfg := newScratchWorkspace(t)
	doc := cfg.StatePath("tasks", "working", "tasks-scratch-blocked.md")
	writeScratchDocument(t, doc, validScratchTaskFrontMatter)
	manager := New(cfg, newFakeRecipient(), extensions.Runner{Directory: filepath.Join(root, "missing")})
	now := time.Now().UTC()
	id := leaseID("tasks:"+phaseTaskImplementation, "tasks-scratch-blocked")
	lease := Lease{
		ID: id, ClaimID: id, DocumentType: "task", Route: "tasks", OwnerID: manager.ownerID,
		File: doc, SessionKey: "sess-blocked", State: "recovering", Phase: phaseTaskImplementation,
		ClaimAttempt: 1, StartedAt: now, HeartbeatAt: now, RecoveryCount: 2, HermesBlockedCount: hermesBlocked,
	}
	if err := manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	return manager, lease, doc, cfg.StatePath("tasks", "failed", "tasks-scratch-blocked.md")
}

// hermesBlockedErr is the error the Herdr harness returns for a Hermes pane still blocked past
// its wait, wrapped as the supervisor hands it on.
func hermesBlockedErr() error {
	return errors.New("herdr agent wk-t-a1-blocked blocked: " + harness.HermesBlockedCode + ": the hermes worker pane stayed blocked on a prompt for 5m0s")
}

// A Hermes pane blocked past the harness wait is a harness condition the prompt watcher clears,
// not a task failure: the error is logged as hermes_blocked, does not spend RecoveryCount, and
// leaves the task in working/ for another dispatch even when two recoveries are already spent.
func TestHermesBlockedErrorDoesNotCountTowardRecoveryFailOut(t *testing.T) {
	manager, lease, doc, failed := blockedTaskFixture(t, 0)
	var kinds []string
	manager.LogError = func(source, kind, message string) { kinds = append(kinds, kind) }

	manager.recordError(context.Background(), lease, hermesBlockedErr())

	if fileExists(failed) || !fileExists(doc) {
		t.Fatalf("a hermes_blocked error moved the task to failed/")
	}
	saved, err := manager.loadLease(lease.ID)
	if err != nil {
		t.Fatalf("lease not kept for another dispatch: %v", err)
	}
	if saved.RecoveryCount != 2 {
		t.Fatalf("RecoveryCount = %d, want 2: a hermes_blocked error is not counted", saved.RecoveryCount)
	}
	if saved.HermesBlockedCount != 1 || saved.State != "error" || !strings.Contains(saved.LastError, harness.HermesBlockedCode) {
		t.Fatalf("lease = %+v, want state error, hermes_blocked_count 1 and the error kept", saved)
	}
	if len(kinds) != 1 || kinds[0] != "hermes_blocked" {
		t.Fatalf("logged kinds = %q, want one distinct hermes_blocked line", kinds)
	}
	kept, err := ReadDocument(doc)
	if err != nil || !strings.Contains(kept.Body, "attempt not spent") {
		t.Fatalf("progress note missing (%v): %s", err, kept.Body)
	}
}

// The uncounted errors have their own budget, so a pane nobody can clear cannot re-dispatch for
// ever: past hermesBlockedBudget the error counts like any other.
func TestHermesBlockedPastItsBudgetCountsAgain(t *testing.T) {
	manager, lease, doc, failed := blockedTaskFixture(t, hermesBlockedBudget)

	manager.recordError(context.Background(), lease, hermesBlockedErr())

	if !fileExists(failed) || fileExists(doc) {
		t.Fatalf("a hermes_blocked error past its budget of %d did not count toward the fail-out", hermesBlockedBudget)
	}
}

// Other runners keep their behaviour: an ordinary herdr agent_blocked error spends RecoveryCount
// and the third one moves the task to failed/.
func TestNonHermesAgentBlockedStillCounts(t *testing.T) {
	manager, lease, doc, failed := blockedTaskFixture(t, 0)
	err := errors.New(`herdr agent wk-t-a1-blocked blocked: {"error":{"code":"agent_blocked","message":"agent wk-t-a1-blocked is blocked and requires interactive input"},"id":"cli:agent:prompt"}`)

	manager.recordError(context.Background(), lease, err)

	if !fileExists(failed) || fileExists(doc) {
		t.Fatalf("an agent_blocked error from another runner no longer counts toward the fail-out")
	}
}

// Through the dispatch path: a task whose Hermes pane stays blocked through four dispatches (the
// first and three recoveries) is still in working/ with no recovery spent; before this change the
// third such error moved it to failed/.
func TestHermesBlockedDispatchesDoNotFailTheTask(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Create(cfg, "tasks", "hermes blocked fixture", ""); err != nil {
		t.Fatal(err)
	}
	fake := newFakeRecipient()
	fake.sendErrs = []error{hermesBlockedErr(), hermesBlockedErr(), hermesBlockedErr(), hermesBlockedErr()}
	manager := New(cfg, fake, extensions.Runner{})
	ctx := context.Background()
	if err := manager.ScanOnce(ctx); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	for i := 0; i < 3; i++ {
		leases, err := manager.loadLeases()
		if err != nil || len(leases) != 1 {
			t.Fatalf("after dispatch %d: leases = %#v, %v", i+1, leases, err)
		}
		// Age the error lease past its 10 s re-dispatch threshold.
		leases[0].HeartbeatAt = time.Now().UTC().Add(-time.Minute)
		if err := manager.saveLease(leases[0]); err != nil {
			t.Fatal(err)
		}
		if err := manager.recoverStale(ctx); err != nil {
			t.Fatal(err)
		}
		manager.Wait()
	}
	if fake.calls != 4 {
		t.Fatalf("harness calls = %d, want 4", fake.calls)
	}
	if entries, _ := os.ReadDir(cfg.StatePath("tasks", "failed")); len(entries) != 0 {
		t.Fatalf("hermes_blocked dispatches moved the task to failed/: %v", entries)
	}
	if entries, _ := os.ReadDir(cfg.StatePath("tasks", "working")); len(entries) != 1 {
		t.Fatalf("working/ = %v, want the task kept for another dispatch", entries)
	}
	leases, err := manager.loadLeases()
	if err != nil || len(leases) != 1 {
		t.Fatalf("leases = %#v, %v", leases, err)
	}
	if got := leases[0]; got.RecoveryCount != 0 || got.HermesBlockedCount != 4 || got.State != "error" {
		t.Fatalf("lease recovery_count %d, hermes_blocked_count %d, state %q; want 0, 4, error", got.RecoveryCount, got.HermesBlockedCount, got.State)
	}
}
