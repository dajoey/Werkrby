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
	"github.com/agent0ai/spynel/internal/workspace"
)

// The 2026-11-01 monthly-patch reboot drained the Hermes gateway for about nine minutes while
// Werkrby kept launching workers: every launch failed in 2-3 s with the gateway's own
// unavailable wording, and twelve of them burned the 3-strike fail-out twice on healthy tasks
// the pump then had to refund. These tests pin the accounting rule: a launch that never got a
// turn because the gateway was draining/starting/absent/inaccessible spends neither
// RecoveryCount nor the attempt, up to gatewayUnavailableBudget; past the budget it counts
// like any other error.

// gatewayUnavailableFixture is a working task whose lease has already spent two recoveries:
// one more counted error moves it to failed/.
func gatewayUnavailableFixture(t *testing.T, gatewayUnavailable int) (*Manager, Lease, string, string) {
	t.Helper()
	root, cfg := newScratchWorkspace(t)
	doc := cfg.StatePath("tasks", "working", "tasks-scratch-gateway.md")
	writeScratchDocument(t, doc, validScratchTaskFrontMatter)
	manager := New(cfg, newFakeRecipient(), extensions.Runner{Directory: filepath.Join(root, "missing")})
	now := time.Now().UTC()
	id := leaseID("tasks:"+phaseTaskImplementation, "tasks-scratch-gateway")
	lease := Lease{
		ID: id, ClaimID: id, DocumentType: "task", Route: "tasks", OwnerID: manager.ownerID,
		File: doc, SessionKey: "sess-gateway", State: "recovering", Phase: phaseTaskImplementation,
		ClaimAttempt: 1, StartedAt: now, HeartbeatAt: now, RecoveryCount: 2,
		GatewayUnavailableCount: gatewayUnavailable,
	}
	if err := manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	return manager, lease, doc, cfg.StatePath("tasks", "failed", "tasks-scratch-gateway.md")
}

// gatewayDrainErr is the verbatim turn error the 2026-11-01 07:46-07:54 ET drain window
// produced (attempt forensics on tasks-helm-fe6a78ee, run 1).
const gatewayDrainErr = "runner turn failed: Command helper: applied 4 secrets Error: Gateway draining: not_ready"

// A launch that failed because the gateway was not ready is a platform condition the gateway's
// own revival clears, not a task failure: the error is logged as gateway_unavailable, does not
// spend RecoveryCount, and leaves the task in working/ for another dispatch even when two
// recoveries are already spent.
func TestGatewayUnavailableErrorDoesNotCountTowardRecoveryFailOut(t *testing.T) {
	manager, lease, doc, failed := gatewayUnavailableFixture(t, 0)
	var kinds []string
	manager.LogError = func(source, kind, message string) { kinds = append(kinds, kind) }

	manager.recordError(context.Background(), lease, errors.New(gatewayDrainErr))

	if fileExists(failed) || !fileExists(doc) {
		t.Fatalf("a gateway-unavailable launch moved the task to failed/")
	}
	saved, err := manager.loadLease(lease.ID)
	if err != nil {
		t.Fatalf("lease not kept for another dispatch: %v", err)
	}
	if saved.RecoveryCount != 2 {
		t.Fatalf("RecoveryCount = %d, want 2: a gateway-unavailable launch is not counted", saved.RecoveryCount)
	}
	if saved.GatewayUnavailableCount != 1 || saved.State != "error" || !strings.Contains(saved.LastError, "Gateway draining") {
		t.Fatalf("lease = %+v, want state error, gateway_unavailable_count 1 and the error kept", saved)
	}
	if len(kinds) != 1 || kinds[0] != "gateway_unavailable" {
		t.Fatalf("logged kinds = %q, want one distinct gateway_unavailable line", kinds)
	}
	kept, err := ReadDocument(doc)
	if err != nil || !strings.Contains(kept.Body, "no turn ran") || !strings.Contains(kept.Body, "attempt not spent") {
		t.Fatalf("progress note missing (%v): %s", err, kept.Body)
	}
}

// The uncounted launches have their own budget, so a gateway nobody revives cannot re-dispatch
// for ever: past gatewayUnavailableBudget the error counts like any other.
func TestGatewayUnavailablePastItsBudgetCountsAgain(t *testing.T) {
	manager, lease, doc, failed := gatewayUnavailableFixture(t, gatewayUnavailableBudget)

	manager.recordError(context.Background(), lease, errors.New(gatewayDrainErr))

	if !fileExists(failed) || fileExists(doc) {
		t.Fatalf("a gateway-unavailable launch past its budget of %d did not count toward the fail-out", gatewayUnavailableBudget)
	}
}

// Through the dispatch path: a task dispatched into a draining gateway four times (the first
// and three recoveries) is still in working/ with no recovery spent; before this change the
// third such error moved it to failed/. A gateway-unavailable launch is also retried once in
// place (transientProviderError), so each adoption consumes two sends.
func TestGatewayUnavailableDispatchesDoNotFailTheTask(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Create(cfg, "tasks", "gateway drain fixture", ""); err != nil {
		t.Fatal(err)
	}
	fake := newFakeRecipient()
	drainErr := errors.New(gatewayDrainErr)
	for i := 0; i < 8; i++ {
		fake.sendErrs = append(fake.sendErrs, drainErr)
	}
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
	if fake.calls != 8 {
		t.Fatalf("harness calls = %d, want 8 (four adoptions, one in-place retry each)", fake.calls)
	}
	if entries, _ := os.ReadDir(cfg.StatePath("tasks", "failed")); len(entries) != 0 {
		t.Fatalf("gateway-unavailable dispatches moved the task to failed/: %v", entries)
	}
	if entries, _ := os.ReadDir(cfg.StatePath("tasks", "working")); len(entries) != 1 {
		t.Fatalf("working/ = %v, want the task kept for another dispatch", entries)
	}
	leases, err := manager.loadLeases()
	if err != nil || len(leases) != 1 {
		t.Fatalf("leases = %#v, %v", leases, err)
	}
	if got := leases[0]; got.RecoveryCount != 0 || got.GatewayUnavailableCount != 4 || got.State != "error" {
		t.Fatalf("lease recovery_count %d, gateway_unavailable_count %d, state %q; want 0, 4, error", got.RecoveryCount, got.GatewayUnavailableCount, got.State)
	}
}

// The in-place retry that already covers a draining or inaccessible gateway also covers a
// gateway still inside its start deadline (the third wording the 2026-11-01 window produced).
func TestTransientProviderRetryCoversEveryGatewayUnavailableWording(t *testing.T) {
	for _, msg := range []string{
		gatewayDrainErr,
		"runner turn failed: Command helper: applied 4 secrets Error: Gateway starting: deadline",
		"runner turn failed: Command helper: applied 4 secrets Error: Gateway inaccessible: service_identity_unverified",
	} {
		if !transientProviderError(errors.New(msg)) {
			t.Errorf("transientProviderError(%q) = false, want the in-place retry", msg)
		}
	}
	if transientProviderError(errors.New("runner turn failed: Gateway connection failed: socket closed mid-turn")) {
		t.Errorf("transientProviderError matched a mid-turn gateway connection failure; a blind retry could run the task twice")
	}
}

func TestIsGatewayUnavailableError(t *testing.T) {
	// Every positive string below is the verbatim error text a real launch produced in the
	// 2026-11-01 drain window (attempt forensics, runs 1-12 of tasks-helm-fe6a78ee and
	// runs 3-4 of tasks-helm-6eca5662).
	for _, msg := range []string{
		gatewayDrainErr,
		"execution=error Command helper: applied 4 secrets Error: Gateway draining: not_ready",
		"runner turn failed: Command helper: applied 4 secrets Error: Gateway starting: deadline",
		"runner turn failed: Command helper: applied 4 secrets Error: Gateway inaccessible: service_identity_unverified",
	} {
		if !isGatewayUnavailableError(errors.New(msg)) {
			t.Errorf("isGatewayUnavailableError(%q) = false, want true", msg)
		}
	}
	for _, msg := range []string{
		"runner turn failed: Command helper: applied 4 secrets Error: Gateway incompatible: version",
		"runner turn failed: Command helper: applied 4 secrets Error: Gateway conflict: other_session",
		"runner turn failed: Credit balance is too low",
		"the gateway is draining its backup queue",
		"Gateway drained: shutdown complete",
		"worker sj-t-a1-4291 never answered",
	} {
		if isGatewayUnavailableError(errors.New(msg)) {
			t.Errorf("isGatewayUnavailableError(%q) = true, want false", msg)
		}
	}
	if isGatewayUnavailableError(nil) {
		t.Errorf("isGatewayUnavailableError(nil) = true, want false")
	}
}
