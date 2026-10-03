package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/workspace"
)

// claimOrderTestManager builds a workspace whose only claim slot is held by
// the first dispatch for the duration of the assertions, so a single ScanOnce
// performs at most one claim and the claim's pick is directly observable.
func claimOrderTestManager(t *testing.T) (config.Config, *fakeHarness, *Manager, func()) {
	t.Helper()
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Orchestrator.MaxParallel = 1
	release := make(chan struct{})
	fake := newFakeRecipient()
	fake.beforeEmit = func() { <-release }
	manager := New(cfg, fake, extensions.Runner{Directory: filepath.Join(root, "missing")})
	var finishOnce sync.Once
	finish := func() { finishOnce.Do(func() { close(release); manager.Wait() }) }
	t.Cleanup(finish)
	return cfg, fake, manager, finish
}

func writeClaimFixture(t *testing.T, todoDir, name, createdAt string) string {
	t.Helper()
	front := map[string]any{
		"id": "fixture-" + name, "title": "fixture " + name, "status": "todo",
		"updated_at": "2026-10-03T15:00:00Z", "attempt": 0, "review_required": true,
	}
	if createdAt != "" {
		front["created_at"] = createdAt
	}
	path := filepath.Join(todoDir, name)
	if err := WriteDocument(path, Document{FrontMatter: front, Body: "# fixture\n\n## Progress\n\n- Fixture document.\n"}); err != nil {
		t.Fatal(err)
	}
	return path
}

func claimOrderAssertSingleClaim(t *testing.T, cfg config.Config, fake *fakeHarness, finish func(), claimedName string) {
	t.Helper()
	route := workflowRoutes()[0]
	base := filepath.Dir(cfg.Resolve(route.Source))
	if _, err := os.Stat(filepath.Join(base, "working", claimedName)); err != nil {
		t.Fatalf("expected %s to be claimed into working/: %v", claimedName, err)
	}
	remaining, err := os.ReadDir(filepath.Join(base, "todo"))
	if err != nil || len(remaining) != 1 {
		t.Fatalf("todo/ after claim = %d entries (%v), want the unclaimed one left behind", len(remaining), err)
	}
	if remaining[0].Name() == claimedName {
		t.Fatalf("claimed document %s is still in todo/", claimedName)
	}
	finish()
	if fake.calls != 1 {
		t.Fatalf("provider dispatches = %d, want exactly the single claim", fake.calls)
	}
}

// Filename order and created_at order disagree: tasks-20261003-zeta.md sorts
// first by name but was created an hour later than tasks-helm-alpha.md. The
// only free slot must go to the older document (spyjo live case 2026-10-03:
// date-named tasks filed 15:18Z-15:23Z overtook Helm-filed tasks waiting since
// 14:06Z because ReadDir returns filename order).
func TestClaimOrderPrefersOldestCreatedAtOverFilename(t *testing.T) {
	cfg, fake, manager, finish := claimOrderTestManager(t)
	route := workflowRoutes()[0]
	todoDir := cfg.Resolve(route.Source)
	writeClaimFixture(t, todoDir, "tasks-20261003-zeta.md", "2026-10-03T16:00:00Z")
	writeClaimFixture(t, todoDir, "tasks-helm-alpha.md", "2026-10-03T15:00:00Z")

	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	claimOrderAssertSingleClaim(t, cfg, fake, finish, "tasks-helm-alpha.md")
}

// Equal created_at falls back to filename order, so ordering stays
// deterministic for documents created in the same second.
func TestClaimOrderEqualCreatedAtKeepsFilenameTiebreak(t *testing.T) {
	cfg, fake, manager, finish := claimOrderTestManager(t)
	route := workflowRoutes()[0]
	todoDir := cfg.Resolve(route.Source)
	writeClaimFixture(t, todoDir, "tasks-aaa.md", "2026-10-03T15:00:00Z")
	writeClaimFixture(t, todoDir, "tasks-bbb.md", "2026-10-03T15:00:00Z")

	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	claimOrderAssertSingleClaim(t, cfg, fake, finish, "tasks-aaa.md")
}

// A document with a missing or unparseable created_at sorts last and never
// stops the scan: dated documents keep claiming ahead of it and the queue
// keeps moving (an unparseable document is reportUnparseableDocument's case;
// a merely undated one must not stall the board either).
func TestClaimOrderMissingCreatedAtSortsLast(t *testing.T) {
	cfg, fake, manager, finish := claimOrderTestManager(t)
	route := workflowRoutes()[0]
	todoDir := cfg.Resolve(route.Source)
	writeClaimFixture(t, todoDir, "tasks-missing.md", "")
	writeClaimFixture(t, todoDir, "tasks-present.md", "2026-10-03T15:00:00Z")

	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	claimOrderAssertSingleClaim(t, cfg, fake, finish, "tasks-present.md")
}

func TestClaimOrderUnparseableCreatedAtSortsLast(t *testing.T) {
	cfg, fake, manager, finish := claimOrderTestManager(t)
	route := workflowRoutes()[0]
	todoDir := cfg.Resolve(route.Source)
	writeClaimFixture(t, todoDir, "tasks-badtime.md", "not-a-timestamp")
	writeClaimFixture(t, todoDir, "tasks-present.md", "2026-10-03T15:00:00Z")

	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	claimOrderAssertSingleClaim(t, cfg, fake, finish, "tasks-present.md")
}

// The review queue still takes the free slot ahead of new implementation
// once ordering is applied to both scans (F4 behaviour, observer 2026-09-17).
func TestClaimOrderReviewQueueStillClaimsBeforeImplementation(t *testing.T) {
	cfg, fake, manager, finish := claimOrderTestManager(t)
	route := workflowRoutes()[0]
	base := filepath.Dir(cfg.Resolve(route.Source))
	reviewed := writeClaimFixture(t, cfg.Resolve(route.Source), "tasks-review-waiting.md", "2026-10-03T15:00:00Z")
	if err := moveDocument(reviewed, filepath.Join(base, "review", filepath.Base(reviewed)), "review", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	writeClaimFixture(t, cfg.Resolve(route.Source), "tasks-20261003-zeta.md", "2026-10-03T15:30:00Z")

	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(base, "reviewing", "tasks-review-waiting.md")); err != nil {
		t.Fatalf("the waiting review did not take the only free slot: %v", err)
	}
	if _, err := os.Stat(filepath.Join(base, "todo", "tasks-20261003-zeta.md")); err != nil {
		t.Fatalf("new implementation was claimed ahead of the waiting review: %v", err)
	}
	finish()
	if fake.calls != 1 {
		t.Fatalf("provider dispatches = %d, want exactly the review claim", fake.calls)
	}
}
