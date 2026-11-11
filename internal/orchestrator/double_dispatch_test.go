package orchestrator

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// TestDispatchRefusedWhileLiveLeaseCoversDocument pins the claim invariant from
// the 2026-10-26 double dispatch (tasks-20261026-werkrby-double-dispatch-01):
// a second provider dispatch for a document and phase that another lease
// already covers with a fresh heartbeat must be refused. One live
// implementation lease must mean exactly one implementation session; a second
// dispatch attempt under that condition may not reach the harness.
func TestDispatchRefusedWhileLiveLeaseCoversDocument(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	task, err := Create(cfg, "tasks", "double dispatch guard", "")
	if err != nil {
		t.Fatal(err)
	}
	workingDir := filepath.Join(filepath.Dir(cfg.Resolve(workflowRoutes()[0].Source)), "working")
	working := filepath.Join(workingDir, filepath.Base(task))
	if _, err := ClaimDocument(task, working, "working", time.Now()); err != nil {
		t.Fatal(err)
	}
	route, ok := routeByName("tasks")
	if !ok {
		t.Fatal("tasks route missing")
	}
	now := time.Now().UTC()
	base := "orchestrator:tasks:task_implementation:" + filepath.Base(task)
	live := Lease{
		ID: base + ":1", ClaimID: base + ":1", DocumentType: "task", Route: route.Name,
		OwnerID: manager.ownerID,
		File:    working, SessionKey: base + ":1", State: "processing",
		Phase: phaseTaskImplementation, ClaimAttempt: 1, StartedAt: now, HeartbeatAt: now,
	}
	if err := manager.saveLease(live); err != nil {
		t.Fatal(err)
	}
	second := live
	second.ID = base + ":2"
	second.ClaimID = base + ":2"
	second.SessionKey = base + ":2"
	second.ClaimAttempt = 2
	if err := manager.saveLease(second); err != nil {
		t.Fatal(err)
	}
	if conflict, found := manager.liveLeaseConflict(second); !found || conflict.ID != live.ID {
		t.Fatalf("live lease conflict not detected: conflict=%#v found=%v", conflict, found)
	}
	manager.dispatch(context.Background(), route, second, false)
	manager.Wait()
	fake.mu.Lock()
	calls, keys := fake.calls, fake.keys
	fake.mu.Unlock()
	if calls != 0 {
		t.Fatalf("second dispatch reached the harness %d time(s) while a live lease covered the document; keys=%v", calls, keys)
	}
	if _, err := manager.loadLease(second.ID); err != nil {
		t.Fatalf("refused dispatch dropped its lease: %v", err)
	}
}

// TestDispatchAllowedWhenCoveringLeaseIsStale keeps the guard from wedge-ing
// recovery: a same-document lease whose heartbeat is older than the route's
// stale window is not live, so a new dispatch must proceed.
func TestDispatchAllowedWhenCoveringLeaseIsStale(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	_ = fake
	task, err := Create(cfg, "tasks", "stale lease not live", "")
	if err != nil {
		t.Fatal(err)
	}
	workingDir := filepath.Join(filepath.Dir(cfg.Resolve(workflowRoutes()[0].Source)), "working")
	working := filepath.Join(workingDir, filepath.Base(task))
	if _, err := ClaimDocument(task, working, "working", time.Now()); err != nil {
		t.Fatal(err)
	}
	route, ok := routeByName("tasks")
	if !ok {
		t.Fatal("tasks route missing")
	}
	now := time.Now().UTC()
	base := "orchestrator:tasks:task_implementation:" + filepath.Base(task)
	stale := Lease{
		ID: base + ":1", ClaimID: base + ":1", DocumentType: "task", Route: route.Name,
		OwnerID: manager.ownerID,
		File:    working, SessionKey: base + ":1", State: "processing",
		Phase: phaseTaskImplementation, ClaimAttempt: 1, StartedAt: now, HeartbeatAt: now.Add(-2 * route.StaleAfter),
	}
	if err := manager.saveLease(stale); err != nil {
		t.Fatal(err)
	}
	if _, found := manager.liveLeaseConflict(stale); found {
		t.Fatal("stale lease wrongly reported as a live conflict")
	}
}
