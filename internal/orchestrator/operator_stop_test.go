package orchestrator

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/extensions"
)

// An operator pressing Stop cancels the job's context. recordError used to ask only
// "is the context cancelled?" and filed the stop as a service restart ("interrupted by
// a service restart; attempt not spent"), parking the lease for re-adoption, so the
// task was dispatched again and a recovery agent worked on what the operator had just
// stopped (found live 2026-10-03: Joey ordered omasky left alone; the stop was
// resurrected twice in nine minutes). MarkControlCancellation already records that the
// cancel was deliberate; recordError must honour it.

func stoppedTaskFixture(t *testing.T) (*Manager, Lease, string, string) {
	t.Helper()
	root, cfg := newScratchWorkspace(t)
	doc := cfg.StatePath("tasks", "working", "tasks-scratch-stopped.md")
	writeScratchDocument(t, doc, validScratchTaskFrontMatter)
	manager := New(cfg, newFakeRecipient(), extensions.Runner{Directory: filepath.Join(root, "missing")})
	now := time.Now().UTC()
	id := leaseID("tasks:"+phaseTaskImplementation, "tasks-scratch-stopped")
	lease := Lease{
		ID: id, ClaimID: id, DocumentType: "task", Route: "tasks", OwnerID: manager.ownerID,
		File: doc, SessionKey: "sess-stopped", State: "processing", Phase: phaseTaskImplementation,
		ClaimAttempt: 1, StartedAt: now, HeartbeatAt: now,
	}
	if err := manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	return manager, lease, doc, cfg.StatePath("tasks", "waiting", "tasks-scratch-stopped.md")
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func str(v any) string { return fmt.Sprint(v) }

func cancelledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func TestOperatorStopParksTheTaskInsteadOfReadoptingIt(t *testing.T) {
	manager, lease, doc, waiting := stoppedTaskFixture(t)
	if got := manager.MarkControlCancellation("sess-stopped"); got != lease.ID {
		t.Fatalf("MarkControlCancellation = %q, want %q", got, lease.ID)
	}
	before := time.Now().UTC()

	manager.recordError(cancelledContext(), lease, context.Canceled)

	if fileExists(doc) {
		t.Fatalf("stopped task is still in working/: it would be re-adopted and re-dispatched")
	}
	parked, err := ReadDocument(waiting)
	if err != nil {
		t.Fatalf("stopped task was not parked in waiting/: %v", err)
	}
	if parked.FrontMatter["status"] != "waiting" {
		t.Fatalf("status = %v, want waiting", parked.FrontMatter["status"])
	}
	wake, perr := time.Parse(time.RFC3339, str(parked.FrontMatter["wake_at"]))
	if perr != nil || wake.Before(before.Add(operatorStopHold-time.Minute)) || wake.After(before.Add(operatorStopHold+time.Minute)) {
		t.Fatalf("wake_at = %v (%v), want about %v from now", parked.FrontMatter["wake_at"], perr, operatorStopHold)
	}
	if parked.FrontMatter["stopped_by"] != "operator" {
		t.Fatalf("stopped_by = %v, want operator", parked.FrontMatter["stopped_by"])
	}
	if credit, _ := parked.FrontMatter[resumeCreditField].(bool); !credit {
		t.Fatalf("a stop must not spend an attempt: %#v", parked.FrontMatter)
	}
	if !strings.Contains(parked.Body, "stopped by an operator") || strings.Contains(parked.Body, "service restart") {
		t.Fatalf("progress note wrong: %s", parked.Body)
	}
	if leases, _ := manager.loadLeases(); len(leases) != 0 {
		t.Fatalf("lease still exists, so Spynel would adopt it again: %#v", leases)
	}
	if manager.isControlCancelled(lease.ID) {
		t.Fatalf("the cancellation marker was not cleared")
	}
}

func TestACancelledContextWithoutAStopStillCountsAsARestart(t *testing.T) {
	manager, lease, doc, waiting := stoppedTaskFixture(t)

	manager.recordError(cancelledContext(), lease, context.Canceled)

	if !fileExists(doc) || fileExists(waiting) {
		t.Fatalf("a real service restart must still leave the document for re-adoption")
	}
	kept, err := ReadDocument(doc)
	if err != nil || !strings.Contains(kept.Body, "interrupted by a service restart") {
		t.Fatalf("restart note missing (%v): %s", err, kept.Body)
	}
	if leases, _ := manager.loadLeases(); len(leases) != 1 {
		t.Fatalf("lease must be kept for re-adoption on a restart: %#v", leases)
	}
}
