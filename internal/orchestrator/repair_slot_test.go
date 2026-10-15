package orchestrator

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// A repair task for stuck workers must be able to start while the stuck
// workers hold every normal slot: a task stamped `lane: repair` claims the
// reserved capacity (orchestrator.max_parallel + orchestrator.repair_slots)
// and normal tasks keep waiting, with at most repair_slots repair tasks
// running at once (2026-10-15).
func TestRepairLaneClaimsReservedSlotWhileNormalSlotsAreFull(t *testing.T) {
	cfg, _, manager, finish := claimOrderTestManager(t)
	const normalSlots = 6
	manager.capacityMu.Lock()
	manager.capacityLimit = normalSlots
	manager.capacityMu.Unlock()
	route := workflowRoutes()[0]
	todoDir := cfg.Resolve(route.Source)
	base := filepath.Dir(todoDir)

	// Six normal tasks in flight hold every normal slot.
	for i := 0; i < normalSlots; i++ {
		writeClaimFixture(t, todoDir, fmt.Sprintf("tasks-normal-active-%d.md", i), "2026-09-01T00:00:00Z")
	}
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	// A queued normal task stays queued; a lane: repair task takes the
	// reserved slot; a second repair task waits because only repair_slots
	// repair tasks may run at once.
	writeClaimFixture(t, todoDir, "tasks-normal-queued.md", "2026-09-02T00:00:00Z")
	for name, created := range map[string]string{
		"tasks-repair-queued.md":   "2026-09-03T00:00:00Z",
		"tasks-repair-queued-2.md": "2026-09-04T00:00:00Z",
	} {
		path := writeClaimFixture(t, todoDir, name, created)
		document, err := ReadDocument(path)
		if err != nil {
			t.Fatal(err)
		}
		document.FrontMatter["lane"] = "repair"
		if err := WriteDocument(path, document); err != nil {
			t.Fatal(err)
		}
	}
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(filepath.Join(base, "working", "tasks-repair-queued.md")); err != nil {
		t.Fatalf("lane: repair task was not claimed while stuck workers held every normal slot: %v", err)
	}
	for _, name := range []string{"tasks-normal-queued.md", "tasks-repair-queued-2.md"} {
		if _, err := os.Stat(filepath.Join(base, "todo", name)); err != nil {
			t.Fatalf("%s was claimed although only the reserved repair slot was free: %v", name, err)
		}
	}
	finish()
}
