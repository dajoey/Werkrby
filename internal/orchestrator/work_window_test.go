package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/workspace"
)

// During working hours one slot is kept for work Claude cards (tasks the Helm
// pump marks claude_only). Personal work may use every other slot; outside the
// window nothing changes. These tests pin that rule through ScanOnce, the live
// claim path.

func workWindowFixture() config.WorkWindow {
	return config.WorkWindow{Enabled: true, Start: "09:00", End: "18:00", Timezone: "America/New_York", ReservedSlots: 1}
}

// inWindow is a Thursday at noon New York time, outsideWindow the Saturday after.
func inWindow(t *testing.T) time.Time {
	t.Helper()
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	return time.Date(2026, 10, 15, 12, 0, 0, 0, loc)
}

func outsideWindow(t *testing.T) time.Time { return inWindow(t).AddDate(0, 0, 2) }

func setWorkWindow(manager *Manager, window config.WorkWindow, now time.Time) {
	manager.runtimeConfigMu.Lock()
	manager.runtimeConfig.Orchestrator.WorkWindow = window
	manager.runtimeConfigMu.Unlock()
	manager.workClock = func() time.Time { return now }
}

func writeWorkCardFixture(t *testing.T, dir, name, createdAt string) string {
	t.Helper()
	front := map[string]any{
		"id": "fixture-" + name, "title": "work card " + name, "status": "todo",
		"created_at": createdAt, "updated_at": "2026-10-15T15:00:00Z", "attempt": 0,
		"agent": "claude", "staff": "second", "claude_only": true, "review_required": false,
	}
	path := filepath.Join(dir, name)
	if err := WriteDocument(path, Document{FrontMatter: front, Body: "# work card\n\n## Progress\n\n- Fixture document.\n"}); err != nil {
		t.Fatal(err)
	}
	return path
}

func slots(t *testing.T, limit int) (*Manager, func(), string, string) {
	t.Helper()
	cfg, _, manager, finish := claimOrderTestManager(t)
	manager.capacityMu.Lock()
	manager.capacityLimit = limit
	manager.capacityMu.Unlock()
	taskRoute := workflowRoutes()[0]
	return manager, finish, cfg.Resolve(taskRoute.Source), filepath.Dir(cfg.Resolve(taskRoute.Source))
}

func assertWorking(t *testing.T, base, name string, want bool) {
	t.Helper()
	_, err := os.Stat(filepath.Join(base, "working", name))
	if want && err != nil {
		t.Fatalf("%s should be in working/: %v", name, err)
	}
	if !want && err == nil {
		t.Fatalf("%s was claimed but the reserved slot should have held it back", name)
	}
	if !want {
		if _, err := os.Stat(filepath.Join(base, "todo", name)); err != nil {
			t.Fatalf("%s should still be in todo/: %v", name, err)
		}
	}
}

func scan(t *testing.T, manager *Manager) {
	t.Helper()
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// The last slot stays empty for personal work and goes to the work card.
func TestWorkWindowKeepsTheLastSlotForAWorkCard(t *testing.T) {
	manager, finish, todoDir, base := slots(t, 2)
	setWorkWindow(manager, workWindowFixture(), inWindow(t))
	writeClaimFixture(t, todoDir, "tasks-personal-a.md", "2026-10-15T10:00:00Z")
	scan(t, manager)
	assertWorking(t, base, "tasks-personal-a.md", true)

	writeClaimFixture(t, todoDir, "tasks-personal-b.md", "2026-10-15T10:05:00Z")
	scan(t, manager)
	assertWorking(t, base, "tasks-personal-b.md", false)

	writeWorkCardFixture(t, todoDir, "tasks-helm-work-card.md", "2026-10-15T16:00:00Z")
	scan(t, manager)
	assertWorking(t, base, "tasks-helm-work-card.md", true)
	assertWorking(t, base, "tasks-personal-b.md", false)
	finish()
}

func TestWorkWindowOutsideHoursPersonalWorkUsesEverySlot(t *testing.T) {
	manager, finish, todoDir, base := slots(t, 2)
	setWorkWindow(manager, workWindowFixture(), outsideWindow(t))
	writeClaimFixture(t, todoDir, "tasks-personal-a.md", "2026-10-15T10:00:00Z")
	writeClaimFixture(t, todoDir, "tasks-personal-b.md", "2026-10-15T10:05:00Z")
	scan(t, manager)
	scan(t, manager)
	assertWorking(t, base, "tasks-personal-a.md", true)
	assertWorking(t, base, "tasks-personal-b.md", true)
	finish()
}

func TestWorkWindowDisabledChangesNothing(t *testing.T) {
	manager, finish, todoDir, base := slots(t, 2)
	window := workWindowFixture()
	window.Enabled = false
	setWorkWindow(manager, window, inWindow(t))
	writeClaimFixture(t, todoDir, "tasks-personal-a.md", "2026-10-15T10:00:00Z")
	writeClaimFixture(t, todoDir, "tasks-personal-b.md", "2026-10-15T10:05:00Z")
	scan(t, manager)
	scan(t, manager)
	assertWorking(t, base, "tasks-personal-a.md", true)
	assertWorking(t, base, "tasks-personal-b.md", true)
	finish()
}

// A running work card is not personal work: it must not use up the personal
// allowance. Four slots, one reserved: a work card plus three personal tasks fit.
func TestWorkWindowRunningWorkCardDoesNotEatThePersonalAllowance(t *testing.T) {
	manager, finish, todoDir, base := slots(t, 4)
	setWorkWindow(manager, workWindowFixture(), inWindow(t))
	writeWorkCardFixture(t, todoDir, "tasks-helm-work-card.md", "2026-10-15T16:00:00Z")
	writeClaimFixture(t, todoDir, "tasks-personal-a.md", "2026-10-15T10:00:00Z")
	writeClaimFixture(t, todoDir, "tasks-personal-b.md", "2026-10-15T10:01:00Z")
	writeClaimFixture(t, todoDir, "tasks-personal-c.md", "2026-10-15T10:02:00Z")
	writeClaimFixture(t, todoDir, "tasks-personal-d.md", "2026-10-15T10:03:00Z")
	for i := 0; i < 5; i++ {
		scan(t, manager)
	}
	assertWorking(t, base, "tasks-helm-work-card.md", true)
	assertWorking(t, base, "tasks-personal-a.md", true)
	assertWorking(t, base, "tasks-personal-b.md", true)
	assertWorking(t, base, "tasks-personal-c.md", true)
	assertWorking(t, base, "tasks-personal-d.md", false)
	finish()
}

// Personal work already running when the window opens is never stopped, and it
// holds back new personal claims until the count drops under the allowance.
func TestWorkWindowNeverStopsRunningPersonalWork(t *testing.T) {
	manager, finish, todoDir, base := slots(t, 2)
	setWorkWindow(manager, workWindowFixture(), outsideWindow(t))
	writeClaimFixture(t, todoDir, "tasks-personal-a.md", "2026-10-15T10:00:00Z")
	writeClaimFixture(t, todoDir, "tasks-personal-b.md", "2026-10-15T10:05:00Z")
	scan(t, manager)
	scan(t, manager)
	assertWorking(t, base, "tasks-personal-a.md", true)
	assertWorking(t, base, "tasks-personal-b.md", true)

	// The window opens with both slots full of personal work.
	setWorkWindow(manager, workWindowFixture(), inWindow(t))
	writeWorkCardFixture(t, todoDir, "tasks-helm-work-card.md", "2026-10-15T16:00:00Z")
	scan(t, manager)
	assertWorking(t, base, "tasks-personal-a.md", true)
	assertWorking(t, base, "tasks-personal-b.md", true)
	assertWorking(t, base, "tasks-helm-work-card.md", false)
	finish()
}

// Inside the window a work card claims ahead of an older goal-derived task.
func TestWorkWindowWorkCardClaimsAheadOfGoalWork(t *testing.T) {
	cfg, fake, manager, finish := claimOrderTestManager(t)
	setWorkWindow(manager, workWindowFixture(), inWindow(t))
	todoDir := cfg.Resolve(workflowRoutes()[0].Source)
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-old.md", "2026-10-14T10:00:00Z")
	writeWorkCardFixture(t, todoDir, "tasks-helm-work-card.md", "2026-10-15T16:00:00Z")
	scan(t, manager)
	claimOrderAssertSingleClaim(t, cfg, fake, finish, "tasks-helm-work-card.md")
}

// Outside the window the existing order stands: goal work first.
func TestWorkWindowOutsideHoursGoalWorkStillClaimsFirst(t *testing.T) {
	cfg, fake, manager, finish := claimOrderTestManager(t)
	setWorkWindow(manager, workWindowFixture(), outsideWindow(t))
	todoDir := cfg.Resolve(workflowRoutes()[0].Source)
	writeGoalDerivedFixture(t, todoDir, "tasks-goal-old.md", "2026-10-14T10:00:00Z")
	writeWorkCardFixture(t, todoDir, "tasks-helm-work-card.md", "2026-10-15T16:00:00Z")
	scan(t, manager)
	claimOrderAssertSingleClaim(t, cfg, fake, finish, "tasks-goal-old.md")
}

// The reserve covers goal planning too: a goal waiting to be planned does not
// take the last slot.
func TestWorkWindowKeepsTheLastSlotFromGoalPlanning(t *testing.T) {
	cfg, _, manager, finish := claimOrderTestManager(t)
	manager.capacityMu.Lock()
	manager.capacityLimit = 2
	manager.capacityMu.Unlock()
	setWorkWindow(manager, workWindowFixture(), inWindow(t))
	taskRoute := workflowRoutes()[0]
	goalRoute := workflowRoutes()[1]
	writeClaimFixture(t, cfg.Resolve(taskRoute.Source), "tasks-personal-a.md", "2026-10-15T10:00:00Z")
	scan(t, manager)
	goal, err := Create(cfg, "goals", "goal planning must not take the reserved slot", "")
	if err != nil {
		t.Fatal(err)
	}
	scan(t, manager)
	goalBase := filepath.Dir(cfg.Resolve(goalRoute.Source))
	if _, err := os.Stat(filepath.Join(goalBase, "planning", filepath.Base(goal))); err == nil {
		t.Fatal("a goal took the slot reserved for work Claude")
	}
	finish()
}

// The live path end to end: the window is written to config.yaml, saved the
// way the settings console saves it, loaded back from disk, and the manager
// built from that file holds the reserved slot.
func TestWorkWindowSurvivesConfigFileRoundTripAndHoldsTheSlot(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Orchestrator.MaxParallel = 2
	cfg.Orchestrator.WorkWindow = workWindowFixture()
	cfg.Orchestrator.WorkWindow.Days = []string{"mon", "tue", "wed", "thu", "fri"}
	if err := config.Save(cfg); err != nil {
		t.Fatal(err)
	}
	loaded, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	got := loaded.Orchestrator.WorkWindow
	if !got.Enabled || got.Start != "09:00" || got.End != "18:00" || got.Timezone != "America/New_York" || got.Reserved() != 1 || len(got.Days) != 5 {
		t.Fatalf("work_window did not survive a save and load: %+v", got)
	}

	release := make(chan struct{})
	fake := newFakeRecipient()
	fake.beforeEmit = func() { <-release }
	manager := New(loaded, fake, extensions.Runner{Directory: filepath.Join(root, "missing")})
	t.Cleanup(func() { close(release); manager.Wait() })
	now := inWindow(t)
	manager.workClock = func() time.Time { return now }
	todoDir := loaded.Resolve(workflowRoutes()[0].Source)
	base := filepath.Dir(todoDir)
	writeClaimFixture(t, todoDir, "tasks-personal-a.md", "2026-10-15T10:00:00Z")
	writeClaimFixture(t, todoDir, "tasks-personal-b.md", "2026-10-15T10:05:00Z")
	writeWorkCardFixture(t, todoDir, "tasks-helm-work-card.md", "2026-10-15T16:00:00Z")
	scan(t, manager)
	scan(t, manager)
	assertWorking(t, base, "tasks-helm-work-card.md", true)
	assertWorking(t, base, "tasks-personal-a.md", true)
	assertWorking(t, base, "tasks-personal-b.md", false)
}
