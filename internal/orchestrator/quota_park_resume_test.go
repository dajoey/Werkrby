package orchestrator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A quota wall hit during review or goal planning used to park the document in
// waiting/ with nothing saying which phase it left, and the scheduled wake
// sent every task to todo/ (the implementer re-ran finished work) and every
// goal to review. Parks record resume_status and the wake honours it.

func quotaWall(resetIn time.Duration) error {
	return errors.New("429 rate limit exceeded; resets at " + time.Now().UTC().Add(resetIn).Truncate(time.Minute).Format(time.RFC3339))
}

func dueNow(t *testing.T, path string) {
	t.Helper()
	editFrontMatter(t, path, func(fm map[string]any) {
		fm["wake_at"] = time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	})
}

func TestBH_ReviewQuotaWallReturnsTaskToReview(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	route := workflowRoutes()[0]
	base := filepath.Dir(cfg.Resolve(route.Source))
	task, err := Create(cfg, "tasks", "quota wall during review", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	editFrontMatter(t, task, func(fm map[string]any) {
		fm["status"] = "review"
		fm["attempt"] = 1
		fm["agent"] = "kimi"
	})
	if err := os.MkdirAll(filepath.Join(base, "review"), 0o700); err != nil {
		t.Fatal(err)
	}
	review := filepath.Join(base, "review", name)
	if err := moveDocument(task, review, "review", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	fake.sendErrs = []error{quotaWall(2 * time.Hour)}
	scanAndWait(t, manager)

	waiting := filepath.Join(base, "waiting", name)
	parked, err := ReadDocument(waiting)
	if err != nil {
		t.Fatalf("review quota wall did not park the task in waiting/: %v", err)
	}
	if got := parked.FrontMatter["resume_status"]; got != "review" {
		t.Fatalf("resume_status = %v, want review: %#v", got, parked.FrontMatter)
	}

	dueNow(t, waiting)
	if err := manager.wakeWaitingDocuments(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(base, "todo", name)); err == nil {
		t.Fatal("woken review task went to todo/: the implementer would re-run finished work")
	}
	woken, err := ReadDocument(filepath.Join(base, "review", name))
	if err != nil {
		t.Fatalf("woken review task is not in review/: %v", err)
	}
	if _, left := woken.FrontMatter["resume_status"]; left {
		t.Fatalf("resume_status survived the wake and would misroute the next park: %#v", woken.FrontMatter)
	}

	// The next claim is a review of the same implementation attempt.
	scanAndWait(t, manager)
	claimed, err := ReadDocument(filepath.Join(base, "reviewing", name))
	if err != nil {
		t.Fatalf("woken task was not claimed for review: %v", err)
	}
	if got := numberValue(claimed.FrontMatter["attempt"]); got != 1 {
		t.Fatalf("implementation attempt = %d after the review resumed, want 1", got)
	}
}

func TestBH_ImplementationQuotaWallStillWakesToTodo(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	base := filepath.Dir(cfg.Resolve(workflowRoutes()[0].Source))
	task, err := Create(cfg, "tasks", "quota wall during implementation", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	fake.sendErrs = []error{quotaWall(2 * time.Hour)}
	scanAndWait(t, manager)

	waiting := filepath.Join(base, "waiting", name)
	parked, err := ReadDocument(waiting)
	if err != nil {
		t.Fatalf("implementation quota wall did not park the task: %v", err)
	}
	if got, present := parked.FrontMatter["resume_status"]; present {
		t.Fatalf("implementation park recorded resume_status %v", got)
	}
	dueNow(t, waiting)
	if err := manager.wakeWaitingDocuments(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(base, "todo", name)); err != nil {
		t.Fatalf("woken implementation task is not in todo/: %v", err)
	}
}

func TestBH_PlanningQuotaWallWakesGoalIntoPlanning(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	base := filepath.Dir(cfg.Resolve(workflowRoutes()[1].Source))
	goal, err := Create(cfg, "goals", "quota wall during planning", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(goal)
	fake.sendErrs = []error{quotaWall(2 * time.Hour)}
	scanAndWait(t, manager)

	waiting := filepath.Join(base, "waiting", name)
	parked, err := ReadDocument(waiting)
	if err != nil {
		t.Fatalf("planning quota wall did not park the goal in waiting/: %v", err)
	}
	if got := parked.FrontMatter["resume_status"]; got != "planning" {
		t.Fatalf("resume_status = %v, want planning: %#v", got, parked.FrontMatter)
	}

	dueNow(t, waiting)
	if err := manager.wakeWaitingDocuments(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if _, err := os.Stat(filepath.Join(base, "review", name)); err == nil {
		t.Fatal("woken planning goal went to review/ without a plan to review")
	}
	woken, err := ReadDocument(filepath.Join(base, "planning", name))
	if err != nil {
		t.Fatalf("woken goal is not back in planning/: %v", err)
	}
	if _, left := woken.FrontMatter["resume_status"]; left {
		t.Fatalf("resume_status survived the wake: %#v", woken.FrontMatter)
	}
}

func TestBH_GoalReviewQuotaWallWakesGoalIntoReview(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	base := filepath.Dir(cfg.Resolve(workflowRoutes()[1].Source))
	goal, err := Create(cfg, "goals", "quota wall during goal review", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(goal)
	editFrontMatter(t, goal, func(fm map[string]any) {
		fm["status"] = "review"
		fm["planning_attempt"] = 1
	})
	if err := os.MkdirAll(filepath.Join(base, "review"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := moveDocument(goal, filepath.Join(base, "review", name), "review", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	fake.sendErrs = []error{quotaWall(2 * time.Hour)}
	scanAndWait(t, manager)

	waiting := filepath.Join(base, "waiting", name)
	parked, err := ReadDocument(waiting)
	if err != nil {
		t.Fatalf("goal review quota wall did not park the goal: %v", err)
	}
	if got := parked.FrontMatter["resume_status"]; got != "review" {
		t.Fatalf("resume_status = %v, want review", got)
	}
	dueNow(t, waiting)
	if err := manager.wakeWaitingDocuments(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(base, "review", name)); err != nil {
		t.Fatalf("woken goal review is not in review/: %v", err)
	}
}
