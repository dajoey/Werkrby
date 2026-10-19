package orchestrator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/workspace"
)

// testResumeCredit is the front-matter contract external resume paths read
// about; spelled out so these tests also compile against a tree without it.
const testResumeCredit = "resume_credit"

// A task's `attempt` feeds both the roster escalation ladder and the fleet's
// attempt cap. On 2026-09-24, 30 of 40 ops tasks with attempt>1 had climbed
// through a park (a wake_at timer or a Helm question) or a restart, and only
// five through a genuine model failure. These tests pin the accounting rule:
// a park that an agent turn ended in is not a failed attempt, so the claim
// that resumes it continues the same attempt; anything else still spends one.

func editFrontMatter(t *testing.T, path string, edit func(map[string]any)) {
	t.Helper()
	document, err := ReadDocument(path)
	if err != nil {
		t.Fatal(err)
	}
	edit(document.FrontMatter)
	if err := WriteDocument(path, document); err != nil {
		t.Fatal(err)
	}
}

func scanAndWait(t *testing.T, manager *Manager) {
	t.Helper()
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
}

func claimedAttempt(t *testing.T, manager *Manager, working string) (Document, Lease) {
	t.Helper()
	document, err := ReadDocument(working)
	if err != nil {
		t.Fatalf("task was not claimed back into working/: %v", err)
	}
	leases, err := manager.loadLeases()
	if err != nil {
		t.Fatal(err)
	}
	for _, lease := range leases {
		if filepath.Clean(lease.File) == filepath.Clean(working) {
			return document, lease
		}
	}
	t.Fatalf("no lease owns %s: %#v", working, leases)
	return Document{}, Lease{}
}

// parkDuringTurn makes the first implementation turn park its task in
// waiting/, the way an agent does: front matter first, then the move.
func parkDuringTurn(t *testing.T, fake *fakeHarness, base, name string, edit func(map[string]any)) {
	var once sync.Once
	fake.beforeEmit = func() {
		once.Do(func() {
			working := filepath.Join(base, "working", name)
			editFrontMatter(t, working, edit)
			if err := moveDocument(working, filepath.Join(base, "waiting", name), "waiting", time.Now().UTC()); err != nil {
				t.Errorf("park: %v", err)
			}
		})
	}
}

// resumeLikeParksPump reproduces ~/ops/werkrby_parks.py move_item on a Joey
// answer: it rewrites the front matter it read (dropping the park keys and
// keeping every other field verbatim), then renames waiting/ -> todo/.
func resumeLikeParksPump(t *testing.T, base, name string) {
	t.Helper()
	waiting := filepath.Join(base, "waiting", name)
	editFrontMatter(t, waiting, func(fm map[string]any) {
		for _, key := range []string{"joey_ask", "wake_at", "waiting_for", "completion_summary"} {
			delete(fm, key)
		}
		fm["joey_answer"] = map[string]any{"value": "proceed"}
		fm["status"] = "todo"
	})
	if err := os.Rename(waiting, filepath.Join(base, "todo", name)); err != nil {
		t.Fatal(err)
	}
}

func TestWakeResumedParkDoesNotSpendAnAttempt(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	route := workflowRoutes()[0]
	task, err := Create(cfg, "tasks", "park on a clock and resume", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	base := filepath.Dir(cfg.Resolve(route.Source))
	parkDuringTurn(t, fake, base, name, func(fm map[string]any) {
		fm["waiting_for"] = "the nightly build to publish"
		fm["wake_at"] = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	})

	scanAndWait(t, manager) // claim attempt 1; the turn parks it
	if err := manager.reconcileTransitions(context.Background()); err != nil {
		t.Fatal(err)
	}
	editFrontMatter(t, filepath.Join(base, "waiting", name), func(fm map[string]any) {
		fm["wake_at"] = time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	})
	scanAndWait(t, manager) // wake it and claim it again

	working := filepath.Join(base, "working", name)
	document, lease := claimedAttempt(t, manager, working)
	if got := numberValue(document.FrontMatter["attempt"]); got != 1 {
		t.Fatalf("attempt after a wake-resumed park = %d, want 1 (the park is not a failed attempt)", got)
	}
	if lease.ClaimAttempt != 1 {
		t.Fatalf("lease ClaimAttempt = %d, want 1", lease.ClaimAttempt)
	}
	if _, present := document.FrontMatter[testResumeCredit]; present {
		t.Fatalf("%s survived the claim that consumed it: %#v", testResumeCredit, document.FrontMatter)
	}
	if !strings.Contains(document.Body, "scheduled wake condition became due") || !strings.Contains(document.Body, "attempt not spent") {
		t.Fatalf("resume was not journaled as an unspent attempt: %s", document.Body)
	}
}

func TestExternallyResumedParkDoesNotSpendAnAttempt(t *testing.T) {
	// Joey answers the Helm question; werkrby_parks.py moves the document back
	// to todo/. It only has to keep the front matter it did not name.
	cfg, fake, manager := workflowTestManager(t)
	route := workflowRoutes()[0]
	task, err := Create(cfg, "tasks", "park on Joey and resume", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	base := filepath.Dir(cfg.Resolve(route.Source))
	parkDuringTurn(t, fake, base, name, func(fm map[string]any) {
		fm["waiting_for"] = "Joey to pick a vendor"
		fm["joey_ask"] = map[string]any{"kind": "choice", "prompt": "Which vendor?"}
	})

	scanAndWait(t, manager) // claim attempt 1; the turn parks it on Joey
	scanAndWait(t, manager) // reconcile the park; nothing is due
	if _, err := os.Stat(filepath.Join(base, "waiting", name)); err != nil {
		t.Fatalf("park on Joey did not stay in waiting/: %v", err)
	}
	resumeLikeParksPump(t, base, name)
	scanAndWait(t, manager)

	document, lease := claimedAttempt(t, manager, filepath.Join(base, "working", name))
	if got := numberValue(document.FrontMatter["attempt"]); got != 1 || lease.ClaimAttempt != 1 {
		t.Fatalf("attempt after an answered park = %d (lease %d), want 1", got, lease.ClaimAttempt)
	}
	if _, present := document.FrontMatter[testResumeCredit]; present {
		t.Fatalf("%s survived the claim that consumed it", testResumeCredit)
	}
}

func TestReviewerParkResumesTheAcceptedImplementationAttempt(t *testing.T) {
	// A reviewer that accepts the work but needs a human confirmation parks
	// the task instead of rejecting it; no implementation attempt failed.
	cfg, fake, manager := workflowTestManager(t)
	route := workflowRoutes()[0]
	task, err := Create(cfg, "tasks", "accepted build awaiting an in-game grade", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	base := filepath.Dir(cfg.Resolve(route.Source))
	turn := 0
	fake.beforeEmit = func() {
		turn++
		switch turn {
		case 1: // implementation submits for review
			_ = moveDocument(filepath.Join(base, "working", name), filepath.Join(base, "review", name), "review", time.Now().UTC())
		case 2: // review accepts and parks on a clock
			reviewing := filepath.Join(base, "reviewing", name)
			editFrontMatter(t, reviewing, func(fm map[string]any) {
				fm["waiting_for"] = "the in-game grade"
				fm["wake_at"] = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
			})
			_ = moveDocument(reviewing, filepath.Join(base, "waiting", name), "waiting", time.Now().UTC())
		}
	}
	scanAndWait(t, manager) // 1: implementation submits for review
	scanAndWait(t, manager) // 2: review accepts and parks
	editFrontMatter(t, filepath.Join(base, "waiting", name), func(fm map[string]any) {
		fm["wake_at"] = time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	})
	scanAndWait(t, manager) // 3: wakes from waiting and claims implementation
	document, lease := claimedAttempt(t, manager, filepath.Join(base, "working", name))
	if got := numberValue(document.FrontMatter["attempt"]); got != 1 || lease.ClaimAttempt != 1 {
		t.Fatalf("implementation attempt after a reviewer park = %d (lease %d), want 1", got, lease.ClaimAttempt)
	}
	if got := numberValue(document.FrontMatter["review_attempt"]); got != 1 {
		t.Fatalf("review_attempt = %d, want the one review that ran", got)
	}
}

func TestRequeueAfterAWorkTurnStillSpendsAnAttempt(t *testing.T) {
	for _, test := range []struct {
		name   string
		forged bool
	}{
		{name: "incomplete work returned to todo"},
		// An agent turn cannot mint the credit for itself: Werkrby strips it
		// from every transition it reconciles that is not a park.
		{name: "turn writes its own resume credit", forged: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg, fake, manager := workflowTestManager(t)
			route := workflowRoutes()[0]
			task, err := Create(cfg, "tasks", "genuinely incomplete work", "")
			if err != nil {
				t.Fatal(err)
			}
			name := filepath.Base(task)
			base := filepath.Dir(cfg.Resolve(route.Source))
			var once sync.Once
			fake.beforeEmit = func() {
				once.Do(func() {
					working := filepath.Join(base, "working", name)
					if test.forged {
						editFrontMatter(t, working, func(fm map[string]any) { fm[testResumeCredit] = true })
					}
					_ = moveDocument(working, filepath.Join(base, "todo", name), "todo", time.Now().UTC())
				})
			}
			scanAndWait(t, manager)
			scanAndWait(t, manager)
			document, lease := claimedAttempt(t, manager, filepath.Join(base, "working", name))
			if got := numberValue(document.FrontMatter["attempt"]); got != 2 || lease.ClaimAttempt != 2 {
				t.Fatalf("attempt after a requeue = %d (lease %d), want 2", got, lease.ClaimAttempt)
			}
			if strings.Contains(document.Body, "attempt not spent") {
				t.Fatalf("a work requeue was journaled as unspent: %s", document.Body)
			}
		})
	}
}

func TestResumeCreditIsConsumedByExactlyOneClaim(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	route := workflowRoutes()[0]
	task, err := Create(cfg, "tasks", "credited once", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	base := filepath.Dir(cfg.Resolve(route.Source))
	editFrontMatter(t, task, func(fm map[string]any) {
		fm["attempt"] = 2
		fm[testResumeCredit] = true
	})
	var once sync.Once
	fake.beforeEmit = func() {
		once.Do(func() {
			_ = moveDocument(filepath.Join(base, "working", name), filepath.Join(base, "todo", name), "todo", time.Now().UTC())
		})
	}
	scanAndWait(t, manager)
	scanAndWait(t, manager)
	document, lease := claimedAttempt(t, manager, filepath.Join(base, "working", name))
	if got := numberValue(document.FrontMatter["attempt"]); got != 3 || lease.ClaimAttempt != 3 {
		t.Fatalf("attempt = %d (lease %d), want 3: the credited claim keeps 2, the next genuine requeue spends 3", got, lease.ClaimAttempt)
	}
	if strings.Count(document.Body, "attempt not spent") != 1 {
		t.Fatalf("credit should be journaled exactly once: %s", document.Body)
	}
}

func TestResumeCreditNeedsAnAttemptToContinue(t *testing.T) {
	// A document that never ran has nothing to continue: attempt 0 -> 1.
	cfg, _, manager := workflowTestManager(t)
	route := workflowRoutes()[0]
	task, err := Create(cfg, "tasks", "never ran", "")
	if err != nil {
		t.Fatal(err)
	}
	editFrontMatter(t, task, func(fm map[string]any) { fm[testResumeCredit] = true })
	scanAndWait(t, manager)
	document, lease := claimedAttempt(t, manager, filepath.Join(filepath.Dir(cfg.Resolve(route.Source)), "working", filepath.Base(task)))
	if got := numberValue(document.FrontMatter["attempt"]); got != 1 || lease.ClaimAttempt != 1 {
		t.Fatalf("attempt = %d (lease %d), want 1", got, lease.ClaimAttempt)
	}
	if _, present := document.FrontMatter[testResumeCredit]; present {
		t.Fatalf("%s must be consumed even when it cannot be honoured", testResumeCredit)
	}
}

func TestCreditedClaimInterruptedAfterRenameIsNotCreditedTwice(t *testing.T) {
	// The claim renamed the document but crashed before its metadata write,
	// so the file in working/ still carries the mark. Recovery finishes the
	// claim from the journaled attempt and must not leave the mark behind for
	// a later requeue to spend.
	cfg, _, manager := workflowTestManager(t)
	route := workflowRoutes()[0]
	task, err := Create(cfg, "tasks", "credited claim crashes mid-way", "")
	if err != nil {
		t.Fatal(err)
	}
	editFrontMatter(t, task, func(fm map[string]any) {
		fm["attempt"] = 1
		fm[testResumeCredit] = true
	})
	failed := false
	manager.claimDocument = func(source, target, status, attemptField string, now time.Time) (Document, error) {
		if !failed {
			failed = true
			return claimDocumentWithWriter(source, target, status, attemptField, now, func(string, Document) error {
				return errors.New("injected metadata replacement failure")
			})
		}
		return claimDocument(source, target, status, attemptField, now)
	}
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	scanAndWait(t, manager)
	document, lease := claimedAttempt(t, manager, filepath.Join(filepath.Dir(cfg.Resolve(route.Source)), "working", filepath.Base(task)))
	if got := numberValue(document.FrontMatter["attempt"]); got != 1 || lease.ClaimAttempt != 1 {
		t.Fatalf("attempt = %d (lease %d), want the credited 1", got, lease.ClaimAttempt)
	}
	if _, present := document.FrontMatter[testResumeCredit]; present {
		t.Fatalf("%s survived a recovered claim: %#v", testResumeCredit, document.FrontMatter)
	}
	if numberValue(document.FrontMatter[testCreditsUsed]) != 1 || numberValue(document.FrontMatter[testCreditsAttempt]) != 1 {
		t.Fatalf("recovered credited claim did not use the budget: %#v", document.FrontMatter)
	}
}

func TestParkMadeBetweenAttemptsStillSpendsTheNextAttempt(t *testing.T) {
	// The fleet attempt cap parks a task straight out of todo/ after its third
	// attempt already ended. That park did not interrupt an attempt, so the
	// resume after Joey's go-ahead is a new attempt (3 -> 4).
	cfg, _, manager := workflowTestManager(t)
	route := workflowRoutes()[0]
	task, err := Create(cfg, "tasks", "attempt-capped task", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	base := filepath.Dir(cfg.Resolve(route.Source))
	editFrontMatter(t, task, func(fm map[string]any) {
		fm["attempt"] = 3
		fm["joey_ask"] = map[string]any{"kind": "choice", "prompt": "Used 3 attempts. What next?"}
	})
	if err := os.Rename(task, filepath.Join(base, "waiting", name)); err != nil {
		t.Fatal(err)
	}
	scanAndWait(t, manager)
	resumeLikeParksPump(t, base, name)
	scanAndWait(t, manager)
	document, lease := claimedAttempt(t, manager, filepath.Join(base, "working", name))
	if got := numberValue(document.FrontMatter["attempt"]); got != 4 || lease.ClaimAttempt != 4 {
		t.Fatalf("attempt = %d (lease %d), want 4", got, lease.ClaimAttempt)
	}
}

// directDoneTurns makes implementation turn N (1-based) complete directly
// with summaries[N-1]; a nil entry or a turn past the list leaves the task in
// working/. updated_at and completed_at are written in one edit, as required.
func directDoneTurns(t *testing.T, cfg config.Config, name string, summaries ...map[string]any) *fakeHarness {
	t.Helper()
	route := workflowRoutes()[0]
	base := filepath.Dir(cfg.Resolve(route.Source))
	fake := newFakeRecipient()
	turn := 0
	fake.beforeEmit = func() {
		turn++
		if turn > len(summaries) || summaries[turn-1] == nil {
			return
		}
		working := filepath.Join(base, "working", name)
		now := time.Now().UTC().Truncate(time.Second)
		summary := map[string]any{}
		for key, value := range summaries[turn-1] {
			summary[key] = value
		}
		if _, ok := summary["completed_at"]; ok {
			summary["completed_at"] = now.Format(time.RFC3339)
		}
		editFrontMatter(t, working, func(fm map[string]any) {
			if len(summary) == 0 {
				delete(fm, "completion_summary") // no summary written at all
				return
			}
			fm["completion_summary"] = summary
		})
		if err := moveDocument(working, filepath.Join(base, "done", name), "done", now); err != nil {
			t.Errorf("direct done: %v", err)
		}
	}
	return fake
}

func noReviewTaskManager(t *testing.T, title string, summaries ...map[string]any) (config.Config, *Manager, string, string) {
	t.Helper()
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	task, err := CreateWithOptions(cfg, "tasks", title, "", CreateOptions{NoReview: true})
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	fake := directDoneTurns(t, cfg, name, summaries...)
	manager := New(cfg, fake, extensions.Runner{Directory: filepath.Join(root, "missing")})
	return cfg, manager, filepath.Dir(cfg.Resolve(workflowRoutes()[0].Source)), name
}

func TestFormatOnlyDirectCompletionRejectionKeepsTheAttemptOnce(t *testing.T) {
	// The work is done and recorded; only the summary's shape is wrong
	// (2026-09-24 audit: verdict "done", a 526-char outcome, a host path).
	// The first such rejection in an attempt is repaired without spending
	// the attempt; a second one in the same attempt spends it as before.
	misshaped := map[string]any{
		"verdict": "done", "outcome": "Rotated the key and restarted the stack.",
		"evidence": "Health endpoint returned 200.", "uncertainty": "None known.",
		"completed_at": "",
	}
	_, manager, base, name := noReviewTaskManager(t, "rotate a key", misshaped, misshaped)

	scanAndWait(t, manager) // attempt 1 completes directly with a misshaped summary
	scanAndWait(t, manager) // rejected on format; the repair turn is still attempt 1
	done, err := ReadDocument(filepath.Join(base, "done", name))
	if err != nil {
		t.Fatalf("repair turn did not complete again: %v", err)
	}
	if got := numberValue(done.FrontMatter["attempt"]); got != 1 {
		t.Fatalf("attempt after one format-only rejection = %d, want 1", got)
	}
	if !strings.Contains(done.Body, `verdict must be "completed" for a direct done, found "done"`) {
		t.Fatalf("rejection note lost the exact failing rule: %s", done.Body)
	}

	scanAndWait(t, manager) // second format rejection inside attempt 1 spends it
	document, lease := claimedAttempt(t, manager, filepath.Join(base, "working", name))
	if got := numberValue(document.FrontMatter["attempt"]); got != 2 || lease.ClaimAttempt != 2 {
		t.Fatalf("attempt after a repeated format rejection = %d (lease %d), want 2", got, lease.ClaimAttempt)
	}
}

func TestSubstantiveDirectCompletionRejectionSpendsTheAttempt(t *testing.T) {
	// No summary at all means no recorded evidence: that is incomplete work.
	_, manager, base, name := noReviewTaskManager(t, "complete without evidence", map[string]any{})
	scanAndWait(t, manager)
	scanAndWait(t, manager)
	document, lease := claimedAttempt(t, manager, filepath.Join(base, "working", name))
	if got := numberValue(document.FrontMatter["attempt"]); got != 2 || lease.ClaimAttempt != 2 {
		t.Fatalf("attempt after a missing-evidence rejection = %d (lease %d), want 2", got, lease.ClaimAttempt)
	}
}

// The credit budget: a task that keeps re-parking on a clock inside one
// attempt must still reach the ladder and the cap eventually.
const (
	testCreditsUsed    = "resume_credits_used"
	testCreditsAttempt = "resume_credits_attempt"
)

// parkEveryTurn makes every implementation turn park its task on a wake_at
// that is already due, so each scan reconciles, wakes, and re-claims it.
func parkEveryTurn(t *testing.T, fake *fakeHarness, base, name string) {
	fake.beforeEmit = func() {
		working := filepath.Join(base, "working", name)
		if _, err := os.Stat(working); err != nil {
			return
		}
		editFrontMatter(t, working, func(fm map[string]any) {
			fm["waiting_for"] = "the upstream release"
			fm["wake_at"] = time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
		})
		if err := moveDocument(working, filepath.Join(base, "waiting", name), "waiting", time.Now().UTC()); err != nil {
			t.Errorf("park: %v", err)
		}
	}
}

func TestResumeCreditBudgetIsFiveParksPerAttempt(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	route := workflowRoutes()[0]
	task, err := Create(cfg, "tasks", "re-parks on a clock forever", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	base := filepath.Dir(cfg.Resolve(route.Source))
	waiting := filepath.Join(base, "waiting", name)
	parkEveryTurn(t, fake, base, name)

	scanAndWait(t, manager) // attempt 1 runs and parks
	for honoured := 1; honoured <= 5; honoured++ {
		scanAndWait(t, manager) // wake, credited claim, park again
		document, err := ReadDocument(waiting)
		if err != nil {
			t.Fatalf("resume %d: %v", honoured, err)
		}
		if got := numberValue(document.FrontMatter["attempt"]); got != 1 {
			t.Fatalf("resume %d: attempt = %d, want 1 while the budget lasts", honoured, got)
		}
		if got := numberValue(document.FrontMatter[testCreditsUsed]); got != honoured || numberValue(document.FrontMatter[testCreditsAttempt]) != 1 {
			t.Fatalf("resume %d: credits used = %v for attempt %v, want %d for attempt 1", honoured, document.FrontMatter[testCreditsUsed], document.FrontMatter[testCreditsAttempt], honoured)
		}
	}
	scanAndWait(t, manager) // sixth resume in attempt 1: not honoured
	document, err := ReadDocument(waiting)
	if err != nil {
		t.Fatal(err)
	}
	if got := numberValue(document.FrontMatter["attempt"]); got != 2 {
		t.Fatalf("sixth resume in one attempt: attempt = %d, want 2", got)
	}
	if !strings.Contains(document.Body, "5 parks in one attempt; this claim spends attempt 2") {
		t.Fatalf("exhausted budget was not journaled: %s", document.Body)
	}

	scanAndWait(t, manager) // attempt 2 has a fresh budget
	document, err = ReadDocument(waiting)
	if err != nil {
		t.Fatal(err)
	}
	if got := numberValue(document.FrontMatter["attempt"]); got != 2 || numberValue(document.FrontMatter[testCreditsUsed]) != 1 || numberValue(document.FrontMatter[testCreditsAttempt]) != 2 {
		t.Fatalf("attempt 2 budget = attempt %v, used %v for attempt %v; want 2, 1, 2", document.FrontMatter["attempt"], document.FrontMatter[testCreditsUsed], document.FrontMatter[testCreditsAttempt])
	}
}

func TestResumeCreditBudgetResetsOnAGenuineNewAttempt(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	route := workflowRoutes()[0]
	task, err := Create(cfg, "tasks", "exhausted budget, then real rework", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	base := filepath.Dir(cfg.Resolve(route.Source))
	// Attempt 1 used its whole budget, then its work came back incomplete.
	editFrontMatter(t, task, func(fm map[string]any) {
		fm["attempt"] = 1
		fm[testCreditsAttempt] = 1
		fm[testCreditsUsed] = 5
	})
	parkEveryTurn(t, fake, base, name)
	scanAndWait(t, manager) // genuine claim: attempt 2, which parks
	scanAndWait(t, manager) // its park is honoured from a fresh budget
	document, err := ReadDocument(filepath.Join(base, "waiting", name))
	if err != nil {
		t.Fatal(err)
	}
	if got := numberValue(document.FrontMatter["attempt"]); got != 2 {
		t.Fatalf("attempt = %d, want 2 (one genuine new attempt, then a credited resume)", got)
	}
	if numberValue(document.FrontMatter[testCreditsUsed]) != 1 || numberValue(document.FrontMatter[testCreditsAttempt]) != 2 {
		t.Fatalf("budget did not reset: used %v for attempt %v, want 1 for attempt 2", document.FrontMatter[testCreditsUsed], document.FrontMatter[testCreditsAttempt])
	}
}

func TestFormatOnlyRepairSpendsTheSameCreditBudget(t *testing.T) {
	misshaped := map[string]any{
		"verdict": "complete", "outcome": "Rotated the key.", "evidence": "Health 200.",
		"uncertainty": "None known.", "completed_at": "",
	}
	_, manager, base, name := noReviewTaskManager(t, "format repair counts", misshaped)
	scanAndWait(t, manager) // attempt 1 completes with a misshaped summary
	scanAndWait(t, manager) // format-only repair claim, still attempt 1
	document, lease := claimedAttempt(t, manager, filepath.Join(base, "working", name))
	if numberValue(document.FrontMatter["attempt"]) != 1 || lease.ClaimAttempt != 1 {
		t.Fatalf("attempt = %v (lease %d), want 1", document.FrontMatter["attempt"], lease.ClaimAttempt)
	}
	if numberValue(document.FrontMatter[testCreditsUsed]) != 1 || numberValue(document.FrontMatter[testCreditsAttempt]) != 1 {
		t.Fatalf("format repair did not use the budget: used %v for attempt %v", document.FrontMatter[testCreditsUsed], document.FrontMatter[testCreditsAttempt])
	}
}

// reviewTurns drives implementation -> review, then lets review do reviewer.
func reviewTurns(t *testing.T, fake *fakeHarness, base, name string, reviewer func(reviewing string)) {
	turn := 0
	fake.beforeEmit = func() {
		turn++
		switch turn {
		case 1:
			_ = moveDocument(filepath.Join(base, "working", name), filepath.Join(base, "review", name), "review", time.Now().UTC())
		case 2:
			reviewer(filepath.Join(base, "reviewing", name))
		}
	}
}

func TestRiskHighReviewHandOffKeepsTheAttempt(t *testing.T) {
	// A routine reviewer that finds the task should have been risk: high gives
	// no verdict: it stamps the risk and returns the task so the implementer
	// can hand it straight to the risk-high reviewer. Nothing was reworked
	// (2026-09-24: tasks-20260924-daily-report-factcheck-drops-real-times went
	// 1 -> 2 on the hand-off and hit the top rung early).
	cfg, fake, manager := workflowTestManager(t)
	route := workflowRoutes()[0]
	task, err := Create(cfg, "tasks", "changes what Joey reads", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	base := filepath.Dir(cfg.Resolve(route.Source))
	reviewTurns(t, fake, base, name, func(reviewing string) {
		editFrontMatter(t, reviewing, func(fm map[string]any) { fm["risk"] = "high" })
		_ = moveDocumentWithProgress(reviewing, filepath.Join(base, "todo", name), "todo", time.Now().UTC(), "no rework: needs the risk-high reviewer (criterion 3)")
	})
	for scan := 0; scan < 3; scan++ {
		scanAndWait(t, manager)
	}
	document, lease := claimedAttempt(t, manager, filepath.Join(base, "working", name))
	if got := numberValue(document.FrontMatter["attempt"]); got != 1 || lease.ClaimAttempt != 1 {
		t.Fatalf("attempt after a risk-high hand-off = %d (lease %d), want 1", got, lease.ClaimAttempt)
	}
	if numberValue(document.FrontMatter[testCreditsUsed]) != 1 || numberValue(document.FrontMatter[testCreditsAttempt]) != 1 {
		t.Fatalf("hand-off did not use the credit budget: used %v for attempt %v", document.FrontMatter[testCreditsUsed], document.FrontMatter[testCreditsAttempt])
	}
}

func TestReviewRejectionWithoutARiskEscalationStillSpendsTheAttempt(t *testing.T) {
	for _, test := range []struct {
		name        string
		initialRisk string
	}{
		{name: "routine rejection, risk unchanged"},
		// Already high before review: the reviewer rejecting it did not raise
		// anything, so this is ordinary rework.
		{name: "risk-high rejection", initialRisk: "high"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg, fake, manager := workflowTestManager(t)
			route := workflowRoutes()[0]
			task, err := Create(cfg, "tasks", "reviewer finds real defects", "")
			if err != nil {
				t.Fatal(err)
			}
			if test.initialRisk != "" {
				editFrontMatter(t, task, func(fm map[string]any) { fm["risk"] = test.initialRisk })
			}
			name := filepath.Base(task)
			base := filepath.Dir(cfg.Resolve(route.Source))
			reviewTurns(t, fake, base, name, func(reviewing string) {
				_ = moveDocumentWithProgress(reviewing, filepath.Join(base, "todo", name), "todo", time.Now().UTC(), "rejected: criterion 2 fails")
			})
			for scan := 0; scan < 3; scan++ {
				scanAndWait(t, manager)
			}
			document, lease := claimedAttempt(t, manager, filepath.Join(base, "working", name))
			if got := numberValue(document.FrontMatter["attempt"]); got != 2 || lease.ClaimAttempt != 2 {
				t.Fatalf("attempt after a review rejection = %d (lease %d), want 2", got, lease.ClaimAttempt)
			}
		})
	}
}
