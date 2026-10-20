package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/roster"
	"github.com/agent0ai/spynel/internal/workspace"
)

func workflowTestManager(t *testing.T) (config.Config, *fakeHarness, *Manager) {
	t.Helper()
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	fake := newFakeRecipient()
	manager := New(cfg, fake, extensions.Runner{Directory: filepath.Join(root, "missing")})
	return cfg, fake, manager
}

func TestOrphanedClaimedTaskReceivesRecoveryLease(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	task, err := Create(cfg, "tasks", "recover an interrupted claim", "")
	if err != nil {
		t.Fatal(err)
	}
	working := filepath.Join(filepath.Dir(cfg.Resolve(workflowRoutes()[0].Source)), "working", filepath.Base(task))
	if _, err := ClaimDocument(task, working, "working", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	leases, err := manager.loadLeases()
	if err != nil || len(leases) != 1 {
		t.Fatalf("recovery leases = %#v, %v", leases, err)
	}
	if leases[0].Phase != phaseTaskImplementation || leases[0].ClaimAttempt != 1 || leases[0].RecoveryCount == 0 || fake.calls != 1 {
		t.Fatalf("orphan recovery = lease %#v, calls %d", leases[0], fake.calls)
	}
	recovered, err := ReadDocument(working)
	if err != nil || !strings.Contains(recovered.Body, "Werkrby started recovery attempt 1 for task implementation") {
		t.Fatalf("recovery progress was not journaled: body=%q err=%v", recovered.Body, err)
	}
}

func TestOrphanedNotifiedTaskKeepsTransitionIdentityThroughReconciliation(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	cfg.Orchestrator.TaskNotifications = config.TaskNotificationsAlways
	manager.ApplyRuntimeConfig(cfg)
	task, err := Create(cfg, "tasks", "recover and notify an interrupted claim", "")
	if err != nil {
		t.Fatal(err)
	}
	document, err := ReadDocument(task)
	if err != nil {
		t.Fatal(err)
	}
	document.FrontMatter["notify"] = map[string]any{"enabled": true, "origin": "tui/local", "on": []any{"failed"}}
	if err := WriteDocument(task, document); err != nil {
		t.Fatal(err)
	}
	working := filepath.Join(filepath.Dir(cfg.Resolve(workflowRoutes()[0].Source)), "working", filepath.Base(task))
	if _, err := ClaimDocument(task, working, "working", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	failed := filepath.Join(filepath.Dir(filepath.Dir(working)), "failed", filepath.Base(working))
	var settle sync.Once
	fake.beforeEmit = func() {
		settle.Do(func() {
			if err := moveDocument(working, failed, "failed", time.Now().UTC()); err != nil {
				t.Errorf("settle recovered task: %v", err)
			}
		})
	}
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if fake.calls != 2 || len(fake.prompts) != 2 || !strings.Contains(fake.prompts[1], failed) {
		t.Fatalf("recovered notification dispatch lost terminal task identity: calls=%d prompts=%d", fake.calls, len(fake.prompts))
	}
}

func TestJournaledClaimIsFinishedAfterRestart(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	task, err := Create(cfg, "tasks", "finish interrupted rename", "")
	if err != nil {
		t.Fatal(err)
	}
	document, err := ReadDocument(task)
	if err != nil {
		t.Fatal(err)
	}
	id := documentID(document)
	target := filepath.Join(filepath.Dir(cfg.Resolve(workflowRoutes()[0].Source)), "working", filepath.Base(task))
	now := time.Now().UTC()
	claimKey := leaseID("tasks:"+phaseTaskImplementation, id)
	lease := Lease{
		ID: claimKey, ClaimID: claimKey, DocumentType: "task", Route: "tasks", File: target, SourceFile: task,
		SessionKey: phaseSessionKey("tasks", id, phaseTaskImplementation, 1), State: "claiming", Phase: phaseTaskImplementation,
		ClaimAttempt: 1, StartedAt: now, HeartbeatAt: now,
	}
	if err := manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("journaled claim target = %v", err)
	}
	if _, err := os.Stat(task); !os.IsNotExist(err) {
		t.Fatalf("journaled source still exists: %v", err)
	}
	recovered, err := manager.loadLease(claimKey)
	if err != nil || recovered.State != "awaiting_transition" || recovered.SourceFile != "" || fake.calls != 1 {
		t.Fatalf("resumed journaled claim = %#v, calls %d, error %v", recovered, fake.calls, err)
	}
}

func TestJournaledReviewClaimFinishesMetadataAfterRenameOnlyCrash(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	task, err := Create(cfg, "tasks", "finish interrupted review rename", "")
	if err != nil {
		t.Fatal(err)
	}
	document, err := ReadDocument(task)
	if err != nil {
		t.Fatal(err)
	}
	document.FrontMatter["status"] = "review"
	document.FrontMatter["attempt"] = 1
	review := filepath.Join(filepath.Dir(task), "..", "review", filepath.Base(task))
	if err := os.MkdirAll(filepath.Dir(review), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := WriteDocument(review, document); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(task); err != nil {
		t.Fatal(err)
	}

	id := documentID(document)
	reviewing := filepath.Join(filepath.Dir(task), "..", "reviewing", filepath.Base(task))
	started := time.Now().UTC().Add(-time.Minute)
	claimKey := leaseID("tasks:"+phaseTaskReview, id)
	lease := Lease{
		ID: claimKey, ClaimID: claimKey, DocumentType: "task", Route: "tasks", File: reviewing, SourceFile: review,
		SessionKey: phaseSessionKey("tasks", id, phaseTaskReview, 1), State: "claiming", Phase: phaseTaskReview,
		ClaimAttempt: 1, StartedAt: started, HeartbeatAt: started,
	}
	if err := manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	// Reproduce a crash after the atomic rename but before claimDocument writes
	// the claimed status and phase-specific attempt metadata.
	if err := os.MkdirAll(filepath.Dir(reviewing), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(review, reviewing); err != nil {
		t.Fatal(err)
	}

	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	stored, err := ReadDocument(reviewing)
	if err != nil {
		t.Fatal(err)
	}
	if stored.FrontMatter["status"] != "reviewing" || stored.FrontMatter["attempt"] != 1 || stored.FrontMatter["review_attempt"] != 1 {
		t.Fatalf("recovered review metadata = %#v", stored.FrontMatter)
	}
	if fake.calls != 1 {
		t.Fatalf("review provider calls = %d, want 1", fake.calls)
	}
}

func TestPhaseClaimTargetCollisionDropsJournalWithoutDispatch(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	route := workflowRoutes()[0]
	base := filepath.Dir(cfg.Resolve(route.Source))
	source := filepath.Join(base, "review", "collision.md")
	target := filepath.Join(base, "reviewing", "collision.md")
	if err := WriteDocument(source, Document{FrontMatter: map[string]any{
		"id": "source", "status": "review", "attempt": 1,
	}, Body: "source body\n"}); err != nil {
		t.Fatal(err)
	}
	if err := WriteDocument(target, Document{FrontMatter: map[string]any{
		"id": "target", "status": "reviewing", "attempt": 7, "review_attempt": 4,
	}, Body: "target body\n"}); err != nil {
		t.Fatal(err)
	}

	if err := manager.scanPhaseQueue(context.Background(), route, filepath.Dir(source), filepath.Dir(target), phaseTaskReview); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if fake.calls != 0 {
		t.Fatalf("collision dispatched %d providers", fake.calls)
	}
	if leases, err := manager.loadLeases(); err != nil || len(leases) != 0 {
		t.Fatalf("collision leases = %#v, %v", leases, err)
	}
	storedSource, err := ReadDocument(source)
	if err != nil {
		t.Fatal(err)
	}
	storedTarget, err := ReadDocument(target)
	if err != nil {
		t.Fatal(err)
	}
	if documentID(storedSource) != "source" || stringField(storedSource, "status") != "review" || storedSource.Body != "source body\n" {
		t.Fatalf("collision source mutated = %#v, %q", storedSource.FrontMatter, storedSource.Body)
	}
	if documentID(storedTarget) != "target" || numberValue(storedTarget.FrontMatter["attempt"]) != 7 || numberValue(storedTarget.FrontMatter["review_attempt"]) != 4 || storedTarget.Body != "target body\n" {
		t.Fatalf("collision target mutated = %#v, %q", storedTarget.FrontMatter, storedTarget.Body)
	}
}

func TestPhaseClaimWriteFailureAfterRenameKeepsJournalForRecovery(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	task, err := Create(cfg, "tasks", "recover a failed post-rename metadata write", "")
	if err != nil {
		t.Fatal(err)
	}
	document, err := ReadDocument(task)
	if err != nil {
		t.Fatal(err)
	}
	document.FrontMatter["status"] = "review"
	document.FrontMatter["attempt"] = 1
	review := filepath.Join(filepath.Dir(task), "..", "review", filepath.Base(task))
	if err := os.MkdirAll(filepath.Dir(review), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := WriteDocument(review, document); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(task); err != nil {
		t.Fatal(err)
	}

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
	if fake.calls != 0 {
		t.Fatalf("provider ran after incomplete claim: %d calls", fake.calls)
	}
	leases, err := manager.loadLeases()
	if err != nil || len(leases) != 1 || leases[0].State != "claiming" || leases[0].ClaimAttempt != 1 {
		t.Fatalf("preserved claim journal = %#v, %v", leases, err)
	}
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	reviewing := filepath.Join(filepath.Dir(review), "..", "reviewing", filepath.Base(review))
	stored, err := ReadDocument(reviewing)
	if err != nil {
		t.Fatal(err)
	}
	if stored.FrontMatter["status"] != "reviewing" || stored.FrontMatter["attempt"] != 1 || stored.FrontMatter["review_attempt"] != 1 {
		t.Fatalf("recovered failed claim metadata = %#v", stored.FrontMatter)
	}
	if fake.calls != 1 {
		t.Fatalf("recovered provider calls = %d, want 1", fake.calls)
	}
}

func TestFreshLeaseFromPreviousManagerRecoversWithoutFullStaleDelay(t *testing.T) {
	cfg, fake, first := workflowTestManager(t)
	if _, err := Create(cfg, "tasks", "recover after process restart", ""); err != nil {
		t.Fatal(err)
	}
	if err := first.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	first.Wait()
	leases, err := first.loadLeases()
	if err != nil || len(leases) != 1 || leases[0].OwnerID != first.ownerID {
		t.Fatalf("first lease = %#v, %v", leases, err)
	}
	second := New(cfg, fake, extensions.Runner{Directory: filepath.Join(cfg.Root, "missing")})
	if second.ownerID == first.ownerID {
		t.Fatal("manager ownership IDs were reused")
	}
	if err := second.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	second.Wait()
	if fake.calls != 2 {
		t.Fatalf("fresh foreign lease was not recovered after restart: %d calls", fake.calls)
	}
	recovered, err := second.loadLeases()
	if err != nil || len(recovered) != 1 || recovered[0].OwnerID != second.ownerID || recovered[0].RecoveryCount != 1 {
		t.Fatalf("recovered ownership = %#v, %v", recovered, err)
	}
}

func TestWaitingTaskWithWakeTimeReturnsToImplementationQueue(t *testing.T) {
	cfg, _, manager := workflowTestManager(t)
	task, err := Create(cfg, "tasks", "resume after dependency", "")
	if err != nil {
		t.Fatal(err)
	}
	document, err := ReadDocument(task)
	if err != nil {
		t.Fatal(err)
	}
	document.FrontMatter["wake_at"] = time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	if err := WriteDocument(task, document); err != nil {
		t.Fatal(err)
	}
	waiting := filepath.Join(filepath.Dir(task), "..", "waiting", filepath.Base(task))
	if err := moveDocument(task, waiting, "waiting", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	working := filepath.Join(filepath.Dir(filepath.Dir(task)), "working", filepath.Base(task))
	if _, err := os.Stat(working); err != nil {
		t.Fatalf("due waiting task was not claimed: %v", err)
	}
	resumed, err := ReadDocument(working)
	if err != nil || !strings.Contains(resumed.Body, "scheduled wake condition became due") {
		t.Fatalf("waiting wake was not journaled: body=%q err=%v", resumed.Body, err)
	}
}

func TestRuntimeProgressJournalStaysInsideProgressSection(t *testing.T) {
	now := time.Date(2026, 8, 8, 12, 34, 56, 0, time.UTC)
	document := Document{FrontMatter: map[string]any{}, Body: "# Task\n\n## Progress\n\n- Existing entry.\n\n## Notes\n\nKeep this note.\n"}
	appendProgress(&document, now, "  Runtime repaired\n  a transition. ")
	appendProgress(&document, now, "Runtime repaired a transition.")
	want := "## Progress\n\n- Existing entry.\n\n- 2026-08-08T12:34:56Z — Runtime repaired a transition.\n\n## Notes\n\nKeep this note."
	if !strings.Contains(document.Body, want) {
		t.Fatalf("progress journal escaped its section:\n%s", document.Body)
	}
	if count := strings.Count(document.Body, "Runtime repaired a transition."); count != 1 {
		t.Fatalf("idempotent progress count = %d:\n%s", count, document.Body)
	}
}

func TestGoalRoundSettlesIntoFreshGoalReviewAndContinuesToPlanning(t *testing.T) {
	cfg, _, manager := workflowTestManager(t)
	goal, err := Create(cfg, "goals", "keep the release healthy", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	goalBase := filepath.Dir(cfg.Resolve(workflowRoutes()[1].Source))
	planning := filepath.Join(goalBase, "planning", filepath.Base(goal))
	goalDocument, err := ReadDocument(planning)
	if err != nil {
		t.Fatal(err)
	}
	goalID := documentID(goalDocument)
	goalDocument.FrontMatter["round"] = 1
	goalDocument.FrontMatter["status"] = "active"
	if err := WriteDocument(planning, goalDocument); err != nil {
		t.Fatal(err)
	}
	task, err := CreateWithOptions(cfg, "tasks", "verify release", "", CreateOptions{GoalID: goalID, GoalRound: 1})
	if err != nil {
		t.Fatal(err)
	}
	taskDocument, err := ReadDocument(task)
	if err != nil {
		t.Fatal(err)
	}
	goalDocument.FrontMatter["round_task_ids"] = []any{documentID(taskDocument)}
	if err := WriteDocument(planning, goalDocument); err != nil {
		t.Fatal(err)
	}
	active := filepath.Join(goalBase, "active", filepath.Base(goal))
	if err := os.Rename(planning, active); err != nil {
		t.Fatal(err)
	}
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	taskBase := filepath.Dir(cfg.Resolve(workflowRoutes()[0].Source))
	workingTask := filepath.Join(taskBase, "working", filepath.Base(task))
	reviewTask := filepath.Join(taskBase, "review", filepath.Base(task))
	if err := moveDocument(workingTask, reviewTask, "review", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	reviewingTask := filepath.Join(taskBase, "reviewing", filepath.Base(task))
	doneTask := filepath.Join(taskBase, "done", filepath.Base(task))
	if err := moveDocument(reviewingTask, doneTask, "done", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	reviewingGoal := filepath.Join(goalBase, "reviewing", filepath.Base(goal))
	if _, err := os.Stat(reviewingGoal); err != nil {
		t.Fatalf("settled goal was not claimed for review: %v", err)
	}
	leasing, err := manager.loadLeases()
	if err != nil {
		t.Fatal(err)
	}
	var reviewLease Lease
	for _, lease := range leasing {
		if lease.Phase == phaseGoalReview {
			reviewLease = lease
		}
	}
	if reviewLease.ID == "" || !strings.Contains(reviewLease.SessionKey, phaseGoalReview) {
		t.Fatalf("goal review lease = %#v", reviewLease)
	}
	goalDocument, err = ReadDocument(reviewingGoal)
	if err != nil {
		t.Fatal(err)
	}
	goalDocument.FrontMatter["last_review"] = map[string]any{
		"round": 1, "verdict": "continue", "criteria_satisfied": false,
		"reviewed_at": time.Now().UTC().Format(time.RFC3339),
	}
	if err := WriteDocument(reviewingGoal, goalDocument); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(reviewingGoal, planning); err != nil {
		t.Fatal(err)
	}
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	leasing, _ = manager.loadLeases()
	var planningLease Lease
	for _, lease := range leasing {
		if lease.Phase == phaseGoalPlanning {
			planningLease = lease
		}
	}
	if planningLease.ID == "" || planningLease.SessionKey == reviewLease.SessionKey {
		t.Fatalf("continuation did not receive a fresh planning lease: review=%#v planning=%#v", reviewLease, planningLease)
	}
}

func TestGoalRoundUsesCanonicalTaskFolders(t *testing.T) {
	cfg, _, manager := workflowTestManager(t)
	taskRoute := workflowRoutes()[0]
	goalRoute := workflowRoutes()[1]
	goal, err := Create(cfg, "goals", "preserve admitted task cohort", "")
	if err != nil {
		t.Fatal(err)
	}
	goalBase := filepath.Dir(cfg.Resolve(goalRoute.Source))
	planning := filepath.Join(goalBase, "planning", filepath.Base(goal))
	goalDocument, err := ClaimDocument(goal, planning, "planning", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	goalID := documentID(goalDocument)
	task, err := CreateWithOptions(cfg, "tasks", "finish admitted cohort", "", CreateOptions{GoalID: goalID, GoalRound: 1})
	if err != nil {
		t.Fatal(err)
	}
	taskDocument, err := ReadDocument(task)
	if err != nil {
		t.Fatal(err)
	}
	goalDocument.FrontMatter["round"] = 1
	goalDocument.FrontMatter["round_task_ids"] = []any{documentID(taskDocument)}
	goalDocument.FrontMatter["status"] = "active"
	if err := WriteDocument(planning, goalDocument); err != nil {
		t.Fatal(err)
	}
	active := filepath.Join(goalBase, "active", filepath.Base(goal))
	if err := os.Rename(planning, active); err != nil {
		t.Fatal(err)
	}
	lease := Lease{
		ID: "admitted-goal-route", Route: "goals",
		File: planning, SessionKey: "orchestrator:goals:admitted-route", State: "awaiting_transition", Phase: phaseGoalPlanning,
		ClaimAttempt: 1, StartedAt: time.Now().UTC(), HeartbeatAt: time.Now().UTC(),
	}
	if err := manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	customPrompt := filepath.Join(cfg.Root, "goal-route-generation-prompt.md")
	if err := os.WriteFile(customPrompt, []byte("Task source: {{TASK_SOURCE}}\n\nLinked tasks:\n{{RELATED_TASKS}}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	prompt, err := manager.renderPrompt(goalRoute, Lease{File: active}, customPrompt)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(prompt, cfg.Resolve(taskRoute.Source)) {
		t.Fatalf("goal prompt omitted canonical task folder:\n%s", prompt)
	}
	if !strings.Contains(prompt, task) || strings.Contains(prompt, "No tasks are linked to the current round") {
		t.Fatalf("capacity-delayed goal prompt lost admitted linked-task evidence:\n%s", prompt)
	}
	if err := manager.reconcileTransitions(context.Background()); err != nil {
		t.Fatal(err)
	}
	activated, err := ReadDocument(active)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := activated.FrontMatter["round_task_route"]; exists {
		t.Fatal("goal persisted redundant workflow paths")
	}
	taskDocument.FrontMatter["status"] = "done"
	done := filepath.Join(filepath.Dir(cfg.Resolve(taskRoute.Source)), "done", filepath.Base(task))
	if err := WriteDocument(task, taskDocument); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(task, done); err != nil {
		t.Fatal(err)
	}
	if err := manager.advanceActiveGoals(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(goalBase, "review", filepath.Base(goal))); err != nil {
		t.Fatalf("goal did not observe its task cohort: %v", err)
	}
}

func TestSettledGoalReviewIgnoresFutureCheckpointAcrossRestart(t *testing.T) {
	for _, trigger := range []string{"all_round_tasks_settled", "all_round_tasks_settled_or_checkpoint"} {
		t.Run(trigger, func(t *testing.T) {
			cfg, fake, first := workflowTestManager(t)
			goal := writeActiveGoalRound(t, cfg, trigger, time.Now().Add(7*24*time.Hour), "done", "done")
			goalBase := filepath.Dir(cfg.Resolve(workflowRoutes()[1].Source))

			// Model a restart after active-goal eligibility has been persisted but
			// before the review queue's claim phase runs.
			if err := first.advanceActiveGoals(); err != nil {
				t.Fatal(err)
			}
			queued := filepath.Join(goalBase, "review", filepath.Base(goal))
			queuedDocument, err := ReadDocument(queued)
			if err != nil {
				t.Fatalf("settled goal was not queued for review: %v", err)
			}
			if stringField(queuedDocument, "next_review_at") == "" {
				t.Fatal("queued goal lost its historical round checkpoint")
			}

			restarted := New(cfg, fake, extensions.Runner{Directory: filepath.Join(cfg.Root, "missing")})
			if err := restarted.ScanOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			restarted.Wait()
			reviewing := filepath.Join(goalBase, "reviewing", filepath.Base(goal))
			if _, err := os.Stat(reviewing); err != nil {
				t.Fatalf("future checkpoint stalled queued goal review: %v", err)
			}
			leases, err := restarted.loadLeases()
			if err != nil {
				t.Fatal(err)
			}
			reviewLeases := 0
			for _, lease := range leases {
				if lease.Phase == phaseGoalReview {
					reviewLeases++
					if !strings.HasSuffix(lease.SessionKey, ":1") {
						t.Fatalf("goal review session = %q", lease.SessionKey)
					}
				}
			}
			if reviewLeases != 1 || fake.calls != 1 {
				t.Fatalf("goal review dispatches = %d leases, %d calls", reviewLeases, fake.calls)
			}
			if err := restarted.ScanOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			restarted.Wait()
			if fake.calls != 1 {
				t.Fatalf("duplicate scan dispatched %d goal reviews", fake.calls)
			}
		})
	}
}

func TestTaskSettlementRequestsImmediateGoalReconsideration(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	cfg.Orchestrator.IntervalSec = 3600
	manager.ApplyRuntimeConfig(cfg)
	goal := writeActiveGoalRound(t, cfg, "all_round_tasks_settled", time.Now().Add(7*24*time.Hour), "todo")
	document, err := ReadDocument(goal)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := manager.goalRoundTasks(document)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("round tasks = %#v, %v", tasks, err)
	}
	working := filepath.Join(filepath.Dir(filepath.Dir(tasks[0].Path)), "working", filepath.Base(tasks[0].Path))
	failed := filepath.Join(filepath.Dir(filepath.Dir(tasks[0].Path)), "failed", filepath.Base(tasks[0].Path))
	var settle sync.Once
	fake.beforeEmit = func() {
		settle.Do(func() {
			if err := moveDocument(working, failed, "failed", time.Now()); err != nil {
				t.Errorf("settle task: %v", err)
			}
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()
	reviewing := filepath.Join(filepath.Dir(filepath.Dir(goal)), "reviewing", filepath.Base(goal))
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(reviewing); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("task settlement did not wake goal reconsideration before the periodic interval")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("orchestrator did not stop")
	}
	manager.Wait()
}

func TestCheckpointTriggeredGoalWaitsUntilDue(t *testing.T) {
	for _, test := range []struct {
		name       string
		taskStatus string
	}{
		{name: "unsettled", taskStatus: "waiting"},
		{name: "settled-scheduled-only", taskStatus: "done"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg, _, manager := workflowTestManager(t)
			goal := writeActiveGoalRound(t, cfg, "scheduled", time.Now().Add(7*24*time.Hour), test.taskStatus)
			if err := manager.ScanOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			manager.Wait()
			if _, err := os.Stat(goal); err != nil {
				t.Fatalf("future scheduled goal reviewed early: %v", err)
			}

			document, err := ReadDocument(goal)
			if err != nil {
				t.Fatal(err)
			}
			document.FrontMatter["next_review_at"] = time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
			if err := WriteDocument(goal, document); err != nil {
				t.Fatal(err)
			}
			if err := manager.ScanOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			manager.Wait()
			reviewing := filepath.Join(filepath.Dir(filepath.Dir(goal)), "reviewing", filepath.Base(goal))
			if _, err := os.Stat(reviewing); err != nil {
				t.Fatalf("due checkpoint did not dispatch goal review: %v", err)
			}
		})
	}
}

func TestGoalActivationValidatesConfiguredCheckpoint(t *testing.T) {
	for _, test := range []struct {
		name, trigger string
		checkpoint    any
		remove        bool
	}{
		{name: "scheduled-missing", trigger: "scheduled", remove: true},
		{name: "scheduled-invalid", trigger: "scheduled", checkpoint: "next week"},
		{name: "settled-or-checkpoint-invalid", trigger: "all_round_tasks_settled_or_checkpoint", checkpoint: "next week"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg, _, manager := workflowTestManager(t)
			goal := writeActiveGoalRound(t, cfg, test.trigger, time.Now().Add(time.Hour), "waiting")
			document, err := ReadDocument(goal)
			if err != nil {
				t.Fatal(err)
			}
			if test.remove {
				delete(document.FrontMatter, "next_review_at")
			} else {
				document.FrontMatter["next_review_at"] = test.checkpoint
			}
			if err := manager.validateGoalActivation(document); err == nil {
				t.Fatal("invalid checkpoint was accepted for activation")
			}
		})
	}
}

func TestGoalPlanningCheckpointRequiresReason(t *testing.T) {
	cfg, _, manager := workflowTestManager(t)
	goal := writeActiveGoalRound(t, cfg, "scheduled", time.Now().Add(time.Hour), "waiting")
	document, err := ReadDocument(goal)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.validateGoalPlanningTransition(document); err == nil || !strings.Contains(err.Error(), "checkpoint_reason") {
		t.Fatalf("missing checkpoint rationale error = %v", err)
	}
	document.FrontMatter["checkpoint_reason"] = "Recheck a bounded asynchronous rollout after ten minutes."
	if err := manager.validateGoalPlanningTransition(document); err != nil {
		t.Fatalf("reasoned checkpoint rejected: %v", err)
	}
	if err := WriteDocument(goal, document); err != nil {
		t.Fatal(err)
	}
	waits, err := manager.ScheduledCheckpoints(time.Now())
	if err != nil || len(waits) != 1 || waits[0].Reason == "" || waits[0].ID != documentID(document) {
		t.Fatalf("scheduled checkpoint status = %#v, %v", waits, err)
	}
}

func TestReviewQueuesIgnoreEarlierPhaseDates(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	future := time.Now().Add(7 * 24 * time.Hour).UTC().Format(time.RFC3339)
	for index, route := range workflowRoutes()[:2] {
		base := filepath.Dir(cfg.Resolve(route.Source))
		path := filepath.Join(base, "review", route.Name+"-dated.md")
		document := Document{FrontMatter: map[string]any{
			"id": route.Name + "-dated", "title": "dated review", "status": "review",
			"created_at": time.Now().UTC().Format(time.RFC3339), "updated_at": time.Now().UTC().Format(time.RFC3339),
			"not_before": future, "next_dispatch_at": future, "next_review_at": future,
		}, Body: "# Dated review\n"}
		if err := WriteDocument(path, document); err != nil {
			t.Fatal(err)
		}
		phase := phaseTaskReview
		if index == 1 {
			phase = phaseGoalReview
		}
		if err := manager.scanPhaseQueue(context.Background(), route, filepath.Dir(path), filepath.Join(base, "reviewing"), phase); err != nil {
			t.Fatal(err)
		}
	}
	manager.Wait()
	if fake.calls != 2 {
		t.Fatalf("phase-eligible dated reviews dispatched %d times", fake.calls)
	}
}

func writeActiveGoalRound(t *testing.T, cfg config.Config, trigger string, checkpoint time.Time, taskStatuses ...string) string {
	t.Helper()
	goal, err := Create(cfg, "goals", "exercise goal review eligibility", "")
	if err != nil {
		t.Fatal(err)
	}
	document, err := ReadDocument(goal)
	if err != nil {
		t.Fatal(err)
	}
	goalID := documentID(document)
	document.FrontMatter["round"] = 1
	document.FrontMatter["status"] = "active"
	document.FrontMatter["review_trigger"] = trigger
	document.FrontMatter["next_review_at"] = checkpoint.UTC().Format(time.RFC3339)
	ids := make([]any, 0, len(taskStatuses))
	taskBase := filepath.Dir(cfg.Resolve(workflowRoutes()[0].Source))
	for index, status := range taskStatuses {
		task, createErr := CreateWithOptions(cfg, "tasks", "round task", "", CreateOptions{GoalID: goalID, GoalRound: 1})
		if createErr != nil {
			t.Fatal(createErr)
		}
		taskDocument, readErr := ReadDocument(task)
		if readErr != nil {
			t.Fatal(readErr)
		}
		ids = append(ids, documentID(taskDocument))
		target := filepath.Join(taskBase, status, strings.TrimSuffix(filepath.Base(task), ".md")+string(rune('a'+index))+".md")
		if err := moveDocument(task, target, status, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	document.FrontMatter["round_task_ids"] = ids
	if err := WriteDocument(goal, document); err != nil {
		t.Fatal(err)
	}
	active := filepath.Join(filepath.Dir(cfg.Resolve(workflowRoutes()[1].Source)), "active", filepath.Base(goal))
	if err := os.Rename(goal, active); err != nil {
		t.Fatal(err)
	}
	return active
}

func TestGoalDoneRequiresReviewProof(t *testing.T) {
	cfg, _, manager := workflowTestManager(t)
	goalRoute := workflowRoutes()[1]
	base := filepath.Dir(cfg.Resolve(goalRoute.Source))
	path := filepath.Join(base, "reviewing", "proof.md")
	now := time.Now().UTC()
	document := Document{FrontMatter: map[string]any{
		"id": "goal-proof", "title": "proof", "status": "reviewing", "round": 1,
		"created_at": now.Format(time.RFC3339), "updated_at": now.Format(time.RFC3339),
		"review_trigger":   "all_round_tasks_settled",
		"success_criteria": []any{map[string]any{"id": "bar", "condition": "verified", "evidence_required": "test output"}},
	}, Body: "# proof\n"}
	if err := WriteDocument(path, document); err != nil {
		t.Fatal(err)
	}
	lease := Lease{ID: "goal-review", Route: "goals", File: path, Phase: phaseGoalReview, SessionKey: "review-session", State: "processing", StartedAt: now, HeartbeatAt: now}
	if err := manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	done := filepath.Join(base, "done", filepath.Base(path))
	if err := moveDocument(path, done, "done", now); err != nil {
		t.Fatal(err)
	}
	if err := manager.reconcileTransitions(context.Background()); err != nil {
		t.Fatal(err)
	}
	queued := filepath.Join(base, "review", filepath.Base(path))
	if _, err := os.Stat(queued); err != nil {
		t.Fatalf("unproven goal completion was not rejected: %v", err)
	}
}

// awaitingTransitionOrphan claims a task into working/ and parks a lease on it in
// awaiting_transition, owned by this manager, as a finished provider turn that
// never moved the document leaves it behind.
func awaitingTransitionOrphan(t *testing.T, cfg config.Config, manager *Manager, title string, age time.Duration, recoveries int) string {
	t.Helper()
	route := workflowRoutes()[0]
	task, err := Create(cfg, "tasks", title, "")
	if err != nil {
		t.Fatal(err)
	}
	working := filepath.Join(cfg.Resolve(route.Working), filepath.Base(task))
	document, err := ClaimDocument(task, working, "working", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().UTC().Add(-age)
	lease := Lease{
		ID: leaseID(route.Name+":"+phaseTaskImplementation, documentID(document)), ClaimID: "implementation",
		DocumentType: "task", OwnerID: manager.ownerID, Route: route.Name, File: working,
		SessionKey: "orchestrator:tasks:task_implementation:" + documentID(document) + ":1",
		ThreadID:   "implementation-thread", State: "awaiting_transition", Phase: phaseTaskImplementation,
		ClaimAttempt: 1, RecoveryCount: recoveries, StartedAt: stamp, HeartbeatAt: stamp,
	}
	if err := manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	return working
}

// A restart's recovery turn that finds no surviving worker returns in seconds and
// parks its lease in awaiting_transition with the document untouched. Waiting out
// route.StaleAfter there loses the race with the fleet's external runner watchdog,
// which fails the task forward at 10 minutes and charges it an attempt.
func TestEndedTurnThatMovedNothingRecoversBeforeTheExternalFailForward(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	working := awaitingTransitionOrphan(t, cfg, manager, "recover a turn that ended without moving its task", 3*time.Minute, 0)

	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()

	if fake.calls != 1 {
		t.Fatalf("ended-turn orphan dispatches = %d, want 1", fake.calls)
	}
	document, err := ReadDocument(working)
	if err != nil {
		t.Fatalf("task left its claimed directory: %v", err)
	}
	if document.FrontMatter["attempt"] != 1 {
		t.Fatalf("recovery consumed an attempt: %#v", document.FrontMatter)
	}
	if !strings.Contains(document.Body, "Werkrby started recovery attempt 1 for task implementation") {
		t.Fatalf("recovery was not journaled: %q", document.Body)
	}
	base := filepath.Dir(cfg.Resolve(workflowRoutes()[0].Source))
	for _, status := range []string{"failed", "todo"} {
		if _, err := os.Stat(filepath.Join(base, status, filepath.Base(working))); err == nil {
			t.Fatalf("restart orphan passed through %s/", status)
		}
	}
	leases, err := manager.loadLeases()
	if err != nil || len(leases) != 1 || leases[0].RecoveryCount != 1 {
		t.Fatalf("recovered lease = %#v, %v", leases, err)
	}
	// The external watchdog reads this heartbeat and fails the task forward when
	// it goes stale, so recovery has to refresh it, not merely re-dispatch.
	if age := time.Since(leases[0].HeartbeatAt); age > time.Minute {
		t.Fatalf("recovery left the heartbeat stale by %s", age)
	}
}

// The quick path only helps if every quick recovery lands inside the window the
// fleet's runner watchdog gives a task before it fails it forward.
func TestQuickRecoveryFitsInsideTheExternalFailForwardWindow(t *testing.T) {
	const externalFailForward = 10 * time.Minute
	if span := awaitingTransitionStaleAfter * quickAwaitingTransitionRecoveries; span >= externalFailForward {
		t.Fatalf("quick recovery spans %s, which does not fit inside the %s external fail-forward window", span, externalFailForward)
	}
}

// The quick path is bounded: turns that keep ending empty are a broken runner, not
// an interrupted one, so recovery hands them back to the ordinary stale threshold
// instead of re-dispatching every couple of minutes forever.
func TestRepeatedlyEmptyTurnsStopUsingTheQuickRecoveryPath(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	awaitingTransitionOrphan(t, cfg, manager, "stop retrying a runner that never starts", 3*time.Minute, quickAwaitingTransitionRecoveries)

	if err := manager.recoverStale(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()

	if fake.calls != 0 {
		t.Fatalf("exhausted quick recovery still dispatched: calls=%d", fake.calls)
	}
	leases, err := manager.loadLeases()
	if err != nil || len(leases) != 0 {
		t.Fatalf("lease still exists after requeue: %#v, %v", leases, err)
	}
}

func TestStrandedTaskGetsOneRecoveryThenRequeuesAsAttempt(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	// 1. Initial stranded task in awaiting_transition (RecoveryCount = 0)
	working := awaitingTransitionOrphan(t, cfg, manager, "stranded task gets one recovery then requeues", 3*time.Minute, 0)

	// First scan: dispatches the single quick recovery
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()

	if fake.calls != 1 {
		t.Fatalf("expected 1 recovery dispatch, got %d", fake.calls)
	}

	// 2. Recovery turn ends without moving document (RecoveryCount = 1)
	leases, err := manager.loadLeases()
	if err != nil || len(leases) != 1 {
		t.Fatalf("expected 1 lease after recovery dispatch, got %#v", leases)
	}
	lease := leases[0]
	lease.State = "awaiting_transition"
	lease.HeartbeatAt = time.Now().UTC().Add(-3 * time.Minute)
	if err := manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}

	// Second scan: quick recovery exhausted (1 >= 1) -> requeues to todo/ as ordinary new attempt
	if err := manager.recoverStale(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()

	if fake.calls != 1 {
		t.Fatalf("stranded task dispatched another recovery instead of requeuing: calls=%d", fake.calls)
	}

	// Lease should be removed
	leasesAfter, err := manager.loadLeases()
	if err != nil || len(leasesAfter) != 0 {
		t.Fatalf("lease still exists after requeue: %#v", leasesAfter)
	}

	// Task file moved to todo/
	todoFile := filepath.Join(filepath.Dir(cfg.Resolve(workflowRoutes()[0].Source)), "todo", filepath.Base(working))
	doc, err := ReadDocument(todoFile)
	if err != nil {
		t.Fatalf("task was not requeued to todo/: %v", err)
	}
	if doc.FrontMatter["status"] != "todo" {
		t.Fatalf("requeued task status = %v, want todo", doc.FrontMatter["status"])
	}
	if credit, _ := doc.FrontMatter[resumeCreditField].(bool); credit {
		t.Fatalf("requeued task has resume_credit set; must be ordinary new attempt")
	}
	if !strings.Contains(doc.Body, "requeued") {
		t.Fatalf("requeue was not journaled in progress: %s", doc.Body)
	}

	// 3. Claiming this task now starts attempt 2
	nextWorking := filepath.Join(cfg.Resolve(workflowRoutes()[0].Working), filepath.Base(working))
	claimedDoc, err := ClaimDocument(todoFile, nextWorking, "working", time.Now().UTC())
	if err != nil {
		t.Fatalf("claiming requeued task failed: %v", err)
	}
	attempt := numberValue(claimedDoc.FrontMatter["attempt"])
	if attempt != 2 {
		t.Fatalf("claimed attempt = %d, want 2", attempt)
	}
}

// A turn that is still running owns its lease no matter how long it has been quiet:
// an in-flight dispatch or a live harness session is never re-dispatched underneath.
func TestLiveSessionKeepsItsAwaitingTransitionLease(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	working := awaitingTransitionOrphan(t, cfg, manager, "leave a live session alone", 3*time.Minute, 0)
	leases, err := manager.loadLeases()
	if err != nil || len(leases) != 1 {
		t.Fatalf("fixture leases = %#v, %v", leases, err)
	}
	fake.active[leases[0].SessionKey] = true

	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()

	if fake.calls != 0 {
		t.Fatalf("live session was re-dispatched: calls=%d", fake.calls)
	}
	if _, err := ReadDocument(working); err != nil {
		t.Fatalf("live session lost its task: %v", err)
	}
	after, err := manager.loadLeases()
	if err != nil || len(after) != 1 || after[0].RecoveryCount != 0 {
		t.Fatalf("live session lease was disturbed: %#v, %v", after, err)
	}
}

func TestRestartSignalDuringStartupGraceDoesNotSpendAttempts(t *testing.T) {
	// Three SIGTERM storms inside the post-start grace window must leave every
	// in-flight task with its attempt and RecoveryCount intact and none in
	// failed/ — the load that beat the 2026-09-18 awaiting_transition fix.
	cfg, fake, manager := workflowTestManager(t)
	route := workflowRoutes()[0]
	termErr := errors.New("herdr prompt failed: signal: terminated (out: signal: terminated)")
	const taskCount = 3
	manager.Config.Orchestrator.MaxParallel = taskCount
	manager.capacityLimit = taskCount

	for i := 0; i < taskCount; i++ {
		if _, err := Create(cfg, "tasks", "survive three restart kills "+string(rune('A'+i)), ""); err != nil {
			t.Fatal(err)
		}
	}
	for round := 0; round < 3; round++ {
		fake.sendErrs = nil
		for i := 0; i < taskCount; i++ {
			fake.sendErrs = append(fake.sendErrs, termErr)
		}
		if err := manager.ScanOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		manager.Wait()
		leases, err := manager.loadLeases()
		if err != nil || len(leases) != taskCount {
			t.Fatalf("round %d leases = %d (%v), want %d", round, len(leases), err, taskCount)
		}
		for _, current := range leases {
			if current.RecoveryCount != 0 {
				t.Fatalf("round %d RecoveryCount = %d on %s, want 0", round, current.RecoveryCount, current.File)
			}
			if current.State != "error" {
				t.Fatalf("round %d state = %q on %s, want error for quick re-adoption", round, current.State, current.File)
			}
			current.HeartbeatAt = time.Now().UTC().Add(-15 * time.Second)
			if err := manager.saveLease(current); err != nil {
				t.Fatal(err)
			}
		}
	}

	entries, _ := os.ReadDir(cfg.Resolve(route.Working))
	if len(entries) != taskCount {
		t.Fatalf("working entries = %d, want %d", len(entries), taskCount)
	}
	for _, entry := range entries {
		working := filepath.Join(cfg.Resolve(route.Working), entry.Name())
		document, err := ReadDocument(working)
		if err != nil {
			t.Fatal(err)
		}
		if numberValue(document.FrontMatter["attempt"]) != 1 {
			t.Fatalf("%s attempt = %v, want 1", entry.Name(), document.FrontMatter["attempt"])
		}
		if !strings.Contains(document.Body, "interrupted by a service restart") || !strings.Contains(document.Body, "attempt not spent") {
			t.Fatalf("%s progress missing restart interrupt note: %s", entry.Name(), document.Body)
		}
	}
	failedDir := filepath.Join(filepath.Dir(cfg.Resolve(route.Source)), "failed")
	failed, _ := os.ReadDir(failedDir)
	if len(failed) != 0 {
		t.Fatalf("tasks landed in failed/: %v", failed)
	}
}

func TestRestartSignalAfterGraceStillCountsTowardFailure(t *testing.T) {
	// Outside the startup grace, a SIGTERM on a healthy instance is still a
	// real runner death and must keep the consecutive-error fail path.
	cfg, fake, manager := workflowTestManager(t)
	manager.startedAt = time.Now().UTC().Add(-10 * time.Minute)
	termErr := errors.New("herdr prompt failed: signal: terminated (out: signal: terminated)")
	fake.sendErrs = []error{termErr, termErr, termErr}

	if _, err := Create(cfg, "tasks", "signal after grace fails forward", ""); err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 3; round++ {
		if err := manager.ScanOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		manager.Wait()
		if round < 2 {
			leases, err := manager.loadLeases()
			if err != nil || len(leases) != 1 {
				t.Fatalf("round %d leases = %#v, %v", round, leases, err)
			}
			leases[0].HeartbeatAt = time.Now().UTC().Add(-15 * time.Second)
			if err := manager.saveLease(leases[0]); err != nil {
				t.Fatal(err)
			}
			fake.sendErrs = append(fake.sendErrs, termErr)
		}
	}

	route := workflowRoutes()[0]
	failedDir := filepath.Join(filepath.Dir(cfg.Resolve(route.Source)), "failed")
	failed, _ := os.ReadDir(failedDir)
	if len(failed) != 1 {
		t.Fatalf("failed entries = %d, want 1 after three counted signal kills", len(failed))
	}
}

func TestShutdownContextCancelDoesNotSpendAttempts(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	manager.startedAt = time.Now().UTC().Add(-10 * time.Minute) // grace expired; ctx cancel must still protect
	termErr := errors.New("herdr prompt failed: signal: terminated (out: signal: terminated)")
	fake.sendErrs = []error{termErr, termErr, termErr}

	if _, err := Create(cfg, "tasks", "shutdown cancel preserves attempt", ""); err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 3; round++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := manager.ScanOnce(ctx); err != nil {
			t.Fatal(err)
		}
		manager.Wait()
		leases, err := manager.loadLeases()
		if err != nil || len(leases) != 1 {
			t.Fatalf("round %d leases = %#v, %v", round, leases, err)
		}
		if leases[0].RecoveryCount != 0 {
			t.Fatalf("round %d RecoveryCount = %d, want 0", round, leases[0].RecoveryCount)
		}
		leases[0].HeartbeatAt = time.Now().UTC().Add(-15 * time.Second)
		if err := manager.saveLease(leases[0]); err != nil {
			t.Fatal(err)
		}
		if round < 2 {
			fake.sendErrs = append(fake.sendErrs, termErr)
		}
	}
	route := workflowRoutes()[0]
	failed, _ := os.ReadDir(filepath.Join(filepath.Dir(cfg.Resolve(route.Source)), "failed"))
	if len(failed) != 0 {
		t.Fatalf("shutdown cancel moved tasks to failed/: %v", failed)
	}
	entries, _ := os.ReadDir(cfg.Resolve(route.Working))
	if len(entries) != 1 {
		t.Fatalf("working entries = %d, want 1", len(entries))
	}
	document, err := ReadDocument(filepath.Join(cfg.Resolve(route.Working), entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	if numberValue(document.FrontMatter["attempt"]) != 1 {
		t.Fatalf("attempt = %v, want 1", document.FrontMatter["attempt"])
	}
}

func TestWaitingWithoutRoutablePathIsRejected(t *testing.T) {
	// 1. Task implementation transition: prose-only waiting_for is rejected
	{
		cfg, fake, manager := workflowTestManager(t)
		route := workflowRoutes()[0]
		task, err := Create(cfg, "tasks", "task implementation prose park rejected", "")
		if err != nil {
			t.Fatal(err)
		}
		name := filepath.Base(task)
		base := filepath.Dir(cfg.Resolve(route.Source))
		parkDuringTurn(t, fake, base, name, func(fm map[string]any) {
			fm["waiting_for"] = "prose only description"
		})
		scanAndWait(t, manager)
		if err := manager.reconcileTransitions(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Must be rejected and redirected back to todo/
		todoPath := filepath.Join(base, "todo", name)
		doc, err := ReadDocument(todoPath)
		if err != nil {
			t.Fatalf("task was not returned to todo/: %v", err)
		}
		if doc.FrontMatter["status"] != "todo" {
			t.Fatalf("status = %v, want todo", doc.FrontMatter["status"])
		}
		if credit, _ := doc.FrontMatter[resumeCreditField].(bool); !credit {
			t.Fatalf("resume_credit was not set on rejected park: %#v", doc.FrontMatter)
		}
		if !strings.Contains(doc.Body, "Park rejected") {
			t.Fatalf("rejection rule not written in progress: %s", doc.Body)
		}

		// Next claim continues attempt 1
		scanAndWait(t, manager)
		working := filepath.Join(base, "working", name)
		docAfter, _ := claimedAttempt(t, manager, working)
		if got := numberValue(docAfter.FrontMatter["attempt"]); got != 1 {
			t.Fatalf("attempt after rejected park = %d, want 1 (attempt not spent)", got)
		}
	}

	// 2. Task implementation transition: past wake_at is rejected
	{
		cfg, fake, manager := workflowTestManager(t)
		route := workflowRoutes()[0]
		task, err := Create(cfg, "tasks", "task implementation past wake_at rejected", "")
		if err != nil {
			t.Fatal(err)
		}
		name := filepath.Base(task)
		base := filepath.Dir(cfg.Resolve(route.Source))
		parkDuringTurn(t, fake, base, name, func(fm map[string]any) {
			fm["waiting_for"] = "past wake_at"
			fm["wake_at"] = time.Now().Add(-10 * time.Minute).UTC().Format(time.RFC3339)
		})
		scanAndWait(t, manager)
		if err := manager.reconcileTransitions(context.Background()); err != nil {
			t.Fatal(err)
		}

		todoPath := filepath.Join(base, "todo", name)
		doc, err := ReadDocument(todoPath)
		if err != nil {
			t.Fatalf("task was not returned to todo/ on past wake_at: %v", err)
		}
		if credit, _ := doc.FrontMatter[resumeCreditField].(bool); !credit {
			t.Fatalf("resume_credit was not set on rejected past wake_at: %#v", doc.FrontMatter)
		}
	}

	// 3. Task implementation transition: future wake_at IS accepted
	{
		cfg, fake, manager := workflowTestManager(t)
		route := workflowRoutes()[0]
		task, err := Create(cfg, "tasks", "task implementation future wake_at accepted", "")
		if err != nil {
			t.Fatal(err)
		}
		name := filepath.Base(task)
		base := filepath.Dir(cfg.Resolve(route.Source))
		parkDuringTurn(t, fake, base, name, func(fm map[string]any) {
			fm["waiting_for"] = "future wake_at"
			fm["wake_at"] = time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339)
		})
		scanAndWait(t, manager)
		if err := manager.reconcileTransitions(context.Background()); err != nil {
			t.Fatal(err)
		}

		waitingPath := filepath.Join(base, "waiting", name)
		doc, err := ReadDocument(waitingPath)
		if err != nil {
			t.Fatalf("task was not accepted into waiting/: %v", err)
		}
		if doc.FrontMatter["status"] != "waiting" {
			t.Fatalf("status = %v, want waiting", doc.FrontMatter["status"])
		}
	}

	// 4. Task implementation transition: joey_ask IS accepted
	{
		cfg, fake, manager := workflowTestManager(t)
		route := workflowRoutes()[0]
		task, err := Create(cfg, "tasks", "task implementation joey_ask accepted", "")
		if err != nil {
			t.Fatal(err)
		}
		name := filepath.Base(task)
		base := filepath.Dir(cfg.Resolve(route.Source))
		parkDuringTurn(t, fake, base, name, func(fm map[string]any) {
			fm["waiting_for"] = "joey decision"
			fm["joey_ask"] = map[string]any{"kind": "choice", "prompt": "Approve?"}
		})
		scanAndWait(t, manager)
		if err := manager.reconcileTransitions(context.Background()); err != nil {
			t.Fatal(err)
		}

		waitingPath := filepath.Join(base, "waiting", name)
		doc, err := ReadDocument(waitingPath)
		if err != nil {
			t.Fatalf("task with joey_ask was not accepted into waiting/: %v", err)
		}
		if doc.FrontMatter["status"] != "waiting" {
			t.Fatalf("status = %v, want waiting", doc.FrontMatter["status"])
		}
	}

	// 5. Task review transition: prose-only waiting_for is rejected
	{
		cfg, fake, manager := workflowTestManager(t)
		route := workflowRoutes()[0]
		task, err := Create(cfg, "tasks", "task review prose park rejected", "")
		if err != nil {
			t.Fatal(err)
		}
		name := filepath.Base(task)
		base := filepath.Dir(cfg.Resolve(route.Source))
		fake.beforeEmit = func() {
			working := filepath.Join(base, "working", name)
			if _, err := os.Stat(working); err == nil {
				// Turn 1: move to review
				_ = moveDocument(working, filepath.Join(base, "review", name), "review", time.Now().UTC())
				return
			}
			reviewing := filepath.Join(base, "reviewing", name)
			if _, err := os.Stat(reviewing); err == nil {
				// Turn 2: reviewer moves to waiting without routable path
				editFrontMatter(t, reviewing, func(fm map[string]any) {
					fm["waiting_for"] = "prose only in review"
				})
				_ = moveDocument(reviewing, filepath.Join(base, "waiting", name), "waiting", time.Now().UTC())
			}
		}
		scanAndWait(t, manager) // claim implementation, move to review
		scanAndWait(t, manager) // claim review, park without routable path
		if err := manager.reconcileTransitions(context.Background()); err != nil {
			t.Fatal(err)
		}

		todoPath := filepath.Join(base, "todo", name)
		doc, err := ReadDocument(todoPath)
		if err != nil {
			t.Fatalf("review park without routable path was not returned to todo/: %v", err)
		}
		if credit, _ := doc.FrontMatter[resumeCreditField].(bool); !credit {
			t.Fatalf("resume_credit was not set on review park rejection: %#v", doc.FrontMatter)
		}
		if !strings.Contains(doc.Body, "Park rejected") {
			t.Fatalf("rejection rule not written in progress: %s", doc.Body)
		}
	}

	// 6. Goal planning transition: prose-only waiting_for is rejected to proposed
	{
		cfg, _, manager := workflowTestManager(t)
		route := workflowRoutes()[1] // goals
		goal, err := Create(cfg, "goals", "goal planning prose park rejected", "")
		if err != nil {
			t.Fatal(err)
		}
		name := filepath.Base(goal)
		base := filepath.Dir(cfg.Resolve(route.Source))
		planning := filepath.Join(base, "planning", name)
		if _, err := ClaimDocument(goal, planning, "planning", time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		waiting := filepath.Join(base, "waiting", name)
		editFrontMatter(t, planning, func(fm map[string]any) {
			fm["waiting_for"] = "prose only goal wait"
		})
		if err := moveDocument(planning, waiting, "waiting", time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		lease := Lease{
			ID: leaseID(route.Name+":"+phaseGoalPlanning, name), ClaimID: "planning",
			DocumentType: "goal", OwnerID: manager.ownerID, Route: route.Name, File: planning,
			SessionKey: "test:goal:planning", Phase: phaseGoalPlanning, ClaimAttempt: 1,
			StartedAt: time.Now().UTC(), HeartbeatAt: time.Now().UTC(), State: "processing",
		}
		_ = manager.saveLease(lease)

		if err := manager.reconcileTransitions(context.Background()); err != nil {
			t.Fatal(err)
		}

		proposedPath := filepath.Join(base, "proposed", name)
		doc, err := ReadDocument(proposedPath)
		if err != nil {
			t.Fatalf("goal was not returned to proposed/: %v", err)
		}
		if doc.FrontMatter["status"] != "proposed" {
			t.Fatalf("status = %v, want proposed", doc.FrontMatter["status"])
		}
		if !strings.Contains(doc.Body, "Park rejected") {
			t.Fatalf("rejection rule not written in progress: %s", doc.Body)
		}
	}

	// 7. Goal review transition: prose-only waiting_for is rejected to review
	{
		cfg, _, manager := workflowTestManager(t)
		route := workflowRoutes()[1] // goals
		goal, err := Create(cfg, "goals", "goal review prose park rejected", "")
		if err != nil {
			t.Fatal(err)
		}
		name := filepath.Base(goal)
		base := filepath.Dir(cfg.Resolve(route.Source))
		reviewing := filepath.Join(base, "reviewing", name)
		editFrontMatter(t, goal, func(fm map[string]any) {
			fm["status"] = "review"
		})
		if _, err := ClaimDocument(goal, reviewing, "reviewing", time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		waiting := filepath.Join(base, "waiting", name)
		editFrontMatter(t, reviewing, func(fm map[string]any) {
			fm["waiting_for"] = "prose only goal review wait"
		})
		if err := moveDocument(reviewing, waiting, "waiting", time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		lease := Lease{
			ID: leaseID(route.Name+":"+phaseGoalReview, name), ClaimID: "review",
			DocumentType: "goal", OwnerID: manager.ownerID, Route: route.Name, File: reviewing,
			SessionKey: "test:goal:review", Phase: phaseGoalReview, ClaimAttempt: 1,
			StartedAt: time.Now().UTC(), HeartbeatAt: time.Now().UTC(), State: "processing",
		}
		_ = manager.saveLease(lease)

		if err := manager.reconcileTransitions(context.Background()); err != nil {
			t.Fatal(err)
		}

		reviewPath := filepath.Join(base, "review", name)
		doc, err := ReadDocument(reviewPath)
		if err != nil {
			t.Fatalf("goal review was not returned to review/: %v", err)
		}
		if doc.FrontMatter["status"] != "review" {
			t.Fatalf("status = %v, want review", doc.FrontMatter["status"])
		}
		if !strings.Contains(doc.Body, "Park rejected") {
			t.Fatalf("rejection rule not written in progress: %s", doc.Body)
		}
	}
}

func TestQuotaErrorParksUntilResetWithoutSpendingAttempt(t *testing.T) {
	// Variant 1: reset time present in provider message
	{
		cfg, fake, manager := workflowTestManager(t)
		route := workflowRoutes()[0]
		task, err := Create(cfg, "tasks", "task 429 with reset time", "")
		if err != nil {
			t.Fatal(err)
		}
		name := filepath.Base(task)
		base := filepath.Dir(cfg.Resolve(route.Source))
		editFrontMatter(t, task, func(fm map[string]any) {
			fm["agent"] = "kimi"
		})
		// The reset time must stay in the future: parseQuotaResetTime rejects
		// past timestamps, so a hardcoded one turns this test into a time bomb.
		resetStamp := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Minute).Format(time.RFC3339)
		fake.sendErrs = []error{errors.New("429 rate limit exceeded; resets at " + resetStamp)}

		scanAndWait(t, manager)

		waitingPath := filepath.Join(base, "waiting", name)
		doc, err := ReadDocument(waitingPath)
		if err != nil {
			t.Fatalf("task was not parked in waiting/: %v", err)
		}
		if doc.FrontMatter["status"] != "waiting" {
			t.Fatalf("status = %v, want waiting", doc.FrontMatter["status"])
		}
		if doc.FrontMatter["quota_reset_at"] != resetStamp {
			t.Fatalf("quota_reset_at = %v, want %s", doc.FrontMatter["quota_reset_at"], resetStamp)
		}
		if doc.FrontMatter["wake_at"] != resetStamp {
			t.Fatalf("wake_at = %v, want %s", doc.FrontMatter["wake_at"], resetStamp)
		}
		if doc.FrontMatter["agent"] != "kimi" {
			t.Fatalf("agent changed to %v, want kimi", doc.FrontMatter["agent"])
		}
		if credit, _ := doc.FrontMatter[resumeCreditField].(bool); !credit {
			t.Fatalf("resume_credit was not set: %#v", doc.FrontMatter)
		}
		if !strings.Contains(doc.Body, "quota wall on kimi; parked until "+resetStamp+"; attempt not spent") {
			t.Fatalf("progress note missing: %s", doc.Body)
		}

		leases, _ := manager.loadLeases()
		if len(leases) != 0 {
			t.Fatalf("lease still exists: %#v", leases)
		}

		// When wake_at arrives, next claim continues attempt 1
		editFrontMatter(t, waitingPath, func(fm map[string]any) {
			fm["wake_at"] = time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
		})
		scanAndWait(t, manager)
		working := filepath.Join(base, "working", name)
		claimedDoc, _ := claimedAttempt(t, manager, working)
		if got := numberValue(claimedDoc.FrontMatter["attempt"]); got != 1 {
			t.Fatalf("attempt after quota resume = %d, want 1", got)
		}
	}

	// Variant 2: reset time from snapshot.json
	{
		cfg, fake, manager := workflowTestManager(t)
		route := workflowRoutes()[0]
		task, err := Create(cfg, "tasks", "task 429 with snapshot reset", "")
		if err != nil {
			t.Fatal(err)
		}
		name := filepath.Base(task)
		base := filepath.Dir(cfg.Resolve(route.Source))
		editFrontMatter(t, task, func(fm map[string]any) {
			fm["agent"] = "kimi"
		})

		snapDir := t.TempDir()
		snapFile := filepath.Join(snapDir, "snapshot.json")
		expectedReset := time.Now().Add(45 * time.Minute).UTC().Truncate(time.Second)
		snapContent := fmt.Sprintf(`{"plans":{"kimi_coding":{"five_hour":{"pct":100,"resets_at":"%s"}}}}`, expectedReset.Format(time.RFC3339))
		if err := os.WriteFile(snapFile, []byte(snapContent), 0644); err != nil {
			t.Fatal(err)
		}
		origSnapshot := usageSnapshotPath
		usageSnapshotPath = snapFile
		defer func() { usageSnapshotPath = origSnapshot }()

		fake.sendErrs = []error{errors.New("429 Too Many Requests")}
		scanAndWait(t, manager)

		waitingPath := filepath.Join(base, "waiting", name)
		doc, err := ReadDocument(waitingPath)
		if err != nil {
			t.Fatalf("task was not parked in waiting/: %v", err)
		}
		if doc.FrontMatter["quota_reset_at"] != expectedReset.Format(time.RFC3339) {
			t.Fatalf("quota_reset_at = %v, want %s", doc.FrontMatter["quota_reset_at"], expectedReset.Format(time.RFC3339))
		}
	}

	// Variant 4: Anthropic subscription wall — "Individual quota reached ...
	// Resets in 4h10m9s" (exact bytes from the 2026-10-16 incident: 360
	// dispatch errors and one terminal task failure because isQuotaError did
	// not know the message and the composite NhNmNs window could not be
	// parsed). Must park like any other quota wall, wake at the composite
	// reset, and spend no attempt.
	{
		cfg, fake, manager := workflowTestManager(t)
		route := workflowRoutes()[0]
		task, err := Create(cfg, "tasks", "task individual quota reached", "")
		if err != nil {
			t.Fatal(err)
		}
		name := filepath.Base(task)
		base := filepath.Dir(cfg.Resolve(route.Source))
		editFrontMatter(t, task, func(fm map[string]any) {
			fm["agent"] = "kimi"
		})

		origSnapshot := usageSnapshotPath
		usageSnapshotPath = filepath.Join(t.TempDir(), "nonexistent.json")
		defer func() { usageSnapshotPath = origSnapshot }()

		before := time.Now().UTC()
		fake.sendErrs = []error{errors.New("runner turn failed: Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 4h10m9s.")}
		scanAndWait(t, manager)
		after := time.Now().UTC()

		waitingPath := filepath.Join(base, "waiting", name)
		doc, err := ReadDocument(waitingPath)
		if err != nil {
			t.Fatalf("task was not parked in waiting/ (quota message misclassified): %v", err)
		}
		if doc.FrontMatter["status"] != "waiting" {
			t.Fatalf("status = %v, want waiting", doc.FrontMatter["status"])
		}
		resetStr, _ := doc.FrontMatter["quota_reset_at"].(string)
		parsedReset, err := time.Parse(time.RFC3339, resetStr)
		if err != nil {
			t.Fatalf("unparseable quota_reset_at: %v", resetStr)
		}
		want := 4*time.Hour + 10*time.Minute + 9*time.Second
		if parsedReset.Before(before.Add(want-5*time.Second)) || parsedReset.After(after.Add(want+5*time.Second)) {
			t.Fatalf("parsedReset = %v, want ~%v from now (%v..%v)", parsedReset, want, before.Add(want), after.Add(want))
		}
		if credit, _ := doc.FrontMatter[resumeCreditField].(bool); !credit {
			t.Fatalf("resume_credit was not set: %#v", doc.FrontMatter)
		}
		if !strings.Contains(doc.Body, "; attempt not spent") {
			t.Fatalf("attempt-not-spent note missing: %s", doc.Body)
		}
	}

	// Variant 3: no reset time in error, no snapshot -> now + 1h
	{
		cfg, fake, manager := workflowTestManager(t)
		route := workflowRoutes()[0]
		task, err := Create(cfg, "tasks", "task 429 fallback +1h", "")
		if err != nil {
			t.Fatal(err)
		}
		name := filepath.Base(task)
		base := filepath.Dir(cfg.Resolve(route.Source))
		editFrontMatter(t, task, func(fm map[string]any) {
			fm["agent"] = "cursor" // no snapshot plan for cursor
		})

		origSnapshot := usageSnapshotPath
		usageSnapshotPath = filepath.Join(t.TempDir(), "nonexistent.json")
		defer func() { usageSnapshotPath = origSnapshot }()

		before := time.Now().UTC().Add(1 * time.Hour)
		fake.sendErrs = []error{errors.New("429 resource_exhausted: quota limit exceeded")}
		scanAndWait(t, manager)
		after := time.Now().UTC().Add(1 * time.Hour)

		waitingPath := filepath.Join(base, "waiting", name)
		doc, err := ReadDocument(waitingPath)
		if err != nil {
			t.Fatalf("task was not parked in waiting/: %v", err)
		}
		resetStr, _ := doc.FrontMatter["quota_reset_at"].(string)
		parsedReset, err := time.Parse(time.RFC3339, resetStr)
		if err != nil {
			t.Fatalf("unparseable quota_reset_at: %v", resetStr)
		}
		if parsedReset.Before(before.Add(-5*time.Second)) || parsedReset.After(after.Add(5*time.Second)) {
			t.Fatalf("parsedReset = %v, want ~1h from now (%v..%v)", parsedReset, before, after)
		}
		if credit, _ := doc.FrontMatter[resumeCreditField].(bool); !credit {
			t.Fatalf("resume_credit was not set: %#v", doc.FrontMatter)
		}
	}
}

// TestQuotaErrorParksAtRealPlanResetForRosterRoutedTasks pins the 2026-09-28
// quota-wall defects: every roster-routed park (no agent/runner pin; the seat
// is chosen at dispatch) named "default" and woke a blind hour later because
// recordQuotaError resolved the runner only from the document, and
// readSnapshotResetTime returned the EARLIEST reset among exhausted windows,
// so a plan with a short and a long window both at 100% woke at the short one
// and re-hit the wall until the long one cleared.
func TestQuotaErrorParksAtRealPlanResetForRosterRoutedTasks(t *testing.T) {
	writeRoster := func(t *testing.T, cfg config.Config) {
		t.Helper()
		rosterBody := `
staff:
  implementer: {runner: opencode, args: ["--model", "kimi-for-coding/kimi-k3"]}
  zai-code: {runner: pi, args: ["--model", "zai/glm-5.3"]}
roles:
  task_implementation: implementer
`
		if err := os.MkdirAll(cfg.StatePath(), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(roster.Path(cfg.StatePath()), []byte(rosterBody), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeSnapshot := func(t *testing.T, snapContent string) {
		t.Helper()
		snapFile := filepath.Join(t.TempDir(), "snapshot.json")
		if err := os.WriteFile(snapFile, []byte(snapContent), 0o644); err != nil {
			t.Fatal(err)
		}
		origSnapshot := usageSnapshotPath
		usageSnapshotPath = snapFile
		t.Cleanup(func() { usageSnapshotPath = origSnapshot })
	}
	resetStamp := func(d time.Duration) string {
		return time.Now().Add(d).UTC().Truncate(time.Second).Format(time.RFC3339)
	}

	// (a) role-routed seat (no agent/runner pin): the wake is the plan's LAST
	// exhausted reset and the note names the seat, not "default".
	{
		cfg, fake, manager := workflowTestManager(t)
		writeRoster(t, cfg)
		route := workflowRoutes()[0]
		task, err := Create(cfg, "tasks", "task 429 role-routed seat", "")
		if err != nil {
			t.Fatal(err)
		}
		name := filepath.Base(task)
		base := filepath.Dir(cfg.Resolve(route.Source))

		rollingReset := resetStamp(3 * time.Hour)
		weeklyReset := resetStamp(4 * 24 * time.Hour)
		writeSnapshot(t, fmt.Sprintf(`{"plans":{"kimi_coding":{"rolling":{"pct":100,"resets_at":"%s"},"weekly":{"pct":100,"resets_at":"%s"}}}}`, rollingReset, weeklyReset))

		fake.sendErrs = []error{errors.New("429 Too Many Requests")}
		scanAndWait(t, manager)

		waitingPath := filepath.Join(base, "waiting", name)
		doc, err := ReadDocument(waitingPath)
		if err != nil {
			t.Fatalf("task was not parked in waiting/: %v", err)
		}
		if doc.FrontMatter["wake_at"] != weeklyReset {
			t.Errorf("wake_at = %v, want the weekly (last exhausted) reset %s", doc.FrontMatter["wake_at"], weeklyReset)
		}
		if doc.FrontMatter["quota_reset_at"] != weeklyReset {
			t.Errorf("quota_reset_at = %v, want %s", doc.FrontMatter["quota_reset_at"], weeklyReset)
		}
		if strings.Contains(doc.Body, "quota wall on default") {
			t.Errorf("note still names the default runner: %s", doc.Body)
		}
		if !strings.Contains(doc.Body, "quota wall on implementer; parked until "+weeklyReset+"; attempt not spent") {
			t.Errorf("progress note missing seat name: %s", doc.Body)
		}
		if credit, _ := doc.FrontMatter[resumeCreditField].(bool); !credit {
			t.Errorf("resume_credit was not set: %#v", doc.FrontMatter)
		}
		leases, _ := manager.loadLeases()
		if len(leases) != 0 {
			t.Errorf("lease still exists: %#v", leases)
		}
	}

	// (b) staff-pinned pi zai seat: plan zai, two exhausted windows, the wake
	// is the LATER one.
	{
		cfg, fake, manager := workflowTestManager(t)
		writeRoster(t, cfg)
		route := workflowRoutes()[0]
		task, err := Create(cfg, "tasks", "task 429 staff-pinned zai seat", "")
		if err != nil {
			t.Fatal(err)
		}
		editFrontMatter(t, task, func(fm map[string]any) {
			fm["staff"] = "zai-code"
			fm["pinned_by"] = "joey"
		})
		name := filepath.Base(task)
		base := filepath.Dir(cfg.Resolve(route.Source))

		fiveHourReset := resetStamp(1 * time.Hour)
		weeklyReset := resetStamp(2 * 24 * time.Hour)
		writeSnapshot(t, fmt.Sprintf(`{"plans":{"zai":{"five_hour":{"pct":100,"resets_at":"%s"},"weekly":{"pct":100,"resets_at":"%s"}}}}`, fiveHourReset, weeklyReset))

		fake.sendErrs = []error{errors.New("429 rate limit exceeded")}
		scanAndWait(t, manager)

		waitingPath := filepath.Join(base, "waiting", name)
		doc, err := ReadDocument(waitingPath)
		if err != nil {
			t.Fatalf("task was not parked in waiting/: %v", err)
		}
		if doc.FrontMatter["wake_at"] != weeklyReset {
			t.Errorf("wake_at = %v, want the weekly (last exhausted) reset %s", doc.FrontMatter["wake_at"], weeklyReset)
		}
		if !strings.Contains(doc.Body, "quota wall on zai-code; parked until "+weeklyReset+"; attempt not spent") {
			t.Errorf("progress note missing seat name: %s", doc.Body)
		}
	}

	// (c) legacy agent pin, two exhausted windows: the wake is the LATER one
	// on the legacy path too (readSnapshotResetTime used to return the
	// earliest).
	{
		cfg, fake, manager := workflowTestManager(t)
		route := workflowRoutes()[0]
		task, err := Create(cfg, "tasks", "task 429 legacy latest exhausted", "")
		if err != nil {
			t.Fatal(err)
		}
		editFrontMatter(t, task, func(fm map[string]any) {
			fm["agent"] = "kimi"
		})
		name := filepath.Base(task)
		base := filepath.Dir(cfg.Resolve(route.Source))

		fiveHourReset := resetStamp(45 * time.Minute)
		weeklyReset := resetStamp(2 * 24 * time.Hour)
		writeSnapshot(t, fmt.Sprintf(`{"plans":{"kimi_coding":{"five_hour":{"pct":100,"resets_at":"%s"},"weekly":{"pct":100,"resets_at":"%s"}}}}`, fiveHourReset, weeklyReset))

		fake.sendErrs = []error{errors.New("429 Too Many Requests")}
		scanAndWait(t, manager)

		waitingPath := filepath.Join(base, "waiting", name)
		doc, err := ReadDocument(waitingPath)
		if err != nil {
			t.Fatalf("task was not parked in waiting/: %v", err)
		}
		if doc.FrontMatter["wake_at"] != weeklyReset {
			t.Errorf("wake_at = %v, want the weekly (last exhausted) reset %s", doc.FrontMatter["wake_at"], weeklyReset)
		}
		if !strings.Contains(doc.Body, "quota wall on kimi; parked until "+weeklyReset+"; attempt not spent") {
			t.Errorf("progress note missing runner name: %s", doc.Body)
		}
	}
}

func TestCreditExhaustionParksLikeAQuotaWall(t *testing.T) {
	// A headless credit seat whose prepaid API credit is spent fails with Claude Code's
	// "Credit balance is too low": a quota wall, not the task's fault.
	cfg, fake, manager := workflowTestManager(t)
	route := workflowRoutes()[0]
	task, err := Create(cfg, "tasks", "task on a spent credit", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	base := filepath.Dir(cfg.Resolve(route.Source))
	fake.sendErrs = []error{errors.New("runner turn failed: Credit balance is too low")}

	scanAndWait(t, manager)

	doc, err := ReadDocument(filepath.Join(base, "waiting", name))
	if err != nil {
		t.Fatalf("task was not parked in waiting/: %v", err)
	}
	if !strings.Contains(doc.Body, "attempt not spent") {
		t.Fatalf("progress note missing: %s", doc.Body)
	}
	for _, msg := range []string{"Credit balance too low · Add funds", "Your credit balance is too low to access the Anthropic API"} {
		if !isQuotaError(errors.New(msg)) {
			t.Errorf("isQuotaError(%q) = false", msg)
		}
	}
}

func TestIsQuotaErrorMatches429OnlyAsAWholeToken(t *testing.T) {
	for _, msg := range []string{
		"herdr prompt failed: 429 You've reached your weekly usage limit",
		"429 Too Many Requests",
		"API Error: 429 {\"type\":\"error\"}",
		"HTTP/1.1 429",
		"status=429",
		"request failed (429)",
		"upstream returned error 429.",
		"rate limit exceeded",
	} {
		if !isQuotaError(errors.New(msg)) {
			t.Errorf("isQuotaError(%q) = false, want true", msg)
		}
	}
	for _, msg := range []string{
		"read tasks/todo/fix-issue-429.md: no such file or directory",
		"open /var/log/20260429.log: permission denied",
		"worker sj-t-a1-4291 never answered",
		"build 1429 failed",
		"version 1.429.0 not found",
		"/api/v1/429/items returned bad json",
		"job_429abc exited",
	} {
		if isQuotaError(errors.New(msg)) {
			t.Errorf("isQuotaError(%q) = true, want false", msg)
		}
	}
}

func TestPathDigitsInErrorAreNotAQuotaWall(t *testing.T) {
	// An error that merely mentions "429" inside a path or a date is an ordinary dispatch
	// error: it counts toward the 3-error failed/ path and is never parked as a quota wall.
	cfg, fake, manager := workflowTestManager(t)
	route := workflowRoutes()[0]
	task, err := Create(cfg, "tasks", "task with digits in its error", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	base := filepath.Dir(cfg.Resolve(route.Source))
	fake.sendErrs = []error{errors.New("runner turn failed: read tasks/todo/fix-issue-429.md 20260429: no such file")}

	scanAndWait(t, manager)

	if _, err := ReadDocument(filepath.Join(base, "waiting", name)); err == nil {
		t.Fatalf("task was parked in waiting/ as a quota wall")
	}
	leases, err := manager.loadLeases()
	if err != nil {
		t.Fatal(err)
	}
	if len(leases) != 1 || leases[0].State != "error" || leases[0].RecoveryCount != 1 {
		t.Fatalf("lease = %+v, want one lease in error state with RecoveryCount 1", leases)
	}
	// recordError writes the autopsy on a detached goroutine; wait for it so it cannot race
	// t.TempDir cleanup.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if matches, _ := filepath.Glob(filepath.Join(cfg.StatePath("diagnostics"), "err-*.json")); len(matches) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Kimi's walls are a 403 without "429", "quota limit" or "rate limit" in the text (the 2026-10-15
// 10.5 h stall); a headless Kimi seat hands this line back as its turn failure.
func TestIsQuotaErrorMatchesKimiUsageLimitWalls(t *testing.T) {
	for _, msg := range []string{
		"runner turn failed: error: failed to run prompt: [provider.auth_error] 403 You've reached your 5-hour usage limit. Your quota will reset when the current 5-hour window ends.",
		"runner turn failed: error: failed to run prompt: 403 You've reached your weekly (7-day) usage limit",
	} {
		if !isQuotaError(errors.New(msg)) {
			t.Errorf("isQuotaError(%q) = false, want true", msg)
		}
	}
	for _, msg := range []string{
		"runner turn failed: error: failed to run prompt: [provider.auth_error] 401 invalid token",
		"runner turn failed: error: failed to run prompt: process exceeded its memory usage limit",
		"runner turn failed: MCP server venice: usage limit for this tool is 10 calls",
	} {
		if isQuotaError(errors.New(msg)) {
			t.Errorf("isQuotaError(%q) = true; only Kimi's wall wording is a quota wall", msg)
		}
	}
}
