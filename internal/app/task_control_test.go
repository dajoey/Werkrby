package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/harness"
	"github.com/agent0ai/spynel/internal/orchestrator"
	"github.com/agent0ai/spynel/internal/workspace"
)

// taskControlHarness records every control primitive the task-control route
// can reach: queued control sends, live (interrupt) deliveries, queued-control
// withdrawals, and job interrupts.
type taskControlHarness struct {
	*heldServiceHarness
	controlRequests   []harness.ControlRequest
	controlResult     harness.ControlResult
	controlErr        error
	liveRequests      []harness.ControlRequest
	liveErr           error
	cancelledControls []string
	interruptFn       func(ctx context.Context, key string) (bool, error)
}

func (h *taskControlHarness) Interrupt(ctx context.Context, key string) (bool, error) {
	if h.interruptFn != nil {
		return h.interruptFn(ctx, key)
	}
	return h.heldServiceHarness.Interrupt(ctx, key)
}

func (h *taskControlHarness) SendControl(_ context.Context, _ string, request harness.ControlRequest) (harness.ControlResult, error) {
	h.controlRequests = append(h.controlRequests, request)
	return h.controlResult, h.controlErr
}

func (h *taskControlHarness) SendLiveControl(_ context.Context, _ string, request harness.ControlRequest) (harness.ControlResult, error) {
	h.liveRequests = append(h.liveRequests, request)
	return harness.ControlResult{}, h.liveErr
}

func (h *taskControlHarness) CancelQueuedControl(_ string, id string) bool {
	if id == "" {
		return false
	}
	for _, known := range h.cancelledControls {
		if known == id {
			return false
		}
	}
	h.cancelledControls = append(h.cancelledControls, id)
	return true
}

func newTaskControlService(t *testing.T) (*Service, *taskControlHarness) {
	t.Helper()
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	target := &taskControlHarness{heldServiceHarness: newHeldServiceHarness(), controlResult: harness.ControlResult{Queued: true}}
	service := New(cfg, target)
	return service, target
}

// claimTaskForControl stages a claimed task exactly like a live dispatch:
// document in working/, a lease naming the canonical orchestrator session key,
// and a live durable job bound to that session.
func claimTaskForControl(t *testing.T, service *Service, taskID string, attempt int, frontMatter map[string]any) (string, orchestrator.Lease) {
	t.Helper()
	path := filepath.Join(service.Config.StatePath("tasks", "working"), taskID+".md")
	if frontMatter == nil {
		frontMatter = map[string]any{}
	}
	frontMatter["id"] = taskID
	if _, ok := frontMatter["status"]; !ok {
		frontMatter["status"] = "working"
	}
	if _, ok := frontMatter["title"]; !ok {
		frontMatter["title"] = "Task control fixture"
	}
	if _, ok := frontMatter["created_at"]; !ok {
		frontMatter["created_at"] = "2026-09-25T08:00:00Z"
	}
	if _, ok := frontMatter["updated_at"]; !ok {
		frontMatter["updated_at"] = "2026-09-25T08:00:00Z"
	}
	if _, ok := frontMatter["review_required"]; !ok {
		frontMatter["review_required"] = true
	}
	if _, ok := frontMatter["attempt"]; !ok {
		frontMatter["attempt"] = attempt
	}
	document := orchestrator.Document{FrontMatter: frontMatter, Body: "# Task\n\n## Progress\n\n- claimed\n"}
	if err := orchestrator.WriteDocument(path, document); err != nil {
		t.Fatal(err)
	}
	lease := orchestrator.Lease{
		ID: "lease-" + taskID, OwnerID: "owner-" + taskID,
		SessionKey: fmt.Sprintf("orchestrator:tasks:task_implementation:%s:%d", taskID, attempt),
		File:       path, Route: "tasks", State: "processing", Phase: "task_implementation",
		StartedAt: time.Now().UTC(), HeartbeatAt: time.Now().UTC(), ClaimAttempt: attempt,
	}
	writeJobLease(t, service, lease)
	service.Runtime.BeginJobWithDetails(lease.SessionKey, "orchestrator", "markdown", taskID+".md", JobDetails{
		Kind: "task", Route: "tasks", DurableFile: path,
	})
	return path, lease
}

func taskControlCode(err error) string {
	if err == nil {
		return ""
	}
	var coded interface{ Code() string }
	if errors.As(err, &coded) {
		return coded.Code()
	}
	return ""
}

func TestTaskControlResolvesLiveJobByTaskIDWithStrongIdentityChecks(t *testing.T) {
	service, _ := newTaskControlService(t)
	taskID := "tasks-20260925-control-demo"
	path, _ := claimTaskForControl(t, service, taskID, 2, nil)
	jobID := service.Runtime.Jobs()[0].ID
	service.Runtime.RecordJobEvent(jobID, core.Event{Kind: core.EventStatus, Text: "working hard", Done: true})

	result, err := service.TaskControl(context.Background(), TaskControlRequest{TaskID: taskID, Action: "output", Tail: 4096})
	if err != nil {
		t.Fatalf("output control failed: %v", err)
	}
	if !result.OK || result.Job != service.Runtime.Jobs()[0].Number || !strings.Contains(result.Output, "working hard") {
		t.Fatalf("output result = %#v", result)
	}

	// Unknown id resolves to a routable not_found.
	if _, err := service.TaskControl(context.Background(), TaskControlRequest{TaskID: "tasks-nope", Action: "output"}); taskControlCode(err) != "not_found" {
		t.Fatalf("unknown task error = %v code=%q", err, taskControlCode(err))
	}

	// A job whose durable document names a different id is never resolved:
	// the strong identity check must reject session-key matches alone.
	if err := os.WriteFile(path, []byte("---\nid: tasks-someone-else\nstatus: working\ntitle: swap\n---\n# Swapped\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := service.TaskControl(context.Background(), TaskControlRequest{TaskID: taskID, Action: "output"}); taskControlCode(err) != "not_found" {
		t.Fatalf("swapped identity error = %v code=%q", err, taskControlCode(err))
	}

	// Terminal execution states are not steerable.
	if err := os.WriteFile(path, []byte("---\nid: "+taskID+"\nstatus: working\ntitle: fixture\n---\n# Task\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	service.Runtime.UpdateJob(jobID, core.ExecutionStatus{State: string(JobAwaitingTransition)})
	if _, err := service.TaskControl(context.Background(), TaskControlRequest{TaskID: taskID, Action: "output"}); taskControlCode(err) != "not_steerable" {
		t.Fatalf("terminal error = %v code=%q", err, taskControlCode(err))
	}
}

func TestTaskControlResolutionIgnoresNonTaskJobsAndNotificationOrigins(t *testing.T) {
	service, target := newTaskControlService(t)
	taskID := "tasks-20260925-control-cross"
	_, lease := claimTaskForControl(t, service, taskID, 1, map[string]any{
		"notify": map[string]any{"enabled": true, "origin": "telegram/TG-private", "on": []any{"done"}},
	})
	// A goal job and a chat job share the task id inside their session keys but
	// must never resolve: control admission is kind-based, with no conversation,
	// channel, or notification-origin filtering of the resolved task job itself.
	service.Runtime.BeginJobWithDetails(fmt.Sprintf("orchestrator:goals:goal_planning:%s:1", taskID), "orchestrator", "markdown", taskID+".md", JobDetails{Kind: "goal", Route: "goals"})
	service.Runtime.BeginJob("chat:tui:"+taskID, "tui", "local", "question")

	result, err := service.TaskControl(context.Background(), TaskControlRequest{TaskID: taskID, Action: "message", Text: "cross-origin control"})
	if err != nil {
		t.Fatalf("control failed: %v", err)
	}
	if !result.Queued || result.ControlID == "" || len(target.controlRequests) != 1 {
		t.Fatalf("cross-origin result = %#v requests=%d", result, len(target.controlRequests))
	}
	if session := service.Runtime.Jobs()[0].SessionKey; session != lease.SessionKey {
		t.Fatalf("resolved session %q, want the task job %q", session, lease.SessionKey)
	}
}

func TestTaskControlMessageQueuesWithAuditableControlIDAndEnvelope(t *testing.T) {
	service, target := newTaskControlService(t)
	taskID := "tasks-20260925-control-msg"
	claimTaskForControl(t, service, taskID, 1, nil)

	result, err := service.TaskControl(context.Background(), TaskControlRequest{TaskID: taskID, Action: "message", Text: "prioritise the failing test first"})
	if err != nil {
		t.Fatalf("message control failed: %v", err)
	}
	if !result.Queued || result.ControlID == "" {
		t.Fatalf("message result = %#v", result)
	}
	if len(target.controlRequests) != 1 {
		t.Fatalf("control requests = %d", len(target.controlRequests))
	}
	request := target.controlRequests[0]
	for _, want := range []string{
		"nonterminal operator coordination",
		`encoding="json"`,
		"prioritise the failing test first",
		"quote it verbatim",
	} {
		if !strings.Contains(request.Prompt, want) {
			t.Fatalf("control prompt missing %q:\n%s", want, request.Prompt)
		}
	}
	if request.ContinuationPrompt == "" || request.Validate == nil || request.PrepareContinuation == nil || request.ReserveProviderTurn == nil {
		t.Fatalf("continuation guards missing: %#v", request)
	}

	// The queued envelope must instruct an auditable verbatim Progress quote.
	if !strings.Contains(request.Prompt, "## Progress") {
		t.Fatalf("prompt does not point at the durable progress log:\n%s", request.Prompt)
	}
}

func TestTaskControlInterruptDeliversIntoLiveSessionNow(t *testing.T) {
	service, target := newTaskControlService(t)
	taskID := "tasks-20260925-control-int"
	claimTaskForControl(t, service, taskID, 3, nil)

	result, err := service.TaskControl(context.Background(), TaskControlRequest{TaskID: taskID, Action: "interrupt", Text: "change of plan: ship the small fix"})
	if err != nil {
		t.Fatalf("interrupt control failed: %v", err)
	}
	if !result.Delivered || result.Queued {
		t.Fatalf("interrupt result = %#v", result)
	}
	if len(target.liveRequests) != 1 || len(target.controlRequests) != 0 {
		t.Fatalf("live=%d queued=%d", len(target.liveRequests), len(target.controlRequests))
	}
	if !strings.Contains(target.liveRequests[0].Prompt, "change of plan") {
		t.Fatalf("live prompt missing text:\n%s", target.liveRequests[0].Prompt)
	}

	// A harness without any live delivery capability reports a routable error.
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(config.PathForRoot(root))
	bare := New(cfg, newHeldServiceHarness())
	otherID := "tasks-20260925-control-bare"
	claimTaskForControl(t, bare, otherID, 1, nil)
	_, err = bare.TaskControl(context.Background(), TaskControlRequest{TaskID: otherID, Action: "interrupt", Text: "hello"})
	if taskControlCode(err) != "unsupported" {
		t.Fatalf("bare harness error = %v code=%q", err, taskControlCode(err))
	}
}

func TestTaskControlCancelWithdrawsQueuedMessageByID(t *testing.T) {
	service, _ := newTaskControlService(t)
	taskID := "tasks-20260925-control-cancel"
	claimTaskForControl(t, service, taskID, 1, nil)

	first, err := service.TaskControl(context.Background(), TaskControlRequest{TaskID: taskID, Action: "message", Text: "first thought"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.TaskControl(context.Background(), TaskControlRequest{TaskID: taskID, Action: "cancel-queued", ControlID: first.ControlID})
	if err != nil || !result.Cancelled {
		t.Fatalf("cancel result = %#v err = %v", result, err)
	}
	again, err := service.TaskControl(context.Background(), TaskControlRequest{TaskID: taskID, Action: "cancel-queued", ControlID: first.ControlID})
	if err != nil || again.Cancelled {
		t.Fatalf("double cancel result = %#v err = %v", again, err)
	}
}

func TestTaskControlOutputIsBounded(t *testing.T) {
	service, _ := newTaskControlService(t)
	taskID := "tasks-20260925-control-out"
	claimTaskForControl(t, service, taskID, 1, nil)
	jobID := service.Runtime.Jobs()[0].ID
	service.Runtime.RecordJobEvent(jobID, core.Event{Kind: core.EventStatus, Text: strings.Repeat("x", 4096)})

	result, err := service.TaskControl(context.Background(), TaskControlRequest{TaskID: taskID, Action: "output", Tail: 512})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Output) > 4096 || result.State == "" {
		t.Fatalf("bounded output = %d bytes state=%q", len(result.Output), result.State)
	}
	if _, err := service.TaskControl(context.Background(), TaskControlRequest{TaskID: taskID, Action: "output", Tail: 1 << 20}); taskControlCode(err) != "bad_request" {
		t.Fatalf("oversized tail error = %v code=%q", err, taskControlCode(err))
	}
}

func TestTaskControlStopStopsTheRunWithoutTouchingTheDocument(t *testing.T) {
	service, target := newTaskControlService(t)
	taskID := "tasks-20260925-control-stop"
	path, lease := claimTaskForControl(t, service, taskID, 1, nil)
	target.active[lease.SessionKey] = true

	result, err := service.TaskControl(context.Background(), TaskControlRequest{TaskID: taskID, Action: "stop"})
	if err != nil {
		t.Fatalf("stop control failed: %v", err)
	}
	if !result.Stopped {
		t.Fatalf("stop result = %#v", result)
	}
	if _, still := service.Runtime.Job(service.Runtime.Jobs()[0].ID); false {
		_ = still
	}
	if data, err := os.ReadFile(path); err != nil || !strings.Contains(string(data), "status: working") {
		t.Fatalf("stop mutated the document: err=%v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("document left working/: %v", err)
	}
}

func settleFixture(t *testing.T, action string) (string, orchestrator.Document) {
	t.Helper()
	service, target := newTaskControlService(t)
	taskID := "tasks-20260925-control-" + action
	path, lease := claimTaskForControl(t, service, taskID, 2, nil)
	target.active[lease.SessionKey] = true

	result, err := service.TaskControl(context.Background(), TaskControlRequest{TaskID: taskID, Action: action})
	if err != nil {
		t.Fatalf("%s control failed: %v", action, err)
	}
	if !result.Stopped {
		t.Fatalf("%s result = %#v", action, result)
	}
	settled := filepath.Join(filepath.Dir(filepath.Dir(path)), result.State, filepath.Base(path))
	document, err := orchestrator.ReadDocument(settled)
	if err != nil {
		t.Fatalf("%s settled document unreadable at %s: %v", action, settled, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("%s left the claimed document in working/: %v", action, err)
	}
	return settled, document
}

func TestTaskControlStopDoneSettlesCheckerAcceptedCompletion(t *testing.T) {
	settled, document := settleFixture(t, "stop-done")

	if status, _ := document.FrontMatter["status"].(string); status != "done" {
		t.Fatalf("settled status = %q", status)
	}
	summary, ok := document.FrontMatter["completion_summary"].(map[string]any)
	if !ok {
		t.Fatalf("no completion summary: %#v", document.FrontMatter["completion_summary"])
	}
	if verdict, _ := summary["verdict"].(string); verdict != "completed" {
		t.Fatalf("verdict = %#v", summary["verdict"])
	}
	updated, _ := document.FrontMatter["updated_at"].(string)
	completed, _ := summary["completed_at"].(string)
	if updated == "" || completed != updated {
		t.Fatalf("completed_at %q must equal updated_at %q", completed, updated)
	}
	for _, key := range []string{"outcome", "evidence", "uncertainty"} {
		if value, _ := summary[key].(string); strings.TrimSpace(value) == "" {
			t.Fatalf("summary.%s is empty: %#v", key, summary[key])
		}
	}
	progress := document.Body
	if !strings.Contains(progress, "Operator control (Helm web conversation)") || !strings.Contains(progress, "Stop and done") {
		t.Fatalf("progress lacks the operator authority line:\n%s", progress)
	}
	if review, ok := document.FrontMatter["review_required"].(bool); ok && review {
		t.Fatalf("operator done settle left review_required true: %#v", document.FrontMatter["review_required"])
	}
	if !strings.Contains(progress, "independent-review requirement") {
		t.Fatalf("progress lacks the recorded authority for clearing review:\n%s", progress)
	}
	_ = settled
}

func TestTaskControlStopCancelDocumentsOperatorAuthority(t *testing.T) {
	settled, document := settleFixture(t, "stop-cancel")

	if status, _ := document.FrontMatter["status"].(string); status != "cancelled" {
		t.Fatalf("settled status = %q", status)
	}
	summary, ok := document.FrontMatter["completion_summary"].(map[string]any)
	if !ok {
		t.Fatalf("no completion summary: %#v", document.FrontMatter["completion_summary"])
	}
	if verdict, _ := summary["verdict"].(string); verdict != "cancelled" {
		t.Fatalf("verdict = %#v", summary["verdict"])
	}
	if !strings.Contains(document.Body, "Stop and cancel") || !strings.Contains(document.Body, "Authority") {
		t.Fatalf("progress lacks the cancellation authority:\n%s", document.Body)
	}
	_ = settled
}

func TestTaskControlValidatesPayloadBounds(t *testing.T) {
	service, _ := newTaskControlService(t)
	taskID := "tasks-20260925-control-bounds"
	claimTaskForControl(t, service, taskID, 1, nil)

	for _, request := range []TaskControlRequest{
		{Action: "output"},
		{TaskID: taskID},
		{TaskID: taskID, Action: "explode"},
		{TaskID: strings.Repeat("t", 200), Action: "output"},
		{TaskID: taskID, Action: "message", Text: strings.Repeat("x", 9000)},
		{TaskID: taskID, Action: "interrupt", Text: "   "},
	} {
		if _, err := service.TaskControl(context.Background(), request); taskControlCode(err) != "bad_request" {
			t.Fatalf("request %#v error = %v code=%q", request, err, taskControlCode(err))
		}
	}
}

func TestTaskControlStopFamilyAcksAsynchronouslyWhileInterruptBlocks(t *testing.T) {
	for _, action := range []string{"stop", "stop-done", "stop-cancel"} {
		t.Run(action, func(t *testing.T) {
			service, target := newTaskControlService(t)
			taskID := "tasks-20260925-async-stop-" + action
			path, lease := claimTaskForControl(t, service, taskID, 1, nil)
			target.active[lease.SessionKey] = true

			interruptStarted := make(chan struct{})
			releaseInterrupt := make(chan struct{})
			defer close(releaseInterrupt)

			target.interruptFn = func(ctx context.Context, key string) (bool, error) {
				close(interruptStarted)
				select {
				case <-releaseInterrupt:
					return true, nil
				case <-ctx.Done():
					return false, ctx.Err()
				}
			}

			type callResult struct {
				res TaskControlResult
				err error
			}
			done := make(chan callResult, 1)
			go func() {
				res, err := service.TaskControl(context.Background(), TaskControlRequest{TaskID: taskID, Action: action})
				done <- callResult{res: res, err: err}
			}()

			select {
			case r := <-done:
				if r.err != nil {
					t.Fatalf("%s control failed: %v", action, r.err)
				}
				if !r.res.Stopped {
					t.Fatalf("%s result = %#v", action, r.res)
				}
				switch action {
				case "stop":
					if r.res.State != "stopping" {
						t.Fatalf("expected state 'stopping', got %q", r.res.State)
					}
					if _, err := os.Stat(path); err != nil {
						t.Fatalf("document left working/: %v", err)
					}
				case "stop-done":
					if r.res.State != "done" || r.res.Settled != "done" {
						t.Fatalf("expected state 'done' and settled 'done', got %#v", r.res)
					}
					settled := filepath.Join(filepath.Dir(filepath.Dir(path)), "done", filepath.Base(path))
					if _, err := os.Stat(settled); err != nil {
						t.Fatalf("document did not settle into done/: %v", err)
					}
				case "stop-cancel":
					if r.res.State != "cancelled" || r.res.Settled != "cancelled" {
						t.Fatalf("expected state 'cancelled' and settled 'cancelled', got %#v", r.res)
					}
					settled := filepath.Join(filepath.Dir(filepath.Dir(path)), "cancelled", filepath.Base(path))
					if _, err := os.Stat(settled); err != nil {
						t.Fatalf("document did not settle into cancelled/: %v", err)
					}
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("action %q did not ack within 2s while Interrupt was blocking", action)
			}
		})
	}
}

func TestTaskControlStopJobEndsWithinGraceWhenTurnNeverResolves(t *testing.T) {
	service, target := newTaskControlService(t)
	service.jobCancellationGrace = 50 * time.Millisecond
	taskID := "tasks-20260925-never-resolves-stop"
	_, lease := claimTaskForControl(t, service, taskID, 1, nil)
	jobID := service.Runtime.Jobs()[0].ID
	target.active[lease.SessionKey] = true

	// Turn never resolves: Interrupt returns true, but IsActive remains true
	target.interruptFn = func(ctx context.Context, key string) (bool, error) {
		return true, nil
	}

	result, err := service.TaskControl(context.Background(), TaskControlRequest{TaskID: taskID, Action: "stop"})
	if err != nil {
		t.Fatalf("stop control failed: %v", err)
	}
	if !result.Stopped {
		t.Fatalf("stop result = %#v", result)
	}

	// Also simulate an intermediate lease update to awaiting_transition
	// (this must not prevent the cancelling job from reaching EndJob)
	service.Runtime.UpdateJobFromLease(jobID, "awaiting_transition", "task_implementation", "", time.Now().UTC(), 0)

	// Poll Runtime for EndJob within grace (50ms) + 1 poll interval (200ms) + small buffer
	deadline := time.Now().Add(500 * time.Millisecond)
	ended := false
	for time.Now().Before(deadline) {
		if _, ok := service.Runtime.Job(jobID); !ok {
			ended = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !ended {
		job, _ := service.Runtime.Job(jobID)
		t.Fatalf("job %d remained alive past grace (execution: %s)", jobID, job.Execution)
	}
}

func stageTaskInFolder(t *testing.T, service *Service, taskID, folder string, frontMatter map[string]any) string {
	t.Helper()
	dir := service.Config.StatePath("tasks", folder)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, taskID+".md")
	if frontMatter == nil {
		frontMatter = map[string]any{}
	}
	frontMatter["id"] = taskID
	if _, ok := frontMatter["status"]; !ok {
		frontMatter["status"] = folder
	}
	if _, ok := frontMatter["title"]; !ok {
		frontMatter["title"] = "Task fixture in " + folder
	}
	if _, ok := frontMatter["created_at"]; !ok {
		frontMatter["created_at"] = "2026-09-25T08:00:00Z"
	}
	if _, ok := frontMatter["updated_at"]; !ok {
		frontMatter["updated_at"] = "2026-09-25T08:00:00Z"
	}
	doc := orchestrator.Document{FrontMatter: frontMatter, Body: "# Task in " + folder + "\n\n## Progress\n\n- staged\n"}
	if err := orchestrator.WriteDocument(path, doc); err != nil {
		t.Fatal(err)
	}
	return path
}

func claimTaskForReview(t *testing.T, service *Service, taskID string, attempt int, frontMatter map[string]any) (string, orchestrator.Lease) {
	t.Helper()
	dir := service.Config.StatePath("tasks", "reviewing")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, taskID+".md")
	if frontMatter == nil {
		frontMatter = map[string]any{}
	}
	frontMatter["id"] = taskID
	frontMatter["status"] = "reviewing"
	frontMatter["title"] = "Reviewing fixture"
	frontMatter["created_at"] = "2026-09-25T08:00:00Z"
	frontMatter["updated_at"] = "2026-09-25T08:00:00Z"
	frontMatter["attempt"] = 1
	frontMatter["review_attempt"] = attempt
	doc := orchestrator.Document{FrontMatter: frontMatter, Body: "# Reviewing\n\n## Progress\n\n- reviewing\n"}
	if err := orchestrator.WriteDocument(path, doc); err != nil {
		t.Fatal(err)
	}
	lease := orchestrator.Lease{
		ID: "lease-review-" + taskID, OwnerID: "owner-review-" + taskID,
		SessionKey: fmt.Sprintf("orchestrator:tasks:task_review:%s:%d", taskID, attempt),
		File:       path, Route: "tasks", State: "processing", Phase: "task_review",
		StartedAt: time.Now().UTC(), HeartbeatAt: time.Now().UTC(), ClaimAttempt: attempt,
	}
	writeJobLease(t, service, lease)
	service.Runtime.BeginJobWithDetails(lease.SessionKey, "orchestrator", "markdown", taskID+".md", JobDetails{
		Kind: "task", Route: "tasks", DurableFile: path,
	})
	return path, lease
}

func TestTaskControlApproveSettlesReviewStateTaskDone(t *testing.T) {
	service, _ := newTaskControlService(t)
	taskID := "tasks-20260925-approve-demo"
	reviewPath := stageTaskInFolder(t, service, taskID, "review", map[string]any{
		"review_required": true,
	})

	result, err := service.TaskControl(context.Background(), TaskControlRequest{
		TaskID: taskID,
		Action: "approve",
		Text:   "Looks great, approved",
	})
	if err != nil {
		t.Fatalf("approve failed: %v", err)
	}
	if !result.OK || result.State != "done" || result.Settled != "done" {
		t.Fatalf("approve result = %#v", result)
	}

	// File must be gone from review/ and present in done/
	if _, err := os.Stat(reviewPath); !os.IsNotExist(err) {
		t.Fatalf("file still exists in review/: %v", err)
	}
	donePath := filepath.Join(service.Config.StatePath("tasks", "done"), taskID+".md")
	doc, err := orchestrator.ReadDocument(donePath)
	if err != nil {
		t.Fatalf("failed reading done document: %v", err)
	}

	if doc.FrontMatter["status"] != "done" {
		t.Errorf("status = %v, want done", doc.FrontMatter["status"])
	}
	if req, ok := doc.FrontMatter["review_required"].(bool); ok && req {
		t.Errorf("review_required was not cleared: %v", req)
	}

	// Verify completion summary
	summary, ok := doc.FrontMatter["completion_summary"].(map[string]any)
	if !ok {
		t.Fatalf("completion_summary missing or not a map: %#v", doc.FrontMatter["completion_summary"])
	}
	if summary["verdict"] != "completed" {
		t.Errorf("summary verdict = %v, want completed", summary["verdict"])
	}
	if summary["completed_at"] == "" || summary["completed_at"] != doc.FrontMatter["updated_at"] {
		t.Errorf("completed_at (%v) != updated_at (%v)", summary["completed_at"], doc.FrontMatter["updated_at"])
	}

	// Progress must mention operator review verdict
	if !strings.Contains(doc.Body, "Operator review verdict (Helm web conversation): Approved") {
		t.Errorf("progress note missing operator approval: %s", doc.Body)
	}
}

func TestTaskControlRequestChangesReturnsSameTaskIDToTodoWithRework(t *testing.T) {
	service, _ := newTaskControlService(t)
	taskID := "tasks-20260925-request-changes-demo"
	reviewPath := stageTaskInFolder(t, service, taskID, "review", map[string]any{
		"review_required": true,
		"review_attempt":  1,
	})

	result, err := service.TaskControl(context.Background(), TaskControlRequest{
		TaskID: taskID,
		Action: "request-changes",
		Text:   "Please fix the error handling in foo.go",
	})
	if err != nil {
		t.Fatalf("request-changes failed: %v", err)
	}
	if !result.OK || result.State != "todo" || result.Settled != "" {
		t.Fatalf("request-changes result = %#v", result)
	}

	// File must be moved to todo/ with same task ID
	if _, err := os.Stat(reviewPath); !os.IsNotExist(err) {
		t.Fatalf("file still exists in review/: %v", err)
	}
	todoPath := filepath.Join(service.Config.StatePath("tasks", "todo"), taskID+".md")
	doc, err := orchestrator.ReadDocument(todoPath)
	if err != nil {
		t.Fatalf("failed reading todo document: %v", err)
	}

	if doc.FrontMatter["status"] != "todo" {
		t.Errorf("status = %v, want todo", doc.FrontMatter["status"])
	}
	// The verdict is recorded as a rejected review summary; rework_count is
	// code-managed (derived from review_attempt by the framework stamp),
	// never hand-incremented by the route.
	summary, ok := doc.FrontMatter["completion_summary"].(map[string]any)
	if !ok {
		t.Fatalf("rejected summary missing: %#v", doc.FrontMatter["completion_summary"])
	}
	if summary["verdict"] != "rejected" {
		t.Errorf("summary verdict = %v, want rejected", summary["verdict"])
	}
	if rework, ok := summary["rework_count"].(int); !ok || rework != 1 {
		t.Errorf("summary rework_count = %#v, want 1 (derived from review_attempt 1)", summary["rework_count"])
	}
	if !strings.Contains(doc.Body, "Requested changes at") || !strings.Contains(doc.Body, "Please fix the error handling in foo.go") {
		t.Errorf("progress note missing revision instruction: %s", doc.Body)
	}
}

func TestTaskControlRequestChangesRequiresNonEmptyText(t *testing.T) {
	service, _ := newTaskControlService(t)
	taskID := "tasks-20260925-req-empty"
	stageTaskInFolder(t, service, taskID, "review", nil)

	_, err := service.TaskControl(context.Background(), TaskControlRequest{
		TaskID: taskID,
		Action: "request-changes",
		Text:   "   ",
	})
	if err == nil {
		t.Fatal("expected error on empty revision instruction")
	}
	if code := taskControlCode(err); code != "bad_request" {
		t.Errorf("code = %q, want bad_request", code)
	}
}

func TestTaskControlClaimedReviewingRejectsApproveAndRequestChanges(t *testing.T) {
	service, _ := newTaskControlService(t)
	taskID := "tasks-20260925-claimed-reviewing"
	reviewingPath, _ := claimTaskForReview(t, service, taskID, 1, nil)

	// approve while claimed in reviewing must fail with review_in_progress
	_, errApprove := service.TaskControl(context.Background(), TaskControlRequest{
		TaskID: taskID,
		Action: "approve",
	})
	if errApprove == nil {
		t.Fatal("expected error approving claimed review")
	}
	if code := taskControlCode(errApprove); code != "review_in_progress" {
		t.Errorf("code = %q, want review_in_progress", code)
	}

	// request-changes while claimed in reviewing must fail with review_in_progress
	_, errReq := service.TaskControl(context.Background(), TaskControlRequest{
		TaskID: taskID,
		Action: "request-changes",
		Text:   "rejecting mid-review",
	})
	if errReq == nil {
		t.Fatal("expected error requesting changes on claimed review")
	}
	if code := taskControlCode(errReq); code != "review_in_progress" {
		t.Errorf("code = %q, want review_in_progress", code)
	}

	// File must remain untouched in reviewing/
	if _, err := os.Stat(reviewingPath); err != nil {
		t.Fatalf("reviewing file was modified or removed: %v", err)
	}
}

func TestTaskControlSnoozeSetsWakeAtOnWaitingTask(t *testing.T) {
	service, _ := newTaskControlService(t)
	taskID := "tasks-20260925-snooze-demo"
	waitingPath := stageTaskInFolder(t, service, taskID, "waiting", map[string]any{
		"waiting_for": "an event",
	})

	wakeAt := time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339)
	result, err := service.TaskControl(context.Background(), TaskControlRequest{
		TaskID: taskID,
		Action: "snooze",
		WakeAt: wakeAt,
	})
	if err != nil {
		t.Fatalf("snooze failed: %v", err)
	}
	if !result.OK || result.State != "waiting" || result.WakeAt != wakeAt {
		t.Fatalf("snooze result = %#v", result)
	}

	doc, err := orchestrator.ReadDocument(waitingPath)
	if err != nil {
		t.Fatalf("read waiting doc failed: %v", err)
	}
	if doc.FrontMatter["wake_at"] != wakeAt {
		t.Errorf("wake_at = %v, want %s", doc.FrontMatter["wake_at"], wakeAt)
	}
	if !strings.Contains(doc.Body, "Snoozed until "+wakeAt) {
		t.Errorf("progress note missing snooze log: %s", doc.Body)
	}
}

func TestTaskControlWakeNowReturnsWaitingTaskToTodo(t *testing.T) {
	service, _ := newTaskControlService(t)
	taskID := "tasks-20260925-wake-now-demo"
	wakeAt := time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339)
	waitingPath := stageTaskInFolder(t, service, taskID, "waiting", map[string]any{
		"waiting_for": "an event",
		"wake_at":     wakeAt,
	})

	result, err := service.TaskControl(context.Background(), TaskControlRequest{
		TaskID: taskID,
		Action: "wake-now",
	})
	if err != nil {
		t.Fatalf("wake-now failed: %v", err)
	}
	if !result.OK || result.State != "todo" || result.Settled != "" {
		t.Fatalf("wake-now result = %#v", result)
	}

	// Must be moved from waiting/ to todo/
	if _, err := os.Stat(waitingPath); !os.IsNotExist(err) {
		t.Fatalf("file still in waiting/: %v", err)
	}
	todoPath := filepath.Join(service.Config.StatePath("tasks", "todo"), taskID+".md")
	doc, err := orchestrator.ReadDocument(todoPath)
	if err != nil {
		t.Fatalf("read todo doc failed: %v", err)
	}
	if doc.FrontMatter["status"] != "todo" {
		t.Errorf("status = %v, want todo", doc.FrontMatter["status"])
	}
	if _, ok := doc.FrontMatter["wake_at"]; ok {
		t.Errorf("wake_at was not removed: %v", doc.FrontMatter["wake_at"])
	}
	if !strings.Contains(doc.Body, "Woke now; returned to todo/") {
		t.Errorf("progress note missing wake now log: %s", doc.Body)
	}
}

func TestTaskControlReassignRewritesStaffSurgically(t *testing.T) {
	service, _ := newTaskControlService(t)
	taskID := "tasks-20260925-reassign-demo"
	origContent := `---
# Important heading comment
attempt: 1
created_at: "2026-09-25T08:00:00Z"
id: tasks-20260925-reassign-demo
review_required: true
risk: high
staff: cursor # legacy seat
status: working
title: Task fixture
updated_at: "2026-09-25T08:00:00Z"
---

# Task body
Untouched body.

## Progress

- claimed
`
	workingDir := service.Config.StatePath("tasks", "working")
	_ = os.MkdirAll(workingDir, 0o700)
	taskPath := filepath.Join(workingDir, taskID+".md")
	if err := os.WriteFile(taskPath, []byte(origContent), 0o600); err != nil {
		t.Fatal(err)
	}

	result, err := service.TaskControl(context.Background(), TaskControlRequest{
		TaskID: taskID,
		Action: "reassign",
		Staff:  "agy",
	})
	if err != nil {
		t.Fatalf("reassign failed: %v", err)
	}
	if !result.OK || result.Staff != "agy" {
		t.Fatalf("reassign result = %#v", result)
	}

	rawBytes, err := os.ReadFile(taskPath)
	if err != nil {
		t.Fatal(err)
	}
	raw := string(rawBytes)

	if !strings.Contains(raw, "staff: agy\n") {
		t.Errorf("expected staff: agy, got:\n%s", raw)
	}
	if !strings.Contains(raw, "# Important heading comment") {
		t.Errorf("heading comment lost")
	}
	if !strings.Contains(raw, "Untouched body.") {
		t.Errorf("body modified")
	}
	if !strings.Contains(raw, "Reassigned staff to agy.") {
		t.Errorf("progress log missing reassign line: %s", raw)
	}
}

func TestTaskControlReassignRoundTripsStaffWithYAMLSpecials(t *testing.T) {
	for _, staff := range []string{"Joey #2 backup", "a: b", "yes", "123", "- x"} {
		t.Run(staff, func(t *testing.T) {
			service, _ := newTaskControlService(t)
			taskID := "tasks-20260925-reassign-special"
			taskPath := stageTaskInFolder(t, service, taskID, "working", map[string]any{"staff": "cursor"})

			result, err := service.TaskControl(context.Background(), TaskControlRequest{
				TaskID: taskID,
				Action: "reassign",
				Staff:  staff,
			})
			if err != nil {
				t.Fatalf("reassign failed: %v", err)
			}
			if result.Staff != staff {
				t.Errorf("result staff = %q, want %q", result.Staff, staff)
			}
			doc, err := orchestrator.ReadDocument(taskPath)
			if err != nil {
				t.Fatalf("task unparseable after reassign: %v", err)
			}
			if got, _ := doc.FrontMatter["staff"].(string); got != staff {
				t.Errorf("persisted staff = %#v, want %q", doc.FrontMatter["staff"], staff)
			}
		})
	}
}

func TestTaskControlReassignRejectsControlCharactersInStaff(t *testing.T) {
	for _, staff := range []string{"x\nstatus: done", "x\r\nstatus: done", "x\ty", "x\x00y"} {
		t.Run(fmt.Sprintf("%q", staff), func(t *testing.T) {
			service, _ := newTaskControlService(t)
			taskID := "tasks-20260925-reassign-control"
			taskPath := stageTaskInFolder(t, service, taskID, "working", map[string]any{"staff": "cursor"})
			before, err := os.ReadFile(taskPath)
			if err != nil {
				t.Fatal(err)
			}

			_, err = service.TaskControl(context.Background(), TaskControlRequest{
				TaskID: taskID,
				Action: "reassign",
				Staff:  staff,
			})
			if err == nil {
				t.Fatal("expected reassign to reject control characters")
			}
			if code := taskControlCode(err); code != "bad_request" {
				t.Errorf("code = %q, want bad_request", code)
			}
			after, err := os.ReadFile(taskPath)
			if err != nil {
				t.Fatal(err)
			}
			if string(after) != string(before) {
				t.Errorf("task file changed by a rejected reassign:\n%s", after)
			}
			if _, err := orchestrator.ReadDocument(taskPath); err != nil {
				t.Errorf("task unparseable: %v", err)
			}
		})
	}
}

func TestTaskControlSnoozeAndWakeNowRejectNonWaitingTask(t *testing.T) {
	service, _ := newTaskControlService(t)
	taskID := "tasks-20260925-non-waiting"
	stageTaskInFolder(t, service, taskID, "working", nil)

	_, errSnooze := service.TaskControl(context.Background(), TaskControlRequest{
		TaskID: taskID,
		Action: "snooze",
		WakeAt: time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
	})
	if errSnooze == nil {
		t.Fatal("expected error on snooze for non-waiting task")
	}
	if code := taskControlCode(errSnooze); code != "not_steerable" {
		t.Errorf("code = %q, want not_steerable", code)
	}

	_, errWake := service.TaskControl(context.Background(), TaskControlRequest{
		TaskID: taskID,
		Action: "wake-now",
	})
	if errWake == nil {
		t.Fatal("expected error on wake-now for non-waiting task")
	}
	if code := taskControlCode(errWake); code != "not_steerable" {
		t.Errorf("code = %q, want not_steerable", code)
	}
}


// Rework finding 1 (2026-09-25 review): the mainline mixed-review flow — a
// task whose reviewer rejected it (rejected completion_summary persisted by
// Spynel in todo/), reworked, back in review/ — must web-Approve into a
// readable done/ document with a single, valid operator summary.
func TestTaskControlApproveReplacesPriorRejectedReviewSummary(t *testing.T) {
	service, _ := newTaskControlService(t)
	taskID := "tasks-20260925-approve-prior-rejected"
	reviewPath := stageTaskInFolder(t, service, taskID, "review", map[string]any{
		"review_required": true,
		"review_attempt":  2,
		"completion_summary": map[string]any{
			"evidence":     "shipped but flawed, see findings",
			"outcome":      "rejected for rework: composer collisions and corrupt settle",
			"reviewed_at":  "2026-09-25T11:03:03Z",
			"rework_count": 2,
			"verdict":      "rejected",
		},
	})

	if _, err := service.TaskControl(context.Background(), TaskControlRequest{TaskID: taskID, Action: "approve"}); err != nil {
		t.Fatalf("approve failed: %v", err)
	}
	if _, err := os.Stat(reviewPath); !os.IsNotExist(err) {
		t.Fatalf("file still in review/: %v", err)
	}
	donePath := filepath.Join(service.Config.StatePath("tasks", "done"), taskID+".md")
	doc, err := orchestrator.ReadDocument(donePath)
	if err != nil {
		t.Fatalf("done document unreadable after approve on prior rejected summary: %v", err)
	}
	summary, ok := doc.FrontMatter["completion_summary"].(map[string]any)
	if !ok {
		t.Fatalf("completion_summary missing or not a map: %#v", doc.FrontMatter["completion_summary"])
	}
	if summary["verdict"] != "completed" {
		t.Errorf("verdict = %v, want completed (old block must be gone)", summary["verdict"])
	}
	// rework_count is code-managed: derived from review_attempt (2) minus one
	// for the done settle, not hand-written.
	if rework, ok := summary["rework_count"].(int); !ok || rework != 1 {
		t.Errorf("rework_count = %#v, want 1 (derived from review_attempt 2)", summary["rework_count"])
	}
}

// Rework finding 2 + minor (2026-09-25 review): request-changes records a
// rejected review summary (Joey's own verdict) with rework_count left to the
// framework, and the response never claims todo as settled.
func TestTaskControlRequestChangesRecordsRejectedSummaryWithCodeManagedRework(t *testing.T) {
	service, _ := newTaskControlService(t)
	taskID := "tasks-20260925-req-summary"
	stageTaskInFolder(t, service, taskID, "review", map[string]any{
		"review_required": true,
		"review_attempt":  2,
	})

	result, err := service.TaskControl(context.Background(), TaskControlRequest{
		TaskID: taskID,
		Action: "request-changes",
		Text:   "Re-shoot the phone screenshots",
	})
	if err != nil {
		t.Fatalf("request-changes failed: %v", err)
	}
	if result.Settled != "" {
		t.Errorf("result.Settled = %q, want empty (todo is not settled)", result.Settled)
	}
	todoPath := filepath.Join(service.Config.StatePath("tasks", "todo"), taskID+".md")
	doc, err := orchestrator.ReadDocument(todoPath)
	if err != nil {
		t.Fatalf("todo document unreadable: %v", err)
	}
	summary, ok := doc.FrontMatter["completion_summary"].(map[string]any)
	if !ok {
		t.Fatalf("rejected review summary missing: %#v", doc.FrontMatter["completion_summary"])
	}
	if summary["verdict"] != "rejected" {
		t.Errorf("verdict = %#v, want rejected", summary["verdict"])
	}
	if summary["reviewed_at"] == nil || summary["reviewed_at"] == "" {
		t.Errorf("reviewed_at missing from rejected summary: %#v", summary)
	}
	if rework, ok := summary["rework_count"].(int); !ok || rework != 2 {
		t.Errorf("summary rework_count = %#v, want 2 (derived from review_attempt, not hand-incremented)", summary["rework_count"])
	}
}

// Minor finding (2026-09-25 review): wake-now strips waiting_for as the
// recorded route contract says, and its response does not claim settled.
func TestTaskControlWakeNowStripsWaitingForAndWakeAt(t *testing.T) {
	service, _ := newTaskControlService(t)
	taskID := "tasks-20260925-wakenow-strip"
	stageTaskInFolder(t, service, taskID, "waiting", map[string]any{
		"wake_at":     "2026-09-26T00:00:00Z",
		"waiting_for": "'the nightly run to finish'",
	})

	result, err := service.TaskControl(context.Background(), TaskControlRequest{TaskID: taskID, Action: "wake-now"})
	if err != nil {
		t.Fatalf("wake-now failed: %v", err)
	}
	if result.Settled != "" {
		t.Errorf("result.Settled = %q, want empty", result.Settled)
	}
	todoPath := filepath.Join(service.Config.StatePath("tasks", "todo"), taskID+".md")
	raw, err := os.ReadFile(todoPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "waiting_for") || strings.Contains(string(raw), "wake_at") {
		t.Errorf("waiting fields survived wake-now:\n%s", raw)
	}
	doc, err := orchestrator.ReadDocument(todoPath)
	if err != nil {
		t.Fatalf("todo document unreadable: %v", err)
	}
	if doc.FrontMatter["status"] != "todo" {
		t.Errorf("status = %v, want todo", doc.FrontMatter["status"])
	}
}
