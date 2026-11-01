package orchestrator

import (
	"context"
	"time"
)

// FinalizeOperatorTaskTransition completes an operator-initiated transition
// of a task document that has NO live claim lease — the web review-composer
// verdicts (approve, request-changes, wake-now) act on unclaimed review/ and
// waiting/ files. The caller has already written the document surgically and
// moved it to its destination folder; this method runs the same terminal
// machinery a claimed settle reaches through reconcileTransitions:
//
//   - the code-managed completion-summary stamp (rework_count derived from
//     review_attempt, never hand-written) for done and todo verdict settles;
//   - the task.completed extension hooks for terminal outcomes, keyed by a
//     stable event id so extensions deduplicate visible effects across
//     retries even without a lease to persist receipts into;
//   - the notification decision turn for outcomes that require one;
//   - a queue rescan, so a task returned to todo/ dispatches promptly.
//
// Failures inside the machinery are logged, not returned: the document move
// already happened and stands; a hook or notification failure is a separate
// recoverable defect, exactly as completeTransition treats it.
func (m *Manager) FinalizeOperatorTaskTransition(ctx context.Context, targetPath, taskID, threadID, status string) {
	// A finite-executed task gets none of the legacy settle machinery: no summary stamp, hooks,
	// or notification turn.
	if externallyExecutedAt(targetPath) {
		return
	}
	switch status {
	case "done", "todo":
		m.finalizeTaskCompletionSummary(targetPath, status)
	case "failed", "cancelled":
		// No completion-summary stamp for these web paths today; hooks and
		// notification below still apply if a future action settles them.
	}
	if status == "done" || status == "failed" || status == "cancelled" {
		m.completeUnclaimedTaskTransition(ctx, targetPath, taskID, threadID, status)
	}
	m.requestScan()
}

// completeUnclaimedTaskTransition runs the terminal half of a transition —
// tracked task.completed hooks plus the notification decision turn — for a
// document that carries no claim lease.
func (m *Manager) completeUnclaimedTaskTransition(ctx context.Context, path, taskID, threadID, status string) {
	document, err := ReadDocument(path)
	if err != nil {
		m.log("operator transition read: " + err.Error())
		return
	}
	if m.runtimeSnapshot().Extensions.Enabled {
		// No claim lease exists to persist hook receipts into; the stable
		// event id (operator:<task>:<status>) lets extensions deduplicate
		// visible effects across retries — the same contract a crashed
		// receipt write relies on in completeTransition.
		receipts := map[string]bool{}
		if _, hookErr := m.Hooks.RunTracked(ctx, "task.completed", map[string]any{
			"route": "tasks", "file": path, "thread_id": threadID,
			"outcome": status, "event_id": "operator:" + taskID + ":" + status,
		}, receipts, func(string) error { return nil }); hookErr != nil {
			m.log("task.completed hook: " + hookErr.Error())
		}
	}
	if requiresTaskNotificationDecision(document, status, time.Now().UTC()) {
		source := Lease{
			ID:    "operator-" + taskID,
			Route: "tasks", Phase: phaseTaskReview,
			ClaimAttempt: max(numberValue(document.FrontMatter["review_attempt"]), numberValue(document.FrontMatter["attempt"])),
			File:         path,
		}
		// The web control route invokes this on the HTTP request context,
		// which is canceled the moment the control response returns; the
		// async notification turn must outlive the request exactly as the
		// claimed-settle path's long-lived loop context does.
		m.startTaskNotificationAgent(context.WithoutCancel(ctx), source, status, path)
	}
}
