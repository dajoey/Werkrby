package finite

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// A later run must see what earlier runs of the same task concluded, the
// reviewer's findings, and Joey's answer, or it repeats the question or
// reworks blind.
func TestTaskHistoryCarriesAnswersAndReviewFindings(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	task := intakeTestTask(t, s, "task-history", true)
	if got, err := s.TaskHistory(ctx, task.ID); err != nil || got != "" {
		t.Fatalf("fresh task history = %q, %v", got, err)
	}

	ask := dispatchTestRun(t, s, task, PhaseImplementation)
	result := validTestResult(ask, ResultWait)
	result.Progress = "Found two ways to do it; need a choice."
	result.Wait = &Wait{Kind: WaitJoey, Reason: "Pick one", Ask: json.RawMessage(`{"question":"Which approach?","options":[{"id":"fast","label":"The fast one"},{"id":"safe","label":"The safe one"}]}`)}
	waiting, err := s.SettleRun(ctx, SettlementRequest{RunID: ask.ID, ExpectedTaskVersion: task.Version + 1, ExecutionOutcome: RunOK, Result: result})
	if err != nil {
		t.Fatal(err)
	}
	answered, err := s.AnswerJoey(ctx, task.ID, waiting.Version, "safe")
	if err != nil {
		t.Fatal(err)
	}

	impl := dispatchTestRun(t, s, answered, PhaseImplementation)
	implResult := validTestResult(impl, ResultReview)
	implResult.Progress = "Built the safe one."
	toReview, err := s.SettleRun(ctx, SettlementRequest{RunID: impl.ID, ExpectedTaskVersion: answered.Version + 1, ExecutionOutcome: RunOK, Result: implResult})
	if err != nil {
		t.Fatal(err)
	}
	review := dispatchTestRun(t, s, toReview, PhaseReview)
	reviewResult := validTestResult(review, ResultRejected)
	reviewResult.Progress = "Two problems found."
	reviewResult.Findings = []string{"missing error check in parse()", "no test for empty input"}
	if _, err := s.SettleRun(ctx, SettlementRequest{RunID: review.ID, ExpectedTaskVersion: toReview.Version + 1, ExecutionOutcome: RunOK, Result: reviewResult}); err != nil {
		t.Fatal(err)
	}

	history, err := s.TaskHistory(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Found two ways to do it", "Joey answered", "Which approach?", "The safe one", "(safe)",
		"Built the safe one.", "rejected", "missing error check in parse()", "no test for empty input",
	} {
		if !strings.Contains(history, want) {
			t.Errorf("history lacks %q:\n%s", want, history)
		}
	}
	if strings.Index(history, "Which approach?") > strings.Index(history, "Built the safe one.") {
		t.Errorf("history is not in event order:\n%s", history)
	}
}
