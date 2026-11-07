package finite

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// A tap that arrives with a note must reach every later run verbatim: the
// note is Joey's instruction and overrides the option label when they
// conflict. Live 2026-11-06: run 862 saw only "Post as drafted
// (post-as-drafted)", never the note that said to follow it instead, and
// acted on the superseded label.
func TestTaskHistoryCarriesAnswerNoteThatOverridesOption(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	task := intakeTestTask(t, s, "answer-note", true)

	ask := dispatchTestRun(t, s, task, PhaseImplementation)
	result := validTestResult(ask, ResultWait)
	result.Wait = &Wait{Kind: WaitJoey, Reason: "Pick a reply",
		Ask: json.RawMessage(`{"question":"GitHub issue #12 (LazyMarket buttons): how should I answer shpowell?","options":[{"id":"post-as-drafted","label":"Post as drafted"},{"id":"edit-first","label":"Edit first"},{"id":"leave-it","label":"Leave it"}]}`)}
	waiting, err := s.SettleRun(ctx, SettlementRequest{RunID: ask.ID, ExpectedTaskVersion: task.Version + 1, ExecutionOutcome: RunOK, Result: result})
	if err != nil {
		t.Fatal(err)
	}
	const note = "Follow this note, not the drafted reply: release the fix to the testing build first, then post a different reply. Don't post before the testing build is out."
	answered, err := s.AnswerJoey(ctx, task.ID, waiting.Version, "post-as-drafted", note)
	if err != nil {
		t.Fatal(err)
	}

	history, err := s.TaskHistory(ctx, answered.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Joey answered", "Post as drafted", "(post-as-drafted)",
		"Follow this note, not the drafted reply",
		"Don't post before the testing build is out",
	} {
		if !strings.Contains(history, want) {
			t.Errorf("history lacks %q:\n%s", want, history)
		}
	}
	if !strings.Contains(history, "verride") {
		t.Errorf("history does not say the note overrides the option:\n%s", history)
	}
}

// A tap without a note renders exactly as before.
func TestTaskHistoryAnswerWithoutNoteUnchanged(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	task := intakeTestTask(t, s, "answer-plain", true)

	ask := dispatchTestRun(t, s, task, PhaseImplementation)
	result := validTestResult(ask, ResultWait)
	result.Wait = &Wait{Kind: WaitJoey, Reason: "Pick one",
		Ask: json.RawMessage(`{"question":"Which approach?","options":[{"id":"safe","label":"The safe one"}]}`)}
	waiting, err := s.SettleRun(ctx, SettlementRequest{RunID: ask.ID, ExpectedTaskVersion: task.Version + 1, ExecutionOutcome: RunOK, Result: result})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AnswerJoey(ctx, task.ID, waiting.Version, "safe", ""); err != nil {
		t.Fatal(err)
	}
	history, err := s.TaskHistory(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := `- Joey answered "Which approach?": The safe one (safe)`
	if !strings.Contains(history, want) {
		t.Errorf("plain answer line = %q, want to contain %q", history, want)
	}
	if strings.Contains(history, "verride") {
		t.Errorf("plain answer must not carry the note override wording:\n%s", history)
	}
}
