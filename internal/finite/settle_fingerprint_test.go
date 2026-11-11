package finite

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

// The settlement fingerprint is taken over the canonical Joey ask, and
// settlement never rewrites the caller's request, so replaying a
// commit-unknown settlement, or one whose ask differs only in JSON key order
// or spacing, is an idempotent replay rather than a conflict.
func TestFiniteJoeyAskSettlementReplayIsIdempotent(t *testing.T) {
	s := newTestStore(t)
	task := intakeTestTask(t, s, "replay-joey", false)
	run := dispatchTestRun(t, s, task, PhaseImplementation)
	newRequest := func(ask string) SettlementRequest {
		result := validTestResult(run, ResultWait)
		result.Wait = &Wait{Kind: WaitJoey, Reason: "need answer", Ask: json.RawMessage(ask)}
		return SettlementRequest{RunID: run.ID, ExpectedTaskVersion: task.Version + 1, ExecutionOutcome: RunOK, Result: result}
	}
	const unordered = `{"options":[{"label":"Yes","id":"y"}],"question":"Proceed?"}`
	request := newRequest(unordered)
	sentinel := errors.New("post-commit result is ambiguous")
	restore := registerStoreTransactionFault(storeTransactionAfterCommit, func() error { return sentinel })
	_, err := s.SettleRun(context.Background(), request)
	restore()
	if !errors.Is(err, sentinel) {
		t.Fatalf("ambiguous settlement = %v", err)
	}
	if string(request.Result.Wait.Ask) != unordered {
		t.Fatalf("settlement rewrote the caller's ask to %s", request.Result.Wait.Ask)
	}
	if _, err := s.SettleRun(context.Background(), request); err != nil {
		t.Fatalf("identical replay after commit-unknown = %v", err)
	}
	spaced := newRequest(`{ "question": "Proceed?", "options": [ { "id": "y", "label": "Yes" } ] }`)
	if _, err := s.SettleRun(context.Background(), spaced); err != nil {
		t.Fatalf("replay with an equivalent ask = %v", err)
	}
	different := newRequest(`{"question":"Proceed now?","options":[{"id":"y","label":"Yes"}]}`)
	if _, err := s.SettleRun(context.Background(), different); !errors.Is(err, ErrSettlementConflict) {
		t.Fatalf("replay with a different ask = %v, want a settlement conflict", err)
	}
	current, err := s.GetTask(context.Background(), task.ID)
	if err != nil || current.State != StateWaiting || current.JoeyAskJSON != `{"question":"Proceed?","options":[{"id":"y","label":"Yes"}]}` {
		t.Fatalf("settled task = %+v, %v", current, err)
	}
}
