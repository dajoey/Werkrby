package finite

import (
	"context"
	"testing"
	"time"
)

// TestFailureClassTableIsComplete pins the one failure-class table: every
// known class name has exactly one row, every RunOutcome constant is the
// stored outcome of at least one row, and each row's Charged matches its
// stored outcome's own Charged(), so runs.outcome alone keeps derived
// accounting and the charged_attempts view correct.
func TestFailureClassTableIsComplete(t *testing.T) {
	names := []string{
		failureResult, failureProviderFail, failureTimeout, failureSupervisorDied,
		failureHermesNoSpend, failureQuotaWall, failureLost, failureInterrupted, failureCancelled,
	}
	seen := map[string]bool{}
	for _, name := range names {
		if seen[name] {
			t.Fatalf("class %q is listed twice", name)
		}
		seen[name] = true
		row, ok := failureClassTable[name]
		if !ok {
			t.Fatalf("class %q has no table row", name)
		}
		if row.Charged != row.Outcome.Charged() {
			t.Fatalf("class %q stores outcome %q with charged %v, want %v", name, row.Outcome, row.Charged, row.Outcome.Charged())
		}
	}
	if len(failureClassTable) != len(names) {
		t.Fatalf("table has %d rows for %d named classes", len(failureClassTable), len(names))
	}
	for _, outcome := range []RunOutcome{RunOK, RunFail, RunTimeout, RunLost, RunInterrupted, RunWall} {
		found := false
		for _, row := range failureClassTable {
			found = found || row.Outcome == outcome
		}
		if !found {
			t.Fatalf("no class row stores outcome %q", outcome)
		}
	}
}

func TestNoTokenSpend(t *testing.T) {
	if noTokenSpend(nil) {
		t.Fatal("a missing usage record is not decided by noTokenSpend")
	}
	if !noTokenSpend(&Usage{}) {
		t.Fatal("a usage record with every counter null proves no spend")
	}
	zero := int64(0)
	allZero := &Usage{FreshInputTokens: &zero, CacheReadTokens: &zero, CacheWriteTokens: &zero,
		OutputTokens: &zero, ReasoningTokens: &zero, APICalls: &zero, ToolCalls: &zero}
	if !noTokenSpend(allZero) {
		t.Fatal("a usage record with every counter zero proves no spend")
	}
	for name, set := range map[string]func(*Usage){
		"fresh_input":    func(u *Usage) { one := int64(1); u.FreshInputTokens = &one },
		"cache_read":     func(u *Usage) { one := int64(1); u.CacheReadTokens = &one },
		"cache_write":    func(u *Usage) { one := int64(1); u.CacheWriteTokens = &one },
		"output":         func(u *Usage) { one := int64(1); u.OutputTokens = &one },
		"reasoning":      func(u *Usage) { one := int64(1); u.ReasoningTokens = &one },
		"api_calls":      func(u *Usage) { one := int64(1); u.APICalls = &one },
		"tool_calls":     func(u *Usage) { one := int64(1); u.ToolCalls = &one },
	} {
		usage := &Usage{}
		set(usage)
		if noTokenSpend(usage) {
			t.Fatalf("a usage record with a nonzero %s counter is not a no-spend record", name)
		}
	}
}

// TestSettleRunPrecedence pins the settlement precedence: a cancel ends the
// task as cancelled whatever class recovery asked for, a valid result is the
// run's own verdict, and only a classless request derives the class from the
// execution outcome. An unknown class is a settlement error.
func TestSettleRunPrecedence(t *testing.T) {
	ctx := context.Background()

	t.Run("cancel_beats_failure_class", func(t *testing.T) {
		s := newTestStore(t)
		task := intakeTestTask(t, s, "precedence-cancel", false)
		run := dispatchTestRun(t, s, task, PhaseImplementation)
		working, err := s.GetTask(ctx, task.ID)
		if err != nil {
			t.Fatal(err)
		}
		stopped, err := s.RequestStop(ctx, task.ID, working.Version)
		if err != nil {
			t.Fatal(err)
		}
		settled, err := s.SettleRun(ctx, SettlementRequest{
			RunID: run.ID, ExpectedTaskVersion: stopped.Version,
			ExecutionOutcome: RunFail, FailureClass: failureHermesNoSpend,
			Error: "runner turn failed: gateway gone",
		})
		if err != nil {
			t.Fatal(err)
		}
		if settled.State != StateCancelled {
			t.Fatalf("cancelled settlement recorded state %s, want cancelled", settled.State)
		}
		runRow, err := s.GetRun(ctx, run.ID)
		if err != nil || runRow.Outcome != RunInterrupted {
			t.Fatalf("cancelled run outcome = %q, %v; want interrupted", runRow.Outcome, err)
		}
		payload := gatewaySettledPayload(t, s, run.ID)
		if payload["failure_class"] != failureCancelled || payload["charged"] != false {
			t.Fatalf("cancelled payload = %v, want failure_class %q charged false", payload, failureCancelled)
		}
	})

	t.Run("valid_result_beats_execution_outcome", func(t *testing.T) {
		s := newTestStore(t)
		task := intakeTestTask(t, s, "precedence-result", false)
		run := dispatchTestRun(t, s, task, PhaseImplementation)
		working, err := s.GetTask(ctx, task.ID)
		if err != nil {
			t.Fatal(err)
		}
		settled, err := s.SettleRun(ctx, SettlementRequest{
			RunID: run.ID, ExpectedTaskVersion: working.Version,
			ExecutionOutcome: RunFail, Result: validTestResult(run, ResultDone),
		})
		if err != nil {
			t.Fatal(err)
		}
		if settled.State != StateDone {
			t.Fatalf("result settlement recorded state %s, want done", settled.State)
		}
		runRow, err := s.GetRun(ctx, run.ID)
		if err != nil || runRow.Outcome != RunOK {
			t.Fatalf("result run outcome = %q, %v; want stored ok", runRow.Outcome, err)
		}
		payload := gatewaySettledPayload(t, s, run.ID)
		if payload["failure_class"] != failureResult || payload["charged"] != true {
			t.Fatalf("result payload = %v, want failure_class %q charged true", payload, failureResult)
		}
	})

	t.Run("unknown_class_rejected", func(t *testing.T) {
		s := newTestStore(t)
		task := intakeTestTask(t, s, "precedence-unknown", false)
		run := dispatchTestRun(t, s, task, PhaseImplementation)
		working, err := s.GetTask(ctx, task.ID)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.SettleRun(ctx, SettlementRequest{
			RunID: run.ID, ExpectedTaskVersion: working.Version,
			ExecutionOutcome: RunFail, FailureClass: "not-a-class",
		}); err == nil {
			t.Fatal("an unknown failure class settled without error")
		}
		runRow, err := s.GetRun(ctx, run.ID)
		if err != nil || runRow.EndedAt != nil {
			t.Fatalf("rejected settlement left the run = %+v, %v; want still open", runRow, err)
		}
	})

	t.Run("requested_class_used_as_is", func(t *testing.T) {
		s := newTestStore(t)
		task := intakeTestTask(t, s, "precedence-requested", false)
		run := dispatchTestRun(t, s, task, PhaseImplementation)
		working, err := s.GetTask(ctx, task.ID)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.SettleRun(ctx, SettlementRequest{
			RunID: run.ID, ExpectedTaskVersion: working.Version,
			ExecutionOutcome: RunFail, FailureClass: failureHermesNoSpend,
			Error: "runner turn failed: gateway gone",
		}); err != nil {
			t.Fatal(err)
		}
		runRow, err := s.GetRun(ctx, run.ID)
		if err != nil || runRow.Outcome != RunInterrupted {
			t.Fatalf("requested-class run outcome = %q, %v; want interrupted", runRow.Outcome, err)
		}
		payload := gatewaySettledPayload(t, s, run.ID)
		if payload["failure_class"] != failureHermesNoSpend || payload["charged"] != false {
			t.Fatalf("requested-class payload = %v, want failure_class %q charged false", payload, failureHermesNoSpend)
		}
	})
}

// TestFingerprintUnchangedWithoutClass pins the settlement fingerprint of
// classless requests to the values computed on 1bdb369, where the fingerprint
// struct had no failure_class field: an empty class must hash to exactly the
// old value so replays of older settlements still match.
func TestFingerprintUnchangedWithoutClass(t *testing.T) {
	result := &Result{
		SchemaVersion: 1, RunID: 7, TaskID: "golden-task", Phase: PhaseImplementation,
		Outcome: ResultDone, Progress: "Completed the golden objective.",
		Summary: &Summary{Outcome: "Complete", Evidence: "golden evidence", Uncertainty: "none"},
	}
	okReq := SettlementRequest{RunID: 7, ExpectedTaskVersion: 3, ExecutionOutcome: RunOK, Result: result}
	failReq := SettlementRequest{RunID: 7, ExpectedTaskVersion: 3, ExecutionOutcome: RunFail, Error: "golden failure"}
	wake := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	wallReq := SettlementRequest{RunID: 7, ExpectedTaskVersion: 3, ExecutionOutcome: RunWall, WallWakeAt: &wake, Error: "golden wall"}
	golden := map[string]struct {
		request SettlementRequest
		want    string
	}{
		"ok":   {okReq, "5663298de664179ead625f2ba72f08e6e70ec022fbb4b481bbe5f8d19b6f46db"},
		"fail": {failReq, "16ead8cf15399f93520c2f5a1acabc72c38b05488d7c7e7de651df3c90fc7f4a"},
		"wall": {wallReq, "689d5c700a9545a1db183494bd3b7320afd534cfe4f178cc806d2a1f1acbd23c"},
	}
	for name, want := range golden {
		got, err := settlementFingerprint(want.request)
		if err != nil {
			t.Fatalf("%s fingerprint: %v", name, err)
		}
		if got != want.want {
			t.Fatalf("%s classless fingerprint = %s, want the 1bdb369 golden %s", name, got, want.want)
		}
	}
}
