package finite

import (
	"context"
	"testing"
	"time"
)

func TestFiniteRecoveryRaceTrace(t *testing.T) {
	engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
		"1": {Result: ResultDone},
		"2": {Result: ResultRejected},
		"3": {Result: ResultDone},
		"4": {Result: ResultAccepted},
	}})
	task := intakeTestTask(t, engine.store, "recovery-race-trace", true)
	engine.opts.hooks.inspectRun = func(ctx context.Context, runID int64, token string) (RunProcesses, error) {
		observation, err := InspectRun(ctx, runID, token)
		if runID >= 3 || err != nil {
			t.Logf("inspect run=%d liveness=%s supervisor=%+v processes=%d err=%v", runID, observation.Liveness, observation.Supervisor, len(observation.Processes), err)
		}
		return observation, err
	}
	engine.opts.hooks.beforeSettlement = func(request SettlementRequest) {
		t.Logf("settle begin run=%d expected_version=%d outcome=%s result=%+v", request.RunID, request.ExpectedTaskVersion, request.ExecutionOutcome, request.Result)
	}
	engine.opts.hooks.afterSettlement = func(request SettlementRequest) {
		t.Logf("settle committed run=%d", request.RunID)
	}
	if err := engine.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if err := engine.ScanOnce(context.Background()); err != nil {
			t.Logf("scan error: %v", err)
		}
		current, err := engine.store.GetTask(context.Background(), task.ID)
		if err != nil {
			t.Fatal(err)
		}
		runs, err := engine.store.ListRuns(context.Background(), task.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.State == StateDone && len(runs) == 4 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	runs, _ := engine.store.ListRuns(context.Background(), task.ID)
	current, _ := engine.store.GetTask(context.Background(), task.ID)
	t.Fatalf("trace did not reach done: task=%+v runs=%+v", current, runs)
}
