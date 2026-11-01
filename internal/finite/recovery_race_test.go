package finite

import (
	"context"
	"os"
	"path/filepath"
	"sync"
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

func TestFiniteRecoverySettlesCompletedBoundaryInOnePass(t *testing.T) {
	engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
		"1": {Result: ResultDone, Block: true},
	}})
	task := intakeTestTask(t, engine.store, "completed-boundary-race", false)
	if err := engine.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	run, err := engine.store.GetRun(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	waitEngineInvocationCount(t, engine, 1)
	writeEngineRunResult(t, run, *validTestResult(run, ResultDone))
	writeEngineJSONFile(t, filepath.Join(run.RunDir, "completion.json"), map[string]any{
		"schema_version": 1, "run_id": run.ID, "outcome": RunOK, "completed_at": time.Now().UTC().Format(time.RFC3339Nano),
	})

	live := make(chan struct{})
	var observed sync.Once
	engine.opts.hooks.inspectRun = func(ctx context.Context, runID int64, token string) (RunProcesses, error) {
		observation, err := InspectRun(ctx, runID, token)
		if observation.Liveness == LivenessLive {
			observed.Do(func() { close(live) })
		}
		return observation, err
	}
	releaseErr := make(chan error, 1)
	go func() {
		<-live
		time.Sleep(100 * time.Millisecond)
		releaseErr <- os.WriteFile(filepath.Join(run.RunDir, "release"), []byte("finish"), 0o600)
	}()
	if err := engine.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-releaseErr; err != nil {
		t.Fatal(err)
	}
	current, err := engine.store.GetTask(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	settled, err := engine.store.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != StateDone || settled.EndedAt == nil || settled.Outcome != RunOK {
		t.Fatalf("completed boundary was not settled in its recovery pass: task=%+v run=%+v", current, settled)
	}
	if got := countRows(t, engine.store, `SELECT COUNT(*) FROM task_events WHERE run_id=? AND kind='settled'`, run.ID); got != 1 {
		t.Fatalf("settlement events = %d, want one", got)
	}
	if got := countRows(t, engine.store, `SELECT COUNT(*) FROM outbox`); got != 1 {
		t.Fatalf("terminal outbox rows = %d, want one", got)
	}
	if got := engineInvocationCount(t, engine); got != 1 {
		t.Fatalf("provider invocations = %d, want one", got)
	}

	writeEngineRunResult(t, run, *validTestResult(run, ResultFailed))
	beforeEvents := countRows(t, engine.store, `SELECT COUNT(*) FROM task_events`)
	if err := engine.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	current, _ = engine.store.GetTask(context.Background(), task.ID)
	if current.State != StateDone || countRows(t, engine.store, `SELECT COUNT(*) FROM task_events`) != beforeEvents {
		t.Fatalf("late result changed terminal state: %+v", current)
	}
	assertEngineInvariants(t, engine.store, task.ID)
}
