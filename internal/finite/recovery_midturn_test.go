package finite

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A supervisor killed after its provider turn started has spent an attempt:
// recovery charges it as a failed run so redispatch is bounded by
// MaxFailures instead of repeating the turn forever.
func TestFiniteMidTurnSupervisorKillIsChargedAndBounded(t *testing.T) {
	engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
		"1": {Result: ResultDone, Block: true}, "2": {Result: ResultDone, Block: true},
		"3": {Result: ResultDone, Block: true}, "4": {Result: ResultDone, Block: true},
	}})
	ctx := context.Background()
	task, err := engine.store.IntakeBrief(ctx, writeTestBrief(t, "midturn-kill", false))
	if err != nil {
		t.Fatal(err)
	}
	for kill := 1; kill <= task.MaxFailures; kill++ {
		if err := engine.ScanOnce(ctx); err != nil {
			t.Fatalf("kill %d: dispatch scan: %v", kill, err)
		}
		waitEngineInvocationCount(t, engine, kill)
		runs, err := engine.store.ListRuns(ctx, task.ID)
		if err != nil || len(runs) != kill {
			t.Fatalf("kill %d: runs = %+v, %v", kill, runs, err)
		}
		current := runs[kill-1]
		if current.SupervisorPID == nil || current.EndedAt != nil {
			t.Fatalf("kill %d: run is not a live supervised turn: %+v", kill, current)
		}
		if _, err := os.Stat(filepath.Join(current.RunDir, "supervisor-started.json")); err != nil {
			t.Fatalf("kill %d: provider turn did not record its start: %v", kill, err)
		}
		process, err := os.FindProcess(*current.SupervisorPID)
		if err != nil {
			t.Fatal(err)
		}
		if err := process.Kill(); err != nil {
			t.Fatalf("kill %d: kill supervisor: %v", kill, err)
		}
		settled := waitRunSettled(t, engine, current.ID)
		if settled.Outcome != RunFail || !strings.Contains(settled.Error, "provider turn") {
			t.Fatalf("kill %d: mid-turn kill settled as %q (%q), want a charged failure", kill, settled.Outcome, settled.Error)
		}
		payload := gatewaySettledPayload(t, engine.store, current.ID)
		if payload["failure_class"] != failureSupervisorDied || payload["charged"] != true {
			t.Fatalf("kill %d: settled payload = %v, want failure_class %q charged true", kill, payload, failureSupervisorDied)
		}
		accounting, err := engine.store.DerivedAccounting(ctx, task.ID, PhaseImplementation)
		if err != nil || accounting.Attempts != kill || accounting.Failures != kill {
			t.Fatalf("kill %d: accounting = %+v, %v; want %d charged failures", kill, accounting, err, kill)
		}
	}
	final, err := engine.store.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != StateFailed {
		t.Fatalf("task after %d mid-turn kills = %s, want failed", task.MaxFailures, final.State)
	}
	for i := 0; i < 3; i++ {
		_ = engine.ScanOnce(ctx)
	}
	if got := engineInvocationCount(t, engine); got != task.MaxFailures {
		t.Fatalf("provider invoked %d times, want exactly MaxFailures=%d", got, task.MaxFailures)
	}
	assertEngineInvariants(t, engine.store, task.ID)
}

// A run whose supervisor never started its provider turn spent nothing and
// stays an uncharged lost run.
func TestFiniteRunWithoutProviderStartStaysUnchargedLost(t *testing.T) {
	engine := newEngineFixture(t, engineFixturePlan{})
	ctx := context.Background()
	task := intakeTestTask(t, engine.store, "never-started", false)
	run, err := engine.store.Dispatch(ctx, DispatchRequest{
		TaskID: task.ID, ExpectedVersion: task.Version, Phase: PhaseImplementation,
		Seat: "fake-seat", SessionKey: "finite:never-started", LaunchReason: "initial",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	token := strings.Repeat("n", 43)
	if err := engine.prepareRun(ctx, run, token, Prompt{Text: "never started", BundleHash: "bundle", FullBundle: true}); err != nil {
		t.Fatal(err)
	}
	run.ProcessToken = token
	if err := engine.recoverRun(ctx, run); err != nil {
		t.Fatalf("recover never-started run: %v", err)
	}
	settled, err := engine.store.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if settled.Outcome != RunLost {
		t.Fatalf("never-started run settled as %q, want lost", settled.Outcome)
	}
	accounting, err := engine.store.DerivedAccounting(ctx, task.ID, PhaseImplementation)
	if err != nil || accounting.Attempts != 0 || accounting.Failures != 0 {
		t.Fatalf("never-started run was charged: %+v, %v", accounting, err)
	}
}

func waitRunSettled(t *testing.T, engine *Engine, runID int64) Run {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_ = engine.ScanOnce(context.Background())
		run, err := engine.store.GetRun(context.Background(), runID)
		if err == nil && run.EndedAt != nil {
			return run
		}
		time.Sleep(20 * time.Millisecond)
	}
	run, _ := engine.store.GetRun(context.Background(), runID)
	t.Fatalf("run %d did not settle: %+v", runID, run)
	return Run{}
}
