package finite

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A live adopted run whose result and execution record are already published
// is waited on for reliable absence, but only for a bounded time: the scan
// holds scanMu, so an unbounded wait would block stop and answer requests
// for as long as the boundary lives.
func TestFiniteAdoptedCompletionWaitIsBounded(t *testing.T) {
	engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
		"1": {Result: ResultDone, Block: true},
	}})
	ctx := context.Background()
	task := intakeTestTask(t, engine.store, "bounded-adopted-wait", false)
	if err := engine.ScanOnce(ctx); err != nil {
		t.Fatal(err)
	}
	waitEngineInvocationCount(t, engine, 1)
	run, err := engine.store.GetRun(ctx, 1)
	if err != nil || run.SupervisorPID == nil || run.EndedAt != nil {
		t.Fatalf("run is not a live supervised turn: %+v, %v", run, err)
	}
	release := func() {
		if err := os.WriteFile(filepath.Join(run.RunDir, "release"), []byte("complete"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// The turn's artifacts are complete while its boundary is still alive.
	writeEngineRunResult(t, run, *validTestResult(run, ResultDone))
	writeEngineJSONFile(t, filepath.Join(run.RunDir, "execution.json"), engineExecutionRecord{SchemaVersion: 1, RunID: run.ID, Outcome: RunOK})

	bound := adoptedSettleWait(engine.opts.StopGrace)
	scanDone := make(chan error, 1)
	started := time.Now()
	go func() { scanDone <- engine.ScanOnce(ctx) }()
	time.Sleep(50 * time.Millisecond)
	answerDone := make(chan error, 1)
	go func() {
		_, err := engine.AnswerJoey(ctx, task.ID, task.Version, "none")
		answerDone <- err
	}()
	limit := bound + 5*time.Second
	select {
	case <-scanDone:
	case <-time.After(limit):
		release()
		t.Fatalf("scan blocked more than %s on a live adopted boundary", limit)
	}
	if elapsed := time.Since(started); elapsed < bound {
		t.Fatalf("scan returned after %s, before the %s settle bound", elapsed, bound)
	}
	select {
	case <-answerDone:
	case <-time.After(5 * time.Second):
		release()
		t.Fatal("answer request stayed blocked behind the scan")
	}
	open, err := engine.store.GetRun(ctx, run.ID)
	if err != nil || open.EndedAt != nil {
		t.Fatalf("live run was settled or lost after the bounded wait: %+v, %v", open, err)
	}
	if runs, _ := engine.store.ListRuns(ctx, task.ID); len(runs) != 1 {
		t.Fatalf("bounded wait started a replacement run: %+v", runs)
	}
	release()
	scanEngineUntil(t, engine, task.ID, StateDone, 1)
	if got := engineInvocationCount(t, engine); got != 1 {
		t.Fatalf("provider invoked %d times, want the original turn only", got)
	}
	assertEngineInvariants(t, engine.store, task.ID)
}

func TestFiniteAdoptedSettleWaitBound(t *testing.T) {
	if got := adoptedSettleWait(0); got != 30*time.Second {
		t.Fatalf("unset stop grace bound = %s, want 30s", got)
	}
	if got := adoptedSettleWait(5 * time.Second); got != 5*time.Second+adoptedSettleMargin {
		t.Fatalf("stop grace bound = %s, want grace plus margin", got)
	}
}
