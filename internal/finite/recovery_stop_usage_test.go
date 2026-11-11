package finite

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// A stopped run still spent provider tokens; recovery records the usage the
// run published before the stop fence settled it.
func TestFiniteStoppedRunKeepsRecoveredUsage(t *testing.T) {
	engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
		"1": {Result: ResultDone, Block: true},
	}})
	ctx := context.Background()
	task := intakeTestTask(t, engine.store, "stopped-usage", false)
	if err := engine.ScanOnce(ctx); err != nil {
		t.Fatal(err)
	}
	waitEngineInvocationCount(t, engine, 1)
	run, err := engine.store.GetRun(ctx, 1)
	if err != nil || run.EndedAt != nil {
		t.Fatalf("run is not open: %+v, %v", run, err)
	}
	output := int64(42)
	writeEngineJSONFile(t, filepath.Join(run.RunDir, "usage.json"), engineUsageRecord{
		RunID: run.ID, OutputTokens: &output, Source: "stop-test", CollectedAt: time.Now().UTC(),
	})
	current, err := engine.store.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.RequestStop(ctx, task.ID, current.Version); err != nil {
		t.Fatal(err)
	}
	scanEngineUntil(t, engine, task.ID, StateCancelled, 1)
	settled, err := engine.store.GetRun(ctx, run.ID)
	if err != nil || settled.Outcome != RunInterrupted {
		t.Fatalf("stopped run = %+v, %v; want interrupted", settled, err)
	}
	usage, err := engine.store.GetUsage(ctx, run.ID)
	if err != nil {
		t.Fatalf("stopped run lost its usage: %v", err)
	}
	if usage.OutputTokens == nil || *usage.OutputTokens != output || usage.Source != "stop-test" {
		t.Fatalf("stopped run usage = %+v", usage)
	}
	assertEngineInvariants(t, engine.store, task.ID)
}
