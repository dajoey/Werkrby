package finite

import (
	"context"
	"testing"
)

// The engine never runs more provider turns at once than MaxOpenRuns; queued
// tasks wait in todo until a run ends.
func TestFiniteMaxOpenRunsCapsConcurrentDispatch(t *testing.T) {
	engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
		"1": {Result: ResultDone, Block: true}, "2": {Result: ResultDone, Block: true},
		"3": {Result: ResultDone, Block: true},
	}})
	engine.opts.MaxOpenRuns = 2
	ctx := context.Background()
	for _, id := range []string{"cap-a", "cap-b", "cap-c"} {
		if _, err := engine.store.IntakeBrief(ctx, writeTestBrief(t, id, false)); err != nil {
			t.Fatal(err)
		}
	}
	if err := engine.ScanOnce(ctx); err != nil {
		t.Fatal(err)
	}
	waitEngineInvocationCount(t, engine, 2)
	open, err := engine.store.CountOpenRuns(ctx)
	if err != nil || open != 2 {
		t.Fatalf("open runs = %d, %v; want 2", open, err)
	}
	if err := engine.ScanOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if open, _ := engine.store.CountOpenRuns(ctx); open != 2 {
		t.Fatalf("a second scan opened a run past the cap: %d open", open)
	}
	third, err := engine.store.GetTask(ctx, "cap-c")
	if err != nil || third.State != StateTodo {
		t.Fatalf("third task = %+v, %v; want it still queued", third, err)
	}
}
