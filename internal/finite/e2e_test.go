package finite

import (
	"context"
	"os"
	"testing"
)

func init() {
	if len(os.Args) <= 1 || os.Args[1] != "supervise" || os.Getenv(engineFixturePlanEnv) == "" {
		return
	}
	if gate := os.Getenv(engineFixtureDuplicateGateEnv); gate != "" {
		if err := waitForReleaseFile(context.Background(), gate); err != nil {
			os.Exit(2)
		}
	}
	if err := RunSupervisor(context.Background(), os.Args[1:], newEngineFixtureHarness); err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}

func TestFiniteFakeProviderWorkflow(t *testing.T) {
	engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
		"1": {Result: ResultDone},
		"2": {Result: ResultAccepted},
	}})
	task := intakeTestTask(t, engine.store, "fake-workflow-世界", true)
	if err := engine.ScanOnce(context.Background()); err != nil {
		t.Fatalf("initial owner scan: %v", err)
	}
	runs, err := engine.store.ListRuns(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].Phase != PhaseImplementation {
		t.Fatalf("initial scan did not commit exactly one implementation run: %+v", runs)
	}
	done := scanEngineUntil(t, engine, task.ID, StateDone, 2)
	if done.Version != 4 {
		t.Fatalf("workflow version = %d, want dispatch/settle for two phases", done.Version)
	}
	runs, err = engine.store.ListRuns(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if runs[0].Outcome != RunOK || runs[1].Outcome != RunOK || runs[0].Phase != PhaseImplementation || runs[1].Phase != PhaseReview {
		t.Fatalf("workflow runs = %+v", runs)
	}
	if runs[0].ID == runs[1].ID || runs[0].ProcessToken == "" || runs[1].ProcessToken == "" || runs[0].ProcessToken == runs[1].ProcessToken {
		t.Fatalf("run identities were not unique and persisted: %+v", runs)
	}
	for _, run := range runs {
		if run.SupervisorPID == nil || run.StartedAt == nil || run.EndedAt == nil {
			t.Fatalf("run %d lacks persisted start/end identity: %+v", run.ID, run)
		}
		if !fileExists(run.RunDir + string(os.PathSeparator) + "session.json") || !fileExists(run.RunDir+string(os.PathSeparator)+"usage.json") {
			t.Fatalf("run %d lacks session/usage artifacts in %s", run.ID, run.RunDir)
		}
		usage, err := engine.store.GetUsage(context.Background(), run.ID)
		if err != nil || usage.FreshInputTokens == nil || *usage.FreshInputTokens != run.ID || usage.OutputTokens == nil || *usage.OutputTokens != 7 {
			t.Fatalf("run %d normalized usage = %+v, %v", run.ID, usage, err)
		}
	}
	if got := engineInvocationCount(t, engine); got != 2 {
		t.Fatalf("real provider invocations = %d, want 2", got)
	}
	if got := countRows(t, engine.store, `SELECT COUNT(*) FROM task_events WHERE task_id=? AND kind='dispatched'`, task.ID); got != 2 {
		t.Fatalf("dispatch events = %d, want 2", got)
	}
	if got := countRows(t, engine.store, `SELECT COUNT(*) FROM task_events WHERE task_id=? AND kind='settled'`, task.ID); got != 2 {
		t.Fatalf("settlement events = %d, want 2", got)
	}
	if got := countRows(t, engine.store, `SELECT COUNT(*) FROM outbox WHERE event_id IN (SELECT event_id FROM task_events WHERE task_id=?)`, task.ID); got != 1 {
		t.Fatalf("terminal outbox rows = %d, want 1", got)
	}
	assertEngineInvariants(t, engine.store, task.ID)
}
