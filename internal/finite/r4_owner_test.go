package finite

import (
	"context"
	"os"
	"strings"
	"testing"
)

func r4StatementFailpoints() []SettlementFailpoint {
	return []SettlementFailpoint{
		SettlementAfterRunResultsInsert,
		SettlementAfterRunUpdate,
		SettlementAfterUsageInsert,
		SettlementAfterTaskUpdate,
		SettlementAfterSettlementEventInsert,
		SettlementAfterOutboxInsert,
		SettlementBeforeCommit,
		SettlementAfterCommit,
	}
}

func TestFiniteOwnerCrashRecovery_R4StatementHooks(t *testing.T) {
	for _, point := range r4StatementFailpoints() {
		t.Run("R4/"+string(point), func(t *testing.T) {
			ownerCase := prepareEngineOwnerCase(t, "crash-r4-hook-"+strings.ReplaceAll(string(point), "_", "-"), false, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
				"1": {Result: ResultDone},
			}}, string(point), "")
			runEngineOwner(t, ownerCase, true)
			engine := openCrashedEngine(t, ownerCase)
			committed := point == SettlementAfterCommit
			request := verifyR4CrashState(t, engine, ownerCase, committed)
			if err := engine.ScanOnce(context.Background()); err != nil {
				t.Fatalf("recover after %s: %v", point, err)
			}
			verifyR4SettledState(t, engine, ownerCase)

			before := r4StateCounts(t, engine, ownerCase.TaskID)
			settled, err := engine.store.SettleRun(context.Background(), request)
			if err != nil {
				t.Fatalf("identical replay after %s: %v", point, err)
			}
			if settled.State != StateDone {
				t.Fatalf("identical replay changed state after %s: %+v", point, settled)
			}
			after := r4StateCounts(t, engine, ownerCase.TaskID)
			if after != before {
				t.Fatalf("identical replay changed durable counts after %s: before=%+v after=%+v", point, before, after)
			}
			if got := ownerInvocationCount(t, ownerCase); got != 1 {
				t.Fatalf("provider invocations after %s replay = %d, want one", point, got)
			}
			assertEngineInvariants(t, engine.store, ownerCase.TaskID)
		})
	}
}

type r4Counts struct {
	TaskVersion int64
	Runs        int
	Results     int
	Usage       int
	Events      int
	Dispatched  int
	Settled     int
	Outbox      int
}

func r4StateCounts(t *testing.T, engine *Engine, taskID string) r4Counts {
	t.Helper()
	task, err := engine.store.GetTask(context.Background(), taskID)
	if err != nil {
		t.Fatal(err)
	}
	return r4Counts{
		TaskVersion: task.Version,
		Runs:        countRows(t, engine.store, `SELECT COUNT(*) FROM runs WHERE task_id=?`, taskID),
		Results:     countRows(t, engine.store, `SELECT COUNT(*) FROM run_results WHERE task_id=?`, taskID),
		Usage:       countRows(t, engine.store, `SELECT COUNT(*) FROM run_usage WHERE run_id IN (SELECT id FROM runs WHERE task_id=?)`, taskID),
		Events:      countRows(t, engine.store, `SELECT COUNT(*) FROM task_events WHERE task_id=?`, taskID),
		Dispatched:  countRows(t, engine.store, `SELECT COUNT(*) FROM task_events WHERE task_id=? AND kind='dispatched'`, taskID),
		Settled:     countRows(t, engine.store, `SELECT COUNT(*) FROM task_events WHERE task_id=? AND kind='settled'`, taskID),
		Outbox:      countRows(t, engine.store, `SELECT COUNT(*) FROM outbox WHERE event_id IN (SELECT event_id FROM task_events WHERE task_id=?)`, taskID),
	}
}

func verifyR4CrashState(t *testing.T, engine *Engine, ownerCase engineOwnerCase, committed bool) SettlementRequest {
	t.Helper()
	task, err := engine.store.GetTask(context.Background(), ownerCase.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	run, err := engine.store.GetRun(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	want := 0
	if committed {
		want = 1
		if task.State != StateDone || task.Version != 2 || run.EndedAt == nil || run.Outcome != RunOK {
			t.Fatalf("post-commit crash lost durable settlement: task=%+v run=%+v", task, run)
		}
	} else if task.State != StateWorking || task.Version != 1 || run.EndedAt != nil || run.Outcome != "" {
		t.Fatalf("pre-commit crash partially settled: task=%+v run=%+v", task, run)
	}
	counts := r4StateCounts(t, engine, ownerCase.TaskID)
	if counts.Runs != 1 || counts.Results != want || counts.Usage != want || counts.Dispatched != 1 || counts.Settled != want || counts.Outbox != want || counts.Events != 1+want {
		t.Fatalf("crash write counts (committed=%t) = %+v", committed, counts)
	}
	if err := engine.store.CheckInvariants(context.Background()); err != nil {
		t.Fatalf("SQLite invariants after crash: %v", err)
	}
	verifyR4OldProjection(t, task, run)
	if got := ownerInvocationCount(t, ownerCase); got != 1 {
		t.Fatalf("provider invocations at crash = %d, want one", got)
	}

	result, err := ReadResult(run)
	if err != nil {
		t.Fatalf("read result for identical replay: %v", err)
	}
	usage, err := readRecoveredUsage(run)
	if err != nil || usage == nil {
		t.Fatalf("fixture did not reach usage settlement: usage=%+v err=%v", usage, err)
	}
	execution, err := readRecoveredExecution(run)
	if err != nil || execution == nil {
		t.Fatalf("fixture lacks terminal execution record: execution=%+v err=%v", execution, err)
	}
	return SettlementRequest{
		RunID: run.ID, ExpectedTaskVersion: run.TaskVersion + 1, ExecutionOutcome: execution.Outcome,
		ExitCode: execution.ExitCode, Result: &result, Usage: usage, WallWakeAt: execution.WallWakeAt, Error: execution.Error,
	}
}

func verifyR4OldProjection(t *testing.T, task Task, run Run) {
	t.Helper()
	oldTask := task
	oldTask.State = StateWorking
	oldTask.Version = 1
	oldTask.UpdatedAt = run.CreatedAt
	oldTask.ResumeState = ""
	oldTask.WaitKind = ""
	oldTask.WakeAt = nil
	oldTask.WaitingFor = ""
	oldTask.JoeyAskJSON = ""
	oldRun := run
	oldRun.EndedAt = nil
	oldRun.ExitCode = nil
	oldRun.Outcome = ""
	oldRun.Error = ""
	projection, err := os.ReadFile(task.ProjectionPath)
	if err != nil {
		t.Fatalf("read pre-recovery projection: %v", err)
	}
	if expected := RenderProjection(oldTask, []Run{oldRun}); string(projection) != expected {
		t.Fatalf("crash left a torn or unexpected projection\n--- got ---\n%s\n--- want complete old ---\n%s", projection, expected)
	}
}

func verifyR4SettledState(t *testing.T, engine *Engine, ownerCase engineOwnerCase) {
	t.Helper()
	task, err := engine.store.GetTask(context.Background(), ownerCase.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	run, err := engine.store.GetRun(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if task.State != StateDone || task.Version != 2 || run.EndedAt == nil || run.Outcome != RunOK {
		t.Fatalf("recovery did not produce exactly one durable settlement: task=%+v run=%+v", task, run)
	}
	counts := r4StateCounts(t, engine, ownerCase.TaskID)
	if counts.Runs != 1 || counts.Results != 1 || counts.Usage != 1 || counts.Events != 2 || counts.Dispatched != 1 || counts.Settled != 1 || counts.Outbox != 1 {
		t.Fatalf("recovered write counts = %+v", counts)
	}
	if got := ownerInvocationCount(t, ownerCase); got != 1 {
		t.Fatalf("recovery spawned a second provider: %d invocations", got)
	}
	assertEngineInvariants(t, engine.store, ownerCase.TaskID)
}
