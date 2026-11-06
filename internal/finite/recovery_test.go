package finite

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestFiniteAmbiguousLiveness_L1(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "scan_permission_error", err: errors.New("fixture process scan permission denied")},
		{name: "scan_truncated", err: errors.New("fixture process scan truncated")},
		{name: "scan_timeout", err: context.DeadlineExceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{"1": {Result: ResultDone, Block: true}}})
			task := intakeTestTask(t, engine.store, "ambiguous-"+strings.ReplaceAll(test.name, "_", "-"), false)
			if err := engine.ScanOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			runs, _ := engine.store.ListRuns(context.Background(), task.ID)
			if len(runs) != 1 || runs[0].ProcessToken == "" {
				t.Fatalf("ambiguous-liveness setup did not launch one bound run: %+v", runs)
			}
			waitEngineInvocationCount(t, engine, 1)
			engine.opts.hooks.inspectRun = func(context.Context, int64, string) (RunProcesses, error) {
				return RunProcesses{Liveness: LivenessUnknown}, errors.Join(ErrProcessUnknown, test.err)
			}
			if err := engine.ScanOnce(context.Background()); err == nil {
				t.Fatal("ambiguous process observation was not surfaced")
			}
			current, _ := engine.store.GetTask(context.Background(), task.ID)
			open, _ := engine.store.GetRun(context.Background(), runs[0].ID)
			if current.State != StateWorking || open.EndedAt != nil || engineInvocationCount(t, engine) != 1 {
				t.Fatalf("ambiguous liveness changed ownership: task=%+v run=%+v invocations=%d", current, open, engineInvocationCount(t, engine))
			}
			engine.opts.hooks.inspectRun = nil
			observation, err := InspectRun(context.Background(), open.ID, open.ProcessToken)
			if err != nil || observation.Liveness != LivenessLive {
				t.Fatalf("ambiguous scan signaled or lost the real provider: %+v, %v", observation, err)
			}
			assertEngineInvariants(t, engine.store, task.ID)
		})
	}

	t.Run("duplicate_wrappers_real_processes", func(t *testing.T) {
		engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{"1": {Result: ResultDone, Block: true}}})
		task := intakeTestTask(t, engine.store, "ambiguous-duplicates", false)
		if err := engine.ScanOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		runs, _ := engine.store.ListRuns(context.Background(), task.ID)
		if len(runs) != 1 || runs[0].ProcessToken == "" {
			t.Fatalf("duplicate-wrapper setup did not launch one bound run: %+v", runs)
		}
		run := runs[0]
		waitEngineInvocationCount(t, engine, 1)
		gate := run.RunDir + "/duplicate-release"
		command := exec.Command(engine.opts.Executable, "supervise", "--run-id", strconv.FormatInt(run.ID, 10), "--run-dir", run.RunDir, "--process-token", run.ProcessToken)
		command.Env = mergeEnvironment(os.Environ(), engine.opts.Harness.Env, runEnvironment(run.ID, run.ProcessToken, run.RunDir), []string{engineFixtureDuplicateGateEnv + "=" + gate})
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = command.Process.Kill()
			_, _ = waitForCommand(command, 5*time.Second)
		})
		deadline := time.Now().Add(10 * time.Second)
		var duplicateErr error
		for time.Now().Before(deadline) {
			_, duplicateErr = InspectRun(context.Background(), run.ID, run.ProcessToken)
			if errors.Is(duplicateErr, ErrDuplicateSupervisor) {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if !errors.Is(duplicateErr, ErrDuplicateSupervisor) {
			t.Fatalf("real duplicate wrappers were not discovered: %v", duplicateErr)
		}
		if err := engine.ScanOnce(context.Background()); err == nil {
			t.Fatal("duplicate wrappers were not surfaced as ambiguous")
		}
		current, _ := engine.store.GetTask(context.Background(), task.ID)
		open, _ := engine.store.GetRun(context.Background(), run.ID)
		if current.State != StateWorking || open.EndedAt != nil || engineInvocationCount(t, engine) != 1 {
			t.Fatalf("duplicate wrappers caused settlement or redispatch: task=%+v run=%+v", current, open)
		}
		assertEngineInvariants(t, engine.store, task.ID)
	})
}

func TestFiniteRecoveryResultPublicationRace(t *testing.T) {
	engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{"1": {Result: ResultDone, Block: true}}})
	task := intakeTestTask(t, engine.store, "result-publication-race", false)
	if err := engine.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	run, err := engine.store.GetRun(context.Background(), 1)
	if err != nil || run.ProcessToken == "" {
		t.Fatalf("result-race setup did not launch: %+v, %v", run, err)
	}
	waitEngineInvocationCount(t, engine, 1)
	engine.opts.hooks.inspectRun = func(ctx context.Context, runID int64, token string) (RunProcesses, error) {
		if err := os.WriteFile(run.RunDir+"/release", []byte("publish"), 0o600); err != nil {
			return RunProcesses{Liveness: LivenessUnknown}, err
		}
		for {
			observation, err := InspectRun(ctx, runID, token)
			if err != nil || observation.Liveness == LivenessAbsent {
				return observation, err
			}
			select {
			case <-ctx.Done():
				return RunProcesses{Liveness: LivenessUnknown}, ctx.Err()
			case <-time.After(time.Millisecond):
			}
		}
	}
	if err := engine.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	current, _ := engine.store.GetTask(context.Background(), task.ID)
	settled, _ := engine.store.GetRun(context.Background(), run.ID)
	if current.State != StateDone || settled.Outcome != RunOK || settled.Error != "" {
		t.Fatalf("result published during absence scan was lost: task=%+v run=%+v", current, settled)
	}
	assertEngineInvariants(t, engine.store, task.ID)
}

// A provider turn whose owning Hermes gateway died mid-turn (reboot, crash,
// OOM, kill) is a platform fault: recovery settles it interrupted, the task
// returns to its queue, and no failure attempt is spent.
func TestFiniteGatewayDeathMidTurnSettlesInterruptedUncharged(t *testing.T) {
	engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
		"1": {Block: true}, "2": {Block: true},
	}})
	ctx := context.Background()
	task := intakeTestTask(t, engine.store, "gateway-death-interrupted", false)
	if err := engine.ScanOnce(ctx); err != nil {
		t.Fatal(err)
	}
	waitEngineInvocationCount(t, engine, 1)
	runs, err := engine.store.ListRuns(ctx, task.ID)
	if err != nil || len(runs) != 1 || runs[0].ProcessToken == "" {
		t.Fatalf("gateway-death setup did not launch one bound run: %+v, %v", runs, err)
	}
	run := runs[0]
	if _, err := os.Stat(filepath.Join(run.RunDir, "supervisor-started.json")); err != nil {
		t.Fatalf("provider turn did not record its start: %v", err)
	}
	engine.opts.hooks.inspectRun = func(context.Context, int64, string) (RunProcesses, error) {
		return RunProcesses{Liveness: LivenessAbsent, ProviderOrphaned: true}, nil
	}
	if err := engine.ScanOnce(ctx); err != nil {
		t.Fatalf("recovery scan: %v", err)
	}
	engine.opts.hooks.inspectRun = nil
	settled, err := engine.store.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if settled.Outcome != RunInterrupted || settled.EndedAt == nil {
		t.Fatalf("gateway-death run = %+v, want a settled interrupted run", settled)
	}
	if settled.Error != "Hermes gateway ended during the provider turn; not charged" {
		t.Fatalf("gateway-death run error = %q", settled.Error)
	}
	// The task went straight back to its queue: a replacement provider turn
	// was dispatched without the gateway's death spending anything.
	waitEngineInvocationCount(t, engine, 2)
	allRuns, err := engine.store.ListRuns(ctx, task.ID)
	if err != nil || len(allRuns) != 2 {
		t.Fatalf("runs after gateway death = %+v, %v; want the interrupted run and one replacement", allRuns, err)
	}
	accounting, err := engine.store.DerivedAccounting(ctx, task.ID, PhaseImplementation)
	if err != nil || accounting.Attempts != 0 || accounting.Failures != 0 || accounting.Runs != 2 {
		t.Fatalf("gateway death was charged: %+v, %v; want two uncharged runs", accounting, err)
	}
	// Let the replacement turn finish, so the task proves it kept working.
	if err := os.WriteFile(filepath.Join(allRuns[1].RunDir, "release"), []byte("release"), 0o600); err != nil {
		t.Fatal(err)
	}
	replacement := waitRunSettled(t, engine, allRuns[1].ID)
	current, err := engine.store.GetTask(ctx, task.ID)
	if err != nil || current.State != StateDone || replacement.Outcome != RunOK {
		t.Fatalf("task after the replacement turn = %+v run=%+v %v; want done", current, replacement, err)
	}
	assertEngineInvariants(t, engine.store, task.ID)
}

// The same dead boundary without the orphan signal is still the charged
// providerTurnStarted failure it was before the orphaned outcome existed.
func TestFiniteMidTurnDeathWithoutOrphanSignalStaysChargedFail(t *testing.T) {
	engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
		"1": {Block: true}, "2": {Block: true},
	}})
	ctx := context.Background()
	task := intakeTestTask(t, engine.store, "gateway-death-charged", false)
	if err := engine.ScanOnce(ctx); err != nil {
		t.Fatal(err)
	}
	waitEngineInvocationCount(t, engine, 1)
	runs, err := engine.store.ListRuns(ctx, task.ID)
	if err != nil || len(runs) != 1 || runs[0].ProcessToken == "" {
		t.Fatalf("charged-death setup did not launch one bound run: %+v, %v", runs, err)
	}
	run := runs[0]
	if _, err := os.Stat(filepath.Join(run.RunDir, "supervisor-started.json")); err != nil {
		t.Fatalf("provider turn did not record its start: %v", err)
	}
	engine.opts.hooks.inspectRun = func(context.Context, int64, string) (RunProcesses, error) {
		return RunProcesses{Liveness: LivenessAbsent, ProviderOrphaned: false}, nil
	}
	if err := engine.ScanOnce(ctx); err != nil {
		t.Fatalf("recovery scan: %v", err)
	}
	engine.opts.hooks.inspectRun = nil
	settled, err := engine.store.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if settled.Outcome != RunFail || settled.EndedAt == nil || !strings.Contains(settled.Error, "provider turn") {
		t.Fatalf("boundary death without orphan signal = %+v, want the charged providerTurnStarted failure", settled)
	}
	// The charged failure still allowed exactly the retry MaxFailures bounds:
	// a replacement turn is dispatched with one failure on the books.
	waitEngineInvocationCount(t, engine, 2)
	allRuns, err := engine.store.ListRuns(ctx, task.ID)
	if err != nil || len(allRuns) != 2 {
		t.Fatalf("runs after the charged death = %+v, %v; want the failed run and one replacement", allRuns, err)
	}
	accounting, err := engine.store.DerivedAccounting(ctx, task.ID, PhaseImplementation)
	if err != nil || accounting.Attempts != 1 || accounting.Failures != 1 || accounting.Runs != 2 {
		t.Fatalf("boundary death without orphan signal = %+v, %v; want one charged failure over two runs", accounting, err)
	}
	if err := os.WriteFile(filepath.Join(allRuns[1].RunDir, "release"), []byte("release"), 0o600); err != nil {
		t.Fatal(err)
	}
	replacement := waitRunSettled(t, engine, allRuns[1].ID)
	current, err := engine.store.GetTask(ctx, task.ID)
	if err != nil || current.State != StateDone || replacement.Outcome != RunOK {
		t.Fatalf("task after the replacement turn = %+v run=%+v %v; want done", current, replacement, err)
	}
	assertEngineInvariants(t, engine.store, task.ID)
}
