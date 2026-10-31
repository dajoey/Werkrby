package finite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/harness"
)

const engineOwnerConfigEnv = "WERKRBY_FINITE_ENGINE_OWNER_CONFIG"

type engineOwnerCase struct {
	Base           string
	Root           string
	Worker         string
	PlanPath       string
	BriefPath      string
	TaskID         string
	Failpoint      string
	Action         string
	MarkerPath     string
}

type engineOwnerMarker struct {
	Failpoint string          `json:"failpoint"`
	OwnerPID  int             `json:"owner_pid"`
	RunID     int64           `json:"run_id"`
	Identity  ProcessIdentity `json:"identity,omitempty"`
}

func TestFiniteEngineOwnerHelperProcess(t *testing.T) {
	path := os.Getenv(engineOwnerConfigEnv)
	if path == "" {
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var ownerCase engineOwnerCase
	if err := json.Unmarshal(data, &ownerCase); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	crash := func(failpoint string, runID int64, identity ProcessIdentity) {
		marker, _ := json.Marshal(engineOwnerMarker{Failpoint: failpoint, OwnerPID: os.Getpid(), RunID: runID, Identity: identity})
		_ = os.WriteFile(ownerCase.MarkerPath, marker, 0o600)
		os.Exit(86)
	}
	var engine *Engine
	engine, err = NewEngine(Config{Root: ownerCase.Root, WorkerDir: ownerCase.Worker}, EngineOptions{
		Harness: harness.HarnessConfig{Name: "finite-engine-fixture", Cwd: ownerCase.Worker, Env: []string{engineFixturePlanEnv + "=" + ownerCase.PlanPath}},
		Seats: func(context.Context) ([]Seat, error) { return []Seat{{Name: "fake-seat", Available: true}}, nil },
		Executable: executable,
		StopGrace: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	engine.opts.hooks.afterDispatchCommit = func(run Run) {
		if ownerCase.Failpoint == "after_dispatch_commit" {
			crash(ownerCase.Failpoint, run.ID, ProcessIdentity{})
		}
	}
	engine.opts.hooks.beforeStart = func(run Run) {
		if ownerCase.Action == "stop_before_spawn" {
			task, getErr := engine.store.GetTask(context.Background(), run.TaskID)
			if getErr != nil {
				t.Fatal(getErr)
			}
			if _, stopErr := engine.store.RequestStop(context.Background(), task.ID, task.Version); stopErr != nil {
				t.Fatal(stopErr)
			}
		}
		if ownerCase.Failpoint == "before_supervisor_start" {
			crash(ownerCase.Failpoint, run.ID, ProcessIdentity{})
		}
	}
	engine.opts.hooks.afterSpawn = func(run Run, identity ProcessIdentity) {
		if ownerCase.Failpoint == "after_spawn_before_pid_persist" {
			crash(ownerCase.Failpoint, run.ID, identity)
		}
	}
	engine.opts.hooks.afterPIDPersist = func(run Run, identity ProcessIdentity) {
		if ownerCase.Failpoint == "after_pid_persist" {
			crash(ownerCase.Failpoint, run.ID, identity)
		}
	}
	engine.opts.hooks.beforeSettlement = func(request SettlementRequest) {
		if ownerCase.Action == "stop_before_settlement" {
			run, getErr := engine.store.GetRun(context.Background(), request.RunID)
			if getErr != nil {
				t.Fatal(getErr)
			}
			task, getErr := engine.store.GetTask(context.Background(), run.TaskID)
			if getErr != nil {
				t.Fatal(getErr)
			}
			if _, stopErr := engine.store.RequestStop(context.Background(), task.ID, task.Version); stopErr != nil {
				t.Fatal(stopErr)
			}
			ownerCase.Action = ""
		}
		if ownerCase.Failpoint == "before_settlement" {
			crash(ownerCase.Failpoint, request.RunID, ProcessIdentity{})
		}
	}
	engine.opts.hooks.afterSettlement = func(request SettlementRequest) {
		if ownerCase.Failpoint == "after_settlement_commit" {
			crash(ownerCase.Failpoint, request.RunID, ProcessIdentity{})
		}
	}
	if _, err := engine.store.IntakeBrief(context.Background(), ownerCase.BriefPath); err != nil {
		t.Fatal(err)
	}
	for range 200 {
		_ = engine.ScanOnce(context.Background())
		time.Sleep(10 * time.Millisecond)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
}

func prepareEngineOwnerCase(t *testing.T, taskID string, review bool, plan engineFixturePlan, failpoint, action string) engineOwnerCase {
	t.Helper()
	base := t.TempDir()
	ownerCase := engineOwnerCase{
		Base: base, Root: filepath.Join(base, "owner state 世界"), Worker: filepath.Join(base, "owner worker λ"),
		PlanPath: filepath.Join(base, "owner plan.json"), BriefPath: filepath.Join(base, taskID+".md"),
		TaskID: taskID, Failpoint: failpoint, Action: action, MarkerPath: filepath.Join(base, "owner crash marker.json"),
	}
	if err := os.MkdirAll(ownerCase.Worker, 0o700); err != nil {
		t.Fatal(err)
	}
	plan.InvocationsPath = filepath.Join(base, "owner invocations.jsonl")
	writeEngineJSONFile(t, ownerCase.PlanPath, plan)
	brief := fmt.Sprintf("---\nid: %s\ntitle: Crash task %s\nrisk: low\nreview_required: %t\n---\n\nExercise real owner recovery.\n", taskID, taskID, review)
	if err := os.WriteFile(ownerCase.BriefPath, []byte(brief), 0o600); err != nil {
		t.Fatal(err)
	}
	return ownerCase
}

func runEngineOwner(t *testing.T, ownerCase engineOwnerCase, wantCrash bool) {
	t.Helper()
	configPath := filepath.Join(ownerCase.Base, "owner config.json")
	writeEngineJSONFile(t, configPath, ownerCase)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(ownerCase.Base, "owner process.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run=^TestFiniteEngineOwnerHelperProcess$")
	command.Env = append(os.Environ(), engineOwnerConfigEnv+"="+configPath)
	command.Stdout = logFile
	command.Stderr = logFile
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		t.Fatal(err)
	}
	ownerPID := command.Process.Pid
	err = command.Wait()
	_ = logFile.Close()
	if wantCrash {
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 86 {
			output, _ := os.ReadFile(logPath)
			t.Fatalf("owner %d did not die at %s: err=%v output=%s", ownerPID, ownerCase.Failpoint, err, output)
		}
		marker, readErr := os.ReadFile(ownerCase.MarkerPath)
		if readErr != nil {
			t.Fatalf("owner %d left no failpoint identity: %v", ownerPID, readErr)
		}
		t.Logf("owner_pid=%d failpoint=%s marker=%s", ownerPID, ownerCase.Failpoint, marker)
		return
	}
	if err != nil {
		output, _ := os.ReadFile(logPath)
		t.Fatalf("owner %d failed: %v output=%s", ownerPID, err, output)
	}
}

func openCrashedEngine(t *testing.T, ownerCase engineOwnerCase) *Engine {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(Config{Root: ownerCase.Root, WorkerDir: ownerCase.Worker}, EngineOptions{
		Harness: harness.HarnessConfig{Name: "finite-engine-fixture", Cwd: ownerCase.Worker, Env: []string{engineFixturePlanEnv + "=" + ownerCase.PlanPath}},
		Seats: func(context.Context) ([]Seat, error) { return []Seat{{Name: "fake-seat", Available: true}}, nil },
		Executable: executable,
		StopGrace: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("reopen crashed owner: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rows, queryErr := engine.store.db.Query(`SELECT id,process_token FROM runs WHERE ended_at IS NULL AND process_token IS NOT NULL`)
		if queryErr == nil {
			for rows.Next() {
				var id int64
				var token string
				if rows.Scan(&id, &token) == nil {
					_, _ = StopRun(ctx, id, token, 0)
				}
			}
			_ = rows.Close()
		}
		_ = engine.Close()
	})
	return engine
}

func ownerInvocationCount(t *testing.T, ownerCase engineOwnerCase) int {
	t.Helper()
	data, err := os.ReadFile(ownerCase.PlanPath)
	if err != nil {
		t.Fatal(err)
	}
	var plan engineFixturePlan
	if err := json.Unmarshal(data, &plan); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(plan.InvocationsPath)
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	return len(stringsSplitNonempty(string(contents)))
}

func stringsSplitNonempty(value string) []string {
	var lines []string
	for _, line := range strings.Split(strings.TrimSpace(value), "\n") {
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func TestFiniteOwnerCrashRecovery_R1_R4(t *testing.T) {
	t.Run("R1/after_open_run_commit_before_wrapper_start", func(t *testing.T) {
		ownerCase := prepareEngineOwnerCase(t, "crash-r1", false, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
			"1": {Result: ResultDone, Block: true}, "2": {Result: ResultDone, Block: true},
		}}, "after_dispatch_commit", "")
		runEngineOwner(t, ownerCase, true)
		engine := openCrashedEngine(t, ownerCase)
		if err := engine.ScanOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		runs, _ := engine.store.ListRuns(context.Background(), ownerCase.TaskID)
		if len(runs) != 2 || runs[0].Outcome != RunLost || runs[1].ID <= runs[0].ID || runs[1].EndedAt != nil {
			t.Fatalf("R1 did not requeue onto a new run without charging: %+v", runs)
		}
		accounting, _ := engine.store.DerivedAccounting(context.Background(), ownerCase.TaskID, PhaseImplementation)
		if accounting.Attempts != 0 || accounting.Failures != 0 {
			t.Fatalf("R1 charged a pre-spawn run: %+v", accounting)
		}
		waitEngineInvocationCount(t, engine, 1)
		assertEngineInvariants(t, engine.store, ownerCase.TaskID)
	})

	for _, failpoint := range []string{"after_spawn_before_pid_persist", "after_pid_persist"} {
		t.Run("R2/"+failpoint, func(t *testing.T) {
			ownerCase := prepareEngineOwnerCase(t, "crash-r2-"+strings.ReplaceAll(failpoint, "_", "-"), false, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
				"1": {Result: ResultDone, Block: true},
			}}, failpoint, "")
			runEngineOwner(t, ownerCase, true)
			engine := openCrashedEngine(t, ownerCase)
			before, err := engine.store.GetRun(context.Background(), 1)
			if err != nil {
				t.Fatal(err)
			}
			if failpoint == "after_spawn_before_pid_persist" && before.SupervisorPID != nil {
				t.Fatalf("R2 precondition PID persisted before crash: %+v", before)
			}
			if failpoint == "after_pid_persist" && (before.SupervisorPID == nil || before.ProcessToken == "") {
				t.Fatalf("R2 companion case lacks PID/token persistence: %+v", before)
			}
			if err := engine.ScanOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			adopted, _ := engine.store.GetRun(context.Background(), 1)
			runs, _ := engine.store.ListRuns(context.Background(), ownerCase.TaskID)
			if len(runs) != 1 || adopted.EndedAt != nil || adopted.SupervisorPID == nil {
				t.Fatalf("R2 replaced or failed to adopt original run: %+v", runs)
			}
			waitEngineInvocationCount(t, engine, 1)
			if err := os.WriteFile(filepath.Join(adopted.RunDir, "release"), []byte("complete"), 0o600); err != nil {
				t.Fatal(err)
			}
			scanEngineUntil(t, engine, ownerCase.TaskID, StateDone, 1)
			if got := ownerInvocationCount(t, ownerCase); got != 1 {
				t.Fatalf("R2 provider executions = %d, want original only", got)
			}
			if got := countRows(t, engine.store, `SELECT COUNT(*) FROM task_events WHERE run_id=1 AND kind='settled'`); got != 1 {
				t.Fatalf("R2 settlement events = %d", got)
			}
			assertEngineInvariants(t, engine.store, ownerCase.TaskID)
		})
	}

	t.Run("R3/complete_result_before_owner_read", func(t *testing.T) {
		ownerCase := prepareEngineOwnerCase(t, "crash-r3", false, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
			"1": {Result: ResultDone},
		}}, "after_pid_persist", "")
		runEngineOwner(t, ownerCase, true)
		engine := openCrashedEngine(t, ownerCase)
		run, err := engine.store.GetRun(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(10 * time.Second)
		for {
			observation, inspectErr := InspectRun(context.Background(), run.ID, run.ProcessToken)
			if inspectErr == nil && observation.Liveness == LivenessAbsent {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("R3 supervisor did not reach reliable absence: %+v %v", observation, inspectErr)
			}
			time.Sleep(10 * time.Millisecond)
		}
		if err := engine.ScanOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		task, _ := engine.store.GetTask(context.Background(), ownerCase.TaskID)
		runs, _ := engine.store.ListRuns(context.Background(), ownerCase.TaskID)
		if task.State != StateDone || len(runs) != 1 || runs[0].Outcome != RunOK {
			t.Fatalf("R3 did not settle original result: task=%+v runs=%+v", task, runs)
		}
		if got := ownerInvocationCount(t, ownerCase); got != 1 {
			t.Fatalf("R3 spawned a replacement provider: %d", got)
		}
		assertEngineInvariants(t, engine.store, ownerCase.TaskID)
	})

	for _, failpoint := range []string{"before_settlement", "after_settlement_commit"} {
		t.Run("R4/"+failpoint, func(t *testing.T) {
			ownerCase := prepareEngineOwnerCase(t, "crash-r4-"+strings.ReplaceAll(failpoint, "_", "-"), false, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
				"1": {Result: ResultDone},
			}}, failpoint, "")
			runEngineOwner(t, ownerCase, true)
			engine := openCrashedEngine(t, ownerCase)
			scanEngineUntil(t, engine, ownerCase.TaskID, StateDone, 1)
			if got := countRows(t, engine.store, `SELECT COUNT(*) FROM task_events WHERE run_id=1 AND kind='settled'`); got != 1 {
				t.Fatalf("R4 settlement events = %d, want exactly one", got)
			}
			if got := countRows(t, engine.store, `SELECT COUNT(*) FROM outbox`); got != 1 {
				t.Fatalf("R4 terminal outbox rows = %d, want exactly one", got)
			}
			if got := ownerInvocationCount(t, ownerCase); got != 1 {
				t.Fatalf("R4 provider executions = %d, want one", got)
			}
			assertEngineInvariants(t, engine.store, ownerCase.TaskID)
		})
	}
}

func TestFiniteStopFencePrecedence_P2(t *testing.T) {
	t.Run("before_spawn", func(t *testing.T) {
		ownerCase := prepareEngineOwnerCase(t, "stop-before-spawn", false, engineFixturePlan{Runs: map[string]engineFixtureBehavior{"1": {Result: ResultDone}}}, "", "stop_before_spawn")
		runEngineOwner(t, ownerCase, false)
		engine := openCrashedEngine(t, ownerCase)
		task, _ := engine.store.GetTask(context.Background(), ownerCase.TaskID)
		run, _ := engine.store.GetRun(context.Background(), 1)
		if task.State != StateCancelled || run.Outcome != RunInterrupted || ownerInvocationCount(t, ownerCase) != 0 {
			t.Fatalf("stop before spawn = task %+v run %+v invocations %d", task, run, ownerInvocationCount(t, ownerCase))
		}
		assertEngineInvariants(t, engine.store, ownerCase.TaskID)
	})

	t.Run("live_result_and_restart", func(t *testing.T) {
		ownerCase := prepareEngineOwnerCase(t, "stop-live-restart", false, engineFixturePlan{Runs: map[string]engineFixtureBehavior{"1": {Result: ResultDone, Block: true}}}, "after_pid_persist", "")
		runEngineOwner(t, ownerCase, true)
		engine := openCrashedEngine(t, ownerCase)
		run, _ := engine.store.GetRun(context.Background(), 1)
		waitEngineInvocationCount(t, engine, 1)
		writeEngineRunResult(t, run, *validTestResult(run, ResultDone))
		task, _ := engine.store.GetTask(context.Background(), ownerCase.TaskID)
		if _, err := engine.RequestStop(context.Background(), task.ID, task.Version); err != nil {
			t.Fatal(err)
		}
		if err := engine.ScanOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		task, _ = engine.store.GetTask(context.Background(), ownerCase.TaskID)
		run, _ = engine.store.GetRun(context.Background(), 1)
		if task.State != StateCancelled || run.Outcome != RunInterrupted || run.Error != "cancel_requested" {
			t.Fatalf("stop did not beat completed result after restart: task=%+v run=%+v", task, run)
		}
		accounting, _ := engine.store.DerivedAccounting(context.Background(), task.ID, PhaseImplementation)
		if accounting.Attempts != 0 {
			t.Fatalf("stopped run was charged: %+v", accounting)
		}
		assertEngineInvariants(t, engine.store, task.ID)
	})

	t.Run("after_result_validation_before_settlement", func(t *testing.T) {
		ownerCase := prepareEngineOwnerCase(t, "stop-before-settlement", false, engineFixturePlan{Runs: map[string]engineFixtureBehavior{"1": {Result: ResultDone}}}, "", "stop_before_settlement")
		runEngineOwner(t, ownerCase, false)
		engine := openCrashedEngine(t, ownerCase)
		task, _ := engine.store.GetTask(context.Background(), ownerCase.TaskID)
		run, _ := engine.store.GetRun(context.Background(), 1)
		if task.State != StateCancelled || run.Outcome != RunInterrupted || countRows(t, engine.store, `SELECT COUNT(*) FROM run_results`) != 0 {
			t.Fatalf("pre-commit stop did not fence result: task=%+v run=%+v", task, run)
		}
		assertEngineInvariants(t, engine.store, ownerCase.TaskID)
	})

	t.Run("unresolved_termination_stays_stop_pending", func(t *testing.T) {
		engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{"1": {Result: ResultDone, Block: true}}})
		task := intakeTestTask(t, engine.store, "stop-unresolved", false)
		if err := engine.ScanOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		if runs, _ := engine.store.ListRuns(context.Background(), task.ID); len(runs) != 1 {
			t.Fatalf("stop setup did not dispatch: %+v", runs)
		}
		waitEngineInvocationCount(t, engine, 1)
		current, _ := engine.store.GetTask(context.Background(), task.ID)
		if _, err := engine.RequestStop(context.Background(), current.ID, current.Version); err != nil {
			t.Fatal(err)
		}
		engine.opts.hooks.stopRun = func(context.Context, int64, string, time.Duration) (Liveness, error) {
			return LivenessUnknown, ErrProcessUnknown
		}
		if err := engine.ScanOnce(context.Background()); err == nil {
			t.Fatal("unresolved stop was not surfaced")
		}
		pending, _ := engine.store.GetTask(context.Background(), task.ID)
		open, _ := engine.store.GetRun(context.Background(), 1)
		if pending.State != StateWorking || !pending.CancelRequested || open.EndedAt != nil {
			t.Fatalf("unresolved stop was prematurely completed: task=%+v run=%+v", pending, open)
		}
		engine.opts.hooks.stopRun = nil
		if err := engine.ScanOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		stopped, _ := engine.store.GetTask(context.Background(), task.ID)
		if stopped.State != StateCancelled {
			t.Fatalf("confirmed stop did not settle: %+v", stopped)
		}
		assertEngineInvariants(t, engine.store, task.ID)
	})
}
