package finite

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/harness"
)

const wakeReviewCrashConfigEnv = "WERKRBY_FINITE_WAKE_REVIEW_CRASH_CONFIG"

type wakeReviewCrashCase struct {
	Owner engineOwnerCase
	Point string
	RunID int64
}

type wakeReviewCrashMarker struct {
	OwnerPID int    `json:"owner_pid"`
	RunID    int64  `json:"run_id"`
	Point    string `json:"point"`
}

func TestFiniteWakeReviewOwnerHelperProcess(t *testing.T) {
	path := os.Getenv(wakeReviewCrashConfigEnv)
	if path == "" {
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var crashCase wakeReviewCrashCase
	if err := json.Unmarshal(data, &crashCase); err != nil {
		t.Fatal(err)
	}
	ownerCase := crashCase.Owner
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(Config{Root: ownerCase.Root, WorkerDir: ownerCase.Worker}, EngineOptions{
		Harness:    harness.HarnessConfig{Name: "finite-engine-fixture", Cwd: ownerCase.Worker, Env: []string{engineFixturePlanEnv + "=" + ownerCase.PlanPath}},
		Seats:      func(context.Context) ([]Seat, error) { return []Seat{{Name: "fake-seat", Available: true}}, nil },
		Executable: executable, StopGrace: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	crash := func(runID int64) {
		marker, _ := json.Marshal(wakeReviewCrashMarker{OwnerPID: os.Getpid(), RunID: runID, Point: crashCase.Point})
		_ = os.WriteFile(filepath.Join(ownerCase.Base, "wake-review-crash.json"), marker, 0o600)
		os.Exit(86)
	}
	engine.opts.hooks.beforeWake = func(Task) {
		if crashCase.Point == "before_wake" {
			crash(0)
		}
	}
	engine.opts.hooks.afterWake = func(Task) {
		if crashCase.Point == "after_wake_commit" {
			crash(0)
		}
	}
	engine.opts.hooks.beforeSettlement = func(request SettlementRequest) {
		if crashCase.Point == "before_settlement" && request.RunID == crashCase.RunID {
			crash(request.RunID)
		}
	}
	engine.opts.hooks.afterSettlement = func(request SettlementRequest) {
		if crashCase.Point == "after_settlement_commit" && request.RunID == crashCase.RunID {
			crash(request.RunID)
		}
		if crashCase.Point == "during_wake_commit" && request.RunID == 1 {
			registerStoreTransactionFault(storeTransactionBeforeCommit, func() error {
				crash(0)
				return nil
			})
		}
	}
	if _, err := engine.store.IntakeBrief(context.Background(), ownerCase.BriefPath); err != nil {
		t.Fatal(err)
	}
	for range 500 {
		_ = engine.ScanOnce(context.Background())
		time.Sleep(10 * time.Millisecond)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
}

func runWakeReviewCrashOwner(t *testing.T, crashCase wakeReviewCrashCase) {
	t.Helper()
	configPath := filepath.Join(crashCase.Owner.Base, "wake-review-config.json")
	writeEngineJSONFile(t, configPath, crashCase)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(crashCase.Owner.Base, "wake-review-owner.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run=^TestFiniteWakeReviewOwnerHelperProcess$")
	command.Env = append(os.Environ(), wakeReviewCrashConfigEnv+"="+configPath)
	command.Stdout = logFile
	command.Stderr = logFile
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		t.Fatal(err)
	}
	ownerPID := command.Process.Pid
	err = command.Wait()
	_ = logFile.Close()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 86 {
		output, _ := os.ReadFile(logPath)
		t.Fatalf("wake/review owner %d did not crash at %s: err=%v output=%s", ownerPID, crashCase.Point, err, output)
	}
	markerData, err := os.ReadFile(filepath.Join(crashCase.Owner.Base, "wake-review-crash.json"))
	if err != nil {
		t.Fatal(err)
	}
	var marker wakeReviewCrashMarker
	if err := json.Unmarshal(markerData, &marker); err != nil || marker.OwnerPID != ownerPID || marker.RunID != crashCase.RunID || marker.Point != crashCase.Point {
		t.Fatalf("invalid wake/review crash marker: %s, err=%v", markerData, err)
	}
	t.Logf("owner_pid=%d failpoint=%s run_id=%d", ownerPID, crashCase.Point, crashCase.RunID)
}

func TestFiniteWakeReviewCrashes_P3_P4(t *testing.T) {
	for _, point := range []string{"before_wake", "during_wake_commit", "after_wake_commit"} {
		t.Run("P3/"+point, func(t *testing.T) {
			wakeAt := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
			ownerCase := prepareEngineOwnerCase(t, "p3-"+strings.ReplaceAll(point, "_", "-"), false, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
				"1": {Result: ResultWait, Wait: &Wait{Kind: WaitTime, WakeAt: wakeAt, Reason: "process wake crash"}},
				"2": {Result: ResultDone},
			}}, "", "")
			crashCase := wakeReviewCrashCase{Owner: ownerCase, Point: point}
			runWakeReviewCrashOwner(t, crashCase)
			engine := openCrashedEngine(t, ownerCase)
			task, err := engine.store.GetTask(context.Background(), ownerCase.TaskID)
			if err != nil {
				t.Fatal(err)
			}
			wantWoke := 0
			if point == "after_wake_commit" {
				wantWoke = 1
				if task.State != StateTodo {
					t.Fatalf("post-commit wake crash state = %s, want todo", task.State)
				}
			} else if task.State != StateWaiting {
				t.Fatalf("pre/transaction wake crash state = %s, want waiting", task.State)
			}
			if got := countRows(t, engine.store, `SELECT COUNT(*) FROM task_events WHERE kind='woke'`); got != wantWoke {
				t.Fatalf("wake events at crash = %d, want %d", got, wantWoke)
			}
			if got := countRows(t, engine.store, `SELECT COUNT(*) FROM runs`); got != 1 {
				t.Fatalf("wake crash created a replacement run before recovery: %d", got)
			}
			scanEngineUntil(t, engine, ownerCase.TaskID, StateDone, 2)
			runs, _ := engine.store.ListRuns(context.Background(), ownerCase.TaskID)
			if len(runs) != 2 || runs[0].Phase != PhaseImplementation || runs[1].Phase != PhaseImplementation {
				t.Fatalf("wake did not resume original implementation phase: %+v", runs)
			}
			if got := ownerInvocationCount(t, ownerCase); got != 2 {
				t.Fatalf("wake recovery provider executions = %d, want 2", got)
			}
			if got := countRows(t, engine.store, `SELECT COUNT(*) FROM task_events WHERE kind='woke'`); got != 1 {
				t.Fatalf("recovered wake events = %d, want exactly 1", got)
			}
			for _, run := range runs {
				if got := countRows(t, engine.store, `SELECT COUNT(*) FROM task_events WHERE run_id=? AND kind='settled'`, run.ID); got != 1 {
					t.Fatalf("run %d settlement events = %d", run.ID, got)
				}
			}
			beforeEvents := countRows(t, engine.store, `SELECT COUNT(*) FROM task_events`)
			writeEngineRunResult(t, runs[0], *validTestResult(runs[0], ResultFailed))
			if err := engine.ScanOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			current, _ := engine.store.GetTask(context.Background(), ownerCase.TaskID)
			if current.State != StateDone || countRows(t, engine.store, `SELECT COUNT(*) FROM task_events`) != beforeEvents {
				t.Fatalf("late pre-wake result changed terminal state: %+v", current)
			}
			if got := countRows(t, engine.store, `SELECT COUNT(*) FROM outbox`); got != 1 {
				t.Fatalf("wake recovery outbox rows = %d, want 1", got)
			}
			assertEngineInvariants(t, engine.store, ownerCase.TaskID)
		})
	}

	for _, testCase := range []struct {
		name          string
		targetRun     int
		plan          engineFixturePlan
		beforeState   State
		afterState    State
		finalRuns     int
		reviewCycles  int
		reviewAttempt int
	}{
		{
			name: "implementation_to_review", targetRun: 1, beforeState: StateWorking, afterState: StateReview, finalRuns: 2, reviewAttempt: 1,
			plan: engineFixturePlan{Runs: map[string]engineFixtureBehavior{"1": {Result: ResultDone}, "2": {Result: ResultAccepted}}},
		},
		{
			name: "review_accepted", targetRun: 2, beforeState: StateReviewing, afterState: StateDone, finalRuns: 2, reviewAttempt: 1,
			plan: engineFixturePlan{Runs: map[string]engineFixtureBehavior{"1": {Result: ResultDone}, "2": {Result: ResultAccepted}}},
		},
		{
			name: "review_rejected", targetRun: 2, beforeState: StateReviewing, afterState: StateTodo, finalRuns: 4, reviewCycles: 1, reviewAttempt: 2,
			plan: engineFixturePlan{Runs: map[string]engineFixtureBehavior{"1": {Result: ResultDone}, "2": {Result: ResultRejected}, "3": {Result: ResultDone}, "4": {Result: ResultAccepted}}},
		},
	} {
		for _, point := range []string{"before_settlement", "after_settlement_commit"} {
			t.Run("P4/"+testCase.name+"/"+point, func(t *testing.T) {
				ownerCase := prepareEngineOwnerCase(t, "p4-"+strings.ReplaceAll(testCase.name+"-"+point, "_", "-"), true, testCase.plan, "", "")
				crashCase := wakeReviewCrashCase{Owner: ownerCase, Point: point, RunID: int64(testCase.targetRun)}
				runWakeReviewCrashOwner(t, crashCase)
				engine := openCrashedEngine(t, ownerCase)
				task, err := engine.store.GetTask(context.Background(), ownerCase.TaskID)
				if err != nil {
					t.Fatal(err)
				}
				wantState := testCase.beforeState
				if point == "after_settlement_commit" {
					wantState = testCase.afterState
				}
				if task.State != wantState {
					t.Fatalf("crash state = %s, want %s", task.State, wantState)
				}
				if got := countRows(t, engine.store, `SELECT COUNT(*) FROM runs`); got != testCase.targetRun {
					t.Fatalf("crash run count = %d, want %d", got, testCase.targetRun)
				}
				scanEngineUntil(t, engine, ownerCase.TaskID, StateDone, testCase.finalRuns)
				runs, _ := engine.store.ListRuns(context.Background(), ownerCase.TaskID)
				if len(runs) != testCase.finalRuns {
					t.Fatalf("recovered run count = %d, want %d", len(runs), testCase.finalRuns)
				}
				for index, run := range runs {
					wantPhase := PhaseImplementation
					if index%2 == 1 {
						wantPhase = PhaseReview
					}
					if run.Phase != wantPhase {
						t.Fatalf("run %d phase = %s, want %s", run.ID, run.Phase, wantPhase)
					}
					if got := countRows(t, engine.store, `SELECT COUNT(*) FROM task_events WHERE run_id=? AND kind='settled'`, run.ID); got != 1 {
						t.Fatalf("run %d settlement events = %d", run.ID, got)
					}
				}
				if got := ownerInvocationCount(t, ownerCase); got != testCase.finalRuns {
					t.Fatalf("review recovery provider executions = %d, want %d", got, testCase.finalRuns)
				}
				if got := countRows(t, engine.store, `SELECT COUNT(*) FROM task_events`); got != 2*testCase.finalRuns {
					t.Fatalf("review recovery event count = %d, want %d", got, 2*testCase.finalRuns)
				}
				if got := countRows(t, engine.store, `SELECT COUNT(*) FROM outbox`); got != 1 {
					t.Fatalf("review recovery outbox rows = %d, want 1", got)
				}
				accounting, err := engine.store.DerivedAccounting(context.Background(), ownerCase.TaskID, PhaseReview)
				if err != nil || accounting.Attempts != testCase.reviewAttempt || accounting.ReviewCycles != testCase.reviewCycles {
					t.Fatalf("derived review accounting = %+v, %v", accounting, err)
				}
				beforeEvents := countRows(t, engine.store, `SELECT COUNT(*) FROM task_events`)
				for _, run := range runs[:testCase.targetRun] {
					writeEngineRunResult(t, run, *validTestResult(run, ResultFailed))
				}
				if err := engine.ScanOnce(context.Background()); err != nil {
					t.Fatal(err)
				}
				current, _ := engine.store.GetTask(context.Background(), ownerCase.TaskID)
				if current.State != StateDone || countRows(t, engine.store, `SELECT COUNT(*) FROM task_events`) != beforeEvents {
					t.Fatalf("late implementation/review result changed terminal state: %+v", current)
				}
				assertEngineInvariants(t, engine.store, ownerCase.TaskID)
			})
		}
	}
}
