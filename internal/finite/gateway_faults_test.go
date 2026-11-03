package finite

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/harness"
)

// The November 1 gateway restart failed two waves of Hermes runs (production
// runs 9 to 18) that provably spent no model tokens: either a usage record
// whose seven counters are all null (runs 9 to 13) or no usage record at all
// for a prompt the gateway never admitted (runs 14 to 18). Settlement must
// store those as uncharged interrupted runs that wait on a backoff timer, not
// as charged failures, while every other failure stays charged exactly as
// today. The fixtures under testdata/gateway-faults are verbatim copies of the
// production artifacts of runs 9 and 14.

type gatewayFaultPlan struct {
	hermes       bool   // supervisor.json expands to a headless Hermes staff entry
	completion   string // completion fixture under testdata/gateway-faults, "" for none
	usage        string // usage fixture under testdata/gateway-faults, "" for none
	outputTokens *int64 // when set, the usage artifact carries this output_tokens value
	admitted     *bool  // when set, pins the hermesPromptAdmitted seam for this test
}

func gatewayFaultToken(runID int64) string {
	raw := make([]byte, 32)
	binary.BigEndian.PutUint64(raw, uint64(runID))
	return base64.RawURLEncoding.EncodeToString(raw)
}

func stubHermesPromptAdmitted(t *testing.T, admitted bool) {
	t.Helper()
	previous := hermesPromptAdmitted
	hermesPromptAdmitted = func(context.Context, Run) (bool, error) { return admitted, nil }
	t.Cleanup(func() { hermesPromptAdmitted = previous })
}

func writeGatewayFaultSupervisor(t *testing.T, run Run, hermes bool) {
	t.Helper()
	request := supervisorRequest{
		SchemaVersion: supervisorRequestVersion,
		RunID:         run.ID,
		RunDir:        run.RunDir,
		ProcessToken:  run.ProcessToken,
		SessionKey:    run.SessionKey,
	}
	if hermes {
		worker := writeLiveWorker(t, t.TempDir(), "wk-test")
		request.Harness = harness.HarnessConfig{Name: "herdr", Cwd: worker, Model: "@pilot"}
	} else {
		request.Harness = harness.HarnessConfig{Name: "herdr", Cwd: t.TempDir()}
	}
	data, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(run.RunDir, "supervisor.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeGatewayFaultArtifact(t *testing.T, run Run, fixture, destination string, outputTokens *int64) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "gateway-faults", fixture))
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	value["run_id"] = run.ID
	if outputTokens != nil {
		value["output_tokens"] = *outputTokens
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(run.RunDir, destination), encoded, 0o600); err != nil {
		t.Fatal(err)
	}
}

// dispatchGatewayFaultRun prepares one run of the task on the engine fixture,
// writes its supervisor.json, copies the named production fault artifacts into
// the run directory, and returns the run with its process token set.
func dispatchGatewayFaultRun(t *testing.T, engine *Engine, task Task, plan gatewayFaultPlan) Run {
	t.Helper()
	ctx := context.Background()
	current, err := engine.store.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	run, err := engine.store.Dispatch(ctx, DispatchRequest{
		TaskID: current.ID, ExpectedVersion: current.Version, Phase: PhaseImplementation,
		Seat: "fake-seat", SessionKey: "finite:gateway-faults", LaunchReason: "retry", PromptHash: "gateway-fault",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	token := gatewayFaultToken(run.ID)
	if err := engine.prepareRun(ctx, run, token, Prompt{Text: "gateway fault probe", BundleHash: "gateway-fault", FullBundle: true}); err != nil {
		t.Fatal(err)
	}
	run.ProcessToken = token
	writeGatewayFaultSupervisor(t, run, plan.hermes)
	if plan.completion != "" {
		writeGatewayFaultArtifact(t, run, plan.completion, "completion.json", nil)
	}
	if plan.usage != "" {
		writeGatewayFaultArtifact(t, run, plan.usage, "usage.json", plan.outputTokens)
	}
	if plan.admitted != nil {
		stubHermesPromptAdmitted(t, *plan.admitted)
	}
	return run
}

func settleGatewayFaultRun(t *testing.T, engine *Engine, run Run) Run {
	t.Helper()
	if err := engine.recoverRun(context.Background(), run); err != nil {
		t.Fatalf("recover run %d: %v", run.ID, err)
	}
	settled, err := engine.store.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if settled.EndedAt == nil {
		t.Fatalf("run %d did not settle: %+v", run.ID, settled)
	}
	return settled
}

func wakeGatewayWait(t *testing.T, engine *Engine, clock *engineTestClock, taskID string) Task {
	t.Helper()
	ctx := context.Background()
	task, err := engine.store.GetTask(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if task.State != StateWaiting || task.WaitKind != WaitTime || task.WakeAt == nil {
		t.Fatalf("task %s is not on a timed wait: %+v", taskID, task)
	}
	clock.Advance(task.WakeAt.UTC().Sub(clock.Now()) + time.Second)
	woken, err := engine.store.WakeTime(ctx, task.ID, task.Version)
	if err != nil {
		t.Fatal(err)
	}
	if woken.State != StateTodo && woken.State != StateReview {
		t.Fatalf("wake left the task in %s, want the implementation queue", woken.State)
	}
	return woken
}

func gatewaySettledPayload(t *testing.T, store *Store, runID int64) map[string]any {
	t.Helper()
	rows, err := store.db.Query(`SELECT payload_json FROM task_events WHERE run_id=? AND kind='settled'`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("run %d has no settled event", runID)
	}
	var payload string
	if err := rows.Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		t.Fatalf("run %d has more than one settled event", runID)
	}
	var value map[string]any
	if err := json.Unmarshal([]byte(payload), &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func requireSettledPayload(t *testing.T, engine *Engine, runID int64, class string, charged bool) {
	t.Helper()
	payload := gatewaySettledPayload(t, engine.store, runID)
	if payload["failure_class"] != class {
		t.Fatalf("run %d settled payload failure_class = %v, want %q", runID, payload["failure_class"], class)
	}
	if payload["charged"] != charged {
		t.Fatalf("run %d settled payload charged = %v, want %v", runID, payload["charged"], charged)
	}
}

func requireUnchargedWait(t *testing.T, engine *Engine, task Task, run Run, settleAt time.Time, delay time.Duration) {
	t.Helper()
	settled := settleGatewayFaultRun(t, engine, run)
	if settled.Outcome != RunInterrupted {
		t.Fatalf("no-spend gateway fault settled as %q (%q), want interrupted", settled.Outcome, settled.Error)
	}
	current, err := engine.store.GetTask(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := settleAt.Add(delay)
	if current.State != StateWaiting || current.WaitKind != WaitTime || current.WakeAt == nil || !current.WakeAt.Equal(want) {
		t.Fatalf("task after the fault = state %s wait_kind %s wake_at %v, want waiting on %s", current.State, current.WaitKind, current.WakeAt, want)
	}
	requireSettledPayload(t, engine, run.ID, "hermes-no-spend", false)
}

func newGatewayFaultEngine(t *testing.T) (*Engine, *engineTestClock) {
	t.Helper()
	engine := newEngineFixture(t, engineFixturePlan{})
	clock := newEngineTestClock()
	engine.store.now = clock.Now
	return engine, clock
}

func boolValue(value bool) *bool { return &value }

func TestGatewayFaultWithNullUsageIsUncharged(t *testing.T) {
	engine, clock := newGatewayFaultEngine(t)
	task := intakeTestTask(t, engine.store, "gateway-null-usage", false)
	run := dispatchGatewayFaultRun(t, engine, task, gatewayFaultPlan{
		hermes: true, completion: "completion-run9.json", usage: "usage-run9.json",
	})
	settleAt := clock.Now()
	requireUnchargedWait(t, engine, task, run, settleAt, 30*time.Second)
	accounting, err := engine.store.DerivedAccounting(context.Background(), task.ID, PhaseImplementation)
	if err != nil || accounting.Attempts != 0 || accounting.Failures != 0 || accounting.Runs != 1 {
		t.Fatalf("accounting = %+v, %v; want one uncharged run", accounting, err)
	}
}

func TestGatewayFaultNeverAdmittedIsUncharged(t *testing.T) {
	engine, clock := newGatewayFaultEngine(t)
	task := intakeTestTask(t, engine.store, "gateway-never-admitted", false)
	run := dispatchGatewayFaultRun(t, engine, task, gatewayFaultPlan{
		hermes: true, completion: "completion-run14.json", admitted: boolValue(false),
	})
	settleAt := clock.Now()
	requireUnchargedWait(t, engine, task, run, settleAt, 30*time.Second)
	accounting, err := engine.store.DerivedAccounting(context.Background(), task.ID, PhaseImplementation)
	if err != nil || accounting.Attempts != 0 || accounting.Failures != 0 || accounting.Runs != 1 {
		t.Fatalf("accounting = %+v, %v; want one uncharged run", accounting, err)
	}
}

func TestHermesFailWithoutUsageButAdmittedStaysCharged(t *testing.T) {
	engine, _ := newGatewayFaultEngine(t)
	task := intakeTestTask(t, engine.store, "gateway-admitted-no-usage", false)
	run := dispatchGatewayFaultRun(t, engine, task, gatewayFaultPlan{
		hermes: true, completion: "completion-run14.json", admitted: boolValue(true),
	})
	settled := settleGatewayFaultRun(t, engine, run)
	if settled.Outcome != RunFail {
		t.Fatalf("admitted Hermes failure settled as %q, want a charged fail", settled.Outcome)
	}
	accounting, err := engine.store.DerivedAccounting(context.Background(), task.ID, PhaseImplementation)
	if err != nil || accounting.Failures != 1 || accounting.Attempts != 1 {
		t.Fatalf("accounting = %+v, %v; want 1 charged failure", accounting, err)
	}
	current, err := engine.store.GetTask(context.Background(), task.ID)
	if err != nil || current.State != StateTodo {
		t.Fatalf("task after the charged failure = %+v, %v; want todo with no wait", current, err)
	}
}

func TestHermesFailWithTokensStaysCharged(t *testing.T) {
	engine, _ := newGatewayFaultEngine(t)
	task := intakeTestTask(t, engine.store, "gateway-spent-tokens", false)
	spent := int64(12)
	run := dispatchGatewayFaultRun(t, engine, task, gatewayFaultPlan{
		hermes: true, completion: "completion-run9.json", usage: "usage-run9.json", outputTokens: &spent,
	})
	settled := settleGatewayFaultRun(t, engine, run)
	if settled.Outcome != RunFail {
		t.Fatalf("Hermes failure with token spend settled as %q, want a charged fail", settled.Outcome)
	}
	accounting, err := engine.store.DerivedAccounting(context.Background(), task.ID, PhaseImplementation)
	if err != nil || accounting.Failures != 1 || accounting.Attempts != 1 {
		t.Fatalf("accounting = %+v, %v; want 1 charged failure", accounting, err)
	}
}

func TestNonHermesFailWithoutUsageStaysCharged(t *testing.T) {
	engine, _ := newGatewayFaultEngine(t)
	task := intakeTestTask(t, engine.store, "gateway-non-hermes", false)
	run := dispatchGatewayFaultRun(t, engine, task, gatewayFaultPlan{
		hermes: false, completion: "completion-run14.json", admitted: boolValue(false),
	})
	settled := settleGatewayFaultRun(t, engine, run)
	if settled.Outcome != RunFail {
		t.Fatalf("non-Hermes failure settled as %q, want a charged fail", settled.Outcome)
	}
	accounting, err := engine.store.DerivedAccounting(context.Background(), task.ID, PhaseImplementation)
	if err != nil || accounting.Failures != 1 || accounting.Attempts != 1 {
		t.Fatalf("accounting = %+v, %v; want 1 charged failure", accounting, err)
	}
}

func TestGatewayFaultBackoffDoublesAndResets(t *testing.T) {
	t.Run("doubles", func(t *testing.T) {
		engine, clock := newGatewayFaultEngine(t)
		task := intakeTestTask(t, engine.store, "gateway-backoff-doubles", false)
		delays := []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute}
		for i, delay := range delays {
			if i > 0 {
				wakeGatewayWait(t, engine, clock, task.ID)
			}
			run := dispatchGatewayFaultRun(t, engine, task, gatewayFaultPlan{
				hermes: true, completion: "completion-run9.json", usage: "usage-run9.json", admitted: boolValue(false),
			})
			requireUnchargedWait(t, engine, task, run, clock.Now(), delay)
		}
	})
	t.Run("resets_after_charged_failure", func(t *testing.T) {
		engine, clock := newGatewayFaultEngine(t)
		task := intakeTestTask(t, engine.store, "gateway-backoff-resets", false)
		run := dispatchGatewayFaultRun(t, engine, task, gatewayFaultPlan{
			hermes: true, completion: "completion-run9.json", usage: "usage-run9.json", admitted: boolValue(false),
		})
		requireUnchargedWait(t, engine, task, run, clock.Now(), 30*time.Second)
		wakeGatewayWait(t, engine, clock, task.ID)
		spent := int64(12)
		charged := dispatchGatewayFaultRun(t, engine, task, gatewayFaultPlan{
			hermes: true, completion: "completion-run9.json", usage: "usage-run9.json", outputTokens: &spent,
		})
		settled := settleGatewayFaultRun(t, engine, charged)
		if settled.Outcome != RunFail {
			t.Fatalf("charged failure settled as %q, want fail", settled.Outcome)
		}
		current, err := engine.store.GetTask(context.Background(), task.ID)
		if err != nil || current.State != StateTodo || current.WakeAt != nil || current.WaitKind != "" {
			t.Fatalf("task after the charged failure = %+v, %v; want todo with no wait", current, err)
		}
		requireSettledPayload(t, engine, charged.ID, "provider-fail", true)
		accounting, err := engine.store.DerivedAccounting(context.Background(), task.ID, PhaseImplementation)
		if err != nil || accounting.Attempts != 1 || accounting.Failures != 1 {
			t.Fatalf("accounting after the charged failure = %+v, %v; want 1 attempt and 1 failure", accounting, err)
		}
		run = dispatchGatewayFaultRun(t, engine, task, gatewayFaultPlan{
			hermes: true, completion: "completion-run9.json", usage: "usage-run9.json", admitted: boolValue(false),
		})
		requireUnchargedWait(t, engine, task, run, clock.Now(), 30*time.Second)
		accounting, err = engine.store.DerivedAccounting(context.Background(), task.ID, PhaseImplementation)
		if err != nil || accounting.Attempts != 1 || accounting.Failures != 1 || accounting.Runs != 3 {
			t.Fatalf("accounting after the reset fault = %+v, %v; want 3 runs, still 1 charged failure", accounting, err)
		}
	})
}

func TestGatewayFaultsStillHitMaxRuns(t *testing.T) {
	engine, clock := newGatewayFaultEngine(t)
	task := intakeTestTask(t, engine.store, "gateway-max-runs", false)
	for i := 1; i <= task.MaxRuns; i++ {
		if i > 1 {
			wakeGatewayWait(t, engine, clock, task.ID)
		}
		run := dispatchGatewayFaultRun(t, engine, task, gatewayFaultPlan{
			hermes: true, completion: "completion-run9.json", usage: "usage-run9.json", admitted: boolValue(false),
		})
		settled := settleGatewayFaultRun(t, engine, run)
		if settled.Outcome != RunInterrupted {
			t.Fatalf("failure %d settled as %q, want interrupted", i, settled.Outcome)
		}
	}
	current, err := engine.store.GetTask(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != StateFailed {
		t.Fatalf("task after %d uncharged gateway faults = %s, want failed", task.MaxRuns, current.State)
	}
	accounting, err := engine.store.DerivedAccounting(context.Background(), task.ID, PhaseImplementation)
	if err != nil || accounting.Runs != task.MaxRuns || accounting.Attempts != 0 || accounting.Failures != 0 {
		t.Fatalf("accounting = %+v, %v; want %d uncharged runs", accounting, err, task.MaxRuns)
	}
	last, err := engine.store.ListRuns(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	payload := gatewaySettledPayload(t, engine.store, last[len(last)-1].ID)
	if payload["failure_reason"] != "max_runs" {
		t.Fatalf("twelfth settled payload failure_reason = %v, want max_runs", payload["failure_reason"])
	}
	requireSettledPayload(t, engine, last[len(last)-1].ID, "hermes-no-spend", false)
}

// The two November 1 waves replayed: five tasks, each settling one
// null-usage failure and then one never-admitted failure, exactly the
// production run pairs of runs 9 to 18. The run numbers are labels; the
// artifacts are the run 9 and run 14 fixtures.
func TestReplayRuns9To18(t *testing.T) {
	pairs := []struct {
		taskID string
		first  string
		second string
	}{
		{"tasks-20261029-nas-retention-fixes-01", "run 9", "run 16"},
		{"tasks-20261101-finite-windows-supervision-fixes-g2", "run 10", "run 18"},
		{"tasks-helm-4511873c-t-feed-token-log-leak-20261101-4573deda", "run 11", "run 17"},
		{"tasks-helm-8e0869b9-backup-chain-gap-destination-c-83f32d2d", "run 12", "run 15"},
		{"tasks-20261029-jev-library-curator", "run 13", "run 14"},
	}
	for _, pair := range pairs {
		t.Run(pair.taskID, func(t *testing.T) {
			engine, clock := newGatewayFaultEngine(t)
			task := intakeTestTask(t, engine.store, pair.taskID, false)
			stubHermesPromptAdmitted(t, false)
			ctx := context.Background()
			first := dispatchGatewayFaultRun(t, engine, task, gatewayFaultPlan{
				hermes: true, completion: "completion-run9.json", usage: "usage-run9.json",
			})
			settled := settleGatewayFaultRun(t, engine, first)
			if settled.Outcome != RunInterrupted {
				t.Fatalf("%s settled as %q, want interrupted", pair.first, settled.Outcome)
			}
			requireSettledPayload(t, engine, first.ID, "hermes-no-spend", false)
			wakeGatewayWait(t, engine, clock, task.ID)
			second := dispatchGatewayFaultRun(t, engine, task, gatewayFaultPlan{
				hermes: true, completion: "completion-run14.json",
			})
			settled = settleGatewayFaultRun(t, engine, second)
			if settled.Outcome != RunInterrupted {
				t.Fatalf("%s settled as %q, want interrupted", pair.second, settled.Outcome)
			}
			requireSettledPayload(t, engine, second.ID, "hermes-no-spend", false)
			accounting, err := engine.store.DerivedAccounting(ctx, task.ID, PhaseImplementation)
			if err != nil || accounting.Runs != 2 || accounting.Attempts != 0 || accounting.Failures != 0 {
				t.Fatalf("accounting = %+v, %v; want two uncharged runs", accounting, err)
			}
		})
	}
}
