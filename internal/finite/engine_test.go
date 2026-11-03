package finite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/harness"
)

const (
	engineFixturePlanEnv          = "WERKRBY_FINITE_ENGINE_FIXTURE_PLAN"
	engineFixtureDuplicateGateEnv = "WERKRBY_FINITE_ENGINE_DUPLICATE_GATE"
)

type engineFixtureBehavior struct {
	Result    ResultOutcome `json:"result,omitempty"`
	Wait      *Wait         `json:"wait,omitempty"`
	Block     bool          `json:"block,omitempty"`
	NoResult  bool          `json:"no_result,omitempty"`
	Fail      bool          `json:"fail,omitempty"`
	Wall      bool          `json:"wall,omitempty"`
	WakeAt    string        `json:"wake_at,omitempty"`
	SkipUsage bool          `json:"skip_usage,omitempty"`
	ExitCode  *int          `json:"exit_code,omitempty"`
	// WallError replaces the fake wall's error text, so a test can seed the
	// provider wording the wake resolution reads.
	WallError string `json:"wall_error,omitempty"`
	// WallProbe writes wall_source "probe" with a provisional wake: the
	// provider named a wall without a parseable reset.
	WallProbe bool `json:"wall_probe,omitempty"`
}

type engineFixturePlan struct {
	Runs            map[string]engineFixtureBehavior `json:"runs"`
	InvocationsPath string                           `json:"invocations_path"`
}

type engineTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func newEngineTestClock() *engineTestClock {
	return &engineTestClock{now: time.Now().UTC()}
}

func (c *engineTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *engineTestClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

func newEngineFixture(t *testing.T, plan engineFixturePlan) *Engine {
	t.Helper()
	base := t.TempDir()
	root := filepath.Join(base, "finite state 世界")
	worker := filepath.Join(base, "worker target λ")
	if err := os.MkdirAll(worker, 0o700); err != nil {
		t.Fatal(err)
	}
	plan.InvocationsPath = filepath.Join(base, "provider invocations.jsonl")
	planPath := filepath.Join(base, "provider plan.json")
	writeEngineJSONFile(t, planPath, plan)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(Config{Root: root, WorkerDir: worker}, EngineOptions{
		Harness: harness.HarnessConfig{
			Name: "finite-engine-fixture",
			Cwd:  worker,
			Env:  []string{engineFixturePlanEnv + "=" + planPath},
		},
		Seats: func(context.Context) ([]Seat, error) {
			return []Seat{{Name: "fake-seat", Available: true}}, nil
		},
		Executable:   executable,
		PollInterval: 10 * time.Millisecond,
		StopGrace:    25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rows, err := engine.store.db.Query(`SELECT id,process_token FROM runs WHERE ended_at IS NULL AND process_token IS NOT NULL`)
		if err == nil {
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

func writeEngineJSONFile(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeEngineRunResult(t *testing.T, run Run, result Result) {
	t.Helper()
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(run.RunDir, "result.json")
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temporary, path); err != nil {
		t.Fatal(err)
	}
}

func scanEngineUntil(t *testing.T, engine *Engine, taskID string, desired State, maxRuns int) Task {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var last Task
	for time.Now().Before(deadline) {
		_ = engine.ScanOnce(context.Background())
		task, err := engine.store.GetTask(context.Background(), taskID)
		if err == nil {
			last = task
			runs, _ := engine.store.ListRuns(context.Background(), taskID)
			if task.State == desired && (maxRuns <= 0 || len(runs) == maxRuns) {
				return task
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	runs, _ := engine.store.ListRuns(context.Background(), taskID)
	for _, run := range runs {
		result, _ := os.ReadFile(filepath.Join(run.RunDir, "result.json"))
		completion, _ := os.ReadFile(filepath.Join(run.RunDir, "completion.json"))
		log, _ := os.ReadFile(filepath.Join(run.RunDir, "supervisor.log"))
		t.Logf("run=%+v result=%s completion=%s supervisor_log=%s", run, result, completion, log)
	}
	t.Fatalf("task did not reach %s with %d runs; last state %+v", desired, maxRuns, last)
	return Task{}
}

func assertEngineInvariants(t *testing.T, store *Store, taskID string) {
	t.Helper()
	if err := store.CheckInvariants(context.Background()); err != nil {
		t.Fatalf("store invariants: %v", err)
	}
	task, err := store.GetTask(context.Background(), taskID)
	if err != nil {
		t.Fatal(err)
	}
	runs, err := store.ListRuns(context.Background(), taskID)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := os.ReadFile(task.ProjectionPath)
	if err != nil {
		t.Fatalf("read projection: %v", err)
	}
	if expected := RenderProjection(task, runs); string(projection) != expected {
		t.Fatalf("projection is not exact committed state\n--- got ---\n%s\n--- want ---\n%s", projection, expected)
	}
}

func engineInvocationCount(t *testing.T, engine *Engine) int {
	t.Helper()
	var count int
	for _, variable := range engine.opts.Harness.Env {
		if !strings.HasPrefix(variable, engineFixturePlanEnv+"=") {
			continue
		}
		data, err := os.ReadFile(strings.TrimPrefix(variable, engineFixturePlanEnv+"="))
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
		for _, line := range strings.Split(strings.TrimSpace(string(contents)), "\n") {
			if line != "" {
				count++
			}
		}
	}
	return count
}

func waitEngineInvocationCount(t *testing.T, engine *Engine, desired int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if engineInvocationCount(t, engine) == desired {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("provider invocation count = %d, want %d", engineInvocationCount(t, engine), desired)
}

type engineFixtureHarness struct{}

func newEngineFixtureHarness(harness.HarnessConfig) (harness.Harness, error) {
	return &engineFixtureHarness{}, nil
}

func (h *engineFixtureHarness) Start(context.Context) error { return nil }
func (h *engineFixtureHarness) Close() error                { return nil }

func (h *engineFixtureHarness) Send(ctx context.Context, sessionKey, prompt string, emit core.Emit) (string, bool, error) {
	runID, err := strconv.ParseInt(os.Getenv(finiteRunIDEnv), 10, 64)
	if err != nil {
		return "", false, err
	}
	runDir := os.Getenv(finiteRunDirEnv)
	planPath := os.Getenv(engineFixturePlanEnv)
	data, err := os.ReadFile(planPath)
	if err != nil {
		return "", false, err
	}
	var plan engineFixturePlan
	if err := json.Unmarshal(data, &plan); err != nil {
		return "", false, err
	}
	behavior := plan.Runs[strconv.FormatInt(runID, 10)]
	invocation, _ := json.Marshal(map[string]any{"run_id": runID, "pid": os.Getpid(), "session_key": sessionKey})
	file, err := os.OpenFile(plan.InvocationsPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return "", false, err
	}
	_, appendErr := file.Write(append(invocation, '\n'))
	closeErr := file.Close()
	if err := errors.Join(appendErr, closeErr); err != nil {
		return "", false, err
	}
	if behavior.Block {
		if err := waitForReleaseFile(ctx, filepath.Join(runDir, "release")); err != nil {
			return "", false, err
		}
	}
	taskID, phase, err := engineFixturePromptBinding(prompt)
	if err != nil {
		return "", false, err
	}
	outcome := behavior.Result
	if outcome == "" {
		outcome = ResultDone
		if phase == PhaseReview {
			outcome = ResultAccepted
		}
	}
	if !behavior.NoResult {
		result := Result{SchemaVersion: 1, RunID: runID, TaskID: taskID, Phase: phase, Outcome: outcome, Progress: "fake provider completed the run", Summary: &Summary{Outcome: string(outcome), Evidence: "real fake-provider process", Uncertainty: "none"}, Wait: behavior.Wait}
		encoded, _ := json.Marshal(result)
		if err := writeEngineFixtureAtomic(filepath.Join(runDir, "result.json"), encoded); err != nil {
			return "", false, err
		}
	}
	if !behavior.SkipUsage {
		fresh, output, calls := runID, int64(7), int64(1)
		usage := map[string]any{"run_id": runID, "fresh_input_tokens": fresh, "output_tokens": output, "api_calls": calls, "source": "engine-fake", "collected_at": time.Now().UTC().Format(time.RFC3339Nano)}
		encoded, _ := json.Marshal(usage)
		if err := writeEngineFixtureAtomic(filepath.Join(runDir, "usage.json"), encoded); err != nil {
			return "", false, err
		}
	}
	if behavior.Wall || behavior.ExitCode != nil {
		execution := map[string]any{"schema_version": 1, "run_id": runID, "outcome": RunFail, "error": "fake provider failure"}
		if behavior.Wall {
			execution["outcome"] = RunWall
			execution["wall_wake_at"] = behavior.WakeAt
			execution["error"] = "fake quota wall"
			if behavior.WallError != "" {
				execution["error"] = behavior.WallError
			}
			if behavior.WallProbe {
				execution["wall_source"] = "probe"
				execution["wall_wake_at"] = time.Now().UTC().Add(15 * time.Minute).Format(time.RFC3339Nano)
			}
		}
		if behavior.ExitCode != nil {
			execution["exit_code"] = *behavior.ExitCode
		}
		encoded, _ := json.Marshal(execution)
		if err := writeEngineFixtureAtomic(filepath.Join(runDir, "execution.json"), encoded); err != nil {
			return "", false, err
		}
	}
	emit(core.Event{Kind: core.EventFinal, Text: "engine fake complete", Done: true})
	if behavior.Wall {
		return "", false, fmt.Errorf("fake quota wall")
	}
	if behavior.Fail {
		return "", false, fmt.Errorf("fake provider failure")
	}
	return "engine-fixture-thread", false, nil
}

func (h *engineFixtureHarness) Interrupt(context.Context, string) (bool, error) { return true, nil }
func (h *engineFixtureHarness) ResetSession(string) error                       { return nil }
func (h *engineFixtureHarness) ThreadID(string) string                          { return "engine-fixture-thread" }
func (h *engineFixtureHarness) IsActive(string) bool                            { return false }

func engineFixturePromptBinding(prompt string) (string, Phase, error) {
	var taskID string
	var phase Phase
	for _, line := range strings.Split(prompt, "\n") {
		if strings.HasPrefix(line, "task_id: ") {
			taskID = strings.TrimPrefix(line, "task_id: ")
		}
		if strings.HasPrefix(line, "phase: ") {
			phase = Phase(strings.TrimPrefix(line, "phase: "))
		}
	}
	if taskID == "" || (phase != PhaseImplementation && phase != PhaseReview) {
		return "", "", fmt.Errorf("fake provider prompt lacks binding")
	}
	return taskID, phase, nil
}

func writeEngineFixtureAtomic(path string, data []byte) error {
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, data, 0o600); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}
