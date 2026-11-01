package finite

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/harness"
)

func newLiveHermesOwner(t *testing.T, cfg Config, provider harness.HarnessConfig) *Engine {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	var owner *Engine
	inspect := func(ctx context.Context, runID int64, token string) (RunProcesses, error) {
		processes, processErr := InspectRun(ctx, runID, token)
		run, runErr := owner.store.GetRun(ctx, runID)
		if runErr != nil {
			return RunProcesses{Liveness: LivenessUnknown}, runErr
		}
		provider, providerErr := harness.InspectHermesRunBoundary(ctx, harness.RunBoundarySpec{RunID: runID, ProcessToken: token, RunDir: run.RunDir})
		if processErr != nil || providerErr != nil {
			return RunProcesses{Liveness: LivenessUnknown, Supervisor: processes.Supervisor, Processes: processes.Processes}, errors.Join(processErr, providerErr)
		}
		if processes.Liveness == LivenessLive || provider.Liveness == harness.BoundaryLive {
			processes.Liveness = LivenessLive
			return processes, nil
		}
		return processes, nil
	}
	stop := func(ctx context.Context, runID int64, token string, grace time.Duration) (Liveness, error) {
		processState, processErr := StopRun(ctx, runID, token, grace)
		run, runErr := owner.store.GetRun(ctx, runID)
		if runErr != nil {
			return LivenessUnknown, runErr
		}
		providerState, providerErr := harness.StopHermesRunBoundary(ctx, harness.RunBoundarySpec{RunID: runID, ProcessToken: token, RunDir: run.RunDir})
		if processErr != nil || providerErr != nil {
			return LivenessUnknown, errors.Join(processErr, providerErr)
		}
		if processState == LivenessLive || providerState == harness.BoundaryLive {
			return LivenessLive, nil
		}
		if processState == LivenessAbsent && providerState == harness.BoundaryAbsent {
			return LivenessAbsent, nil
		}
		return LivenessUnknown, ErrProcessUnknown
	}
	owner, err = NewEngine(cfg, EngineOptions{
		Harness: provider, Executable: executable, PollInterval: 50 * time.Millisecond, StopGrace: 25 * time.Millisecond,
		Seats: func(context.Context) ([]Seat, error) { return []Seat{{Name: "wk-finite-test", Available: true}}, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	owner.opts.hooks.inspectRun = inspect
	owner.opts.hooks.stopRun = stop
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		rows, err := owner.store.db.Query(`SELECT id,process_token FROM runs WHERE ended_at IS NULL AND process_token IS NOT NULL`)
		if err == nil {
			for rows.Next() {
				var id int64
				var token string
				if rows.Scan(&id, &token) == nil {
					_, _ = stop(ctx, id, token, 0)
				}
			}
			_ = rows.Close()
		}
		_ = owner.Close()
	})
	return owner
}

func liveHermesBrief(root string, wake time.Time) string {
	result := func(run int, phase Phase, outcome ResultOutcome) string {
		return fmt.Sprintf(`{"schema_version":1,"run_id":%d,"task_id":"finite-hermes-live","phase":%q,"outcome":%q,"progress":"scripted live protocol step","summary":{"outcome":%q,"evidence":"real Hermes turn","uncertainty":"none"}}`, run, phase, outcome, outcome)
	}
	var lines []string
	lines = append(lines, `---
id: finite-hermes-live
title: Exercise the disposable Hermes finite boundary
risk: low
review_required: true
max_runs: 12
max_failures: 4
max_review_cycles: 3
---
This is a disposable protocol test. Perform no investigation and make no other writes. For the current run only, use the terminal tool to write the specified bytes to the specified result.json, then stop.`)
	lines = append(lines, fmt.Sprintf("Run 1 (implementation): write exactly `not json` to `%s`.", filepath.Join(root, "state", "runs", "1", "result.json")))
	lines = append(lines, fmt.Sprintf("Run 2 (implementation): write a JSON result with schema_version=1, run_id=2, task_id=finite-hermes-live, phase=implementation, outcome=wait, progress=scripted wait, summary={outcome:wait,evidence:real Hermes turn,uncertainty:none}, and wait={kind:time,wake_at:%q,reason:scripted wake} to `%s`.", wake.UTC().Format(time.RFC3339), filepath.Join(root, "state", "runs", "2", "result.json")))
	for _, step := range []struct {
		run     int
		phase   Phase
		outcome ResultOutcome
	}{{3, PhaseImplementation, ResultReview}, {4, PhaseReview, ResultRejected}, {5, PhaseImplementation, ResultReview}, {6, PhaseReview, ResultAccepted}} {
		text := result(step.run, step.phase, step.outcome)
		if step.outcome == ResultRejected {
			text = strings.TrimSuffix(text, "}") + `,"findings":["scripted review rejection"]}`
		}
		lines = append(lines, fmt.Sprintf("Run %d (%s): write exactly this JSON to `%s`: %s", step.run, step.phase, filepath.Join(root, "state", "runs", fmt.Sprint(step.run), "result.json"), text))
	}
	return strings.Join(lines, "\n")
}

func TestFiniteLiveHeadlessHermesWorkflow(t *testing.T) {
	if os.Getenv("WERKRBY_FINITE_HERMES_LIVE") != "1" {
		t.Skip("real finite Hermes workflow requires explicit live opt-in")
	}
	home := os.Getenv("WERKRBY_FINITE_HERMES_HOME")
	if home == "" {
		t.Fatal("WERKRBY_FINITE_HERMES_HOME is required")
	}
	marker, err := os.ReadFile(filepath.Join(home, ".finite-hermes-disposable"))
	if err != nil || string(marker) != "wk-finite-test\n" {
		t.Fatalf("disposable Hermes marker: %v", err)
	}
	t.Setenv("HERMES_HOME", home)
	root := t.TempDir()
	if evidence := os.Getenv("WERKRBY_FINITE_HERMES_EVIDENCE"); evidence != "" {
		root = filepath.Join(evidence, "finite-workflow")
		if err := os.MkdirAll(evidence, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(root, 0o700); err != nil {
			t.Fatalf("refuse to reuse finite live evidence: %v", err)
		}
	}
	worker := filepath.Join(root, "worker")
	if err := os.MkdirAll(filepath.Join(worker, ".werkrby"), 0o700); err != nil {
		t.Fatal(err)
	}
	model := os.Getenv("WERKRBY_FINITE_HERMES_MODEL")
	if model == "" {
		model = "zai/glm-5.3-flash"
	}
	roster := fmt.Sprintf("staff:\n  pilot:\n    runner: hermes\n    headless: true\n    args: [\"-p\", \"wk-finite-test\", \"--model\", %q]\n", model)
	if err := os.WriteFile(filepath.Join(worker, ".werkrby", "roster.yaml"), []byte(roster), 0o600); err != nil {
		t.Fatal(err)
	}
	briefPath := filepath.Join(root, "brief.md")
	if err := os.WriteFile(briefPath, []byte(liveHermesBrief(root, time.Now().Add(2*time.Second))), 0o600); err != nil {
		t.Fatal(err)
	}
	provider := harness.HarnessConfig{
		Name: "herdr", Model: "@pilot", Cwd: worker,
		Env: []string{"HERMES_HOME=" + home, "WERKRBY_FINITE_HERMES_LIVE=1"},
	}
	cfg := Config{Root: filepath.Join(root, "state"), WorkerDir: worker}
	owner := newLiveHermesOwner(t, cfg, provider)
	task, err := owner.store.IntakeBrief(context.Background(), briefPath)
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(8 * time.Minute)
	restarted := false
	var last Task
	for time.Now().Before(deadline) {
		_ = owner.ScanOnce(context.Background())
		last, err = owner.store.GetTask(context.Background(), task.ID)
		if err != nil {
			t.Fatal(err)
		}
		runs, listErr := owner.store.ListRuns(context.Background(), task.ID)
		if listErr != nil {
			t.Fatal(listErr)
		}
		if !restarted && len(runs) >= 4 && runs[3].StartedAt != nil && runs[3].EndedAt == nil {
			if err := owner.Close(); err != nil {
				t.Fatal(err)
			}
			owner = newLiveHermesOwner(t, cfg, provider)
			restarted = true
		}
		if last.State == StateDone && len(runs) == 6 && restarted {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !restarted {
		t.Fatalf("owner was not restarted during the first review; last=%+v", last)
	}
	if last.State != StateDone {
		runs, _ := owner.store.ListRuns(context.Background(), task.ID)
		for _, run := range runs {
			result, _ := os.ReadFile(filepath.Join(run.RunDir, "result.json"))
			completion, _ := os.ReadFile(filepath.Join(run.RunDir, "completion.json"))
			log, _ := os.ReadFile(filepath.Join(run.RunDir, "supervisor.log"))
			t.Logf("run=%+v result=%s completion=%s log=%s", run, result, completion, log)
		}
		t.Fatalf("live workflow did not finish: %+v", last)
	}
	runs, err := owner.store.ListRuns(context.Background(), task.ID)
	if err != nil || len(runs) != 6 {
		t.Fatalf("live runs = %+v, %v", runs, err)
	}
	seen := map[string]bool{}
	for index, run := range runs {
		if run.Outcome != RunOK {
			t.Errorf("run %d outcome = %s, want ok", run.ID, run.Outcome)
		}
		observation, err := harness.InspectHermesRunBoundary(context.Background(), harness.RunBoundarySpec{RunID: run.ID, ProcessToken: run.ProcessToken, RunDir: run.RunDir})
		if err != nil || observation.Liveness != harness.BoundaryAbsent || observation.Identity == nil {
			t.Errorf("run %d terminal boundary = %+v, %v", run.ID, observation, err)
			continue
		}
		if observation.Identity.RunID != run.ID || observation.Identity.ProcessToken != run.ProcessToken || seen[observation.Identity.AdmissionID] {
			t.Errorf("run %d exact identity = %+v", run.ID, observation.Identity)
		}
		seen[observation.Identity.AdmissionID] = true
		usage, err := owner.store.GetUsage(context.Background(), run.ID)
		if err != nil || usage.APICalls == nil || *usage.APICalls < 1 || usage.Source != "hermes" {
			t.Errorf("run %d usage = %+v, %v", run.ID, usage, err)
		}
		if index == 1 && observation.Outcome != "completed" {
			t.Errorf("wait run provider outcome = %s", observation.Outcome)
		}
	}
	if got := countRows(t, owner.store, `SELECT COUNT(*) FROM task_events WHERE task_id=? AND kind='woke'`, task.ID); got != 1 {
		t.Errorf("wake events = %d, want 1", got)
	}
	if got := countRows(t, owner.store, `SELECT COUNT(*) FROM task_events WHERE task_id=? AND kind='settled' AND json_extract(payload_json,'$.error')='invalid_result'`, task.ID); got != 1 {
		t.Errorf("invalid-result settlements = %d, want 1", got)
	}
	accounting, err := owner.store.DerivedAccounting(context.Background(), task.ID, PhaseReview)
	if err != nil || accounting.ReviewCycles != 1 || accounting.Attempts != 2 {
		t.Errorf("review accounting = %+v, %v; want one rejection and two attempts", accounting, err)
	}
	assertEngineInvariants(t, owner.store, task.ID)
}
