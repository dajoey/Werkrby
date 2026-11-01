package finite

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFiniteCorrectnessNewRunAlwaysGetsFullPrompt(t *testing.T) {
	engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
		"1": {Result: ResultTodo},
		"2": {Result: ResultDone},
	}})
	task := intakeTestTask(t, engine.store, "correctness-full-prompt", false)
	scanEngineUntil(t, engine, task.ID, StateDone, 2)
	runs, err := engine.store.ListRuns(context.Background(), task.ID)
	if err != nil || len(runs) != 2 {
		t.Fatalf("prompt runs = %+v, %v", runs, err)
	}
	for _, run := range runs {
		prompt, err := os.ReadFile(filepath.Join(run.RunDir, "prompt.txt"))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(prompt), universalPromptCore) {
			t.Fatalf("new run %d omitted the static prompt bundle", run.ID)
		}
		sessionData, err := os.ReadFile(filepath.Join(run.RunDir, "session.json"))
		if err != nil {
			t.Fatal(err)
		}
		var session engineSessionRecord
		if err := json.Unmarshal(sessionData, &session); err != nil {
			t.Fatal(err)
		}
		if !session.FullBundle {
			t.Fatalf("new run %d was marked as a delta continuation", run.ID)
		}
	}
	if runs[0].PromptHash != runs[1].PromptHash {
		t.Fatalf("fixture did not exercise equal static hashes: %q != %q", runs[0].PromptHash, runs[1].PromptHash)
	}
}

func TestFiniteCorrectnessBriefHashFailsClosed(t *testing.T) {
	engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{"1": {Result: ResultDone}}})
	task := intakeTestTask(t, engine.store, "correctness-brief-hash", false)
	data, err := os.ReadFile(task.BriefPath)
	if err != nil {
		t.Fatal(err)
	}
	originalLength := len(data)
	data[len(data)-2] = 'X'
	if len(data) != originalLength {
		t.Fatal("fixture changed brief length")
	}
	if err := os.WriteFile(task.BriefPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	err = engine.ScanOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "SHA-256") {
		t.Fatalf("same-size brief mutation was not rejected by SHA-256: %v", err)
	}
	current, getErr := engine.store.GetTask(context.Background(), task.ID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if current.State != StateTodo || current.Version != 0 {
		t.Fatalf("brief mutation changed task state: %+v", current)
	}
	if got := countRows(t, engine.store, `SELECT COUNT(*) FROM runs`); got != 0 {
		t.Fatalf("brief mutation created %d runs", got)
	}
	if got := countRows(t, engine.store, `SELECT COUNT(*) FROM task_events`); got != 0 {
		t.Fatalf("brief mutation created %d events", got)
	}
	if got := engineInvocationCount(t, engine); got != 0 {
		t.Fatalf("brief mutation spawned %d providers", got)
	}
	assertEngineInvariants(t, engine.store, task.ID)
}

func TestFiniteCorrectnessAdmissionFailureIsTerminal(t *testing.T) {
	t.Run("time_wait_at_cap", func(t *testing.T) {
		clock := newEngineTestClock()
		wake := clock.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
		engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
			"1": {Result: ResultWait, Wait: &Wait{Kind: WaitTime, WakeAt: wake, Reason: "cap wait"}},
		}})
		engine.store.now = clock.Now
		task := intakeCorrectnessCapTask(t, engine, "correctness-cap-time")
		scanEngineUntil(t, engine, task.ID, StateWaiting, 1)
		clock.Advance(2 * time.Hour)
		if err := engine.ScanOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		assertCorrectnessAdmissionFailure(t, engine, task, 4, 4)
	})

	t.Run("joey_wait_at_cap", func(t *testing.T) {
		engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
			"1": {Result: ResultWait, Wait: &Wait{Kind: WaitJoey, Reason: "cap answer", Ask: json.RawMessage(`{"question":"Continue?","options":[{"id":"yes","label":"Yes"}]}`)}},
		}})
		task := intakeCorrectnessCapTask(t, engine, "correctness-cap-joey")
		waiting := scanEngineUntil(t, engine, task.ID, StateWaiting, 1)
		answered, err := engine.AnswerJoey(context.Background(), task.ID, waiting.Version, "yes")
		if err != nil || answered.State != StateTodo {
			t.Fatalf("answer at cap = %+v, %v", answered, err)
		}
		if err := engine.ScanOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		assertCorrectnessAdmissionFailure(t, engine, task, 4, 4)
	})

	t.Run("non_dispatch_at_cap", func(t *testing.T) {
		engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
			"1": {Result: ResultTodo},
		}})
		task := intakeCorrectnessCapTask(t, engine, "correctness-cap-direct")
		scanEngineUntil(t, engine, task.ID, StateTodo, 1)
		if err := engine.ScanOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		assertCorrectnessAdmissionFailure(t, engine, task, 3, 3)
	})
}

func intakeCorrectnessCapTask(t *testing.T, engine *Engine, id string) Task {
	t.Helper()
	source := filepath.Join(t.TempDir(), id+".md")
	brief := fmt.Sprintf("---\nid: %s\ntitle: Correctness cap %s\nrisk: low\nreview_required: false\nmax_runs: 1\n---\n\nExercise the cap exactly once.\n", id, id)
	if err := os.WriteFile(source, []byte(brief), 0o600); err != nil {
		t.Fatal(err)
	}
	task, err := engine.store.IntakeBrief(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func assertCorrectnessAdmissionFailure(t *testing.T, engine *Engine, task Task, version, events int64) {
	t.Helper()
	current, err := engine.store.GetTask(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != StateFailed || current.Version != version {
		t.Fatalf("cap did not produce terminal failure: task=%+v, want failed version %d", current, version)
	}
	runs, err := engine.store.ListRuns(context.Background(), task.ID)
	if err != nil || len(runs) != 1 {
		t.Fatalf("cap created an extra provider run: %+v, %v", runs, err)
	}
	accounting, err := engine.store.DerivedAccounting(context.Background(), task.ID, PhaseImplementation)
	if err != nil || accounting.Attempts != 1 {
		t.Fatalf("cap accounting = %+v, %v", accounting, err)
	}
	if got := countRows(t, engine.store, `SELECT COUNT(*) FROM task_events WHERE task_id=?`, task.ID); got != int(events) {
		t.Fatalf("cap event count = %d, want %d", got, events)
	}
	if got := countRows(t, engine.store, `SELECT COUNT(*) FROM task_events WHERE task_id=? AND kind='admission_failed'`, task.ID); got != 1 {
		t.Fatalf("admission failure events = %d, want one", got)
	}
	var payload string
	if err := engine.store.db.QueryRow(`SELECT payload_json FROM task_events WHERE task_id=? AND kind='admission_failed'`, task.ID).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(payload, "max_runs") {
		t.Fatalf("admission failure payload = %q, want max_runs reason", payload)
	}
	if got := countRows(t, engine.store, `SELECT COUNT(*) FROM outbox`); got != 1 {
		t.Fatalf("terminal outbox rows = %d, want one", got)
	}
	if got := engineInvocationCount(t, engine); got != 1 {
		t.Fatalf("provider invocations = %d, want one", got)
	}
	assertEngineInvariants(t, engine.store, task.ID)
}
