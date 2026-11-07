package finite

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestFiniteWaitWallReviewTransitions_P1_P3_P4(t *testing.T) {
	t.Run("P1/implementation_wall_wakes_without_charge", func(t *testing.T) {
		clock := newEngineTestClock()
		wake := clock.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
		engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
			"1": {NoResult: true, Wall: true, WakeAt: wake},
			"2": {Result: ResultDone},
		}})
		engine.store.now = clock.Now
		task := intakeTestTask(t, engine.store, "wall-implementation", false)
		if err := engine.ScanOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		if runs, _ := engine.store.ListRuns(context.Background(), task.ID); len(runs) != 1 {
			t.Fatalf("wall setup did not dispatch one real run: %+v", runs)
		}
		waiting := scanEngineUntil(t, engine, task.ID, StateWaiting, 1)
		if waiting.ResumeState != StateTodo || waiting.WaitKind != WaitTime || waiting.WakeAt == nil {
			t.Fatalf("implementation wall wait = %+v", waiting)
		}
		first, _ := engine.store.GetRun(context.Background(), 1)
		accounting, _ := engine.store.DerivedAccounting(context.Background(), task.ID, PhaseImplementation)
		if first.Outcome != RunWall || accounting.Attempts != 0 || accounting.Failures != 0 {
			t.Fatalf("wall charged or misclassified: run=%+v accounting=%+v", first, accounting)
		}
		_ = engine.ScanOnce(context.Background())
		if got := engineInvocationCount(t, engine); got != 1 {
			t.Fatalf("dispatch occurred before provider reset: %d invocations", got)
		}
		clock.Advance(2 * time.Hour)
		scanEngineUntil(t, engine, task.ID, StateDone, 2)
		assertEngineInvariants(t, engine.store, task.ID)
	})

	t.Run("P1/review_wall_resumes_review", func(t *testing.T) {
		clock := newEngineTestClock()
		wake := clock.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
		engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
			"1": {Result: ResultDone},
			"2": {NoResult: true, Wall: true, WakeAt: wake},
			"3": {Result: ResultAccepted},
		}})
		engine.store.now = clock.Now
		task := intakeTestTask(t, engine.store, "wall-review", true)
		if err := engine.ScanOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		if runs, _ := engine.store.ListRuns(context.Background(), task.ID); len(runs) != 1 {
			t.Fatalf("review wall setup did not dispatch: %+v", runs)
		}
		waiting := scanEngineUntil(t, engine, task.ID, StateWaiting, 2)
		if waiting.ResumeState != StateReview {
			t.Fatalf("review wall resumed into %s", waiting.ResumeState)
		}
		clock.Advance(2 * time.Hour)
		scanEngineUntil(t, engine, task.ID, StateDone, 3)
		runs, _ := engine.store.ListRuns(context.Background(), task.ID)
		if runs[1].Outcome != RunWall || runs[2].Phase != PhaseReview {
			t.Fatalf("review wall runs = %+v", runs)
		}
		accounting, _ := engine.store.DerivedAccounting(context.Background(), task.ID, PhaseReview)
		if accounting.Attempts != 1 {
			t.Fatalf("review wall changed charged attempts: %+v", accounting)
		}
		assertEngineInvariants(t, engine.store, task.ID)
	})

	t.Run("P1/valid_result_precedes_wall_error", func(t *testing.T) {
		engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
			"1": {Result: ResultDone, Wall: true, WakeAt: time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)},
		}})
		task := intakeTestTask(t, engine.store, "wall-result-precedence", false)
		if err := engine.ScanOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		if runs, _ := engine.store.ListRuns(context.Background(), task.ID); len(runs) != 1 {
			t.Fatalf("result/wall setup did not dispatch: %+v", runs)
		}
		scanEngineUntil(t, engine, task.ID, StateDone, 1)
		run, _ := engine.store.GetRun(context.Background(), 1)
		if run.Outcome != RunOK {
			t.Fatalf("valid result did not beat wall error: %+v", run)
		}
		assertEngineInvariants(t, engine.store, task.ID)
	})

	t.Run("P3/time_wait_has_one_wake_transition", func(t *testing.T) {
		clock := newEngineTestClock()
		wake := clock.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
		engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
			"1": {Result: ResultWait, Wait: &Wait{Kind: WaitTime, WakeAt: wake, Reason: "test clock"}},
			"2": {Result: ResultDone},
		}})
		engine.store.now = clock.Now
		task := intakeTestTask(t, engine.store, "wait-time", false)
		if err := engine.ScanOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		if runs, _ := engine.store.ListRuns(context.Background(), task.ID); len(runs) != 1 {
			t.Fatalf("time-wait setup did not dispatch: %+v", runs)
		}
		waiting := scanEngineUntil(t, engine, task.ID, StateWaiting, 1)
		if _, err := engine.store.WakeTime(context.Background(), task.ID, waiting.Version); !errors.Is(err, ErrStateConflict) {
			t.Fatalf("early wake error = %v, want state conflict", err)
		}
		clock.Advance(2 * time.Hour)
		scanEngineUntil(t, engine, task.ID, StateDone, 2)
		if got := countRows(t, engine.store, `SELECT COUNT(*) FROM task_events WHERE task_id=? AND kind='woke'`, task.ID); got != 1 {
			t.Fatalf("wake events = %d, want 1", got)
		}
		assertEngineInvariants(t, engine.store, task.ID)
	})

	t.Run("P3/joey_wait_ignores_clock_and_duplicate_answers", func(t *testing.T) {
		ask := json.RawMessage(`{"question":"Continue?","options":[{"id":"yes","label":"Yes"},{"id":"no","label":"No"}]}`)
		engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
			"1": {Result: ResultWait, Wait: &Wait{Kind: WaitJoey, Reason: "need Joey", Ask: ask}},
			"2": {Result: ResultDone},
		}})
		task := intakeTestTask(t, engine.store, "wait-joey", false)
		if err := engine.ScanOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		if runs, _ := engine.store.ListRuns(context.Background(), task.ID); len(runs) != 1 {
			t.Fatalf("Joey-wait setup did not dispatch: %+v", runs)
		}
		waiting := scanEngineUntil(t, engine, task.ID, StateWaiting, 1)
		if _, err := engine.WakeTime(context.Background(), task.ID, waiting.Version); !errors.Is(err, ErrStateConflict) {
			t.Fatalf("clock woke Joey wait: %v", err)
		}
		if _, err := engine.AnswerJoey(context.Background(), task.ID, waiting.Version, "maybe", ""); err == nil {
			t.Fatal("unoffered Joey answer was accepted")
		}
		answered, err := engine.AnswerJoey(context.Background(), task.ID, waiting.Version, "yes", "")
		if err != nil || answered.State != StateTodo || answered.WakeAt != nil || answered.WaitKind != "" {
			t.Fatalf("Joey answer transition = %+v, %v", answered, err)
		}
		if _, err := engine.AnswerJoey(context.Background(), task.ID, waiting.Version, "yes", ""); !errors.Is(err, ErrVersionConflict) {
			t.Fatalf("duplicate answer error = %v", err)
		}
		scanEngineUntil(t, engine, task.ID, StateDone, 2)
		if got := countRows(t, engine.store, `SELECT COUNT(*) FROM task_events WHERE task_id=? AND kind='answered'`, task.ID); got != 1 {
			t.Fatalf("answer events = %d, want 1", got)
		}
		assertEngineInvariants(t, engine.store, task.ID)
	})

	t.Run("P4/review_rejection_derives_cycle_and_ignores_late_result", func(t *testing.T) {
		engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
			"1": {Result: ResultDone},
			"2": {Result: ResultRejected},
			"3": {Result: ResultDone},
			"4": {Result: ResultAccepted},
		}})
		task := intakeTestTask(t, engine.store, "review-rejection", true)
		if err := engine.ScanOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		if runs, _ := engine.store.ListRuns(context.Background(), task.ID); len(runs) != 1 {
			t.Fatalf("review setup did not dispatch: %+v", runs)
		}
		scanEngineUntil(t, engine, task.ID, StateDone, 4)
		runs, _ := engine.store.ListRuns(context.Background(), task.ID)
		accounting, err := engine.store.DerivedAccounting(context.Background(), task.ID, PhaseReview)
		if err != nil || accounting.Attempts != 2 || accounting.ReviewCycles != 1 {
			t.Fatalf("derived review accounting = %+v, %v; runs=%+v", accounting, err, runs)
		}
		late := validTestResult(runs[0], ResultFailed)
		writeEngineRunResult(t, runs[0], *late)
		beforeEvents := countRows(t, engine.store, `SELECT COUNT(*) FROM task_events`)
		if err := engine.ScanOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		current, _ := engine.store.GetTask(context.Background(), task.ID)
		if current.State != StateDone || countRows(t, engine.store, `SELECT COUNT(*) FROM task_events`) != beforeEvents {
			t.Fatalf("late implementation result mutated settled review workflow: %+v", current)
		}
		assertEngineInvariants(t, engine.store, task.ID)
	})
}
