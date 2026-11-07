package finite

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// MaxRuns bounds every charged run of a phase. Lost and interrupted runs
// are not charged attempts, but each one is a run, so a cause that repeats
// forever still ends in a failed task. A provider wall spends nothing and
// is bounded by wallStreakBudget instead (wall_budget_test.go).

func TestFiniteRepeatedLaunchFailureStopsAtMaxRuns(t *testing.T) {
	engine := newEngineFixture(t, engineFixturePlan{})
	engine.opts.Executable = filepath.Join(t.TempDir(), "missing-werkrby-finite-supervisor")
	ctx := context.Background()
	task, err := engine.store.IntakeBrief(ctx, writeTestBrief(t, "launch-loop", false))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3*task.MaxRuns; i++ {
		_ = engine.ScanOnce(ctx)
	}
	runs, err := engine.store.ListRuns(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	current, err := engine.store.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != task.MaxRuns || current.State != StateFailed {
		t.Fatalf("launch failure loop: runs=%d state=%s, want %d runs and failed", len(runs), current.State, task.MaxRuns)
	}
	for _, run := range runs {
		if run.EndedAt == nil || run.Outcome != RunLost {
			t.Fatalf("launch-failed run = %+v, want settled lost", run)
		}
	}
	accounting, err := engine.store.DerivedAccounting(ctx, task.ID, PhaseImplementation)
	if err != nil || accounting.Runs != task.MaxRuns || accounting.Attempts != 0 {
		t.Fatalf("accounting = %+v, %v; want %d uncharged runs", accounting, err, task.MaxRuns)
	}
	assertFailureReason(t, engine.store, task.ID, "max_runs")
	assertEngineInvariants(t, engine.store, task.ID)
}

func writeCappedBrief(t *testing.T, id string, maxRuns int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), id+".md")
	data := fmt.Sprintf("---\nid: %s\ntitle: Capped %s\nrisk: low\nreview_required: false\nmax_runs: %d\n---\n\nImplement one finite objective.\n", id, id, maxRuns)
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestFiniteUnchargedOutcomesCountTowardMaxRuns(t *testing.T) {
	wake := time.Date(2026, 11, 1, 13, 0, 0, 0, time.UTC)
	for _, outcome := range []RunOutcome{RunLost, RunInterrupted} {
		t.Run(string(outcome), func(t *testing.T) {
			s := newTestStore(t)
			ctx := context.Background()
			task, err := s.IntakeBrief(ctx, writeCappedBrief(t, "cap-"+string(outcome), 2))
			if err != nil {
				t.Fatal(err)
			}
			var settled Task
			for i := 0; i < 2; i++ {
				current, err := s.GetTask(ctx, task.ID)
				if err != nil {
					t.Fatal(err)
				}
				if current.State == StateWaiting {
					s.now = func() time.Time { return wake }
					if current, err = s.WakeTime(ctx, task.ID, current.Version); err != nil {
						t.Fatal(err)
					}
				}
				run := dispatchTestRun(t, s, current, PhaseImplementation)
				request := SettlementRequest{RunID: run.ID, ExpectedTaskVersion: current.Version + 1, ExecutionOutcome: outcome, Error: "uncharged " + string(outcome)}
				if outcome == RunWall {
					request.WallWakeAt = &wake
				}
				if settled, err = s.SettleRun(ctx, request); err != nil {
					t.Fatal(err)
				}
				if i == 0 && settled.State == StateFailed {
					t.Fatalf("first %s run already failed the task", outcome)
				}
			}
			if settled.State != StateFailed {
				t.Fatalf("after MaxRuns %s runs task is %s, want failed", outcome, settled.State)
			}
			accounting, err := s.DerivedAccounting(ctx, task.ID, PhaseImplementation)
			if err != nil || accounting.Runs != 2 || accounting.Attempts != 0 || accounting.Failures != 0 {
				t.Fatalf("accounting = %+v, %v; want 2 uncharged runs", accounting, err)
			}
			if got := countRows(t, s, `SELECT COUNT(*) FROM outbox WHERE kind='task_terminal'`); got != 1 {
				t.Fatalf("terminal outbox rows = %d, want 1", got)
			}
			assertFailureReason(t, s, task.ID, "max_runs")
		})
	}
}

func TestFiniteAdmissionFailsWhenUnchargedRunsReachMaxRuns(t *testing.T) {
	task := Task{State: StateTodo, MaxRuns: 2, MaxFailures: 3, MaxReviewCycles: 3}
	decision := Admit(AdmissionInput{Task: task, Phase: PhaseImplementation, Accounting: Accounting{Runs: 2},
		Seats: []Seat{{Name: "seat-a", Available: true}}})
	if decision.Action != AdmissionFail || decision.Reason != "max_runs" {
		t.Fatalf("admission with %d uncharged runs = %+v, want max_runs failure", 2, decision)
	}
}

// assertFailureReason requires the failing transition to record which cap
// ended the task, whether settlement or admission made the transition.
func assertFailureReason(t *testing.T, s *Store, taskID, reason string) {
	t.Helper()
	rows, err := s.db.Query(`SELECT payload_json FROM task_events WHERE task_id=? AND to_state='failed'`, taskID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var payloads []string
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			t.Fatal(err)
		}
		payloads = append(payloads, payload)
		var decoded map[string]any
		if json.Unmarshal([]byte(payload), &decoded) == nil && (decoded["reason"] == reason || decoded["failure_reason"] == reason) {
			return
		}
	}
	t.Fatalf("no failed transition records reason %q: %v", reason, payloads)
}
