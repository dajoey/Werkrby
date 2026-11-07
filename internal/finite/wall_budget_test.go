package finite

import (
	"context"
	"testing"
	"time"
)

// A provider-side wall (Z.ai "Insufficient balance" HTTP 429, a quota
// reset) spends nothing: it must not consume the run budget. Live
// 2026-11-06: a 12.5 h balance outage walled every queued task once an
// hour; each wall counted as a run, so tasks failed at max_runs for a
// provider outage that was not their fault and had to be retried by
// hand. Walls are bounded instead by a wall streak that must outlast
// wallStreakBudget before the task fails.

func TestFiniteWallsDoNotSpendRunBudget(t *testing.T) {
	start := time.Date(2026, 11, 6, 3, 0, 0, 0, time.UTC)
	s := newTestStore(t)
	s.now = func() time.Time { return start }
	ctx := context.Background()
	task, err := s.IntakeBrief(ctx, writeCappedBrief(t, "wall-budget", 3))
	if err != nil {
		t.Fatal(err)
	}
	var settled Task
	for i := 0; i < 5; i++ {
		current, err := s.GetTask(ctx, task.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.State == StateWaiting {
			s.now = func() time.Time { return start.Add(time.Duration(i+1) * time.Hour) }
			if current, err = s.WakeTime(ctx, task.ID, current.Version); err != nil {
				t.Fatal(err)
			}
		}
		run := dispatchTestRun(t, s, current, PhaseImplementation)
		wake := start.Add(time.Duration(i+2) * time.Hour)
		settled, err = s.SettleRun(ctx, SettlementRequest{RunID: run.ID, ExpectedTaskVersion: current.Version + 1,
			ExecutionOutcome: RunWall, Error: "HTTP 429: Insufficient balance or no resource package. Please recharge.", WallWakeAt: &wake})
		if err != nil {
			t.Fatal(err)
		}
		if settled.State == StateFailed {
			t.Fatalf("wall %d failed the task: a wall is not a spent run", i+1)
		}
	}
	accounting, err := s.DerivedAccounting(ctx, task.ID, PhaseImplementation)
	if err != nil || accounting.Runs != 0 || accounting.Attempts != 0 || accounting.Failures != 0 {
		t.Fatalf("accounting = %+v, %v; walls must spend nothing", accounting, err)
	}
}

func TestFiniteWallStreakPastBudgetFailsTask(t *testing.T) {
	start := time.Date(2026, 11, 6, 3, 0, 0, 0, time.UTC)
	s := newTestStore(t)
	s.now = func() time.Time { return start }
	ctx := context.Background()
	task, err := s.IntakeBrief(ctx, writeCappedBrief(t, "wall-span", 3))
	if err != nil {
		t.Fatal(err)
	}
	var settled Task
	for i := 0; i < 3; i++ {
		current, err := s.GetTask(ctx, task.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.State == StateWaiting {
			// one probe ladder step per wall, then the whole budget at once
			step := time.Duration(i+1) * time.Hour
			if i == 2 {
				step = wallStreakBudget + time.Hour
			}
			s.now = func() time.Time { return start.Add(step) }
			if current, err = s.WakeTime(ctx, task.ID, current.Version); err != nil {
				t.Fatal(err)
			}
		}
		run := dispatchTestRun(t, s, current, PhaseImplementation)
		wake := s.now().Add(time.Hour)
		settled, err = s.SettleRun(ctx, SettlementRequest{RunID: run.ID, ExpectedTaskVersion: current.Version + 1,
			ExecutionOutcome: RunWall, Error: "HTTP 429: Insufficient balance or no resource package. Please recharge.", WallWakeAt: &wake})
		if err != nil {
			t.Fatal(err)
		}
	}
	if settled.State != StateFailed {
		t.Fatalf("a wall streak outlasting the budget leaves the task %s, want failed", settled.State)
	}
	assertFailureReason(t, s, task.ID, "wall_span")
}
