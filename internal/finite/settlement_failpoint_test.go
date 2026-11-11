package finite

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestFiniteSettlementFailpointsAtomicityAndReplay(t *testing.T) {
	points := []SettlementFailpoint{
		SettlementAfterRunResultsInsert,
		SettlementAfterRunUpdate,
		SettlementAfterUsageInsert,
		SettlementAfterTaskUpdate,
		SettlementAfterSettlementEventInsert,
		SettlementAfterOutboxInsert,
		SettlementBeforeCommit,
		SettlementAfterCommit,
	}
	for _, point := range points {
		t.Run(string(point), func(t *testing.T) {
			s := newTestStore(t)
			task := intakeTestTask(t, s, "settlement-"+strings.ReplaceAll(string(point), "_", "-"), false)
			run := dispatchTestRun(t, s, task, PhaseImplementation)
			fresh := int64(11)
			usage := Usage{FreshInputTokens: &fresh, Source: "settlement-failpoint-test", CollectedAt: s.now()}
			req := SettlementRequest{
				RunID: run.ID, ExpectedTaskVersion: task.Version + 1,
				ExecutionOutcome: RunOK, Result: validTestResult(run, ResultDone), Usage: &usage,
			}
			sentinel := fmt.Errorf("failpoint reached: %s", point)
			reached := false
			restore := registerSettlementFailpoint(point, func() error {
				reached = true
				return sentinel
			})
			_, err := s.SettleRun(context.Background(), req)
			restore()
			if !reached || !errors.Is(err, sentinel) {
				t.Errorf("failpoint %s was not reached: reached=%t err=%v", point, reached, err)
				return
			}

			committed := point == SettlementAfterCommit
			current, getErr := s.GetTask(context.Background(), task.ID)
			if getErr != nil {
				t.Fatal(getErr)
			}
			persistedRun, getErr := s.GetRun(context.Background(), run.ID)
			if getErr != nil {
				t.Fatal(getErr)
			}
			if !committed {
				if current.State != StateWorking || current.Version != task.Version+1 || persistedRun.EndedAt != nil {
					t.Fatalf("pre-commit failpoint partially settled: task=%+v run=%+v", current, persistedRun)
				}
				assertSettlementWriteCounts(t, s, run.ID, 0)
				if _, err := s.SettleRun(context.Background(), req); err != nil {
					t.Fatalf("settlement after rollback: %v", err)
				}
			} else {
				if current.State != StateDone || current.Version != task.Version+2 || persistedRun.Outcome != RunOK {
					t.Fatalf("post-commit failpoint lost durable settlement: task=%+v run=%+v", current, persistedRun)
				}
				assertSettlementWriteCounts(t, s, run.ID, 1)
			}

			eventsBefore := countRows(t, s, `SELECT COUNT(*) FROM task_events`)
			outboxBefore := countRows(t, s, `SELECT COUNT(*) FROM outbox`)
			settled, err := s.SettleRun(context.Background(), req)
			if err != nil {
				t.Fatalf("identical settlement replay: %v", err)
			}
			if settled.State != StateDone {
				t.Fatalf("replay changed terminal state: %+v", settled)
			}
			assertSettlementWriteCounts(t, s, run.ID, 1)
			if got := countRows(t, s, `SELECT COUNT(*) FROM task_events`); got != eventsBefore {
				t.Errorf("replay added events: before=%d after=%d", eventsBefore, got)
			}
			if got := countRows(t, s, `SELECT COUNT(*) FROM outbox`); got != outboxBefore {
				t.Errorf("replay added outbox rows: before=%d after=%d", outboxBefore, got)
			}
			if err := s.CheckInvariants(context.Background()); err != nil {
				t.Errorf("invariants after failpoint replay: %v", err)
			}
		})
	}
}

func assertSettlementWriteCounts(t *testing.T, s *Store, runID int64, want int) {
	t.Helper()
	queries := map[string]string{
		"results":     `SELECT COUNT(*) FROM run_results WHERE run_id=?`,
		"usage":       `SELECT COUNT(*) FROM run_usage WHERE run_id=?`,
		"settlements": `SELECT COUNT(*) FROM task_events WHERE run_id=? AND kind='settled'`,
	}
	for name, query := range queries {
		if got := countRows(t, s, query, runID); got != want {
			t.Errorf("%s rows = %d, want %d", name, got, want)
		}
	}
	if got := countRows(t, s, `SELECT COUNT(*) FROM outbox`); got != want {
		t.Errorf("outbox rows = %d, want %d", got, want)
	}
}
