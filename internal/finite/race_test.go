package finite

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestFiniteConcurrentClaimWakeStopResult(t *testing.T) {
	for _, kind := range []WaitKind{WaitTime, WaitJoey} {
		t.Run(string(kind), func(t *testing.T) {
			clock := newEngineTestClock()
			wait := &Wait{Kind: kind, Reason: "race wait"}
			if kind == WaitTime {
				wait.WakeAt = clock.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
			} else {
				wait.Ask = json.RawMessage(`{"question":"Continue?","options":[{"id":"yes","label":"Yes"}]}`)
			}
			engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
				"1": {Result: ResultWait, Wait: wait},
				"2": {Result: ResultDone, Block: true},
			}})
			engine.store.now = clock.Now
			task := intakeTestTask(t, engine.store, "race-"+string(kind), false)
			if err := engine.ScanOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			if runs, _ := engine.store.ListRuns(context.Background(), task.ID); len(runs) != 1 {
				t.Fatalf("race setup did not commit exactly one claim: %+v", runs)
			}
			scanEngineUntil(t, engine, task.ID, StateWaiting, 1)
			if kind == WaitTime {
				clock.Advance(2 * time.Hour)
			}

			var workers sync.WaitGroup
			for worker := 0; worker < 4; worker++ {
				workers.Add(1)
				go func(seed int64) {
					defer workers.Done()
					random := rand.New(rand.NewSource(seed))
					for range 60 {
						_ = engine.ScanOnce(context.Background())
						time.Sleep(time.Duration(random.Intn(3)) * time.Millisecond)
					}
				}(int64(worker + 1))
			}
			for worker := 0; worker < 2; worker++ {
				workers.Add(1)
				go func(seed int64) {
					defer workers.Done()
					random := rand.New(rand.NewSource(seed))
					for range 40 {
						current, err := engine.store.GetTask(context.Background(), task.ID)
						if err == nil && current.State == StateWaiting {
							if random.Intn(2) == 0 {
								_, _ = engine.WakeTime(context.Background(), task.ID, current.Version)
							} else {
								_, _ = engine.AnswerJoey(context.Background(), task.ID, current.Version, "yes", "")
							}
						}
						time.Sleep(time.Duration(random.Intn(3)) * time.Millisecond)
					}
				}(int64(worker + 20))
			}
			workers.Add(1)
			go func() {
				defer workers.Done()
				for range 2000 {
					runs, _ := engine.store.ListRuns(context.Background(), task.ID)
					if len(runs) >= 2 && runs[1].StartedAt != nil {
						break
					}
					current, err := engine.store.GetTask(context.Background(), task.ID)
					if err != nil || terminalState(current.State) {
						return
					}
					time.Sleep(5 * time.Millisecond)
				}
				for range 20 {
					current, err := engine.store.GetTask(context.Background(), task.ID)
					if err == nil && !terminalState(current.State) {
						_, stopErr := engine.RequestStop(context.Background(), task.ID, current.Version)
						if stopErr == nil || !errors.Is(stopErr, ErrVersionConflict) {
							return
						}
					}
					time.Sleep(2 * time.Millisecond)
				}
			}()
			workers.Wait()

			for range 20 {
				current, err := engine.store.GetTask(context.Background(), task.ID)
				if err == nil && current.State == StateCancelled {
					break
				}
				if err == nil && !terminalState(current.State) {
					_, _ = engine.RequestStop(context.Background(), task.ID, current.Version)
				}
				_ = engine.ScanOnce(context.Background())
				time.Sleep(5 * time.Millisecond)
			}
			cancelled := scanEngineUntil(t, engine, task.ID, StateCancelled, 0)
			if !cancelled.CancelRequested {
				t.Fatalf("terminal cancellation lost durable stop fence: %+v", cancelled)
			}
			runs, err := engine.store.ListRuns(context.Background(), task.ID)
			if err != nil || len(runs) < 1 || len(runs) > 2 {
				t.Fatalf("concurrent claims produced %d runs: %+v", len(runs), runs)
			}
			for _, run := range runs {
				if run.EndedAt == nil || run.Outcome == "" {
					t.Fatalf("terminal task retained live ownership: %+v", run)
				}
			}
			if got := countRows(t, engine.store, `SELECT COUNT(*) FROM task_events WHERE task_id=? AND kind='settled'`, task.ID); got != len(runs) {
				t.Fatalf("settled events = %d, runs = %d", got, len(runs))
			}
			if got := countRows(t, engine.store, `SELECT COUNT(*) FROM outbox`); got != 1 {
				t.Fatalf("terminal outbox rows = %d, want one", got)
			}
			assertEngineInvariants(t, engine.store, task.ID)
		})
	}
}
