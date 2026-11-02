package finite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestFiniteAdmissionFallbackWalk(t *testing.T) {
	reset := time.Date(2026, 11, 1, 1, 0, 0, 0, time.UTC)
	later := time.Date(2026, 11, 1, 2, 0, 0, 0, time.UTC)
	task := Task{ID: "task-walk", State: StateTodo, MaxRuns: 12, MaxFailures: 3, MaxReviewCycles: 3}
	base := AdmissionInput{Task: task, Phase: PhaseImplementation, RosterSeats: true, StartSeat: "seat-a"}

	t.Run("available_start_seat_is_chosen_not_the_sort_first_outsider", func(t *testing.T) {
		in := base
		in.Seats = []Seat{
			{Name: "seat-a", Available: true, Fallback: "seat-b"},
			{Name: "seat-b", Available: true},
			{Name: "aaa-outsider", Available: true},
		}
		decision := Admit(in)
		if decision.Action != AdmissionDispatch || decision.Seat != "seat-a" {
			t.Fatalf("walk admission = %+v, want dispatch on the start seat", decision)
		}
	})

	t.Run("fallback_and_two_hop_chain_are_chosen", func(t *testing.T) {
		in := base
		in.Seats = []Seat{
			{Name: "seat-a", Available: false, QuotaReset: &reset, Fallback: "seat-b"},
			{Name: "seat-b", Available: true, Fallback: "seat-c"},
			{Name: "seat-c", Available: true},
			{Name: "aaa-outsider", Available: true},
		}
		if decision := Admit(in); decision.Action != AdmissionDispatch || decision.Seat != "seat-b" {
			t.Fatalf("one-hop admission = %+v, want seat-b", decision)
		}
		in.Seats[1].Available = false
		in.Seats[1].QuotaReset = &later
		if decision := Admit(in); decision.Action != AdmissionDispatch || decision.Seat != "seat-c" {
			t.Fatalf("two-hop admission = %+v, want seat-c", decision)
		}
	})

	t.Run("exhausted_chain_waits_and_never_leaves_it", func(t *testing.T) {
		in := base
		in.Seats = []Seat{
			{Name: "seat-a", Available: false, QuotaReset: &later, Fallback: "seat-b"},
			{Name: "seat-b", Available: false, QuotaReset: &reset},
			{Name: "aaa-outsider", Available: true},
		}
		decision := Admit(in)
		if decision.Action != AdmissionWait || decision.Seat != "" {
			t.Fatalf("exhausted chain admission = %+v, want a wait on no seat", decision)
		}
		if decision.WakeAt == nil || !decision.WakeAt.Equal(reset) {
			t.Fatalf("exhausted chain wake = %+v, want the earliest chain reset", decision)
		}
	})

	t.Run("cycle_and_over_long_chain_stop_and_wait", func(t *testing.T) {
		in := base
		in.Seats = []Seat{
			{Name: "seat-a", Available: false, Fallback: "seat-b"},
			{Name: "seat-b", Available: false, Fallback: "seat-a"},
		}
		decision := Admit(in)
		if decision.Action != AdmissionWait {
			t.Fatalf("cyclic chain admission = %+v, want a wait", decision)
		}
		chain := []Seat{{Name: "seat-a", Available: false, Fallback: "seat-01"}}
		for hop := 1; hop <= 10; hop++ {
			chain = append(chain, Seat{Name: fmt.Sprintf("seat-%02d", hop),
				Available: hop == 9, Fallback: fmt.Sprintf("seat-%02d", hop+1)})
		}
		chain[10].Fallback = ""
		in.Seats = chain
		decision = Admit(in)
		if decision.Action != AdmissionWait {
			t.Fatalf("over-long chain admission = %+v, want the walk to stop at 8 hops", decision)
		}
		exact := append([]Seat(nil), chain[:9]...) // exactly 9 seats: 8 hops reach seat-08
		exact[8].Available = true
		in.Seats = exact
		if decision = Admit(in); decision.Action != AdmissionDispatch || decision.Seat != "seat-08" {
			t.Fatalf("eight-hop chain admission = %+v, want dispatch on the ninth seat", decision)
		}
	})

	t.Run("start_seat_missing_from_roster_fails_the_task", func(t *testing.T) {
		in := base
		in.StartSeat = "seat-ghost"
		in.Seats = []Seat{{Name: "seat-a", Available: true}}
		decision := Admit(in)
		if decision.Action != AdmissionFail || decision.Reason != "staff_not_in_roster" {
			t.Fatalf("missing start seat admission = %+v, want staff_not_in_roster failure", decision)
		}
	})

	t.Run("slot_list_behaviour_is_unchanged", func(t *testing.T) {
		in := base
		in.RosterSeats = false
		in.StartSeat = "seat-a" // not in the slot list
		in.Seats = []Seat{
			{Name: "slot-a", Available: false, QuotaReset: &reset},
			{Name: "slot-b", Available: true},
		}
		decision := Admit(in)
		if decision.Action != AdmissionDispatch || decision.Seat != "slot-b" {
			t.Fatalf("slot admission = %+v, want the sorted first available slot", decision)
		}
	})

	t.Run("walled_pool_benches_even_a_roster_available_seat", func(t *testing.T) {
		in := base
		in.WalledPools = map[string]struct{}{"zai": {}}
		in.Seats = []Seat{
			{Name: "seat-a", Available: true, Pool: "zai", Fallback: "seat-b"},
			{Name: "seat-b", Available: true, Pool: "zai"},
		}
		if decision := Admit(in); decision.Action != AdmissionWait {
			t.Fatalf("same-pool fallback admission = %+v, want a wait", decision)
		}
		in.Seats[1].Pool = "venice"
		if decision := Admit(in); decision.Action != AdmissionDispatch || decision.Seat != "seat-b" {
			t.Fatalf("cross-pool fallback admission = %+v, want dispatch on seat-b", decision)
		}
	})
}

func TestFiniteExpireSeats(t *testing.T) {
	now := time.Date(2026, 11, 2, 1, 0, 0, 0, time.UTC)
	past := now.Add(-time.Minute)
	exact := now
	future := now.Add(15 * time.Minute)
	reset := now.Add(time.Hour)
	pointer := func(value time.Time) *time.Time { return &value }

	t.Run("expired_seat_comes_back_unavailable", func(t *testing.T) {
		got := expireSeats([]Seat{{Name: "seat-bal", Available: true, Pool: "zai_balance", AvailableUntil: pointer(past)}}, now)
		if got[0].Available {
			t.Fatalf("past expiry seat = %+v, want unavailable", got[0])
		}
		if got[0].AvailableUntil == nil || !got[0].AvailableUntil.Equal(past) {
			t.Fatalf("past expiry seat = %+v, want the reading kept", got[0])
		}
	})

	t.Run("expiry_at_the_instant_closes_too", func(t *testing.T) {
		got := expireSeats([]Seat{{Name: "seat-bal", Available: true, AvailableUntil: pointer(exact)}}, now)
		if got[0].Available {
			t.Fatalf("instant expiry seat = %+v, want unavailable", got[0])
		}
	})

	t.Run("later_expiry_and_no_expiry_are_unchanged", func(t *testing.T) {
		got := expireSeats([]Seat{
			{Name: "seat-later", Available: true, AvailableUntil: pointer(future)},
			{Name: "seat-plain", Available: true},
		}, now)
		if !got[0].Available || !got[1].Available {
			t.Fatalf("unexpired seats = %+v, want both available", got)
		}
	})

	t.Run("unavailable_seat_stays_unavailable", func(t *testing.T) {
		got := expireSeats([]Seat{
			{Name: "seat-closed", Available: false, AvailableUntil: pointer(future)},
			{Name: "seat-closed-expired", Available: false, AvailableUntil: pointer(past)},
		}, now)
		if got[0].Available || got[1].Available {
			t.Fatalf("closed seats = %+v, want both unavailable", got)
		}
	})

	t.Run("reset_fallback_and_pool_are_never_touched", func(t *testing.T) {
		got := expireSeats([]Seat{{
			Name: "seat-bal", Available: true, QuotaReset: pointer(reset),
			Fallback: "seat-next", Pool: "zai_balance", AvailableUntil: pointer(past),
		}}, now)
		if got[0].QuotaReset == nil || !got[0].QuotaReset.Equal(reset) || got[0].Fallback != "seat-next" || got[0].Pool != "zai_balance" {
			t.Fatalf("expired seat = %+v, want reset, fallback and pool kept", got[0])
		}
	})

	t.Run("input_slice_is_not_modified", func(t *testing.T) {
		seats := []Seat{
			{Name: "seat-bal", Available: true, AvailableUntil: pointer(past)},
			{Name: "seat-plain", Available: true},
		}
		want := append([]Seat(nil), seats...)
		_ = expireSeats(seats, now)
		if !reflect.DeepEqual(seats, want) {
			t.Fatalf("input seats = %+v, want the caller slice untouched (was %+v)", seats, want)
		}
	})
}

func TestFiniteAdmissionWaitCancelAndVersionFences(t *testing.T) {
	reset := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	task := Task{ID: "task-admit", State: StateTodo, MaxRuns: 12, MaxFailures: 3, MaxReviewCycles: 3}
	decision := Admit(AdmissionInput{Task: task, Phase: PhaseImplementation, Seats: []Seat{{Name: "seat-b", Available: true}, {Name: "seat-a", Available: true}}})
	if decision.Action != AdmissionDispatch || decision.Seat != "seat-a" {
		t.Errorf("admission = %+v", decision)
	}
	cancelled := task
	cancelled.CancelRequested = true
	if got := Admit(AdmissionInput{Task: cancelled, Phase: PhaseImplementation, Seats: []Seat{{Name: "seat-a", Available: true}}}); got.Action != AdmissionWait {
		t.Errorf("stop-pending admission = %+v", got)
	}
	if got := Admit(AdmissionInput{Task: task, Phase: PhaseImplementation, Accounting: Accounting{Attempts: 12}, Seats: []Seat{{Name: "seat-a", Available: true}}}); got.Action != AdmissionFail {
		t.Errorf("cap admission = %+v", got)
	}
	waiting := Admit(AdmissionInput{Task: task, Phase: PhaseImplementation, Seats: []Seat{{Name: "seat-b", QuotaReset: &reset}, {Name: "seat-a"}}})
	if waiting.Action != AdmissionWait || waiting.WakeAt == nil || !waiting.WakeAt.Equal(reset) {
		t.Errorf("quota admission = %+v", waiting)
	}

	t.Run("time_wait_wake_is_atomic_and_joey_ignores_clock", func(t *testing.T) {
		s := newTestStore(t)
		task := intakeTestTask(t, s, "task-wait", false)
		run := dispatchTestRun(t, s, task, PhaseImplementation)
		result := validTestResult(run, ResultWait)
		wake := s.now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
		result.Wait = &Wait{Kind: WaitTime, WakeAt: wake, Reason: "Deployment window"}
		waitingTask, err := s.SettleRun(context.Background(), SettlementRequest{RunID: run.ID, ExpectedTaskVersion: task.Version + 1, ExecutionOutcome: RunOK, Result: result})
		if err != nil {
			t.Fatal(err)
		}
		if waitingTask.State != StateWaiting || waitingTask.ResumeState != StateTodo || waitingTask.WakeAt == nil {
			t.Errorf("wait settlement = %+v", waitingTask)
			return
		}
		if _, err := s.WakeTime(context.Background(), task.ID, waitingTask.Version); !errors.Is(err, ErrStateConflict) {
			t.Errorf("early wake error = %v", err)
		}
		s.now = func() time.Time { return time.Date(2026, 10, 31, 13, 0, 0, 0, time.UTC) }
		awake, err := s.WakeTime(context.Background(), task.ID, waitingTask.Version)
		if err != nil || awake.State != StateTodo || awake.WaitKind != "" || awake.WakeAt != nil {
			t.Errorf("wake = %+v, %v", awake, err)
			return
		}
		if _, err := s.WakeTime(context.Background(), task.ID, waitingTask.Version); !errors.Is(err, ErrVersionConflict) {
			t.Errorf("duplicate wake error = %v", err)
		}

		joeyTask := intakeTestTask(t, s, "task-joey", false)
		joeyRun := dispatchTestRun(t, s, joeyTask, PhaseImplementation)
		joeyResult := validTestResult(joeyRun, ResultWait)
		joeyResult.Wait = &Wait{Kind: WaitJoey, Reason: "Need a decision", Ask: json.RawMessage(`{"question":"Continue?","options":[{"id":"yes","label":"Yes"},{"id":"no","label":"No"}]}`)}
		joeyWaiting, err := s.SettleRun(context.Background(), SettlementRequest{RunID: joeyRun.ID, ExpectedTaskVersion: joeyTask.Version + 1, ExecutionOutcome: RunOK, Result: joeyResult})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.WakeTime(context.Background(), joeyTask.ID, joeyWaiting.Version); !errors.Is(err, ErrStateConflict) {
			t.Errorf("clock woke Joey wait: %v", err)
		}
		if _, err := s.AnswerJoey(context.Background(), joeyTask.ID, joeyWaiting.Version, "invalid"); err == nil {
			t.Errorf("unknown Joey answer was accepted")
		}
		answered, err := s.AnswerJoey(context.Background(), joeyTask.ID, joeyWaiting.Version, "yes")
		if err != nil || answered.State != StateTodo || answered.JoeyAskJSON != "" {
			t.Errorf("Joey answer = %+v, %v", answered, err)
		}
	})

	t.Run("stop_fence_beats_valid_result_and_charges_nothing", func(t *testing.T) {
		s := newTestStore(t)
		task := intakeTestTask(t, s, "task-stop", false)
		run := dispatchTestRun(t, s, task, PhaseImplementation)
		stopped, err := s.RequestStop(context.Background(), task.ID, task.Version+1)
		if err != nil || !stopped.CancelRequested || stopped.State != StateWorking {
			t.Errorf("stop fence = %+v, %v", stopped, err)
			return
		}
		settled, err := s.SettleRun(context.Background(), SettlementRequest{
			RunID: run.ID, ExpectedTaskVersion: stopped.Version, ExecutionOutcome: RunOK,
			Result: validTestResult(run, ResultDone),
		})
		if err != nil || settled.State != StateCancelled {
			t.Errorf("stop settlement = %+v, %v", settled, err)
			return
		}
		persisted, _ := s.GetRun(context.Background(), run.ID)
		accounting, _ := s.DerivedAccounting(context.Background(), task.ID, PhaseImplementation)
		if persisted.Outcome != RunInterrupted || accounting.Attempts != 0 {
			t.Errorf("stop accounting run=%+v accounting=%+v", persisted, accounting)
		}
	})

	t.Run("dispatch_rechecks_stale_version_and_open_run", func(t *testing.T) {
		s := newTestStore(t)
		task := intakeTestTask(t, s, "task-dispatch-fence", false)
		_ = dispatchTestRun(t, s, task, PhaseImplementation)
		called := false
		_, err := s.Dispatch(context.Background(), DispatchRequest{TaskID: task.ID, ExpectedVersion: task.Version, Phase: PhaseImplementation, Seat: "seat", SessionKey: "finite:fence", LaunchReason: "retry"}, func(context.Context, Run) error {
			called = true
			return nil
		})
		if !errors.Is(err, ErrVersionConflict) || called {
			t.Errorf("stale dispatch error=%v launched=%t", err, called)
		}
	})
}
