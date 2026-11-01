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

// writeFallbackBrief writes a brief whose front matter already carries the
// extra lines (staff, fallbacks, review staff, review fallbacks).
func writeFallbackBrief(t *testing.T, id string, reviewRequired bool, extraFrontMatter string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), id+".md")
	data := fmt.Sprintf("---\nid: %s\ntitle: Fallback task %s\nrisk: low\nreview_required: %t\n%s---\n\nImplement one finite objective.\n", id, id, reviewRequired, extraFrontMatter)
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatalf("write brief: %v", err)
	}
	return path
}

const fallbackBriefFrontMatter = `staff: finite-a
fallbacks:
  - seat: finite-b
review_staff: finite-r
review_fallbacks:
  - seat: finite-r2
`

func TestFiniteBriefParsesFallbackChains(t *testing.T) {
	data, err := os.ReadFile(writeFallbackBrief(t, "brief-fallbacks", true, fallbackBriefFrontMatter))
	if err != nil {
		t.Fatal(err)
	}
	brief, err := ParseBrief(data)
	if err != nil {
		t.Fatalf("brief with fallback chains was refused: %v", err)
	}
	if brief.Staff != "finite-a" || brief.ReviewStaff != "finite-r" {
		t.Fatalf("brief staff = %q / %q", brief.Staff, brief.ReviewStaff)
	}
	if len(brief.Fallbacks) != 1 || brief.Fallbacks[0].Seat != "finite-b" || brief.Fallbacks[0].Gate != "" {
		t.Fatalf("implementation fallbacks = %+v", brief.Fallbacks)
	}
	if len(brief.ReviewFallbacks) != 1 || brief.ReviewFallbacks[0].Seat != "finite-r2" {
		t.Fatalf("review fallbacks = %+v", brief.ReviewFallbacks)
	}
	if chain := brief.PhaseChain(PhaseImplementation); len(chain) != 2 || chain[0].Seat != "finite-a" || chain[1].Seat != "finite-b" {
		t.Fatalf("implementation chain = %+v", chain)
	}
	if chain := brief.PhaseChain(PhaseReview); len(chain) != 2 || chain[0].Seat != "finite-r" || chain[1].Seat != "finite-r2" {
		t.Fatalf("review chain = %+v", chain)
	}
}

func TestFiniteBriefFallbackValidation(t *testing.T) {
	cases := []struct {
		name  string
		extra string
	}{
		{"fallbacks_without_staff", "fallbacks:\n  - seat: finite-b\n"},
		{"review_fallbacks_without_review_staff", "staff: finite-a\nreview_fallbacks:\n  - seat: finite-b\n"},
		{"duplicate_rung_seat", "staff: finite-a\nfallbacks:\n  - seat: finite-b\n  - seat: finite-b\n"},
		{"rung_repeats_primary", "staff: finite-a\nfallbacks:\n  - seat: finite-a\n"},
		{"unknown_gate", "staff: finite-a\nfallbacks:\n  - seat: finite-b\n    gate: sugar\n"},
		{"invalid_rung_seat", "staff: finite-a\nfallbacks:\n  - seat: not a seat\n"},
		{"chain_too_long", "staff: finite-a\nfallbacks:\n  - seat: finite-b1\n  - seat: finite-b2\n  - seat: finite-b3\n  - seat: finite-b4\n  - seat: finite-b5\n  - seat: finite-b6\n  - seat: finite-b7\n  - seat: finite-b8\n  - seat: finite-b9\n"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			data, err := os.ReadFile(writeFallbackBrief(t, "brief-invalid-"+testCase.name, false, testCase.extra))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ParseBrief(data); err == nil {
				t.Fatalf("invalid brief front matter was accepted: %s", testCase.extra)
			}
		})
	}
}

func TestFiniteChooseSeatRung(t *testing.T) {
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	chain := []SeatRung{{Seat: "finite-a"}, {Seat: "finite-b"}}
	balanceChain := []SeatRung{{Seat: "finite-a"}, {Seat: "finite-bal", Gate: SeatGateBalance}}
	paidChain := []SeatRung{{Seat: "finite-a"}, {Seat: "finite-paid", Gate: SeatGatePaidOK}}

	index, earliest := chooseSeatRung(chain, nil, now)
	if index != 0 || earliest != nil {
		t.Fatalf("no walls: index=%d earliest=%v", index, earliest)
	}

	index, _ = chooseSeatRung(chain, []SeatWall{{Seat: "finite-a", WakeAt: now.Add(time.Hour)}}, now)
	if index != 1 {
		t.Fatalf("walled primary did not fall back: index=%d", index)
	}

	far := now.Add(2 * time.Hour)
	index, earliest = chooseSeatRung(chain, []SeatWall{
		{Seat: "finite-a", WakeAt: now.Add(time.Hour)},
		{Seat: "finite-b", WakeAt: far},
	}, now)
	if index != -1 || earliest == nil || !earliest.Equal(now.Add(time.Hour)) {
		t.Fatalf("all-walled chain: index=%d earliest=%v", index, earliest)
	}

	index, _ = chooseSeatRung(chain, []SeatWall{{Seat: "finite-a", WakeAt: now.Add(-time.Minute)}}, now)
	if index != 0 {
		t.Fatalf("resolved wall did not return to the primary: index=%d", index)
	}

	// A balance rung is gated while the pool it backs resets within the lead.
	index, earliest = chooseSeatRung(balanceChain, []SeatWall{{Seat: "finite-a", WakeAt: now.Add(5 * time.Minute)}}, now)
	if index != -1 || earliest == nil || !earliest.Equal(now.Add(5*time.Minute)) {
		t.Fatalf("balance rung spent against an imminent reset: index=%d earliest=%v", index, earliest)
	}

	index, _ = chooseSeatRung(balanceChain, []SeatWall{{Seat: "finite-a", WakeAt: now.Add(time.Hour)}}, now)
	if index != 1 {
		t.Fatalf("balance rung was not used with headroom: index=%d", index)
	}

	// A paid-ok rung has no reset-proximity gate.
	index, _ = chooseSeatRung(paidChain, []SeatWall{{Seat: "finite-a", WakeAt: now.Add(5 * time.Minute)}}, now)
	if index != 1 {
		t.Fatalf("paid-ok rung was gated: index=%d", index)
	}

	// A wall recorded against a seat outside the chain (pre-upgrade run rows)
	// walls the primary rung.
	index, _ = chooseSeatRung(chain, []SeatWall{{Seat: "seat-a", WakeAt: now.Add(time.Hour)}}, now)
	if index != 1 {
		t.Fatalf("legacy seat wall did not bench the primary: index=%d", index)
	}
}

// A walled run with a fallback seat that has headroom is re-dispatched on that
// seat instead of waiting for the provider reset.
func TestFiniteWallFallsBackToSeat(t *testing.T) {
	clock := newEngineTestClock()
	wake := clock.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
	engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
		"1": {NoResult: true, Wall: true, WakeAt: wake},
		"2": {Result: ResultDone},
	}})
	engine.store.now = clock.Now
	task, err := engine.store.IntakeBrief(context.Background(), writeFallbackBrief(t, "wall-fallback", false,
		"staff: finite-a\nfallbacks:\n  - seat: finite-b\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	first, err := engine.store.GetRun(context.Background(), 1)
	if err != nil || first.Seat != "finite-a" {
		t.Fatalf("first run was not dispatched on the primary seat: %+v %v", first, err)
	}
	// The wall settles to the queue, not to a wait, and the very next scan
	// re-dispatches on the fallback seat.
	working := scanEngineUntil(t, engine, task.ID, StateWorking, 2)
	_ = working
	second, err := engine.store.GetRun(context.Background(), 2)
	if err != nil || second.Seat != "finite-b" {
		t.Fatalf("walled run was not re-dispatched on the fallback seat: %+v %v", second, err)
	}
	scanEngineUntil(t, engine, task.ID, StateDone, 2)
	runs, _ := engine.store.ListRuns(context.Background(), task.ID)
	if runs[0].Outcome != RunWall || runs[1].Outcome != RunOK {
		t.Fatalf("fallback run outcomes = %+v", runs)
	}
	accounting, _ := engine.store.DerivedAccounting(context.Background(), task.ID, PhaseImplementation)
	if accounting.Attempts != 1 || accounting.Failures != 0 {
		t.Fatalf("wall fallback changed accounting: %+v", accounting)
	}
	assertEngineInvariants(t, engine.store, task.ID)
}

// An all-walled chain waits for the earliest reset and then resumes on the
// first rung whose wall has cleared.
func TestFiniteWallAllWalledWaitsForEarliestReset(t *testing.T) {
	clock := newEngineTestClock()
	firstWake := clock.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
	secondWake := clock.Now().Add(30 * time.Minute).UTC().Format(time.RFC3339Nano)
	engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
		"1": {NoResult: true, Wall: true, WakeAt: firstWake},
		"2": {NoResult: true, Wall: true, WakeAt: secondWake},
		"3": {Result: ResultDone},
	}})
	engine.store.now = clock.Now
	task, err := engine.store.IntakeBrief(context.Background(), writeFallbackBrief(t, "wall-all-walled", false,
		"staff: finite-a\nfallbacks:\n  - seat: finite-b\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	waiting := scanEngineUntil(t, engine, task.ID, StateWaiting, 2)
	parsedFirstWake, _ := time.Parse(time.RFC3339Nano, firstWake)
	parsedSecondWake, _ := time.Parse(time.RFC3339Nano, secondWake)
	if waiting.WaitKind != WaitTime || waiting.WakeAt == nil || waiting.WakeAt.UTC().Equal(parsedFirstWake) {
		t.Fatalf("all-walled chain waited past the earliest reset: %+v (first=%s second=%s)", waiting, parsedFirstWake, parsedSecondWake)
	}
	if waiting.WakeAt.UTC().After(parsedSecondWake) {
		t.Fatalf("all-walled chain woke after the earliest reset: %+v", waiting)
	}
	if waiting.WaitingFor == "" {
		t.Fatalf("all-walled wait lost its reason: %+v", waiting)
	}
	if got := engineInvocationCount(t, engine); got != 2 {
		t.Fatalf("provider invoked %d times while the whole chain was walled", got)
	}
	// The fallback rung's reset passes first; work resumes there without
	// waiting for the primary pool.
	clock.Advance(45 * time.Minute)
	scanEngineUntil(t, engine, task.ID, StateDone, 3)
	third, err := engine.store.GetRun(context.Background(), 3)
	if err != nil || third.Seat != "finite-b" {
		t.Fatalf("wake did not resume on the recovered rung: %+v %v", third, err)
	}
	assertEngineInvariants(t, engine.store, task.ID)
}

// A balance rung is not spent while the pool it backs resets within the lead;
// the task waits those minutes instead.
func TestFiniteWallBalanceRungGatedNearReset(t *testing.T) {
	clock := newEngineTestClock()
	wake := clock.Now().Add(5 * time.Minute).UTC().Format(time.RFC3339Nano)
	engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
		"1": {NoResult: true, Wall: true, WakeAt: wake},
		"2": {Result: ResultDone},
	}})
	engine.store.now = clock.Now
	task, err := engine.store.IntakeBrief(context.Background(), writeFallbackBrief(t, "wall-balance-gate", false,
		"staff: finite-a\nfallbacks:\n  - seat: finite-bal\n    gate: balance\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	waiting := scanEngineUntil(t, engine, task.ID, StateWaiting, 1)
	if waiting.WaitKind != WaitTime || waiting.WakeAt == nil {
		t.Fatalf("gated balance rung did not wait: %+v", waiting)
	}
	if got := engineInvocationCount(t, engine); got != 1 {
		t.Fatalf("balance rung was spent against an imminent reset: %d invocations", got)
	}
	clock.Advance(6 * time.Minute)
	scanEngineUntil(t, engine, task.ID, StateDone, 2)
	second, err := engine.store.GetRun(context.Background(), 2)
	if err != nil || second.Seat != "finite-a" {
		t.Fatalf("work did not return to the primary seat after its reset: %+v %v", second, err)
	}
	assertEngineInvariants(t, engine.store, task.ID)
}

// When every rung is gated or walled at dispatch time (the reset approached
// while the task sat in the queue), the task waits until the earliest wall
// clears instead of spinning in the queue.
func TestFiniteDispatchDefersWhenAllRungsGated(t *testing.T) {
	clock := newEngineTestClock()
	wake := clock.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
	engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
		"1": {NoResult: true, Wall: true, WakeAt: wake},
		"2": {Result: ResultDone},
	}})
	engine.store.now = clock.Now
	task, err := engine.store.IntakeBrief(context.Background(), writeFallbackBrief(t, "wall-defer-gated", false,
		"staff: finite-a\nfallbacks:\n  - seat: finite-bal\n    gate: balance\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The wall settles with the reset one hour away, so the balance rung is
	// allowed and the task re-queues...
	if _, err := engine.store.GetRun(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	waitEngineInvocationCount(t, engine, 1)
	// ...but before the next dispatch the clock moves to where the lead gate
	// closes. The re-queued task must defer, not spend, not spin.
	clock.Advance(55 * time.Minute)
	_ = engine.ScanOnce(context.Background())
	_ = engine.ScanOnce(context.Background())
	deferred, err := engine.store.GetTask(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if deferred.State != StateWaiting || deferred.WaitKind != WaitTime || deferred.WakeAt == nil {
		t.Fatalf("all-gated task was not deferred to a wait: %+v", deferred)
	}
	if got := engineInvocationCount(t, engine); got != 1 {
		t.Fatalf("all-gated task spent a rung: %d invocations", got)
	}
	clock.Advance(10 * time.Minute)
	scanEngineUntil(t, engine, task.ID, StateDone, 2)
	second, err := engine.store.GetRun(context.Background(), 2)
	if err != nil || second.Seat != "finite-a" {
		t.Fatalf("deferred task did not resume on its primary seat: %+v %v", second, err)
	}
	assertEngineInvariants(t, engine.store, task.ID)
}

// The wall settlement records the seat it walled on and the reset it waits
// for, which is what later walks read; a store-level check of the settled
// event payload.
func TestFiniteWallSettlementRecordsSeatAndWake(t *testing.T) {
	s := newTestStore(t)
	now := s.now().UTC()
	task, err := s.IntakeBrief(context.Background(), writeFallbackBrief(t, "wall-event-payload", false,
		"staff: finite-a\nfallbacks:\n  - seat: finite-b\n"))
	if err != nil {
		t.Fatal(err)
	}
	run := dispatchTestRun(t, s, task, PhaseImplementation)
	wake := now.Add(time.Hour)
	settled, err := s.SettleRun(context.Background(), SettlementRequest{
		RunID: run.ID, ExpectedTaskVersion: task.Version + 1, ExecutionOutcome: RunWall,
		WallWakeAt: &wake, Error: "429 quota exhausted; reset at " + wake.Format(time.RFC3339),
		SeatChain: taskChainForTest(t, s, task),
	})
	if err != nil {
		t.Fatal(err)
	}
	if settled.State != StateTodo {
		t.Fatalf("walled run with fallback headroom settled to %s, want todo", settled.State)
	}
	var payload string
	if err := s.db.QueryRowContext(context.Background(),
		`SELECT payload_json FROM task_events WHERE run_id=? AND kind='settled'`, run.ID).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	var audit struct {
		Seat       string `json:"seat"`
		WallWakeAt string `json:"wall_wake_at"`
	}
	if err := json.Unmarshal([]byte(payload), &audit); err != nil {
		t.Fatal(err)
	}
	if audit.Seat != "seat-a" {
		t.Fatalf("settled event seat = %q, want the dispatched seat", audit.Seat)
	}
	if audit.WallWakeAt == "" {
		t.Fatalf("settled event lacks the wall wake")
	}
	walls, err := s.PhaseWalls(context.Background(), task.ID, PhaseImplementation)
	if err != nil {
		t.Fatal(err)
	}
	if len(walls) != 1 {
		t.Fatalf("phase walls = %+v", walls)
	}
	if !walls[0].WakeAt.Equal(wake) {
		t.Fatalf("phase wall wake = %v, want %v", walls[0].WakeAt, wake)
	}
	if walls[0].Seat != "seat-a" {
		t.Fatalf("phase wall seat = %q", walls[0].Seat)
	}
}

func taskChainForTest(t *testing.T, s *Store, task Task) []SeatRung {
	t.Helper()
	data, err := os.ReadFile(task.BriefPath)
	if err != nil {
		t.Fatal(err)
	}
	brief, err := ParseBrief(data)
	if err != nil {
		t.Fatal(err)
	}
	return brief.PhaseChain(PhaseImplementation)
}

// A review-phase chain falls back the same way as an implementation chain.
func TestFiniteReviewWallFallsBackToSeat(t *testing.T) {
	clock := newEngineTestClock()
	wake := clock.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
	engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
		"1": {Result: ResultDone},
		"2": {NoResult: true, Wall: true, WakeAt: wake},
		"3": {Result: ResultAccepted},
	}})
	engine.store.now = clock.Now
	task, err := engine.store.IntakeBrief(context.Background(), writeFallbackBrief(t, "wall-review-fallback", true,
		"staff: finite-a\nreview_staff: finite-r\nreview_fallbacks:\n  - seat: finite-r2\n"))
	if err != nil {
		t.Fatal(err)
	}
	scanEngineUntil(t, engine, task.ID, StateReviewing, 2)
	second, err := engine.store.GetRun(context.Background(), 2)
	if err != nil || second.Seat != "finite-r" {
		t.Fatalf("review run was not dispatched on the review seat: %+v %v", second, err)
	}
	scanEngineUntil(t, engine, task.ID, StateReviewing, 3)
	third, err := engine.store.GetRun(context.Background(), 3)
	if err != nil || third.Seat != "finite-r2" {
		t.Fatalf("walled review did not re-dispatch on the fallback seat: %+v %v", third, err)
	}
	scanEngineUntil(t, engine, task.ID, StateDone, 3)
	runs, _ := engine.store.ListRuns(context.Background(), task.ID)
	if runs[1].Outcome != RunWall || runs[2].Phase != PhaseReview || runs[2].Outcome != RunOK {
		t.Fatalf("review fallback runs = %+v", runs)
	}
	accounting, _ := engine.store.DerivedAccounting(context.Background(), task.ID, PhaseReview)
	if accounting.Attempts != 1 {
		t.Fatalf("review wall fallback changed charged attempts: %+v", accounting)
	}
	assertEngineInvariants(t, engine.store, task.ID)
}
