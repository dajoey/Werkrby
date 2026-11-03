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

// The quota-fallback feature: a run that ends in a provider quota wall moves
// the task's next run onto the first available seat of its staff's fallback
// chain right away, instead of sleeping to the provider reset while a fallback
// seat could run the work.

func writeFallbackBrief(t *testing.T, id string, reviewRequired bool, staff, reviewStaff string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), id+".md")
	data := fmt.Sprintf("---\nid: %s\ntitle: Fallback task %s\nrisk: low\nreview_required: %t\nstaff: %s\nreview_staff: %s\n---\n\nImplement one finite objective.\n",
		id, id, reviewRequired, staff, reviewStaff)
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatalf("write brief: %v", err)
	}
	return path
}

// newEngineFallbackFixture builds the fixture engine with roster-backed seats
// the test can rewrite between scans.
func newEngineFallbackFixture(t *testing.T, plan engineFixturePlan, seats *[]Seat) *Engine {
	t.Helper()
	engine := newEngineFixture(t, plan)
	engine.opts.RosterSeats = true
	engine.opts.Seats = func(context.Context) ([]Seat, error) {
		return append([]Seat(nil), (*seats)...), nil
	}
	return engine
}

// supervisorHarnessModel reads the harness model the supervisor was launched
// with, so a test can prove which seat the walk chose.
func supervisorHarnessModel(t *testing.T, run Run) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(run.RunDir, "supervisor.json"))
	if err != nil {
		t.Fatalf("read supervisor request for run %d: %v", run.ID, err)
	}
	var request struct {
		Harness struct {
			Model string `json:"model"`
		} `json:"harness"`
	}
	if err := json.Unmarshal(data, &request); err != nil {
		t.Fatalf("decode supervisor request for run %d: %v", run.ID, err)
	}
	return request.Harness.Model
}

func fallbackWokeEvents(t *testing.T, engine *Engine, taskID string) int {
	t.Helper()
	rows, err := engine.store.db.Query(`SELECT payload_json FROM task_events WHERE task_id=? AND kind='woke'`, taskID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(payload, `"fallback"`) {
			count++
		}
	}
	return count
}

func TestFiniteQuotaFallbackWallContinuesOnFallbackSeat(t *testing.T) {
	clock := newEngineTestClock()
	wake := clock.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
	seats := &[]Seat{
		{Name: "seat-a", Available: true, Pool: "zai", Fallback: "seat-b"},
		{Name: "seat-b", Available: false, Pool: "venice"}, // closed until the wall settles
	}
	engine := newEngineFallbackFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
		"1": {NoResult: true, Wall: true, WakeAt: wake},
		"2": {Result: ResultDone},
	}}, seats)
	engine.store.now = clock.Now
	ctx := context.Background()
	task, err := engine.store.IntakeBrief(ctx, writeFallbackBrief(t, "fallback-continue", false, "seat-a", ""))
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.ScanOnce(ctx); err != nil {
		t.Fatal(err)
	}
	waiting := scanEngineUntil(t, engine, task.ID, StateWaiting, 1)
	if waiting.WakeAt == nil {
		t.Fatalf("wall wait = %+v, want a wake deadline", waiting)
	}
	wallReset := *waiting.WakeAt
	first, _ := engine.store.GetRun(ctx, 1)
	if first.Seat != "seat-a" || first.Outcome != RunWall {
		t.Fatalf("first run = %+v, want seat-a walled", first)
	}
	if model := supervisorHarnessModel(t, first); model != "@seat-a" {
		t.Fatalf("first run harness model = %q, want @seat-a", model)
	}
	if got := fallbackWokeEvents(t, engine, task.ID); got != 0 {
		t.Fatalf("fallback wake events = %d, want none while the fallback is closed", got)
	}
	// The fallback seat opens: before the provider reset, the next run starts.
	(*seats)[1].Available = true
	scanEngineUntil(t, engine, task.ID, StateDone, 2)
	run, _ := engine.store.GetRun(ctx, 2)
	if run.Seat != "seat-b" {
		t.Fatalf("second run seat = %q, want the fallback seat-b", run.Seat)
	}
	if run.LaunchReason != "wake" {
		t.Fatalf("second run launch reason = %q, want wake", run.LaunchReason)
	}
	if model := supervisorHarnessModel(t, run); model != "@seat-b" {
		t.Fatalf("second run harness model = %q, want @seat-b", model)
	}
	if !run.CreatedAt.Before(wallReset) {
		t.Fatalf("second run started at %s, before the wall reset %s", run.CreatedAt, wallReset)
	}
	if got := fallbackWokeEvents(t, engine, task.ID); got != 1 {
		t.Fatalf("fallback wake events = %d, want exactly one", got)
	}
	projection, err := os.ReadFile(task.ProjectionPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(projection), "| 1 | implementation | seat-a | wall |") ||
		!strings.Contains(string(projection), "| 2 | implementation | seat-b | ok |") {
		t.Fatalf("projection lacks the Seat column:\n%s", projection)
	}
	assertEngineInvariants(t, engine.store, task.ID)
}

func TestFiniteQuotaFallbackAllSeatsWalledWaitsForReset(t *testing.T) {
	clock := newEngineTestClock()
	wake := clock.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
	engine := newEngineFallbackFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
		"1": {NoResult: true, Wall: true, WakeAt: wake},
		"2": {Result: ResultDone},
	}}, &[]Seat{
		{Name: "seat-a", Available: true, Pool: "zai", Fallback: "seat-b"},
		{Name: "seat-b", Available: true, Pool: "zai"},
	})
	engine.store.now = clock.Now
	ctx := context.Background()
	task, err := engine.store.IntakeBrief(ctx, writeFallbackBrief(t, "fallback-walled", false, "seat-a", ""))
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.ScanOnce(ctx); err != nil {
		t.Fatal(err)
	}
	waiting := scanEngineUntil(t, engine, task.ID, StateWaiting, 1)
	wallReset := *waiting.WakeAt
	for i := 0; i < 3; i++ {
		if err := engine.ScanOnce(ctx); err != nil {
			t.Fatal(err)
		}
	}
	current, _ := engine.store.GetTask(ctx, task.ID)
	runs, _ := engine.store.ListRuns(ctx, task.ID)
	if current.State != StateWaiting || len(runs) != 1 {
		t.Fatalf("all-walled task = %s with %d runs, want waiting with one", current.State, len(runs))
	}
	if current.WakeAt == nil || !current.WakeAt.Equal(wallReset) {
		t.Fatalf("all-walled wake = %+v, want the wall reset %s", current.WakeAt, wallReset)
	}
	if got := fallbackWokeEvents(t, engine, task.ID); got != 0 {
		t.Fatalf("fallback wake events = %d, want none while every chain seat is walled", got)
	}
	// After the wait lifts, the next run starts from the brief's own staff.
	clock.Advance(2 * time.Hour)
	scanEngineUntil(t, engine, task.ID, StateDone, 2)
	run, _ := engine.store.GetRun(ctx, 2)
	if run.Seat != "seat-a" || run.LaunchReason != "wake" {
		t.Fatalf("post-reset run = %+v, want seat-a with launch reason wake", run)
	}
	assertEngineInvariants(t, engine.store, task.ID)
}

func TestFiniteQuotaFallbackReviewWalksFromReviewStaff(t *testing.T) {
	clock := newEngineTestClock()
	wake := clock.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
	engine := newEngineFallbackFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
		"1": {Result: ResultDone},
		"2": {NoResult: true, Wall: true, WakeAt: wake},
		"3": {Result: ResultAccepted},
	}}, &[]Seat{
		{Name: "seat-a", Available: true, Pool: "venice"},
		{Name: "seat-r", Available: true, Pool: "zai", Fallback: "seat-rb"},
		{Name: "seat-rb", Available: true, Pool: "venice"},
	})
	engine.store.now = clock.Now
	ctx := context.Background()
	task, err := engine.store.IntakeBrief(ctx, writeFallbackBrief(t, "fallback-review", true, "seat-a", "seat-r"))
	if err != nil {
		t.Fatal(err)
	}
	scanEngineUntil(t, engine, task.ID, StateDone, 3)
	review, _ := engine.store.GetRun(ctx, 2)
	if review.Phase != PhaseReview || review.Seat != "seat-r" {
		t.Fatalf("review run = %+v, want review phase on the brief's review staff seat-r", review)
	}
	if model := supervisorHarnessModel(t, review); model != "@seat-r" {
		t.Fatalf("review harness model = %q, want @seat-r", model)
	}
	final, _ := engine.store.GetRun(ctx, 3)
	if final.Phase != PhaseReview || final.Seat != "seat-rb" || final.LaunchReason != "wake" {
		t.Fatalf("post-wall review run = %+v, want review on seat-rb with launch reason wake", final)
	}
	assertEngineInvariants(t, engine.store, task.ID)
}

func TestFiniteQuotaFallbackWallHopsCountTowardMaxRuns(t *testing.T) {
	clock := newEngineTestClock()
	wake := clock.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
	engine := newEngineFallbackFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
		"1": {NoResult: true, Wall: true, WakeAt: wake},
		"2": {NoResult: true, Wall: true, WakeAt: wake},
	}}, &[]Seat{
		{Name: "seat-a", Available: true, Pool: "zai", Fallback: "seat-b"},
		{Name: "seat-b", Available: true, Pool: "venice"},
	})
	engine.store.now = clock.Now
	ctx := context.Background()
	task, err := engine.store.IntakeBrief(ctx, writeCappedBrief(t, "fallback-cap", 2))
	if err != nil {
		t.Fatal(err)
	}
	// The capped brief carries no staff; the walk starts from the engine default.
	engine.opts.Harness.Model = "@seat-a"
	failed := scanEngineUntil(t, engine, task.ID, StateFailed, 2)
	if failed.State != StateFailed {
		t.Fatalf("capped fallback task = %+v, want failed", failed)
	}
	runs, _ := engine.store.ListRuns(ctx, task.ID)
	if len(runs) != 2 || runs[0].Seat != "seat-a" || runs[1].Seat != "seat-b" {
		t.Fatalf("capped fallback runs = %+v, want one run per seat", runs)
	}
	accounting, _ := engine.store.DerivedAccounting(ctx, task.ID, PhaseImplementation)
	if accounting.Runs != 2 || accounting.Attempts != 0 {
		t.Fatalf("capped fallback accounting = %+v, want two uncharged runs", accounting)
	}
	assertFailureReason(t, engine.store, task.ID, "max_runs")
	assertEngineInvariants(t, engine.store, task.ID)
}

func TestFiniteQuotaFallbackEarlyWakeRespectsMaxOpen(t *testing.T) {
	clock := newEngineTestClock()
	wake := clock.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
	seats := &[]Seat{
		{Name: "seat-a", Available: true, Pool: "zai", Fallback: "seat-b"},
		{Name: "seat-b", Available: false, Pool: "venice"}, // closed until the cap probe
	}
	engine := newEngineFallbackFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
		"1": {NoResult: true, Wall: true, WakeAt: wake},
		"2": {Block: true},
		"3": {Result: ResultDone},
	}}, seats)
	engine.store.now = clock.Now
	engine.opts.MaxOpenRuns = 1
	ctx := context.Background()
	wallTask, err := engine.store.IntakeBrief(ctx, writeFallbackBrief(t, "fallback-maxopen", false, "seat-a", ""))
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.ScanOnce(ctx); err != nil {
		t.Fatal(err)
	}
	scanEngineUntil(t, engine, wallTask.ID, StateWaiting, 1)
	// A second task now holds the single open-run slot with a blocked run.
	holdTask, err := engine.store.IntakeBrief(ctx, writeFallbackBrief(t, "fallback-maxopen-hold", false, "seat-a", ""))
	if err != nil {
		t.Fatal(err)
	}
	scanEngineUntil(t, engine, holdTask.ID, StateWorking, 1)
	// The fallback seat opens, but the open-run cap is full: no early wake.
	(*seats)[1].Available = true
	for i := 0; i < 3; i++ {
		if err := engine.ScanOnce(ctx); err != nil {
			t.Fatal(err)
		}
	}
	runs, _ := engine.store.ListRuns(ctx, wallTask.ID)
	if len(runs) != 1 {
		t.Fatalf("wall task runs = %d, want no early wake while max-open is full", len(runs))
	}
	if got := fallbackWokeEvents(t, engine, wallTask.ID); got != 0 {
		t.Fatalf("fallback wake events = %d, want none while max-open is full", got)
	}
	// Releasing the blocked run frees the slot and the early wake fires.
	hold, _ := engine.store.GetRun(ctx, 2)
	if err := os.WriteFile(filepath.Join(hold.RunDir, "release"), []byte("go"), 0o600); err != nil {
		t.Fatal(err)
	}
	scanEngineUntil(t, engine, holdTask.ID, StateDone, 1)
	scanEngineUntil(t, engine, wallTask.ID, StateDone, 2)
	run, _ := engine.store.GetRun(ctx, 3)
	if run.Seat != "seat-b" || run.LaunchReason != "wake" {
		t.Fatalf("post-release wall task run = %+v, want seat-b with launch reason wake", run)
	}
	assertEngineInvariants(t, engine.store, wallTask.ID)
}

func TestFiniteQuotaFallbackClosedBalanceSeatNeverDispatched(t *testing.T) {
	clock := newEngineTestClock()
	wake := clock.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
	engine := newEngineFallbackFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
		"1": {NoResult: true, Wall: true, WakeAt: wake},
		"2": {Result: ResultDone},
	}}, &[]Seat{
		{Name: "seat-a", Available: true, Pool: "zai", Fallback: "seat-bal"},
		{Name: "seat-bal", Available: false, Pool: "zai_balance"},
	})
	engine.store.now = clock.Now
	ctx := context.Background()
	task, err := engine.store.IntakeBrief(ctx, writeFallbackBrief(t, "fallback-balance", false, "seat-a", ""))
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.ScanOnce(ctx); err != nil {
		t.Fatal(err)
	}
	waiting := scanEngineUntil(t, engine, task.ID, StateWaiting, 1)
	for i := 0; i < 3; i++ {
		if err := engine.ScanOnce(ctx); err != nil {
			t.Fatal(err)
		}
	}
	current, _ := engine.store.GetTask(ctx, task.ID)
	runs, _ := engine.store.ListRuns(ctx, task.ID)
	if current.State != StateWaiting || waiting.WakeAt == nil || len(runs) != 1 {
		t.Fatalf("closed-balance task = %s with %d runs, want waiting with one run", current.State, len(runs))
	}
	if got := fallbackWokeEvents(t, engine, task.ID); got != 0 {
		t.Fatalf("fallback wake events = %d, want none while the balance seat reads closed", got)
	}
	assertEngineInvariants(t, engine.store, task.ID)
}

// The roster seat expiry: a balance seat carries available_until, the reading
// time plus BALANCE_STALE_S, and the engine closes it at that instant even if
// the roster file never changes again.

func TestFiniteQuotaFallbackExpiredSeatNeverDispatched(t *testing.T) {
	clock := newEngineTestClock()
	wake := clock.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
	expired := clock.Now().Add(-time.Minute).UTC()
	engine := newEngineFallbackFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
		"1": {NoResult: true, Wall: true, WakeAt: wake},
		"2": {Result: ResultDone},
	}}, &[]Seat{
		{Name: "seat-a", Available: true, Pool: "zai", Fallback: "seat-bal"},
		{Name: "seat-bal", Available: true, Pool: "zai_balance", AvailableUntil: &expired},
	})
	engine.store.now = clock.Now
	ctx := context.Background()
	task, err := engine.store.IntakeBrief(ctx, writeFallbackBrief(t, "fallback-expired-balance", false, "seat-a", ""))
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.ScanOnce(ctx); err != nil {
		t.Fatal(err)
	}
	waiting := scanEngineUntil(t, engine, task.ID, StateWaiting, 1)
	for i := 0; i < 3; i++ {
		if err := engine.ScanOnce(ctx); err != nil {
			t.Fatal(err)
		}
	}
	current, _ := engine.store.GetTask(ctx, task.ID)
	runs, _ := engine.store.ListRuns(ctx, task.ID)
	if current.State != StateWaiting || waiting.WakeAt == nil || len(runs) != 1 {
		t.Fatalf("expired-balance task = %s with %d runs, want waiting for the wall reset with one run", current.State, len(runs))
	}
	if runs[0].Seat != "seat-a" {
		t.Fatalf("runs = %+v, want the only run on seat-a", runs)
	}
	if got := fallbackWokeEvents(t, engine, task.ID); got != 0 {
		t.Fatalf("fallback wake events = %d, want none while the balance seat is expired", got)
	}
	assertEngineInvariants(t, engine.store, task.ID)
}

func TestFiniteQuotaFallbackUnexpiredSeatDispatched(t *testing.T) {
	clock := newEngineTestClock()
	wake := clock.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
	unexpired := clock.Now().Add(15 * time.Minute).UTC()
	engine := newEngineFallbackFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
		"1": {NoResult: true, Wall: true, WakeAt: wake},
		"2": {Result: ResultDone},
	}}, &[]Seat{
		{Name: "seat-a", Available: true, Pool: "zai", Fallback: "seat-bal"},
		{Name: "seat-bal", Available: true, Pool: "zai_balance", AvailableUntil: &unexpired},
	})
	engine.store.now = clock.Now
	ctx := context.Background()
	task, err := engine.store.IntakeBrief(ctx, writeFallbackBrief(t, "fallback-unexpired-balance", false, "seat-a", ""))
	if err != nil {
		t.Fatal(err)
	}
	scanEngineUntil(t, engine, task.ID, StateDone, 2)
	run, _ := engine.store.GetRun(ctx, 2)
	if run.Seat != "seat-bal" || run.LaunchReason != "wake" {
		t.Fatalf("second run = %+v, want the unexpired balance seat with launch reason wake", run)
	}
	assertEngineInvariants(t, engine.store, task.ID)
}

func TestFiniteQuotaFallbackExpiredSeatNoEarlyWake(t *testing.T) {
	clock := newEngineTestClock()
	wake := clock.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
	expired := clock.Now().Add(-time.Minute).UTC()
	seats := &[]Seat{
		{Name: "seat-a", Available: true, Pool: "zai", Fallback: "seat-bal"},
		{Name: "seat-bal", Available: false, Pool: "zai_balance"}, // closed when the wall settles
	}
	engine := newEngineFallbackFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
		"1": {NoResult: true, Wall: true, WakeAt: wake},
		"2": {Result: ResultDone},
	}}, seats)
	engine.store.now = clock.Now
	ctx := context.Background()
	task, err := engine.store.IntakeBrief(ctx, writeFallbackBrief(t, "fallback-expired-nowake", false, "seat-a", ""))
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.ScanOnce(ctx); err != nil {
		t.Fatal(err)
	}
	waiting := scanEngineUntil(t, engine, task.ID, StateWaiting, 1)
	// The roster changes under the waiting task: the balance seat now reads
	// available, with an expiry the store clock has already passed.
	(*seats)[1].Available = true
	(*seats)[1].AvailableUntil = &expired
	for i := 0; i < 3; i++ {
		if err := engine.ScanOnce(ctx); err != nil {
			t.Fatal(err)
		}
	}
	current, _ := engine.store.GetTask(ctx, task.ID)
	runs, _ := engine.store.ListRuns(ctx, task.ID)
	if current.State != StateWaiting || current.Version != waiting.Version ||
		current.WakeAt == nil || waiting.WakeAt == nil || !current.WakeAt.Equal(*waiting.WakeAt) {
		t.Fatalf("expired-nowake task = %+v, want the wall wait unchanged (was %+v)", current, waiting)
	}
	if len(runs) != 1 {
		t.Fatalf("runs = %+v, want exactly the one walled run", runs)
	}
	if got := fallbackWokeEvents(t, engine, task.ID); got != 0 {
		t.Fatalf("fallback wake events = %d, want none for an expired balance seat", got)
	}
	assertEngineInvariants(t, engine.store, task.ID)
}

func TestFiniteRosterExpiredStartSeatWaits(t *testing.T) {
	clock := newEngineTestClock()
	expired := clock.Now().Add(-time.Minute).UTC()
	engine := newEngineFallbackFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{
		"1": {Result: ResultDone},
	}}, &[]Seat{
		{Name: "seat-a", Available: true, Pool: "zai", AvailableUntil: &expired},
	})
	engine.store.now = clock.Now
	ctx := context.Background()
	task, err := engine.store.IntakeBrief(ctx, writeFallbackBrief(t, "expired-start-seat", false, "seat-a", ""))
	if err != nil {
		t.Fatal(err)
	}
	before, err := engine.store.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.ScanOnce(ctx); err != nil {
		t.Fatal(err)
	}
	after, _ := engine.store.GetTask(ctx, task.ID)
	runs, _ := engine.store.ListRuns(ctx, task.ID)
	if after.State != StateTodo || after.State != before.State || after.Version != before.Version {
		t.Fatalf("expired start seat task = %+v, want todo with its version unchanged (was %+v)", after, before)
	}
	if len(runs) != 0 {
		t.Fatalf("runs = %+v, want none while the start seat is expired", runs)
	}
	assertEngineInvariants(t, engine.store, task.ID)
}

func TestFiniteCLIRosterAcceptsFallbackAndPoolKeys(t *testing.T) {
	base := t.TempDir()
	path := filepath.Join(base, "availability.yaml")
	reset := time.Date(2026, 11, 1, 3, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	roster := "seats:\n" +
		"  - name: seat-a\n" +
		"    available: true\n" +
		"    fallback: seat-b\n" +
		"    pool: zai\n" +
		"  - name: seat-b\n" +
		"    available: false\n" +
		"    quota_reset: " + reset + "\n" +
		"    pool: zai_balance\n"
	if err := os.WriteFile(path, []byte(roster), 0o600); err != nil {
		t.Fatal(err)
	}
	seats, err := loadCLIRoster(path)
	if err != nil {
		t.Fatalf("load roster with fallback and pool keys: %v", err)
	}
	if len(seats) != 2 {
		t.Fatalf("roster seats = %+v", seats)
	}
	if seats[0].Fallback != "seat-b" || seats[0].Pool != "zai" || !seats[0].Available {
		t.Fatalf("seat-a = %+v, want its fallback, pool and availability", seats[0])
	}
	if seats[1].Available || seats[1].QuotaReset == nil || !seats[1].QuotaReset.Equal(reset2(t, reset)) || seats[1].Pool != "zai_balance" {
		t.Fatalf("seat-b = %+v, want unavailable with its reset and pool", seats[1])
	}
	unknown := filepath.Join(base, "unknown.yaml")
	if err := os.WriteFile(unknown, []byte("seats:\n  - name: seat-a\n    available: true\n    surprise: yes\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadCLIRoster(unknown); err == nil || !strings.Contains(err.Error(), "field surprise not found") {
		t.Fatalf("unknown roster key error = %v, want the strict decoder to refuse it", err)
	}
}

func reset2(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func TestFiniteCLIRosterAvailableUntil(t *testing.T) {
	base := t.TempDir()
	path := filepath.Join(base, "availability.yaml")
	roster := "seats:\n" +
		"  - name: seat-a\n" +
		"    available: true\n" +
		"    available_until: '2026-11-02T00:30:25Z'\n" + // the writer's yaml.safe_dump form
		"  - name: seat-b\n" +
		"    available: true\n" +
		"    available_until: 2026-11-02T00:45:25Z\n"
	if err := os.WriteFile(path, []byte(roster), 0o600); err != nil {
		t.Fatal(err)
	}
	seats, err := loadCLIRoster(path)
	if err != nil {
		t.Fatalf("load roster with available_until: %v", err)
	}
	if len(seats) != 2 {
		t.Fatalf("roster seats = %+v", seats)
	}
	quoted := time.Date(2026, 11, 2, 0, 30, 25, 0, time.UTC)
	bare := time.Date(2026, 11, 2, 0, 45, 25, 0, time.UTC)
	if seats[0].AvailableUntil == nil || !seats[0].AvailableUntil.Equal(quoted) {
		t.Fatalf("seat-a = %+v, want the quoted available_until %s", seats[0], quoted)
	}
	if seats[1].AvailableUntil == nil || !seats[1].AvailableUntil.Equal(bare) {
		t.Fatalf("seat-b = %+v, want the bare available_until %s", seats[1], bare)
	}
	notTime := filepath.Join(base, "not-a-time.yaml")
	if err := os.WriteFile(notTime, []byte("seats:\n  - name: seat-a\n    available: true\n    available_until: soon\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadCLIRoster(notTime); err == nil {
		t.Fatalf("non-time available_until was accepted")
	}
	unknown := filepath.Join(base, "unknown.yaml")
	if err := os.WriteFile(unknown, []byte("seats:\n  - name: seat-a\n    available: true\n    surprise: yes\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadCLIRoster(unknown); err == nil || !strings.Contains(err.Error(), "field surprise not found") {
		t.Fatalf("unknown roster key error = %v, want the strict decoder to refuse it", err)
	}
}

// TestFiniteCLIRosterLoadsWriterSample decodes the roster writer's real
// availability.yaml with the engine's strict decoder. It is the roster read
// proof for installs: every seat the writer stamped with available_until
// carries one, and no other seat does.
func TestFiniteCLIRosterLoadsWriterSample(t *testing.T) {
	sample := os.Getenv("WERKRBY_FINITE_ROSTER_SAMPLE")
	if sample == "" {
		t.Skip("WERKRBY_FINITE_ROSTER_SAMPLE must name the writer's availability.yaml")
	}
	expiring := make(map[string]bool)
	for _, name := range strings.Split(os.Getenv("WERKRBY_FINITE_ROSTER_SAMPLE_EXPIRING"), ",") {
		if name = strings.TrimSpace(name); name != "" {
			expiring[name] = true
		}
	}
	seats, err := loadCLIRoster(sample)
	if err != nil {
		t.Fatalf("load writer sample %s: %v", sample, err)
	}
	if len(seats) == 0 {
		t.Fatalf("writer sample %s decoded no seats", sample)
	}
	for _, seat := range seats {
		if expiring[seat.Name] {
			if seat.AvailableUntil == nil {
				t.Fatalf("seat %s = %+v, want the writer's available_until", seat.Name, seat)
			}
			continue
		}
		if seat.AvailableUntil != nil {
			t.Fatalf("seat %s = %+v, want no available_until", seat.Name, seat)
		}
	}
}

func TestFiniteProjectionRunsTableShowsSeat(t *testing.T) {
	task := Task{ID: "task-seat-column", Title: "Seat column", State: StateDone, Risk: "low",
		Version: 2, CreatedAt: time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC), UpdatedAt: time.Date(2026, 11, 1, 12, 1, 0, 0, time.UTC)}
	ended := time.Date(2026, 11, 1, 12, 0, 30, 0, time.UTC)
	runs := []Run{{
		ID: 1, TaskID: task.ID, Phase: PhaseImplementation, Seat: "seat-a", SessionKey: "finite:t",
		LaunchReason: "initial", RunDir: "/tmp/none", CreatedAt: task.CreatedAt, EndedAt: &ended, Outcome: RunOK,
	}}
	out := RenderProjection(task, runs)
	if !strings.Contains(out, "| Run | Phase | Seat | Execution | Created | Ended |") {
		t.Fatalf("projection runs header lacks the Seat column:\n%s", out)
	}
	if !strings.Contains(out, "| 1 | implementation | seat-a | ok |") {
		t.Fatalf("projection runs row lacks the seat:\n%s", out)
	}
}
