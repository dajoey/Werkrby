package finite

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/ncruces/go-sqlite3/driver"
)

// zaiWallText is the live Z.ai weekly-limit wording: the reset it prints is
// UTC+8, which a UTC reader misreads by exactly eight hours.
const zaiWallText = "HTTP 429: Weekly/Monthly Limit Exhausted. Your limit will reset at 2026-11-07 02:53:12"

func TestResolveWallWakeZaiTextIsUTCPlus8(t *testing.T) {
	misread := time.Date(2026, 11, 7, 2, 53, 12, 0, time.UTC)
	want := time.Date(2026, 11, 6, 18, 53, 12, 0, time.UTC)
	settle := time.Date(2026, 11, 2, 6, 34, 16, 0, time.UTC)
	wake, source := resolveWallWake(wallInputs{Pool: "zai", ErrorText: zaiWallText, HarnessWake: misread, SettleAt: settle})
	if !wake.Equal(want) || source != WallSourceText {
		t.Fatalf("zai text wake = %s %q, want %s text", wake, source, want)
	}
	wake, source = resolveWallWake(wallInputs{Pool: "pool-a", ErrorText: zaiWallText, HarnessWake: misread, SettleAt: settle})
	if !wake.Equal(misread) || source != WallSourceText {
		t.Fatalf("zone-less pool text wake = %s %q, want %s text", wake, source, misread)
	}
}

func TestResolveWallWakeRosterResetOutranksText(t *testing.T) {
	roster := time.Date(2026, 11, 6, 18, 53, 12, 0, time.UTC)
	settle := time.Date(2026, 11, 2, 6, 34, 16, 0, time.UTC)
	text := "HTTP 429: quota exhausted. Your limit will reset at 2026-11-08 00:00:00"
	harness := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	wake, source := resolveWallWake(wallInputs{Pool: "zai", ErrorText: text, RosterReset: &roster, HarnessWake: harness, SettleAt: settle})
	if !wake.Equal(roster) || source != WallSourceStructured {
		t.Fatalf("roster wake = %s %q, want %s structured", wake, source, roster)
	}
	past := settle.Add(-time.Minute)
	// The text reads in the pool's own zone: 2026-11-08 00:00:00 UTC+8.
	textWant := time.Date(2026, 11, 7, 16, 0, 0, 0, time.UTC)
	wake, source = resolveWallWake(wallInputs{Pool: "zai", ErrorText: text, RosterReset: &past, HarnessWake: harness, SettleAt: settle})
	if !wake.Equal(textWant) || source != WallSourceText {
		t.Fatalf("past-roster wake = %s %q, want %s text", wake, source, textWant)
	}
}

func TestResolveWallWakeUnparsedProbeLadder(t *testing.T) {
	text := "HTTP 429: rate limit exceeded"
	settle := time.Date(2026, 11, 2, 6, 34, 16, 0, time.UTC)
	for prior, delay := range map[int]time.Duration{0: 15 * time.Minute, 1: 30 * time.Minute, 2: 60 * time.Minute, 3: 60 * time.Minute} {
		wake, source := resolveWallWake(wallInputs{Pool: "zai", ErrorText: text, HarnessProbe: true, PriorProbes: prior, SettleAt: settle})
		if !wake.Equal(settle.Add(delay)) || source != WallSourceProbe {
			t.Fatalf("probe %d wake = %s %q, want %s probe", prior, wake, source, settle.Add(delay))
		}
	}
	harness := settle.Add(45 * time.Minute)
	wake, source := resolveWallWake(wallInputs{Pool: "zai", ErrorText: text, HarnessWake: harness, SettleAt: settle})
	if !wake.Equal(harness) || source != WallSourceHarness {
		t.Fatalf("harness wake = %s %q, want %s harness", wake, source, harness)
	}
}

// seedLegacyWallRow intakes a task and settles one wall on it the way a
// store-level caller did before walls carried a source: WallWakeAt only, no
// resolved wake, so the payload holds no wall_source. It is the legacy row
// shape every wall deployed before this fix wrote.
func seedLegacyWallRow(t *testing.T, engine *Engine, id, seat string, wake time.Time, errorText string) Task {
	t.Helper()
	ctx := context.Background()
	task, err := engine.store.IntakeBrief(ctx, writeFallbackBrief(t, id, false, seat, ""))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.store.db.Exec(`UPDATE tasks SET state='working' WHERE id=?`, task.ID); err != nil {
		t.Fatal(err)
	}
	runDir := filepath.Join(t.TempDir(), "legacy-run-dir")
	if _, err := engine.store.db.Exec(`INSERT INTO runs(task_id,phase,task_version,seat,session_key,launch_reason,run_dir,created_at) VALUES(?,?,?,?,?,?,?,?)`,
		task.ID, "implementation", task.Version, seat, "finite:"+task.ID+":implementation", "initial", runDir, stamp(engine.store.now())); err != nil {
		t.Fatal(err)
	}
	var runID int64
	if err := engine.store.db.QueryRow(`SELECT id FROM runs WHERE run_dir=?`, runDir).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.store.SettleRun(ctx, SettlementRequest{RunID: runID, ExpectedTaskVersion: task.Version, ExecutionOutcome: RunWall, WallWakeAt: &wake, Error: errorText}); err != nil {
		t.Fatal(err)
	}
	return task
}

func TestPoolWallsLegacyRowIsStable(t *testing.T) {
	clock := newEngineTestClock()
	engine := newEngineFixture(t, engineFixturePlan{})
	engine.store.now = clock.Now
	stored := time.Date(2026, 11, 7, 2, 53, 12, 0, time.UTC)
	if task := seedLegacyWallRow(t, engine, "legacy-stable", "seat-a", stored, zaiWallText); task.ID == "" {
		t.Fatal("seeded task has no id")
	}
	want := time.Date(2026, 11, 6, 18, 53, 12, 0, time.UTC)
	first := time.Date(2026, 11, 3, 6, 34, 16, 0, time.UTC)
	laterReset := time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)
	lifts, err := engine.store.poolWalls(context.Background(), []Seat{{Name: "seat-a", Pool: "zai", QuotaReset: &laterReset}}, first)
	if err != nil {
		t.Fatal(err)
	}
	if len(lifts) != 1 || !lifts["zai"].Equal(want) {
		t.Fatalf("first read lifts = %v, want zai at %s", lifts, want)
	}
	second := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	laterStill := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	lifts, err = engine.store.poolWalls(context.Background(), []Seat{{Name: "seat-a", Pool: "zai", QuotaReset: &laterStill}}, second)
	if err != nil {
		t.Fatal(err)
	}
	if len(lifts) != 1 || !lifts["zai"].Equal(want) {
		t.Fatalf("second read lifts = %v, want zai at %s", lifts, want)
	}
}

// TestReplayLiveWalls re-reads the live engine's settled wall rows through the
// legacy lift and prints what each wakes at now. It is read-only and asserts
// nothing; it exists so a deploy can be checked against real rows first.
func TestReplayLiveWalls(t *testing.T) {
	dbPath := os.Getenv("WERKRBY_FINITE_REPLAY_DB")
	if dbPath == "" {
		t.Skip("WERKRBY_FINITE_REPLAY_DB must name the engine database copy")
	}
	var rosterSeats []Seat
	if rosterPath := os.Getenv("WERKRBY_FINITE_REPLAY_ROSTER"); rosterPath != "" {
		seats, err := loadCLIRoster(rosterPath)
		if err != nil {
			t.Fatal(err)
		}
		rosterSeats = seats
	}
	db, err := sql.Open("sqlite3", "file:"+filepath.ToSlash(dbPath)+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT r.id, r.seat, e.payload_json FROM task_events e JOIN runs r ON r.id = e.run_id WHERE e.kind='settled' AND r.outcome='wall' ORDER BY r.id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	poolOf := make(map[string]string, len(rosterSeats))
	for _, seat := range rosterSeats {
		poolOf[seat.Name] = seat.Pool
	}
	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()
	for rows.Next() {
		var id int64
		var seat, payload string
		if err := rows.Scan(&id, &seat, &payload); err != nil {
			t.Fatal(err)
		}
		var audit struct {
			Pool       string `json:"pool"`
			WallWakeAt string `json:"wall_wake_at"`
			Error      string `json:"error"`
		}
		if json.Unmarshal([]byte(payload), &audit) != nil {
			continue
		}
		pool := audit.Pool
		if pool == "" {
			pool = poolOf[seat]
		}
		stored, err := time.Parse(time.RFC3339Nano, audit.WallWakeAt)
		if err != nil {
			continue
		}
		fmt.Fprintf(out, "%d %s %s %s\n", id, seat, pool, legacyWallLift(pool, audit.Error, stored).UTC().Format(time.RFC3339))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}
