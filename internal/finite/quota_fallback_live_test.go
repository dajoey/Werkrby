package finite

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/ncruces/go-sqlite3/driver"
)

// The controlled live wall with a fallback roster: the compiled binary and a
// real Hermes, both seats pointed at a local endpoint that answers 429 with a
// reset time. Run 1 walls on seat A; run 2 must start on seat B before run 1's
// reset; with both pools walled the task waits; with B closed there is exactly
// one run.

func liveWallFallbackHome(t *testing.T) string {
	t.Helper()
	home := os.Getenv("WERKRBY_FINITE_HERMES_WALL_HOME")
	if home == "" || !filepath.IsAbs(home) {
		t.Fatalf("WERKRBY_FINITE_HERMES_WALL_HOME must name an absolute disposable home")
	}
	marker, err := os.ReadFile(filepath.Join(home, ".finite-hermes-disposable"))
	if err != nil || !strings.HasPrefix(string(marker), "wk-finite-wall") {
		t.Fatalf("disposable wall-fallback marker: %v", err)
	}
	return home
}

func writeLiveFallbackWorker(t *testing.T, root, profileA, profileB string) string {
	t.Helper()
	worker := filepath.Join(root, "worker")
	if err := os.MkdirAll(filepath.Join(worker, ".werkrby"), 0o700); err != nil {
		t.Fatal(err)
	}
	roster := fmt.Sprintf("staff:\n"+
		"  seat-a:\n    runner: hermes\n    headless: true\n    args: [\"-p\", %q, \"--model\", \"zai/glm-5.3-flash\"]\n"+
		"  seat-b:\n    runner: hermes\n    headless: true\n    args: [\"-p\", %q, \"--model\", \"zai/glm-5.3-flash\"]\n",
		profileA, profileB)
	if err := os.WriteFile(filepath.Join(worker, ".werkrby", "roster.yaml"), []byte(roster), 0o600); err != nil {
		t.Fatal(err)
	}
	return worker
}

func writeLiveAvailability(t *testing.T, root string, seatBAvailable bool) string {
	t.Helper()
	path := filepath.Join(root, "availability.yaml")
	value := "false"
	if seatBAvailable {
		value = "true"
	}
	roster := "seats:\n" +
		"  - name: seat-a\n    available: true\n    fallback: seat-b\n    pool: pool-a\n" +
		"  - name: seat-b\n    available: " + value + "\n    pool: pool-b\n"
	if err := os.WriteFile(path, []byte(roster), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeLiveFallbackBrief(t *testing.T, root, id, staff string) string {
	t.Helper()
	brief := fmt.Sprintf("---\nid: %s\ntitle: Compiled finite Hermes quota fallback\nrisk: low\nreview_required: false\nstaff: %s\n---\nThis is a disposable protocol test. Do not inspect or modify unrelated files.\n", id, staff)
	path := filepath.Join(root, "brief.md")
	if err := os.WriteFile(path, []byte(brief), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

type liveFallbackRun struct {
	ID        int64
	Seat      string
	Outcome   RunOutcome
	CreatedAt time.Time
	RunDir    string
	Ended     bool
}

func liveFallbackRuns(t *testing.T, root string) []liveFallbackRun {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+filepath.ToSlash(filepath.Join(root, "engine.db"))+"?mode=ro&_pragma=busy_timeout(2000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT id,seat,COALESCE(outcome,''),created_at,run_dir,ended_at IS NOT NULL FROM runs ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []liveFallbackRun
	for rows.Next() {
		var run liveFallbackRun
		var outcome, created string
		if err := rows.Scan(&run.ID, &run.Seat, &outcome, &created, &run.RunDir, &run.Ended); err != nil {
			t.Fatal(err)
		}
		run.Outcome = RunOutcome(outcome)
		if run.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil {
			t.Fatal(err)
		}
		out = append(out, run)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func liveRunWallWakeAt(t *testing.T, run liveFallbackRun) time.Time {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(run.RunDir, "execution.json"))
	if err != nil {
		t.Fatal(err)
	}
	var execution struct {
		WallWakeAt time.Time `json:"wall_wake_at"`
	}
	if err := json.Unmarshal(data, &execution); err != nil || execution.WallWakeAt.IsZero() {
		t.Fatalf("run %d wall record = %s (%v)", run.ID, data, err)
	}
	return execution.WallWakeAt
}

func TestFiniteLiveCompiledHermesQuotaWallFallback(t *testing.T) {
	if os.Getenv("WERKRBY_FINITE_HERMES_LIVE") != "1" {
		t.Skip("compiled live Hermes quota fallback requires explicit opt-in")
	}
	binary := liveBinary(t)
	home := liveWallFallbackHome(t)
	endpoint := newLiveControlledEndpoint(t)
	writeLiveProfile(t, home, "wk-finite-wall-a", endpoint.URL+"/wall/v1")
	writeLiveProfile(t, home, "wk-finite-wall-b", endpoint.URL+"/wall/v1")
	root := liveTestRoot(t, "compiled-wall-fallback")
	worker := writeLiveFallbackWorker(t, root, "wk-finite-wall-a", "wk-finite-wall-b")
	availability := writeLiveAvailability(t, root, true)
	taskID := "compiled-wall-fb"
	stateRoot := filepath.Join(root, "state")
	brief := writeLiveFallbackBrief(t, root, taskID, "seat-a")
	owner := startLiveCommand(t, binary, []string{
		"serve", "--root", stateRoot, "--worker", worker, "--roster", availability,
		"--harness", "herdr", "--model", "@seat-a", "--poll-interval", "25ms", "--stop-grace", "25ms",
	}, liveEnv(home), filepath.Join(root, "owner.log"))
	defer owner.terminate(t)
	waitForFixture(t, func() bool { return fileExists(filepath.Join(stateRoot, "control.json")) }, "compiled owner control publication")
	runLiveControl(t, binary, "intake", "--root", stateRoot, "--brief", brief)

	// Run 1 walls on seat A.
	status := waitLiveState(t, binary, stateRoot, taskID, StateWaiting, 2*time.Minute)
	runs := liveFallbackRuns(t, stateRoot)
	if len(runs) != 1 || runs[0].Seat != "seat-a" || runs[0].Outcome != RunWall || !runs[0].Ended {
		t.Fatalf("first runs = %+v, want one settled wall on seat-a", runs)
	}
	firstReset := liveRunWallWakeAt(t, runs[0])

	// Run 2 continues on seat B before run 1's reset, then walls there too.
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		status = waitLiveStatus(t, binary, stateRoot, taskID, 30*time.Second)
		runs = liveFallbackRuns(t, stateRoot)
		if len(runs) == 2 && runs[1].Ended && status.State == StateWaiting {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(runs) != 2 {
		t.Fatalf("runs = %+v, want two settled runs after the fallback wall", runs)
	}
	if runs[1].Seat != "seat-b" || runs[1].Outcome != RunWall {
		t.Fatalf("second run = %+v, want a wall on the fallback seat-b", runs[1])
	}
	if !runs[1].CreatedAt.Before(firstReset) {
		t.Fatalf("second run created %s, want it before run 1's reset %s", runs[1].CreatedAt, firstReset)
	}
	// Both pools walled: the task waits for the wall reset with exactly two runs.
	if status.State != StateWaiting || status.WaitKind != WaitTime || status.WakeAt == nil {
		t.Fatalf("all-walled status = %+v, want a timed wait", status)
	}
	secondReset := liveRunWallWakeAt(t, runs[1])
	if !status.WakeAt.Equal(secondReset) {
		t.Fatalf("wake at %s, want the wall reset %s", status.WakeAt, secondReset)
	}
	if wall, _ := endpoint.counts(); wall < 2 {
		t.Fatalf("controlled endpoint saw %d wall requests, want both seats to hit it", wall)
	}
}

func TestFiniteLiveCompiledHermesQuotaWallFallbackClosedSeat(t *testing.T) {
	if os.Getenv("WERKRBY_FINITE_HERMES_LIVE") != "1" {
		t.Skip("compiled live Hermes quota fallback requires explicit opt-in")
	}
	binary := liveBinary(t)
	home := liveWallFallbackHome(t)
	endpoint := newLiveControlledEndpoint(t)
	writeLiveProfile(t, home, "wk-finite-wall-a", endpoint.URL+"/wall/v1")
	writeLiveProfile(t, home, "wk-finite-wall-b", endpoint.URL+"/wall/v1")
	root := liveTestRoot(t, "compiled-wall-fallback-closed")
	worker := writeLiveFallbackWorker(t, root, "wk-finite-wall-a", "wk-finite-wall-b")
	availability := writeLiveAvailability(t, root, false)
	taskID := "compiled-wall-fbc"
	stateRoot := filepath.Join(root, "state")
	brief := writeLiveFallbackBrief(t, root, taskID, "seat-a")
	owner := startLiveCommand(t, binary, []string{
		"serve", "--root", stateRoot, "--worker", worker, "--roster", availability,
		"--harness", "herdr", "--model", "@seat-a", "--poll-interval", "25ms", "--stop-grace", "25ms",
	}, liveEnv(home), filepath.Join(root, "owner.log"))
	defer owner.terminate(t)
	waitForFixture(t, func() bool { return fileExists(filepath.Join(stateRoot, "control.json")) }, "compiled owner control publication")
	runLiveControl(t, binary, "intake", "--root", stateRoot, "--brief", brief)

	status := waitLiveState(t, binary, stateRoot, taskID, StateWaiting, 2*time.Minute)
	if status.WaitKind != WaitTime || status.WakeAt == nil {
		t.Fatalf("closed-fallback status = %+v, want a timed wait", status)
	}
	time.Sleep(2 * time.Second)
	runs := liveFallbackRuns(t, stateRoot)
	if len(runs) != 1 || runs[0].Seat != "seat-a" || runs[0].Outcome != RunWall {
		t.Fatalf("closed-fallback runs = %+v, want exactly one walled run on seat-a", runs)
	}
	if current := waitLiveStatus(t, binary, stateRoot, taskID, 30*time.Second); current.State != StateWaiting {
		t.Fatalf("closed-fallback task = %s, want still waiting", current.State)
	}
}
