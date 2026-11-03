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

// liveWallFallbackHomeProfiles are the disposable profiles the fallback wall
// tests run; the home marker must list every one of them.
var liveWallFallbackHomeProfiles = []string{"wk-finite-wall-a", "wk-finite-wall-b", "wk-finite-wall-closed-a", "wk-finite-wall-closed-b", "wk-finite-wall-expired-a", "wk-finite-wall-expired-b", "wk-finite-wall-zone-a", "wk-finite-wall-zone-b"}

func liveWallFallbackHome(t *testing.T) string {
	t.Helper()
	home := os.Getenv("WERKRBY_FINITE_HERMES_WALL_HOME")
	if home == "" || !filepath.IsAbs(home) {
		t.Fatalf("WERKRBY_FINITE_HERMES_WALL_HOME must name an absolute disposable home")
	}
	marker, err := os.ReadFile(filepath.Join(home, ".finite-hermes-disposable"))
	if err != nil {
		t.Fatalf("disposable wall-fallback marker: %v", err)
	}
	lines := strings.Split(string(marker), "\n")
	for _, profile := range liveWallFallbackHomeProfiles {
		listed := false
		for _, line := range lines {
			listed = listed || line == profile
		}
		if !listed {
			t.Fatalf("disposable wall-fallback marker does not list profile %s", profile)
		}
	}
	return home
}

// resetLiveProfile drops a stale profile config so the test can republish it
// against this run's endpoint port. The rest of the profile dir — including
// the prepared isolated Hermes runtime — is kept, so warming survives.
func resetLiveProfile(t *testing.T, home, profile string) {
	t.Helper()
	_ = os.Remove(filepath.Join(home, "profiles", profile, "config.yaml"))
}

// writeLiveFallbackProfile publishes a disposable profile whose secret stream
// drops the shared telegram token: two seat gateways live side by side in this
// test, and one platform token may serve only one gateway. Provider keys stay.
func writeLiveFallbackProfile(t *testing.T, home, profile, baseURL string) {
	t.Helper()
	bridge, err := liveSecretBridgePath()
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(home, "profiles", profile)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	secrets := fmt.Sprintf("/bin/sh -c %q", bridge+" --dotenv | grep -v '^TELEGRAM_'")
	config := fmt.Sprintf(`model:
  provider: zai
  model: zai/glm-5.3-flash
  base_url: %s
secrets:
  command:
    command: %q
    enabled: true
    override_existing: false
memory:
  memory_enabled: false
  user_profile_enabled: false
platform_toolsets:
  cli:
    - terminal
    - file
mcp_servers: {}
plugins:
  disabled:
    - kanban
`, baseURL, secrets)
	path := filepath.Join(dir, "config.yaml")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("write disposable profile: %v", err)
	}
	if _, err := file.WriteString(config); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
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

// writeLiveAvailabilityExpiring writes seat-b open with an available_until,
// quoted the way the roster writer's yaml.safe_dump emits it, so the compiled
// engine must decode the real writer form and expire the seat at it.
func writeLiveAvailabilityExpiring(t *testing.T, root string, until time.Time) string {
	t.Helper()
	path := filepath.Join(root, "availability.yaml")
	roster := "seats:\n" +
		"  - name: seat-a\n    available: true\n    fallback: seat-b\n    pool: pool-a\n" +
		"  - name: seat-b\n    available: true\n    pool: pool-b\n" +
		"    available_until: '" + until.UTC().Format("2006-01-02T15:04:05Z") + "'\n"
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

func setLiveAvailability(t *testing.T, path string, seatBAvailable bool) {
	t.Helper()
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
}

// TestFiniteLiveCompiledHermesQuotaWallProviderZone: a real Hermes turn whose
// provider prints the reset in its own UTC+8 zone wakes at the printed
// instant, not eight hours late.
func TestFiniteLiveCompiledHermesQuotaWallProviderZone(t *testing.T) {
	if os.Getenv("WERKRBY_FINITE_HERMES_LIVE") != "1" {
		t.Skip("compiled live Hermes quota fallback requires explicit opt-in")
	}
	binary := liveBinary(t)
	home := liveWallFallbackHome(t)
	endpoint := newLiveControlledEndpointZone(t, time.FixedZone("UTC+8", 8*3600))
	resetLiveProfile(t, home, "wk-finite-wall-zone-a")
	resetLiveProfile(t, home, "wk-finite-wall-zone-b")
	writeLiveFallbackProfile(t, home, "wk-finite-wall-zone-a", endpoint.URL+"/wall/v1")
	writeLiveFallbackProfile(t, home, "wk-finite-wall-zone-b", endpoint.URL+"/wall/v1")
	root := liveTestRoot(t, "compiled-wall-zone")
	worker := writeLiveFallbackWorker(t, root, "wk-finite-wall-zone-a", "wk-finite-wall-zone-b")
	availability := filepath.Join(root, "availability.yaml")
	// The zone roster: seat-a spends the UTC+8 pool and carries no
	// quota_reset, so only the provider text can date the wall.
	roster := "seats:\n" +
		"  - name: seat-a\n    available: true\n    fallback: seat-b\n    pool: zai-live\n" +
		"  - name: seat-b\n    available: false\n    pool: pool-b\n"
	if err := os.WriteFile(availability, []byte(roster), 0o600); err != nil {
		t.Fatal(err)
	}
	taskID := "compiled-wall-zone"
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
		t.Fatalf("zone wall status = %+v, want a timed provider wait", status)
	}
	runs := liveFallbackRuns(t, stateRoot)
	if len(runs) != 1 || runs[0].Seat != "seat-a" || runs[0].Outcome != RunWall || !runs[0].Ended {
		t.Fatalf("zone wall runs = %+v, want one settled wall on seat-a", runs)
	}
	if !status.WakeAt.Equal(endpoint.reset) {
		t.Fatalf("zone wall wake = %s, want the printed reset %s, not the UTC misread %s", status.WakeAt, endpoint.reset, endpoint.reset.Add(8*time.Hour))
	}
	if wall, _ := endpoint.counts(); wall < 1 {
		t.Fatal("controlled endpoint saw no real Hermes wall request")
	}
}

func TestFiniteLiveCompiledHermesQuotaWallFallback(t *testing.T) {
	if os.Getenv("WERKRBY_FINITE_HERMES_LIVE") != "1" {
		t.Skip("compiled live Hermes quota fallback requires explicit opt-in")
	}
	binary := liveBinary(t)
	home := liveWallFallbackHome(t)
	endpoint := newLiveControlledEndpoint(t)
	resetLiveProfile(t, home, "wk-finite-wall-a")
	resetLiveProfile(t, home, "wk-finite-wall-b")
	writeLiveFallbackProfile(t, home, "wk-finite-wall-a", endpoint.URL+"/wall/v1")
	writeLiveFallbackProfile(t, home, "wk-finite-wall-b", endpoint.URL+"/wall/v1")
	root := liveTestRoot(t, "compiled-wall-fallback")
	worker := writeLiveFallbackWorker(t, root, "wk-finite-wall-a", "wk-finite-wall-b")
	availability := filepath.Join(root, "availability.yaml")
	setLiveAvailability(t, availability, false) // seat B stays closed until run 1 has walled
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

	// Run 1 walls on seat A, and the closed fallback keeps the task waiting.
	status := waitLiveState(t, binary, stateRoot, taskID, StateWaiting, 2*time.Minute)
	if status.WaitKind != WaitTime || status.WakeAt == nil {
		t.Fatalf("wall status = %+v, want a timed provider wait", status)
	}
	runs := liveFallbackRuns(t, stateRoot)
	if len(runs) != 1 || runs[0].Seat != "seat-a" || runs[0].Outcome != RunWall || !runs[0].Ended {
		t.Fatalf("first runs = %+v, want one settled wall on seat-a", runs)
	}
	firstReset := liveRunWallWakeAt(t, runs[0])

	// Seat B opens: run 2 continues there before run 1's reset, then walls too.
	setLiveAvailability(t, availability, true)
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
	resetLiveProfile(t, home, "wk-finite-wall-closed-a")
	resetLiveProfile(t, home, "wk-finite-wall-closed-b")
	writeLiveFallbackProfile(t, home, "wk-finite-wall-closed-a", endpoint.URL+"/wall/v1")
	writeLiveFallbackProfile(t, home, "wk-finite-wall-closed-b", endpoint.URL+"/wall/v1")
	root := liveTestRoot(t, "compiled-wall-fallback-closed")
	worker := writeLiveFallbackWorker(t, root, "wk-finite-wall-closed-a", "wk-finite-wall-closed-b")
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

func TestFiniteLiveCompiledHermesQuotaWallFallbackExpiredSeat(t *testing.T) {
	if os.Getenv("WERKRBY_FINITE_HERMES_LIVE") != "1" {
		t.Skip("compiled live Hermes quota fallback requires explicit opt-in")
	}
	binary := liveBinary(t)
	home := liveWallFallbackHome(t)
	endpoint := newLiveControlledEndpoint(t)
	resetLiveProfile(t, home, "wk-finite-wall-expired-a")
	resetLiveProfile(t, home, "wk-finite-wall-expired-b")
	writeLiveFallbackProfile(t, home, "wk-finite-wall-expired-a", endpoint.URL+"/wall/v1")
	writeLiveFallbackProfile(t, home, "wk-finite-wall-expired-b", endpoint.URL+"/wall/v1")
	root := liveTestRoot(t, "compiled-wall-fallback-expired")
	worker := writeLiveFallbackWorker(t, root, "wk-finite-wall-expired-a", "wk-finite-wall-expired-b")
	availability := writeLiveAvailabilityExpiring(t, root, time.Now().Add(-time.Hour))
	taskID := "compiled-wall-fbe"
	stateRoot := filepath.Join(root, "state")
	brief := writeLiveFallbackBrief(t, root, taskID, "seat-a")
	owner := startLiveCommand(t, binary, []string{
		"serve", "--root", stateRoot, "--worker", worker, "--roster", availability,
		"--harness", "herdr", "--model", "@seat-a", "--poll-interval", "25ms", "--stop-grace", "25ms",
	}, liveEnv(home), filepath.Join(root, "owner.log"))
	defer owner.terminate(t)
	waitForFixture(t, func() bool { return fileExists(filepath.Join(stateRoot, "control.json")) }, "compiled owner control publication")
	runLiveControl(t, binary, "intake", "--root", stateRoot, "--brief", brief)

	// Run 1 walls on seat A; the fallback reads open but its available_until
	// has passed, so the engine must expire it and keep the task waiting.
	status := waitLiveState(t, binary, stateRoot, taskID, StateWaiting, 2*time.Minute)
	if status.WaitKind != WaitTime || status.WakeAt == nil {
		t.Fatalf("expired-fallback status = %+v, want a timed wait", status)
	}
	time.Sleep(2 * time.Second)
	runs := liveFallbackRuns(t, stateRoot)
	if len(runs) != 1 || runs[0].Seat != "seat-a" || runs[0].Outcome != RunWall {
		t.Fatalf("expired-fallback runs = %+v, want exactly one walled run on seat-a", runs)
	}
	if current := waitLiveStatus(t, binary, stateRoot, taskID, 30*time.Second); current.State != StateWaiting {
		t.Fatalf("expired-fallback task = %s, want still waiting", current.State)
	}
}
