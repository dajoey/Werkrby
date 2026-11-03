package finite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/harness"
	_ "github.com/ncruces/go-sqlite3/driver"
)

type liveControlledEndpoint struct {
	*httptest.Server
	mu           sync.Mutex
	wallRequests int
	holdRequests int
	holdStarted  chan struct{}
	holdOnce     sync.Once
	releaseHold  chan struct{}
}

func newLiveControlledEndpoint(t *testing.T) *liveControlledEndpoint {
	t.Helper()
	endpoint := &liveControlledEndpoint{holdStarted: make(chan struct{}), releaseHold: make(chan struct{})}
	endpoint.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case strings.Contains(req.URL.Path, "/wall/"):
			endpoint.mu.Lock()
			endpoint.wallRequests++
			endpoint.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			reset := time.Now().Add(2 * time.Hour).UTC().Format("2006-01-02 15:04:05")
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"message": "HTTP 429: quota exhausted. Your limit will reset at " + reset, "type": "rate_limit_error"}})
		case strings.Contains(req.URL.Path, "/hold/"):
			if req.Method != http.MethodPost || !strings.Contains(req.URL.Path, "chat/completions") {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"object":"list","data":[]}`))
				return
			}
			endpoint.mu.Lock()
			endpoint.holdRequests++
			endpoint.mu.Unlock()
			endpoint.holdOnce.Do(func() { close(endpoint.holdStarted) })
			select {
			case <-req.Context().Done():
			case <-endpoint.releaseHold:
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(func() {
		close(endpoint.releaseHold)
		endpoint.Close()
	})
	return endpoint
}

func (e *liveControlledEndpoint) counts() (wall, hold int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.wallRequests, e.holdRequests
}

type liveCommandProcess struct {
	cmd      *exec.Cmd
	done     chan error
	finished chan struct{}
	waitOnce sync.Once
	log      *os.File
}

func startLiveCommand(t *testing.T, binary string, args []string, env []string, logPath string) *liveCommandProcess {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
		t.Fatal(err)
	}
	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, args...)
	command.Env = env
	command.Stdout = log
	command.Stderr = log
	process := &liveCommandProcess{cmd: command, done: make(chan error, 1), finished: make(chan struct{}), log: log}
	if err := command.Start(); err != nil {
		_ = log.Close()
		t.Fatal(err)
	}
	go func() {
		process.done <- command.Wait()
		close(process.finished)
	}()
	t.Cleanup(func() {
		if command.ProcessState == nil || !command.ProcessState.Exited() {
			_ = command.Process.Kill()
		}
		process.waitOnce.Do(func() { <-process.done })
		_ = log.Close()
	})
	return process
}

func (p *liveCommandProcess) kill(t *testing.T) {
	t.Helper()
	if p.cmd.ProcessState == nil || !p.cmd.ProcessState.Exited() {
		if err := p.cmd.Process.Kill(); err != nil {
			t.Fatal(err)
		}
	}
	p.waitOnce.Do(func() { <-p.done })
}

func (p *liveCommandProcess) terminate(t *testing.T) {
	t.Helper()
	if p.cmd.ProcessState != nil && p.cmd.ProcessState.Exited() {
		p.waitOnce.Do(func() { <-p.done })
		return
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.finished:
		p.waitOnce.Do(func() { <-p.done })
	case <-time.After(5 * time.Second):
		p.kill(t)
	}
}

func liveTestRoot(t *testing.T, name string) string {
	t.Helper()
	if evidence := os.Getenv("WERKRBY_FINITE_HERMES_EVIDENCE"); evidence != "" {
		root := filepath.Join(evidence, name)
		if err := os.MkdirAll(evidence, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(root, 0o700); err != nil {
			t.Fatalf("refuse to reuse live evidence %s: %v", root, err)
		}
		return root
	}
	return t.TempDir()
}

func liveBinary(t *testing.T) string {
	t.Helper()
	path := os.Getenv("WERKRBY_FINITE_COMMAND")
	if path == "" || !filepath.IsAbs(path) {
		t.Fatal("WERKRBY_FINITE_COMMAND must name the compiled finite binary")
	}
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		t.Fatalf("compiled finite binary: %v", err)
	}
	return path
}

func liveHome(t *testing.T, env, profile string) string {
	t.Helper()
	home := os.Getenv(env)
	if home == "" || !filepath.IsAbs(home) {
		t.Fatalf("%s must name an absolute disposable home", env)
	}
	marker, err := os.ReadFile(filepath.Join(home, ".finite-hermes-disposable"))
	if err != nil || string(marker) != profile+"\n" {
		t.Fatalf("disposable marker for %s: %v", profile, err)
	}
	return home
}

func liveSecretBridgePath() (string, error) {
	path := os.Getenv("WERKRBY_FINITE_HERMES_SECRET_BRIDGE")
	if path == "" || !filepath.IsAbs(path) {
		return "", fmt.Errorf("setup required: set WERKRBY_FINITE_HERMES_SECRET_BRIDGE to the absolute Hermes dotenv secret bridge executable")
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("setup required: Hermes secret bridge is not a readable regular executable: %w", err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("setup required: Hermes secret bridge is not a regular executable")
	}
	return path, nil
}

func writeLiveProfile(t *testing.T, home, profile, baseURL string) {
	t.Helper()
	bridge, err := liveSecretBridgePath()
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(home, "profiles", profile)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
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
`, baseURL, bridge+" --dotenv")
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

func writeLiveWorker(t *testing.T, root, profile string) string {
	t.Helper()
	worker := filepath.Join(root, "worker")
	if err := os.MkdirAll(filepath.Join(worker, ".werkrby"), 0o700); err != nil {
		t.Fatal(err)
	}
	roster := fmt.Sprintf("staff:\n  pilot:\n    runner: hermes\n    headless: true\n    args: [\"-p\", %q, \"--model\", \"zai/glm-5.3-flash\"]\n", profile)
	if err := os.WriteFile(filepath.Join(worker, ".werkrby", "roster.yaml"), []byte(roster), 0o600); err != nil {
		t.Fatal(err)
	}
	return worker
}

func writeLiveBrief(t *testing.T, root, id string) string {
	t.Helper()
	brief := fmt.Sprintf(`---
id: %s
title: Compiled finite Hermes acceptance
risk: low
review_required: false
---
This is a disposable protocol test. Do not inspect or modify unrelated files.
`, id)
	path := filepath.Join(root, "brief.md")
	if err := os.WriteFile(path, []byte(brief), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func liveEnv(home string) []string {
	return append(os.Environ(), "HERMES_HOME="+home)
}

func runLiveControl(t *testing.T, binary string, args ...string) ControlStatus {
	t.Helper()
	command := exec.Command(binary, args...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %v: %s", strings.Join(args, " "), err, output)
	}
	var status ControlStatus
	if err := json.Unmarshal(output, &status); err != nil {
		t.Fatalf("decode control status %s: %v", output, err)
	}
	return status
}

func waitLiveStatus(t *testing.T, binary, stateRoot, taskID string, timeout time.Duration) ControlStatus {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		command := exec.Command(binary, "status", "--root", stateRoot, "--task", taskID)
		output, err := command.CombinedOutput()
		if err == nil {
			var status ControlStatus
			if json.Unmarshal(output, &status) == nil {
				return status
			}
		}
		last = err
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("control did not become ready: %v", last)
	return ControlStatus{}
}

func waitLiveState(t *testing.T, binary, stateRoot, taskID string, desired State, timeout time.Duration) ControlStatus {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last ControlStatus
	for time.Now().Before(deadline) {
		last = waitLiveStatus(t, binary, stateRoot, taskID, timeout)
		if last.State == desired {
			return last
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("task did not reach %s: %+v", desired, last)
	return ControlStatus{}
}

func startLiveServe(t *testing.T, binary, root, worker, home, receiver, logName string) *liveCommandProcess {
	t.Helper()
	args := []string{"serve", "--root", root, "--worker", worker, "--seats", "wk-finite-test", "--harness", "herdr", "--model", "@pilot", "--poll-interval", "25ms", "--stop-grace", "25ms"}
	if receiver != "" {
		args = append(args, "--receiver-config", receiver)
	}
	process := startLiveCommand(t, binary, args, liveEnv(home), filepath.Join(root, logName))
	waitForFixture(t, func() bool { return fileExists(filepath.Join(root, "control.json")) }, "compiled owner control publication")
	return process
}

type liveRunRow struct {
	ID           int64
	Outcome      RunOutcome
	RunDir       string
	ProcessToken string
	Ended        bool
}

func liveRuns(t *testing.T, root string) []liveRunRow {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+filepath.ToSlash(filepath.Join(root, "engine.db"))+"?mode=ro&_pragma=busy_timeout(2000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT id,COALESCE(outcome,''),run_dir,COALESCE(process_token,''),ended_at IS NOT NULL FROM runs ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []liveRunRow
	for rows.Next() {
		var run liveRunRow
		if err := rows.Scan(&run.ID, &run.Outcome, &run.RunDir, &run.ProcessToken, &run.Ended); err != nil {
			t.Fatal(err)
		}
		result = append(result, run)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func liveAdmission(t *testing.T, run liveRunRow) harness.HermesBoundaryObservation {
	t.Helper()
	observation, err := harness.InspectHermesRunBoundary(context.Background(), harness.RunBoundarySpec{RunID: run.ID, ProcessToken: run.ProcessToken, RunDir: run.RunDir})
	if err != nil {
		t.Fatalf("inspect run %d: %v", run.ID, err)
	}
	return observation
}

func waitLiveAdmission(t *testing.T, run liveRunRow, desired harness.BoundaryLiveness, timeout time.Duration) harness.HermesBoundaryObservation {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var observation harness.HermesBoundaryObservation
	var err error
	for time.Now().Before(deadline) {
		observation, err = harness.InspectHermesRunBoundary(context.Background(), harness.RunBoundarySpec{RunID: run.ID, ProcessToken: run.ProcessToken, RunDir: run.RunDir})
		if err == nil && observation.Liveness == desired && observation.Identity != nil {
			return observation
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("run %d did not reach %s: %+v, %v", run.ID, desired, observation, err)
	return observation
}

func TestFiniteLiveCompiledHermesQuotaWall(t *testing.T) {
	if os.Getenv("WERKRBY_FINITE_HERMES_LIVE") != "1" {
		t.Skip("compiled live Hermes wall requires explicit opt-in")
	}
	binary := liveBinary(t)
	home := liveHome(t, "WERKRBY_FINITE_HERMES_WALL_HOME", "wk-finite-wall")
	endpoint := newLiveControlledEndpoint(t)
	writeLiveProfile(t, home, "wk-finite-wall", endpoint.URL+"/wall/v1")
	root := liveTestRoot(t, "compiled-wall")
	worker := writeLiveWorker(t, root, "wk-finite-wall")
	taskID := "compiled-wall"
	stateRoot := filepath.Join(root, "state")
	brief := writeLiveBrief(t, root, taskID)
	owner := startLiveServe(t, binary, stateRoot, worker, home, "", "owner.log")
	defer owner.terminate(t)
	runLiveControl(t, binary, "intake", "--root", stateRoot, "--brief", brief)
	status := waitLiveState(t, binary, stateRoot, taskID, StateWaiting, 2*time.Minute)
	if status.WaitKind != WaitTime || status.WakeAt == nil {
		t.Fatalf("wall status = %+v, want a timed provider wait", status)
	}
	runs := liveRuns(t, stateRoot)
	if len(runs) != 1 || runs[0].Outcome != RunWall || !runs[0].Ended {
		t.Fatalf("wall runs = %+v, want one settled RunWall", runs)
	}
	execution, err := os.ReadFile(filepath.Join(runs[0].RunDir, "execution.json"))
	if err != nil || !strings.Contains(string(execution), `"outcome":"wall"`) || !strings.Contains(string(execution), `"wall_wake_at"`) {
		t.Fatalf("provider-produced wall record = %s, %v", execution, err)
	}
	usage, err := os.ReadFile(filepath.Join(runs[0].RunDir, "usage.json"))
	if err != nil || !strings.Contains(string(usage), `"source":"hermes"`) {
		t.Fatalf("wall usage = %s, %v", usage, err)
	}
	observation := liveAdmission(t, runs[0])
	if observation.Liveness != harness.BoundaryAbsent || observation.Outcome != "failed" || observation.Identity == nil {
		t.Fatalf("wall admission = %+v, want exact failed terminal absence", observation)
	}
	if count, _ := endpoint.counts(); count < 1 {
		t.Fatal("controlled endpoint saw no real Hermes wall request")
	}
}

func TestFiniteLiveCompiledHermesForceKillStopDelivery(t *testing.T) {
	if os.Getenv("WERKRBY_FINITE_HERMES_LIVE") != "1" {
		t.Skip("compiled live Hermes recovery requires explicit opt-in")
	}
	binary := liveBinary(t)
	home := liveHome(t, "WERKRBY_FINITE_HERMES_HOME", "wk-finite-test")
	endpoint := newLiveControlledEndpoint(t)
	writeLiveProfile(t, home, "wk-finite-test", endpoint.URL+"/hold/v1")
	root := liveTestRoot(t, "compiled-recovery")
	worker := writeLiveWorker(t, root, "wk-finite-test")
	taskID := "compiled-recovery"
	stateRoot := filepath.Join(root, "state")
	brief := writeLiveBrief(t, root, taskID)
	receiverRoot := filepath.Join(root, "receiver")
	receiver := startLiveCommand(t, binary, []string{"test-receiver", "--root", receiverRoot}, liveEnv(home), filepath.Join(root, "receiver.log"))
	defer receiver.terminate(t)
	waitForFixture(t, func() bool { return fileExists(filepath.Join(receiverRoot, "receiver.json")) }, "compiled receiver rendezvous")
	owner := startLiveServe(t, binary, stateRoot, worker, home, filepath.Join(receiverRoot, "receiver.json"), "owner-first.log")
	runLiveControl(t, binary, "intake", "--root", stateRoot, "--brief", brief)
	select {
	case <-endpoint.holdStarted:
	case <-time.After(2 * time.Minute):
		t.Fatal("real Hermes hold request did not start")
	}
	runs := liveRuns(t, stateRoot)
	if len(runs) != 1 || runs[0].Ended {
		t.Fatalf("held runs = %+v, want one open run", runs)
	}
	observation := waitLiveAdmission(t, runs[0], harness.BoundaryLive, 30*time.Second)
	if observation.Liveness != harness.BoundaryLive || observation.Identity == nil {
		t.Fatalf("held admission = %+v, want exact live identity", observation)
	}
	identityBefore := observation.Identity
	supervisorBefore, err := os.ReadFile(filepath.Join(runs[0].RunDir, "supervisor-started.json"))
	if err != nil {
		t.Fatal(err)
	}
	owner.kill(t)
	owner = startLiveServe(t, binary, stateRoot, worker, home, filepath.Join(receiverRoot, "receiver.json"), "owner-restarted.log")
	_ = owner
	status := waitLiveStatus(t, binary, stateRoot, taskID, time.Minute)
	supervisorAfter, err := os.ReadFile(filepath.Join(runs[0].RunDir, "supervisor-started.json"))
	if err != nil || string(supervisorBefore) != string(supervisorAfter) {
		t.Fatalf("owner recovery replaced the supervisor: %v", err)
	}
	observation = waitLiveAdmission(t, runs[0], harness.BoundaryLive, 30*time.Second)
	if observation.Liveness != harness.BoundaryLive || observation.Identity == nil ||
		observation.Identity.SessionID != identityBefore.SessionID || observation.Identity.RequestID != identityBefore.RequestID ||
		observation.Identity.AdmissionID != identityBefore.AdmissionID || observation.Identity.OwnerEpoch != identityBefore.OwnerEpoch {
		t.Fatalf("recovered admission = %+v, want the identical pre-kill turn %+v", observation.Identity, identityBefore)
	}
	runLiveControl(t, binary, "stop", "--root", stateRoot, "--task", taskID, "--expected-version", fmt.Sprint(status.Version))
	final := waitLiveState(t, binary, stateRoot, taskID, StateCancelled, time.Minute)
	if !final.CancelRequested {
		t.Fatalf("operator stop final = %+v", final)
	}
	runs = liveRuns(t, stateRoot)
	if len(runs) != 1 || runs[0].Outcome != RunInterrupted || !runs[0].Ended {
		t.Fatalf("stopped runs = %+v, want one settled interrupted run", runs)
	}
	observation = waitLiveAdmission(t, runs[0], harness.BoundaryAbsent, 30*time.Second)
	if observation.Liveness != harness.BoundaryAbsent || observation.Identity == nil {
		t.Fatalf("stopped admission = %+v, want exact terminal absence", observation)
	}
	deadline := time.Now().Add(30 * time.Second)
	var deliveries []ReceivedDelivery
	for time.Now().Before(deadline) {
		deliveries, err = ReadReceiverDeliveries(receiverRoot)
		if err != nil {
			t.Fatal(err)
		}
		if len(deliveries) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(deliveries) == 0 {
		t.Fatal("live outbox produced no durable receiver delivery")
	}
	found := false
	for _, delivery := range deliveries {
		if strings.Contains(string(delivery.Payload), taskID) && strings.Contains(string(delivery.Payload), "cancelled") {
			found = true
		}
	}
	if !found {
		t.Fatalf("receiver payloads lack the cancelled task: %+v", deliveries)
	}
	owner.terminate(t)
}

// readLiveGatewayState reads the subset of the hermes home's
// gateway_state.json the live fault test needs: the gateway pid and its state.
func readLiveGatewayState(t *testing.T, hermesHome string) (int, string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(hermesHome, "gateway_state.json"))
	if err != nil {
		t.Fatalf("read gateway_state.json: %v", err)
	}
	var state struct {
		PID          int    `json:"pid"`
		HermesHome   string `json:"hermes_home"`
		GatewayState string `json:"gateway_state"`
	}
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatalf("decode gateway_state.json: %v", err)
	}
	return state.PID, state.GatewayState
}

func liveSettledPayload(t *testing.T, stateRoot string, runID int64) (map[string]any, string) {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+filepath.ToSlash(filepath.Join(stateRoot, "engine.db"))+"?mode=ro&_pragma=busy_timeout(2000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var payload, endedAt string
	if err := db.QueryRow(`SELECT e.payload_json, COALESCE(r.ended_at,'') FROM task_events e
		JOIN runs r ON r.id=e.run_id WHERE e.run_id=? AND e.kind='settled'`, runID).Scan(&payload, &endedAt); err != nil {
		t.Fatalf("run %d settled event: %v", runID, err)
	}
	var value map[string]any
	if err := json.Unmarshal([]byte(payload), &value); err != nil {
		t.Fatalf("decode settled payload of run %d: %v", runID, err)
	}
	return value, endedAt
}

func liveEndedAt(t *testing.T, stateRoot string, runID int64) time.Time {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+filepath.ToSlash(filepath.Join(stateRoot, "engine.db"))+"?mode=ro&_pragma=busy_timeout(2000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var ended string
	if err := db.QueryRow(`SELECT COALESCE(ended_at,'') FROM runs WHERE id=?`, runID).Scan(&ended); err != nil {
		t.Fatalf("run %d ended_at: %v", runID, err)
	}
	parsed, err := time.Parse(time.RFC3339Nano, ended)
	if err != nil {
		t.Fatalf("parse ended_at %q of run %d: %v", ended, runID, err)
	}
	return parsed
}

func logLiveFaultDiagnostics(t *testing.T, run liveRunRow) {
	t.Helper()
	entries, err := os.ReadDir(run.RunDir)
	if err == nil {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Logf("run %d directory: %v", run.ID, names)
	}
	for _, name := range []string{"usage.json", "hermes-usage.json", "completion.json", "provider.json"} {
		if data, err := os.ReadFile(filepath.Join(run.RunDir, name)); err == nil {
			t.Logf("run %d %s: %s", run.ID, name, data)
		}
	}
	observation, admissionErr := harness.InspectHermesRunBoundary(context.Background(), harness.RunBoundarySpec{RunID: run.ID, ProcessToken: run.ProcessToken, RunDir: run.RunDir})
	t.Logf("run %d admission observation = %+v, %v", run.ID, observation, admissionErr)
}

// A compiled end-to-end proof on a real `hermes -z`: the disposable Hermes
// gateway is SIGTERMed while a model request is in flight, the way the
// November 1 engine gateway restart hit production runs 9 to 18. The run must
// settle uncharged (interrupted, class hermes-no-spend) and wait on a backoff
// timer about 30 s after it ended, never as a charged failure.
func TestFiniteLiveCompiledHermesGatewayFaultUncharged(t *testing.T) {
	if os.Getenv("WERKRBY_FINITE_HERMES_LIVE") != "1" {
		t.Skip("compiled live Hermes gateway fault requires explicit opt-in")
	}
	binary := liveBinary(t)
	home := liveHome(t, "WERKRBY_FINITE_HERMES_FAULT_HOME", "wk-finite-fault")
	endpoint := newLiveControlledEndpoint(t)
	writeLiveProfile(t, home, "wk-finite-fault", endpoint.URL+"/hold/v1")
	root := liveTestRoot(t, "compiled-gateway-fault")
	worker := writeLiveWorker(t, root, "wk-finite-fault")
	taskID := "compiled-gateway-fault"
	stateRoot := filepath.Join(root, "state")
	brief := writeLiveBrief(t, root, taskID)
	owner := startLiveServe(t, binary, stateRoot, worker, home, "", "owner.log")
	runLiveControl(t, binary, "intake", "--root", stateRoot, "--brief", brief)
	select {
	case <-endpoint.holdStarted:
	case <-time.After(2 * time.Minute):
		t.Fatal("real Hermes hold request did not start")
	}
	runs := liveRuns(t, stateRoot)
	if len(runs) != 1 || runs[0].Ended {
		t.Fatalf("held runs = %+v, want one open run", runs)
	}
	providerData, err := os.ReadFile(filepath.Join(runs[0].RunDir, "hermes-provider.json"))
	if err != nil {
		t.Fatalf("open run has no hermes-provider.json: %v", err)
	}
	var provider struct {
		HermesHome string `json:"hermes_home"`
	}
	if err := json.Unmarshal(providerData, &provider); err != nil || provider.HermesHome == "" {
		t.Fatalf("hermes-provider.json hermes_home = %q, %v", provider.HermesHome, err)
	}
	gatewayPID, gatewayState := readLiveGatewayState(t, provider.HermesHome)
	t.Logf("disposable gateway pid %d state %s", gatewayPID, gatewayState)
	cmdline, cmdlineErr := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", gatewayPID))
	if cmdlineErr != nil || !strings.Contains(string(cmdline), "gateway") {
		t.Fatalf("pid %d is not the hermes gateway: cmdline=%q err=%v", gatewayPID, cmdline, cmdlineErr)
	}
	if err := syscall.Kill(gatewayPID, syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM gateway pid %d: %v", gatewayPID, err)
	}
	// Wait for the task to leave working, then branch on what the run shows.
	deadline := time.Now().Add(5 * time.Minute)
	var status ControlStatus
	for time.Now().Before(deadline) {
		status = waitLiveStatus(t, binary, stateRoot, taskID, time.Minute)
		if status.State != StateWorking {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("task left working into state %+v", status)
	runs = liveRuns(t, stateRoot)
	if len(runs) == 0 {
		t.Fatal("no runs recorded")
	}
	first := runs[0]
	if !first.Ended {
		logLiveFaultDiagnostics(t, first)
		t.Fatalf("first run %d never settled within the wait: %+v", first.ID, first)
	}
	if first.Outcome == RunInterrupted {
		payload, _ := liveSettledPayload(t, stateRoot, first.ID)
		if payload["failure_class"] != "hermes-no-spend" || payload["charged"] != false {
			logLiveFaultDiagnostics(t, first)
			t.Fatalf("run %d settled payload = %v, want hermes-no-spend uncharged", first.ID, payload)
		}
		if status.State != StateWaiting || status.WakeAt == nil {
			logLiveFaultDiagnostics(t, first)
			t.Fatalf("task status = %+v, want waiting on a timed backoff", status)
		}
		ended := liveEndedAt(t, stateRoot, first.ID)
		delay := status.WakeAt.Sub(ended)
		if delay < 25*time.Second || delay > 35*time.Second {
			logLiveFaultDiagnostics(t, first)
			t.Fatalf("backoff wake %s is %s after the run ended %s, want 25-35 s", status.WakeAt, delay, ended)
		}
	} else {
		// The only other tolerable shape is a charged fail with proven spend;
		// everything else means the no-spend signals do not cover this fault.
		usage, usageErr := os.ReadFile(filepath.Join(first.RunDir, "usage.json"))
		admitted := false
		if os.IsNotExist(usageErr) {
			if observation, admissionErr := harness.InspectHermesRunBoundary(context.Background(), harness.RunBoundarySpec{RunID: first.ID, ProcessToken: first.ProcessToken, RunDir: first.RunDir}); admissionErr == nil {
				admitted = observation.Identity != nil
			}
		}
		logLiveFaultDiagnostics(t, first)
		if first.Outcome == RunFail && usageErr == nil && !liveUsageSpent(t, usage) {
			t.Fatalf("run %d settled fail with an unspent usage record: %s", first.ID, usage)
		}
		if first.Outcome == RunFail && usageErr != nil && !admitted {
			t.Fatalf("run %d settled fail with no usage record and a non-admitted prompt", first.ID)
		}
		t.Fatalf("run %d settled as %q: the no-spend signals do not hold for a gateway restart", first.ID, first.Outcome)
	}
	owner.terminate(t)
	// Any gateway the disposable home still records is SIGTERMed on the way out.
	if pid, state := readLiveGatewayState(t, provider.HermesHome); pid > 0 && state == "running" {
		t.Logf("SIGTERM lingering disposable gateway pid %d", pid)
		_ = syscall.Kill(pid, syscall.SIGTERM)
	}
}

func liveUsageSpent(t *testing.T, data []byte) bool {
	t.Helper()
	var record engineUsageRecord
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatalf("decode live usage.json: %v", err)
	}
	spent := func(v *int64) bool { return v != nil && *v != 0 }
	return spent(record.FreshInputTokens) || spent(record.CacheReadTokens) || spent(record.CacheWriteTokens) ||
		spent(record.OutputTokens) || spent(record.ReasoningTokens) || spent(record.APICalls) || spent(record.ToolCalls)
}
