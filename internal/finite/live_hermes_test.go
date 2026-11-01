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
