package finite

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// fakeGatewayDeps records every engine-boundary call in order and fails the
// gates the test configures. Nothing here touches systemd, Hermes, or /proc.
type fakeGatewayDeps struct {
	mu    sync.Mutex
	clock *fakeGatewayClock

	steps []string

	// gateway_state.json reads are consumed in order; the last one repeats.
	states    []gatewayRuntimeState
	statesErr error
	cmdlines  map[int][]string
	alive     map[int]bool
	mcp       gatewayMCPCheck
	mcpErr    error
	inCgroup  bool

	// knobs
	termIgnored        bool
	ensureState        string
	ensureStates       []string // sequenced verdicts; the last one repeats
	ensureErr          error
	systemctlFailAfter int
	serveState         string

	// records
	termSignals   []int
	killSignals   []int
	systemctlOps  []string
	bridgeTouched bool
	bridgeRemoved bool
	ensureCalls   int
}

// fakeGatewayClock advances only when the engine sleeps, so deadline logic is
// deterministic without a real wait.
type fakeGatewayClock struct {
	mu      sync.Mutex
	current time.Time
	step    time.Duration
}

func newFakeGatewayClock() *fakeGatewayClock {
	return &fakeGatewayClock{current: time.Date(2026, 11, 1, 18, 0, 0, 0, time.UTC), step: time.Second}
}

func (c *fakeGatewayClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

func (c *fakeGatewayClock) sleep(ctx context.Context, d time.Duration) error {
	c.mu.Lock()
	c.current = c.current.Add(d)
	c.mu.Unlock()
	return ctx.Err()
}

func newFakeGatewayDeps(states ...gatewayRuntimeState) *fakeGatewayDeps {
	if len(states) == 0 {
		states = []gatewayRuntimeState{{}}
	}
	return &fakeGatewayDeps{
		clock:              newFakeGatewayClock(),
		states:             states,
		cmdlines:           map[int][]string{},
		alive:              map[int]bool{},
		mcp:                gatewayMCPCheck{SummaryLines: 1, Servers: map[string]int{}},
		ensureState:        "ready",
		systemctlFailAfter: -1,
		serveState:         "active",
	}
}

func (f *fakeGatewayDeps) record(step string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recordLocked(step)
}

// recordLocked records with f.mu already held by the calling closure.
func (f *fakeGatewayDeps) recordLocked(step string) {
	f.steps = append(f.steps, step)
}

func (f *fakeGatewayDeps) deps(logf func(string, ...any)) gatewayRestartDeps {
	return gatewayRestartDeps{
		systemctl: func(_ context.Context, args ...string) (string, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.systemctlFailAfter >= 0 && len(f.systemctlOps) >= f.systemctlFailAfter {
				return "", fmt.Errorf("systemctl exploded")
			}
			op := strings.Join(args, " ")
			f.systemctlOps = append(f.systemctlOps, op)
			switch {
			case strings.Contains(op, " is-active "):
				// Real systemctl prints the state and exits 3 for every state
				// but active; the command must read the state, not the code.
				if f.serveState != "active" {
					return f.serveState + "\n", fmt.Errorf("systemctl is-active: exit status 3")
				}
				return f.serveState + "\n", nil
			case strings.Contains(op, " stop "):
				f.serveState = "inactive"
				f.recordLocked("serve-stop")
			case strings.Contains(op, " start "):
				f.serveState = "active"
				f.recordLocked("serve-start")
			}
			return "", nil
		},
		ensureGateway: func(_ context.Context, _ gatewayRestartOptions) (string, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.ensureCalls++
			f.recordLocked("ensure")
			verdict := f.ensureState
			if len(f.ensureStates) > 0 {
				verdict = f.ensureStates[0]
				if len(f.ensureStates) > 1 {
					f.ensureStates = f.ensureStates[1:]
				}
			}
			return verdict, f.ensureErr
		},
		readGatewayState: func(_ context.Context) (gatewayRuntimeState, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.statesErr != nil {
				return gatewayRuntimeState{}, f.statesErr
			}
			state := f.states[0]
			if len(f.states) > 1 {
				f.states = f.states[1:]
			}
			return state, nil
		},
		countSupervisors: func(_ context.Context) (int, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			return 0, nil
		},
		signalProcess: func(pid int, sig syscall.Signal) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			if sig == syscall.SIGTERM {
				f.termSignals = append(f.termSignals, pid)
				f.recordLocked(fmt.Sprintf("term-%d", pid))
				if !f.termIgnored {
					f.alive[pid] = false
				}
			}
			if sig == syscall.SIGKILL {
				f.killSignals = append(f.killSignals, pid)
				f.recordLocked(fmt.Sprintf("kill-%d", pid))
				f.alive[pid] = false
			}
			return nil
		},
		processCmdline: func(_ context.Context, pid int) ([]string, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			args, ok := f.cmdlines[pid]
			if !ok {
				return nil, fmt.Errorf("no /proc/%d", pid)
			}
			return args, nil
		},
		processAlive: func(_ context.Context, pid int) bool {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.alive[pid]
		},
		inServeCgroup: func() bool { return f.inCgroup },
		mcpCheck: func(_ context.Context, _ time.Time) (gatewayMCPCheck, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.mcp, f.mcpErr
		},
		removeBridge: func(_ context.Context) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.recordLocked("bridge-remove")
			f.bridgeRemoved = true
			return nil
		},
		touchBridge: func(_ context.Context) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.recordLocked("bridge-touch")
			f.bridgeTouched = true
			return nil
		},
		now:   f.clock.now,
		sleep: f.clock.sleep,
		logf:  logf,
	}
}

func gatewayFakeState(pid int) gatewayRuntimeState {
	return gatewayRuntimeState{
		PID:          pid,
		Kind:         "hermes-gateway",
		HermesHome:   "/disp/hermes",
		GatewayState: "running",
		ActiveAgents: 0,
		CodeSHA:      "aaa1112223334444555666777888999a",
		StartTime:    1000,
	}
}

func gatewayDrainStates(t *testing.T, pid int, agents ...int) []gatewayRuntimeState {
	t.Helper()
	var states []gatewayRuntimeState
	for _, count := range agents {
		state := gatewayFakeState(pid)
		state.ActiveAgents = count
		states = append(states, state)
	}
	return states
}

func gatewayRestartTestOptions() gatewayRestartOptions {
	return gatewayRestartOptions{
		Root: "/disp/state", HermesHome: "/disp/hermes", RelayDir: "/disp/relay",
		ServeUnit: "werkrby-finite-serve.service", DrainTimeout: time.Minute, DrainPoll: time.Second,
		KillGrace: 5 * time.Second, EnsureTimeout: time.Minute, EnsureRetry: 2 * time.Minute, EnsurePoll: time.Second,
		MCPServers: []string{"spark", "graphiti", "rag"},
	}
}

func TestGatewayRestartWaitsForFullDrain(t *testing.T) {
	states := gatewayDrainStates(t, 400, 2, 1, 0)
	states = append(states, gatewayFakeState(400), gatewayFakeState(401)) // kill read, verify read
	f := newFakeGatewayDeps(states...)
	f.alive[400] = true
	f.cmdlines[400] = []string{"/hermes/gateway", "gateway", "run", "--quiet"}
	f.mcp.Servers = map[string]int{"spark": 36, "graphiti": 4, "rag": 2}
	var log bytes.Buffer
	deps := f.deps(func(format string, args ...any) { fmt.Fprintf(&log, format, args...) })
	options := gatewayRestartTestOptions()
	result, err := runGatewayRestart(context.Background(), options, deps)
	if err != nil {
		t.Fatalf("drained restart failed: %v\n%s", err, log.String())
	}
	if result.OldPID != 400 || result.NewPID != 401 {
		t.Fatalf("result pids = %d/%d, want 400/401", result.OldPID, result.NewPID)
	}
	if got := f.termSignals; len(got) != 1 || got[0] != 400 {
		t.Fatalf("TERM signals = %v, want [400]", got)
	}
	if len(f.killSignals) != 0 {
		t.Fatalf("unexpected KILL signals: %v", f.killSignals)
	}
	assertGatewayOrder(t, f.steps)
	if !f.bridgeTouched {
		t.Fatal("intake was not restored after a verified restart")
	}
}

func TestGatewayRestartAbortsWhenDrainTimesOut(t *testing.T) {
	stuck := gatewayFakeState(400)
	stuck.ActiveAgents = 3
	f := newFakeGatewayDeps(stuck)
	f.alive[400] = true
	f.cmdlines[400] = []string{"/hermes/gateway", "gateway", "run", "--quiet"}
	var log bytes.Buffer
	deps := f.deps(func(format string, args ...any) { fmt.Fprintf(&log, format, args...) })
	options := gatewayRestartTestOptions()
	options.DrainTimeout = 5 * time.Second
	_, err := runGatewayRestart(context.Background(), options, deps)
	if err == nil {
		t.Fatal("restart proceeded with open agent work")
	}
	if !strings.Contains(err.Error(), "drain") {
		t.Fatalf("abort error names the drain: %v", err)
	}
	if len(f.termSignals) != 0 {
		t.Fatalf("the gateway was signalled despite an undrained state: %v", f.termSignals)
	}
	if f.ensureCalls != 0 {
		t.Fatal("gateway ensure ran despite an undrained state")
	}
	for _, op := range f.systemctlOps {
		if strings.Contains(op, " start ") {
			t.Fatalf("serve was resumed after the drain abort: %v", f.systemctlOps)
		}
	}
	if f.bridgeTouched {
		t.Fatal("intake was restored after the drain abort; Werkrby must stay off")
	}
	if !f.bridgeRemoved || !containsOp(f.systemctlOps, " stop ") {
		t.Fatal("the abort did not leave intake removed and serve stopped")
	}
}

func TestGatewayRestartAbortsWhenSupervisorStuck(t *testing.T) {
	f := newFakeGatewayDeps(gatewayFakeState(400))
	f.alive[400] = true
	f.cmdlines[400] = []string{"/hermes/gateway", "gateway", "run", "--quiet"}
	fake := f.deps(func(string, ...any) {})
	// The fake supervisor count never reaches zero.
	fake.countSupervisors = func(context.Context) (int, error) { return 2, nil }
	options := gatewayRestartTestOptions()
	options.DrainTimeout = 5 * time.Second
	_, err := runGatewayRestart(context.Background(), options, fake)
	if err == nil {
		t.Fatal("restart proceeded with a stuck supervisor")
	}
	if !strings.Contains(err.Error(), "drain") {
		t.Fatalf("abort error names the drain: %v", err)
	}
	if len(f.termSignals) != 0 {
		t.Fatalf("TERM reached the gateway with a supervisor alive: %v", f.termSignals)
	}
	if f.bridgeTouched || containsOp(f.systemctlOps, " start ") {
		t.Fatal("Werkrby was resumed after the stuck-supervisor abort")
	}
}

func TestGatewayRestartRefusesDefaultHermesHome(t *testing.T) {
	f := newFakeGatewayDeps(gatewayRuntimeState{})
	deps := f.deps(func(string, ...any) {})
	options := gatewayRestartTestOptions()
	options.HermesHome = filepath.Join(string(filepath.Separator), "home", "someuser", ".hermes")
	if _, err := runGatewayRestart(context.Background(), options, deps); err == nil {
		t.Fatal("the restart accepted the default ~/.hermes home")
	}
	if len(f.systemctlOps) != 0 || len(f.termSignals) != 0 || f.bridgeRemoved {
		t.Fatal("the refusal happened after engine state changed")
	}
}

func TestGatewayRestartRefusesServeCgroupCaller(t *testing.T) {
	f := newFakeGatewayDeps(gatewayFakeState(400))
	f.inCgroup = true
	deps := f.deps(func(string, ...any) {})
	if _, err := runGatewayRestart(context.Background(), gatewayRestartTestOptions(), deps); err == nil {
		t.Fatal("the restart accepted a caller inside the serve cgroup")
	}
	if len(f.systemctlOps) != 0 || len(f.termSignals) != 0 || f.bridgeRemoved {
		t.Fatal("the refusal happened after engine state changed")
	}
}

func TestGatewayRestartRefusesForeignGatewayCmdline(t *testing.T) {
	f := newFakeGatewayDeps(gatewayFakeState(400), gatewayFakeState(400))
	f.alive[400] = true
	f.cmdlines[400] = []string{"/usr/bin/python3", "worker.py"} // not a `gateway run` process
	deps := f.deps(func(string, ...any) {})
	if _, err := runGatewayRestart(context.Background(), gatewayRestartTestOptions(), deps); err == nil {
		t.Fatal("the restart signalled a pid that does not run `gateway run`")
	}
	if len(f.termSignals) != 0 {
		t.Fatalf("TERM reached a foreign process: %v", f.termSignals)
	}
	if !containsOp(f.systemctlOps, " stop ") {
		t.Fatal("the cmdline refusal did not happen at the kill step, after serve was stopped")
	}
}

func TestGatewayRestartEscalatesToKillAfterGrace(t *testing.T) {
	f := newFakeGatewayDeps(gatewayFakeState(400), gatewayFakeState(400), gatewayFakeState(400), gatewayFakeState(401))
	f.alive[400] = true
	f.termIgnored = true // the gateway ignores TERM
	f.cmdlines[400] = []string{"/hermes/gateway", "gateway", "run", "--quiet"}
	f.mcp.Servers = map[string]int{"spark": 36, "graphiti": 4, "rag": 2}
	deps := f.deps(func(string, ...any) {})
	options := gatewayRestartTestOptions()
	options.KillGrace = 50 * time.Millisecond
	result, err := runGatewayRestart(context.Background(), options, deps)
	if err != nil {
		t.Fatalf("restart failed: %v", err)
	}
	if len(f.killSignals) != 1 || f.killSignals[0] != 400 {
		t.Fatalf("KILL signals = %v, want one KILL to 400", f.killSignals)
	}
	if result.NewPID != 401 {
		t.Fatalf("new pid = %d, want 401", result.NewPID)
	}
}

func TestGatewayRestartVerificationGateKeepsServeOff(t *testing.T) {
	for name, states := range map[string][]gatewayRuntimeState{
		"mcp server missing": {gatewayFakeState(400), gatewayFakeState(400), gatewayFakeState(400), gatewayFakeState(401)},
		"mcp cooldown":       {gatewayFakeState(400), gatewayFakeState(400), gatewayFakeState(400), gatewayFakeState(401)},
		"no fresh boot":      {gatewayFakeState(400), gatewayFakeState(400), gatewayFakeState(400), gatewayFakeState(400)},
		"no code sha":        {gatewayFakeState(400), gatewayFakeState(400), gatewayFakeState(400), gatewayFakeState(401)},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFakeGatewayDeps(states...)
			f.alive[400] = true
			f.cmdlines[400] = []string{"/hermes/gateway", "gateway", "run", "--quiet"}
			switch name {
			case "mcp server missing":
				f.mcp.Servers = map[string]int{"spark": 36, "graphiti": 4} // rag never registered
			case "mcp cooldown":
				f.mcp.Servers = map[string]int{"spark": 36, "graphiti": 4, "rag": 2}
				f.mcp.CooldownLines = 2
			case "no fresh boot":
				f.mcp.Servers = map[string]int{"spark": 36, "graphiti": 4, "rag": 2}
			case "no code sha":
				f.mcp.Servers = map[string]int{"spark": 36, "graphiti": 4, "rag": 2}
				f.states[len(f.states)-1].CodeSHA = ""
			}
			deps := f.deps(func(string, ...any) {})
			_, err := runGatewayRestart(context.Background(), gatewayRestartTestOptions(), deps)
			if err == nil {
				t.Fatal("the restart resumed serve despite a failed verification gate")
			}
			for _, op := range f.systemctlOps {
				if strings.Contains(op, " start ") {
					t.Fatalf("serve was started after the failed gate: %v", f.systemctlOps)
				}
			}
			if f.bridgeTouched {
				t.Fatal("intake was restored after the failed gate; Werkrby must stay off")
			}
		})
	}
}

func TestGatewayRestartEnsureNotReadyKeepsServeOff(t *testing.T) {
	f := newFakeGatewayDeps(gatewayFakeState(400), gatewayFakeState(400))
	f.alive[400] = true
	f.cmdlines[400] = []string{"/hermes/gateway", "gateway", "run", "--quiet"}
	f.ensureState = "starting"
	deps := f.deps(func(string, ...any) {})
	if _, err := runGatewayRestart(context.Background(), gatewayRestartTestOptions(), deps); err == nil {
		t.Fatal("the restart accepted a non-ready gateway")
	}
	for _, op := range f.systemctlOps {
		if strings.Contains(op, " start ") {
			t.Fatalf("serve was started despite ensure not reporting ready: %v", f.systemctlOps)
		}
	}
	if f.bridgeTouched {
		t.Fatal("intake was restored despite ensure not reporting ready")
	}
}

func TestGatewayRestartEnsureRetriesDrainingUntilReady(t *testing.T) {
	// The live abort of 2026-11-02T01:43Z: the gateway hermes ensure had just
	// started fatal-exited at boot and ensure answered "draining" (the
	// mid-shutdown owner). Hermes's contract for a draining owner is to wait
	// it out and ensure again; one non-ready verdict must not leave Werkrby
	// off when the next ensure would report ready.
	states := []gatewayRuntimeState{gatewayFakeState(400), gatewayFakeState(400), gatewayFakeState(400), gatewayFakeState(401)}
	f := newFakeGatewayDeps(states...)
	f.alive[400] = true
	f.cmdlines[400] = []string{"/hermes/gateway", "gateway", "run", "--quiet"}
	f.ensureStates = []string{"draining", "ready"}
	f.mcp.Servers = map[string]int{"spark": 36, "graphiti": 12, "rag": 2}
	deps := f.deps(func(string, ...any) {})
	result, err := runGatewayRestart(context.Background(), gatewayRestartTestOptions(), deps)
	if err != nil {
		t.Fatalf("restart failed on a draining-then-ready ensure: %v", err)
	}
	if f.ensureCalls != 2 {
		t.Fatalf("ensure calls = %d, want 2 (one retry after draining)", f.ensureCalls)
	}
	if result.NewPID != 401 {
		t.Fatalf("new pid = %d, want the fresh gateway 401", result.NewPID)
	}
	started := false
	for _, op := range f.systemctlOps {
		if strings.Contains(op, " start ") {
			started = true
		}
	}
	if !started {
		t.Fatalf("serve was never resumed: %v", f.systemctlOps)
	}
	if !f.bridgeTouched {
		t.Fatal("intake was not restored after the retried ensure reported ready")
	}
}

func TestGatewayRestartEnsureNeverReadyAbortsAfterBudget(t *testing.T) {
	// Forever-draining ensure: the restart keeps Werkrby off but must retry
	// within the overall budget and carry the operator evidence (the dead
	// boot's exit_reason) in the abort.
	states := []gatewayRuntimeState{
		gatewayFakeState(400), gatewayFakeState(400), gatewayFakeState(400),
		{PID: 3295507, Kind: "hermes-gateway", HermesHome: "/disp/hermes", GatewayState: "startup_failed",
			ExitReason: "telegram: Telegram bot token already in use by the 'default' profile gateway (PID 1660).; api_server: Port 8642 already in use."},
	}
	f := newFakeGatewayDeps(states...)
	f.alive[400] = true
	f.cmdlines[400] = []string{"/hermes/gateway", "gateway", "run", "--quiet"}
	f.ensureState = "draining"
	deps := f.deps(func(string, ...any) {})
	_, err := runGatewayRestart(context.Background(), gatewayRestartTestOptions(), deps)
	if err == nil {
		t.Fatal("the restart accepted a gateway that never reported ready")
	}
	msg := err.Error()
	if !strings.Contains(msg, `last verdict "draining"`) {
		t.Fatalf("abort does not name the last ensure verdict: %v", err)
	}
	if !strings.Contains(msg, "exit_reason") || !strings.Contains(msg, "Port 8642") {
		t.Fatalf("abort carries no exit_reason evidence for the operator: %v", err)
	}
	if f.ensureCalls < 2 {
		t.Fatalf("ensure was not retried before the abort: %d call(s)", f.ensureCalls)
	}
	for _, op := range f.systemctlOps {
		if strings.Contains(op, " start ") {
			t.Fatalf("serve was started despite ensure never reporting ready: %v", f.systemctlOps)
		}
	}
	if f.bridgeTouched {
		t.Fatal("intake was restored despite ensure never reporting ready")
	}
}

func TestGatewayRestartResultJSON(t *testing.T) {
	states := []gatewayRuntimeState{gatewayFakeState(400), gatewayFakeState(400), gatewayFakeState(400), gatewayFakeState(401)}
	f := newFakeGatewayDeps(states...)
	f.alive[400] = true
	f.cmdlines[400] = []string{"/hermes/gateway", "gateway", "run", "--quiet"}
	f.mcp.Servers = map[string]int{"spark": 36, "rag": 2}
	var stdout bytes.Buffer
	deps := f.deps(func(string, ...any) {})
	options := gatewayRestartTestOptions()
	options.MCPServers = []string{"spark", "rag"}
	result, err := runGatewayRestart(context.Background(), options, deps)
	if err != nil {
		t.Fatalf("restart failed: %v", err)
	}
	if err := writeGatewayRestartResult(&stdout, result); err != nil {
		t.Fatalf("result json: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &decoded); err != nil {
		t.Fatalf("result is not one JSON object: %v: %s", err, stdout.String())
	}
	if decoded["state"] != "restarted" {
		t.Fatalf("result state = %v", decoded["state"])
	}
	if decoded["code_sha"] != f.states[len(f.states)-1].CodeSHA {
		t.Fatalf("result code_sha = %v", decoded["code_sha"])
	}
	if decoded["new_pid"] != float64(401) {
		t.Fatalf("result new_pid = %v", decoded["new_pid"])
	}
}

func assertGatewayOrder(t *testing.T, steps []string) {
	t.Helper()
	position := make(map[string]int, len(steps))
	for i, step := range steps {
		position[step] = i
	}
	order := []string{"bridge-remove", "serve-stop", "ensure", "serve-start", "bridge-touch"}
	last := -1
	for _, name := range order {
		at, ok := position[name]
		if !ok {
			t.Fatalf("step %q never ran; steps = %v", name, steps)
		}
		if at < last {
			t.Fatalf("steps out of order: %v", steps)
		}
		last = at
	}
	// The gateway kill happens after serve is stopped and before ensure.
	termAt := -1
	for i, step := range steps {
		if strings.HasPrefix(step, "term-") {
			termAt = i
			break
		}
	}
	if termAt < position["serve-stop"] || termAt > position["ensure"] {
		t.Fatalf("gateway kill at %d is not between serve-stop (%d) and ensure (%d): %v",
			termAt, position["serve-stop"], position["ensure"], steps)
	}
}

func containsOp(ops []string, needle string) bool {
	for _, op := range ops {
		if strings.Contains(op, needle) {
			return true
		}
	}
	return false
}

// --- simulated restarts against real processes in a disposable root ---

const gatewaySimAgent = `import os, sys, time, signal, json
state_path = os.environ["GATEWAY_SIM_STATE"]
agents_path = os.environ["GATEWAY_SIM_AGENTS"]
home = os.environ["GATEWAY_SIM_HOME"]
if os.environ.get("GATEWAY_SIM_IGNORE_TERM") == "1":
    signal.signal(signal.SIGTERM, signal.SIG_IGN)

def write_state(*_):
    try:
        with open(agents_path) as handle:
            agents = int(handle.read().strip() or "0")
    except OSError:
        agents = 0
    with open("/proc/self/stat") as handle:
        start_time = int(handle.read().split()[21])
    payload = {
        "pid": os.getpid(),
        "kind": "hermes-gateway",
        "hermes_home": home,
        "gateway_state": "running",
        "active_agents": agents,
        "code_sha": os.environ.get("GATEWAY_SIM_SHA", "ccc3334445556666777888999000aaab"),
        "start_time": start_time,
    }
    tmp = state_path + ".tmp"
    with open(tmp, "w") as handle:
        json.dump(payload, handle)
    os.replace(tmp, state_path)

signal.signal(signal.SIGUSR1, write_state)
signal.signal(signal.SIGALRM, write_state)
write_state()
deadline = time.time() + 300
while time.time() < deadline:
    signal.alarm(2)
    time.sleep(2)
`

const gatewaySimHermes = `#!/usr/bin/env python3
"""fake hermes: only the machine-readable gateway ensure boundary exists; it
starts a simulated gateway and reports ready only once a genuinely NEW pid has
taken the state file (the old state must never pass for a fresh boot). With
GATEWAY_SIM_DRAIN_MARK set, the first invocation answers "draining" (exit 6)
without starting anything — the mid-shutdown owner the 2026-11-02 01:43Z live
abort saw — and every later invocation runs the normal fresh-start path."""
import json, os, subprocess, sys, time

argv = sys.argv[1:]
if "gateway" not in argv or "ensure" not in argv:
    print("unsupported invocation: %s" % " ".join(argv), file=sys.stderr)
    sys.exit(2)

mark = os.environ.get("GATEWAY_SIM_DRAIN_MARK", "")
if mark and not os.path.exists(mark):
    open(mark, "w").close()
    print('{"state":"draining"}')
    sys.exit(6)

state = os.environ["GATEWAY_SIM_STATE"]
agent = os.environ["GATEWAY_SIM_AGENT"]
old_pid = 0
try:
    old_pid = json.load(open(state))["pid"]
except Exception:
    pass
try:
    os.remove(state)
except FileNotFoundError:
    pass
subprocess.Popen(["python3", agent], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                 stdin=subprocess.DEVNULL, start_new_session=True)
deadline = time.time() + 5
while time.time() < deadline:
    try:
        pid = json.load(open(state))["pid"]
        if pid and pid != old_pid:
            print('{"state":"ready"}')
            sys.exit(0)
    except Exception:
        pass
    time.sleep(0.05)
print('{"state":"inaccessible"}')
sys.exit(7)
`

const gatewaySimSystemctl = `#!/bin/sh
# fake systemctl --user for a simulated serve unit backed by a state file.
state_file="$GATEWAY_SIM_SYSTEMD_STATE"
[ "${1:-}" = "--user" ] && shift
cmd="$1"
unit="$2"
shift 2
case "$cmd" in
  stop)
    echo inactive > "$state_file" ;;
  start)
    echo active > "$state_file" ;;
  is-active)
    cat "$state_file" 2>/dev/null || echo inactive ;;
  *)
    echo "unsupported systemctl invocation: $cmd $unit $*" >&2
    exit 2 ;;
esac
`

type gatewaySim struct {
	base         string
	root         string
	home         string
	relay        string
	statePath    string
	agentsPath   string
	systemdState string
	hermesBin    string
	systemctlBin string
	agentPath    string
	gatewayPID   int
}

func startGatewaySim(t *testing.T, ignoreTerm bool) *gatewaySim {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("the simulated drained restart needs a Linux /proc")
	}
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 is not available for the simulated gateway")
	}
	base := t.TempDir()
	sim := &gatewaySim{
		base:         base,
		root:         filepath.Join(base, "state"),
		home:         filepath.Join(base, "hermes"),
		relay:        filepath.Join(base, "relay"),
		statePath:    filepath.Join(base, "hermes", "gateway_state.json"),
		agentsPath:   filepath.Join(base, "hermes", "agents"),
		systemdState: filepath.Join(base, "systemd-state"),
	}
	for _, dir := range []string{sim.root, sim.home, sim.relay} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(sim.home, ".finite-hermes-disposable"), []byte("wk-finite-test\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sim.relay, "bridge.on"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sim.agentsPath, []byte("0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sim.systemdState, []byte("active"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GATEWAY_SIM_SYSTEMD_STATE", sim.systemdState)
	sim.agentPath = filepath.Join(base, "gateway_sim_agent.py")
	if err := os.WriteFile(sim.agentPath, []byte(gatewaySimAgent), 0o600); err != nil {
		t.Fatal(err)
	}
	sim.hermesBin = filepath.Join(base, "hermes-sim")
	if err := os.WriteFile(sim.hermesBin, []byte(gatewaySimHermes), 0o700); err != nil {
		t.Fatal(err)
	}
	sim.systemctlBin = filepath.Join(base, "systemctl-sim")
	if err := os.WriteFile(sim.systemctlBin, []byte(gatewaySimSystemctl), 0o700); err != nil {
		t.Fatal(err)
	}
	// The first gateway is a real process whose cmdline contains `gateway run`,
	// exactly like the production finite gateway's /proc/<pid>/cmdline: the
	// simulated agent is invoked as `python3 <base>/gateway-run gateway run`.
	python, _ := exec.LookPath("python3")
	agentFile := filepath.Join(base, "gateway-run")
	if err := os.WriteFile(agentFile, []byte(gatewaySimAgent), 0o600); err != nil {
		t.Fatal(err)
	}
	gateway := exec.Command(python, agentFile, "gateway", "run", "--quiet")
	gateway.Env = append(os.Environ(),
		"GATEWAY_SIM_STATE="+sim.statePath,
		"GATEWAY_SIM_AGENTS="+sim.agentsPath,
		"GATEWAY_SIM_HOME="+sim.home,
		"GATEWAY_SIM_IGNORE_TERM="+boolFlag(ignoreTerm),
	)
	gateway.Stdout = os.Stderr
	gateway.Stderr = os.Stderr
	if err := gateway.Start(); err != nil {
		t.Fatal(err)
	}
	sim.gatewayPID = gateway.Process.Pid
	t.Cleanup(func() {
		_ = gateway.Process.Kill()
		_, _ = gateway.Process.Wait()
	})
	sim.waitState(t, 10*time.Second)
	return sim
}

func boolFlag(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

func (sim *gatewaySim) waitState(t *testing.T, timeout time.Duration) gatewayRuntimeState {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		state, err := readGatewayStateFile(sim.statePath)
		if err == nil {
			return state
		}
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("simulated gateway state never appeared: %v", lastErr)
	return gatewayRuntimeState{}
}

func (sim *gatewaySim) options(drainTimeout time.Duration) gatewayRestartOptions {
	return gatewayRestartOptions{
		Root: sim.root, HermesHome: sim.home, RelayDir: sim.relay,
		ServeUnit: "werkrby-finite-serve.service", DrainTimeout: drainTimeout, DrainPoll: 200 * time.Millisecond,
		KillGrace: 2 * time.Second, EnsureTimeout: 30 * time.Second, EnsureRetry: 30 * time.Second, EnsurePoll: 250 * time.Millisecond,
		MCPServers: nil, // no profiles exist here: the MCP gate is not requested
	}
}

func (sim *gatewaySim) deps(t *testing.T, options gatewayRestartOptions, log *bytes.Buffer) gatewayRestartDeps {
	t.Helper()
	deps, err := resolveGatewayRestartDeps(options)
	if err != nil {
		t.Fatal(err)
	}
	// The sim runs inside whatever cgroup the test runner lives in (often the
	// real serve unit); the guard's own consistency is covered separately.
	deps.inServeCgroup = func() bool { return false }
	// Count only supervisors spawned under the disposable tree: production
	// supervisors of the real engine share /proc and must not gate this sim.
	base := sim.base
	deps.countSupervisors = func(ctx context.Context) (int, error) {
		pids, err := listProcSupervisorPIDs(ctx)
		if err != nil {
			return 0, err
		}
		count := 0
		for _, pid := range pids {
			args, err := deps.processCmdline(ctx, pid)
			if err != nil || len(args) == 0 {
				continue
			}
			if strings.HasPrefix(args[0], base+string(filepath.Separator)) {
				count++
			}
		}
		return count, nil
	}
	deps.systemctl = func(ctx context.Context, args ...string) (string, error) {
		return runGatewayHelper(ctx, sim.systemctlBin, args...)
	}
	deps.ensureGateway = func(ctx context.Context, options gatewayRestartOptions) (string, error) {
		return ensureGatewayViaHermes(ctx, options, sim.hermesBin, []string{
			"GATEWAY_SIM_STATE=" + sim.statePath,
			"GATEWAY_SIM_AGENTS=" + sim.agentsPath,
			"GATEWAY_SIM_HOME=" + sim.home,
			"GATEWAY_SIM_AGENT=" + sim.agentPath,
			"GATEWAY_SIM_DRAIN_MARK=" + filepath.Join(sim.base, "ensure-drain-mark"),
		})
	}
	deps.logf = func(format string, args ...any) { fmt.Fprintf(log, format, args...) }
	return deps
}

func TestGatewayRestartSimulatedDrainedRestart(t *testing.T) {
	sim := startGatewaySim(t, false)
	oldPID := sim.gatewayPID
	// One supervisor plus one live agent drain while the restart watches.
	decoy := startSimulatedSupervisor(t, sim.base)
	if err := os.WriteFile(sim.agentsPath, []byte("1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var log bytes.Buffer
	options := sim.options(30 * time.Second)
	deps := sim.deps(t, options, &log)
	go func() {
		time.Sleep(700 * time.Millisecond)
		_ = syscall.Kill(decoy.Process.Pid, syscall.SIGTERM)
		_ = os.WriteFile(sim.agentsPath, []byte("0\n"), 0o600)
	}()
	result, err := runGatewayRestart(context.Background(), options, deps)
	if err != nil {
		t.Fatalf("simulated restart failed: %v\n%s", err, log.String())
	}
	if result.OldPID != oldPID {
		t.Fatalf("old pid = %d, want %d", result.OldPID, oldPID)
	}
	state := sim.waitState(t, 10*time.Second)
	if state.PID == oldPID {
		t.Fatalf("the gateway pid did not change: %d", state.PID)
	}
	if result.NewPID != state.PID {
		t.Fatalf("result new pid = %d, live state pid = %d", result.NewPID, state.PID)
	}
	if serveState := readSimSystemdState(t, sim); serveState != "active" {
		t.Fatalf("serve state after restart = %q", serveState)
	}
	if _, err := os.Stat(filepath.Join(sim.relay, "bridge.on")); err != nil {
		t.Fatalf("intake was not restored: %v", err)
	}
	if result.DrainedSeconds >= 30*time.Second {
		t.Fatalf("drain took the whole budget: %s", result.DrainedSeconds)
	}
	if result.DrainedSeconds < 400*time.Millisecond {
		t.Fatalf("drain did not wait for the open work: %s", result.DrainedSeconds)
	}
}

func TestGatewayRestartSimulatedEnsureDrainingRetries(t *testing.T) {
	// The simulated hermes answers "draining" once without starting anything
	// (the 2026-11-02T01:43Z live abort), then runs the normal fresh-start
	// path: the restart must ensure again and complete.
	sim := startGatewaySim(t, false)
	oldPID := sim.gatewayPID
	var log bytes.Buffer
	options := sim.options(30 * time.Second)
	deps := sim.deps(t, options, &log)
	result, err := runGatewayRestart(context.Background(), options, deps)
	if err != nil {
		t.Fatalf("simulated restart failed after a draining ensure: %v\n%s", err, log.String())
	}
	if result.OldPID != oldPID {
		t.Fatalf("old pid = %d, want %d", result.OldPID, oldPID)
	}
	state := sim.waitState(t, 10*time.Second)
	if state.PID == oldPID {
		t.Fatalf("the gateway pid did not change: %d", state.PID)
	}
	if serveState := readSimSystemdState(t, sim); serveState != "active" {
		t.Fatalf("serve state after restart = %q", serveState)
	}
	if _, err := os.Stat(filepath.Join(sim.relay, "bridge.on")); err != nil {
		t.Fatalf("intake was not restored: %v", err)
	}
}

func TestGatewayRestartSimulatedStuckSupervisorAborts(t *testing.T) {
	sim := startGatewaySim(t, false)
	oldPID := sim.gatewayPID
	decoy := startSimulatedSupervisor(t, sim.base)
	defer func() { _ = syscall.Kill(decoy.Process.Pid, syscall.SIGTERM) }()
	var log bytes.Buffer
	options := sim.options(1 * time.Second)
	deps := sim.deps(t, options, &log)
	_, err := runGatewayRestart(context.Background(), options, deps)
	if err == nil {
		t.Fatal("the restart drained past a stuck supervisor")
	}
	// The gateway is untouched and still the same live process.
	if err := syscall.Kill(oldPID, 0); err != nil {
		t.Fatalf("the gateway died during the abort: %v", err)
	}
	if serveState := readSimSystemdState(t, sim); serveState != "inactive" {
		t.Fatalf("serve state after abort = %q, want inactive", serveState)
	}
	if _, err := os.Stat(filepath.Join(sim.relay, "bridge.on")); !os.IsNotExist(err) {
		t.Fatalf("intake was restored after the abort: %v", err)
	}
}

func TestGatewayRestartSimulatedStuckGatewayKill(t *testing.T) {
	sim := startGatewaySim(t, true) // the gateway ignores TERM
	oldPID := sim.gatewayPID
	var log bytes.Buffer
	options := sim.options(30 * time.Second)
	options.KillGrace = 1 * time.Second
	deps := sim.deps(t, options, &log)
	result, err := runGatewayRestart(context.Background(), options, deps)
	if err != nil {
		t.Fatalf("simulated restart failed: %v\n%s", err, log.String())
	}
	if result.NewPID == oldPID {
		t.Fatal("the stuck gateway was replaced by the same pid")
	}
	if procProcessAlive(context.Background(), oldPID) {
		t.Fatal("the TERM-ignoring gateway survived the restart")
	}
	if serveState := readSimSystemdState(t, sim); serveState != "active" {
		t.Fatalf("serve state = %q, want active", serveState)
	}
}

func startSimulatedSupervisor(t *testing.T, base string) *exec.Cmd {
	t.Helper()
	dir, err := os.MkdirTemp(base, "decoy-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	// A python file literally named `supervise` next to a fake finite argv:
	// os.execv re-spawns python with argv[0] "<dir>/werkrby-finite" and
	// argv[1] "supervise" — exactly what the real engine's supervisor
	// processes look like in /proc/cmdline — and then sleeps forever.
	if err := os.WriteFile(filepath.Join(dir, "supervise"), []byte("import time; time.sleep(3600)\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "werkrby-finite"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is not available for the simulated supervisor")
	}
	bootstrap := fmt.Sprintf("import os, sys; os.chdir(%q); os.execv(sys.executable, [%q, 'supervise'])", dir, filepath.Join(dir, "werkrby-finite"))
	command := exec.Command(python, "-c", bootstrap)
	command.Env = os.Environ()
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = command.Process.Kill()
		_, _ = command.Process.Wait()
	})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		pids, err := listProcSupervisorPIDs(context.Background())
		if err == nil {
			for _, pid := range pids {
				if pid == command.Process.Pid {
					return command
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("the simulated supervisor never became visible to the /proc scan")
	return command
}

func readSimSystemdState(t *testing.T, sim *gatewaySim) string {
	t.Helper()
	content, err := os.ReadFile(sim.systemdState)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(content))
}

func TestParseGatewayStateFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway_state.json")
	content := `{"pid": 2574846, "kind": "hermes-gateway", "hermes_home": "/h", "gateway_state": "running",
	  "active_agents": 6, "code_sha": "abc", "start_time": 3567291, "updated_at": "2026-11-01T22:58:55"}`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	state, err := readGatewayStateFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if state.PID != 2574846 || state.ActiveAgents != 6 || state.Kind != "hermes-gateway" ||
		state.GatewayState != "running" || state.StartTime != 3567291 || state.HermesHome != "/h" {
		t.Fatalf("decoded state = %+v", state)
	}
	if !state.running() {
		t.Fatal("state.running() = false for a running gateway")
	}
}

func TestMCPLogScanFindsRegistrationsAfterStart(t *testing.T) {
	dir := t.TempDir()
	profile := filepath.Join(dir, "profiles", "wk-finite-test", "logs")
	if err := os.MkdirAll(profile, 0o700); err != nil {
		t.Fatal(err)
	}
	lines := strings.Join([]string{
		"2026-11-01 17:00:00,000 INFO tools.mcp_tool: MCP server 'spark' (HTTP): registered 36 tool(s): a, b",
		"2026-11-01 17:48:49,288 INFO tools.mcp_tool: MCP server 'spark' (HTTP): registered 36 tool(s): a, b",
		"2026-11-01 17:48:49,290 INFO tools.mcp_tool: MCP server 'graphiti' (SSE): registered 4 tool(s): c",
		"2026-11-01 17:48:49,291 INFO tools.mcp_tool: MCP server 'rag' (SSE): registered 2 tool(s): d",
		"2026-11-01 17:48:49,334 INFO tools.mcp_tool:   MCP: 44 tool(s) from 7 server(s)",
		"2026-11-01 17:48:50,000 INFO tools.other: unrelated",
		"2026-11-01 17:48:51,000 INFO tools.mcp_tool: MCP: 0 tool(s) from 7 server(s) (1 failed: rag (not attempted (in retry cooldown)))",
		"",
	}, "\n")
	if err := os.WriteFile(filepath.Join(profile, "agent.log"), []byte(lines), 0o600); err != nil {
		t.Fatal(err)
	}
	since := time.Date(2026, 11, 1, 17, 48, 0, 0, time.Local)
	check, err := scanMCPLogs(dir, []string{"spark", "graphiti", "rag"}, since)
	if err != nil {
		t.Fatal(err)
	}
	if check.Servers["spark"] != 36 || check.Servers["graphiti"] != 4 || check.Servers["rag"] != 2 {
		t.Fatalf("server registrations = %+v", check.Servers)
	}
	if check.SummaryLines != 1 {
		t.Fatalf("healthy summary lines = %d, want 1 (the failed line must not count)", check.SummaryLines)
	}
	if check.CooldownLines != 1 {
		t.Fatalf("cooldown lines = %d, want 1", check.CooldownLines)
	}
	if check.allConnected([]string{"spark", "graphiti", "rag"}) {
		t.Fatal("a cooldown line in the window must fail the MCP gate")
	}
	healthy := check
	healthy.CooldownLines = 0
	if !healthy.allConnected([]string{"spark", "graphiti", "rag"}) {
		t.Fatalf("check without cooldown lines = %+v, want all connected", healthy)
	}
}

func TestGatewayRestartParseOptionsDefaults(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Skip("no home dir")
	}
	options, err := parseGatewayRestartOptions([]string{"restart", "--root", filepath.Join(home, "werkrby-finite", "state")})
	if err != nil {
		t.Fatal(err)
	}
	if options.ServeUnit != "werkrby-finite-serve.service" {
		t.Fatalf("serve unit default = %q", options.ServeUnit)
	}
	if options.HermesHome != filepath.Join(home, "werkrby-finite", "hermes") {
		t.Fatalf("hermes home default = %q", options.HermesHome)
	}
	if options.RelayDir != filepath.Join(home, "werkrby-finite", "relay") {
		t.Fatalf("relay dir default = %q", options.RelayDir)
	}
	if options.DrainTimeout != time.Hour || options.KillGrace != 180*time.Second || options.EnsureTimeout != 120*time.Second {
		t.Fatalf("time defaults = %s/%s/%s", options.DrainTimeout, options.KillGrace, options.EnsureTimeout)
	}
	if options.EnsureRetry != 10*time.Minute || options.EnsurePoll != 5*time.Second {
		t.Fatalf("ensure retry defaults = %s/%s", options.EnsureRetry, options.EnsurePoll)
	}
	if strings.Join(options.MCPServers, ",") != "spark,graphiti,rag" {
		t.Fatalf("mcp servers default = %v", options.MCPServers)
	}
	if _, err := parseGatewayRestartOptions([]string{"restart"}); err == nil {
		t.Fatal("root is required")
	}
	if _, err := parseGatewayRestartOptions([]string{"frobnicate", "--root", "/x"}); err == nil {
		t.Fatal("unknown gateway subcommands must fail")
	}
}
