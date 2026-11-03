package finite

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// gatewayRestartOptions configures `werkrby-finite gateway restart`, the
// engine-owned drained restart of the finite Hermes gateway. The defaults are
// the hand procedure recorded in notebook Projects/Werkrby Finite Engine.
type gatewayRestartOptions struct {
	Root           string        // finite state root (required)
	HermesHome     string        // dedicated finite Hermes home (never ~/.hermes)
	RelayDir       string        // relay directory holding bridge.on
	ServeUnit      string        // systemd user unit owning dispatch
	SystemctlBin   string        // systemctl executable
	HermesBin      string        // hermes executable for the fresh gateway boot
	GatewayProfile string        // profile passed to the fresh gateway boot
	DrainTimeout   time.Duration // whole-drain budget before the abort
	DrainPoll      time.Duration // drain poll interval
	KillGrace      time.Duration // TERM-to-KILL grace on the old gateway
	EnsureTimeout  time.Duration // per-attempt fresh-boot readiness budget
	EnsureRetry    time.Duration // overall budget across fresh-boot attempts before the abort
	EnsurePoll     time.Duration // wait between fresh-boot attempts
	MCPServers     []string      // servers the verification gate requires; empty skips the gate

	// OldGatewayPID is set by runGatewayRestart just before the boot loop:
	// the boot's readiness poll must ignore any state file still naming the
	// signalled old gateway.
	OldGatewayPID int
}

// gatewayRuntimeState is the subset of gateway_state.json the restart reads.
type gatewayRuntimeState struct {
	PID          int    `json:"pid"`
	Kind         string `json:"kind"`
	HermesHome   string `json:"hermes_home"`
	GatewayState string `json:"gateway_state"`
	ActiveAgents int    `json:"active_agents"`
	CodeSHA      string `json:"code_sha"`
	StartTime    uint64 `json:"start_time"`
	UpdatedAt    string `json:"updated_at"`
	ExitReason   string `json:"exit_reason"`
}

func (s gatewayRuntimeState) running() bool {
	return s.Kind == "hermes-gateway" && s.GatewayState == "running" && s.PID > 0
}

// gatewayMCPCheck is the Step-4-style shared check over the profile agent logs.
type gatewayMCPCheck struct {
	Servers       map[string]int `json:"servers"`
	SummaryLines  int            `json:"summary_lines"`
	CooldownLines int            `json:"cooldown_lines"`
}

// allConnected reports the MCP verification gate: every required server
// registered at least one tool after the fresh boot, a healthy summary line
// appeared, and no server sat out in the retry cooldown.
func (c gatewayMCPCheck) allConnected(required []string) bool {
	if c.CooldownLines > 0 || c.SummaryLines < 1 {
		return false
	}
	for _, name := range required {
		if c.Servers[name] < 1 {
			return false
		}
	}
	return true
}

// gatewayRestartResult is the one-JSON-object stdout contract of the command.
type gatewayRestartResult struct {
	State            string         `json:"state"`
	OldPID           int            `json:"old_pid"`
	NewPID           int            `json:"new_pid"`
	CodeSHA          string         `json:"code_sha"`
	DrainedSeconds   time.Duration  `json:"drained_seconds"`
	MCPServers       map[string]int `json:"mcp_servers,omitempty"`
	MCPCooldownLines int            `json:"mcp_cooldown_lines"`
	ServeRestarted   bool           `json:"serve_restarted"`
	IntakeRestored   bool           `json:"intake_restored"`
}

// gatewayAbortError marks a gate failure after dispatch was already stopped:
// serve stays stopped and bridge.on stays absent, so Werkrby stays off and the
// operator decides what happens next.
type gatewayAbortError struct {
	Step   string
	Reason string
}

func (e *gatewayAbortError) Error() string {
	return fmt.Sprintf("gateway restart aborted at %s: %s (werkrby stays off: serve stopped, bridge.on absent; operator decides)", e.Step, e.Reason)
}

// gatewayRestartDeps is the engine boundary of the restart: systemd, the
// fresh-gateway boot, the gateway state files, process signals, and the
// profile log scan. Tests replace every member; the real constructors live in
// gateway_restart_linux.go and this file.
type gatewayRestartDeps struct {
	systemctl        func(ctx context.Context, args ...string) (string, error)
	ensureGateway    func(ctx context.Context, options gatewayRestartOptions) (string, error)
	readGatewayState func(ctx context.Context) (gatewayRuntimeState, error)
	countSupervisors func(ctx context.Context) (int, error)
	signalProcess    func(pid int, sig syscall.Signal) error
	processCmdline   func(ctx context.Context, pid int) ([]string, error)
	processAlive     func(ctx context.Context, pid int) bool
	inServeCgroup    func() bool
	mcpCheck         func(ctx context.Context, since time.Time) (gatewayMCPCheck, error)
	removeBridge     func(ctx context.Context) error
	touchBridge      func(ctx context.Context) error
	now              func() time.Time
	sleep            func(ctx context.Context, d time.Duration) error
	logf             func(format string, args ...any)
}

// runGatewayRestart performs the drained restart. Nothing touches SQLite.
func runGatewayRestart(ctx context.Context, options gatewayRestartOptions, deps gatewayRestartDeps) (gatewayRestartResult, error) {
	logf := deps.logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	abort := func(step, format string, args ...any) (gatewayRestartResult, error) {
		reason := fmt.Sprintf(format, args...)
		logf("ABORT [%s]: %s", step, reason)
		return gatewayRestartResult{}, &gatewayAbortError{Step: step, Reason: reason}
	}
	if !gatewayRestartPlatformSupported() {
		return gatewayRestartResult{}, fmt.Errorf("gateway restart: unsupported platform; the drained restart needs Linux with systemd user units")
	}
	if err := refuseDefaultHermesHome(options.HermesHome); err != nil {
		return gatewayRestartResult{}, err
	}
	if deps.inServeCgroup() {
		return gatewayRestartResult{}, fmt.Errorf("gateway restart: refused: this process runs inside the %s cgroup; a worker turn cannot restart the gateway it runs on (arm it with systemd-run --user instead)", options.ServeUnit)
	}

	// Preflight: the target must be the finite gateway, up, and drained-ready.
	state, err := deps.readGatewayState(ctx)
	if err != nil {
		return gatewayRestartResult{}, fmt.Errorf("gateway restart: read gateway state: %w", err)
	}
	if !state.running() {
		return abort("preflight", "gateway_state.json does not report a running hermes-gateway (kind=%q state=%q pid=%d)", state.Kind, state.GatewayState, state.PID)
	}
	if filepath.Clean(state.HermesHome) != filepath.Clean(options.HermesHome) {
		return abort("preflight", "gateway hermes_home %q does not match the configured home %q", state.HermesHome, options.HermesHome)
	}

	// Stop intake and dispatch. The unit has KillMode=process, so open
	// supervisors keep running and are adopted on the next start.
	logf("removing %s (stop intake)", filepath.Join(options.RelayDir, "bridge.on"))
	if err := deps.removeBridge(ctx); err != nil {
		return abort("stop-intake", "remove bridge.on: %v", err)
	}
	logf("stopping %s (stop dispatch)", options.ServeUnit)
	if _, err := deps.systemctl(ctx, "--user", "stop", options.ServeUnit); err != nil {
		return abort("stop-dispatch", "systemctl stop: %v", err)
	}
	if err := waitServeState(ctx, deps, options, "inactive", 30*time.Second); err != nil {
		return abort("stop-dispatch", "%v", err)
	}

	// Drain: no finite supervisor process and no live gateway agent. The runs
	// finish on their own; nothing is ever killed to make the drain happen.
	drainStart := deps.now()
	logf("draining: waiting for 0 supervise processes and active_agents 0 (budget %s)", options.DrainTimeout)
	drainDeadline := deps.now().Add(options.DrainTimeout)
	for {
		supervisors, err := deps.countSupervisors(ctx)
		if err != nil {
			return abort("drain", "count supervisors: %v", err)
		}
		drainState, err := deps.readGatewayState(ctx)
		if err != nil {
			return abort("drain", "read gateway state: %v", err)
		}
		if supervisors == 0 && drainState.ActiveAgents == 0 {
			break
		}
		if !deps.now().Before(drainDeadline) {
			return abort("drain", "timeout after %s with %d supervise processes and active_agents %d; the gateway was not touched", options.DrainTimeout, supervisors, drainState.ActiveAgents)
		}
		if err := deps.sleep(ctx, options.DrainPoll); err != nil {
			return abort("drain", "interrupted: %v", err)
		}
	}
	drainedAt := deps.now()
	logf("drained after %s", drainedAt.Sub(drainStart).Truncate(time.Millisecond))

	// Stop the old gateway. Only a pid whose cmdline says `gateway run` and
	// whose recorded home is this home is ever signalled.
	state, err = deps.readGatewayState(ctx)
	if err != nil {
		return abort("stop-gateway", "read gateway state: %v", err)
	}
	if !state.running() {
		return abort("stop-gateway", "gateway_state.json no longer reports a running gateway (kind=%q state=%q pid=%d)", state.Kind, state.GatewayState, state.PID)
	}
	if filepath.Clean(state.HermesHome) != filepath.Clean(options.HermesHome) {
		return abort("stop-gateway", "gateway hermes_home %q does not match %q", state.HermesHome, options.HermesHome)
	}
	oldPID, oldStart := state.PID, state.StartTime
	if err := verifyGatewayCmdline(ctx, deps, oldPID); err != nil {
		return abort("stop-gateway", "%v", err)
	}
	logf("SIGTERM to the old gateway pid %d", oldPID)
	if err := deps.signalProcess(oldPID, syscall.SIGTERM); err != nil {
		return abort("stop-gateway", "SIGTERM %d: %v", oldPID, err)
	}
	killDeadline := deps.now().Add(options.KillGrace)
	for deps.processAlive(ctx, oldPID) && deps.now().Before(killDeadline) {
		_ = deps.sleep(ctx, 250*time.Millisecond)
	}
	if deps.processAlive(ctx, oldPID) {
		// KILL only after the grace, and only after re-proving the identity.
		if err := verifyGatewayCmdline(ctx, deps, oldPID); err != nil {
			return abort("stop-gateway", "%v", err)
		}
		logf("gateway pid %d ignored TERM for %s; sending SIGKILL", oldPID, options.KillGrace)
		if err := deps.signalProcess(oldPID, syscall.SIGKILL); err != nil {
			return abort("stop-gateway", "SIGKILL %d: %v", oldPID, err)
		}
		if !waitForGatewayExit(ctx, deps, oldPID, 15*time.Second) {
			return abort("stop-gateway", "gateway pid %d survived SIGKILL", oldPID)
		}
	}

	// Start the fresh gateway the way hermes's own clients start it: a
	// detached `gateway run --quiet --idle-exit` whose readiness is the state
	// file (Desktop's `gateway ensure` boot fatal-exits at startup in this
	// environment while the untouched ~/.hermes gateway holds the telegram
	// bot token and the api_server port — the 2026-11-02T06:46Z live abort).
	// A boot that has not reached running yet is not terminal: a fresh owner
	// that dies at startup leaves its exit_reason in the state file, so
	// retry within the overall budget; the verification gate below still
	// refuses anything but a proven fresh boot before serve ever resumes.
	options.OldGatewayPID = oldPID
	ensureStartedAt := deps.now()
	ensureDeadline := deps.now().Add(options.EnsureRetry)
	var lastVerdict, lastExitReason string
	logf("starting a fresh gateway (client-style boot; attempt timeout %s, overall budget %s)", options.EnsureTimeout, options.EnsureRetry)
	for {
		ensureState, err := deps.ensureGateway(ctx, options)
		if err != nil {
			return abort("ensure", "fresh gateway boot failed: %v", err)
		}
		if ensureState == "ready" {
			break
		}
		lastVerdict = ensureState
		// A fresh boot that died at startup leaves its exit_reason in the
		// state file; surface it so the operator abort names the cause.
		if observed, stateErr := deps.readGatewayState(ctx); stateErr == nil &&
			strings.TrimSpace(observed.ExitReason) != "" {
			lastExitReason = observed.ExitReason
		}
		if !deps.now().Before(ensureDeadline) {
			detail := fmt.Sprintf("fresh gateway never reported running within %s (last verdict %q)",
				options.EnsureRetry, lastVerdict)
			if strings.TrimSpace(lastExitReason) != "" {
				detail += fmt.Sprintf("; gateway_state exit_reason: %s", lastExitReason)
			}
			return abort("ensure", "%s", detail)
		}
		if err := deps.sleep(ctx, options.EnsurePoll); err != nil {
			return abort("ensure", "interrupted: %v", err)
		}
	}

	// Verification gate: a genuinely fresh boot with code identity and the
	// required MCP servers, before dispatch ever resumes.
	fresh, err := deps.readGatewayState(ctx)
	if err != nil {
		return abort("verify", "read gateway state: %v", err)
	}
	if !fresh.running() {
		return abort("verify", "gateway_state.json does not report a running hermes-gateway after ensure (kind=%q state=%q pid=%d)", fresh.Kind, fresh.GatewayState, fresh.PID)
	}
	freshBoot := fresh.PID != oldPID || (oldStart > 0 && fresh.StartTime > oldStart)
	if !freshBoot {
		return abort("verify", "pid %d start_time %d matches the old gateway; no fresh boot happened", fresh.PID, fresh.StartTime)
	}
	if strings.TrimSpace(fresh.CodeSHA) == "" {
		return abort("verify", "the fresh gateway reports no code_sha")
	}
	result := gatewayRestartResult{
		State:          "restarted",
		OldPID:         oldPID,
		NewPID:         fresh.PID,
		CodeSHA:        fresh.CodeSHA,
		DrainedSeconds: drainedAt.Sub(drainStart).Truncate(time.Millisecond),
	}
	if len(options.MCPServers) > 0 {
		check, err := deps.mcpCheck(ctx, ensureStartedAt)
		if err != nil {
			return abort("verify", "MCP check: %v", err)
		}
		if !check.allConnected(options.MCPServers) {
			return abort("verify", "MCP gate failed: servers=%v summary_lines=%d cooldown_lines=%d (required: %s)",
				check.Servers, check.SummaryLines, check.CooldownLines, strings.Join(options.MCPServers, ","))
		}
		result.MCPServers = check.Servers
		result.MCPCooldownLines = check.CooldownLines
	}

	// Resume dispatch and intake only after the gate passed.
	logf("verification passed; starting %s", options.ServeUnit)
	if _, err := deps.systemctl(ctx, "--user", "start", options.ServeUnit); err != nil {
		return abort("resume", "systemctl start: %v", err)
	}
	if err := waitServeState(ctx, deps, options, "active", 60*time.Second); err != nil {
		return abort("resume", "%v", err)
	}
	result.ServeRestarted = true
	if err := deps.touchBridge(ctx); err != nil {
		return abort("resume", "restore bridge.on: %v", err)
	}
	result.IntakeRestored = true
	logf("restart complete: gateway %d -> %d (code %s), serve active, intake restored", oldPID, fresh.PID, fresh.CodeSHA)
	return result, nil
}

// verifyGatewayCmdline refuses to signal anything that is not exactly a
// `gateway run` process; the home match is proven on the state read before.
func verifyGatewayCmdline(ctx context.Context, deps gatewayRestartDeps, pid int) error {
	cmdline, err := deps.processCmdline(ctx, pid)
	if err != nil {
		return fmt.Errorf("read /proc/%d/cmdline: %w", pid, err)
	}
	joined := strings.Join(cmdline, " ")
	if !strings.Contains(joined, "gateway run") {
		return fmt.Errorf("pid %d cmdline %q does not contain `gateway run`; refusing to signal a foreign process", pid, joined)
	}
	return nil
}

func waitForGatewayExit(ctx context.Context, deps gatewayRestartDeps, pid int, timeout time.Duration) bool {
	deadline := deps.now().Add(timeout)
	for deps.processAlive(ctx, pid) {
		if !deps.now().Before(deadline) {
			return false
		}
		_ = deps.sleep(ctx, 250*time.Millisecond)
	}
	return true
}

func waitServeState(ctx context.Context, deps gatewayRestartDeps, options gatewayRestartOptions, want string, timeout time.Duration) error {
	deadline := deps.now().Add(timeout)
	for {
		// systemctl is-active exits 3 for every state but active, so the
		// printed state is the answer whatever the exit code; a unit that
		// failed while stopping is stopped too.
		output, _ := deps.systemctl(ctx, "--user", "is-active", options.ServeUnit)
		got := strings.TrimSpace(output)
		if got == want || (want == "inactive" && got == "failed") {
			return nil
		}
		if !deps.now().Before(deadline) {
			return fmt.Errorf("%s never became %s", options.ServeUnit, want)
		}
		_ = deps.sleep(ctx, 500*time.Millisecond)
	}
}

// refuseDefaultHermesHome enforces the hard boundary: the ordinary ~/.hermes
// gateway is never this command's target.
func refuseDefaultHermesHome(home string) error {
	if strings.TrimSpace(home) == "" {
		return fmt.Errorf("gateway restart: hermes home is empty")
	}
	self, err := user.Current()
	if err != nil {
		return fmt.Errorf("gateway restart: resolve current user: %w", err)
	}
	if self.HomeDir == "" {
		return fmt.Errorf("gateway restart: cannot resolve the user home; refusing to guess the target gateway")
	}
	if filepath.Clean(home) == filepath.Clean(filepath.Join(self.HomeDir, ".hermes")) {
		return fmt.Errorf("gateway restart: %q is the ordinary Hermes home; only the dedicated finite home may be restarted", home)
	}
	return nil
}

// readGatewayStateFile reads the subset of gateway_state.json the restart uses.
func readGatewayStateFile(path string) (gatewayRuntimeState, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return gatewayRuntimeState{}, err
	}
	var state gatewayRuntimeState
	if err := json.Unmarshal(content, &state); err != nil {
		return gatewayRuntimeState{}, fmt.Errorf("decode %s: %w", path, err)
	}
	return state, nil
}

var (
	mcpRegisteredLine = regexp.MustCompile(`MCP server '([^']+)' \([A-Za-z]+\): registered ([0-9]+) tool`)
	mcpHealthySummary = regexp.MustCompile(`tools\.mcp_tool:\s+MCP: [0-9]+ tool\(s\) from [0-9]+ server\(s\)$`)
	mcpCooldownPhrase = "not attempted (in retry cooldown)"
	mcpLogStamp       = "2006-01-02 15:04:05"
)

// scanMCPLogs implements the Step-4-style shared check: walk the home's
// profile agent logs and judge only lines written after `since`.
func scanMCPLogs(home string, required []string, since time.Time) (gatewayMCPCheck, error) {
	check := gatewayMCPCheck{Servers: map[string]int{}}
	profiles, err := os.ReadDir(filepath.Join(home, "profiles"))
	if err != nil {
		if os.IsNotExist(err) {
			return check, nil
		}
		return check, err
	}
	cutoff := since.Format(mcpLogStamp)
	for _, profile := range profiles {
		if !profile.IsDir() {
			continue
		}
		logPath := filepath.Join(home, "profiles", profile.Name(), "logs", "agent.log")
		file, err := os.Open(logPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return check, err
		}
		scanner := bufio.NewScanner(file)
		scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			if len(line) < len(mcpLogStamp) || line[:len(mcpLogStamp)] < cutoff {
				continue
			}
			if match := mcpRegisteredLine.FindStringSubmatch(line); match != nil {
				count, err := strconv.Atoi(match[2])
				if err == nil && count > check.Servers[match[1]] {
					check.Servers[match[1]] = count
				}
				continue
			}
			if mcpHealthySummary.MatchString(line) {
				check.SummaryLines++
				continue
			}
			if strings.Contains(line, mcpCooldownPhrase) {
				check.CooldownLines++
			}
		}
		scanErr := scanner.Err()
		_ = file.Close()
		if scanErr != nil {
			return check, scanErr
		}
	}
	return check, nil
}

// writeGatewayRestartResult prints the one-JSON-object result contract.
func writeGatewayRestartResult(w io.Writer, result gatewayRestartResult) error {
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(result); err != nil {
		return fmt.Errorf("gateway restart: write result: %w", err)
	}
	return nil
}

// runGatewayHelper execs a helper binary with the systemd session
// environment a Werkrby shell often lacks.
func runGatewayHelper(ctx context.Context, binary string, args ...string) (string, error) {
	if strings.TrimSpace(binary) == "" {
		binary = "systemctl"
	}
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, args...)
	command.Env = append(os.Environ(), gatewaySessionEnv()...)
	output, err := command.CombinedOutput()
	if err != nil {
		return string(output), fmt.Errorf("%s %s: %w (output: %s)", binary, strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return string(output), nil
}

func gatewaySessionEnv() []string {
	var extra []string
	if os.Getenv("XDG_RUNTIME_DIR") == "" {
		if self, err := user.Current(); err == nil && self.Uid != "" {
			extra = append(extra, "XDG_RUNTIME_DIR=/run/user/"+self.Uid)
		}
	}
	if os.Getenv("DBUS_SESSION_BUS_ADDRESS") == "" {
		if runtimeDir := os.Getenv("XDG_RUNTIME_DIR"); runtimeDir != "" {
			extra = append(extra, "DBUS_SESSION_BUS_ADDRESS=unix:path="+runtimeDir+"/bus")
		}
	}
	return extra
}

// startFreshGatewayViaHermes boots the fresh gateway the way hermes's own
// clients start it: `gateway run --quiet --idle-exit`, detached in its own
// session with stdio on the home's logs/gateway-stdio.log, and waits for
// gateway_state.json to report the new owner. Desktop's `gateway ensure` —
// the hand procedure's step — spawns a managed gateway without --idle-exit,
// and in this environment that boot fatal-exits (code 78) on every attempt
// while the untouched ~/.hermes gateway holds the telegram bot token and the
// api_server port (live abort 2026-11-02T06:46Z, ten minutes of retries);
// the client-style boot is what every production finite gateway has run on
// since. Readiness is the state file: a running hermes-gateway whose pid is
// not the signalled old one (a stale record of the old gateway must never
// pass for a fresh boot). Verdicts: "ready", "booting" (no running state
// within the attempt budget), "exited" (the boot process died before it
// served; the state file carries its exit_reason when it wrote one).
func startFreshGatewayViaHermes(ctx context.Context, options gatewayRestartOptions, hermesBin string, extraEnv []string) (string, error) {
	if strings.TrimSpace(hermesBin) == "" {
		resolved, err := exec.LookPath("hermes")
		if err != nil {
			return "", fmt.Errorf("resolve the hermes executable: %w", err)
		}
		hermesBin = resolved
	}
	args := []string{"--profile", options.GatewayProfile, "gateway", "run", "--quiet", "--idle-exit"}
	logsDir := filepath.Join(options.HermesHome, "logs")
	if err := os.MkdirAll(logsDir, 0o700); err != nil {
		return "", fmt.Errorf("create %s: %w", logsDir, err)
	}
	stdio, err := os.OpenFile(filepath.Join(logsDir, "gateway-stdio.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return "", fmt.Errorf("open the gateway stdio log: %w", err)
	}
	defer stdio.Close()
	// A plain exec.Command, never CommandContext: the gateway must outlive
	// this call (an operator interrupt mid-restart must not kill the fresh
	// owner it just started).
	command := exec.Command(hermesBin, args...)
	command.Env = append(os.Environ(), "HERMES_HOME="+options.HermesHome, "HERMES_GATEWAY_DETACHED=1")
	command.Env = append(command.Env, extraEnv...)
	command.Stdin = nil
	command.Stdout = stdio
	command.Stderr = stdio
	command.SysProcAttr = gatewayDetachSysProcAttr()
	if err := command.Start(); err != nil {
		return "", fmt.Errorf("%s %s: %w", hermesBin, strings.Join(args, " "), err)
	}
	exited := make(chan error, 1)
	go func() { exited <- command.Wait() }()
	statePath := filepath.Join(options.HermesHome, "gateway_state.json")
	deadline := time.Now().Add(options.EnsureTimeout)
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if state, err := readGatewayStateFile(statePath); err == nil &&
			state.running() && state.PID > 0 && state.PID != options.OldGatewayPID {
			return "ready", nil
		}
		select {
		case <-exited:
			return "exited", nil
		default:
		}
		if !time.Now().Before(deadline) {
			return "booting", nil
		}
		timer := time.NewTimer(gatewayBootPoll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", ctx.Err()
		case <-timer.C:
		}
	}
}

func defaultGatewayRestartDeps() gatewayRestartDeps {
	return gatewayRestartDeps{
		systemctl: func(ctx context.Context, args ...string) (string, error) {
			return runGatewayHelper(ctx, "systemctl", args...)
		},
		ensureGateway: func(ctx context.Context, options gatewayRestartOptions) (string, error) {
			return startFreshGatewayViaHermes(ctx, options, options.HermesBin, nil)
		},
		removeBridge: func(_ context.Context) error {
			return nil // replaced by the resolved constructors in executeGatewayCommand
		},
		touchBridge: func(_ context.Context) error {
			return nil
		},
		now: time.Now,
		sleep: func(ctx context.Context, d time.Duration) error {
			timer := time.NewTimer(d)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		},
		logf: func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, "werkrby-finite gateway: "+format+"\n", args...)
		},
	}
}

// executeGatewayCommand handles `werkrby-finite gateway <subcommand>`.
func executeGatewayCommand(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "restart" {
		return fmt.Errorf("gateway: subcommand required: restart")
	}
	options, err := parseGatewayRestartOptions(args)
	if err != nil {
		return err
	}
	deps, err := resolveGatewayRestartDeps(options)
	if err != nil {
		return err
	}
	result, err := runGatewayRestart(ctx, options, deps)
	if err != nil {
		return err
	}
	return writeGatewayRestartResult(os.Stdout, result)
}

// resolveGatewayRestartDeps binds the real engine boundary to the options.
func resolveGatewayRestartDeps(options gatewayRestartOptions) (gatewayRestartDeps, error) {
	deps := defaultGatewayRestartDeps()
	deps.systemctl = func(ctx context.Context, args ...string) (string, error) {
		return runGatewayHelper(ctx, options.SystemctlBin, args...)
	}
	deps.readGatewayState = func(ctx context.Context) (gatewayRuntimeState, error) {
		_ = ctx
		return readGatewayStateFile(filepath.Join(options.HermesHome, "gateway_state.json"))
	}
	deps.countSupervisors = func(ctx context.Context) (int, error) {
		pids, err := listProcSupervisorPIDs(ctx)
		if err != nil {
			return 0, err
		}
		return len(pids), nil
	}
	deps.signalProcess = func(pid int, sig syscall.Signal) error {
		return syscall.Kill(pid, sig)
	}
	deps.processCmdline = procProcessCmdline
	deps.processAlive = procProcessAlive
	deps.inServeCgroup = procInServeCgroup
	deps.mcpCheck = func(_ context.Context, since time.Time) (gatewayMCPCheck, error) {
		return scanMCPLogs(options.HermesHome, options.MCPServers, since)
	}
	relayDir := options.RelayDir
	bridgePath := filepath.Join(relayDir, "bridge.on")
	deps.removeBridge = func(_ context.Context) error {
		if err := os.Remove(bridgePath); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	deps.touchBridge = func(_ context.Context) error {
		file, err := os.OpenFile(bridgePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		return file.Close()
	}
	return deps, nil
}

// parseGatewayRestartOptions parses `gateway restart` flags.
func parseGatewayRestartOptions(args []string) (gatewayRestartOptions, error) {
	flags := newCLIFlagSet("gateway restart")
	options := gatewayRestartOptions{
		ServeUnit:      "werkrby-finite-serve.service",
		SystemctlBin:   "systemctl",
		GatewayProfile: "default",
		DrainTimeout:   60 * time.Minute,
		DrainPoll:      15 * time.Second,
		KillGrace:      180 * time.Second,
		EnsureTimeout:  120 * time.Second,
		EnsureRetry:    10 * time.Minute,
		EnsurePoll:     5 * time.Second,
	}
	var root, hermesHome, relayDir, mcpServers string
	flags.StringVar(&root, "root", "", "finite state root")
	flags.StringVar(&hermesHome, "hermes-home", "", "finite Hermes home (default: a sibling hermes/ of the state root)")
	flags.StringVar(&relayDir, "relay-dir", "", "relay directory with bridge.on (default: a sibling relay/ of the state root)")
	flags.StringVar(&options.ServeUnit, "serve-unit", options.ServeUnit, "systemd user unit owning dispatch")
	flags.StringVar(&options.SystemctlBin, "systemctl", options.SystemctlBin, "systemctl executable")
	flags.StringVar(&options.HermesBin, "hermes-bin", "", "hermes executable (default: look up hermes on PATH)")
	flags.StringVar(&options.GatewayProfile, "profile", options.GatewayProfile, "profile passed to the fresh gateway boot")
	flags.DurationVar(&options.DrainTimeout, "drain-timeout", options.DrainTimeout, "drain budget before the abort")
	flags.DurationVar(&options.DrainPoll, "drain-poll", options.DrainPoll, "drain poll interval")
	flags.DurationVar(&options.KillGrace, "kill-grace", options.KillGrace, "TERM-to-KILL grace on the old gateway")
	flags.DurationVar(&options.EnsureTimeout, "ensure-timeout", options.EnsureTimeout, "per-attempt fresh-boot readiness budget")
	flags.DurationVar(&options.EnsureRetry, "ensure-retry-timeout", options.EnsureRetry, "overall budget across fresh-boot attempts")
	flags.DurationVar(&options.EnsurePoll, "ensure-poll", options.EnsurePoll, "wait between fresh-boot attempts")
	flags.StringVar(&mcpServers, "mcp-servers", "spark,graphiti,rag", "MCP servers the verification gate requires (empty skips the gate)")
	// Skip the leading subcommand word before flag parsing.
	rest := args
	if len(rest) > 0 && rest[0] == "restart" {
		rest = rest[1:]
	}
	if err := parseCLIFlags(flags, rest); err != nil {
		return gatewayRestartOptions{}, err
	}
	if strings.TrimSpace(root) == "" {
		return gatewayRestartOptions{}, fmt.Errorf("gateway restart: --root is required")
	}
	options.Root = filepath.Clean(root)
	if strings.TrimSpace(hermesHome) == "" {
		options.HermesHome = siblingGatewayPath(options.Root, "hermes")
	} else {
		options.HermesHome = gatewayExpandHome(hermesHome)
	}
	if strings.TrimSpace(relayDir) == "" {
		options.RelayDir = siblingGatewayPath(options.Root, "relay")
	} else {
		options.RelayDir = gatewayExpandHome(relayDir)
	}
	if options.DrainPoll <= 0 || options.DrainTimeout < options.DrainPoll {
		return gatewayRestartOptions{}, fmt.Errorf("gateway restart: --drain-timeout must be at least one --drain-poll")
	}
	if options.KillGrace <= 0 || options.EnsureTimeout <= 0 {
		return gatewayRestartOptions{}, fmt.Errorf("gateway restart: --kill-grace and --ensure-timeout must be positive")
	}
	if options.EnsurePoll <= 0 || options.EnsureRetry < options.EnsurePoll {
		return gatewayRestartOptions{}, fmt.Errorf("gateway restart: --ensure-retry-timeout must be at least one --ensure-poll")
	}
	if strings.TrimSpace(options.GatewayProfile) == "" {
		return gatewayRestartOptions{}, fmt.Errorf("gateway restart: --profile must not be empty")
	}
	if strings.TrimSpace(mcpServers) == "" {
		options.MCPServers = nil
	} else {
		servers, err := splitCLIOptionList(mcpServers)
		if err != nil {
			return gatewayRestartOptions{}, fmt.Errorf("gateway restart: --mcp-servers: %w", err)
		}
		options.MCPServers = servers
	}
	return options, nil
}

func siblingGatewayPath(root, name string) string {
	return filepath.Join(filepath.Dir(filepath.Clean(root)), name)
}

func gatewayExpandHome(path string) string {
	cleaned := filepath.Clean(path)
	if cleaned == "~" || strings.HasPrefix(cleaned, "~/") || strings.HasPrefix(cleaned, "~"+string(filepath.Separator)) {
		home, err := os.UserHomeDir()
		if err != nil {
			return cleaned
		}
		return filepath.Join(home, strings.TrimPrefix(cleaned, "~"))
	}
	return cleaned
}
