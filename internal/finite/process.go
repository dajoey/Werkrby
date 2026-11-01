package finite

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	finiteRunIDEnv         = "WERKRBY_FINITE_RUN_ID"
	finiteRunTokenEnv      = "WERKRBY_FINITE_PROCESS_TOKEN"
	finiteRunDirEnv        = "WERKRBY_FINITE_RUN_DIR"
	finiteJobNameEnv       = "WERKRBY_FINITE_JOB_NAME"
	finiteProviderRunIDArg = "--finite-run-id"
	finiteProviderTokenArg = "--finite-process-token"
)

var (
	ErrUnsupportedPlatform     = errors.New("finite process boundary is unsupported on this platform")
	ErrProcessUnknown          = errors.New("finite process liveness is unknown")
	ErrDuplicateSupervisor     = errors.New("multiple finite supervisors found for one run")
	ErrProcessIdentityMismatch = errors.New("finite process identity does not match")
	ErrProcessTokenMismatch    = errors.New("finite process token does not match")
	ErrDetachedProvider        = errors.New("finite provider remains after supervisor exit")
)

// Liveness is a fail-closed process-boundary verdict.
type Liveness string

const (
	LivenessAbsent  Liveness = "absent"
	LivenessLive    Liveness = "live"
	LivenessUnknown Liveness = "unknown"
)

// ProcessIdentity binds immutable run metadata to an OS process generation.
// Start is an opaque OS start token; PID alone is never an identity.
type ProcessIdentity struct {
	RunID int64  `json:"run_id"`
	Token string `json:"process_token"`
	PID   int    `json:"pid"`
	Start string `json:"process_start"`
}

// Equal compares the complete immutable process identity.
func (id ProcessIdentity) Equal(other ProcessIdentity) bool {
	return id.RunID == other.RunID && id.Token == other.Token && id.PID == other.PID && id.Start == other.Start
}

// RunProcesses is one complete run-boundary observation. Supervisor is nil
// when only provider processes remain. Processes includes the supervisor and
// every discovered provider process. On Linux, members of the supervisor's
// process group are enumerated once the supervisor is gone; while it lives
// the run is live and stop signals reach its whole group at once.
type RunProcesses struct {
	Liveness   Liveness
	Supervisor *ProcessIdentity
	Processes  []ProcessIdentity
	// group is the verified process-group id of the run, or zero. Stop loops
	// signal the whole group so a member forked after the scan is covered.
	group int
	// ungrouped reports a listed process outside group, which the
	// single-syscall group probe cannot observe.
	ungrouped bool
}

// AdoptedRun monitors an exactly identified existing supervisor without
// spawning a replacement.
type AdoptedRun struct {
	expected ProcessIdentity
}

type supervisorBinding struct {
	runID  int64
	runDir string
	token  string
}

type supervisorLaunch interface {
	Started(*exec.Cmd) error
	Close() error
}

type supervisorContainment interface {
	Close() error
}

// NewProcessToken returns a private 256-bit per-run discovery token.
func NewProcessToken() (string, error) {
	data := make([]byte, 32)
	if _, err := rand.Read(data); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

// SupportsProcessBoundary reports whether dispatch can fail open neither in
// discovery nor in termination.
func SupportsProcessBoundary() bool { return processBoundarySupported }

// IdentifyProcess obtains the current OS generation for pid.
func IdentifyProcess(pid int, runID int64, token string) (ProcessIdentity, error) {
	if err := validateRunBinding(runID, token); err != nil {
		return ProcessIdentity{}, err
	}
	if pid <= 0 {
		return ProcessIdentity{}, fmt.Errorf("finite process PID must be positive")
	}
	return identifyProcess(pid, runID, token)
}

// RunBoundary is the recorded identity of one run's process boundary. Only
// RunID and Token are required; every recorded field narrows the scan.
type RunBoundary struct {
	RunID int64
	Token string
	// RunDir is the run's recorded directory. A genuine supervisor bound to
	// another directory belongs to another run or state root and is ignored; a
	// supervisor claiming this directory under another token fails closed.
	RunDir string
	// Supervisor is the recorded supervisor identity. The supervisor is started
	// as a process-group leader, so its PID is the run's process-group id, and
	// POSIX does not reuse that PID while the group exists: group members remain
	// part of the run after the supervisor exits. Start is optional; when it is
	// known, a different generation at that PID proves PID reuse.
	Supervisor ProcessIdentity
	// Executable is the finite binary. Only processes running it (or this
	// process's own executable) are interpreted as supervisors or marked
	// providers. Empty means this process's executable.
	Executable string
}

const (
	// processRescanInterval spaces complete process scans in wait and stop
	// loops; one scan reads /proc for every process on the host.
	processRescanInterval = 250 * time.Millisecond
	// processGroupProbeInterval spaces the single-syscall process-group probe
	// that lets a stop loop rescan as soon as the run's group is gone.
	processGroupProbeInterval = 25 * time.Millisecond
)

// InspectRun performs a complete scan and distinguishes reliable absence from
// unknown liveness. Any error implies LivenessUnknown and forbids redispatch.
// Without a recorded supervisor, members of a supervisor's process group are
// visible only while the supervisor lives; owners use InspectRunBoundary.
func InspectRun(ctx context.Context, runID int64, token string) (RunProcesses, error) {
	return InspectRunBoundary(ctx, RunBoundary{RunID: runID, Token: token})
}

// InspectRunBoundary performs a complete scan of the recorded run boundary.
// Any error implies LivenessUnknown and forbids redispatch.
func InspectRunBoundary(ctx context.Context, boundary RunBoundary) (RunProcesses, error) {
	boundary, err := normalizeRunBoundary(boundary)
	if err != nil {
		return RunProcesses{Liveness: LivenessUnknown}, err
	}
	observation, err := inspectRunBoundary(ctx, boundary)
	if err != nil {
		observation.Liveness = LivenessUnknown
		return observation, errors.Join(ErrProcessUnknown, err)
	}
	if observation.Liveness == LivenessUnknown {
		return observation, ErrProcessUnknown
	}
	return observation, nil
}

func normalizeRunBoundary(boundary RunBoundary) (RunBoundary, error) {
	if err := validateRunBinding(boundary.RunID, boundary.Token); err != nil {
		return boundary, err
	}
	if boundary.RunDir != "" {
		if !filepath.IsAbs(boundary.RunDir) {
			return boundary, fmt.Errorf("finite run directory must be absolute")
		}
		boundary.RunDir = filepath.Clean(boundary.RunDir)
	}
	if boundary.Supervisor.PID < 0 {
		return boundary, fmt.Errorf("finite supervisor PID must not be negative")
	}
	if boundary.Supervisor.PID == 0 {
		boundary.Supervisor = ProcessIdentity{}
	}
	boundary.Supervisor.RunID, boundary.Supervisor.Token = boundary.RunID, boundary.Token
	return boundary, nil
}

// AdoptRun accepts only an exact run/token/PID/start match of a live
// supervisor. Callers scan the complete boundary first; adoption itself
// verifies the one supervisor generation it monitors.
func AdoptRun(ctx context.Context, expected ProcessIdentity) (*AdoptedRun, error) {
	if expected.Start == "" || expected.PID <= 0 {
		return nil, ErrProcessIdentityMismatch
	}
	if err := validateRunBinding(expected.RunID, expected.Token); err != nil {
		return nil, err
	}
	if err := verifyLiveSupervisor(ctx, expected); err != nil {
		return nil, err
	}
	return &AdoptedRun{expected: expected}, nil
}

// verifyLiveSupervisorByScan is the portable adoption check: one complete
// scan that must observe exactly the expected supervisor.
func verifyLiveSupervisorByScan(ctx context.Context, expected ProcessIdentity) error {
	observation, err := InspectRunBoundary(ctx, adoptedBoundary(expected))
	if err != nil {
		return err
	}
	if observation.Liveness != LivenessLive || observation.Supervisor == nil || !observation.Supervisor.Equal(expected) {
		return ErrProcessIdentityMismatch
	}
	return nil
}

func adoptedBoundary(expected ProcessIdentity) RunBoundary {
	return RunBoundary{RunID: expected.RunID, Token: expected.Token, Supervisor: expected}
}

// Wait waits for reliable absence of the whole provider boundary. A provider
// or process-group member surviving its supervisor is never treated as
// absence.
func (a *AdoptedRun) Wait(ctx context.Context) error {
	boundary := adoptedBoundary(a.expected)
	for {
		// While the exact supervisor generation lives the boundary is live;
		// wait for its exit instead of rescanning every process.
		if err := waitForSupervisorExit(ctx, a.expected); err != nil {
			return err
		}
		observation, err := InspectRunBoundary(ctx, boundary)
		if err != nil {
			return err
		}
		switch observation.Liveness {
		case LivenessAbsent:
			return nil
		case LivenessLive:
			if observation.Supervisor == nil {
				return ErrDetachedProvider
			}
			if !observation.Supervisor.Equal(a.expected) {
				return ErrProcessIdentityMismatch
			}
		default:
			return ErrProcessUnknown
		}
		if err := waitForProcessPoll(ctx); err != nil {
			return err
		}
	}
}

// StopSupervisor requires an exact expected supervisor generation before any
// signal. Termination still covers the provider boundary so no child escapes.
func StopSupervisor(ctx context.Context, expected ProcessIdentity, grace time.Duration) (Liveness, error) {
	if expected.Start == "" || expected.PID <= 0 {
		return LivenessUnknown, ErrProcessIdentityMismatch
	}
	boundary := adoptedBoundary(expected)
	observation, err := InspectRunBoundary(ctx, boundary)
	if err != nil {
		return LivenessUnknown, err
	}
	if observation.Liveness == LivenessAbsent {
		return LivenessAbsent, nil
	}
	if observation.Supervisor == nil {
		return LivenessUnknown, ErrDetachedProvider
	}
	if !observation.Supervisor.Equal(expected) {
		return LivenessUnknown, ErrProcessIdentityMismatch
	}
	return stopObservedRun(ctx, boundary, grace)
}

// StopRun terminates the exact run boundary and returns only on reliable
// absence. It never trusts a stored PID without its start identity.
func StopRun(ctx context.Context, runID int64, token string, grace time.Duration) (Liveness, error) {
	return StopRunBoundary(ctx, RunBoundary{RunID: runID, Token: token}, grace)
}

// StopRunBoundary terminates the recorded run boundary, including every
// member of the run's process group, and returns absence only when no member
// remains. SIGTERM is escalated to SIGKILL once grace has elapsed.
func StopRunBoundary(ctx context.Context, boundary RunBoundary, grace time.Duration) (Liveness, error) {
	if !processBoundarySupported {
		return LivenessUnknown, ErrUnsupportedPlatform
	}
	boundary, err := normalizeRunBoundary(boundary)
	if err != nil {
		return LivenessUnknown, err
	}
	return stopObservedRun(ctx, boundary, grace)
}

func stopObservedRun(ctx context.Context, boundary RunBoundary, grace time.Duration) (Liveness, error) {
	if grace < 0 {
		grace = 0
	}
	deadline := time.Now().Add(grace)
	force := grace == 0
	for {
		observation, err := InspectRunBoundary(ctx, boundary)
		if err != nil {
			return LivenessUnknown, err
		}
		if observation.Supervisor != nil && boundary.Supervisor.PID == 0 {
			// Keep the run's process group visible after the supervisor exits.
			boundary.Supervisor = *observation.Supervisor
		}
		if observation.Liveness == LivenessAbsent {
			return LivenessAbsent, nil
		}
		if observation.Liveness != LivenessLive || len(observation.Processes) == 0 {
			return LivenessUnknown, ErrProcessUnknown
		}
		if !force && !time.Now().Before(deadline) {
			force = true
		}
		ordered := observation.Processes
		if observation.Supervisor != nil {
			ordered = append([]ProcessIdentity{*observation.Supervisor}, ordered...)
		}
		seen := make(map[ProcessIdentity]struct{}, len(ordered))
		for _, identity := range ordered {
			if _, duplicate := seen[identity]; duplicate {
				continue
			}
			seen[identity] = struct{}{}
			if err := signalProcess(identity, force); err != nil && !errors.Is(err, os.ErrProcessDone) {
				return LivenessUnknown, errors.Join(ErrProcessUnknown, err)
			}
		}
		if observation.group > 0 {
			if err := signalProcessGroup(observation.group, force); err != nil {
				return LivenessUnknown, errors.Join(ErrProcessUnknown, err)
			}
		}
		wait := processRescanInterval
		if !force {
			wait = min(wait, max(time.Until(deadline), processGroupProbeInterval))
		}
		probe := observation.group
		if observation.ungrouped {
			probe = 0
		}
		if err := waitForStopPoll(ctx, probe, wait); err != nil {
			return LivenessUnknown, errors.Join(ErrProcessUnknown, err)
		}
	}
}

func waitForProcessPoll(ctx context.Context) error {
	timer := time.NewTimer(processRescanInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// waitForStopPoll waits up to limit before the next complete scan, returning
// early once the run's process group is reliably gone.
func waitForStopPoll(ctx context.Context, group int, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for {
		step := min(time.Until(deadline), processGroupProbeInterval)
		if group <= 0 {
			step = time.Until(deadline)
		}
		if step <= 0 {
			return nil
		}
		timer := time.NewTimer(step)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if group > 0 && processGroupGone(group) {
			return nil
		}
	}
}

func parseSupervisorArgs(args []string) (supervisorBinding, error) {
	if len(args) != 7 || args[0] != "supervise" {
		return supervisorBinding{}, fmt.Errorf("supervisor protocol requires supervise --run-id <id> --run-dir <dir> --process-token <token>")
	}
	var binding supervisorBinding
	var runIDText string
	seen := map[string]bool{}
	for index := 1; index < len(args); index += 2 {
		name, value := args[index], args[index+1]
		if seen[name] {
			return supervisorBinding{}, fmt.Errorf("duplicate supervisor argument %s", name)
		}
		seen[name] = true
		switch name {
		case "--run-id":
			runIDText = value
		case "--run-dir":
			binding.runDir = value
		case "--process-token":
			binding.token = value
		default:
			return supervisorBinding{}, fmt.Errorf("unknown supervisor argument %s", name)
		}
	}
	runID, err := strconv.ParseInt(runIDText, 10, 64)
	if err != nil {
		return supervisorBinding{}, err
	}
	binding.runID = runID
	if err := validateRunBinding(binding.runID, binding.token); err != nil {
		return supervisorBinding{}, err
	}
	if binding.runDir == "" || !filepath.IsAbs(binding.runDir) {
		return supervisorBinding{}, fmt.Errorf("supervisor run directory must be absolute")
	}
	binding.runDir = filepath.Clean(binding.runDir)
	return binding, nil
}

func validateRunBinding(runID int64, token string) error {
	if runID <= 0 {
		return fmt.Errorf("finite run ID must be positive")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(decoded) != 32 || len(token) != 43 || strings.ContainsAny(token, " \t\r\n") {
		return fmt.Errorf("finite process token must be 32 random bytes in base64url form")
	}
	return nil
}

func runEnvironment(runID int64, token, runDir string) []string {
	return []string{
		fmt.Sprintf("%s=%d", finiteRunIDEnv, runID),
		finiteRunTokenEnv + "=" + token,
		finiteRunDirEnv + "=" + runDir,
		finiteJobNameEnv + "=" + finiteJobObjectName(token),
	}
}

func finiteJobObjectName(token string) string {
	return `Local\WerkrbyFinite-` + token
}

// inspectRunProcesses inspects one committed run against its recorded
// boundary: run directory, supervisor identity, and finite executable.
func (e *Engine) inspectRunProcesses(ctx context.Context, runID int64, token string) (RunProcesses, error) {
	boundary, err := e.runProcessBoundary(ctx, runID, token)
	if err != nil {
		return RunProcesses{Liveness: LivenessUnknown}, errors.Join(ErrProcessUnknown, err)
	}
	return InspectRunBoundary(ctx, boundary)
}

// stopRunProcesses stops one committed run's recorded boundary.
func (e *Engine) stopRunProcesses(ctx context.Context, runID int64, token string, grace time.Duration) (Liveness, error) {
	boundary, err := e.runProcessBoundary(ctx, runID, token)
	if err != nil {
		return LivenessUnknown, errors.Join(ErrProcessUnknown, err)
	}
	return StopRunBoundary(ctx, boundary, grace)
}

func (e *Engine) runProcessBoundary(ctx context.Context, runID int64, token string) (RunBoundary, error) {
	boundary := RunBoundary{RunID: runID, Token: token, Executable: e.opts.Executable}
	run, err := e.store.GetRun(ctx, runID)
	if err != nil {
		return boundary, err
	}
	if run.ProcessToken != token {
		return boundary, ErrProcessTokenMismatch
	}
	boundary.RunDir = run.RunDir
	boundary.Supervisor, err = recordedSupervisorIdentity(run)
	return boundary, err
}

// recordedSupervisorIdentity merges every durable record of a run's
// supervisor: the owner's identity file, the committed PID, and the
// supervisor's own start record. Records that disagree fail closed.
func recordedSupervisorIdentity(run Run) (ProcessIdentity, error) {
	owner, err := readEngineOwnerIdentity(run)
	if err != nil {
		return ProcessIdentity{}, err
	}
	started, err := readSupervisorStartIdentity(run)
	if err != nil {
		return ProcessIdentity{}, err
	}
	recorded := ProcessIdentity{RunID: run.ID, Token: run.ProcessToken}
	merge := func(pid int, start string) error {
		if pid <= 0 {
			return nil
		}
		if recorded.PID != 0 && recorded.PID != pid || recorded.Start != "" && start != "" && recorded.Start != start {
			return fmt.Errorf("%w: run %d supervisor records disagree", ErrProcessIdentityMismatch, run.ID)
		}
		recorded.PID = pid
		if start != "" {
			recorded.Start = start
		}
		return nil
	}
	if err := merge(owner.PID, owner.Start); err != nil {
		return ProcessIdentity{}, err
	}
	if run.SupervisorPID != nil {
		if err := merge(*run.SupervisorPID, ""); err != nil {
			return ProcessIdentity{}, err
		}
	}
	if err := merge(started.PID, started.Start); err != nil {
		return ProcessIdentity{}, err
	}
	return recorded, nil
}

// readSupervisorStartIdentity reads the supervisor-written start fence, which
// can precede every owner record when the owner crashes right after spawn.
func readSupervisorStartIdentity(run Run) (ProcessIdentity, error) {
	data, err := readSupervisorBoundedFile(filepath.Join(run.RunDir, "supervisor-started.json"), 64*1024)
	if errors.Is(err, os.ErrNotExist) {
		return ProcessIdentity{}, nil
	}
	if err != nil {
		return ProcessIdentity{}, err
	}
	var record supervisorStartRecord
	if err := json.Unmarshal(data, &record); err != nil || record.SchemaVersion != supervisorRequestVersion || record.Identity.PID <= 0 {
		// Not a start record that names a process: no identity evidence.
		return ProcessIdentity{}, nil
	}
	if record.Identity.RunID != run.ID || record.Identity.Token != run.ProcessToken {
		return ProcessIdentity{}, fmt.Errorf("%w: supervisor start record binding", ErrProcessIdentityMismatch)
	}
	return record.Identity, nil
}
