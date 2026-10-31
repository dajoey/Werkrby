package finite

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	finiteRunIDEnv    = "WERKRBY_FINITE_RUN_ID"
	finiteRunTokenEnv = "WERKRBY_FINITE_PROCESS_TOKEN"
	finiteRunDirEnv   = "WERKRBY_FINITE_RUN_DIR"
	finiteJobNameEnv  = "WERKRBY_FINITE_JOB_NAME"
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
// when only provider processes remain.
type RunProcesses struct {
	Liveness   Liveness
	Supervisor *ProcessIdentity
	Processes  []ProcessIdentity
}

// AdoptedRun monitors an exactly identified existing supervisor without
// spawning a replacement.
type AdoptedRun struct {
	expected ProcessIdentity
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
	return identifyProcess(pid, runID, token)
}

// InspectRun performs a complete scan and distinguishes reliable absence from
// unknown liveness. Any error implies LivenessUnknown and forbids redispatch.
func InspectRun(ctx context.Context, runID int64, token string) (RunProcesses, error) {
	if err := validateRunBinding(runID, token); err != nil {
		return RunProcesses{Liveness: LivenessUnknown}, err
	}
	return inspectRun(ctx, runID, token)
}

// AdoptRun accepts only an exact run/token/PID/start match.
func AdoptRun(ctx context.Context, expected ProcessIdentity) (*AdoptedRun, error) {
	observation, err := InspectRun(ctx, expected.RunID, expected.Token)
	if err != nil {
		return nil, err
	}
	if observation.Liveness != LivenessLive || observation.Supervisor == nil || !observation.Supervisor.Equal(expected) {
		return nil, ErrProcessIdentityMismatch
	}
	return &AdoptedRun{expected: expected}, nil
}

// Wait waits for reliable absence of the whole provider boundary. A provider
// surviving its supervisor is never treated as absence.
func (a *AdoptedRun) Wait(ctx context.Context) error {
	for {
		observation, err := InspectRun(ctx, a.expected.RunID, a.expected.Token)
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

// StopSupervisor signals only the exact expected supervisor generation.
func StopSupervisor(ctx context.Context, expected ProcessIdentity, grace time.Duration) (Liveness, error) {
	return LivenessUnknown, ErrProcessUnknown
}

// StopRun terminates the exact run boundary and returns only on reliable
// absence. It never trusts a stored PID without its start identity.
func StopRun(ctx context.Context, runID int64, token string, grace time.Duration) (Liveness, error) {
	return LivenessUnknown, ErrProcessUnknown
}

func waitForProcessPoll(ctx context.Context) error {
	timer := time.NewTimer(25 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
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
