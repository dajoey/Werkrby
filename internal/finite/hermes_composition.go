package finite

import (
	"context"
	"errors"
	"path/filepath"
	"time"

	"github.com/agent0ai/spynel/internal/harness"
)

// runSupervisorWithBoundary activates exact Hermes isolation only for the real
// Herdr profile. Finite fixture and other provider factories keep their
// original supervisor context and lifecycle.
func runSupervisorWithBoundary(ctx context.Context, args []string, factory harness.Factory) error {
	fullArgs := args
	if len(fullArgs) == 0 || fullArgs[0] != "supervise" {
		fullArgs = append([]string{"supervise"}, fullArgs...)
	}
	binding, err := parseSupervisorArgs(fullArgs)
	if err != nil {
		return err
	}
	request, err := readSupervisorRequest(filepath.Join(binding.runDir, "supervisor.json"))
	if err != nil {
		return err
	}
	if request.Harness.Name != "herdr" {
		return RunSupervisor(ctx, fullArgs, factory)
	}
	spec := harness.RunBoundarySpec{RunID: binding.runID, ProcessToken: binding.token, RunDir: binding.runDir}
	return RunSupervisor(harness.WithRunBoundary(ctx, spec), fullArgs, factory)
}

// configureFiniteHermesOwner makes recovery and stop cover both the finite OS
// process boundary and the detached Hermes gateway turn. Other providers retain
// the ordinary OS-only boundary.
func configureFiniteHermesOwner(engine *Engine, provider harness.HarnessConfig) {
	if engine == nil || provider.Name != "herdr" {
		return
	}
	engine.opts.hooks.inspectRun = func(ctx context.Context, runID int64, token string) (RunProcesses, error) {
		processes, processErr := InspectRun(ctx, runID, token)
		run, runErr := engine.store.GetRun(ctx, runID)
		if runErr != nil {
			return RunProcesses{Liveness: LivenessUnknown}, runErr
		}
		provider, providerErr := harness.InspectHermesRunBoundary(ctx, harness.RunBoundarySpec{RunID: runID, ProcessToken: token, RunDir: run.RunDir})
		if processErr != nil || providerErr != nil {
			return RunProcesses{Liveness: LivenessUnknown, Supervisor: processes.Supervisor, Processes: processes.Processes}, errors.Join(processErr, providerErr)
		}
		if processes.Liveness == LivenessLive || provider.Liveness == harness.BoundaryLive {
			processes.Liveness = LivenessLive
		}
		return processes, nil
	}
	engine.opts.hooks.stopRun = func(ctx context.Context, runID int64, token string, grace time.Duration) (Liveness, error) {
		processState, processErr := StopRun(ctx, runID, token, grace)
		run, runErr := engine.store.GetRun(ctx, runID)
		if runErr != nil {
			return LivenessUnknown, runErr
		}
		providerState, providerErr := harness.StopHermesRunBoundary(ctx, harness.RunBoundarySpec{RunID: runID, ProcessToken: token, RunDir: run.RunDir})
		if processErr != nil || providerErr != nil {
			return LivenessUnknown, errors.Join(processErr, providerErr)
		}
		if processState == LivenessLive || providerState == harness.BoundaryLive {
			return LivenessLive, nil
		}
		if processState == LivenessAbsent && providerState == harness.BoundaryAbsent {
			return LivenessAbsent, nil
		}
		return LivenessUnknown, ErrProcessUnknown
	}
}
