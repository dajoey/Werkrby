package finite

import (
	"context"
	"errors"
	"fmt"
	"os"
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
		processes, processErr := engine.inspectRunProcesses(ctx, runID, token)
		run, runErr := engine.store.GetRun(ctx, runID)
		if runErr != nil {
			return RunProcesses{Liveness: LivenessUnknown}, runErr
		}
		provider, providerErr := inspectFiniteHermesRecovery(ctx, run)
		if processErr != nil || providerErr != nil {
			return RunProcesses{Liveness: LivenessUnknown, Supervisor: processes.Supervisor, Processes: processes.Processes}, errors.Join(processErr, providerErr)
		}
		if processes.Liveness == LivenessLive || provider.Liveness == harness.BoundaryLive {
			processes.Liveness = LivenessLive
		}
		// An absent combined boundary whose Hermes owner is gone is a
		// platform fault; recovery must not charge the turn.
		if processes.Liveness == LivenessAbsent {
			processes.ProviderOrphaned = provider.Orphaned
		}
		return processes, nil
	}
	engine.opts.hooks.stopRun = func(ctx context.Context, runID int64, token string, grace time.Duration) (Liveness, error) {
		processState, processErr := engine.stopRunProcesses(ctx, runID, token, grace)
		run, runErr := engine.store.GetRun(ctx, runID)
		if runErr != nil {
			return LivenessUnknown, runErr
		}
		observation, providerErr := inspectFiniteHermesRecovery(ctx, run)
		providerState := observation.Liveness
		if providerErr == nil && providerState == harness.BoundaryLive {
			providerState, providerErr = harness.StopHermesRunBoundary(ctx, harness.RunBoundarySpec{RunID: runID, ProcessToken: token, RunDir: run.RunDir})
		}
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

// inspectFiniteHermesRecovery distinguishes never-admitted terminal runs from
// missing evidence after admission. No supervisor start record proves no client
// was created. With a start record, valid terminal artifacts plus no exact
// admission prove a terminal pre-admission failure; everything else fails closed.
func inspectFiniteHermesRecovery(ctx context.Context, run Run) (harness.HermesBoundaryObservation, error) {
	spec := harness.RunBoundarySpec{RunID: run.ID, ProcessToken: run.ProcessToken, RunDir: run.RunDir}
	_, err := os.Lstat(filepath.Join(run.RunDir, "hermes-provider.json"))
	if err == nil {
		return harness.InspectHermesRunBoundary(ctx, spec)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return harness.HermesBoundaryObservation{Liveness: harness.BoundaryUnknown}, err
	}
	admitted, err := harness.HermesRunHasAdmission(ctx, spec)
	if err != nil {
		return harness.HermesBoundaryObservation{Liveness: harness.BoundaryUnknown}, err
	}
	if admitted {
		return harness.HermesBoundaryObservation{Liveness: harness.BoundaryUnknown}, fmt.Errorf("hermes-provider.json is missing after an exact admission")
	}
	if _, err := os.Lstat(filepath.Join(run.RunDir, "supervisor-started.json")); errors.Is(err, os.ErrNotExist) {
		return harness.HermesBoundaryObservation{Liveness: harness.BoundaryAbsent}, nil
	} else if err != nil {
		return harness.HermesBoundaryObservation{Liveness: harness.BoundaryUnknown}, err
	}
	execution, err := readRecoveredExecution(run)
	if err != nil {
		return harness.HermesBoundaryObservation{Liveness: harness.BoundaryUnknown}, err
	}
	_, resultErr, resultPresent := readRecoveredResult(run)
	if execution == nil && (!resultPresent || resultErr != nil) {
		return harness.HermesBoundaryObservation{Liveness: harness.BoundaryUnknown}, fmt.Errorf("no terminal evidence proves a never-admitted run")
	}
	return harness.HermesBoundaryObservation{Liveness: harness.BoundaryAbsent}, nil
}
