//go:build !linux && !windows

package finite

import (
	"context"
	"os/exec"
)

const processBoundarySupported = false

func identifyProcess(int, int64, string) (ProcessIdentity, error) {
	return ProcessIdentity{}, ErrUnsupportedPlatform
}

func inspectRun(context.Context, int64, string) (RunProcesses, error) {
	return RunProcesses{Liveness: LivenessUnknown}, ErrUnsupportedPlatform
}

func signalProcess(ProcessIdentity, bool) error { return ErrUnsupportedPlatform }

func newSupervisorLaunch(*exec.Cmd, string) (supervisorLaunch, error) {
	return nil, ErrUnsupportedPlatform
}

func openSupervisorContainment(string) (supervisorContainment, error) {
	return nil, ErrUnsupportedPlatform
}
