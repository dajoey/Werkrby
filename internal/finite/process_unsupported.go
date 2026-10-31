//go:build !linux && !windows

package finite

import "context"

const processBoundarySupported = false

func identifyProcess(int, int64, string) (ProcessIdentity, error) {
	return ProcessIdentity{}, ErrUnsupportedPlatform
}

func inspectRun(context.Context, int64, string) (RunProcesses, error) {
	return RunProcesses{Liveness: LivenessUnknown}, ErrUnsupportedPlatform
}

func signalProcess(ProcessIdentity, bool) error { return ErrUnsupportedPlatform }
