//go:build linux

package finite

import "context"

const processBoundarySupported = true

func identifyProcess(pid int, runID int64, token string) (ProcessIdentity, error) {
	return ProcessIdentity{RunID: runID, Token: token, PID: pid, Start: "scaffold"}, nil
}

func inspectRun(context.Context, int64, string) (RunProcesses, error) {
	return RunProcesses{Liveness: LivenessAbsent}, nil
}

func signalProcess(ProcessIdentity, bool) error { return nil }
