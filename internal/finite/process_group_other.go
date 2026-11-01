//go:build !linux

package finite

import "context"

// inspectRunBoundary scans by run binding alone where no process-group
// boundary evidence is recorded; Windows contains the run in its job object.
func inspectRunBoundary(ctx context.Context, boundary RunBoundary) (RunProcesses, error) {
	return inspectRun(ctx, boundary.RunID, boundary.Token)
}

func signalProcessGroup(int, bool) error { return nil }

func processGroupGone(int) bool { return false }

func verifyLiveSupervisor(ctx context.Context, expected ProcessIdentity) error {
	return verifyLiveSupervisorByScan(ctx, expected)
}

// waitForSupervisorExit returns at once; callers fall back to periodic scans.
func waitForSupervisorExit(ctx context.Context, _ ProcessIdentity) error { return ctx.Err() }
