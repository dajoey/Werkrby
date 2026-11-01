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
