//go:build windows

package harness

import (
	"os/exec"
	"time"
)

// configureHeadlessProcess retains ordinary direct-process cancellation on Windows.
// This is only a compile/legacy-behavior boundary, not detached-descendant containment;
// the finite supervisor's Job Object boundary remains separate.
func configureHeadlessProcess(cmd *exec.Cmd) {
	cmd.Cancel = func() error { return cmd.Process.Kill() }
	cmd.WaitDelay = 10 * time.Second
}
