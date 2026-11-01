//go:build unix

package harness

import (
	"os/exec"
	"syscall"
	"time"
)

// configureHeadlessProcess gives the provider its own process group so cancellation
// also kills tool and MCP children. WaitDelay bounds inherited-pipe waits.
func configureHeadlessProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 10 * time.Second
}

// configureBoundedHeadlessProcess keeps a finite run's provider client in the
// supervisor's process group, which is the run's OS boundary: a client that
// outlives a killed supervisor stays visible to recovery and is signalled by
// group stops. Cancellation kills only the client.
func configureBoundedHeadlessProcess(cmd *exec.Cmd) {
	cmd.Cancel = func() error { return cmd.Process.Kill() }
	cmd.WaitDelay = 10 * time.Second
}
