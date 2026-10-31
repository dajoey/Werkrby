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
