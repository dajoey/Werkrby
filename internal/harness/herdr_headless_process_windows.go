//go:build windows

package harness

import (
	"os/exec"
	"syscall"
	"time"
)

// configureHeadlessProcess retains ordinary direct-process cancellation on Windows.
// This is only a compile/legacy-behavior boundary, not detached-descendant containment;
// the finite supervisor's Job Object boundary remains separate.
func configureHeadlessProcess(cmd *exec.Cmd) {
	cmd.Cancel = func() error { return cmd.Process.Kill() }
	cmd.WaitDelay = 10 * time.Second
}

// configureBoundedHeadlessProcess hands the finite run job to the provider
// spawn on Windows: when the config carries the supervisor's query handle
// (WindowsRunJobHandle), listing it in AdditionalInheritedHandles makes the
// provider child inherit that one handle, so the job object stays open and
// observable while any run member lives even after the owner and the
// supervisor are both gone. Creation flags stay unset so the provider keeps
// its supervisor's process group and console for graceful stops.
func configureBoundedHeadlessProcess(cmd *exec.Cmd, cfg HarnessConfig) {
	configureHeadlessProcess(cmd)
	if cfg.WindowsRunJobHandle == 0 {
		return
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{
		AdditionalInheritedHandles: []syscall.Handle{syscall.Handle(cfg.WindowsRunJobHandle)},
	}
}
