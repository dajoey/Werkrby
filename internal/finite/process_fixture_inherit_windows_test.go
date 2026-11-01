//go:build windows

package finite

import (
	"os/exec"
	"syscall"

	"github.com/agent0ai/spynel/internal/harness"
)

// configureInheritFixtureProcess mirrors the production bounded spawn: the
// fixture harness lists the finite run job's query handle from the harness
// config so its provider child inherits it, keeping the job object open
// exactly as the real adapter must.
func configureInheritFixtureProcess(command *exec.Cmd, cfg harness.HarnessConfig) {
	if cfg.WindowsRunJobHandle == 0 {
		return
	}
	command.SysProcAttr = &syscall.SysProcAttr{
		AdditionalInheritedHandles: []syscall.Handle{syscall.Handle(cfg.WindowsRunJobHandle)},
	}
}
