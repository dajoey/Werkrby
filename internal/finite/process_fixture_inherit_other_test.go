//go:build !windows

package finite

import (
	"os/exec"

	"github.com/agent0ai/spynel/internal/harness"
)

// configureInheritFixtureProcess is the non-Windows stub: Linux fixture
// providers stay in the supervisor's process group without handle carriage.
func configureInheritFixtureProcess(*exec.Cmd, harness.HarnessConfig) {}
