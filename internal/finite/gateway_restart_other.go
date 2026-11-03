//go:build !linux

package finite

import (
	"context"
	"fmt"
	"syscall"
	"time"
)

// gatewayRestartPlatformSupported: the drained restart drives a systemd user
// unit and scans procfs, so it exists only on Linux and fails closed elsewhere.
func gatewayRestartPlatformSupported() bool { return false }

func listProcSupervisorPIDs(ctx context.Context) ([]int, error) {
	return nil, fmt.Errorf("supervisor scan: unsupported platform")
}

func procProcessCmdline(_ context.Context, pid int) ([]string, error) {
	return nil, fmt.Errorf("/proc/%d/cmdline: unsupported platform", pid)
}

func procProcessAlive(_ context.Context, pid int) bool {
	return syscall.Kill(pid, 0) == nil
}

func procInServeCgroup() bool { return false }

// gatewayBootPoll: how often the fresh-boot readiness poll reads the state
// file (see gateway_restart_linux.go).
const gatewayBootPoll = 500 * time.Millisecond

// gatewayDetachSysProcAttr: detached-spawn attributes. Only Linux restarts.
func gatewayDetachSysProcAttr() *syscall.SysProcAttr { return nil }
