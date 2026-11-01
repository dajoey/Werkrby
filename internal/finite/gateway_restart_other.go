//go:build !linux

package finite

import (
	"context"
	"fmt"
	"syscall"
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
