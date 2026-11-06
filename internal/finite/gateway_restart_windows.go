//go:build windows

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

// procProcessAlive: there is no procfs signal-0 probe on Windows; nothing
// reads as alive, so every wait-for-exit loop stays terminal.
func procProcessAlive(_ context.Context, _ int) bool {
	return false
}

func procInServeCgroup() bool { return false }

// gatewayBootPoll: how often the fresh-boot readiness poll reads the state
// file (see gateway_restart_linux.go).
const gatewayBootPoll = 500 * time.Millisecond

// gatewayDetachSysProcAttr: detached-spawn attributes. Only Linux restarts.
func gatewayDetachSysProcAttr() *syscall.SysProcAttr { return nil }

// signalPID: process signaling is unsupported on Windows.
func signalPID(_ int, _ syscall.Signal) error {
	return fmt.Errorf("signal: unsupported platform")
}
