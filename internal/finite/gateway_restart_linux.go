//go:build linux

package finite

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// gatewayRestartPlatformSupported: the drained restart drives a systemd user
// unit and scans procfs, so it exists only on Linux.
func gatewayRestartPlatformSupported() bool { return true }

// listProcSupervisorPIDs scans procfs for the finite supervisor processes:
// argv[0] named werkrby-finite with `supervise` as the first argument, exactly
// the shape `ps -eo args | grep '[w]erkrby-finite supervise'` counted by hand.
// It never matches the serve owner, workers' Hermes clients, or any other
// process.
func listProcSupervisorPIDs(ctx context.Context) ([]int, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, fmt.Errorf("read /proc: %w", err)
	}
	var pids []int
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		name := entry.Name()
		pid, convErr := strconv.Atoi(name)
		if convErr != nil || pid <= 0 {
			continue
		}
		args, err := procProcessCmdline(ctx, pid)
		if err != nil || len(args) < 2 {
			continue
		}
		if strings.HasSuffix(args[0], "/werkrby-finite") && args[1] == "supervise" {
			pids = append(pids, pid)
		}
	}
	return pids, nil
}

// procProcessCmdline reads a pid's argv, treating a vanished process as an
// error so callers never act on stale identities.
func procProcessCmdline(_ context.Context, pid int) ([]string, error) {
	content, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
	if err != nil {
		return nil, err
	}
	parts := strings.Split(strings.TrimRight(string(content), "\x00"), "\x00")
	var args []string
	for _, part := range parts {
		if part != "" {
			args = append(args, part)
		}
	}
	if len(args) == 0 {
		return nil, fmt.Errorf("/proc/%d/cmdline is empty", pid)
	}
	return args, nil
}

// procProcessAlive: signal 0 proves a live, signalable process, and a zombie
// (exited, unreaped) counts as dead — exactly what "wait for ps -p to print
// nothing" meant in the hand procedure.
func procProcessAlive(_ context.Context, pid int) bool {
	if syscall.Kill(pid, 0) != nil {
		return false
	}
	content, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false
	}
	closer := strings.LastIndex(string(content), ")")
	if closer < 0 || closer+2 >= len(content) {
		return true
	}
	return content[closer+2] != 'Z'
}

// procInServeCgroup reports whether this process runs inside the serve unit's
// cgroup — a worker turn — which may never restart the gateway it runs on.
// Adopted supervisors keep the unit's cgroup path after the unit stops, so the
// guard holds even mid-restart.
func procInServeCgroup() bool {
	const serveUnit = "werkrby-finite-serve.service"
	content, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		// Unreadable means unverifiable: refuse to run.
		return true
	}
	for _, line := range strings.Split(string(content), "\n") {
		if strings.Contains(line, serveUnit) {
			return true
		}
	}
	return false
}

// gatewayBootPoll: how often the fresh-boot readiness poll reads the state
// file. Boots report running within seconds; this only bounds the latency.
const gatewayBootPoll = 500 * time.Millisecond

// gatewayDetachSysProcAttr: the fresh gateway must outlive this command in
// its own session, exactly like hermes's own client-side detached spawn.
// The !linux build never reaches a boot (the restart refuses unsupported
// platforms in preflight).
func gatewayDetachSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setsid: true}
}
