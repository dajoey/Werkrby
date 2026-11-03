//go:build linux

package finite

import (
	"os"
	"path/filepath"
	"strconv"
	"syscall"
)

// acceptancePlatformSupported gates the check: Linux runs declared commands.
const acceptancePlatformSupported = true

// acceptanceSysProcAttr starts the check in its own process group, so a
// timeout or engine close can kill the whole group, background children
// included.
func acceptanceSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}

// killAcceptanceWorktreeProcesses kills every process whose working directory
// is inside the crashed check's worktree, so nothing survives its removal.
// It scans /proc once and never signals this owner's own group.
func killAcceptanceWorktreeProcesses(worktree string) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return
	}
	target := filepath.Clean(worktree)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 1 || pid == syscall.Getpid() {
			continue
		}
		cwd, err := os.Readlink(filepath.Join("/proc", entry.Name(), "cwd"))
		if err != nil {
			continue
		}
		if pathWithin(filepath.Clean(cwd), target) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	}
}
