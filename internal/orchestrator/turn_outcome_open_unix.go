//go:build !windows

package orchestrator

import (
	"os"
	"syscall"
)

// openResultFile opens result.json without following a symlink and without blocking on a FIFO.
func openResultFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
}
