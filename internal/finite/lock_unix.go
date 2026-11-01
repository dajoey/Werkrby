//go:build !windows

package finite

import (
	"fmt"
	"os"
	"syscall"
)

func lockFile(file *os.File) error {
	// os.OpenFile already requests close-on-exec on Unix. Set it explicitly at
	// this ownership boundary so later platform changes cannot make the owner
	// descriptor inheritable by a supervisor.
	syscall.CloseOnExec(int(file.Fd()))
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if err == syscall.EWOULDBLOCK || err == syscall.EAGAIN {
			return fmt.Errorf("%w: %v", ErrWorkspaceLocked, err)
		}
		return err
	}
	return nil
}

func unlockFile(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
}
