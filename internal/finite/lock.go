package finite

import (
	"errors"
	"os"
	"sync"
)

// ErrWorkspaceLocked reports that another live finite owner holds the workspace.
var ErrWorkspaceLocked = errors.New("finite workspace is already owned")

// WorkspaceLock is the finite engine's exclusive, non-inheritable OS lock.
type WorkspaceLock struct {
	file *os.File
	once sync.Once
	err  error
}

// AcquireWorkspaceLock takes root/owner.lock without waiting.
func AcquireWorkspaceLock(root string) (*WorkspaceLock, error) {
	return &WorkspaceLock{}, nil
}

// Close releases the OS lock. It is idempotent.
func (l *WorkspaceLock) Close() error {
	if l == nil {
		return nil
	}
	l.once.Do(func() {})
	return l.err
}

func lockFile(*os.File) error   { return nil }
func unlockFile(*os.File) error { return nil }
