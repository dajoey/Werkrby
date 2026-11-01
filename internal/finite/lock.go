package finite

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

// AcquireWorkspaceLock takes root/owner.lock without waiting. The lock file is
// retained; only its OS descriptor conveys ownership.
func AcquireWorkspaceLock(root string) (*WorkspaceLock, error) {
	if root == "" {
		return nil, fmt.Errorf("finite workspace root is empty")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(filepath.Join(root, "owner.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := lockFile(file); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &WorkspaceLock{file: file}, nil
}

// Close releases the OS lock. It is idempotent.
func (l *WorkspaceLock) Close() error {
	if l == nil {
		return nil
	}
	l.once.Do(func() {
		if l.file == nil {
			return
		}
		l.err = errors.Join(unlockFile(l.file), l.file.Close())
		l.file = nil
	})
	return l.err
}
