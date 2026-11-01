//go:build !windows

package finite

import (
	"errors"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestFiniteResultRejectsFIFOWithoutBlocking(t *testing.T) {
	run := Run{ID: 42, TaskID: "task-fifo", Phase: PhaseImplementation, RunDir: t.TempDir()}
	path := filepath.Join(run.RunDir, "result.json")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := ReadResult(run)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrInvalidResult) {
			t.Errorf("FIFO result error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Errorf("FIFO open blocked result validation")
	}
}
