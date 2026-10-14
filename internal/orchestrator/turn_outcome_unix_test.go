//go:build !windows

package orchestrator

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Finding 6: result.json is read only when it is a regular file, never through a symlink, a FIFO or
// a directory, and never past the size bound.
func TestReadTurnOutcomeRejectsNonRegularFiles(t *testing.T) {
	valid := `{"outcome":"review","progress":"x"}`
	elsewhere := filepath.Join(t.TempDir(), "outcome.json")
	if err := os.WriteFile(elsewhere, []byte(valid), 0o600); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(path string) error{
		"symlink":   func(path string) error { return os.Symlink(elsewhere, path) },
		"directory": func(path string) error { return os.Mkdir(path, 0o700) },
		"oversize": func(path string) error {
			return os.WriteFile(path, []byte(`{"outcome":"review","progress":"`+strings.Repeat("x", maxTurnOutcomeBytes)+`"}`), 0o600)
		},
	}
	for name, make := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "result.json")
			if err := make(path); err != nil {
				t.Fatal(err)
			}
			if got, _, err := readTurnOutcome(path, ""); err == nil {
				t.Fatalf("read %+v through a %s", got, name)
			}
		})
	}
	t.Run("fifo", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "result.json")
		if err := syscall.Mkfifo(path, 0o600); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			_, _, err := readTurnOutcome(path, "")
			done <- err
		}()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("a FIFO was read as an outcome")
			}
		case <-time.After(3 * time.Second):
			// Release the blocked reader so the test binary can exit.
			if writer, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NONBLOCK, 0); err == nil {
				writer.Close()
			}
			<-done
			t.Fatal("reading a FIFO named result.json blocked the dispatch")
		}
	})
}
