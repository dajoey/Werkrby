//go:build linux

package finite

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// A crashed check leaves at most a directory with its meta and worktree; the
// next engine start removes every leftover and kills anything still running
// from the old worktree.
func TestAcceptanceLeftoverCleanup(t *testing.T) {
	base := t.TempDir()
	repo := acceptanceTestRepo(t, base)
	root := base

	// No meta.json means the check died before step 4: no worktree was ever
	// added, so the directory is deleted outright.
	orphan := filepath.Join(root, "acceptance", "7")
	if err := os.MkdirAll(orphan, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := cleanupAcceptanceLeftovers(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(orphan); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan directory survived cleanup: %v", err)
	}

	// A valid meta with a real worktree and a live process whose cwd is inside
	// it: cleanup kills the process, removes the worktree, and deletes the
	// directory.
	dir := filepath.Join(root, "acceptance", "8")
	wt := filepath.Join(dir, "wt")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	acceptanceGit(t, "-C", repo, "worktree", "add", "--detach", wt, "HEAD")
	meta := fmt.Sprintf(`{"repo":%q,"worktree":%q}`, repo, wt)
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), []byte(meta), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("/bin/sh", "-c", "sleep 60")
	command.Dir = wt
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waitCh := make(chan error, 1)
	go func() { waitCh <- command.Wait() }()
	reaped := false
	defer func() {
		_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		if !reaped {
			<-waitCh
		}
	}()

	if err := cleanupAcceptanceLeftovers(context.Background(), root); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	select {
	case err := <-waitCh:
		reaped = true
		if err == nil {
			t.Log("sleep exited cleanly before the kill reached it")
		}
	case <-time.After(acceptanceStragglerWait):
		t.Fatal("sleep process survived leftover cleanup")
	}
	if time.Now().After(deadline.Add(acceptanceStragglerWait)) {
		t.Fatal("cleanup waited past its own bound")
	}
	worktrees := acceptanceGit(t, "-C", repo, "worktree", "list")
	if strings.Contains(worktrees, wt) {
		t.Fatalf("worktree still listed after cleanup:\n%s", worktrees)
	}
	if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("acceptance directory survived cleanup: %v", err)
	}
}
