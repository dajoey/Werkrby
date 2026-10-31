//go:build unix

package harness

import (
	"bufio"
	"context"
	"errors"
	"os/exec"
	"strconv"
	"syscall"
	"testing"
	"time"
)

func TestConfigureHeadlessProcessUnix(t *testing.T) {
	cmd := exec.Command("unused")
	configureHeadlessProcess(cmd)

	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setpgid {
		t.Fatal("headless process did not receive its own process group")
	}
	if cmd.Cancel == nil {
		t.Fatal("headless process cancellation is not configured")
	}
	if cmd.WaitDelay != 10*time.Second {
		t.Fatalf("headless process WaitDelay = %v, want 10s", cmd.WaitDelay)
	}
}

func TestHeadlessProcessCancelKillsUnixProcessGroup(t *testing.T) {
	cmd := exec.CommandContext(context.Background(), "sh", "-c", `sleep 30 & child=$!; printf '%s\n' "$child"; wait`)
	configureHeadlessProcess(cmd)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Skipf("start shell fixture: %v", err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = cmd.Cancel()
			_ = cmd.Wait()
		}
	}()

	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		t.Fatalf("read child PID: %v", scanner.Err())
	}
	childPID, err := strconv.Atoi(scanner.Text())
	if err != nil {
		t.Fatalf("parse child PID: %v", err)
	}
	if err := cmd.Cancel(); err != nil {
		t.Fatalf("cancel process group: %v", err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("cancelled process exited successfully")
	}
	waited = true

	deadline := time.Now().Add(5 * time.Second)
	for {
		err := syscall.Kill(childPID, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("child process %d survived process-group cancellation (probe error %v)", childPID, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
