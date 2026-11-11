//go:build unix

package harness

import (
	"context"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

// A finite run's OS boundary is its supervisor's process group, so the
// bounded Hermes client must stay in that group instead of starting its own;
// cancelling it must kill only the client, never the supervisor's group.
func TestBoundedHeadlessClientStaysInTheSupervisorGroup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(ctx, "sleep", "30")
	configureBoundedHeadlessProcess(cmd, HarnessConfig{})
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	childGroup, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	if own := syscall.Getpgrp(); childGroup != own {
		t.Fatalf("bounded client process group = %d, want the supervisor's %d", childGroup, own)
	}
	if err := cmd.Cancel(); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cancel did not end the bounded client")
	}
}
