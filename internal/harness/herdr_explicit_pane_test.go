package harness

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// newExplicitPaneHerdr builds a Herdr whose model is the explicit pane target
// "w8:p2" and whose herdr command is a fake that logs every call to a file.
func newExplicitPaneHerdr(t *testing.T) (*Herdr, string) {
	t.Helper()
	dir := t.TempDir()
	callLog := filepath.Join(dir, "calls.txt")
	scriptPath := filepath.Join(dir, "fake-herdr")
	script := fmt.Sprintf(`#!/usr/bin/env bash
echo "$*" >> %q
case "$1 $2" in
"agent list")
	echo '{"result":{"agents":[]}}'
	;;
"agent get")
	echo '{"result":{"agent":{"agent":"hermes","agent_status":"idle","agent_session":{"value":"ses-123"}}}}'
	;;
"agent read")
	echo 'Done with work.'
	;;
"tab get")
	echo '{"result":{"tab":{"focused":false}}}'
	;;
*)
	echo '{"result":{}}'
	;;
esac
`, callLog)
	if err := os.WriteFile(scriptPath, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	h, err := NewHerdr(HarnessConfig{
		Name:         "herdr",
		Command:      scriptPath,
		Model:        "w8:p2",
		Cwd:          dir,
		SessionsFile: filepath.Join(dir, "sessions.json"),
	})
	if err != nil {
		t.Fatal(err)
	}
	h.SetIdleStability(1, 10*time.Millisecond)
	return h, callLog
}

// assertPaneUntouched fails when the call log shows the user-owned pane being
// closed or its agent name cleared.
func assertPaneUntouched(t *testing.T, callLog string) {
	t.Helper()
	data, _ := os.ReadFile(callLog)
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "pane close") || strings.HasPrefix(line, "tab close") ||
			strings.HasPrefix(line, "agent rename") {
			t.Fatalf("explicit pane target was torn down: %q\ncall log:\n%s", line, data)
		}
	}
}

func TestBugHerdrExplicitPaneTargetIsClosedAfterOrchestratorTurn(t *testing.T) {
	h, callLog := newExplicitPaneHerdr(t)

	key := "orchestrator:tasks:task_implementation:test-task:1"
	if _, _, err := h.Send(context.Background(), key, "run test", nil); err != nil {
		t.Fatalf("h.Send failed: %v", err)
	}

	// cleanupCompletedTab runs in a goroutine after a ~1.5s settle pause.
	time.Sleep(3 * time.Second)

	data, _ := os.ReadFile(callLog)
	if !strings.Contains(string(data), "w8:p2") {
		t.Fatalf("turn never addressed the explicit pane; call log:\n%s", data)
	}
	assertPaneUntouched(t, callLog)
}

func TestBugHerdrExplicitPaneTargetIsNotClosedByInterrupt(t *testing.T) {
	h, callLog := newExplicitPaneHerdr(t)
	// The agent never goes idle, so the turn stays active until interrupted.
	script, _ := os.ReadFile(h.config.Command)
	patched := strings.Replace(string(script), `"agent_status":"idle"`, `"agent_status":"working"`, 1)
	if err := os.WriteFile(h.config.Command, []byte(patched), 0755); err != nil {
		t.Fatal(err)
	}

	key := "orchestrator:tasks:task_implementation:test-task:1"
	sendDone := make(chan struct{})
	go func() {
		defer close(sendDone)
		_, _, _ = h.Send(context.Background(), key, "run test", nil)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for !h.IsActive(key) {
		if time.Now().After(deadline) {
			t.Fatal("turn never became active")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ok, err := h.Interrupt(context.Background(), key); err != nil || !ok {
		t.Fatalf("Interrupt = %v, %v; want true, nil", ok, err)
	}
	select {
	case <-sendDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Send did not return after Interrupt")
	}
	time.Sleep(500 * time.Millisecond)
	assertPaneUntouched(t, callLog)
}
