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

func TestHerdrIdleMustBeStableBeforeTabClose(t *testing.T) {
	dir := t.TempDir()
	eventsFile := filepath.Join(dir, "events.txt")
	scriptPath := filepath.Join(dir, "fake-herdr")
	scriptContent := fmt.Sprintf(`#!/usr/bin/env bash
cmd="$1"
sub="$2"
shift 2

case "$cmd" in
workspace)
	echo '{"result":{"workspaces":[{"workspace_id":"w1","label":"[sj-worker] test"}]}}'
	;;
agent)
	case "$sub" in
	list)
		echo '{"result":{"agents":[]}}'
		;;
	start)
		echo '{"result":{}}'
		;;
	get)
		if grep -q "agent-waited" %q 2>/dev/null; then
			echo '{"result":{"agent":{"agent":"hermes","agent_status":"idle","agent_session":{"value":"ses-123"}}}}'
		else
			echo '{"result":{"agent":{"agent":"hermes","agent_status":"working","agent_session":{"value":"ses-123"}}}}'
		fi
		;;
	wait)
		echo "agent-waited" >> %q
		echo '{"result":{}}'
		;;
	rename)
		echo '{"result":{}}'
		;;
	prompt)
		echo "prompt-finished" >> %q
		echo '{"result":{}}'
		;;
	read)
		echo 'Done with work.'
		;;
	*)
		echo '{"result":{}}'
		;;
	esac
	;;
tab)
	case "$sub" in
	create)
		echo "tab-created" >> %q
		echo '{"result":{"tab":{"tab_id":"tab-123"},"root_pane":{"pane_id":"pane-456"}}}'
		;;
	get)
		echo '{"result":{"tab":{"focused":false}}}'
		;;
	close)
		echo "tab-closed" >> %q
		echo '{"result":{}}'
		;;
	esac
	;;
pane)
	case "$sub" in
	close)
		echo "pane-closed" >> %q
		echo '{"result":{}}'
		;;
	esac
	;;
esac
`, eventsFile, eventsFile, eventsFile, eventsFile, eventsFile, eventsFile)

	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0755); err != nil {
		t.Fatal(err)
	}

	h, err := NewHerdr(HarnessConfig{
		Name:         "herdr",
		Command:      scriptPath,
		Cwd:          dir,
		SessionsFile: filepath.Join(dir, "sessions.json"),
	})
	if err != nil {
		t.Fatal(err)
	}
	h.SetIdleStability(3, 10*time.Millisecond)
	// The default runner is hermes, whose new pane waits for its composer instead of
	// returning on the session id this fake reports; keep that wait short.
	h.SetComposerWaitTimeout(20 * time.Millisecond)

	key := "orchestrator:tasks:task_implementation:test-task:1"
	_, _, sendErr := h.Send(context.Background(), key, "run test", nil)
	if sendErr != nil {
		t.Fatalf("h.Send failed: %v", sendErr)
	}

	waitForEvent := func(eventName string, timeout time.Duration) bool {
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			data, err := os.ReadFile(eventsFile)
			if err == nil && strings.Contains(string(data), eventName) {
				return true
			}
			time.Sleep(20 * time.Millisecond)
		}
		return false
	}

	if !waitForEvent("agent-waited", 3*time.Second) {
		data, _ := os.ReadFile(eventsFile)
		t.Fatalf("harness closed tab or finished without waiting for working agent to reach stable idle; events:\n%s", string(data))
	}
	if !waitForEvent("tab-closed", 3*time.Second) {
		data, _ := os.ReadFile(eventsFile)
		t.Fatalf("tab was not closed after agent finished; events:\n%s", string(data))
	}
	data, err := os.ReadFile(eventsFile)
	if err != nil {
		t.Fatal(err)
	}
	events := string(data)
	waitedIdx := strings.Index(events, "agent-waited")
	closedIdx := strings.Index(events, "tab-closed")
	if closedIdx != -1 && closedIdx < waitedIdx {
		t.Fatalf("tab was closed before agent finished waiting; events:\n%s", events)
	}
}

func TestHerdrGenuinelyIdleTurnCloses(t *testing.T) {
	dir := t.TempDir()
	eventsFile := filepath.Join(dir, "events.txt")
	scriptPath := filepath.Join(dir, "fake-herdr")
	scriptContent := fmt.Sprintf(`#!/usr/bin/env bash
cmd="$1"
sub="$2"
shift 2

case "$cmd" in
workspace)
	echo '{"result":{"workspaces":[{"workspace_id":"w1","label":"[sj-worker] test"}]}}'
	;;
agent)
	case "$sub" in
	list)
		echo '{"result":{"agents":[]}}'
		;;
	start)
		echo '{"result":{}}'
		;;
	get)
		echo '{"result":{"agent":{"agent":"hermes","agent_status":"idle","agent_session":{"value":"ses-123"}}}}'
		;;
	rename)
		echo '{"result":{}}'
		;;
	prompt)
		echo "prompt-finished" >> %q
		echo '{"result":{}}'
		;;
	read)
		echo 'Done with work.'
		;;
	*)
		echo '{"result":{}}'
		;;
	esac
	;;
tab)
	case "$sub" in
	create)
		echo "tab-created" >> %q
		echo '{"result":{"tab":{"tab_id":"tab-123"},"root_pane":{"pane_id":"pane-456"}}}'
		;;
	get)
		echo '{"result":{"tab":{"focused":false}}}'
		;;
	close)
		echo "tab-closed" >> %q
		echo '{"result":{}}'
		;;
	esac
	;;
pane)
	case "$sub" in
	close)
		echo "pane-closed" >> %q
		echo '{"result":{}}'
		;;
	esac
	;;
esac
`, eventsFile, eventsFile, eventsFile, eventsFile)

	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0755); err != nil {
		t.Fatal(err)
	}

	h, err := NewHerdr(HarnessConfig{
		Name:         "herdr",
		Command:      scriptPath,
		Cwd:          dir,
		SessionsFile: filepath.Join(dir, "sessions.json"),
	})
	if err != nil {
		t.Fatal(err)
	}
	h.SetIdleStability(3, 10*time.Millisecond)
	// The default runner is hermes, whose new pane waits for its composer instead of
	// returning on the session id this fake reports; keep that wait short.
	h.SetComposerWaitTimeout(20 * time.Millisecond)

	key := "orchestrator:tasks:task_implementation:test-task-idle:1"
	start := time.Now()
	_, _, sendErr := h.Send(context.Background(), key, "run test", nil)
	if sendErr != nil {
		t.Fatalf("h.Send failed: %v", sendErr)
	}
	elapsed := time.Since(start)
	if elapsed > 3*time.Second {
		t.Fatalf("genuinely idle turn took %v, expected fast completion", elapsed)
	}

	waitForEvent := func(eventName string, timeout time.Duration) bool {
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			data, err := os.ReadFile(eventsFile)
			if err == nil && strings.Contains(string(data), eventName) {
				return true
			}
			time.Sleep(20 * time.Millisecond)
		}
		return false
	}

	if !waitForEvent("tab-closed", 3*time.Second) {
		data, _ := os.ReadFile(eventsFile)
		t.Fatalf("tab was not closed after genuinely idle turn; events:\n%s", string(data))
	}
}

// TestHerdrUnknownStatusDoesNotBusyLoop: an agent_status value the harness does
// not know (a future herdr value) must neither count as idle nor spin
// `agent get` in a tight loop while the turn context is alive. Reviewer note
// 2026-09-27: pre-fix, the default switch arm re-exec'd herdr with no sleep.
func TestHerdrUnknownStatusDoesNotBusyLoop(t *testing.T) {
	dir := t.TempDir()
	eventsFile := filepath.Join(dir, "events.txt")
	scriptPath := filepath.Join(dir, "fake-herdr")
	scriptContent := fmt.Sprintf(`#!/usr/bin/env bash
cmd="$1"
sub="$2"
shift 2

case "$cmd" in
workspace)
	echo '{"result":{"workspaces":[{"workspace_id":"w1","label":"[sj-worker] test"}]}}'
	;;
agent)
	case "$sub" in
	list)
		echo '{"result":{"agents":[]}}'
		;;
	start)
		echo '{"result":{}}'
		;;
	get)
		echo "agent-get" >> %q
		echo '{"result":{"agent":{"agent":"hermes","agent_status":"thinking","agent_session":{"value":"ses-123"}}}}'
		;;
	wait)
		echo "agent-waited" >> %q
		echo '{"result":{}}'
		;;
	rename)
		echo '{"result":{}}'
		;;
	prompt)
		echo "prompt-finished" >> %q
		echo '{"result":{}}'
		;;
	read)
		echo 'Done with work.'
		;;
	*)
		echo '{"result":{}}'
		;;
	esac
	;;
tab)
	case "$sub" in
	create)
		echo "tab-created" >> %q
		echo '{"result":{"tab":{"tab_id":"tab-123"},"root_pane":{"pane_id":"pane-456"}}}'
		;;
	get)
		echo '{"result":{"tab":{"focused":false}}}'
		;;
	close)
		echo "tab-closed" >> %q
		echo '{"result":{}}'
		;;
	esac
	;;
pane)
	case "$sub" in
	close)
		echo "pane-closed" >> %q
		echo '{"result":{}}'
		;;
	esac
	;;
esac
`, eventsFile, eventsFile, eventsFile, eventsFile, eventsFile, eventsFile)

	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0755); err != nil {
		t.Fatal(err)
	}

	h, err := NewHerdr(HarnessConfig{
		Name:         "herdr",
		Command:      scriptPath,
		Cwd:          dir,
		SessionsFile: filepath.Join(dir, "sessions.json"),
	})
	if err != nil {
		t.Fatal(err)
	}
	h.SetIdleStability(3, 100*time.Millisecond)

	key := "orchestrator:tasks:task_implementation:test-task-unknown:1"
	// resolveTarget sleeps 1.5s before the wait loop starts, so the window the
	// loop actually runs in is ctx minus ~1.6s of setup. 4s - 1.6s = ~2.4s of
	// looping: bounded cadence is ~24 reads, a busy loop execs hundreds.
	ctx, cancel := context.WithCancel(context.Background())
	sendDone := make(chan struct{})
	go func() {
		defer close(sendDone)
		_, _, _ = h.Send(ctx, key, "run test", nil)
	}()
	defer func() {
		cancel()
		select {
		case <-sendDone:
		case <-time.After(5 * time.Second):
		}
	}()
	select {
	case <-sendDone:
		data, _ := os.ReadFile(eventsFile)
		t.Fatalf("h.Send returned before cancellation on unknown status; events:\n%s", string(data))
	case <-time.After(4 * time.Second):
	}

	// Snapshot before cancellation: once Send returns, failed-turn cleanup may
	// legitimately close the tab. That cleanup is outside the stable-idle assertion.
	data, err := os.ReadFile(eventsFile)
	if err != nil {
		t.Fatal(err)
	}
	events := string(data)
	getCalls := strings.Count(events, "agent-get")
	// ~2.4 s of looping at 100 ms cadence is ~24 reads; a busy loop execs hundreds.
	if getCalls > 60 {
		t.Fatalf("agent get was called %d times in the window: unknown status busy-loops", getCalls)
	}
	if strings.Contains(events, "tab-closed") {
		t.Fatalf("tab was closed on unknown status before cancellation or stable idle; events:\n%s", events)
	}
	cancel()
	select {
	case <-sendDone:
	case <-time.After(5 * time.Second):
		t.Fatal("h.Send did not return after cancellation")
	}
}
