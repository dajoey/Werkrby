package harness

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/core"
)

// TestHerdrFailedTurnStopsWorker pins the fence from the 2026-10-26 double
// dispatch (tasks-20261026-werkrby-double-dispatch-01): when an orchestrator
// turn on a pane-backed worker fails, Werkrby must interrupt the runner and
// close the worker tab, so a session cannot keep editing the document after
// its turn ended. (The prompt_not_delivered failure is exempt: it never
// started a session and keeps the tab for the in-place retry.)
func TestHerdrFailedTurnStopsWorker(t *testing.T) {
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
		echo '{"result":{"agent":{"agent":"hermes","agent_status":"blocked","agent_session":{"value":"sess-1"}}}}'
		;;
	rename)
		echo '{"result":{}}'
		;;
	send-keys)
		echo "send-keys-$1" >> %q
		echo '{"result":{}}'
		;;
	prompt)
		echo "prompt failed: runner exploded"
		exit 1
		;;
	esac
	;;
tab)
	case "$sub" in
	create)
		echo '{"result":{"root_pane":{"pane_id":"w1:p1"},"tab":{"tab_id":"w1:t1"}}}'
		;;
	close)
		echo "tab-closed" >> %q
		echo '{"result":{}}'
		;;
	esac
	;;
esac
`, eventsFile, eventsFile)

	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0755); err != nil {
		t.Fatal(err)
	}

	h, err := NewHerdr(HarnessConfig{
		Name:         "herdr",
		Command:      scriptPath,
		Cwd:          dir,
		Model:        "hermes",
		SessionsFile: filepath.Join(dir, "sessions.json"),
	})
	if err != nil {
		t.Fatal(err)
	}
	h.SetIdleStability(3, 10*time.Millisecond)
	h.SetComposerWaitTimeout(20 * time.Millisecond)

	var gotFinal bool
	emit := func(e core.Event) {
		if e.Kind == core.EventFinal {
			gotFinal = true
		}
	}

	key := "orchestrator:tasks:task_implementation:test-task-fence:1"
	_, _, sendErr := h.Send(context.Background(), key, "run test", emit)
	if sendErr == nil {
		t.Fatal("h.Send succeeded on a blocked agent, expected a failed turn")
	}
	if gotFinal {
		t.Fatal("failed turn emitted EventFinal")
	}

	deadline := time.Now().Add(2 * time.Second)
	var data []byte
	for time.Now().Before(deadline) {
		data, _ = os.ReadFile(eventsFile)
		events := string(data)
		if strings.Contains(events, "send-keys-") && strings.Contains(events, "tab-closed") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("failed turn did not stop its worker; events:\n%s", string(data))
}
