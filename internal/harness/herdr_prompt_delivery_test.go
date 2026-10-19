package harness

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/core"
)

// TestHerdrUndeliveredPromptMustNotCountAsFinishedTurn verifies that when
// prompt --wait returns ok on a newly spawned pane, but there is no agent
// session, no new transcript, and the screen contains only the prompt echo
// plus startup banner (as in job 2760), Werkrby treats the prompt as not
// delivered: it must not emit EventFinal and must not close the tab as a
// finished turn, but return a prompt_not_delivered error.
func TestHerdrUndeliveredPromptMustNotCountAsFinishedTurn(t *testing.T) {
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
		# Fake reports idle with no agent session (session value is empty)
		echo '{"result":{"agent":{"agent":"agy","agent_status":"idle","agent_session":{"value":""}}}}'
		;;
	rename)
		echo '{"result":{}}'
		;;
	prompt)
		echo "prompt-finished" >> %q
		echo '{"result":{}}'
		;;
	read)
		# Reference job 2760 screen: echoed prompt above startup banner, no tool call or answer
		cat << 'EOF'
agy --model gemini-3.8-flash-high
dajoey@jobunthree:~/aibs/werkrby$ agy --model gemini-3.8-flash-high
You are running from the Werkrby orchestrator system.

Route: tasks
Allowed next statuses: todo, working, review, reviewing, waiting, done, failed, cancelled

      ▄▀▀▄        Antigravity CLI 1.2.12
     ▀▀▀▀▀▀       dajoey@gmail.com (Google AI Ultra)
    ▀▀▀▀▀▀▀▀      Gemini 3.8 Flash (High)
   ▄▀▀    ▀▀▄     ~/aibs/werkrby
  ▄▀▀      ▀▀▄
EOF
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
		Model:        "agy",
		SessionsFile: filepath.Join(dir, "sessions.json"),
	})
	if err != nil {
		t.Fatal(err)
	}
	h.SetIdleStability(3, 10*time.Millisecond)
	h.SetComposerWaitTimeout(20 * time.Millisecond)

	var mu sync.Mutex
	var emittedEvents []core.Event
	emit := func(e core.Event) {
		mu.Lock()
		emittedEvents = append(emittedEvents, e)
		mu.Unlock()
	}

	key := "orchestrator:tasks:task_implementation:test-task-undelivered:1"
	_, _, sendErr := h.Send(context.Background(), key, "run test", emit)

	mu.Lock()
	eventsCopy := append([]core.Event(nil), emittedEvents...)
	mu.Unlock()

	for _, ev := range eventsCopy {
		if ev.Kind == core.EventFinal {
			t.Fatalf("undelivered prompt emitted EventFinal as a finished turn: %+v", ev)
		}
	}

	if sendErr == nil {
		t.Fatalf("h.Send succeeded on undelivered prompt, expected error for in-place retry")
	}

	if !strings.Contains(sendErr.Error(), "prompt_not_delivered") {
		t.Fatalf("h.Send error = %q, want prompt_not_delivered", sendErr.Error())
	}

	var foundErrorEvent bool
	for _, ev := range eventsCopy {
		if ev.Kind == core.EventError && strings.Contains(ev.Text, "prompt_not_delivered") {
			foundErrorEvent = true
			break
		}
	}
	if !foundErrorEvent {
		t.Fatalf("did not emit EventError with prompt_not_delivered; events: %+v", eventsCopy)
	}

	time.Sleep(50 * time.Millisecond)
	data, _ := os.ReadFile(eventsFile)
	if strings.Contains(string(data), "tab-closed") {
		t.Fatalf("tab was closed after undelivered prompt; events:\n%s", string(data))
	}
}

// TestHerdrUndeliveredPromptResendsWhenComposerAppears verifies that when
// the first prompt was undelivered (no session/transcript), but the composer
// appears and the re-send succeeds, Werkrby emits EventFinal and completes.
func TestHerdrUndeliveredPromptResendsWhenComposerAppears(t *testing.T) {
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
		# First prompt: empty session. After prompt-2: session conv-123 is bound
		if grep -q "prompt-2" %q 2>/dev/null; then
			echo '{"result":{"agent":{"agent":"agy","agent_status":"idle","agent_session":{"value":"conv-123"}}}}'
		else
			echo '{"result":{"agent":{"agent":"agy","agent_status":"idle","agent_session":{"value":""}}}}'
		fi
		;;
	rename)
		echo '{"result":{}}'
		;;
	prompt)
		if grep -q "prompt-1" %q 2>/dev/null; then
			echo "prompt-2" >> %q
		else
			echo "prompt-1" >> %q
		fi
		echo '{"result":{}}'
		;;
	read)
		# After prompt-1, composer becomes visible
		if grep -q "prompt-2" %q 2>/dev/null; then
			echo "Task completed successfully."
		elif grep -q "prompt-1" %q 2>/dev/null; then
			cat << 'EOF'
      ▄▀▀▄        Antigravity CLI 1.2.12
     ▀▀▀▀▀▀       user@example.com
    ▀▀▀▀▀▀▀▀      Gemini 3.8 Flash (High)
   ▄▀▀    ▀▀▄     ~/aibs/werkrby
  ▄▀▀      ▀▀▄

────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
>
────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
? for shortcuts                                                                                  Gemini 3.8 Flash · high
EOF
		else
			cat << 'EOF'
      ▄▀▀▄        Antigravity CLI 1.2.12
     ▀▀▀▀▀▀       dajoey@gmail.com (Google AI Ultra)
    ▀▀▀▀▀▀▀▀      Gemini 3.8 Flash (High)
   ▄▀▀    ▀▀▄     ~/aibs/werkrby
  ▄▀▀      ▀▀▄
EOF
		fi
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
`, eventsFile, eventsFile, eventsFile, eventsFile, eventsFile, eventsFile, eventsFile, eventsFile, eventsFile)

	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0755); err != nil {
		t.Fatal(err)
	}

	h, err := NewHerdr(HarnessConfig{
		Name:         "herdr",
		Command:      scriptPath,
		Cwd:          dir,
		Model:        "agy",
		SessionsFile: filepath.Join(dir, "sessions.json"),
	})
	if err != nil {
		t.Fatal(err)
	}
	h.SetIdleStability(3, 10*time.Millisecond)
	h.SetComposerWaitTimeout(50 * time.Millisecond)

	var mu sync.Mutex
	var emittedEvents []core.Event
	emit := func(e core.Event) {
		mu.Lock()
		emittedEvents = append(emittedEvents, e)
		mu.Unlock()
	}

	key := "orchestrator:tasks:task_implementation:test-task-resend:1"
	threadID, _, sendErr := h.Send(context.Background(), key, "run test", emit)
	if sendErr != nil {
		t.Fatalf("h.Send failed on recovery re-send: %v", sendErr)
	}
	if threadID != "conv-123" {
		t.Fatalf("threadID = %q, want conv-123", threadID)
	}

	mu.Lock()
	eventsCopy := append([]core.Event(nil), emittedEvents...)
	mu.Unlock()

	var foundFinal bool
	for _, ev := range eventsCopy {
		if ev.Kind == core.EventFinal {
			foundFinal = true
			if !strings.Contains(ev.Text, "Task completed successfully.") {
				t.Fatalf("EventFinal text = %q, want Task completed successfully.", ev.Text)
			}
		}
	}
	if !foundFinal {
		t.Fatalf("expected EventFinal on recovered turn; events: %+v", eventsCopy)
	}
}
