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

func TestHerdrChatIdleSweep(t *testing.T) {
	dir := t.TempDir()
	scratchRoot := filepath.Join(dir, "scratch")
	_ = os.MkdirAll(scratchRoot, 0o755)
	eventsFile := filepath.Join(dir, "events.log")
	scriptPath := filepath.Join(dir, "fake-herdr")

	script := fmt.Sprintf(`#!/bin/sh
cmd="$1"
shift
case "$cmd" in
status)
	echo "status: running"
	;;
agent)
	case "$1" in
	list)
		cat << JSON
{"result":{"agents":[
{"name":"wk-c-cli-old","workspace_id":"w1","tab_id":"w1:told","pane_id":"w1:pold","cwd":%q,"agent_status":"idle"},
{"name":"wk-c-cli-fresh","workspace_id":"w1","tab_id":"w1:tfresh","pane_id":"w1:pfresh","cwd":%q,"agent_status":"idle"},
{"name":"wk-c-telegram-tg-1701167661","workspace_id":"w1","tab_id":"w1:ttele","pane_id":"w1:ptele","cwd":%q,"agent_status":"idle"},
{"name":"wk-c-tui-main","workspace_id":"w1","tab_id":"w1:ttui","pane_id":"w1:ptui","cwd":%q,"agent_status":"idle"},
{"name":"wk-c-cli-working","workspace_id":"w1","tab_id":"w1:twork","pane_id":"w1:pwork","cwd":%q,"agent_status":"working"}
]}}
JSON
		;;
	rename)
		echo "agent rename $2 $3" >> %q
		;;
	esac
	;;
tab)
	echo "tab $1 $2" >> %q
	;;
pane)
	echo "pane $1 $2" >> %q
	;;
esac
`, scratchRoot, scratchRoot, scratchRoot, scratchRoot, scratchRoot, eventsFile, eventsFile, eventsFile)

	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	cliHistDir := filepath.Join(scratchRoot, ".werkrby", "history", "cli")
	teleHistDir := filepath.Join(scratchRoot, ".werkrby", "history", "telegram")
	tuiHistDir := filepath.Join(scratchRoot, ".werkrby", "history", "tui")
	_ = os.MkdirAll(cliHistDir, 0o755)
	_ = os.MkdirAll(teleHistDir, 0o755)
	_ = os.MkdirAll(tuiHistDir, 0o755)

	now := time.Now()

	// 1. old CLI conversation: idle 7 hours (> 6h default threshold)
	oldPath := filepath.Join(cliHistDir, "old.jsonl")
	oldTime := now.Add(-7 * time.Hour)
	oldEntry := fmt.Sprintf(`{"role":"assistant","sender":"Spy","content":"done","at":%q}`+"\n", oldTime.Format(time.RFC3339Nano))
	_ = os.WriteFile(oldPath, []byte(oldEntry), 0o600)
	_ = os.Chtimes(oldPath, oldTime, oldTime)

	// 2. fresh CLI conversation: idle 10 minutes (< 6h threshold)
	freshPath := filepath.Join(cliHistDir, "fresh.jsonl")
	freshTime := now.Add(-10 * time.Minute)
	freshEntry := fmt.Sprintf(`{"role":"assistant","sender":"Spy","content":"working","at":%q}`+"\n", freshTime.Format(time.RFC3339Nano))
	_ = os.WriteFile(freshPath, []byte(freshEntry), 0o600)
	_ = os.Chtimes(freshPath, freshTime, freshTime)

	// 3. telegram conversation: 30 days old
	telePath := filepath.Join(teleHistDir, "tg-1701167661.jsonl")
	teleTime := now.Add(-30 * 24 * time.Hour)
	teleEntry := fmt.Sprintf(`{"role":"assistant","sender":"Spy","content":"hi","at":%q}`+"\n", teleTime.Format(time.RFC3339Nano))
	_ = os.WriteFile(telePath, []byte(teleEntry), 0o600)
	_ = os.Chtimes(telePath, teleTime, teleTime)

	// 4. tui conversation: 30 days old
	tuiPath := filepath.Join(tuiHistDir, "main.jsonl")
	tuiTime := now.Add(-30 * 24 * time.Hour)
	tuiEntry := fmt.Sprintf(`{"role":"assistant","sender":"Spy","content":"tui","at":%q}`+"\n", tuiTime.Format(time.RFC3339Nano))
	_ = os.WriteFile(tuiPath, []byte(tuiEntry), 0o600)
	_ = os.Chtimes(tuiPath, tuiTime, tuiTime)

	// 5. working CLI conversation: 7 hours old but agent_status is working
	workingPath := filepath.Join(cliHistDir, "working.jsonl")
	workingTime := now.Add(-7 * time.Hour)
	workingEntry := fmt.Sprintf(`{"role":"user","sender":"Joey","content":"run","at":%q}`+"\n", workingTime.Format(time.RFC3339Nano))
	_ = os.WriteFile(workingPath, []byte(workingEntry), 0o600)
	_ = os.Chtimes(workingPath, workingTime, workingTime)

	h, err := NewHerdr(HarnessConfig{
		Name:                    "herdr",
		Command:                 scriptPath,
		Cwd:                     scratchRoot,
		SessionsFile:            filepath.Join(dir, "sessions.json"),
		CLIChatIdleThreshold:    6 * time.Hour,
		CLIChatIdleThresholdSet: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	h.sweepIdleCLIChatPanes(context.Background())

	data, _ := os.ReadFile(eventsFile)
	events := string(data)

	// Case 1: CLI chat pane idle past threshold is closed
	if !strings.Contains(events, "tab close w1:told") {
		t.Errorf("idle CLI chat pane w1:told (>6h) was not closed; events:\n%s", events)
	}
	if !strings.Contains(events, "agent rename wk-c-cli-old --clear") {
		t.Errorf("idle CLI chat agent rename was not cleared; events:\n%s", events)
	}

	// Case 2: CLI chat pane inside threshold is kept
	if strings.Contains(events, "w1:tfresh") || strings.Contains(events, "wk-c-cli-fresh") {
		t.Errorf("fresh CLI chat pane w1:tfresh (<6h) was closed; events:\n%s", events)
	}

	// Case 3: telegram and tui chat panes are kept at any age
	if strings.Contains(events, "w1:ttele") || strings.Contains(events, "wk-c-telegram") {
		t.Errorf("telegram chat pane was closed; events:\n%s", events)
	}
	if strings.Contains(events, "w1:ttui") || strings.Contains(events, "wk-c-tui") {
		t.Errorf("tui chat pane was closed; events:\n%s", events)
	}

	// Case 4: working pane is never closed
	if strings.Contains(events, "w1:twork") || strings.Contains(events, "wk-c-cli-working") {
		t.Errorf("working CLI chat pane w1:twork was closed; events:\n%s", events)
	}

	// Case 5: in-flight turn on an old pane prevents close
	_ = os.Remove(eventsFile)
	h.mu.Lock()
	h.active["chat:cli:old"] = &herdrTurn{target: "wk-c-cli-old", tabID: "w1:told"}
	h.mu.Unlock()

	h.sweepIdleCLIChatPanes(context.Background())
	data, _ = os.ReadFile(eventsFile)
	events = string(data)
	if strings.Contains(events, "w1:told") {
		t.Errorf("in-flight turn pane w1:told was closed; events:\n%s", events)
	}
}
