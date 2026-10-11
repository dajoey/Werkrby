package harness

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestNewHerdrDefaults(t *testing.T) {
	cfg := HarnessConfig{Name: "herdr"}
	h, err := NewHerdr(cfg)
	if err != nil {
		t.Fatalf("NewHerdr failed: %v", err)
	}
	if h.config.Command != "herdr" {
		t.Errorf("expected command 'herdr', got %q", h.config.Command)
	}
	if h.config.Cwd != "." {
		t.Errorf("expected cwd '.', got %q", h.config.Cwd)
	}
}

func TestHerdrModels(t *testing.T) {
	h, err := NewHerdr(HarnessConfig{Name: "herdr"})
	if err != nil {
		t.Fatalf("NewHerdr failed: %v", err)
	}
	models, err := h.Models(context.Background())
	if err != nil {
		t.Fatalf("Models failed: %v", err)
	}
	if len(models) == 0 {
		t.Fatalf("expected at least base models, got 0")
	}

	foundClaude := false
	for _, m := range models {
		if m.ID == "claude" {
			foundClaude = true
			break
		}
	}
	if !foundClaude {
		t.Errorf("expected 'claude' model in Herdr models list")
	}
}

func TestHerdrModelsOfferEveryRosterStaffRunner(t *testing.T) {
	// Minimal roster shape matching .spynel/roster.yaml staff.runner pins.
	const rosterYAML = `
staff:
  implementer:
    runner: cursor
  overflow:
    runner: pi
  researcher:
    runner: agy
  chat:
    runner: hermes
  planner:
    runner: opencode
  reviewer:
    runner: claude
`
	offered := map[string]bool{}
	h, err := NewHerdr(HarnessConfig{Name: "herdr"})
	if err != nil {
		t.Fatalf("NewHerdr failed: %v", err)
	}
	models, err := h.Models(context.Background())
	if err != nil {
		t.Fatalf("Models failed: %v", err)
	}
	for _, model := range models {
		offered[model.ID] = true
	}

	required := []string{"cursor", "pi", "agy", "hermes", "opencode", "claude"}
	for _, runner := range required {
		if !offered[runner] {
			t.Errorf("Models() missing roster staff runner %q", runner)
		}
	}

	// Keep the fixture honest: every runner: line in it must be covered above.
	for _, line := range strings.Split(rosterYAML, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "runner:") {
			continue
		}
		runner := strings.TrimSpace(strings.TrimPrefix(line, "runner:"))
		if runner == "" || !offered[runner] {
			t.Errorf("roster fixture runner %q is not offered by Models()", runner)
		}
	}
}

func TestHerdrSessionPersistence(t *testing.T) {
	dir := t.TempDir()
	sessionsFile := filepath.Join(dir, "sessions.json")

	h, err := NewHerdr(HarnessConfig{Name: "herdr", SessionsFile: sessionsFile})
	if err != nil {
		t.Fatalf("NewHerdr failed: %v", err)
	}

	h.rememberSession("test-key", "session-12345")
	if id := h.ThreadID("test-key"); id != "session-12345" {
		t.Fatalf("expected threadID 'session-12345', got %q", id)
	}

	// Reload from another instance
	h2, err := NewHerdr(HarnessConfig{Name: "herdr", SessionsFile: sessionsFile})
	if err != nil {
		t.Fatalf("NewHerdr h2 failed: %v", err)
	}
	if id := h2.ThreadID("test-key"); id != "session-12345" {
		t.Fatalf("expected reloaded threadID 'session-12345', got %q", id)
	}
}

func TestHerdrInBuiltinRegistry(t *testing.T) {
	registry := NewBuiltinRegistry()
	h, err := registry.Create(HarnessConfig{Name: "herdr", Cwd: t.TempDir()})
	if err != nil {
		t.Fatalf("registry.Create(herdr) failed: %v", err)
	}
	if _, ok := h.(*Herdr); !ok {
		t.Fatalf("expected *Herdr, got %T", h)
	}
}

func TestCleanHerdrTerminalOutput(t *testing.T) {
	raw := `  ┃  <!-- Add concise, lasting workspace-specific behavior for the communication agent below. -->
  ┃  </workspace_owner_persistent_instructions>
  ┃
  ┃  End of persistent instructions for the chat agent. The precedence stated above still applies to every imported rule.
  ┃

     Thought: 7.3s

     Acknowledging no available tools and explaining inability to inspect files while noting no prior durable work.

     Fresh start: nothing has been dispatched in this conversation yet, so there’s nothing to report. What would you like to work on first?

     ▣  Build · Muse Spark 1.3 Contributor · 10.1s

  ┃
  ┃
  ┃
  ┃  Build · Muse Spark 1.3 Contributor OpenCode Go · high                                                                                                                                                         ~/aibs/opencode
  ╹▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
   /home/dajoey/aibs/opencode                                                                                                                                             214.9K (20%) · $0.26  ctrl+p commands    • OpenCode 1.18.30`

	got := cleanHerdrTerminalOutput(raw)
	want := "Fresh start: nothing has been dispatched in this conversation yet, so there’s nothing to report. What would you like to work on first?"
	if got != want {
		t.Fatalf("cleanHerdrTerminalOutput = %q, want %q", got, want)
	}
}

func TestCleanHerdrTerminalOutputAgy1212Composer(t *testing.T) {
	fixturePath := filepath.Join(os.Getenv("HOME"), "ops/tests/fixtures/herdr-agy/control-idle-composer.txt")
	content, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Skipf("skipping fixture test: %v", err)
	}
	raw := string(content)
	if !agyInputBoxPattern.MatchString(raw) {
		t.Fatalf("agyInputBoxPattern did not match 1.2.12 composer in %s", fixturePath)
	}
	cleaned := cleanHerdrTerminalOutput(raw)
	if strings.Contains(cleaned, ">") || strings.Contains(cleaned, "? for shortcuts") || strings.Contains(cleaned, "───") {
		t.Fatalf("cleanHerdrTerminalOutput did not strip composer from output:\n%s", cleaned)
	}
}

func TestWorkerNameAndLabelSiblingHelmIDsDoNotCollide(t *testing.T) {
	// Real sibling Helm task IDs from 2026-09-22 agent_name_taken (errors.jsonl).
	// Distinguishing tails sit past the old 32-char head truncate, so both used to
	// become sj-t-a1-helm-joey-20260922-bugsi and the second dispatch failed.
	id7 := "tasks-helm-joey-20260922-bugsink-ops-scripts-7--a6420351-1790117702"
	id8 := "tasks-helm-joey-20260922-bugsink-ops-scripts-8--33005b1c-1790117703"
	key7 := "orchestrator:tasks:task_implementation:" + id7 + ":1"
	key8 := "orchestrator:tasks:task_implementation:" + id8 + ":1"

	name7, _ := workerNameAndLabel(key7, "opencode")
	name8, _ := workerNameAndLabel(key8, "opencode")

	nameRe := regexp.MustCompile(`^[a-z0-9_-]+$`)
	for _, name := range []string{name7, name8} {
		if len(name) == 0 || len(name) > 32 {
			t.Errorf("worker name %q length %d, want 1..32", name, len(name))
		}
		if !nameRe.MatchString(name) {
			t.Errorf("worker name %q must match [a-z0-9_-]+", name)
		}
	}
	if name7 == name8 {
		t.Fatalf("sibling Helm task IDs collided on worker name %q", name7)
	}
	// Deterministic across restarts (re-adoption looks panes up by this name).
	name7b, _ := workerNameAndLabel(key7, "opencode")
	if name7b != name7 {
		t.Fatalf("worker name for %s not stable: %q then %q", id7, name7, name7b)
	}
}

func TestWorkerNameAndLabel(t *testing.T) {
	tests := []struct {
		key          string
		model        string
		wantName     string
		wantTabLabel string
	}{
		{
			key:          "orchestrator:tasks:task_implementation:tasks-20260914-helm-fleet-update-03:1",
			model:        "opencode",
			wantName:     "sj-t-a1-helm-fleet-update-03",
			wantTabLabel: "Task: helm-fleet-update-03 (opencode)",
		},
		{
			key:          "orchestrator:tasks:task_review:tasks-20260914-helm-lmc-verify-02:1",
			model:        "kimi",
			wantName:     "sj-r-a1-helm-lmc-verify-02",
			wantTabLabel: "Review: helm-lmc-verify-02 (kimi)",
		},
		{
			key:          "orchestrator:goals:planning:goals-20260914-202731-helm01:1",
			model:        "opencode",
			wantName:     "sj-p-a1-helm01",
			wantTabLabel: "Plan: helm01 (opencode)",
		},
		{
			key:          "orchestrator:semantic-heartbeat",
			model:        "opencode",
			wantName:     "sj-hb-opencode",
			wantTabLabel: "Heartbeat (%s)",
		},
		{
			key:          "chat:tui:local-2a0229b8d17f26ca2335fa4eaaf39397",
			model:        "opencode",
			wantName:     "sj-c-tui-2a0229b8d17f26ca-31c8d3",
			wantTabLabel: "Chat: 2a0229b8d17f (opencode)",
		},
		{
			key:          "chat:tui:local-71f92e03b302",
			model:        "opencode",
			wantName:     "sj-c-tui-71f92e03b302",
			wantTabLabel: "Chat: 71f92e03b302 (opencode)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.key, func(t *testing.T) {
			gotName, gotLabel := workerNameAndLabel(tc.key, tc.model)
			if len(gotName) > 32 {
				t.Errorf("workerNameAndLabel name %q exceeds 32 chars (len=%d)", gotName, len(gotName))
			}
			if gotName != tc.wantName {
				t.Errorf("workerNameAndLabel(%q, %q) name = %q, want %q", tc.key, tc.model, gotName, tc.wantName)
			}
			if tc.wantTabLabel == "Heartbeat (%s)" {
				tc.wantTabLabel = "Heartbeat (opencode)"
			}
			if gotLabel != tc.wantTabLabel {
				t.Errorf("workerNameAndLabel(%q, %q) label = %q, want %q", tc.key, tc.model, gotLabel, tc.wantTabLabel)
			}
		})
	}
}

func TestWorkerSubjectAndWorkspaceLabel(t *testing.T) {
	tests := []struct {
		key         string
		wantSubject string
		wantLabel   string
	}{
		{
			key:         "orchestrator:tasks:task_implementation:tasks-helm-t-joey-1789190796770-1789493241:1",
			wantSubject: "helm",
			wantLabel:   "[sj-worker] helm",
		},
		{
			key:         "orchestrator:tasks:task_review:tasks-20260914-helm-lmc-verify-02:1",
			wantSubject: "helm",
			wantLabel:   "[sj-worker] helm",
		},
		{
			key:         "orchestrator:tasks:task_implementation:tasks-20260914-bst-prioritize-01:1",
			wantSubject: "tasks",
			wantLabel:   "[sj-worker] tasks",
		},
		{
			key:         "orchestrator:semantic-heartbeat",
			wantSubject: "tasks",
			wantLabel:   "[sj-worker] tasks",
		},
		{
			key:         "chat:telegram:1701167661",
			wantSubject: "telegram",
			wantLabel:   "[sj-worker] telegram",
		},
	}

	for _, tc := range tests {
		t.Run(tc.key, func(t *testing.T) {
			gotSubject := workerSubject(tc.key)
			if gotSubject != tc.wantSubject {
				t.Errorf("workerSubject(%q) = %q, want %q", tc.key, gotSubject, tc.wantSubject)
			}
			gotLabel := workerWorkspaceLabel(gotSubject)
			if gotLabel != tc.wantLabel {
				t.Errorf("workerWorkspaceLabel(%q) = %q, want %q", gotSubject, gotLabel, tc.wantLabel)
			}
		})
	}
}

func TestHerdrInterruptDeadlockAndPaneTeardown(t *testing.T) {
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
		echo '{"result":{"agent":{"agent":"opencode","agent_status":"working","agent_session":{"value":"ses-123"}}}}'
		;;
	rename)
		echo "agent-cleared" >> %q
		echo '{"result":{}}'
		;;
	send-keys)
		echo "send-keys" >> %q
		;;
	prompt)
		echo "prompt-started" >> %q
		while true; do
			sleep 0.1
		done
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
		if grep -q "tab-closed" %q 2>/dev/null; then
			exit 1
		fi
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
`, eventsFile, eventsFile, eventsFile, eventsFile, eventsFile, eventsFile, eventsFile)

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

	key := "orchestrator:tasks:task_implementation:test-task:1"
	sendDone := make(chan error, 1)
	go func() {
		_, _, sendErr := h.Send(context.Background(), key, "run test", nil)
		sendDone <- sendErr
	}()

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

	if !waitForEvent("prompt-started", 3*time.Second) {
		t.Fatal("prompt did not start within timeout")
	}

	interruptDone := make(chan struct{})
	var stopped bool
	var interruptErr error
	go func() {
		stopped, interruptErr = h.Interrupt(context.Background(), key)
		close(interruptDone)
	}()

	select {
	case <-interruptDone:
		if interruptErr != nil {
			t.Fatalf("Interrupt returned error: %v", interruptErr)
		}
		if !stopped {
			t.Fatal("Interrupt returned stopped = false")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Interrupt deadlocked while prompt was active (lockForKey deadlock)")
	}

	if !waitForEvent("tab-closed", 5*time.Second) {
		t.Fatal("tab was not closed after interrupt: worker pane left open")
	}
}

func TestTeardownWorkerForceClosesTabEvenWhenTurnIsActive(t *testing.T) {
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "herdr-mock.sh")
	eventsFile := filepath.Join(dir, "events.txt")

	scriptContent := fmt.Sprintf(`#!/usr/bin/env bash
cmd="$1"
sub="$2"
case "$cmd" in
tab)
	case "$sub" in
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
agent)
	case "$sub" in
	rename)
		echo "agent-cleared" >> %q
		echo '{"result":{}}'
		;;
	esac
	;;
esac
`, eventsFile, eventsFile, eventsFile)

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

	key := "orchestrator:tasks:task_implementation:test-task:1"
	// Simulate a turn that is active
	h.mu.Lock()
	h.active[key] = &herdrTurn{target: "sj-worker-1"}
	h.mu.Unlock()

	if !h.IsActive(key) {
		t.Fatal("expected turn to be active")
	}

	// Non-forced teardown should bail because turn is active
	h.teardownWorker(key, "tab-normal", "", "sj-worker-1", false)
	data, _ := os.ReadFile(eventsFile)
	if strings.Contains(string(data), "tab-closed") {
		t.Fatal("non-forced teardown closed tab while turn was active")
	}

	// Forced teardown MUST close the tab even when turn is active
	h.teardownWorker(key, "tab-force", "", "sj-worker-1", true)
	data, _ = os.ReadFile(eventsFile)
	if !strings.Contains(string(data), "tab-closed") {
		t.Fatal("forced teardown failed to close tab while turn was active")
	}
}

func TestWorkerNameAndLabelGenericFallbackNamesWerkrby(t *testing.T) {
	name, label := workerNameAndLabel("notification:adhoc-probe", "kimi")
	if label != "Werkrby (kimi)" {
		t.Fatalf("generic tab label = %q, want %q", label, "Werkrby (kimi)")
	}
	if !strings.HasPrefix(name, "sj-") {
		t.Fatalf("generic worker name = %q, want the sj- pane prefix", name)
	}
}

func TestAgyComposerPatternsAcceptLegacyAndWerkrbyNames(t *testing.T) {
	for _, name := range []string{"Antigravity", "agy", "SpyJo", "Werkrby"} {
		box := "╭──────────────────╮\n│ Message " + name + " here\n╰──────────────────╯"
		if !agyInputBoxPattern.MatchString(box) {
			t.Errorf("agyInputBoxPattern does not match the %q composer", name)
		}
		cleaned := cleanHerdrTerminalOutput("answer line\n\n" + box + "\n")
		if strings.Contains(cleaned, "Message "+name) {
			t.Errorf("cleanHerdrTerminalOutput kept the %q composer:\n%s", name, cleaned)
		}
	}
}
