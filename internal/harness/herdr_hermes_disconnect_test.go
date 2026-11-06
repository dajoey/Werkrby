package harness

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/core"
)

// runDisconnectTurn is runHermesTurnConfigured with a short ledger settle and an explicit
// disconnect wait, so a test exercises the post-disconnect ledger wait in milliseconds.
func runDisconnectTurn(t *testing.T, dir, script, key, prompt string, wait time.Duration) hermesTurnResult {
	t.Helper()
	h, err := NewHerdr(HarnessConfig{
		Name:         "herdr",
		Command:      script,
		Cwd:          dir,
		Model:        "hermes",
		SessionsFile: filepath.Join(dir, "sessions.json"),
	})
	if err != nil {
		t.Fatal(err)
	}
	h.SetIdleStability(3, 10*time.Millisecond)
	h.SetComposerWaitTimeout(20 * time.Millisecond)
	h.hermesLedgerSettle = 50 * time.Millisecond
	h.hermesDisconnectWait = wait
	var mu sync.Mutex
	var events []core.Event
	emit := func(e core.Event) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _, sendErr := h.Send(ctx, key, prompt, emit)
	// Let the deferred tab cleanup of a successful orchestrator turn finish before TempDir removal.
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	return hermesTurnResult{err: sendErr, events: events}
}

// hermesDisconnectPanel is the corner-less error panel Hermes draws when a pane client's
// gateway link drops mid-turn (gateway_connection.py DISCONNECTED).
func hermesDisconnectPanel() string {
	return " ─ ☤ Hermes " + strings.Repeat("─", 104) + " \n\n " + hermesDisconnectTurnCopy +
		" Resume the printed session ID to check whether it completed or was interrupted; do not resend the work.\n\n " +
		strings.Repeat("─", 116) + " \n"
}

// The pane's gateway link dropped mid-turn: the screen shows the disconnect panel while the
// root gateway goes on and records the finished answer in the profile ledger. The turn must
// wait for that answer and deliver it, never fail with the disconnect copy (2026-10-30 chat
// turn delivered "runner turn failed: Gateway disconnected..." to Telegram).
func TestHerdrHermesDisconnectRecoversFinishedAnswer(t *testing.T) {
	dir := t.TempDir()
	hermesHome := filepath.Join(dir, "hermes-home")
	t.Setenv("HERMES_HOME", hermesHome)
	ledger := newHermesTestLedger(t, hermesChatLedger(hermesHome))
	ledger.session("local-drop", "", "", time.Now().Add(-time.Hour))
	prompt := "Werkrby chat for telegram/4242.\n\nJoey: is the backup done?"
	script := hermesFakeHerdr(t, dir)
	writeHermesScreen(t, dir, hermesTestTurn(prompt, hermesDisconnectPanel())+hermesTestChrome)
	startHermesSim(t, dir, func(n int, submitted string) {
		if !strings.Contains(submitted, "is the backup done?") {
			return
		}
		// The gateway finishes the turn after the pane client gave up: its rows land later
		// than any settle window, well inside the disconnect wait.
		go func() {
			time.Sleep(500 * time.Millisecond)
			ledger.add("local-drop",
				hermesTestRow{role: "user", content: submitted},
				hermesTestRow{role: "assistant", content: "Yes: the backup finished at 03:12 and verified clean.", finishReason: "stop"},
			)
		}()
	})

	got := runDisconnectTurn(t, dir, script, "chat:telegram:4242", prompt+"\n\nchat:telegram:4242", 5*time.Second)

	if got.err != nil {
		t.Fatalf("Send returned %v, want the gateway's finished answer", got.err)
	}
	finals := got.finals()
	if len(finals) != 1 || finals[0] != "Yes: the backup finished at 03:12 and verified clean." {
		t.Fatalf("EventFinal texts = %q, want only the gateway's recorded answer", finals)
	}
	if sent := hermesPromptsSent(t, dir); sent != 1 {
		t.Fatalf("prompt sent %d times, want 1 (the work must never be resent)", sent)
	}
}

// With no recorded outcome before the disconnect wait runs out the turn fails with one plain
// sentence: the raw disconnect copy is a Hermes interface string, never a reply or an error
// shown to a chat user.
func TestHerdrHermesDisconnectExpiryFailsWithAPlainSentence(t *testing.T) {
	dir := t.TempDir()
	hermesHome := filepath.Join(dir, "hermes-home")
	t.Setenv("HERMES_HOME", hermesHome)
	ledger := newHermesTestLedger(t, hermesChatLedger(hermesHome))
	ledger.session("local-drop", "", "", time.Now().Add(-time.Hour))
	prompt := "Werkrby chat for telegram/4242.\n\nJoey: is the backup done?"
	script := hermesFakeHerdr(t, dir)
	writeHermesScreen(t, dir, hermesTestTurn(prompt, hermesDisconnectPanel())+hermesTestChrome)
	startHermesSim(t, dir, nil)

	got := runDisconnectTurn(t, dir, script, "chat:telegram:4242", prompt+"\n\nchat:telegram:4242", 200*time.Millisecond)

	if got.err == nil {
		t.Fatalf("Send succeeded with no recorded outcome, want a plain-sentence turn error")
	}
	if strings.Contains(got.err.Error(), "Gateway disconnected") {
		t.Fatalf("turn error %q carries the raw Hermes disconnect copy", got.err)
	}
	if !strings.Contains(got.err.Error(), "runner turn failed") || !strings.Contains(got.err.Error(), "no answer was recorded") {
		t.Fatalf("turn error %q is not the plain no-outcome sentence", got.err)
	}
	if finals := got.finals(); len(finals) != 0 {
		t.Fatalf("a turn with no recorded outcome was delivered as a reply: %q", finals)
	}
	if sent := hermesPromptsSent(t, dir); sent != 1 {
		t.Fatalf("prompt sent %d times, want 1 (the work must never be resent)", sent)
	}
}
