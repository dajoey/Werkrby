package harness

import (
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// hermesWarmChatScreen is a warm chat pane right after the new prompt was submitted: the earlier
// turn's reply box, then the new turn's rule and prompt preview with no box yet, then the status
// bar and a composer still showing pasted text.
func hermesWarmChatScreen(newPrompt string) string {
	return hermesTestTurn("Earlier question", hermesTestBox("Older reply: the library fill-out resumes now.")) +
		hermesTestTurn(newPrompt) +
		" ☤ glm-5.3 │ ~220K/1M │ [██░░░░░░░░] ~22% │ ◎ 100.0% │ ◷ 16.6s │ ↑ 64 t/s │ 6.7d │ ⏱ 7s  ─ Werkrby chat\n" +
		hermesTestWideRule + "\n## A custom build never goes on his machines without his own yes\n" + hermesTestWideRule + "\n"
}

func hermesEarlierChatTurn(ledger *hermesTestLedger) {
	ledger.session("local-chat", "", "", time.Now().Add(-time.Hour))
	ledger.add("local-chat",
		hermesTestRow{role: "user", content: "earlier question", at: time.Now().Add(-time.Hour)},
		hermesTestRow{role: "assistant", content: "Older reply: the library fill-out resumes now.", finishReason: "stop", at: time.Now().Add(-time.Hour)},
	)
}

func assertNoScreenInFinals(t *testing.T, got hermesTurnResult) {
	t.Helper()
	for _, f := range got.finals() {
		if strings.Contains(f, "Older reply") || strings.Contains(f, "☤") || strings.Contains(f, "─────") || strings.Contains(f, "custom build") {
			t.Fatalf("a chat reply carried pane text instead of this turn's answer:\n%s", f)
		}
	}
}

// 2026-10-31 13:00 telegram chat: herdr reported the warm chat pane idle at 13:00:40 while the
// 56 KB prompt was still reaching the gateway; the ledger's user row landed at 13:00:44 and the
// answer at 13:03:00. The judge found no user row, fell back to the screen, and the 250-line pane
// scrollback (tool calls, diffs, 429 panels, the chat instructions) went to Joey as the reply.
// A readable ledger decides: wait for the prompt's row, then for the turn's answer.
func TestHerdrHermesPromptLandingAfterHerdrIdleWaitsForTheLedgerAnswer(t *testing.T) {
	dir := t.TempDir()
	hermesHome := filepath.Join(dir, "hermes-home")
	t.Setenv("HERMES_HOME", hermesHome)
	ledger := newHermesTestLedger(t, hermesChatLedger(hermesHome))
	hermesEarlierChatTurn(ledger)
	key := "chat:telegram:4242"
	writeWarmHermesWorker(t, dir, key)
	script := hermesFakeHerdr(t, dir)
	writeHermesScreen(t, dir, hermesWarmChatScreen("You are Werkrby's communication agent."))
	var late sync.WaitGroup
	t.Cleanup(late.Wait)
	startHermesSim(t, dir, func(n int, prompt string) {
		late.Add(1)
		go func() {
			defer late.Done()
			time.Sleep(400 * time.Millisecond)
			ledger.add("local-chat", hermesTestRow{role: "user", content: prompt})
			time.Sleep(300 * time.Millisecond)
			ledger.add("local-chat",
				hermesTestRow{role: "assistant", content: "Checking the task.", toolCalls: `[{"id":"c1"}]`, finishReason: "tool_calls"},
				hermesTestRow{role: "tool", content: "{}"},
			)
			time.Sleep(300 * time.Millisecond)
			ledger.add("local-chat", hermesTestRow{role: "assistant", content: "Seen: the mix on that album is still empty; reopened.", finishReason: "stop"})
		}()
	})

	got := runHermesTurn(t, dir, script, key, "", "You are Werkrby's communication agent.\n\nJoey: the mix is still empty")

	assertNoScreenInFinals(t, got)
	if got.err != nil {
		t.Fatalf("Send returned %v, want the ledger's answer", got.err)
	}
	if finals := got.finals(); len(finals) != 1 || finals[0] != "Seen: the mix on that album is still empty; reopened." {
		t.Fatalf("EventFinal texts = %q, want the ledger's answer", finals)
	}
	if sent := hermesPromptsSent(t, dir); sent != 1 {
		t.Fatalf("prompt sent %d times, want 1", sent)
	}
}

// A readable ledger that never receives the prompt: the screen can only hold earlier turns, so
// nothing on it is this turn's reply. The turn is an error, never the older reply or the pane.
func TestHerdrHermesPromptNeverInLedgerIsAnErrorNotTheScreen(t *testing.T) {
	dir := t.TempDir()
	hermesHome := filepath.Join(dir, "hermes-home")
	t.Setenv("HERMES_HOME", hermesHome)
	ledger := newHermesTestLedger(t, hermesChatLedger(hermesHome))
	hermesEarlierChatTurn(ledger)
	key := "chat:telegram:4242"
	writeWarmHermesWorker(t, dir, key)
	script := hermesFakeHerdr(t, dir)
	// The prompt never drew: the newest box on screen is the earlier turn's reply.
	writeHermesScreen(t, dir, hermesTestTurn("Earlier question", hermesTestBox("Older reply: the library fill-out resumes now."))+hermesTestChrome)
	startHermesSim(t, dir, nil)

	got := runHermesTurn(t, dir, script, key, "", "Werkrby chat for telegram/4242.\n\nJoey: anything new?")

	assertNoScreenInFinals(t, got)
	if finals := got.finals(); len(finals) != 0 {
		t.Fatalf("a turn the ledger never received delivered %q", finals)
	}
	if got.err == nil || !strings.Contains(got.err.Error(), "runner turn failed") {
		t.Fatalf("Send error = %v, want a runner turn failure", got.err)
	}
	if sent := hermesPromptsSent(t, dir); sent != 1 {
		t.Fatalf("prompt sent %d times, want 1 (never re-sent blind)", sent)
	}
}

// With no readable ledger, a screen whose newest turn drew no reply box and no error panel has
// no reply on it. Cleaning strips it to nothing; the raw pane is never sent in its place.
func TestHerdrHermesScreenWithoutReplyIsAnErrorNotTheRawPane(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HERMES_HOME", filepath.Join(dir, "no-such-hermes-home"))
	key := "chat:telegram:4242"
	writeWarmHermesWorker(t, dir, key)
	script := hermesFakeHerdr(t, dir)
	writeHermesScreen(t, dir, hermesWarmChatScreen("Werkrby chat for telegram/4242."))
	startHermesSim(t, dir, nil)

	got := runHermesTurn(t, dir, script, key, "", "Werkrby chat for telegram/4242.\n\nJoey: anything new?")

	assertNoScreenInFinals(t, got)
	if finals := got.finals(); len(finals) != 0 {
		t.Fatalf("a screen with no reply on it was delivered as %q", finals)
	}
	if got.err == nil {
		t.Fatalf("Send returned no error for a turn with no reply")
	}
}
