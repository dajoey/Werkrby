package harness

import (
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/core"
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

// The ledger was readable when the wait began; a read that fails mid-wait (busy timeout, a
// Hermes schema change) is a turn error, never a fall back to the screen's older reply.
func TestHerdrHermesLedgerReadFailingMidWaitIsAnErrorNotTheScreen(t *testing.T) {
	dir := t.TempDir()
	hermesHome := filepath.Join(dir, "hermes-home")
	t.Setenv("HERMES_HOME", hermesHome)
	ledger := newHermesTestLedger(t, hermesChatLedger(hermesHome))
	hermesEarlierChatTurn(ledger)
	key := "chat:telegram:4242"
	writeWarmHermesWorker(t, dir, key)
	script := hermesFakeHerdr(t, dir)
	writeHermesScreen(t, dir, hermesTestTurn("Earlier question", hermesTestBox("Older reply: the library fill-out resumes now."))+hermesTestChrome)
	var late sync.WaitGroup
	t.Cleanup(late.Wait)
	startHermesSim(t, dir, func(n int, prompt string) {
		late.Add(1)
		go func() {
			defer late.Done()
			time.Sleep(300 * time.Millisecond)
			ledger.add("local-chat", hermesTestRow{role: "user", content: prompt})
			time.Sleep(300 * time.Millisecond)
			if _, err := ledger.db.Exec(`ALTER TABLE messages RENAME TO messages_moved`); err != nil {
				t.Error(err)
			}
		}()
	})

	got := runHermesTurn(t, dir, script, key, "", "Werkrby chat for telegram/4242.\n\nJoey: anything new?")

	assertNoScreenInFinals(t, got)
	if finals := got.finals(); len(finals) != 0 {
		t.Fatalf("a turn whose ledger read failed mid-wait delivered %q", finals)
	}
	if got.err == nil || !strings.Contains(got.err.Error(), "runner turn failed") {
		t.Fatalf("Send error = %v, want a runner turn failure", got.err)
	}
}

// A prompt the ledger never recorded while the newest turn on screen ends in Hermes's error
// panel: the panel's text is the turn error, so the quota and transient classifiers see it.
func TestHerdrHermesPromptNeverInLedgerKeepsTheScreenErrorPanel(t *testing.T) {
	dir := t.TempDir()
	hermesHome := filepath.Join(dir, "hermes-home")
	t.Setenv("HERMES_HOME", hermesHome)
	ledger := newHermesTestLedger(t, hermesChatLedger(hermesHome))
	hermesEarlierChatTurn(ledger)
	key := "chat:telegram:4242"
	writeWarmHermesWorker(t, dir, key)
	script := hermesFakeHerdr(t, dir)
	errorPanel := " ─ ☤ Hermes " + strings.Repeat("─", 104) + " \n\n Gateway draining: not_ready\n\n " + strings.Repeat("─", 116) + " \n"
	writeHermesScreen(t, dir, hermesTestTurn("Earlier question", hermesTestBox("Older reply: the library fill-out resumes now."))+
		hermesTestTurn("Werkrby chat for telegram/4242.", errorPanel)+hermesTestChrome)
	startHermesSim(t, dir, nil)

	got := runHermesTurn(t, dir, script, key, "", "Werkrby chat for telegram/4242.\n\nJoey: anything new?")

	assertNoScreenInFinals(t, got)
	if got.err == nil || !strings.Contains(got.err.Error(), "Gateway draining: not_ready") {
		t.Fatalf("Send error = %v, want the screen panel's error text", got.err)
	}
}

// The gateway recorded this turn's user row and then went silent (a gateway restart after the
// prompt landed): no assistant row, no failed_turn row. The judge must end the wait at the
// no-new-rows stall bound instead of holding the pane lock for the whole in-flight budget, and
// it must keep reporting status while it waits, so chat activity and the job lease stay live.
func TestHerdrHermesDeliveredSilentTurnEndsWithinTheStallBound(t *testing.T) {
	dir := t.TempDir()
	hermesHome := filepath.Join(dir, "hermes-home")
	t.Setenv("HERMES_HOME", hermesHome)
	ledger := newHermesTestLedger(t, hermesChatLedger(hermesHome))
	hermesEarlierChatTurn(ledger)
	key := "chat:telegram:4242"
	writeWarmHermesWorker(t, dir, key)
	script := hermesFakeHerdr(t, dir)
	writeHermesScreen(t, dir, hermesWarmChatScreen("Werkrby chat for telegram/4242."))
	startHermesSim(t, dir, func(n int, prompt string) {
		ledger.add("local-chat", hermesTestRow{role: "user", content: prompt})
	})

	start := time.Now()
	got := runHermesTurn(t, dir, script, key, "", "Werkrby chat for telegram/4242.\n\nJoey: anything new?")
	elapsed := time.Since(start)

	assertNoScreenInFinals(t, got)
	if finals := got.finals(); len(finals) != 0 {
		t.Fatalf("a delivered silent turn delivered %q", finals)
	}
	if got.err == nil || !strings.Contains(got.err.Error(), HermesUnresolvedTurn) {
		t.Fatalf("Send error = %v, want %q", got.err, HermesUnresolvedTurn)
	}
	if elapsed >= 4*time.Second {
		t.Fatalf("a silent turn held the turn for %s; want the no-new-rows stall bound, well inside the 5s in-flight budget", elapsed)
	}
	statuses := 0
	for _, ev := range got.events {
		if ev.Kind == core.EventStatus && strings.Contains(ev.Text, "still running in the ledger") {
			statuses++
		}
	}
	if statuses == 0 {
		t.Fatalf("no status events during the in-flight wait: %+v", got.events)
	}
}

// herdr did not launch this worker (a direct pane address): no launch args, so no launch
// profile and no trusted ledger. A readable root ledger must not fail the turn after the
// delivery wait; the screen decides, guarded by this turn's own prompt preview.
func TestHerdrHermesUnknownLaunchUsesTheScreenNotTheRootLedger(t *testing.T) {
	dir := t.TempDir()
	hermesHome := filepath.Join(dir, "hermes-home")
	t.Setenv("HERMES_HOME", hermesHome)
	root := newHermesTestLedger(t, filepath.Join(hermesHome, "state.db"))
	root.session("local-root", "", "", time.Now())
	root.add("local-root",
		hermesTestRow{role: "user", content: "earlier question", at: time.Now().Add(-time.Hour)},
		hermesTestRow{role: "assistant", content: "Older root reply.", finishReason: "stop", at: time.Now().Add(-time.Hour)},
	)
	script := hermesFakeHerdr(t, dir)
	writeHermesScreen(t, dir, hermesTestTurn("Earlier question", hermesTestBox("Older root reply."))+
		hermesTestTurn("Werkrby chat for telegram/4242.", hermesTestBox("Pane reply: the dentist card wakes on January 14."))+hermesTestChrome)
	startHermesSim(t, dir, nil)

	got := runHermesTurnConfigured(t, dir, script, "hermes", "chat:telegram:4242", "w1:p9", "Werkrby chat for telegram/4242.\n\nJoey: and the dentist?")

	assertNoScreenInFinals(t, got)
	if got.err != nil {
		t.Fatalf("Send returned %v, want this turn's pane reply", got.err)
	}
	if finals := got.finals(); len(finals) != 1 || finals[0] != "Pane reply: the dentist card wakes on January 14." {
		t.Fatalf("EventFinal texts = %q, want this turn's pane reply", finals)
	}
}

// herdr went idle before Hermes drew this turn: the newest reply box on screen is the earlier
// turn's. With no readable ledger there is no proof this turn ever ran, and no prompt preview
// above the box, so the box is not this turn's reply: the turn is an error.
func TestHerdrHermesIdleBeforeTheRuleDrawnIsAnErrorNotTheOlderBox(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HERMES_HOME", filepath.Join(dir, "no-such-hermes-home"))
	key := "chat:telegram:4242"
	writeWarmHermesWorker(t, dir, key)
	script := hermesFakeHerdr(t, dir)
	writeHermesScreen(t, dir, hermesTestTurn("Earlier question", hermesTestBox("Older reply: the library fill-out resumes now."))+hermesTestChrome)
	startHermesSim(t, dir, nil)

	got := runHermesTurn(t, dir, script, key, "", "Werkrby chat for telegram/4242.\n\nJoey: anything new?")

	assertNoScreenInFinals(t, got)
	if finals := got.finals(); len(finals) != 0 {
		t.Fatalf("the earlier turn's reply box was delivered as %q", finals)
	}
	if got.err == nil {
		t.Fatalf("Send returned no error for a turn with no reply of its own on screen")
	}
}

// `herdr agent prompt` output is the typing receipt, never the reply: with an empty screen
// read it must not be cleaned into a reply — the turn is an error.
func TestHerdrHermesEmptyScreenNeverDeliversThePromptOutput(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HERMES_HOME", filepath.Join(dir, "no-such-hermes-home"))
	key := "chat:telegram:4242"
	writeWarmHermesWorker(t, dir, key)
	script := hermesFakeHerdr(t, dir)
	// No screen.txt: `agent read` returns nothing, while `agent prompt` prints its receipt.
	startHermesSim(t, dir, nil)

	got := runHermesTurn(t, dir, script, key, "", "Werkrby chat for telegram/4242.\n\nJoey: anything new?")

	assertNoScreenInFinals(t, got)
	if finals := got.finals(); len(finals) != 0 {
		t.Fatalf("the prompt command's output was delivered as %q", finals)
	}
	if got.err == nil {
		t.Fatalf("Send returned no error for an empty screen read")
	}
}

// A false idle after the first assistant row: real turns stall minutes between rows (long tool
// runs, provider retries), so a turn with rows and no ending yet is still alive. The judge
// follows it in the ledger past the settle and grace windows instead of erroring, and the
// ending — here a late answer — is the reply.
func TestHerdrHermesIdleAfterFirstAssistantRowFollowsTheTurnToEnd(t *testing.T) {
	dir := t.TempDir()
	hermesHome := filepath.Join(dir, "hermes-home")
	t.Setenv("HERMES_HOME", hermesHome)
	ledger := newHermesTestLedger(t, hermesWorkerLedger(hermesHome))
	ledger.session("local-a", "", "", time.Now())
	script := hermesFakeHerdr(t, dir)
	writeHermesScreen(t, dir, hermesTestTurn("You are running from the Werkrby orchestrator system.", hermesTestBox("Working on it."))+hermesTestChrome)
	var late sync.WaitGroup
	t.Cleanup(late.Wait)
	startHermesSim(t, dir, func(n int, prompt string) {
		ledger.add("local-a",
			hermesTestRow{role: "user", content: prompt},
			hermesTestRow{role: "assistant", content: "Reading the task.", toolCalls: `[{"id":"c1"}]`, finishReason: "tool_calls"},
			hermesTestRow{role: "tool", content: "{}"},
		)
		late.Add(1)
		go func() {
			defer late.Done()
			// Past the settle and grace windows (0.6s + 0.6s here), with another row in
			// between so the no-new-rows bound sees the turn is alive.
			time.Sleep(1600 * time.Millisecond)
			ledger.add("local-a", hermesTestRow{role: "tool", content: "{}"})
			time.Sleep(500 * time.Millisecond)
			ledger.add("local-a", hermesTestRow{role: "assistant", content: "Task moved to review.", finishReason: "stop"})
		}()
	})

	got := runHermesTurn(t, dir, script, "orchestrator:tasks:task_implementation:tasks-20261031-idle-row-01:1", "", hermesTestTaskPrompt("tasks-20261031-idle-row-01"))

	if got.err != nil {
		t.Fatalf("Send returned %v, want the turn's late answer", got.err)
	}
	if finals := got.finals(); len(finals) != 1 || finals[0] != "Task moved to review." {
		t.Fatalf("EventFinal texts = %q, want the turn's late answer", finals)
	}
}
