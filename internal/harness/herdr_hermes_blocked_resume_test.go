package harness

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/core"
)

// newBlockedHerdr is runBlockedTurn's harness, kept so a test can send more than one turn to the
// same worker.
func newBlockedHerdr(t *testing.T, dir, script string, bound time.Duration) *Herdr {
	t.Helper()
	h, err := NewHerdr(HarnessConfig{Name: "herdr", Command: script, Cwd: dir, Model: "hermes", SessionsFile: filepath.Join(dir, "sessions.json")})
	if err != nil {
		t.Fatal(err)
	}
	h.SetIdleStability(3, 10*time.Millisecond)
	h.SetComposerWaitTimeout(20 * time.Millisecond)
	h.hermesLedgerSettle = 600 * time.Millisecond
	h.hermesBlockedWait = bound
	h.hermesBlockedPoll = 20 * time.Millisecond
	return h
}

// sendBlockedTurn sends one turn on h; claimed, when not empty, is the claimed document the
// dispatch names in the turn's context, as the orchestrator does.
func sendBlockedTurn(t *testing.T, h *Herdr, key, prompt, claimed string) blockedTurnResult {
	t.Helper()
	var mu sync.Mutex
	var events []core.Event
	emit := func(e core.Event) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if claimed != "" {
		ctx = WithClaimedDocument(ctx, claimed)
	}
	start := time.Now()
	_, _, err := h.SendWithModel(ctx, key, prompt, "hermes", emit)
	elapsed := time.Since(start)
	// Let the deferred tab cleanup of a successful orchestrator turn finish before TempDir removal.
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	return blockedTurnResult{hermesTurnResult{err: err, events: append([]core.Event(nil), events...)}, elapsed}
}

// typedStates is the herdr status each typed prompt went into, in order.
func typedStates(t *testing.T, dir string) []string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "typed-states"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Fields(string(data))
}

// claimedFixture writes a claimed task document under dir and returns its path.
func claimedFixture(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "tasks", "working", "tasks-20261025-blocked-01.md")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("---\nid: tasks-20261025-blocked-01\nstatus: working\n---\n# T\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// A rejected submission whose pane clears into working has resumed the turn it was blocked in.
// Hermes worker profiles queue input typed into a busy pane (busy_input_mode: queue), so a prompt
// typed then runs as a second full turn after that one. The harness follows the in-flight turn
// to its end and types the prompt only into an idle pane, once.
func TestHerdrHermesBlockedClearsIntoWorkingNeverQueuesTheNextPrompt(t *testing.T) {
	for _, tc := range []struct {
		name    string
		claimed bool
	}{
		{name: "claimed document still claimed", claimed: true},
		{name: "no claimed document named", claimed: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			hermesBlockedLedger(t, dir)
			script := blockedFakeHerdr(t, dir, "hermes", []string{"blocked", "blocked", "working", "working", "working", "idle"}, 1)
			writeWarmWorker(t, dir, blockedTestKey, "hermes", "blocked")
			claimed := ""
			if tc.claimed {
				claimed = claimedFixture(t, dir)
			}

			got := sendBlockedTurn(t, newBlockedHerdr(t, dir, script, 10*time.Second), blockedTestKey, hermesTestTaskPrompt("tasks-20261025-blocked-01"), claimed)

			if got.err != nil {
				t.Fatalf("Send returned %v, want the turn to complete", got.err)
			}
			states := typedStates(t, dir)
			if len(states) > 1 {
				t.Fatalf("prompts typed = %d (%q), want at most one", len(states), states)
			}
			for _, state := range states {
				if state != "idle" && state != "done" {
					t.Fatalf("a prompt was typed while herdr reported %q (typed into %q): it queues behind the resumed turn and runs as a second turn", state, states)
				}
			}
			if len(states) != 1 {
				t.Fatalf("prompts typed = %d, want the prompt typed once the resumed turn ended and the document was still claimed", len(states))
			}
			if finals := got.finals(); len(finals) != 1 || finals[0] != "Task moved to review." {
				t.Fatalf("EventFinal texts = %q, want this prompt's ledger answer", finals)
			}
		})
	}
}

// A rejected submission whose pane clears to idle after the worker's earlier turn moved the
// claimed document out of its folder is not typed: that turn settled the document, and typing
// the prompt would start a second turn on a document the transition already owns.
func TestHerdrHermesBlockedClearedAfterTheDocumentMovedIsNotResubmitted(t *testing.T) {
	dir := t.TempDir()
	hermesBlockedLedger(t, dir)
	script := blockedFakeHerdr(t, dir, "hermes", []string{"blocked", "blocked", "idle"}, 1)
	writeWarmWorker(t, dir, blockedTestKey, "hermes", "blocked")
	claimed := claimedFixture(t, dir)
	if err := os.Remove(claimed); err != nil {
		t.Fatal(err)
	}

	got := sendBlockedTurn(t, newBlockedHerdr(t, dir, script, 10*time.Second), blockedTestKey, hermesTestTaskPrompt("tasks-20261025-blocked-01"), claimed)

	if got.err != nil {
		t.Fatalf("Send returned %v, want no error: the document's transition owns the outcome", got.err)
	}
	if calls, typed := fakeCount(t, dir, "prompt-calls"), hermesPromptsSent(t, dir); calls != 1 || typed != 0 {
		t.Fatalf("prompt submissions = %d, typed = %d; want the one rejected submission and nothing typed", calls, typed)
	}
	finals := got.finals()
	if len(finals) != 1 || strings.Contains(finals[0], "Screen text, not the ledger.") || !strings.Contains(finals[0], "did not send this prompt") {
		t.Fatalf("EventFinal texts = %q, want one note that the prompt was not sent, never the screen's old reply", finals)
	}
}

// The recovery path: the worker's first dispatch typed its prompt and the pane blocked past the
// bound (hermes_blocked). The recovery dispatch is rejected while the pane is still blocked, the
// pane clears into working (the first turn resumed), and that turn moves the task before it ends.
// The recovery prompt is never typed, and the dispatch reports the resumed turn as the ledger
// records it.
func TestHerdrHermesRecoveryAfterTheResumedTurnMovedTheTaskIsNotResubmitted(t *testing.T) {
	dir := t.TempDir()
	hermesHome := filepath.Join(dir, "hermes-home")
	t.Setenv("HERMES_HOME", hermesHome)
	ledger := newHermesTestLedger(t, hermesWorkerLedger(hermesHome))
	ledger.session("local-blocked", "", "", time.Now().Add(-time.Hour))
	writeHermesScreen(t, dir, hermesTestTurn("You are running from the Werkrby orchestrator system.", hermesTestBox("Screen text, not the ledger."))+hermesTestChrome)
	// The first prompt reaches Hermes and its turn starts; its answer lands only when it resumes.
	startHermesSim(t, dir, func(n int, prompt string) {
		ledger.add("local-blocked", hermesTestRow{role: "user", content: prompt})
	})
	script := blockedFakeHerdr(t, dir, "hermes", []string{"blocked"}, 0)
	writeWarmWorker(t, dir, blockedTestKey, "hermes", "idle")
	claimed := claimedFixture(t, dir)
	h := newBlockedHerdr(t, dir, script, 300*time.Millisecond)

	first := sendBlockedTurn(t, h, blockedTestKey, hermesTestTaskPrompt("tasks-20261025-blocked-01"), claimed)
	if first.err == nil || !strings.Contains(first.err.Error(), HermesBlockedCode) {
		t.Fatalf("first dispatch returned %v, want %s", first.err, HermesBlockedCode)
	}

	// Recovery: the pane is still blocked, so herdr rejects the next submission; it then clears
	// into the resumed first turn, which moves the task to review/ and answers before it ends.
	for name, body := range map[string]string{"statuses": "blocked\nworking\nworking\nidle\n", "reject": "2"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Remove(filepath.Join(dir, "gets")); err != nil {
		t.Fatal(err)
	}
	ledger.add("local-blocked", hermesTestRow{role: "assistant", content: "Moved the task to review after the approval.", finishReason: "stop"})
	if err := os.Remove(claimed); err != nil {
		t.Fatal(err)
	}
	recoveryPrompt := "You are Werkrby's recovery agent. Inspect `" + claimed + "` and record the outcome."

	got := sendBlockedTurn(t, h, blockedTestKey, recoveryPrompt, claimed)

	if got.err != nil {
		t.Fatalf("recovery dispatch returned %v, want no error", got.err)
	}
	if calls, typed := fakeCount(t, dir, "prompt-calls"), hermesPromptsSent(t, dir); calls != 2 || typed != 1 {
		t.Fatalf("prompt submissions = %d, typed = %d; want the first prompt typed, the recovery rejected once and never typed", calls, typed)
	}
	if finals := got.finals(); len(finals) != 1 || finals[0] != "Moved the task to review after the approval." {
		t.Fatalf("EventFinal texts = %q, want the resumed turn's answer from the ledger", finals)
	}
}
