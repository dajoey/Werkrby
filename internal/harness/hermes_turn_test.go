package harness

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func readTestHermesTurn(t *testing.T, path, prompt string, since time.Time) hermesLedgerTurn {
	t.Helper()
	turn, err := readHermesTurn(context.Background(), path, prompt, since)
	if err != nil {
		t.Fatalf("readHermesTurn: %v", err)
	}
	return turn
}

func TestReadHermesTurnSeesRowsStillInTheWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	ledger := newHermesTestLedger(t, path)
	since := time.Now()
	ledger.session("local-a", "", "", since)
	ledger.add("local-a",
		hermesTestRow{role: "user", content: "Reply with exactly: ok"},
		hermesTestRow{role: "assistant", content: "ok", finishReason: "stop"},
	)
	if info, err := os.Stat(path + "-wal"); err != nil || info.Size() == 0 {
		t.Fatalf("test ledger rows are not in the WAL (stat %v, %v); the test would not prove the reader sees it", info, err)
	}
	turn := readTestHermesTurn(t, path, "Reply with exactly: ok", since)
	if !turn.Delivered || turn.Final != "ok" || turn.SessionID != "local-a" {
		t.Fatalf("turn = %+v, want delivered with final ok from local-a", turn)
	}
}

func TestReadHermesTurnNeverServesAnEarlierTurn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	ledger := newHermesTestLedger(t, path)
	earlier := time.Now().Add(-time.Minute)
	ledger.session("local-a", "", "", earlier)
	ledger.add("local-a",
		hermesTestRow{role: "user", content: "same prompt", at: earlier},
		hermesTestRow{role: "assistant", content: "earlier answer", finishReason: "stop", at: earlier},
	)
	since := time.Now()
	if turn := readTestHermesTurn(t, path, "same prompt", since); turn.Delivered {
		t.Fatalf("an identical prompt from before this turn proved delivery: %+v", turn)
	}
	ledger.add("local-a", hermesTestRow{role: "user", content: "same prompt"})
	turn := readTestHermesTurn(t, path, "same prompt", since)
	if !turn.Delivered || turn.Final != "" || turn.SawAssistant {
		t.Fatalf("turn = %+v, want delivered with no answer yet (never the earlier turn's)", turn)
	}
}

func TestReadHermesTurnMatchesTheWholePromptNotTheSharedPreamble(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	ledger := newHermesTestLedger(t, path)
	since := time.Now()
	// Two task workers on the same profile: identical preamble and tail, different task.
	ledger.session("local-other", "", "", since)
	ledger.session("local-mine", "", "", since)
	ledger.add("local-other",
		hermesTestRow{role: "user", content: hermesTestTaskPrompt("tasks-20261024-other-01")},
		hermesTestRow{role: "assistant", content: "Other task done.", finishReason: "stop"},
	)
	ledger.add("local-mine",
		// The terminal paste and Hermes's trim reshape whitespace; the match folds it.
		hermesTestRow{role: "user", content: strings.ReplaceAll(hermesTestTaskPrompt("tasks-20261024-mine-01"), "\n\n", "\r\n  \n")},
		hermesTestRow{role: "assistant", content: "My task done.", finishReason: "stop"},
	)
	ledger.add("local-other", hermesTestRow{role: "assistant", content: "Other task, later row.", finishReason: "stop"})

	turn := readTestHermesTurn(t, path, hermesTestTaskPrompt("tasks-20261024-mine-01"), since)
	if turn.SessionID != "local-mine" || turn.Final != "My task done." {
		t.Fatalf("turn = %+v, want local-mine's answer", turn)
	}
	if turn := readTestHermesTurn(t, path, hermesTestTaskPrompt("tasks-20261024-absent-01"), since); turn.Delivered {
		t.Fatalf("a prompt that shares only the preamble proved delivery: %+v", turn)
	}
}

func TestReadHermesTurnFollowsCompressionIntoTheChildSession(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	ledger := newHermesTestLedger(t, path)
	since := time.Now()
	prompt := hermesTestTaskPrompt("tasks-20261024-long-01")
	userAt := time.Now()
	ledger.session("local-parent", "", "compression", since)
	ledger.add("local-parent",
		hermesTestRow{role: "user", content: prompt, at: userAt},
		hermesTestRow{role: "assistant", content: "", toolCalls: `[{"id":"c1"}]`, finishReason: "tool_calls"},
		hermesTestRow{role: "tool", content: "{}"},
	)
	// The child copies the user row with its original timestamp, adds the compaction summary,
	// and carries the turn on.
	ledger.session("local-child", "local-parent", "", time.Now())
	ledger.add("local-child",
		hermesTestRow{role: "user", content: prompt, at: userAt},
		hermesTestRow{role: "assistant", content: "[CONTEXT COMPACTION — REFERENCE ONLY] Earlier turns were compacted.", summary: true},
		hermesTestRow{role: "assistant", content: "Done after compression.", finishReason: "stop"},
	)
	turn := readTestHermesTurn(t, path, prompt, since)
	if turn.SessionID != "local-child" || turn.Final != "Done after compression." {
		t.Fatalf("turn = %+v, want the child's answer", turn)
	}

	// A parent that ended by compression without a copy of the row: follow it to the child.
	path2 := filepath.Join(t.TempDir(), "state.db")
	ledger2 := newHermesTestLedger(t, path2)
	ledger2.session("local-p", "", "compression", since)
	ledger2.add("local-p", hermesTestRow{role: "user", content: prompt})
	ledger2.session("local-c", "local-p", "", time.Now())
	ledger2.add("local-c",
		hermesTestRow{role: "assistant", content: "Answer from an older turn copied over.", finishReason: "stop", at: since.Add(-time.Hour)},
		hermesTestRow{role: "assistant", content: "[CONTEXT COMPACTION — REFERENCE ONLY] summary"},
		hermesTestRow{role: "assistant", content: "Continued answer.", finishReason: "stop"},
	)
	turn = readTestHermesTurn(t, path2, prompt, since)
	if turn.SessionID != "local-c" || turn.Final != "Continued answer." {
		t.Fatalf("turn = %+v, want the child's new answer", turn)
	}
}

func TestReadHermesTurnOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name        string
		rows        []hermesTestRow
		wantFinal   string
		wantFailure []string
		wantSaw     bool
	}{
		{
			name: "gated draft then rewrite",
			rows: []hermesTestRow{
				{role: "assistant", content: "Draft with Routed: x", finishReason: "verify_hook_continue"},
				{role: "assistant", content: "Rewrite.", finishReason: "stop"},
			},
			wantFinal: "Rewrite.", wantSaw: true,
		},
		{
			name:    "gated draft whose rewrite never landed",
			rows:    []hermesTestRow{{role: "assistant", content: "Draft with Routed: x", finishReason: "verify_hook_continue"}},
			wantSaw: true,
		},
		{
			name: "interim text on a tool step is not the answer",
			rows: []hermesTestRow{
				{role: "assistant", content: "Let me read the task first.", toolCalls: `[{"id":"c1"}]`, finishReason: "tool_calls"},
				{role: "tool", content: "{}"},
			},
			wantSaw: true,
		},
		{
			name: "failed turn carries the provider error and code",
			rows: []hermesTestRow{{role: "assistant", content: hermesFailedTurnNotice, displayKind: "failed_turn",
				displayMetadata: `{"error": "HTTP 400: This model always engages in thinking", "error_surface": {"code": "format_error"}}`}},
			wantFailure: []string{"HTTP 400: This model always engages in thinking", "[format_error]"}, wantSaw: true,
		},
		{
			name:        "untyped legacy failed-turn row",
			rows:        []hermesTestRow{{role: "assistant", content: hermesPartialTurnNotice}},
			wantFailure: []string{"This turn did not complete"}, wantSaw: true,
		},
		{
			name: "display-only rows are not replies",
			rows: []hermesTestRow{
				{role: "assistant", content: "The answer.", finishReason: "stop"},
				{role: "assistant", content: "internal", displayKind: "hidden"},
			},
			wantFinal: "The answer.", wantSaw: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.db")
			ledger := newHermesTestLedger(t, path)
			since := time.Now()
			ledger.session("local-a", "", "", since)
			ledger.add("local-a", append([]hermesTestRow{{role: "user", content: "the prompt"}}, tc.rows...)...)
			turn := readTestHermesTurn(t, path, "the prompt", since)
			if !turn.Delivered || turn.Final != tc.wantFinal || turn.SawAssistant != tc.wantSaw {
				t.Fatalf("turn = %+v, want final %q saw %v", turn, tc.wantFinal, tc.wantSaw)
			}
			for _, want := range tc.wantFailure {
				if !strings.Contains(turn.Failure, want) {
					t.Fatalf("failure %q does not carry %q", turn.Failure, want)
				}
			}
			if len(tc.wantFailure) == 0 && turn.Failure != "" {
				t.Fatalf("unexpected failure %q", turn.Failure)
			}
		})
	}
}

func TestReadHermesTurnUnreadableLedgerIsAnError(t *testing.T) {
	if _, err := readHermesTurn(context.Background(), filepath.Join(t.TempDir(), "missing", "state.db"), "p", time.Now()); err == nil {
		t.Fatalf("a missing ledger read without error")
	}
	path := filepath.Join(t.TempDir(), "state.db")
	if err := os.WriteFile(path, []byte("not a database"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readHermesTurn(context.Background(), path, "p", time.Now()); err == nil {
		t.Fatalf("a corrupt ledger read without error")
	}
}

func TestHermesLedgerPath(t *testing.T) {
	root := filepath.Join(t.TempDir(), ".hermes")
	for _, tc := range []struct {
		name string
		home string
		args []string
		want string
		bad  bool
	}{
		{name: "short flag", home: root, args: []string{"-p", "wk-code", "--model", "zai/glm-5.3-flash"}, want: filepath.Join(root, "profiles", "wk-code", "state.db")},
		{name: "long flag", home: root, args: []string{"--model", "zai/glm-5.3", "--profile", "WK-Chat"}, want: filepath.Join(root, "profiles", "wk-chat", "state.db")},
		{name: "equals form", home: root, args: []string{"--profile=wk-notify"}, want: filepath.Join(root, "profiles", "wk-notify", "state.db")},
		{name: "no profile is the root", home: root, args: []string{"--model", "zai/glm-5.3"}, want: filepath.Join(root, "state.db")},
		{name: "default is the root", home: root, args: []string{"-p", "default"}, want: filepath.Join(root, "state.db")},
		{name: "HERMES_HOME names a profile", home: filepath.Join(root, "profiles", "wk-media"), want: filepath.Join(root, "profiles", "wk-media", "state.db")},
		{name: "path escape refused", home: root, args: []string{"-p", "../../etc"}, bad: true},
		{name: "dangling flag refused", home: root, args: []string{"-p"}, bad: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := hermesLedgerPath(tc.home, tc.args)
			if tc.bad {
				if err == nil {
					t.Fatalf("hermesLedgerPath(%q) = %q, want an error", tc.args, got)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("hermesLedgerPath(%q) = %q, %v, want %q", tc.args, got, err, tc.want)
			}
		})
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "active_profile"), []byte("wk-code\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, _ := hermesLedgerPath(root, nil); got != filepath.Join(root, "profiles", "wk-code", "state.db") {
		t.Fatalf("sticky active_profile ignored: %q", got)
	}
}

func TestHermesScreenTurnError(t *testing.T) {
	errorPanel := " ─ ☤ Hermes " + strings.Repeat("─", 104) + " \n\n Rate limited by zai; wait a minute.\n Details: HTTP 429\n\n " + strings.Repeat("─", 116) + " \n"
	for _, tc := range []struct {
		name   string
		screen string
		want   string
		ok     bool
	}{
		{name: "newest turn drew the panel", screen: hermesTestTurn("q1", hermesTestBox("Older reply.")) + hermesTestTurn("q2", errorPanel) + hermesTestChrome,
			want: "Rate limited by zai; wait a minute. Details: HTTP 429", ok: true},
		{name: "cold pane panel", screen: hermesTestTurn("q1", errorPanel) + hermesTestChrome, want: "Rate limited by zai; wait a minute. Details: HTTP 429", ok: true},
		{name: "panel after the turn's interim box", screen: hermesTestTurn("q1", hermesTestBox("Let me look."), "  ┊ 💻 $ df -h\n", errorPanel) + hermesTestChrome,
			want: "Rate limited by zai; wait a minute. Details: HTTP 429", ok: true},
		{name: "an older turn's panel is not this turn's", screen: hermesTestTurn("q1", errorPanel) + hermesTestTurn("q2", hermesTestBox("Fresh reply.")) + hermesTestChrome},
		{name: "reply box", screen: hermesTestTurn("q1", hermesTestBox("Fine.")) + hermesTestChrome},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := hermesScreenTurnError(tc.screen)
			if ok != tc.ok || got != tc.want {
				t.Fatalf("hermesScreenTurnError = %q, %v, want %q, %v", got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestHermesStartupBannerIsNotAReply(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "hermes-startup", "composer-up.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if hermesScreenShowsReply(string(data)) {
		t.Fatalf("the startup banner counted as a Hermes reply (would skip the undelivered-prompt guard)")
	}
	if _, ok := hermesScreenTurnError(string(data)); ok {
		t.Fatalf("the startup banner counted as an error panel")
	}
}

func TestCleanHerdrTerminalOutputHermesDropsPromptPreviewFromBoxlessTurn(t *testing.T) {
	// Hermes failed before it printed the second turn rule, so the preview sits in the newest
	// turn's text; it is the prompt, not the reply.
	screen := hermesTestTurn("q1", hermesTestBox("Older reply.")) + "\n" + hermesTestTurnRule + "\n● And the dentist?\nAgent init failed: gateway unreachable\n" + hermesTestChrome
	got := cleanHerdrTerminalOutput(screen)
	if strings.Contains(got, "And the dentist?") || !strings.Contains(got, "Agent init failed") {
		t.Fatalf("cleanHerdrTerminalOutput = %q, want the turn's own output without the prompt preview", got)
	}
}

// The failed-turn boundary row can land long after the pane went quiet: under the 2026-10-30
// Z.ai quota storm the gateway persisted session local-f2e810d67b…'s row 10.4s after the turn's
// last tool row, past the settle window, so judgeHermesTurn fell through to the generic
// "no final reply" verdict, isQuotaError could not classify it, and three such strikes moved a
// healthy task to failed/. One bounded re-read surfaces the row's real error.
func TestJudgeHermesTurnSurfacesAFailedTurnRowThatLandsPastTheSettle(t *testing.T) {
	dir := t.TempDir()
	hermesHome := filepath.Join(dir, "hermes-home")
	t.Setenv("HERMES_HOME", hermesHome)
	ledger := newHermesTestLedger(t, hermesWorkerLedger(hermesHome))
	ledger.session("local-late", "", "", time.Now())
	prompt := hermesTestTaskPrompt("tasks-20261031-late-boundary-01")
	since := time.Now()
	ledger.add("local-late",
		hermesTestRow{role: "user", content: prompt},
		hermesTestRow{role: "assistant", content: "Let me read the task first.", toolCalls: `[{"id": "call_1", "type": "function", "function": {"name": "terminal", "arguments": "{}"}}]`, finishReason: "tool_calls"},
		hermesTestRow{role: "tool", content: "{}"},
	)
	script := hermesFakeHerdr(t, dir)
	// The screen holds only the turn's interim box: it cannot classify the turn, the ledger must.
	writeHermesScreen(t, dir, hermesTestTurn("You are running from the Werkrby orchestrator system.", hermesTestBox("Let me read the task first."))+hermesTestChrome)
	var late sync.WaitGroup
	t.Cleanup(late.Wait)
	late.Add(1)
	go func() {
		defer late.Done()
		time.Sleep(500 * time.Millisecond) // the settle window (150ms) has closed; the grace has not
		// Real boundary-row bytes: messages row 54170 of session local-f2e810d67b… in
		// ~/.hermes/profiles/wk-code/state.db, persisted 22:34:36.597Z, 10.4s after the
		// turn's last tool row.
		ledger.add("local-late", hermesTestRow{role: "assistant",
			content:         "This turn did not complete. Some actions may already have run; verify their effects before resending.",
			displayKind:     "failed_turn",
			displayMetadata: `{"error": "HTTP 429: Rate limit reached for requests", "error_surface": {"layer": "provider", "code": "rate_limit", "retryable": true, "provider": "zai", "model": "glm-5.3-flash", "provider_label": "Z.AI / GLM"}}`,
		})
	}()

	h, err := NewHerdr(HarnessConfig{Name: "herdr", Command: script, Cwd: dir, Model: "hermes", SessionsFile: filepath.Join(dir, "sessions.json")})
	if err != nil {
		t.Fatal(err)
	}
	h.hermesLedgerSettle = 150 * time.Millisecond
	h.hermesFailureGrace = 1500 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	v := h.judgeHermesTurn(ctx, "w1:p1", []string{"-p", "wk-code"}, prompt, since, nil)

	if !v.delivered || v.final != "" {
		t.Fatalf("verdict = %+v, want delivered with no final", v)
	}
	if v.err == nil {
		t.Fatal("no error: the late failed-turn row was never surfaced")
	}
	for _, want := range []string{"runner turn failed", "HTTP 429: Rate limit reached for requests", "[rate_limit]"} {
		if !strings.Contains(v.err.Error(), want) {
			t.Fatalf("verdict error %q does not carry %q (the orchestrator must see the quota wall)", v.err, want)
		}
	}
}

// Nothing lands inside the grace either: the verdict stays the generic unresolved-turn error,
// which the orchestrator does not count toward the terminal fail-out.
func TestJudgeHermesTurnWithoutAnyEndingRowStaysTheGenericVerdict(t *testing.T) {
	dir := t.TempDir()
	hermesHome := filepath.Join(dir, "hermes-home")
	t.Setenv("HERMES_HOME", hermesHome)
	ledger := newHermesTestLedger(t, hermesWorkerLedger(hermesHome))
	ledger.session("local-silent", "", "", time.Now())
	prompt := hermesTestTaskPrompt("tasks-20261031-silent-01")
	since := time.Now()
	ledger.add("local-silent",
		hermesTestRow{role: "user", content: prompt},
		hermesTestRow{role: "assistant", content: "Let me read the task first.", toolCalls: `[{"id": "call_1", "type": "function", "function": {"name": "terminal", "arguments": "{}"}}]`, finishReason: "tool_calls"},
		hermesTestRow{role: "tool", content: "{}"},
	)
	script := hermesFakeHerdr(t, dir)
	writeHermesScreen(t, dir, hermesTestTurn("You are running from the Werkrby orchestrator system.", hermesTestBox("Let me read the task first."))+hermesTestChrome)

	h, err := NewHerdr(HarnessConfig{Name: "herdr", Command: script, Cwd: dir, Model: "hermes", SessionsFile: filepath.Join(dir, "sessions.json")})
	if err != nil {
		t.Fatal(err)
	}
	h.hermesLedgerSettle = 100 * time.Millisecond
	h.hermesFailureGrace = 200 * time.Millisecond
	// The judge follows the turn's rows now, so the generic verdict arrives when the rows stop
	// (the stall bound) instead of at the grace; the verdict itself is unchanged.
	h.hermesStallBound = 300 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	v := h.judgeHermesTurn(ctx, "w1:p1", []string{"-p", "wk-code"}, prompt, since, nil)

	if !v.delivered || v.final != "" || v.err == nil {
		t.Fatalf("verdict = %+v, want delivered with the generic turn error", v)
	}
	if !strings.Contains(v.err.Error(), "runner turn failed") || !strings.Contains(v.err.Error(), HermesUnresolvedTurn) {
		t.Fatalf("verdict error %q, want the generic unresolved-turn verdict", v.err)
	}
}
