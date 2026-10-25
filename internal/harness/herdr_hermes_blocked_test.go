package harness

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/core"
)

// blockedFakeHerdr writes a herdr stand-in for a warm worker whose pane can block on a prompt:
// `agent get` reports dir/kind and plays dir/statuses one line per call (the last line repeats),
// `agent prompt` rejects the first dir/reject submissions with herdr's agent_blocked error before
// any input is sent (as herdr does for an already blocked agent) and otherwise records the prompt
// in dir/prompts/<n> and returns once the simulator acks it, and `agent send-keys` is counted in
// dir/keys. Each typed prompt appends the status the last `agent get` reported to
// dir/typed-states, so a test sees whether a prompt went into a pane herdr called working.
func blockedFakeHerdr(t *testing.T, dir, kind string, statuses []string, reject int) string {
	t.Helper()
	for _, sub := range []string{"prompts", "acks"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for name, body := range map[string]string{
		"kind":     kind,
		"statuses": strings.Join(statuses, "\n") + "\n",
		"reject":   strconv.Itoa(reject),
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	script := filepath.Join(dir, "fake-herdr")
	content := fmt.Sprintf(`#!/usr/bin/env bash
dir=%q
cmd="$1"
sub="$2"
shift 2
bump() { local n=$(( $(cat "$dir/$1" 2>/dev/null || echo 0) + 1 )); echo "$n" > "$dir/$1"; echo "$n"; }
case "$cmd" in
workspace)
	echo '{"result":{"workspaces":[{"workspace_id":"w1","label":"[sj-worker] test"}]}}'
	;;
agent)
	case "$sub" in
	list)
		cat "$dir/agents.json"
		;;
	get)
		n=$(bump gets)
		total=$(wc -l < "$dir/statuses")
		[ "$n" -gt "$total" ] && n=$total
		st=$(sed -n "${n}p" "$dir/statuses")
		echo "$st" > "$dir/last-status"
		printf '{"result":{"agent":{"name":"%%s","agent":"%%s","agent_status":"%%s","pane_id":"w1:p9","agent_session":{"value":""}}}}\n' "$1" "$(cat "$dir/kind")" "$st"
		;;
	prompt)
		c=$(bump prompt-calls)
		if [ "$c" -le "$(cat "$dir/reject")" ]; then
			printf '{"error":{"code":"agent_blocked","message":"agent %%s is blocked and requires interactive input"},"id":"cli:agent:prompt"}\n' "$1"
			exit 1
		fi
		n=$(( $(ls "$dir/prompts" | wc -l) + 1 ))
		cat "$dir/last-status" >> "$dir/typed-states" 2>/dev/null || echo unread >> "$dir/typed-states"
		printf '%%s' "$2" > "$dir/prompts/.tmp-$n" && mv "$dir/prompts/.tmp-$n" "$dir/prompts/$n"
		for i in $(seq 1 1000); do [ -f "$dir/acks/$n" ] && break; sleep 0.01; done
		echo '{"result":{}}'
		;;
	send-keys)
		bump keys > /dev/null
		echo '{"result":{}}'
		;;
	read)
		cat "$dir/screen.txt" 2>/dev/null
		;;
	*)
		echo '{"result":{}}'
		;;
	esac
	;;
tab)
	case "$sub" in
	get)
		echo '{"result":{"tab":{"focused":false}}}'
		;;
	*)
		echo '{"result":{}}'
		;;
	esac
	;;
pane)
	echo '{"result":{}}'
	;;
esac
`, dir)
	if err := os.WriteFile(script, []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
	return script
}

// writeWarmWorker registers an existing worker of kind for key with the given herdr status, so
// the turn reuses it instead of spawning a pane.
func writeWarmWorker(t *testing.T, dir, key, kind, status string) {
	t.Helper()
	name, _ := workerNameAndLabel(key, kind)
	agents := fmt.Sprintf(`{"result":{"agents":[{"name":%q,"agent":%q,"agent_status":%q,"pane_id":"w1:p9","tab_id":"tab-9","workspace_id":"w1","agent_session":{"value":""}}]}}`, name, kind, status)
	if err := os.WriteFile(filepath.Join(dir, "agents.json"), []byte(agents), 0o644); err != nil {
		t.Fatal(err)
	}
}

func fakeCount(t *testing.T, dir, name string) int {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, name))
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	n, _ := strconv.Atoi(strings.TrimSpace(string(data)))
	return n
}

type blockedTurnResult struct {
	hermesTurnResult
	elapsed time.Duration
}

// runBlockedTurn sends one turn with a short blocked-pane bound and poll, so a test sees the
// wait and its bound without waiting minutes.
func runBlockedTurn(t *testing.T, dir, script, key, model string, bound time.Duration) blockedTurnResult {
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
	var mu sync.Mutex
	var events []core.Event
	emit := func(e core.Event) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	_, _, err = h.SendWithModel(ctx, key, hermesTestTaskPrompt("tasks-20261025-blocked-01"), model, emit)
	elapsed := time.Since(start)
	// Let the deferred tab cleanup of a successful orchestrator turn finish before TempDir removal.
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	return blockedTurnResult{hermesTurnResult{err: err, events: append([]core.Event(nil), events...)}, elapsed}
}

const blockedTestKey = "orchestrator:tasks:task_implementation:tasks-20261025-blocked-01:1"

// hermesBlockedLedger is a wk-code ledger whose simulator answers every delivered prompt.
func hermesBlockedLedger(t *testing.T, dir string) {
	t.Helper()
	hermesHome := filepath.Join(dir, "hermes-home")
	t.Setenv("HERMES_HOME", hermesHome)
	ledger := newHermesTestLedger(t, hermesWorkerLedger(hermesHome))
	ledger.session("local-blocked", "", "", time.Now().Add(-time.Hour))
	writeHermesScreen(t, dir, hermesTestTurn("You are running from the Werkrby orchestrator system.", hermesTestBox("Screen text, not the ledger."))+hermesTestChrome)
	startHermesSim(t, dir, func(n int, prompt string) {
		ledger.add("local-blocked",
			hermesTestRow{role: "user", content: prompt},
			hermesTestRow{role: "assistant", content: "Task moved to review.", finishReason: "stop"},
		)
	})
}

// A Hermes worker pane that blocks on a prompt mid-turn (an approval or clarify modal the prompt
// watcher clears) is waited on: herdr reports blocked for a few polls, then idle, and the turn
// completes with the ledger's answer and no error.
func TestHerdrHermesBlockedMidTurnIsWaitedOn(t *testing.T) {
	dir := t.TempDir()
	hermesBlockedLedger(t, dir)
	script := blockedFakeHerdr(t, dir, "hermes", []string{"blocked", "blocked", "blocked", "blocked", "idle"}, 0)
	writeWarmWorker(t, dir, blockedTestKey, "hermes", "idle")

	got := runBlockedTurn(t, dir, script, blockedTestKey, "hermes", 10*time.Second)

	if got.err != nil {
		t.Fatalf("Send returned %v, want the turn to complete once the pane cleared", got.err)
	}
	if finals := got.finals(); len(finals) != 1 || finals[0] != "Task moved to review." {
		t.Fatalf("EventFinal texts = %q, want the ledger's answer", finals)
	}
	if n := hermesPromptsSent(t, dir); n != 1 {
		t.Fatalf("prompts typed = %d, want 1: a delivered prompt is never re-sent", n)
	}
}

// A submission to a Hermes pane that is already blocked is rejected by herdr before any input is
// sent. The turn waits for the pane to clear and then sends the prompt once.
func TestHerdrHermesBlockedBeforeSendWaitsAndSendsOnce(t *testing.T) {
	dir := t.TempDir()
	hermesBlockedLedger(t, dir)
	script := blockedFakeHerdr(t, dir, "hermes", []string{"blocked", "blocked", "blocked", "idle"}, 1)
	writeWarmWorker(t, dir, blockedTestKey, "hermes", "blocked")

	got := runBlockedTurn(t, dir, script, blockedTestKey, "hermes", 10*time.Second)

	if got.err != nil {
		t.Fatalf("Send returned %v, want the turn to complete once the pane cleared", got.err)
	}
	if finals := got.finals(); len(finals) != 1 || finals[0] != "Task moved to review." {
		t.Fatalf("EventFinal texts = %q, want the ledger's answer", finals)
	}
	if calls, typed := fakeCount(t, dir, "prompt-calls"), hermesPromptsSent(t, dir); calls != 2 || typed != 1 {
		t.Fatalf("prompt submissions = %d, typed = %d; want the rejected submission plus one delivered prompt", calls, typed)
	}
}

// A Hermes pane still blocked when the bound passes ends the turn with one distinct
// hermes_blocked error, after waiting the bound, and the prompt is never typed twice.
func TestHerdrHermesBlockedPastTheBoundIsOneDistinctError(t *testing.T) {
	for _, tc := range []struct {
		name     string
		statuses []string
		reject   int
		typed    int
		// oneSubmission: a pane that never clears is never submitted to again.
		oneSubmission bool
	}{
		{name: "mid-turn", statuses: []string{"blocked"}, reject: 0, typed: 1},
		{name: "before send", statuses: []string{"blocked"}, reject: 1000, typed: 0, oneSubmission: true},
		// herdr reads the pane as clear but rejects every submission: the bound still holds.
		{name: "pane flaps", statuses: []string{"idle"}, reject: 1000, typed: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			hermesBlockedLedger(t, dir)
			script := blockedFakeHerdr(t, dir, "hermes", tc.statuses, tc.reject)
			writeWarmWorker(t, dir, blockedTestKey, "hermes", "idle")
			bound := 300 * time.Millisecond

			got := runBlockedTurn(t, dir, script, blockedTestKey, "hermes", bound)

			if got.err == nil || !strings.Contains(got.err.Error(), HermesBlockedCode) {
				t.Fatalf("Send returned %v, want an error carrying %q", got.err, HermesBlockedCode)
			}
			if strings.Count(got.err.Error(), HermesBlockedCode) != 1 {
				t.Fatalf("error %q names %s more than once", got.err, HermesBlockedCode)
			}
			if got.elapsed < bound {
				t.Fatalf("turn ended after %s, want it to wait the %s bound first", got.elapsed, bound)
			}
			if len(got.finals()) != 0 {
				t.Fatalf("EventFinal texts = %q, want none", got.finals())
			}
			if typed := hermesPromptsSent(t, dir); typed != tc.typed {
				t.Fatalf("prompts typed = %d, want %d", typed, tc.typed)
			}
			if calls := fakeCount(t, dir, "prompt-calls"); tc.oneSubmission && calls != 1 {
				t.Fatalf("prompt submissions = %d, want 1: a pane that never clears is never submitted to again", calls)
			}
		})
	}
}

// Other runners keep their behaviour: a blocked agy pane is an immediate agent_blocked error, with
// no wait and no hermes_blocked code.
func TestHerdrNonHermesBlockedStillErrorsAtOnce(t *testing.T) {
	dir := t.TempDir()
	writeHermesDefaultsRoster(t, dir)
	script := blockedFakeHerdr(t, dir, "agy", []string{"blocked"}, 0)
	writeWarmWorker(t, dir, blockedTestKey, "agy", "idle")
	startHermesSim(t, dir, nil)

	got := runBlockedTurn(t, dir, script, blockedTestKey, "@agyseat", time.Minute)

	if got.err == nil || !strings.Contains(got.err.Error(), "blocked") || strings.Contains(got.err.Error(), HermesBlockedCode) {
		t.Fatalf("Send returned %v, want the ordinary herdr blocked error", got.err)
	}
	if got.elapsed > 10*time.Second {
		t.Fatalf("turn ended after %s, want an immediate error", got.elapsed)
	}
}
