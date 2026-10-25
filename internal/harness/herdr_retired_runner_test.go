package harness

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// No Werkrby code path may launch a retired runner: a bare retired runner name is refused
// at herdrLaunch, the one funnel every pane launch goes through. A roster staff pin
// ("@name") is an explicit staffing edit and is not refused here.
func TestHerdrLaunchRefusesRetiredRunnerNames(t *testing.T) {
	dir := t.TempDir()
	writeHermesDefaultsRoster(t, dir)
	for _, model := range []string{"agy", "Kimi", " opencode ", "pi", "claude"} {
		if _, _, err := herdrLaunch(dir, "", model); err == nil {
			t.Errorf("herdrLaunch(%q) launched, want a refusal: %q is a retired runner", model, strings.TrimSpace(strings.ToLower(model)))
		}
	}
}

// A hermes launch that names no profile runs the session key's worker profile: wk-chat for
// chat turns, wk-notify for notification and heartbeat turns, the code worker otherwise.
func TestHerdrBareLaunchProfileFollowsTheSessionRole(t *testing.T) {
	dir := t.TempDir()
	for _, tc := range []struct{ key, want string }{
		{"chat:tui:local-71f92e03b302", "wk-chat"},
		{"chat:telegram:98451", "wk-chat"},
		{"orchestrator:notification:tasks-x:1", "wk-notify"},
		{"heartbeat", "wk-notify"},
		{"orchestrator:semantic-heartbeat", "wk-notify"},
		{"orchestrator:tasks:task_implementation:tasks-x:1", "wk-code"},
		{"", "wk-code"},
	} {
		kind, args, err := herdrLaunch(dir, tc.key, "")
		if err != nil || kind != "hermes" || !reflect.DeepEqual(args, []string{"-p", tc.want}) {
			t.Errorf("herdrLaunch(key %q) = %q %q, %v, want hermes [-p %s]", tc.key, kind, args, err, tc.want)
		}
	}
}

// A chat turn that falls back to bare hermes (no roster, empty configured model) runs the
// chat worker profile and is judged from that profile's ledger, not the code worker's.
func TestHerdrBareChatFallbackRunsTheChatProfile(t *testing.T) {
	dir := t.TempDir()
	hermesHome := filepath.Join(dir, "hermes-home")
	t.Setenv("HERMES_HOME", hermesHome)
	root := newHermesTestLedger(t, filepath.Join(hermesHome, "state.db"))
	root.session("local-root", "", "", time.Now())
	chat := newHermesTestLedger(t, filepath.Join(hermesHome, "profiles", "wk-chat", "state.db"))
	chat.session("local-chat", "", "", time.Now())
	code := newHermesTestLedger(t, hermesWorkerLedger(hermesHome))
	code.session("local-code", "", "", time.Now())
	script := hermesFakeHerdr(t, dir)
	writeHermesScreen(t, dir, hermesTestTurn("You are running from the Werkrby orchestrator system.", hermesTestBox("Screen text, not the ledger."))+hermesTestChrome)
	startHermesSim(t, dir, func(n int, prompt string) {
		chat.add("local-chat",
			hermesTestRow{role: "user", content: prompt},
			hermesTestRow{role: "assistant", content: "From the wk-chat profile.", finishReason: "stop"},
		)
		code.add("local-code",
			hermesTestRow{role: "user", content: prompt},
			hermesTestRow{role: "assistant", content: "From the wk-code profile.", finishReason: "stop"},
		)
	})

	got := runHermesTurnConfigured(t, dir, script, "", "chat:tui:local-9f2c1", "", hermesTestTaskPrompt("tasks-20261024-bare-chat-01"))

	starts := hermesStarts(t, dir)
	if len(starts) != 1 || starts[0].kind != "hermes" || strings.Join(starts[0].args, " ") != "-p wk-chat" {
		t.Fatalf("agent starts = %q, want one hermes start with args \"-p wk-chat\"", starts)
	}
	if got.err != nil {
		t.Fatalf("Send returned %v, want the wk-chat ledger's answer", got.err)
	}
	if finals := got.finals(); len(finals) != 1 || finals[0] != "From the wk-chat profile." {
		t.Fatalf("EventFinal texts = %q, want the wk-chat ledger's answer", finals)
	}
}

// herdrLaunch is an allowlist: a bare model string other than hermes is refused, whatever runner
// herdr itself supports (codex, cursor and others are installed on the host). Roster staff stay
// the one way to name another runner.
func TestHerdrLaunchRefusesEveryBareKindButHermes(t *testing.T) {
	dir := t.TempDir()
	writeHermesDefaultsRoster(t, dir)
	for _, model := range []string{"codex", "cursor", " Cursor ", "gemini"} {
		if kind, args, err := herdrLaunch(dir, "orchestrator:tasks:task_implementation:tasks-x:1", model); err == nil {
			t.Errorf("herdrLaunch(%q) = kind %q args %q, want a refusal: only hermes or roster staff may launch", model, kind, args)
		}
	}
	for _, model := range []string{"hermes", "Hermes", "", "default"} {
		if kind, _, err := herdrLaunch(dir, "orchestrator:tasks:task_implementation:tasks-x:1", model); err != nil || kind != "hermes" {
			t.Errorf("herdrLaunch(%q) = kind %q, %v; want hermes", model, kind, err)
		}
	}
}

// End to end: a turn whose model is a bare codex never starts a codex pane.
func TestHerdrCodexModelNeverStartsAPane(t *testing.T) {
	dir := t.TempDir()
	writeHermesDefaultsRoster(t, dir)
	script := hermesFakeHerdr(t, dir)
	writeHermesScreen(t, dir, "codex screen\n")
	startHermesSim(t, dir, nil)

	got := runHermesTurnConfigured(t, dir, script, "hermes", "orchestrator:tasks:task_implementation:tasks-x:1", "codex", hermesTestTaskPrompt("tasks-x"))

	if starts := hermesStarts(t, dir); len(starts) != 0 {
		t.Fatalf("agent starts = %q, want none for a bare codex model", starts)
	}
	if got.err == nil {
		t.Fatal("Send returned no error for a bare codex model, want the launch refusal")
	}
}
