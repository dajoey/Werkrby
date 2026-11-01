package harness

import (
	"context"
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/roster"
)

// headlessFixture is a workspace whose roster has one headless seat, and a Herdr whose own command
// does not exist: a turn that reaches a pane path fails instead of passing by accident.
func headlessFixture(t *testing.T, fakeClaude string) (*Herdr, string, string) {
	t.Helper()
	root := t.TempDir()
	state := filepath.Join(root, roster.StateDirName)
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}
	settings := filepath.Join(root, "credits.json")
	if err := os.WriteFile(settings, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	body := "staff:\n  credits:\n    runner: claude\n    headless: true\n    args: [\"--model\", \"sonnet\", \"--settings\", \"" + settings + "\"]\n  pane: {runner: claude}\n"
	if err := os.WriteFile(roster.Path(state), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(root, "fake-claude")
	if err := os.WriteFile(script, []byte("#!/bin/sh\n"+fakeClaude), 0o700); err != nil {
		t.Fatal(err)
	}
	h, err := NewHerdr(HarnessConfig{Command: filepath.Join(root, "no-such-herdr"), Cwd: root, Sandbox: "danger-full-access", ApprovalPolicy: "never"})
	if err != nil {
		t.Fatal(err)
	}
	h.headlessCommand = script
	return h, root, filepath.Join(root, "argv")
}

type eventLog struct {
	mu     sync.Mutex
	events []core.Event
}

func (l *eventLog) emit(e core.Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
}

func (l *eventLog) done() []core.Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []core.Event
	for _, e := range l.events {
		if e.Done {
			out = append(out, e)
		}
	}
	return out
}

const headlessKey = "orchestrator:tasks:task_implementation:t1:1"

func TestHerdrHeadlessStaffRunsClaudePrint(t *testing.T) {
	h, root, argvFile := headlessFixture(t, `printf '%s\n' "$@" > "$(dirname "$0")/argv"
cat > "$(dirname "$0")/stdin"
pwd > "$(dirname "$0")/cwd"
echo '{"type":"system","subtype":"init","session_id":"sess-123","apiKeySource":"apiKeyHelper"}'
echo 'not json'
echo '{"type":"assistant","session_id":"sess-123","message":{"content":[]}}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"DONE","session_id":"sess-123"}'
`)
	var log eventLog
	thread, steered, err := h.SendWithModel(context.Background(), headlessKey, "do the task", "@credits", log.emit)
	if err != nil || steered || thread != "sess-123" {
		t.Fatalf("SendWithModel = %q, %v, %v; want sess-123, false, nil", thread, steered, err)
	}
	argv, _ := os.ReadFile(argvFile)
	got := strings.Split(strings.TrimSpace(string(argv)), "\n")
	want := append([]string{"-p", "--output-format", "stream-json", "--verbose"}, claudePermissionArgs(h.config)...)
	want = append(want, "--model", "sonnet", "--settings", filepath.Join(root, "credits.json"))
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("argv = %q, want %q", got, want)
	}
	if stdin, _ := os.ReadFile(filepath.Join(root, "stdin")); string(stdin) != "do the task" {
		t.Fatalf("stdin = %q, want the prompt", stdin)
	}
	if cwd, _ := os.ReadFile(filepath.Join(root, "cwd")); strings.TrimSpace(string(cwd)) != root {
		t.Fatalf("cwd = %q, want %q", cwd, root)
	}
	done := log.done()
	if len(done) != 1 || done[0].Kind != core.EventFinal || done[0].FinalText == nil || *done[0].FinalText != "DONE" || done[0].ThreadID != "sess-123" {
		t.Fatalf("done events = %+v, want one final DONE on sess-123", done)
	}
	if h.ThreadID(headlessKey) != "sess-123" || h.IsActive(headlessKey) {
		t.Fatalf("session %q active %v; want sess-123 remembered and the turn cleared", h.ThreadID(headlessKey), h.IsActive(headlessKey))
	}
}

func TestHerdrHeadlessErrorResultFailsTheTurn(t *testing.T) {
	h, _, _ := headlessFixture(t, `cat >/dev/null
echo '{"type":"system","subtype":"init","session_id":"sess-9"}'
echo '{"type":"result","subtype":"success","is_error":true,"result":"Credit balance is too low","session_id":"sess-9"}'
exit 1
`)
	var log eventLog
	thread, _, err := h.SendWithModel(context.Background(), headlessKey, "x", "@credits", log.emit)
	if err == nil || !strings.Contains(err.Error(), "runner turn failed: Credit balance is too low") || thread != "sess-9" {
		t.Fatalf("SendWithModel = %q, %v; want the provider text as a runner turn failure", thread, err)
	}
	done := log.done()
	if len(done) != 1 || done[0].Kind != core.EventError || done[0].Text != "Credit balance is too low" {
		t.Fatalf("done events = %+v, want one error event", done)
	}
}

func TestHerdrHeadlessCrashWithoutResultReportsStderr(t *testing.T) {
	h, _, _ := headlessFixture(t, `cat >/dev/null
echo 'apiKeyHelper failed: boom' >&2
exit 2
`)
	_, _, err := h.SendWithModel(context.Background(), headlessKey, "x", "@credits", nil)
	if err == nil || !strings.Contains(err.Error(), "runner turn failed: apiKeyHelper failed: boom") {
		t.Fatalf("err = %v, want the stderr tail", err)
	}
}

func TestHerdrHeadlessInterruptStopsTheProcess(t *testing.T) {
	h, _, _ := headlessFixture(t, `cat >/dev/null
echo '{"type":"system","subtype":"init","session_id":"sess-long"}'
exec sleep 30
`)
	errc := make(chan error, 1)
	go func() {
		_, _, err := h.SendWithModel(context.Background(), headlessKey, "x", "@credits", nil)
		errc <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for !h.IsActive(headlessKey) {
		if time.Now().After(deadline) {
			t.Fatal("headless turn never became active")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := h.PromptLive(context.Background(), headlessKey, "steer"); err == nil {
		t.Fatal("PromptLive on a headless turn should refuse")
	}
	if ok, err := h.Interrupt(context.Background(), headlessKey); !ok || err != nil {
		t.Fatalf("Interrupt = %v, %v", ok, err)
	}
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("interrupted headless turn did not return")
	}
}

func TestHerdrHeadlessInterruptKillsChildrenHoldingStdout(t *testing.T) {
	h, _, _ := headlessFixture(t, `cat >/dev/null
echo '{"type":"system","subtype":"init","session_id":"sess-kids"}'
sleep 30 &
exec sleep 30
`)
	errc := make(chan error, 1)
	go func() {
		_, _, err := h.SendWithModel(context.Background(), headlessKey, "x", "@credits", nil)
		errc <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for !h.IsActive(headlessKey) {
		if time.Now().After(deadline) {
			t.Fatal("headless turn never became active")
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	if ok, _ := h.Interrupt(context.Background(), headlessKey); !ok {
		t.Fatal("Interrupt found no turn")
	}
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a child holding stdout kept the interrupted turn alive")
	}
}

func TestHerdrPaneStaffDoesNotRunHeadless(t *testing.T) {
	h, root, _ := headlessFixture(t, `touch "$(dirname "$0")/ran"; exit 0
`)
	_, _, err := h.SendWithModel(context.Background(), headlessKey, "x", "@pane", nil)
	if err == nil {
		t.Fatal("a pane staff reached no herdr binary and should fail")
	}
	if _, statErr := os.Stat(filepath.Join(root, "ran")); statErr == nil {
		t.Fatal("a staff without headless ran the headless command")
	}
}

func TestHerdrHeadlessSuccessThenNonZeroExitSucceeds(t *testing.T) {
	h, _, _ := headlessFixture(t, `cat >/dev/null
echo '{"type":"result","subtype":"success","is_error":false,"result":"DONE","session_id":"s1"}'
exit 3
`)
	if _, _, err := h.SendWithModel(context.Background(), headlessKey, "x", "@credits", nil); err != nil {
		t.Fatalf("a success result must stand over the exit status: %v", err)
	}
}

func TestHerdrHeadlessNonErrorResultTextNeverBecomesTheFailure(t *testing.T) {
	h, _, _ := headlessFixture(t, `cat >/dev/null
echo '{"type":"result","subtype":"error_during_execution","is_error":false,"result":"Fixed the 429 rate limit handling","session_id":"s2"}'
exit 1
`)
	_, _, err := h.SendWithModel(context.Background(), headlessKey, "x", "@credits", nil)
	if err == nil || strings.Contains(err.Error(), "429") || !strings.Contains(err.Error(), "error_during_execution") {
		t.Fatalf("err = %v, want a failure naming the subtype and not quoting the reply", err)
	}
}

func TestHerdrHeadlessProcessLingeringAfterResultIsCut(t *testing.T) {
	old := headlessResultGrace
	headlessResultGrace = 200 * time.Millisecond
	defer func() { headlessResultGrace = old }()
	h, _, _ := headlessFixture(t, `cat >/dev/null
echo '{"type":"result","subtype":"success","is_error":false,"result":"DONE","session_id":"s3"}'
exec sleep 30
`)
	start := time.Now()
	_, _, err := h.SendWithModel(context.Background(), headlessKey, "x", "@credits", nil)
	if err != nil || time.Since(start) > 10*time.Second {
		t.Fatalf("err = %v after %s; want the result used once the grace ran out", err, time.Since(start))
	}
}

func TestHerdrHeadlessFollowUpResumesItsSession(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	h, root, argvFile := headlessFixture(t, `printf '%s\n' "$@" > "$(dirname "$0")/argv"
cat >/dev/null
echo '{"type":"system","subtype":"init","session_id":"sess-A"}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"DONE","session_id":"sess-A"}'
`)
	if _, _, err := h.SendWithModel(context.Background(), headlessKey, "first", "@credits", nil); err != nil {
		t.Fatal(err)
	}
	if argv, _ := os.ReadFile(argvFile); strings.Contains(string(argv), "--resume") {
		t.Fatalf("a first turn has nothing to resume: %q", argv)
	}
	transcript := transcriptPath("claude", "sess-A", root)
	if err := os.MkdirAll(filepath.Dir(transcript), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(transcript, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.SendWithModel(context.Background(), headlessKey, "follow-up", "@credits", nil); err != nil {
		t.Fatal(err)
	}
	if argv, _ := os.ReadFile(argvFile); !strings.Contains(string(argv), "--resume\nsess-A\n") {
		t.Fatalf("follow-up argv = %q, want --resume sess-A", argv)
	}
}

func TestHerdrHeadlessProductionConfigSkipsPermissionPrompts(t *testing.T) {
	if current, err := user.Current(); err == nil && current.Uid == "0" {
		t.Skip("root gets the acceptEdits form; production runs as a normal user")
	}
	h, _, argvFile := headlessFixture(t, `printf '%s\n' "$@" > "$(dirname "$0")/argv"
cat >/dev/null
echo '{"type":"result","subtype":"success","is_error":false,"result":"DONE","session_id":"s-perm"}'
`)
	if h.config.Sandbox != "danger-full-access" || h.config.ApprovalPolicy != "never" {
		t.Fatalf("fixture config = %q/%q, want the production danger-full-access/never", h.config.Sandbox, h.config.ApprovalPolicy)
	}
	if _, _, err := h.SendWithModel(context.Background(), headlessKey, "x", "@credits", nil); err != nil {
		t.Fatal(err)
	}
	argv, _ := os.ReadFile(argvFile)
	if !strings.Contains("\n"+string(argv), "\n--dangerously-skip-permissions\n") {
		t.Fatalf("argv = %q, want --dangerously-skip-permissions: a headless worker has no pane to answer a prompt", argv)
	}
}

func TestHerdrHeadlessChildEnvDropsWerkrbySecrets(t *testing.T) {
	for _, name := range []string{"SPYJO_BOT_TOKEN", "SPYNEL_TELEGRAM_TOKEN", "OTHER_BOT_TOKEN", "TELEGRAM_API_HASH", "WHATSAPP_SESSION"} {
		t.Setenv(name, "secret-value")
	}
	t.Setenv("LANG", "C.UTF-8")
	t.Setenv("USER", "worker")
	h, root, _ := headlessFixture(t, `cat >/dev/null
env > "$(dirname "$0")/childenv"
echo '{"type":"result","subtype":"success","is_error":false,"result":"DONE","session_id":"s-env"}'
`)
	if _, _, err := h.SendWithModel(context.Background(), headlessKey, "x", "@credits", nil); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, "childenv"))
	if err != nil {
		t.Fatal(err)
	}
	env := string(data)
	for _, leaked := range []string{"SPYJO_BOT_TOKEN", "SPYNEL_TELEGRAM_TOKEN", "OTHER_BOT_TOKEN", "TELEGRAM_API_HASH", "WHATSAPP_SESSION", "secret-value"} {
		if strings.Contains(env, leaked) {
			t.Fatalf("child env carries %q", leaked)
		}
	}
	for _, kept := range []string{"HOME=", "PATH=", "USER=worker", "LANG=C.UTF-8"} {
		if !strings.Contains("\n"+env, "\n"+kept) {
			t.Fatalf("child env lost %q", kept)
		}
	}
}

func TestHeadlessSecretEnvNames(t *testing.T) {
	for _, name := range []string{"SPYJO_BOT_TOKEN", "FOO_BOT_TOKEN", "SPYNEL_TELEGRAM_TOKEN", "telegram_x", "TELEGRAM_X", "WHATSAPP_X"} {
		if !headlessSecretEnvName(name) {
			t.Errorf("%s should be scrubbed", name)
		}
	}
	for _, name := range []string{"HOME", "PATH", "USER", "LANG", "ANTHROPIC_BASE_URL", "XDG_RUNTIME_DIR"} {
		if headlessSecretEnvName(name) {
			t.Errorf("%s must be kept", name)
		}
	}
}

// A status event must never follow the terminal event: the emit below is slow on a status event, so
// without joining the ticker the final event is recorded first.
func TestHerdrHeadlessNoStatusAfterTerminalEvent(t *testing.T) {
	old := headlessStatusInterval
	headlessStatusInterval = 5 * time.Millisecond
	defer func() { headlessStatusInterval = old }()
	h, _, _ := headlessFixture(t, `cat >/dev/null
sleep 0.3
echo '{"type":"result","subtype":"success","is_error":false,"result":"DONE","session_id":"s-tick"}'
`)
	var log eventLog
	slow := func(e core.Event) {
		if e.Kind == core.EventStatus && strings.Contains(e.Text, "working") {
			time.Sleep(150 * time.Millisecond)
		}
		log.emit(e)
	}
	if _, _, err := h.SendWithModel(context.Background(), headlessKey, "x", "@credits", slow); err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond) // let a straggling status emit land
	log.mu.Lock()
	defer log.mu.Unlock()
	finals := 0
	for _, e := range log.events {
		if e.Done {
			finals++
		} else if finals > 0 {
			t.Fatalf("event after the terminal event: %+v", log.events)
		}
	}
	if finals != 1 {
		t.Fatalf("terminal events = %d, want 1", finals)
	}
}

func TestHerdrHeadlessTurnWithoutSessionLineLeavesNoStaleSession(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	h, root, _ := headlessFixture(t, `cat >/dev/null
if [ -f "$(dirname "$0")/second" ]; then
  echo '{"type":"result","subtype":"success","is_error":false,"result":"DONE"}'
else
  touch "$(dirname "$0")/second"
  echo '{"type":"result","subtype":"success","is_error":false,"result":"DONE","session_id":"sess-old"}'
fi
`)
	if _, _, err := h.SendWithModel(context.Background(), headlessKey, "first", "@credits", nil); err != nil {
		t.Fatal(err)
	}
	if h.ThreadID(headlessKey) != "sess-old" {
		t.Fatalf("first turn session = %q, want sess-old", h.ThreadID(headlessKey))
	}
	_ = root
	thread, _, err := h.SendWithModel(context.Background(), headlessKey, "second", "@credits", nil)
	if err != nil || thread != "" {
		t.Fatalf("second turn = %q, %v; want an empty thread id", thread, err)
	}
	if got := h.ThreadID(headlessKey); got != "" {
		t.Fatalf("key still maps to %q after a turn that reported no session", got)
	}
}

// kimiHeadlessFixture is headlessFixture for a `runner: kimi` headless seat; KIMI_CODE_HOME points at
// the workspace so session lookups never read the real ~/.kimi-code.
func kimiHeadlessFixture(t *testing.T, fakeKimi string) (*Herdr, string) {
	t.Helper()
	root := t.TempDir()
	state := filepath.Join(root, roster.StateDirName)
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}
	body := "staff:\n  researcher:\n    runner: kimi\n    headless: true\n    args: [\"--model\", \"kimi-code/k3\"]\n"
	if err := os.WriteFile(roster.Path(state), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(root, "fake-kimi")
	if err := os.WriteFile(script, []byte("#!/bin/sh\n"+fakeKimi), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KIMI_CODE_HOME", filepath.Join(root, "kimi-home"))
	h, err := NewHerdr(HarnessConfig{Command: filepath.Join(root, "no-such-herdr"), Cwd: root, Sandbox: "danger-full-access", ApprovalPolicy: "never"})
	if err != nil {
		t.Fatal(err)
	}
	h.headlessCommand = filepath.Join(root, "no-such-claude")
	h.headlessKimiCommand = script
	return h, root
}

const kimiTurnOK = `printf '%s\n' "$@" > "$(dirname "$0")/argv"
if [ -t 0 ]; then echo tty > "$(dirname "$0")/stdin"; else cat > "$(dirname "$0")/stdin"; fi
echo '{"role":"meta","type":"system.version","version":"2.2.0"}'
echo '{"role":"assistant","tool_calls":[{"type":"function","id":"t1","function":{"name":"Bash","arguments":"{}"}}]}'
echo '{"role":"tool","tool_call_id":"t1","content":"ok"}'
echo '{"role":"assistant","content":"DONE: handed to review"}'
echo '{"role":"meta","type":"session.resume_hint","session_id":"session_abc-123","command":"kimi -r session_abc-123"}'
`

func TestHerdrHeadlessKimiStaffRunsKimiPrint(t *testing.T) {
	h, root := kimiHeadlessFixture(t, kimiTurnOK)
	var log eventLog
	thread, steered, err := h.SendWithModel(context.Background(), headlessKey, "do the task", "@researcher", log.emit)
	if err != nil || steered || thread != "session_abc-123" {
		t.Fatalf("SendWithModel = %q, %v, %v; want session_abc-123, false, nil", thread, steered, err)
	}
	argv, _ := os.ReadFile(filepath.Join(root, "argv"))
	got := strings.Split(strings.TrimSpace(string(argv)), "\n")
	want := []string{"-p", "do the task", "--output-format", "stream-json", "--model", "kimi-code/k3"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("argv = %q, want %q", got, want)
	}
	if stdin, _ := os.ReadFile(filepath.Join(root, "stdin")); len(stdin) != 0 {
		t.Fatalf("stdin = %q, want nothing (the prompt is the -p value)", stdin)
	}
	done := log.done()
	if len(done) != 1 || done[0].Kind != core.EventFinal || done[0].FinalText == nil || *done[0].FinalText != "DONE: handed to review" || done[0].ThreadID != "session_abc-123" {
		t.Fatalf("done events = %+v, want one final with the last assistant text", done)
	}
	if h.IsActive(headlessKey) {
		t.Fatal("turn still active after the process exited")
	}
}

// The 2026-10-15 stall: a Kimi worker hit its 5-hour wall, printed the 403 and sat at its prompt for
// 10.5 h while Herdr reported it working. Headless, the same failure ends the turn at once with the
// provider text, which the quota handling reads.
func TestHerdrHeadlessKimiQuotaWallFailsTheTurnAtOnce(t *testing.T) {
	h, _ := kimiHeadlessFixture(t, `echo '{"role":"meta","type":"system.version","version":"2.2.0"}'
echo "error: failed to run prompt: [provider.auth_error] 403 You've reached your 5-hour usage limit. Your quota will reset when the current 5-hour window ends." >&2
exit 1
`)
	var log eventLog
	start := time.Now()
	_, _, err := h.SendWithModel(context.Background(), headlessKey, "x", "@researcher", log.emit)
	if err == nil || !strings.Contains(err.Error(), "runner turn failed: error: failed to run prompt: [provider.auth_error] 403 You've reached your 5-hour usage limit") {
		t.Fatalf("err = %v, want the provider 403 as a runner turn failure", err)
	}
	if time.Since(start) > 10*time.Second {
		t.Fatalf("failed turn took %s, want it to end when the process exits", time.Since(start))
	}
	done := log.done()
	if len(done) != 1 || done[0].Kind != core.EventError || !strings.Contains(done[0].Text, "usage limit") {
		t.Fatalf("done events = %+v, want one error event carrying the provider text", done)
	}
}

func TestHerdrHeadlessKimiFollowUpResumesItsSession(t *testing.T) {
	h, root := kimiHeadlessFixture(t, kimiTurnOK)
	if _, _, err := h.SendWithModel(context.Background(), headlessKey, "first", "@researcher", nil); err != nil {
		t.Fatal(err)
	}
	// No session directory yet: the remembered id is not resumed.
	argv, _ := os.ReadFile(filepath.Join(root, "argv"))
	if strings.Contains(string(argv), "-S\n") {
		t.Fatalf("argv = %q, resumed a session Kimi does not have", argv)
	}
	if err := os.MkdirAll(filepath.Join(root, "kimi-home", "sessions", "wd_x", "session_abc-123"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.SendWithModel(context.Background(), headlessKey, "second", "@researcher", nil); err != nil {
		t.Fatal(err)
	}
	argv, _ = os.ReadFile(filepath.Join(root, "argv"))
	got := strings.Split(strings.TrimSpace(string(argv)), "\n")
	want := []string{"-p", "second", "--output-format", "stream-json", "-S", "session_abc-123", "--model", "kimi-code/k3"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("argv = %q, want %q", got, want)
	}
}

func TestKimiSessionIDMustLookLikeOne(t *testing.T) {
	t.Setenv("KIMI_CODE_HOME", t.TempDir())
	for _, id := range []string{"", "sess-123", "session_../../etc", "session_*", "/abs"} {
		if kimiSessionExists(id) {
			t.Fatalf("kimiSessionExists(%q) = true", id)
		}
	}
}

// Review 2026-10-15: a child that outlives Kimi (an MCP server, a background command) and keeps
// stdout or stderr open must not turn a finished turn into a hang or a failure.
func TestHerdrHeadlessKimiChildHoldingPipesDoesNotFailAFinishedTurn(t *testing.T) {
	saved := headlessResultGrace
	headlessResultGrace = 200 * time.Millisecond
	t.Cleanup(func() { headlessResultGrace = saved })
	h, _ := kimiHeadlessFixture(t, `echo '{"role":"assistant","content":"DONE"}'
echo '{"role":"meta","type":"session.resume_hint","session_id":"session_abc-123"}'
sleep 30 &
exit 0
`)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := time.Now()
	var log eventLog
	thread, _, err := h.SendWithModel(ctx, headlessKey, "x", "@researcher", log.emit)
	if err != nil || thread != "session_abc-123" {
		t.Fatalf("SendWithModel = %q, %v; want the finished turn", thread, err)
	}
	if time.Since(start) > 15*time.Second {
		t.Fatalf("turn took %s; the grace should end it", time.Since(start))
	}
	if done := log.done(); len(done) != 1 || done[0].FinalText == nil || *done[0].FinalText != "DONE" {
		t.Fatalf("done events = %+v", done)
	}
}

func TestHerdrHeadlessKimiFailedResumedTurnKeepsItsSession(t *testing.T) {
	h, root := kimiHeadlessFixture(t, `if [ -f "$(dirname "$0")/fail" ]; then echo "error: failed to run prompt: 403 You've reached your 5-hour usage limit." >&2; exit 1; fi
`+kimiTurnOK)
	if _, _, err := h.SendWithModel(context.Background(), headlessKey, "first", "@researcher", nil); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "kimi-home", "sessions", "wd_x", "session_abc-123"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "fail"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.SendWithModel(context.Background(), headlessKey, "second", "@researcher", nil); err == nil {
		t.Fatal("want the walled turn to fail")
	}
	if got := h.ThreadID(headlessKey); got != "session_abc-123" {
		t.Fatalf("session after a failed resumed turn = %q, want session_abc-123 kept", got)
	}
}

func TestKimiFailureDetailIsKimisErrorLine(t *testing.T) {
	stderr := "[mcp venice] usage limit noise\nerror: failed to run prompt: boom\nSee log: /x/kimi-code.log\n"
	if got := kimiFailureDetail(errors.New("exit status 1"), stderr); got != "error: failed to run prompt: boom" {
		t.Fatalf("kimiFailureDetail = %q", got)
	}
}

func agyHeadlessFixture(t *testing.T, fakeAgy string) (*Herdr, string) {
	t.Helper()
	root := t.TempDir()
	state := filepath.Join(root, roster.StateDirName)
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}
	body := "staff:\n  researcher:\n    runner: agy\n    headless: true\n    args: [\"--model\", \"gemini-4-argon-high\"]\n"
	if err := os.WriteFile(roster.Path(state), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(root, "fake-agy")
	if err := os.WriteFile(script, []byte("#!/bin/sh\n"+fakeAgy), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGY_HOME", filepath.Join(root, "agy-home"))
	h, err := NewHerdr(HarnessConfig{Command: filepath.Join(root, "no-such-herdr"), Cwd: root, Sandbox: "danger-full-access", ApprovalPolicy: "never"})
	if err != nil {
		t.Fatal(err)
	}
	h.headlessCommand = filepath.Join(root, "no-such-claude")
	h.headlessAgyCommand = script
	return h, root
}

const agyConv = "bb3d5513-a6a9-4be1-afab-f7f71ef1ccf6"

const agyTurnOK = `printf '%s\n' "$@" > "$(dirname "$0")/argv"
echo $$ > "$(dirname "$0")/pid"
echo '{"event":"init","conversation_id":"` + agyConv + `","init":{"model":"gemini-4-argon-high"}}'
echo '{"event":"step_update","step_update":{"conversation_id":"` + agyConv + `","step_index":2,"state":"ACTIVE","step_type":"tool","tool_name":"run_command","tool_info":{"name":"run_command","parameters":{"CommandLine":"ls"}}}}'
echo '{"event":"step_update","step_update":{"conversation_id":"` + agyConv + `","step_index":2,"state":"DONE","step_type":"tool","tool_name":"run_command"}}'
echo '{"event":"result","result":{"conversation_id":"` + agyConv + `","status":"SUCCESS","response":"DONE","num_turns":1}}'
`

func TestHerdrHeadlessAgyStaffRunsAgyPrint(t *testing.T) {
	h, root := agyHeadlessFixture(t, agyTurnOK)
	dir := filepath.Join(root, "activity")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGENT_ACTIVITY_DIR", dir)
	var log eventLog
	thread, _, err := h.SendWithModel(context.Background(), headlessKey, "do the task", "@researcher", log.emit)
	if err != nil || thread != agyConv {
		t.Fatalf("SendWithModel = %q, %v; want %s, nil", thread, err, agyConv)
	}
	argv, _ := os.ReadFile(filepath.Join(root, "argv"))
	got := strings.Join(strings.Split(strings.TrimSpace(string(argv)), "\n"), " ")
	want := "-p do the task --output-format stream-json --dangerously-skip-permissions --model gemini-4-argon-high"
	if got != want {
		t.Fatalf("argv = %q, want %q", got, want)
	}
	done := log.done()
	if len(done) != 1 || done[0].Kind != core.EventFinal || done[0].FinalText == nil || *done[0].FinalText != "DONE" {
		t.Fatalf("done events = %+v, want one final DONE", done)
	}
	pid, _ := os.ReadFile(filepath.Join(root, "pid"))
	wall, _ := os.ReadFile(filepath.Join(dir, strings.TrimSpace(string(pid))+".log"))
	for _, line := range []string{"▶ run_command  ls", "✓ run_command", "◼ turn done", "● run finished"} {
		if !strings.Contains(string(wall), line) {
			t.Fatalf("wall log lacks %q:\n%s", line, wall)
		}
	}
}

func TestHerdrHeadlessAgyErrorResultFailsTheTurn(t *testing.T) {
	h, _ := agyHeadlessFixture(t, `echo '{"event":"result","result":{"conversation_id":"","status":"ERROR","response":"","error":"RESOURCE_EXHAUSTED: quota exceeded for gemini-4-argon"}}'
echo 'error: RESOURCE_EXHAUSTED: quota exceeded for gemini-4-argon' >&2
exit 1
`)
	_, _, err := h.SendWithModel(context.Background(), headlessKey, "x", "@researcher", nil)
	if err == nil || !strings.Contains(err.Error(), "runner turn failed: RESOURCE_EXHAUSTED: quota exceeded") {
		t.Fatalf("err = %v, want agy's error as the turn failure", err)
	}
}

func TestHerdrHeadlessAgyFollowUpResumesItsConversation(t *testing.T) {
	h, root := agyHeadlessFixture(t, agyTurnOK)
	if _, _, err := h.SendWithModel(context.Background(), headlessKey, "first", "@researcher", nil); err != nil {
		t.Fatal(err)
	}
	if argv, _ := os.ReadFile(filepath.Join(root, "argv")); strings.Contains(string(argv), "--conversation") {
		t.Fatalf("resumed a conversation agy does not have: %q", argv)
	}
	if err := os.MkdirAll(filepath.Join(root, "agy-home", "conversations"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "agy-home", "conversations", agyConv+".db"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.SendWithModel(context.Background(), headlessKey, "second", "@researcher", nil); err != nil {
		t.Fatal(err)
	}
	argv, _ := os.ReadFile(filepath.Join(root, "argv"))
	if !strings.Contains(strings.ReplaceAll(string(argv), "\n", " "), "--conversation "+agyConv) {
		t.Fatalf("argv = %q, want --conversation %s", argv, agyConv)
	}
}

// Review 2026-10-15 (agy): a resumed turn that fails before agy prints a conversation id keeps it.
func TestHerdrHeadlessAgyFailedResumedTurnKeepsItsConversation(t *testing.T) {
	h, root := agyHeadlessFixture(t, `if [ -f "$(dirname "$0")/fail" ]; then echo '{"event":"result","result":{"conversation_id":"","status":"ERROR","error":"RESOURCE_EXHAUSTED"}}'; exit 1; fi
`+agyTurnOK)
	if _, _, err := h.SendWithModel(context.Background(), headlessKey, "first", "@researcher", nil); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "agy-home", "conversations"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "agy-home", "conversations", agyConv+".db"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "fail"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.SendWithModel(context.Background(), headlessKey, "second", "@researcher", nil); err == nil {
		t.Fatal("want the failed turn")
	}
	if got := h.ThreadID(headlessKey); got != agyConv {
		t.Fatalf("conversation after a failed resumed turn = %q, want %s kept", got, agyConv)
	}
}

func TestParseAgyLineIsLenient(t *testing.T) {
	// a float step index, a string tool_info and no tool_name still give the step; the line is not lost
	ev, ok := parseAgyLine([]byte(`{"event":"step_update","step_update":{"conversation_id":"c1","step_index":2.0,"state":"ACTIVE","step_type":"tool","tool_info":{"name":"run_command","parameters":{"CommandLine":"ls"}}}}`))
	if !ok || ev.toolID != "2" || ev.toolName != "run_command" || ev.sessionID != "c1" {
		t.Fatalf("step = %+v %v", ev, ok)
	}
	ev, ok = parseAgyLine([]byte(`{"event":"step_update","step_update":"opaque","conversation_id":"c2"}`))
	if !ok || ev.sessionID != "c2" {
		t.Fatalf("string step_update = %+v %v", ev, ok)
	}
	ev, ok = parseAgyLine([]byte(`{"event":"result","result":{"conversation_id":"c3","status":"SUCCESS","response":{"text":"odd"},"usage":"x"}}`))
	if !ok || ev.final == nil || ev.final.Subtype != "success" {
		t.Fatalf("odd result = %+v %v", ev, ok)
	}
	// an ERROR with no error text leaves the reason to stderr
	ev, _ = parseAgyLine([]byte(`{"event":"result","result":{"status":"ERROR"}}`))
	if ev.final == nil || !ev.final.IsError || ev.final.Result != "" {
		t.Fatalf("empty error = %+v", ev.final)
	}
}

func TestHerdrHeadlessAgyEmptyErrorUsesStderr(t *testing.T) {
	h, _ := agyHeadlessFixture(t, `echo '{"event":"result","result":{"status":"ERROR"}}'
echo 'error: model overloaded, try later' >&2
exit 1
`)
	_, _, err := h.SendWithModel(context.Background(), headlessKey, "x", "@researcher", nil)
	if err == nil || !strings.Contains(err.Error(), "error: model overloaded") {
		t.Fatalf("err = %v, want the stderr error line", err)
	}
}

func TestHerdrHeadlessAgyPlanModeIsNotFullAccess(t *testing.T) {
	h, root := agyHeadlessFixture(t, agyTurnOK)
	h.config.ApprovalPolicy = "plan"
	if _, _, err := h.SendWithModel(context.Background(), headlessKey, "x", "@researcher", nil); err != nil {
		t.Fatal(err)
	}
	argv, _ := os.ReadFile(filepath.Join(root, "argv"))
	got := strings.ReplaceAll(string(argv), "\n", " ")
	if strings.Contains(got, "--dangerously-skip-permissions") || !strings.Contains(got, "--mode plan") {
		t.Fatalf("argv = %q, want --mode plan and no skip-permissions", got)
	}
}

// hermesTurnOK is a fake hermes one-shot run: it records its argv and cwd, writes the --usage-file
// report of a completed turn, and prints only the final reply.
const hermesTurnOK = `printf '%s\n' "$@" >> "$(dirname "$0")/argv"
pwd > "$(dirname "$0")/cwd"
prev=""
for a in "$@"; do
  if [ "$prev" = "--usage-file" ]; then
    printf '%s' '{"session_id":"local-abc123456789","completed":true,"failed":false,"turn_exit_reason":"text_response(finish_reason=stop)","failure":null}' > "$a"
  fi
  prev="$a"
done
echo 'DONE: handed to review'
`

// hermesHeadlessFixture is a workspace whose roster has one headless hermes seat, a fake hermes
// one-shot client, and a fake gateway-turn stop script; a turn that reaches a pane path fails
// instead of passing by accident.
func hermesHeadlessFixture(t *testing.T, fakeHermes string) (*Herdr, string) {
	t.Helper()
	root := t.TempDir()
	t.Setenv("AGENT_ACTIVITY_DIR", filepath.Join(root, "activity"))
	state := filepath.Join(root, roster.StateDirName)
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}
	body := "staff:\n  pilot:\n    runner: hermes\n    headless: true\n    args: [\"-p\", \"wk-pilot\", \"--model\", \"zai/glm-5.3-flash\", \"--reasoning\", \"high\"]\n  pane: {runner: hermes}\n"
	if err := os.WriteFile(roster.Path(state), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(root, "fake-hermes")
	if err := os.WriteFile(script, []byte("#!/bin/sh\n"+fakeHermes), 0o700); err != nil {
		t.Fatal(err)
	}
	stop := filepath.Join(root, "fake-stop")
	if err := os.WriteFile(stop, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$(dirname \"$0\")/stop-argv\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	h, err := NewHerdr(HarnessConfig{Command: filepath.Join(root, "no-such-herdr"), Cwd: root, Sandbox: "danger-full-access", ApprovalPolicy: "never"})
	if err != nil {
		t.Fatal(err)
	}
	h.headlessHermesCommand = script
	h.headlessHermesStopScript = stop
	h.headlessHermesStopPython = "/bin/sh"
	return h, root
}

func TestHerdrHeadlessHermesStaffRunsHermesOneshot(t *testing.T) {
	h, root := hermesHeadlessFixture(t, hermesTurnOK)
	var log eventLog
	thread, steered, err := h.SendWithModel(context.Background(), headlessKey, "do the task", "@pilot", log.emit)
	if err != nil || steered || thread != "local-abc123456789" {
		t.Fatalf("SendWithModel = %q, %v, %v; want local-abc123456789, false, nil", thread, steered, err)
	}
	argv, _ := os.ReadFile(filepath.Join(root, "argv"))
	got := strings.Split(strings.TrimSpace(string(argv)), "\n")
	if len(got) != 10 || got[0] != "-z" || got[1] != "do the task" || got[2] != "--usage-file" {
		t.Fatalf("argv = %q, want -z <prompt> --usage-file <path> and the seat args", got)
	}
	usageDir := filepath.Join(root, roster.StateDirName, "runtime", "hermes-usage")
	if !strings.HasPrefix(got[3], usageDir) {
		t.Fatalf("usage file %q, want one under %s", got[3], usageDir)
	}
	want := []string{"-p", "wk-pilot", "--model", "zai/glm-5.3-flash", "--reasoning", "high"}
	if strings.Join(got[4:], " ") != strings.Join(want, " ") {
		t.Fatalf("seat args = %q, want %q", got[4:], want)
	}
	if cwd, _ := os.ReadFile(filepath.Join(root, "cwd")); strings.TrimSpace(string(cwd)) != root {
		t.Fatalf("cwd = %q, want %q", cwd, root)
	}
	done := log.done()
	if len(done) != 1 || done[0].Kind != core.EventFinal || done[0].FinalText == nil || *done[0].FinalText != "DONE: handed to review" || done[0].ThreadID != "local-abc123456789" {
		t.Fatalf("done events = %+v, want one final with the reply text", done)
	}
	// The report is kept under its final per-run name, not the temp name it was written to.
	entries, err := os.ReadDir(usageDir)
	if err != nil || len(entries) != 1 || !strings.HasSuffix(entries[0].Name(), "-usage.json") || strings.HasPrefix(entries[0].Name(), "turn-") {
		t.Fatalf("usage dir = %v, %v; want one <pid>-usage.json report", entries, err)
	}
	if h.ThreadID(headlessKey) != "local-abc123456789" || h.IsActive(headlessKey) {
		t.Fatalf("session %q active %v; want remembered and the turn cleared", h.ThreadID(headlessKey), h.IsActive(headlessKey))
	}
}

// The live wall wording (2026-10-28, zai one-shot): the client exits 2 and the usage report written
// even on failure carries the provider text the quota handling reads.
func TestHerdrHeadlessHermesQuotaWallFailsTheTurn(t *testing.T) {
	h, _ := hermesHeadlessFixture(t, `prev=""
for a in "$@"; do
  if [ "$prev" = "--usage-file" ]; then
    printf '%s' '{"session_id":"local-abc123456789","completed":false,"failed":true,"failure":"HTTP 429: Weekly/Monthly Limit Exhausted. Your limit will reset at 2026-10-31 02:53:12"}' > "$a"
  fi
  prev="$a"
done
echo 'Z.AI / GLM rate-limited every one of 3 attempts — it looks temporarily unavailable.'
exit 2
`)
	var log eventLog
	_, _, err := h.SendWithModel(context.Background(), headlessKey, "x", "@pilot", log.emit)
	if err == nil || !strings.Contains(err.Error(), "runner turn failed: HTTP 429: Weekly/Monthly Limit Exhausted. Your limit will reset at 2026-10-31 02:53:12") {
		t.Fatalf("err = %v, want the provider wall text as a runner turn failure", err)
	}
	done := log.done()
	if len(done) != 1 || done[0].Kind != core.EventError || !strings.Contains(done[0].Text, "Weekly/Monthly Limit Exhausted") {
		t.Fatalf("done events = %+v, want one error event carrying the provider text", done)
	}
}

func TestHerdrHeadlessHermesFollowUpResumesItsSession(t *testing.T) {
	h, root := hermesHeadlessFixture(t, hermesTurnOK)
	if _, _, err := h.SendWithModel(context.Background(), headlessKey, "first", "@pilot", nil); err != nil {
		t.Fatal(err)
	}
	argv, _ := os.ReadFile(filepath.Join(root, "argv"))
	if strings.Contains(string(argv), "--resume") {
		t.Fatalf("argv = %q, resumed a session no report has named yet", argv)
	}
	if _, _, err := h.SendWithModel(context.Background(), headlessKey, "second", "@pilot", nil); err != nil {
		t.Fatal(err)
	}
	argv, _ = os.ReadFile(filepath.Join(root, "argv"))
	got := strings.Split(strings.TrimSpace(string(argv)), "\n")
	if len(got) != 22 { // two invocations of 10, the second carrying --resume <id>
		t.Fatalf("argv = %q, want two recorded invocations", got)
	}
	want := []string{"-z", "second", "--resume", "local-abc123456789", "--usage-file"}
	if strings.Join(got[10:15], " ") != strings.Join(want, " ") {
		t.Fatalf("second invocation = %q, want %q", got[10:15], want)
	}
}

// A remembered session the profile no longer has fails the resume before any run (live: exit 1,
// `Error: No session found matching`, no report); the session is forgotten so the next turn starts
// fresh instead of failing the same way every time.
func TestHerdrHeadlessHermesStaleResumeIsForgotten(t *testing.T) {
	h, _ := hermesHeadlessFixture(t, `if [ -f "$(dirname "$0")/second" ]; then
  echo "Error: No session found matching 'local-abc123456789'. Use 'hermes sessions list' to see available sessions." >&2
  exit 1
fi
touch "$(dirname "$0")/second"
`+hermesTurnOK)
	if _, _, err := h.SendWithModel(context.Background(), headlessKey, "first", "@pilot", nil); err != nil {
		t.Fatal(err)
	}
	if h.ThreadID(headlessKey) != "local-abc123456789" {
		t.Fatalf("session = %q, want it remembered after the first turn", h.ThreadID(headlessKey))
	}
	_, _, err := h.SendWithModel(context.Background(), headlessKey, "second", "@pilot", nil)
	if err == nil || !strings.Contains(err.Error(), "No session found matching") {
		t.Fatalf("err = %v, want the stale-resume failure", err)
	}
	if h.ThreadID(headlessKey) != "" {
		t.Fatalf("session = %q, want the stale id forgotten", h.ThreadID(headlessKey))
	}
}

// The known one-shot gap (2026-10-26): a cut client ends only the client, while the profile's
// gateway keeps the detached turn running. An interrupt kills the client's process group and stops
// the gateway turn before the turn is reported over.
func TestHerdrHeadlessHermesInterruptStopsTheGatewayTurn(t *testing.T) {
	h, root := hermesHeadlessFixture(t, `exec sleep 30
`)
	errc := make(chan error, 1)
	go func() {
		_, _, err := h.SendWithModel(context.Background(), headlessKey, "x", "@pilot", nil)
		errc <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for !h.IsActive(headlessKey) {
		if time.Now().After(deadline) {
			t.Fatal("headless turn never became active")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if ok, err := h.Interrupt(context.Background(), headlessKey); !ok || err != nil {
		t.Fatalf("Interrupt = %v, %v", ok, err)
	}
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("interrupted headless turn did not return")
	}
	stopArgv, err := os.ReadFile(filepath.Join(root, "stop-argv"))
	if err != nil {
		t.Fatalf("gateway-turn stop did not run before the turn returned: %v", err)
	}
	got := strings.Split(strings.TrimSpace(string(stopArgv)), "\n")
	if len(got) != 8 || got[0] != "--profile" || got[1] != "wk-pilot" || got[2] != "--cwd" || got[3] != root || got[6] != "--settle" || got[7] != "240" {
		t.Fatalf("stop argv = %q, want --profile wk-pilot --cwd <root> --since <epoch> --settle 240", got)
	}
	if _, err := strconv.ParseInt(got[5], 10, 64); err != nil {
		t.Fatalf("--since = %q, want an epoch seconds value", got[5])
	}
}

func TestHerdrHeadlessHermesExitZeroWithoutReportFails(t *testing.T) {
	h, _ := hermesHeadlessFixture(t, `echo 'DONE: handed to review'
`)
	var log eventLog
	_, _, err := h.SendWithModel(context.Background(), headlessKey, "x", "@pilot", log.emit)
	if err == nil || !strings.Contains(err.Error(), "hermes -z ended without a usage report") {
		t.Fatalf("err = %v, want the missing-report failure, not a pass on unreadable stdout", err)
	}
}

func TestHermesProfileArg(t *testing.T) {
	got := hermesProfileArg([]string{"-p", "wk-notify", "--model", "zai/glm-5.3-flash"})
	if got != "wk-notify" {
		t.Fatalf("hermesProfileArg = %q, want wk-notify", got)
	}
	if hermesProfileArg([]string{"--reasoning", "high"}) != "" {
		t.Fatal("hermesProfileArg = a value, want \"\" without a -p arg")
	}
}
