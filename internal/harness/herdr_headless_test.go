package harness

import (
	"context"
	"errors"
	"os"
	"os/user"
	"path/filepath"
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
