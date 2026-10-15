package harness

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/roster"
)

// headlessResultLine is the part of a `claude -p --output-format stream-json` line a headless turn reads.
type headlessResultLine struct {
	Type      string `json:"type"`
	Subtype   string `json:"subtype"`
	SessionID string `json:"session_id"`
	IsError   bool   `json:"is_error"`
	Result    string `json:"result"`
}

// headlessResultGrace is how long a headless turn waits for `claude -p` to exit after its result
// line before killing the process group and using the result.
var headlessResultGrace = 30 * time.Second

// headlessStatusInterval overrides the status cadence of a headless turn (tests); zero uses the Herdr cadence.
var headlessStatusInterval time.Duration

// headlessChildEnv is the environment a headless worker gets: Werkrby's own, minus the credentials only
// Werkrby needs (channel bot tokens). A pane worker inherits the Herdr server's environment, which
// does not carry them, plus whatever its pane shell's rc files export: ~/.bashrc must not export a
// bot token (ops tests/test_fleet_mcp_env_allowlist.py guards fleet-mcp-env.sh). The child of a
// direct exec would carry them, and its Bash tool could read them.
func headlessChildEnv(environ []string) []string {
	out := make([]string, 0, len(environ))
	for _, entry := range environ {
		name, _, _ := strings.Cut(entry, "=")
		if !headlessSecretEnvName(name) {
			out = append(out, entry)
		}
	}
	return out
}

// headlessSecretEnvName reports whether an environment variable holds a Werkrby-only secret: any
// bot token (the configured channels.telegram.token_env is SPYJO_BOT_TOKEN), anything for the
// Telegram or WhatsApp channels, and a token, secret or key under Werkrby's own prefixes.
func headlessSecretEnvName(name string) bool {
	upper := strings.ToUpper(name)
	switch {
	case strings.HasSuffix(upper, "_BOT_TOKEN"),
		strings.HasPrefix(upper, "TELEGRAM_"), strings.Contains(upper, "_TELEGRAM_"),
		strings.HasPrefix(upper, "WHATSAPP_"), strings.Contains(upper, "_WHATSAPP_"):
		return true
	}
	for _, prefix := range []string{"SPYJO_", "SPYNEL_", "WERKRBY_"} {
		if strings.HasPrefix(upper, prefix) {
			for _, suffix := range []string{"TOKEN", "SECRET", "KEY", "PASSWORD"} {
				if strings.HasSuffix(upper, suffix) {
					return true
				}
			}
		}
	}
	return false
}

// sendHeadless runs one turn of a `headless: true` roster staff as `claude -p --output-format
// stream-json` in the workspace instead of typing the prompt into an interactive Herdr pane. It keeps
// the Herdr turn contract: it blocks until the turn ends, emits status at the Herdr cadence (status
// events feed the job lease heartbeat), emits one final or error event with Done set, and returns the
// Claude session id. A follow-up turn on the same key resumes the remembered session when its Claude
// transcript exists, so continuations and operator messages keep their context. The caller holds the
// key lock.
func (h *Herdr) sendHeadless(ctx context.Context, key, prompt, staffName string, staff roster.Staff, cfg HarnessConfig, emit core.Emit) (string, bool, error) {
	kimi := staff.Runner == "kimi"
	runner := "claude"
	command := h.headlessCommand
	if kimi {
		runner = "kimi"
		command = h.headlessKimiCommand
		if command == "" {
			command = kimiExecutable()
		}
	} else if command == "" {
		command = "claude"
	}
	target := "headless:" + staffName
	turnContext, cancel := context.WithTimeout(ctx, herdrWaitCeiling)
	turn := &herdrTurn{target: target, cancel: cancel, done: make(chan struct{}), headless: true}
	h.mu.Lock()
	h.active[key] = turn
	h.mu.Unlock()
	defer func() {
		cancel()
		h.mu.Lock()
		delete(h.active, key)
		h.mu.Unlock()
		close(turn.done)
	}()

	h.mu.Lock()
	previous := h.sessions[key]
	h.mu.Unlock()
	var args []string
	resumed := false // a kimi turn passed -S previous
	if kimi {
		// Kimi takes the prompt as the -p value (the pane path already hands Herdr the prompt as one
		// argument, so it fits the same limit) and runs tools without asking in prompt mode.
		args = []string{"-p", prompt, "--output-format", "stream-json"}
		if kimiSessionExists(previous) {
			args = append(args, "-S", previous)
			resumed = true
		}
		args = append(args, staff.Args...)
	} else {
		args = []string{"-p", "--output-format", "stream-json", "--verbose"}
		if path := transcriptPath("claude", previous, cfg.Cwd); path != "" {
			if _, err := os.Stat(path); err == nil {
				args = append(args, "--resume", previous)
			}
		}
		args = append(append(args, claudePermissionArgs(cfg)...), staff.Args...)
	}
	// procContext ends the process; turnContext ends the turn. They differ once a result is in:
	// a process still running headlessResultGrace after its result line is killed, and the
	// result stands.
	procContext, procCancel := context.WithCancel(turnContext)
	defer procCancel()
	cmd := exec.CommandContext(procContext, command, args...)
	cmd.Dir = cfg.Cwd
	cmd.Env = headlessChildEnv(os.Environ())
	// The task id names the run on the Agent Wall and marks it as Werkrby's own agent for the guards.
	if taskID := headlessTaskID(key); taskID != "" {
		cmd.Env = append(cmd.Env, "WERKRBY_TASK_ID="+taskID)
	}
	if !kimi {
		cmd.Stdin = strings.NewReader(prompt)
	}
	// Claude Code starts tool and MCP children that can inherit stdout. On cancel the whole process
	// group is killed, and WaitDelay bounds Wait if a pipe is still held open anyway.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 10 * time.Second
	stderr := newTailBuffer(64 * 1024)
	cmd.Stderr = stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", false, err
	}
	turn.cmd = cmd
	if err := cmd.Start(); err != nil {
		startErr := fmt.Errorf("start headless %s for %s: %w", runner, target, err)
		emitHeadlessError(emit, "", startErr.Error())
		return "", false, startErr
	}
	if emit != nil {
		emit(core.Event{Kind: core.EventStatus, Text: fmt.Sprintf("Headless: running staff %s as %s -p in %s", staffName, runner, cfg.Cwd)})
	}
	// Claude Code writes its own Agent Wall log (the run-activity mod); Kimi's is written from its stream.
	var activity *headlessActivity
	if kimi {
		activity = newHeadlessActivity(headlessActivityDir(), cmd.Process.Pid, cfg.Cwd, prompt, kimiModelArg(staff.Args))
	}

	// The ticker is joined before any terminal event so a status event never follows it.
	tickerDone := make(chan struct{})
	tickerExited := make(chan struct{})
	go func() {
		defer close(tickerExited)
		interval := herdrStatusInterval(strings.TrimSpace(key))
		if headlessStatusInterval > 0 {
			interval = headlessStatusInterval
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		start := time.Now()
		for {
			select {
			case <-tickerDone:
				return
			case <-turnContext.Done():
				return
			case <-ticker.C:
				if emit != nil {
					emit(core.Event{Kind: core.EventStatus, Text: fmt.Sprintf("Headless: %s working (%s)...", target, time.Since(start).Truncate(time.Second))})
				}
			}
		}
	}()

	var threadID string
	var final *headlessResultLine
	var kimiReply string
	kimiDone := false // Kimi printed its closing session.resume_hint: the turn's answer is complete
	reader := bufio.NewReader(stdout)
	for {
		line, readErr := reader.ReadBytes('\n')
		if kimi {
			if msg, ok := parseKimiLine(line); ok {
				if msg.sessionID != "" && threadID == "" {
					threadID = msg.sessionID
					h.rememberSession(key, threadID)
				}
				// The resume hint is Kimi's last line, its result line: a child that outlives Kimi
				// (an MCP server, a background command) and holds stdout or stderr open must not
				// turn the finished turn into a hang or a failure, so the process group gets the
				// same grace as Claude's after its result.
				if msg.sessionID != "" && !kimiDone {
					kimiDone = true
					time.AfterFunc(headlessResultGrace, procCancel)
				}
				if msg.reply != "" {
					kimiReply = msg.reply
				}
				for _, call := range msg.calls {
					activity.call(call.ID, call.Function.Name, call.Function.Arguments)
				}
				if msg.resultFor != "" {
					activity.result(msg.resultFor)
				}
			}
			if readErr != nil {
				break
			}
			continue
		}
		var msg headlessResultLine
		if len(line) > 0 && json.Unmarshal(line, &msg) == nil {
			if msg.SessionID != "" && threadID == "" {
				threadID = msg.SessionID
				h.rememberSession(key, threadID)
			}
			if msg.Type == "result" {
				result := msg
				if final == nil {
					time.AfterFunc(headlessResultGrace, procCancel)
				}
				final = &result
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				final = nil
			}
			break
		}
	}
	waitErr := cmd.Wait()
	close(tickerDone)
	<-tickerExited
	// A turn that reported no session leaves no session for the key: a stale id from an earlier
	// turn would be handed back by ThreadID and resumed by the next turn as if this one had
	// continued it. An interrupted or timed-out turn keeps the earlier session, since it may have
	// died before Claude Code printed one.
	// Kimi prints its session id only when a turn finishes, so a resumed (-S) turn that failed
	// still has its session: keep it.
	if threadID == "" && turnContext.Err() == nil && !(kimi && resumed) {
		h.forgetSession(key)
	}
	if turnContext.Err() != nil {
		activity.finish("stopped: " + turnContext.Err().Error())
		if errors.Is(turnContext.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
			detail := fmt.Sprintf("headless worker %s exceeded %s", target, herdrWaitCeiling)
			emitHeadlessError(emit, threadID, detail)
			return threadID, false, fmt.Errorf("runner turn failed: %s", detail)
		}
		return threadID, false, turnContext.Err()
	}
	if kimi {
		// Kimi prints no result line: exit 0 is a finished turn, anything else failed, and the reason
		// (`error: failed to run prompt: 403 You've reached your 5-hour usage limit ...`) is on stderr.
		// After the resume hint the turn stands whatever the exit: a child holding a pipe ends Wait
		// with ErrWaitDelay, and the grace kill ends it with a signal.
		if waitErr != nil && !kimiDone {
			detail := kimiFailureDetail(waitErr, stderr.String())
			activity.finish(detail)
			emitHeadlessError(emit, threadID, detail)
			return threadID, false, fmt.Errorf("runner turn failed: %s", detail)
		}
		activity.finish("")
		final = &headlessResultLine{Type: "result", Subtype: "success", Result: kimiReply, SessionID: threadID}
	}
	// A success result stands whatever the exit status: the process may have been killed after
	// its result, or exited non-zero while shutting down.
	if final == nil || final.IsError || final.Subtype != "success" {
		detail := headlessFailureDetail(final, waitErr, stderr.String())
		emitHeadlessError(emit, threadID, detail)
		return threadID, false, fmt.Errorf("runner turn failed: %s", detail)
	}
	finalText := final.Result
	if emit != nil {
		emit(core.Event{
			Kind:      core.EventFinal,
			Text:      finalText,
			FinalText: &finalText,
			ThreadID:  threadID,
			Done:      true,
			Execution: &core.ExecutionStatus{State: "finishing"},
		})
	}
	return threadID, false, nil
}

func (h *Herdr) forgetSession(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.sessions[key]; ok {
		delete(h.sessions, key)
		_ = h.saveSessionsLocked()
	}
}

// headlessFailureDetail names why a headless turn failed. The result text is used only when Claude
// Code marked it an error (for example a provider 429 or "Credit balance is too low", which the
// orchestrator's quota handling reads): a non-error result is the agent's own reply, and quoting it
// could make a reply that mentions a 429 read as a quota wall.
func headlessFailureDetail(final *headlessResultLine, waitErr error, stderrTail string) string {
	if final != nil && final.IsError && strings.TrimSpace(final.Result) != "" {
		return strings.TrimSpace(final.Result)
	}
	if tail := strings.TrimSpace(stderrTail); tail != "" {
		if len(tail) > 2000 {
			tail = tail[len(tail)-2000:]
		}
		return tail
	}
	if final != nil && final.Subtype != "" {
		return "claude -p ended with " + final.Subtype
	}
	if waitErr != nil {
		return "claude -p failed: " + waitErr.Error()
	}
	return "claude -p ended without a result"
}

func emitHeadlessError(emit core.Emit, threadID, detail string) {
	if emit == nil {
		return
	}
	emit(core.Event{
		Kind:      core.EventError,
		Text:      detail,
		ThreadID:  threadID,
		Done:      true,
		Execution: &core.ExecutionStatus{State: "error", Detail: detail},
	})
}

// kimiSessionPattern is the shape of a Kimi Code session id (`session_<uuid>`); anything else is
// never resumed, and it keeps the id safe to use as a path element.
var kimiSessionPattern = regexp.MustCompile(`^session_[A-Za-z0-9-]+$`)

// kimiHome is Kimi Code's state directory: KIMI_CODE_HOME, else ~/.kimi-code.
func kimiHome() string {
	if dir := strings.TrimSpace(os.Getenv("KIMI_CODE_HOME")); dir != "" {
		return dir
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".kimi-code")
}

// kimiExecutable is the kimi binary for a headless seat: kimi on PATH, else the installer's
// ~/.kimi-code/bin/kimi (the Werkrby service's PATH need not include it).
func kimiExecutable() string {
	if path, err := exec.LookPath("kimi"); err == nil {
		return path
	}
	if home := kimiHome(); home != "" {
		candidate := filepath.Join(home, "bin", "kimi")
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate
		}
	}
	return "kimi"
}

// kimiSessionExists reports whether a remembered Kimi session can be resumed: Kimi keeps each session
// in <home>/sessions/<workdir key>/<session id>.
func kimiSessionExists(id string) bool {
	if !kimiSessionPattern.MatchString(id) {
		return false
	}
	home := kimiHome()
	if home == "" {
		return false
	}
	matches, err := filepath.Glob(filepath.Join(home, "sessions", "*", id))
	return err == nil && len(matches) > 0
}

// kimiStreamLine is the part of a `kimi -p --output-format stream-json` line a headless turn reads.
type kimiStreamLine struct {
	Role       string          `json:"role"`
	Type       string          `json:"type"`
	SessionID  string          `json:"session_id"`
	Content    json.RawMessage `json:"content"`
	ToolCalls  []kimiToolCall  `json:"tool_calls"`
	ToolCallID string          `json:"tool_call_id"`
}

type kimiToolCall struct {
	ID       string `json:"id"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

// kimiLine is what one stream-json line says: the session id (resume hint), an assistant reply's
// text, the tool calls it makes, or the id of the call a tool result answers.
type kimiLine struct {
	sessionID string
	reply     string
	calls     []kimiToolCall
	resultFor string
}

// parseKimiLine reads one stream-json line; ok is false for a line that is not JSON.
func parseKimiLine(line []byte) (kimiLine, bool) {
	var msg kimiStreamLine
	if len(bytes.TrimSpace(line)) == 0 || json.Unmarshal(line, &msg) != nil {
		return kimiLine{}, false
	}
	switch {
	case msg.Role == "meta" && msg.Type == "session.resume_hint":
		return kimiLine{sessionID: msg.SessionID}, true
	case msg.Role == "tool":
		return kimiLine{resultFor: msg.ToolCallID}, true
	case msg.Role != "assistant":
		return kimiLine{}, true
	}
	out := kimiLine{calls: msg.ToolCalls}
	if len(msg.Content) > 0 {
		var text string
		if json.Unmarshal(msg.Content, &text) == nil {
			out.reply = strings.TrimSpace(text)
		} else {
			var parts []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			}
			if json.Unmarshal(msg.Content, &parts) == nil {
				var b strings.Builder
				for _, part := range parts {
					if part.Type == "text" {
						b.WriteString(part.Text)
					}
				}
				out.reply = strings.TrimSpace(b.String())
			}
		}
	}
	return out, true
}

// kimiModelArg is the --model / -m value in a seat's launch args, for the wall's footer.
func kimiModelArg(args []string) string {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == "--model" || args[i] == "-m" {
			return args[i+1]
		}
	}
	return ""
}

// kimiFailureDetail names why a headless Kimi turn failed: Kimi's own `error:` line on stderr when it
// printed one (the orchestrator's quota handling reads provider text such as "usage limit"; the rest
// of stderr can carry MCP-server or hook output that must not), else the stderr tail, else the exit
// status.
func kimiFailureDetail(waitErr error, stderrTail string) string {
	lines := strings.Split(strings.TrimSpace(stderrTail), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if line := strings.TrimSpace(lines[i]); strings.HasPrefix(line, "error:") {
			return line
		}
	}
	if tail := strings.TrimSpace(stderrTail); tail != "" {
		if len(tail) > 2000 {
			tail = tail[len(tail)-2000:]
		}
		return tail
	}
	return "kimi -p failed: " + waitErr.Error()
}
