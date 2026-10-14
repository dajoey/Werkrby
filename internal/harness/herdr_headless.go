package harness

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
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
	command := h.headlessCommand
	if command == "" {
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

	args := []string{"-p", "--output-format", "stream-json", "--verbose"}
	h.mu.Lock()
	previous := h.sessions[key]
	h.mu.Unlock()
	if path := transcriptPath("claude", previous, cfg.Cwd); path != "" {
		if _, err := os.Stat(path); err == nil {
			args = append(args, "--resume", previous)
		}
	}
	args = append(append(args, claudePermissionArgs(cfg)...), staff.Args...)
	// procContext ends the process; turnContext ends the turn. They differ once a result is in:
	// a process still running headlessResultGrace after its result line is killed, and the
	// result stands.
	procContext, procCancel := context.WithCancel(turnContext)
	defer procCancel()
	cmd := exec.CommandContext(procContext, command, args...)
	cmd.Dir = cfg.Cwd
	cmd.Env = headlessChildEnv(os.Environ())
	cmd.Stdin = strings.NewReader(prompt)
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
		startErr := fmt.Errorf("start headless Claude Code for %s: %w", target, err)
		emitHeadlessError(emit, "", startErr.Error())
		return "", false, startErr
	}
	if emit != nil {
		emit(core.Event{Kind: core.EventStatus, Text: fmt.Sprintf("Headless: running staff %s as claude -p in %s", staffName, cfg.Cwd)})
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
	reader := bufio.NewReader(stdout)
	for {
		line, readErr := reader.ReadBytes('\n')
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
	if threadID == "" && turnContext.Err() == nil {
		h.forgetSession(key)
	}
	if turnContext.Err() != nil {
		if errors.Is(turnContext.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
			detail := fmt.Sprintf("headless worker %s exceeded %s", target, herdrWaitCeiling)
			emitHeadlessError(emit, threadID, detail)
			return threadID, false, fmt.Errorf("runner turn failed: %s", detail)
		}
		return threadID, false, turnContext.Err()
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
