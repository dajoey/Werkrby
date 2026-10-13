package harness

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

// sendHeadless runs one turn of a `headless: true` roster staff as `claude -p --output-format
// stream-json` in the workspace instead of typing the prompt into an interactive Herdr pane. It keeps
// the Herdr turn contract: it blocks until the turn ends, emits status at the Herdr cadence (status
// events feed the job lease heartbeat), emits one final or error event with Done set, and returns the
// Claude session id. Each turn is a fresh session, as a freshly spawned Herdr worker is. The caller
// holds the key lock.
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

	args := append([]string{"-p", "--output-format", "stream-json", "--verbose"}, claudePermissionArgs(cfg)...)
	args = append(args, staff.Args...)
	cmd := exec.CommandContext(turnContext, command, args...)
	cmd.Dir = cfg.Cwd
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

	tickerDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(herdrStatusInterval(strings.TrimSpace(key)))
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
	if turnContext.Err() != nil {
		if errors.Is(turnContext.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
			detail := fmt.Sprintf("headless worker %s exceeded %s", target, herdrWaitCeiling)
			emitHeadlessError(emit, threadID, detail)
			return threadID, false, fmt.Errorf("runner turn failed: %s", detail)
		}
		return threadID, false, turnContext.Err()
	}
	if final == nil || final.IsError || waitErr != nil {
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

// headlessFailureDetail names why a headless turn failed, preferring Claude Code's own result text
// (for example a provider 429 or "Credit balance is too low", which the orchestrator's quota handling
// reads) over the exit status.
func headlessFailureDetail(final *headlessResultLine, waitErr error, stderrTail string) string {
	if final != nil && strings.TrimSpace(final.Result) != "" {
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
