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
	"strings"
	"time"

	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/fsx"
	"github.com/agent0ai/spynel/internal/roster"
)

type hermesBoundaryUsageReport struct {
	SessionID        string `json:"session_id"`
	Completed        bool   `json:"completed"`
	Failed           bool   `json:"failed"`
	TurnExitReason   string `json:"turn_exit_reason"`
	Failure          string `json:"failure"`
	InputTokens      *int64 `json:"input_tokens"`
	OutputTokens     *int64 `json:"output_tokens"`
	CacheReadTokens  *int64 `json:"cache_read_tokens"`
	CacheWriteTokens *int64 `json:"cache_write_tokens"`
	ReasoningTokens  *int64 `json:"reasoning_tokens"`
	APICalls         *int64 `json:"api_calls"`
	EstimatedCostUSD any    `json:"estimated_cost_usd"`
}

func readHermesBoundaryUsage(path string) (*hermesBoundaryUsageReport, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var report hermesBoundaryUsageReport
	if err := json.Unmarshal(data, &report); err != nil {
		return nil, fmt.Errorf("parse Hermes usage report: %w", err)
	}
	return &report, nil
}

func writeHermesBoundaryUsage(spec RunBoundarySpec, report *hermesBoundaryUsageReport) error {
	if report == nil {
		return nil
	}
	record := map[string]any{
		"run_id":       spec.RunID,
		"source":       "hermes",
		"collected_at": time.Now().UTC().Format(time.RFC3339Nano),
	}
	for key, value := range map[string]*int64{
		"fresh_input_tokens": report.InputTokens,
		"output_tokens":      report.OutputTokens,
		"cache_read_tokens":  report.CacheReadTokens,
		"cache_write_tokens": report.CacheWriteTokens,
		"reasoning_tokens":   report.ReasoningTokens,
		"api_calls":          report.APICalls,
	} {
		if value != nil {
			if *value < 0 {
				return fmt.Errorf("Hermes usage %s is negative", key)
			}
			record[key] = *value
		}
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return fsx.AtomicWriteFile(filepath.Join(spec.RunDir, "usage.json"), data, 0o600)
}

func hermesQuotaReset(failure string) (time.Time, bool) {
	lower := strings.ToLower(failure)
	if !strings.Contains(lower, "429") && !strings.Contains(lower, "quota") && !strings.Contains(lower, "usage limit") && !strings.Contains(lower, "limit exhausted") {
		return time.Time{}, false
	}
	marker := "reset at "
	index := strings.Index(lower, marker)
	if index < 0 {
		return time.Time{}, false
	}
	text := strings.TrimSpace(failure[index+len(marker):])
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05", "2006-01-02 15:04"} {
		if parsed, err := time.Parse(layout, text); err == nil {
			return parsed.UTC(), true
		}
	}
	return time.Time{}, false
}

func writeHermesBoundaryWall(spec RunBoundarySpec, report *hermesBoundaryUsageReport) error {
	if report == nil {
		return nil
	}
	reset, ok := hermesQuotaReset(report.Failure)
	if !ok {
		return nil
	}
	record := map[string]any{
		"schema_version": 1,
		"run_id":         spec.RunID,
		"outcome":        "wall",
		"error":          report.Failure,
		"wall_wake_at":   reset.UTC().Format(time.RFC3339Nano),
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return fsx.AtomicWriteFile(filepath.Join(spec.RunDir, "execution.json"), data, 0o600)
}

func waitForHermesBoundaryAbsent(ctx context.Context, spec RunBoundarySpec) (HermesBoundaryObservation, error) {
	for {
		observation, err := InspectHermesRunBoundary(ctx, spec)
		if err != nil {
			return observation, err
		}
		if observation.Liveness == BoundaryAbsent {
			return observation, nil
		}
		select {
		case <-ctx.Done():
			return observation, ctx.Err()
		case <-time.After(hermesBoundaryPoll):
		}
	}
}

func stopHermesBoundaryAfterClientExit(spec RunBoundarySpec) error {
	ctx, cancel := context.WithTimeout(context.Background(), hermesStopTimeout)
	defer cancel()
	state, err := StopHermesRunBoundary(ctx, spec)
	if state == BoundaryAbsent {
		return nil
	}
	return err
}

// sendHeadlessHermesRunBoundary is the finite-only Hermes path. It shares no
// session, usage file, roster accounting, wall activity, or pane lifecycle with
// ordinary Werkrby. The caller still makes exactly one Harness.Send call.
func (h *Herdr) sendHeadlessHermesRunBoundary(ctx context.Context, key, prompt, staffName string, staff roster.Staff, cfg HarnessConfig, emit core.Emit) (string, bool, error) {
	spec, _ := runBoundaryFromContext(ctx)
	boundary, err := newHermesRunBoundary(h, spec, key, prompt, staff)
	if err != nil {
		return "", false, err
	}
	command := h.headlessHermesCommand
	if command == "" {
		command = hermesExecutable()
	}
	usagePath := filepath.Join(spec.RunDir, "hermes-usage.json")
	args := []string{"-z", prompt, "--usage-file", usagePath}
	args = append(args, staff.Args...)

	turnContext, cancel := context.WithTimeout(ctx, herdrWaitCeiling)
	turn := &herdrTurn{target: "headless:" + staffName, cancel: cancel, done: make(chan struct{}), headless: true}
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

	procContext, procCancel := context.WithCancel(turnContext)
	defer procCancel()
	cmd := exec.CommandContext(procContext, command, args...)
	cmd.Dir = cfg.Cwd
	cmd.Env = headlessChildEnv(os.Environ())
	configureBoundedHeadlessProcess(cmd)
	stderr := newTailBuffer(64 * 1024)
	cmd.Stderr = stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", false, err
	}
	turn.cmd = cmd
	if err := cmd.Start(); err != nil {
		startErr := fmt.Errorf("start finite headless Hermes: %w", err)
		emitHeadlessError(emit, "", startErr.Error())
		return "", false, startErr
	}
	if err := boundary.markClientStarted(); err != nil {
		procCancel()
		_ = cmd.Wait()
		return "", false, err
	}
	if emit != nil {
		emit(core.Event{Kind: core.EventStatus, Text: fmt.Sprintf("Finite: running disposable Hermes profile %s for run %d", boundary.meta.Profile, spec.RunID)})
	}

	tickerDone := make(chan struct{})
	tickerExited := make(chan struct{})
	go func() {
		defer close(tickerExited)
		interval := 30 * time.Second
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
					emit(core.Event{Kind: core.EventStatus, Text: fmt.Sprintf("Finite: Hermes run %d working (%s)...", spec.RunID, time.Since(start).Truncate(time.Second))})
				}
			}
		}
	}()

	var result bytes.Buffer
	reader := bufio.NewReader(stdout)
	for {
		line, readErr := reader.ReadBytes('\n')
		result.Write(line)
		if result.Len() > hermesResultCap {
			keep := result.Bytes()[result.Len()-hermesResultCap/2:]
			result.Reset()
			result.Write(keep)
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				procCancel()
			}
			break
		}
	}
	waitErr := cmd.Wait()
	close(tickerDone)
	<-tickerExited
	boundaryErr := boundary.closeBoundary()
	report, reportErr := readHermesBoundaryUsage(usagePath)
	if reportErr == nil {
		if err := writeHermesBoundaryUsage(spec, report); err != nil {
			boundaryErr = errors.Join(boundaryErr, err)
		}
		if err := writeHermesBoundaryWall(spec, report); err != nil {
			boundaryErr = errors.Join(boundaryErr, err)
		}
	}

	if turnContext.Err() != nil {
		stopErr := stopHermesBoundaryAfterClientExit(spec)
		if errors.Is(turnContext.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
			detail := fmt.Sprintf("finite Hermes run %d exceeded %s", spec.RunID, herdrWaitCeiling)
			emitHeadlessError(emit, "", detail)
			return "", false, errors.Join(fmt.Errorf("runner turn failed: %s", detail), stopErr, boundaryErr)
		}
		return "", false, errors.Join(turnContext.Err(), stopErr, boundaryErr)
	}

	observation, boundaryInspectErr := InspectHermesRunBoundary(ctx, spec)
	if reportErr != nil || report == nil || report.Failed || !report.Completed {
		if boundaryInspectErr == nil && observation.Liveness == BoundaryLive {
			boundaryInspectErr = stopHermesBoundaryAfterClientExit(spec)
		}
		detail := hermesHeadlessFailureDetail(nil, result.String(), stderr.String(), waitErr)
		if report != nil {
			detail = hermesHeadlessFailureDetail(&hermesUsageReport{
				SessionID: report.SessionID, Completed: report.Completed, Failed: report.Failed,
				TurnExitReason: report.TurnExitReason, Failure: report.Failure,
			}, result.String(), stderr.String(), waitErr)
		} else if reportErr != nil && waitErr == nil {
			detail = "hermes -z ended without a readable usage report: " + reportErr.Error()
		}
		emitHeadlessError(emit, "", detail)
		return "", false, errors.Join(fmt.Errorf("runner turn failed: %s", detail), boundaryInspectErr, boundaryErr)
	}
	if boundaryInspectErr == nil && observation.Liveness == BoundaryLive {
		observation, boundaryInspectErr = waitForHermesBoundaryAbsent(turnContext, spec)
	}
	if boundaryInspectErr != nil {
		stopErr := stopHermesBoundaryAfterClientExit(spec)
		detail := "finite Hermes provider-turn absence could not be proven"
		emitHeadlessError(emit, "", detail)
		return "", false, errors.Join(fmt.Errorf("runner turn failed: %s: %w", detail, boundaryInspectErr), stopErr, boundaryErr)
	}
	if observation.Identity == nil {
		detail := "finite Hermes completed without an exact provider-turn identity"
		emitHeadlessError(emit, "", detail)
		return "", false, fmt.Errorf("runner turn failed: %s", detail)
	}
	threadID := observation.Identity.SessionID
	if report.SessionID != threadID {
		detail := "finite Hermes usage report session does not match the exact admission"
		emitHeadlessError(emit, threadID, detail)
		return threadID, false, fmt.Errorf("runner turn failed: %s", detail)
	}
	if observation.Outcome != "completed" {
		detail := "finite Hermes admission ended with outcome " + observation.Outcome
		emitHeadlessError(emit, threadID, detail)
		return threadID, false, fmt.Errorf("runner turn failed: %s", detail)
	}
	if boundaryErr != nil {
		detail := "finite Hermes run metadata could not be finalized"
		emitHeadlessError(emit, threadID, detail)
		return threadID, false, errors.Join(fmt.Errorf("runner turn failed: %s", detail), boundaryErr)
	}
	h.rememberSession(boundary.sessionKey, threadID)
	finalText := strings.TrimSpace(result.String())
	if emit != nil {
		emit(core.Event{Kind: core.EventFinal, Text: finalText, FinalText: &finalText, ThreadID: threadID, Done: true, Execution: &core.ExecutionStatus{State: "finishing"}})
	}
	return threadID, false, nil
}
