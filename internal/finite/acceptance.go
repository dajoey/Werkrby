package finite

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	acceptanceVerdictPass           = "pass"
	acceptanceVerdictFail           = "fail"
	acceptanceVerdictVerifierFailed = "verifier_failed"

	acceptanceDefaultTimeoutSeconds = 120
	acceptanceMaxTimeoutSeconds     = 1800

	// acceptanceOutputWindow bounds how much check output is read back for
	// the tail, so a runaway command cannot balloon the record.
	acceptanceOutputWindow = 64 * 1024
	acceptanceTailLines    = 20
	acceptanceTailBytes    = 700
	acceptanceCleanupBound = 30 * time.Second
	acceptanceMetaMode     = 0o600

	// acceptanceShell runs every declared check command.
	acceptanceShell = "/bin/sh"
)

// acceptanceEventID is the deterministic event id of one run's shadow
// acceptance verdict, so the once-per-run record is idempotent.
func acceptanceEventID(runID int64) string {
	return "acceptance:" + strconv.FormatInt(runID, 10)
}

// acceptanceCheckResult is the outcome of one acceptance check run.
type acceptanceCheckResult struct {
	verdict    string
	reason     string
	exitCode   int
	commit     string
	durationMS int64
	outputTail string
}

// acceptancePayload is the typed verdict recorded as one task event.
type acceptancePayload struct {
	SchemaVersion int    `json:"schema_version"`
	RunID         int64  `json:"run_id"`
	Verdict       string `json:"verdict"`
	Reason        string `json:"reason"`
	ExitCode      int    `json:"exit_code"`
	Commit        string `json:"commit"`
	DurationMS    int64  `json:"duration_ms"`
	OutputTail    string `json:"output_tail"`
}

// acceptanceMeta is the leftover-cleanup contract of one check directory.
type acceptanceMeta struct {
	Repo     string `json:"repo"`
	Worktree string `json:"worktree"`
}

// logError reports an engine-side operational problem to serve's error
// channel; it never changes workflow state.
func (e *Engine) logError(err error) {
	if err != nil && e.opts.OnError != nil {
		e.opts.OnError(err)
	}
}

// settledResultOutcome reads the result outcome of a run's settled event.
func (s *Store) settledResultOutcome(ctx context.Context, runID int64) (string, error) {
	var payload string
	err := s.db.QueryRowContext(ctx,
		`SELECT payload_json FROM task_events WHERE run_id=? AND kind='settled'`, runID).Scan(&payload)
	if err != nil {
		return "", err
	}
	var audit struct {
		ResultOutcome string `json:"result_outcome"`
	}
	if err := json.Unmarshal([]byte(payload), &audit); err != nil {
		return "", fmt.Errorf("finite: parse settled event of run %d: %w", runID, err)
	}
	return audit.ResultOutcome, nil
}

// loadBriefReadOnly parses the brief file without any state effect. It never
// calls verifiedBrief or FailTask: the acceptance step is shadow only and may
// never fail a task over its brief.
func (e *Engine) loadBriefReadOnly(task Task) (Brief, error) {
	data, err := os.ReadFile(task.BriefPath)
	if err != nil {
		return Brief{}, err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(data))
	if digest != task.BriefSHA256 {
		return Brief{}, fmt.Errorf("brief sha256 %s does not match the intake digest %s", digest, task.BriefSHA256)
	}
	return ParseBrief(data)
}

// maybeStartAcceptance starts the declared acceptance check for a settled
// implementation run whose result outcome is done or review. Every problem is
// one serve log line and no check; it never fails the task.
func (e *Engine) maybeStartAcceptance(ctx context.Context, taskID string, runID int64) {
	run, err := e.store.GetRun(ctx, runID)
	if err != nil {
		e.logError(fmt.Errorf("finite: task %s: run %d: read run for acceptance: %w", taskID, runID, err))
		return
	}
	if run.Phase != PhaseImplementation {
		return
	}
	outcome, err := e.store.settledResultOutcome(ctx, runID)
	if err != nil {
		e.logError(fmt.Errorf("finite: task %s: run %d: read settled outcome for acceptance: %v", taskID, runID, err))
		return
	}
	if outcome != string(ResultDone) && outcome != string(ResultReview) {
		return
	}
	task, err := e.store.GetTask(ctx, taskID)
	if err != nil {
		e.logError(fmt.Errorf("finite: task %s: run %d: read task for acceptance: %w", taskID, runID, err))
		return
	}
	brief, err := e.loadBriefReadOnly(task)
	if err != nil {
		e.logError(fmt.Errorf("finite: task %s: run %d: read brief for acceptance: %v", taskID, runID, err))
		return
	}
	if brief.AcceptanceCheck == nil {
		return
	}
	e.startAcceptance(task.ID, runID, *brief.AcceptanceCheck)
}

// startAcceptance runs one declared acceptance check detached from the
// worker's run and records its typed verdict. Exactly one check per run: an
// in-flight check or an already recorded verdict starts nothing and calls no
// hook. The hook, when set, is called exactly once per started check.
func (e *Engine) startAcceptance(taskID string, runID int64, check AcceptanceCheck) {
	e.acceptanceMu.Lock()
	if e.acceptanceStopped {
		e.acceptanceMu.Unlock()
		return
	}
	if _, inFlight := e.acceptanceInflight[runID]; inFlight {
		e.acceptanceMu.Unlock()
		return
	}
	if _, exists, err := e.store.AcceptanceEvent(context.Background(), runID); err != nil {
		e.logError(fmt.Errorf("finite: task %s: run %d: read acceptance event: %w", taskID, runID, err))
		e.acceptanceMu.Unlock()
		return
	} else if exists {
		e.acceptanceMu.Unlock()
		return
	}
	e.acceptanceInflight[runID] = struct{}{}
	e.acceptanceWG.Add(1)
	e.acceptanceMu.Unlock()

	go func() {
		defer e.acceptanceWG.Done()
		defer func() {
			e.acceptanceMu.Lock()
			delete(e.acceptanceInflight, runID)
			e.acceptanceMu.Unlock()
		}()
		e.runAcceptance(e.acceptanceCtx, taskID, runID, check)
	}()
}

// runAcceptance runs the check, records the verdict unless the engine is
// closing, and calls the test hook exactly once.
func (e *Engine) runAcceptance(ctx context.Context, taskID string, runID int64, check AcceptanceCheck) {
	result := runAcceptanceCheck(ctx, e.cfg.Root, runID, check, e.logError)
	recorded := false
	if ctx.Err() == nil {
		payload, err := json.Marshal(acceptancePayload{
			SchemaVersion: 1, RunID: runID, Verdict: result.verdict, Reason: result.reason,
			ExitCode: result.exitCode, Commit: result.commit,
			DurationMS: result.durationMS, OutputTail: result.outputTail,
		})
		if err != nil {
			e.logError(fmt.Errorf("finite: task %s: run %d: encode acceptance verdict: %v", taskID, runID, err))
		} else if inserted, recordErr := e.store.RecordAcceptance(ctx, taskID, runID, payload); recordErr != nil {
			e.logError(fmt.Errorf("finite: task %s: run %d: record acceptance verdict: %v", taskID, runID, recordErr))
		} else {
			recorded = inserted
		}
	}
	if e.opts.hooks.afterAcceptance != nil {
		e.opts.hooks.afterAcceptance(runID, recorded)
	}
}

// runAcceptanceCheck executes the declared check in a detached worktree of
// the declared repo, outside the worker's run. Exit 0 is a pass; any other
// exit is a fail; every broken prerequisite is a verifier failure with the
// step's reason. Cleanup runs after every path that created the directory. A
// cancelled context yields no verdict.
func runAcceptanceCheck(ctx context.Context, root string, runID int64, check AcceptanceCheck, log func(error)) acceptanceCheckResult {
	if !acceptancePlatformSupported {
		return acceptanceCheckResult{
			verdict: acceptanceVerdictVerifierFailed, reason: "unsupported platform", exitCode: -1,
		}
	}
	// Step 1: the check's own directory, private to the owner.
	dir := filepath.Join(root, "acceptance", strconv.FormatInt(runID, 10))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return acceptanceCheckResult{
			verdict: acceptanceVerdictVerifierFailed, reason: "dir", exitCode: -1,
		}
	}
	result, worktreeAdded := executeAcceptanceSteps(ctx, dir, check)
	// Step 7: cleanup always runs, on a fresh bound context, and never
	// changes the verdict.
	if err := cleanupAcceptanceWorktree(check.Repo, dir, filepath.Join(dir, "wt"), worktreeAdded); err != nil {
		log(fmt.Errorf("finite: acceptance cleanup for run %d: %w", runID, err))
	}
	return result
}

// executeAcceptanceSteps runs steps 2 through 6 of one check and reports
// whether its worktree was added.
func executeAcceptanceSteps(ctx context.Context, dir string, check AcceptanceCheck) (acceptanceCheckResult, bool) {
	verifierFailed := func(reason string) acceptanceCheckResult {
		return acceptanceCheckResult{
			verdict: acceptanceVerdictVerifierFailed, reason: reason, exitCode: -1,
		}
	}
	// Step 2: the repo must be a git repo with a commit.
	commit := ""
	head, err := exec.Command("git", "-C", check.Repo, "rev-parse", "--verify", "HEAD").Output()
	if err == nil {
		commit = strings.TrimSpace(string(head))
	}
	if len(commit) != 40 || !isHex(commit) {
		return verifierFailed("repo"), false
	}
	started := time.Now()
	duration := func() int64 { return time.Since(started).Milliseconds() }
	// Step 3: the meta for startup cleanup, written before the worktree so a
	// crash can always tell whether a worktree exists.
	if err := writeAcceptanceMeta(filepath.Join(dir, "meta.json"), acceptanceMeta{
		Repo: check.Repo, Worktree: filepath.Join(dir, "wt"),
	}); err != nil {
		result := verifierFailed("dir")
		result.durationMS = duration()
		return result, false
	}
	// Step 4: a detached worktree at the verified commit.
	worktree := filepath.Join(dir, "wt")
	if out, err := exec.Command("git", "-C", check.Repo, "worktree", "add", "--detach", worktree, commit).CombinedOutput(); err != nil {
		result := verifierFailed("worktree")
		result.outputTail = acceptanceTail(string(out))
		result.durationMS = duration()
		return result, false
	}

	// Steps 5-6: the command, in its own process group, killed whole-group on
	// timeout or engine close, and once more after it ends so no background
	// child survives.
	outputPath := filepath.Join(dir, "output.log")
	output, openErr := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, acceptanceMetaMode)
	if openErr != nil {
		result := verifierFailed("start")
		result.durationMS = duration()
		return result, true
	}
	defer output.Close()
	command := exec.Command(acceptanceShell, "-c", check.Command)
	command.Dir = worktree
	command.Stdout = output
	command.Stderr = output
	command.Env = acceptanceEnvironment(commit)
	command.SysProcAttr = acceptanceSysProcAttr()
	command.WaitDelay = 5 * time.Second
	if err := command.Start(); err != nil {
		result := verifierFailed("start")
		result.durationMS = duration()
		return result, true
	}
	pgid := command.Process.Pid
	timedOut := false
	waitDone := make(chan error, 1)
	go func() { waitDone <- command.Wait() }()
	timer := time.NewTimer(time.Duration(check.TimeoutSeconds) * time.Second)
	defer timer.Stop()
	waitErr := func() error {
		for {
			select {
			case err := <-waitDone:
				return err
			case <-timer.C:
				timedOut = true
				_ = signalProcessGroup(pgid, true)
			case <-ctx.Done():
				_ = signalProcessGroup(pgid, true)
				<-waitDone
				return ctx.Err()
			}
		}
	}()
	// The command ended for any reason: kill the whole group once more.
	_ = signalProcessGroup(pgid, true)
	tail := acceptanceFileTail(outputPath)
	if ctx.Err() != nil {
		// The engine is closing: no verdict for an interrupted check.
		return acceptanceCheckResult{exitCode: -1}, true
	}
	result := acceptanceCheckResult{commit: commit, durationMS: duration(), outputTail: tail}
	switch {
	case timedOut:
		result.verdict, result.reason, result.exitCode = acceptanceVerdictVerifierFailed, "timeout", -1
	case waitErr == nil:
		result.verdict, result.reason, result.exitCode = acceptanceVerdictPass, "", 0
	default:
		var exitErr *exec.ExitError
		if errors.As(waitErr, &exitErr) {
			result.verdict, result.reason, result.exitCode = acceptanceVerdictFail, "", exitErr.ExitCode()
		} else {
			result.verdict, result.reason, result.exitCode = acceptanceVerdictVerifierFailed, "start", -1
		}
	}
	return result, true
}

func acceptanceEnvironment(commit string) []string {
	return []string{
		"PATH=/usr/local/bin:/usr/bin:/bin",
		"HOME=" + os.Getenv("HOME"),
		"LANG=C.UTF-8",
		"WERKRBY_ACCEPTANCE_COMMIT=" + commit,
	}
}

func isHex(text string) bool {
	for _, r := range strings.ToLower(text) {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}

// writeAcceptanceMeta publishes the cleanup contract atomically.
func writeAcceptanceMeta(path string, meta acceptanceMeta) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, data, acceptanceMetaMode); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func readAcceptanceMeta(path string) (acceptanceMeta, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return acceptanceMeta{}, err
	}
	var meta acceptanceMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return acceptanceMeta{}, err
	}
	if meta.Repo == "" || meta.Worktree == "" {
		return acceptanceMeta{}, fmt.Errorf("finite: acceptance meta lacks repo or worktree")
	}
	return meta, nil
}

// acceptanceTail reduces check output to its last 20 lines, then its last 700
// bytes, cut forward to a UTF-8 boundary.
func acceptanceTail(output string) string {
	lines := strings.Split(output, "\n")
	if len(lines) > acceptanceTailLines {
		lines = lines[len(lines)-acceptanceTailLines:]
	}
	tail := strings.Join(lines, "\n")
	if len(tail) > acceptanceTailBytes {
		tail = tail[len(tail)-acceptanceTailBytes:]
		for len(tail) > 0 && !utf8.RuneStart(tail[0]) {
			tail = tail[1:]
		}
	}
	return tail
}

// acceptanceFileTail reads the bounded tail of the check's output file.
func acceptanceFileTail(path string) string {
	file, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return ""
	}
	if info.Size() > acceptanceOutputWindow {
		if _, err := file.Seek(info.Size()-acceptanceOutputWindow, io.SeekStart); err != nil {
			return ""
		}
	}
	data, err := io.ReadAll(io.LimitReader(file, acceptanceOutputWindow))
	if err != nil && len(data) == 0 {
		return ""
	}
	return acceptanceTail(string(data))
}

// cleanupAcceptanceWorktree removes the detached worktree, prunes the repo's
// worktree records, and deletes the check directory.
func cleanupAcceptanceWorktree(repo, dir, worktree string, worktreeAdded bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), acceptanceCleanupBound)
	defer cancel()
	var errs []error
	if worktreeAdded {
		if out, err := exec.CommandContext(ctx, "git", "-C", repo, "worktree", "remove", "--force", worktree).CombinedOutput(); err != nil && ctx.Err() == nil {
			errs = append(errs, fmt.Errorf("git worktree remove %s: %v: %s", worktree, err, strings.TrimSpace(string(out))))
		}
		if out, err := exec.CommandContext(ctx, "git", "-C", repo, "worktree", "prune").CombinedOutput(); err != nil && ctx.Err() == nil {
			errs = append(errs, fmt.Errorf("git worktree prune: %v: %s", err, strings.TrimSpace(string(out))))
		}
	}
	if err := os.RemoveAll(dir); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// cleanupAcceptanceLeftovers removes state a crashed acceptance check left
// behind: a directory with no meta never held a worktree and is deleted; a
// directory with a meta gets its stray processes killed and its worktree and
// directory removed. No verdict is written for an interrupted check and
// nothing retries it; a missing verdict means unavailable, never fail.
func cleanupAcceptanceLeftovers(ctx context.Context, root string) error {
	base := filepath.Join(root, "acceptance")
	entries, err := os.ReadDir(base)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var errs []error
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dir := filepath.Join(base, entry.Name())
		meta, metaErr := readAcceptanceMeta(filepath.Join(dir, "meta.json"))
		if metaErr != nil {
			// The check died before step 4: no worktree was ever added.
			if removeErr := os.RemoveAll(dir); removeErr != nil {
				errs = append(errs, removeErr)
			}
			continue
		}
		killAcceptanceWorktreeProcesses(meta.Worktree)
		if err := cleanupAcceptanceWorktree(meta.Repo, dir, meta.Worktree, true); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
