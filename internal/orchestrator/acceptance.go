package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/agent0ai/spynel/internal/config"
)

// The acceptance gate (opt-in, pilot). A task whose front matter carries an
// acceptance block has its check run by Spynel itself when the implementation
// turn hands the task off (to review, or directly to done), instead of relying
// on the worker's own claims. Pass: the hand-off proceeds unchanged. Fail: the
// task returns to todo with the bounded check output, and the failed round
// spends the attempt. After the configured number of failed rounds the task
// still goes to the independent reviewer, who decides whether the check or the
// work is wrong. An infrastructure failure of the check itself (script
// missing, not executable, cannot start, timeout) is a platform problem: it is
// recorded loudly, spends nothing, and the hand-off proceeds as before, so a
// broken check can never stall the board. Tampering with a registered check is
// the one fail-closed case: the hand-off is refused until the registered
// content is restored.
//
// The worker runs as the same user as Spynel, so the pinned hash and the
// append-only registry make tampering evident, not impossible.

const (
	acceptanceField           = "acceptance"
	acceptanceRoundsField     = "acceptance_rounds"
	acceptanceOutputMaxLines  = 20
	acceptanceOutputMaxChars  = 700 // keep the tail; a progress entry is capped at 1000 runes
	acceptanceDefaultTimeoutS = 120
	acceptanceMaxTimeoutS     = 600
	acceptanceDefaultRounds   = 2
	acceptanceMaxRounds       = 5
)

// acceptanceRegistryFile resolves the append-only registration log. Tests
// redirect it with SPYJO_ACCEPTANCE_REGISTRY.
func acceptanceRegistryFile() string {
	if path := strings.TrimSpace(os.Getenv("SPYJO_ACCEPTANCE_REGISTRY")); path != "" {
		return path
	}
	home := os.Getenv("HOME")
	if home == "" {
		home = "/home/dajoey"
	}
	return filepath.Join(home, "ops", "logs", "acceptance-gate", "registry.jsonl")
}

type acceptanceSpec struct {
	command string
	argv    []string
	target  string // the script file the pinned sha256 describes
	sha256  string
	timeout time.Duration
	rounds  int
}

// acceptanceSpecFromDocument parses the acceptance block. The second result
// reports whether the document is armed with a gate at all; a present but
// malformed block is armed and invalid, because ambiguity can never disable
// the gate.
func acceptanceSpecFromDocument(document Document) (acceptanceSpec, bool, error) {
	raw, exists := document.FrontMatter[acceptanceField]
	if !exists || raw == nil {
		return acceptanceSpec{}, false, nil
	}
	malformed := func(reason string) (acceptanceSpec, bool, error) {
		return acceptanceSpec{}, true, errors.New("malformed acceptance block: " + reason)
	}
	block, ok := raw.(map[string]any)
	if !ok {
		return malformed("must be a YAML mapping of command, sha256, timeout_seconds and max_rounds")
	}
	command, _ := block["command"].(string)
	command = strings.TrimSpace(command)
	if command == "" {
		return malformed("command is missing")
	}
	// The authoring tool's paste template wraps the script in an interpreter
	// ("bash /abs/check.sh"); a bare executable path is equally valid. The
	// pinned sha256 always describes the script file itself.
	argv := strings.Fields(command)
	target := ""
	for _, token := range argv {
		if filepath.IsAbs(token) {
			target = token
			break
		}
	}
	if target == "" {
		return malformed("command must name the check script by absolute path (optionally behind an interpreter)")
	}
	sum, _ := block["sha256"].(string)
	sum = strings.ToLower(strings.TrimSpace(sum))
	if len(sum) != 64 || !allHex(sum) {
		return malformed("sha256 must be 64 hex characters")
	}
	spec := acceptanceSpec{
		command: command,
		argv:    argv,
		target:  target,
		sha256:  sum,
		timeout: acceptanceDefaultTimeoutS * time.Second,
		rounds:  acceptanceDefaultRounds,
	}
	if value, present := block["timeout_seconds"]; present {
		seconds := numberValue(value)
		if seconds < 1 {
			return malformed("timeout_seconds must be at least 1")
		}
		if seconds > acceptanceMaxTimeoutS {
			seconds = acceptanceMaxTimeoutS
		}
		spec.timeout = time.Duration(seconds) * time.Second
	}
	if value, present := block["max_rounds"]; present {
		rounds := numberValue(value)
		if rounds < 1 {
			return malformed("max_rounds must be at least 1")
		}
		if rounds > acceptanceMaxRounds {
			rounds = acceptanceMaxRounds
		}
		spec.rounds = rounds
	}
	return spec, true, nil
}

func allHex(value string) bool {
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

// registeredAcceptanceSHA returns the sha256 recorded last for the task in the
// append-only registry, and whether the task is registered at all. Damaged
// lines are skipped: the registry is an append-only log that a human tool
// writes; one bad line must not disable every other gate.
func registeredAcceptanceSHA(taskID string) (string, bool, error) {
	data, err := os.ReadFile(acceptanceRegistryFile())
	if err != nil {
		if os.IsNotExist(err) {
			return "", false, nil
		}
		return "", false, err
	}
	latest, found := "", false
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var entry struct {
			Task   string `json:"task"`
			TaskID string `json:"task_id"`
			SHA256 string `json:"sha256"`
		}
		if json.Unmarshal([]byte(line), &entry) != nil {
			continue
		}
		// The registry carries one JSON line per registration; the authoring
		// tool writes task_id, hand registrations may write task.
		registeredTask := strings.TrimSpace(entry.Task)
		if registeredTask == "" {
			registeredTask = strings.TrimSpace(entry.TaskID)
		}
		if registeredTask == taskID {
			latest, found = strings.ToLower(strings.TrimSpace(entry.SHA256)), true
		}
	}
	return latest, found, nil
}

func fileSHA256(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// trimAcceptanceOutput bounds what a failing check contributes to the task's
// Progress: the last lines (errors live at the end), capped in characters so
// the single progress entry survives the framework's own note cap.
func trimAcceptanceOutput(output []byte) string {
	text := strings.TrimSpace(strings.ReplaceAll(string(output), "\r\n", "\n"))
	lines := strings.Split(text, "\n")
	if len(lines) > acceptanceOutputMaxLines {
		lines = lines[len(lines)-acceptanceOutputMaxLines:]
	}
	joined := strings.Join(lines, " | ")
	if len(joined) > acceptanceOutputMaxChars {
		joined = joined[len(joined)-acceptanceOutputMaxChars:]
	}
	if joined == "" {
		joined = "(no output)"
	}
	return joined
}

// runAcceptanceGate enforces a pre-registered acceptance check at hand-off.
// It returns the possibly redirected status and path, whether it already moved
// the document (the caller must then return), and an error only for a
// condition recovery should retry (an unreadable document, or the manager's
// own context dying mid-check, which writes nothing and spends nothing).
func (m *Manager) runAcceptanceGate(ctx context.Context, base, name, status, path string) (string, string, bool, error) {
	document, err := ReadDocument(path)
	if err != nil {
		return status, path, false, m.skipUnreadableDocument(path, err)
	}
	taskID := documentID(document)
	spec, armed, specErr := acceptanceSpecFromDocument(document)
	registrySHA, registered, registryErr := registeredAcceptanceSHA(taskID)
	if !armed && !registered {
		// No gate anywhere: the hand-off is exactly what it is today.
		return status, path, false, nil
	}
	now := time.Now().UTC()
	if registryErr != nil {
		// The registry being unreadable is the gate's own plumbing, not the
		// worker's failure and not tampering evidence: fail open, loudly.
		m.acceptanceNote(path, now, fmt.Sprintf(
			"ACCEPTANCE CHECK INFRASTRUCTURE FAILURE (no attempt spent, hand-off accepted): the acceptance registry could not be read: %s",
			registryErr.Error()), nil)
		return status, path, false, nil
	}
	if registered && !armed {
		return m.acceptanceRefuse(path, base, name, now, fmt.Sprintf(
			"task %s is registered with an acceptance check but the document carries no acceptance block; restore the registered check", taskID))
	}
	if specErr != nil {
		return m.acceptanceRefuse(path, base, name, now, specErr.Error())
	}
	if !registered {
		return m.acceptanceRefuse(path, base, name, now,
			"the acceptance check is not registered in the acceptance-gate registry; register it before it can gate a hand-off")
	}
	if registrySHA != spec.sha256 {
		return m.acceptanceRefuse(path, base, name, now,
			"the front-matter sha256 does not match the sha256 registered for this task")
	}
	info, statErr := os.Stat(spec.target)
	if statErr != nil {
		if os.IsNotExist(statErr) {
			m.acceptanceNote(path, now, fmt.Sprintf(
				"ACCEPTANCE CHECK INFRASTRUCTURE FAILURE (no attempt spent, hand-off accepted): the check script %s does not exist", spec.target), nil)
			return status, path, false, nil
		}
		m.acceptanceNote(path, now, fmt.Sprintf(
			"ACCEPTANCE CHECK INFRASTRUCTURE FAILURE (no attempt spent, hand-off accepted): the check script could not be inspected: %s", statErr.Error()), nil)
		return status, path, false, nil
	}
	if info.IsDir() || (spec.target == spec.argv[0] && info.Mode()&0o111 == 0) {
		m.acceptanceNote(path, now, fmt.Sprintf(
			"ACCEPTANCE CHECK INFRASTRUCTURE FAILURE (no attempt spent, hand-off accepted): the check script %s is not executable", spec.target), nil)
		return status, path, false, nil
	}
	actualSHA, hashErr := fileSHA256(spec.target)
	if hashErr != nil {
		m.acceptanceNote(path, now, fmt.Sprintf(
			"ACCEPTANCE CHECK INFRASTRUCTURE FAILURE (no attempt spent, hand-off accepted): the check script could not be read: %s", hashErr.Error()), nil)
		return status, path, false, nil
	}
	if actualSHA != spec.sha256 {
		return m.acceptanceRefuse(path, base, name, now,
			"the check script's content does not match its pinned sha256; restore the registered content")
	}

	runCtx, cancel := context.WithTimeout(ctx, spec.timeout)
	defer cancel()
	cmd := exec.CommandContext(runCtx, spec.argv[0], spec.argv[1:]...)
	cmd.WaitDelay = 2 * time.Second
	var output strings.Builder
	cmd.Stdout = &output
	cmd.Stderr = &output
	runErr := cmd.Run()
	if ctx.Err() != nil {
		// A service restart interrupted the check: write nothing, count
		// nothing, and let recovery re-run the transition from scratch.
		return status, path, false, ctx.Err()
	}
	if runCtx.Err() != nil {
		// The check's own deadline fired (a killed script and its error are
		// indistinguishable from a failing exit): that is the gate's plumbing,
		// not the worker's failure.
		m.acceptanceNote(path, now, fmt.Sprintf(
			"ACCEPTANCE CHECK INFRASTRUCTURE FAILURE (no attempt spent, hand-off accepted): the check timed out after %s", spec.timeout), nil)
		return status, path, false, nil
	}
	var exitErr *exec.ExitError
	if runErr != nil && !errors.As(runErr, &exitErr) {
		m.acceptanceNote(path, now, fmt.Sprintf(
			"ACCEPTANCE CHECK INFRASTRUCTURE FAILURE (no attempt spent, hand-off accepted): the check could not be run: %s", runErr.Error()), nil)
		return status, path, false, nil
	}
	if runErr == nil {
		m.acceptanceNote(path, now, "ACCEPTANCE CHECK PASSED: the registered check exited 0; hand-off accepted.", nil)
		return status, path, false, nil
	}

	round := numberValue(document.FrontMatter[acceptanceRoundsField]) + 1
	trimmed := trimAcceptanceOutput([]byte(output.String()))
	recordRound := func(frontMatter map[string]any) {
		frontMatter[acceptanceRoundsField] = round
	}
	if round < spec.rounds {
		note := fmt.Sprintf(
			"ACCEPTANCE CHECK FAILED (round %d of %d): %s. Output: %s. Fix the work (or the check, through its owner) and hand off again; a failed round spends the attempt.",
			round, spec.rounds, runErr.Error(), trimmed)
		return m.acceptanceBounce(path, base, name, now, note, recordRound)
	}
	note := fmt.Sprintf(
		"ACCEPTANCE CHECK FAILED (round %d of %d): %s. The independent reviewer decides whether the acceptance check or the work is wrong; earlier rounds are in Progress above. Output: %s",
		round, spec.rounds, runErr.Error(), trimmed)
	if status == "done" && m.harnessSettings().Reviews != config.TaskReviewsNever {
		// A direct completion whose check keeps failing goes to the
		// independent reviewer instead of completing on its own claim.
		return m.acceptanceRedirectToReview(path, base, name, now, note, recordRound)
	}
	m.acceptanceNote(path, now, note, recordRound)
	return status, path, false, nil
}

// acceptanceNote appends one progress entry, optionally updating front matter
// in the same locked write.
func (m *Manager) acceptanceNote(path string, now time.Time, note string, update func(map[string]any)) {
	if err := updateDocumentProgressWithUpdate(path, now, note, update); err != nil {
		m.log("acceptance gate progress: " + err.Error())
	}
}

// acceptanceBounce returns a gated hand-off to todo. No resume credit: a
// failed acceptance round is a graded failure, so the next claim spends the
// attempt.
func (m *Manager) acceptanceBounce(path, base, name string, now time.Time, note string, update func(map[string]any)) (string, string, bool, error) {
	target := statusPath(base, "todo", name)
	if err := moveDocumentWithUpdate(path, target, "todo", now, note, update); err != nil {
		return "todo", path, false, err
	}
	m.log(note + " " + target)
	return "todo", target, true, nil
}

// acceptanceRefuse refuses a tampered hand-off. Tampering never counts as a
// check round and never passes, however often it is retried.
func (m *Manager) acceptanceRefuse(path, base, name string, now time.Time, reason string) (string, string, bool, error) {
	note := fmt.Sprintf(
		"ACCEPTANCE CHECK TAMPERED: %s. Spynel refused this hand-off; the check is never run and never passes while it is tampered.",
		reason)
	return m.acceptanceBounce(path, base, name, now, note, nil)
}

// acceptanceRedirectToReview moves a directly-completing task whose final
// round failed into the ordinary review queue.
func (m *Manager) acceptanceRedirectToReview(path, base, name string, now time.Time, note string, update func(map[string]any)) (string, string, bool, error) {
	target := statusPath(base, "review", name)
	if err := moveDocumentWithUpdate(path, target, "review", now, note, update); err != nil {
		return "review", path, false, err
	}
	m.log(note + " " + target)
	return "review", target, true, nil
}
