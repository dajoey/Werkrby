package orchestrator

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/agent0ai/spynel/internal/roster"
)

// Explicit turn outcomes. On a roster seat with `explicit_outcome: true` (headless only), a task
// implementation or review turn does not write its task document: the worker returns the outcome as
// data, in result.json in its run directory or a werkrby-outcome block in its final reply, and the
// owner reads it after the worker process exits, validates it, applies the front matter and moves
// the file under the document lock. A missing or invalid outcome returns the task to todo with the
// reason in Progress. Every other seat, and every goal phase, keeps the file-move contract.

const (
	turnResultFileName      = "result.json"
	outcomeFenceInfo        = "werkrby-outcome"
	maxTurnOutcomeBytes     = 256 << 10
	maxOutcomeProgressRunes = 16000
)

var errNoTurnOutcome = errors.New("no outcome returned (no result.json in the run directory and no " + outcomeFenceInfo + " block in the final reply)")

// turnOutcome is the JSON a worker on an explicit-outcome seat returns at the end of its turn.
type turnOutcome struct {
	Outcome     string         `json:"outcome"`
	Progress    string         `json:"progress"`
	Reason      string         `json:"reason"`
	FrontMatter map[string]any `json:"front_matter"`
	Drop        []string       `json:"drop"`
}

// outcomeTransition is a validated outcome: the status folder it moves the task to and the edits
// Werkrby makes in the same locked write.
type outcomeTransition struct {
	Outcome  string
	Status   string
	Progress string
	Reason   string
	Set      map[string]any
	Drop     []string
	Ignored  []string
}

// workerOutcomeKeys are the front matter keys a worker may set or drop through its outcome, with the
// value shape each needs. Every other key is owned by Werkrby or another writer and is ignored.
var workerOutcomeKeys = map[string]string{
	"completion_summary":    "mapping",
	"wake_at":               "time",
	"waiting_for":           "string",
	"joey_ask":              "mapping",
	"wake_check":            "string",
	"wake_check_interval_s": "number",
	"risk":                  "string",
	"staff":                 "string",
	"cancelled_reason":      "string",
	"ffxiv_build":           "bool",
}

// phaseOutcomes maps each outcome a task phase may return to the status folder it moves to.
var phaseOutcomes = map[string]map[string]string{
	phaseTaskImplementation: {
		"done": "done", "review": "review", "todo": "todo", "park": "waiting", "ask_joey": "waiting",
		"failed": "failed", "blocked": "failed", "cancelled": "cancelled",
	},
	phaseTaskReview: {
		"accepted": "done", "done": "done", "rejected": "todo", "todo": "todo",
		"park": "waiting", "ask_joey": "waiting", "cancelled": "cancelled",
	},
}

// explicitTurn is one dispatch on an explicit-outcome seat: where its worker writes the outcome.
type explicitTurn struct {
	Staff      string
	Phase      string
	Dir        string
	ResultPath string
}

// explicitOutcomeTurn reports whether this dispatch runs on an explicit-outcome seat: a task
// implementation or review phase routed to roster staff with headless and explicit_outcome set.
func (m *Manager) explicitOutcomeTurn(route workflowRoute, lease Lease, targetModel string) *explicitTurn {
	if route.Name != "tasks" || lease.ID == "" {
		return nil
	}
	phase := normalizeLeasePhase(route.Name, lease.Phase)
	if _, ok := phaseOutcomes[phase]; !ok {
		return nil
	}
	model := strings.TrimSpace(targetModel)
	staff, ok, err := roster.ExpandStaff(m.Config.StatePath(), model)
	if !ok || err != nil || !staff.Headless || !staff.ExplicitOutcome {
		return nil
	}
	dir := m.Config.StatePath("runtime", "turns", lease.ID)
	return &explicitTurn{
		Staff: strings.TrimPrefix(model, roster.StaffPrefix), Phase: phase,
		Dir: dir, ResultPath: filepath.Join(dir, turnResultFileName),
	}
}

// prepare creates the run directory and removes an outcome left by an earlier turn of this lease.
func (t *explicitTurn) prepare() error {
	if err := os.MkdirAll(t.Dir, 0o700); err != nil {
		return err
	}
	if err := os.Remove(t.ResultPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (t *explicitTurn) phaseLabel() string {
	if t.Phase == phaseTaskReview {
		return "review"
	}
	return "implementation"
}

// contract is the framework-owned turn-end section appended after every other prompt section on an
// explicit-outcome seat. It replaces the file-edit and file-move steps of the templates and of the
// workspace instructions.
func (t *explicitTurn) contract(file string) string {
	keys := make([]string, 0, len(workerOutcomeKeys))
	for key := range workerOutcomeKeys {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for index, key := range keys {
		keys[index] = "`" + key + "`"
	}
	var b strings.Builder
	b.WriteString("## Turn end on this seat: return your outcome as data\n\n")
	b.WriteString("On this seat Werkrby writes the task document. This section replaces every instruction above, and in any workspace instruction or playbook, that tells you to set front matter, run `spyjo_frontmatter.py set` or `check`, append to `## Progress` in the task file, or move the task file.\n\n")
	fmt.Fprintf(&b, "- Read `%s` as often as you need. Do not edit, move, rename or delete it.\n", file)
	fmt.Fprintf(&b, "- As the last action of your turn, write one JSON object to `%s` (the folder exists; replace the file if it is there). Werkrby reads it after your process exits, validates it, adds your progress entry with the exact UTC time, sets `status` and `updated_at`, applies your front matter and moves the file.\n", t.ResultPath)
	fmt.Fprintf(&b, "- If you cannot write that file, end your final reply with the same JSON object in a fenced code block whose info string is `%s`. The file wins when both exist.\n", outcomeFenceInfo)
	b.WriteString("- A missing or invalid outcome returns the task to `todo` with the reason in `## Progress`, and the attempt is spent.\n\n")
	b.WriteString("Fields:\n\n")
	b.WriteString("- `outcome` (required): one of the outcomes listed below.\n")
	b.WriteString("- `progress` (required): your `## Progress` entry in Markdown: what you did, the evidence and verification, and for a park, failure or cancellation the exact condition, reason and next action. Add no timestamp.\n")
	b.WriteString("- `reason`: required for `failed`, `blocked` and `cancelled`; one or two plain sentences.\n")
	fmt.Fprintf(&b, "- `front_matter` (optional): an object of keys to set. Allowed keys: %s. `wake_at` is an RFC 3339 UTC time, `wake_check_interval_s` a number, `ffxiv_build` a boolean, `completion_summary` and `joey_ask` objects. Werkrby ignores any other key and names it in Progress; it owns `status`, `updated_at`, the attempt counters, and the `completed_at` and `reviewed_at` stamps inside `completion_summary`.\n", strings.Join(keys, ", "))
	b.WriteString("- `drop` (optional): a list of allowed keys to remove.\n\n")
	fmt.Fprintf(&b, "Outcomes for this %s turn:\n\n", t.phaseLabel())
	if t.Phase == phaseTaskReview {
		b.WriteString("- `accepted`: every requirement is verified. Send `completion_summary` with `verdict: \"accepted\"`, `outcome` and `evidence`.\n")
		b.WriteString("- `rejected`: findings go back to implementation. List every finding in `progress` and send `completion_summary` with `verdict: \"rejected\"`, `outcome` and `evidence`.\n")
		b.WriteString("- `park` or `ask_joey`: the work is accepted but still needs a confirmation only a person can give. Send `completion_summary` with `verdict: \"accepted\"`, plus `wake_at` (for `park`) or `joey_ask` (for `ask_joey`), and `waiting_for`.\n")
		b.WriteString("- `cancelled`: an operator stopped the task; needs `reason`.\n\n")
		b.WriteString("Example:\n\n```json\n{\"outcome\": \"accepted\", \"progress\": \"Re-ran the acceptance check and the unit tests; both pass. The change matches every criterion.\", \"front_matter\": {\"completion_summary\": {\"verdict\": \"accepted\", \"outcome\": \"Fix verified\", \"evidence\": \"Acceptance check and unit tests pass\"}}}\n```\n")
		return b.String()
	}
	b.WriteString("- `review`: the work is complete and goes to independent review.\n")
	b.WriteString("- `done`: the work is complete. Werkrby still sends it to review when review is required. A direct completion needs `completion_summary` with `verdict: \"completed\"`, `outcome`, `evidence` and `uncertainty`; Werkrby returns one that breaks the direct-completion rules to `todo` and names the rule.\n")
	b.WriteString("- `todo`: back to the queue, for example a hand-off that sets `staff`.\n")
	b.WriteString("- `park`: wait on a time. Needs `wake_at` (a future time) and `waiting_for`; add `wake_check` when the wait is for a host, a login or a service.\n")
	b.WriteString("- `ask_joey`: wait on Joey's decision. Needs `joey_ask` (the whole ask as an object) and `waiting_for`.\n")
	b.WriteString("- `failed` or `blocked`: evidenced inability to finish; needs `reason`.\n")
	b.WriteString("- `cancelled`: an explicit stop decision; needs `reason`.\n\n")
	b.WriteString("Example:\n\n```json\n{\"outcome\": \"review\", \"progress\": \"Fixed the parser bug in internal/parse/reader.go. go test ./... passes, including the new failing-first test.\", \"front_matter\": {\"risk\": \"medium\"}}\n```\n")
	return b.String()
}

// parseTurnOutcome decodes a worker's outcome. A single surrounding code fence is tolerated.
func parseTurnOutcome(data []byte) (turnOutcome, error) {
	text := strings.TrimSpace(string(data))
	if strings.HasPrefix(text, "```") {
		if newline := strings.IndexByte(text, '\n'); newline >= 0 {
			text = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(text[newline+1:]), "```"))
		}
	}
	var outcome turnOutcome
	if err := json.Unmarshal([]byte(text), &outcome); err != nil {
		return turnOutcome{}, fmt.Errorf("the outcome is not one JSON object: %v", err)
	}
	return outcome, nil
}

// extractOutcomeBlock returns the last werkrby-outcome fenced block in a final reply.
func extractOutcomeBlock(finalText string) ([]byte, bool) {
	opener := "```" + outcomeFenceInfo
	start := strings.LastIndex(finalText, opener)
	if start < 0 {
		return nil, false
	}
	rest := finalText[start+len(opener):]
	newline := strings.IndexByte(rest, '\n')
	if newline < 0 {
		return nil, false
	}
	rest = rest[newline+1:]
	end := strings.Index(rest, "```")
	if end < 0 {
		return nil, false
	}
	return []byte(rest[:end]), true
}

// readTurnOutcome reads the outcome a turn returned: result.json first, then the final reply.
func readTurnOutcome(resultPath, finalText string) (turnOutcome, string, error) {
	info, err := os.Stat(resultPath)
	switch {
	case err == nil && info.Size() > maxTurnOutcomeBytes:
		return turnOutcome{}, turnResultFileName, fmt.Errorf("%s is larger than %d bytes", turnResultFileName, maxTurnOutcomeBytes)
	case err == nil:
		data, readErr := os.ReadFile(resultPath)
		if readErr != nil {
			return turnOutcome{}, turnResultFileName, readErr
		}
		outcome, parseErr := parseTurnOutcome(data)
		return outcome, turnResultFileName, parseErr
	case !os.IsNotExist(err):
		return turnOutcome{}, turnResultFileName, err
	}
	if block, ok := extractOutcomeBlock(finalText); ok {
		outcome, parseErr := parseTurnOutcome(block)
		return outcome, "final reply", parseErr
	}
	return turnOutcome{}, "", errNoTurnOutcome
}

// validateTurnOutcome checks an outcome against what its phase may return and the shape of every
// allowed front matter key. Keys a worker may not set are not applied; they are listed in Ignored.
func validateTurnOutcome(outcome turnOutcome, phase string) (outcomeTransition, error) {
	allowed, ok := phaseOutcomes[phase]
	if !ok {
		return outcomeTransition{}, fmt.Errorf("phase %s does not take an explicit outcome", phase)
	}
	name := strings.ToLower(strings.TrimSpace(outcome.Outcome))
	if name == "" {
		return outcomeTransition{}, errors.New("outcome is required")
	}
	status, ok := allowed[name]
	if !ok {
		names := make([]string, 0, len(allowed))
		for key := range allowed {
			names = append(names, key)
		}
		sort.Strings(names)
		return outcomeTransition{}, fmt.Errorf("outcome %q is not allowed in this turn; use one of %s", outcome.Outcome, strings.Join(names, ", "))
	}
	if strings.TrimSpace(outcome.Progress) == "" {
		return outcomeTransition{}, errors.New("progress is required")
	}
	transition := outcomeTransition{
		Outcome: name, Status: status, Progress: strings.TrimSpace(outcome.Progress),
		Reason: strings.TrimSpace(outcome.Reason), Set: map[string]any{},
	}
	ignored := map[string]bool{}
	for key, value := range outcome.FrontMatter {
		shape, known := workerOutcomeKeys[key]
		if !known {
			ignored[key] = true
			continue
		}
		normalized, err := checkOutcomeValue(key, shape, value)
		if err != nil {
			return outcomeTransition{}, err
		}
		transition.Set[key] = normalized
	}
	for _, key := range outcome.Drop {
		if _, known := workerOutcomeKeys[key]; known {
			transition.Drop = append(transition.Drop, key)
		} else {
			ignored[key] = true
		}
	}
	sort.Strings(transition.Drop)
	for key := range ignored {
		transition.Ignored = append(transition.Ignored, key)
	}
	sort.Strings(transition.Ignored)
	switch name {
	case "park":
		if _, ok := transition.Set["wake_at"]; !ok {
			return outcomeTransition{}, errors.New("outcome park needs front_matter.wake_at (a future RFC 3339 UTC time)")
		}
	case "ask_joey":
		if ask, _ := transition.Set["joey_ask"].(map[string]any); len(ask) == 0 {
			return outcomeTransition{}, errors.New("outcome ask_joey needs front_matter.joey_ask (the whole ask as an object)")
		}
	case "failed", "blocked", "cancelled":
		if transition.Reason == "" {
			return outcomeTransition{}, fmt.Errorf("outcome %s needs a reason", name)
		}
	}
	return transition, nil
}

func checkOutcomeValue(key, shape string, value any) (any, error) {
	switch shape {
	case "string":
		if text, ok := value.(string); ok {
			return text, nil
		}
		return nil, fmt.Errorf("front_matter.%s must be a string", key)
	case "time":
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("front_matter.%s must be an RFC 3339 time string", key)
		}
		when, err := time.Parse(time.RFC3339, strings.TrimSpace(text))
		if err != nil {
			return nil, fmt.Errorf("front_matter.%s must be an RFC 3339 time, not %q", key, text)
		}
		return when.UTC().Format(time.RFC3339), nil
	case "number":
		number, ok := value.(float64)
		if !ok || number < 0 {
			return nil, fmt.Errorf("front_matter.%s must be a non-negative number", key)
		}
		if number == math.Trunc(number) && number <= math.MaxInt32 {
			return int(number), nil
		}
		return number, nil
	case "bool":
		if flag, ok := value.(bool); ok {
			return flag, nil
		}
		return nil, fmt.Errorf("front_matter.%s must be true or false", key)
	case "mapping":
		mapping, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("front_matter.%s must be an object", key)
		}
		if key == "completion_summary" {
			for field, item := range mapping {
				if _, ok := item.(string); !ok {
					return nil, fmt.Errorf("front_matter.completion_summary.%s must be a string", field)
				}
			}
		}
		return mapping, nil
	}
	return nil, fmt.Errorf("front_matter.%s has no known shape", key)
}

// apply makes the outcome's edits to a document that is about to move to t.Status: drops, sets, the
// completion summary stamps, the cancellation reason and one Progress entry.
func (t outcomeTransition) apply(document *Document, phase, source string, now time.Time) {
	frontMatter := document.FrontMatter
	for _, key := range t.Drop {
		delete(frontMatter, key)
	}
	for key, value := range t.Set {
		frontMatter[key] = value
	}
	stamp := now.UTC().Format(time.RFC3339)
	if summary, ok := t.Set["completion_summary"].(map[string]any); ok {
		delete(summary, "rework_count")
		if phase == phaseTaskReview {
			delete(summary, "completed_at")
			summary["reviewed_at"] = stamp
		} else {
			delete(summary, "reviewed_at")
			if t.Status == "done" {
				summary["completed_at"] = stamp
			} else {
				delete(summary, "completed_at")
			}
		}
	}
	if t.Status == "cancelled" && t.Reason != "" {
		if existing, _ := frontMatter["cancelled_reason"].(string); strings.TrimSpace(existing) == "" {
			frontMatter["cancelled_reason"] = t.Reason
		}
	}
	entry := fmt.Sprintf("Outcome `%s` (%s): %s", t.Outcome, source, t.Progress)
	if t.Reason != "" {
		entry += "\n\nReason: " + t.Reason
	}
	if len(t.Ignored) > 0 {
		entry += "\n\nWerkrby ignored front matter keys this seat may not set: " + strings.Join(t.Ignored, ", ") + "."
	}
	appendProgressBlock(document, now, entry)
}

// applyExplicitOutcome runs after the worker process of an explicit-outcome turn exited. It reads
// and validates the outcome and makes the transition itself; the ordinary transition reconcile then
// validates the new status exactly as it would a worker's move. A missing or invalid outcome returns
// the task to todo with the reason and spends the attempt, so the turn never waits for a detector.
func (m *Manager) applyExplicitOutcome(route workflowRoute, lease Lease, turn *explicitTurn, finalText string) {
	name := filepath.Base(lease.File)
	if _, err := os.Stat(lease.File); err != nil {
		if os.IsNotExist(err) {
			m.log(fmt.Sprintf("explicit outcome: %s left its claimed path during the turn on %s (a worker or operator move); the ordinary transition reconcile applies", name, turn.Staff))
		} else {
			m.log("explicit outcome: " + err.Error())
		}
		return
	}
	base := filepath.Dir(m.Config.Resolve(route.Source))
	now := time.Now().UTC()
	outcome, source, err := readTurnOutcome(turn.ResultPath, finalText)
	var transition outcomeTransition
	if err == nil {
		transition, err = validateTurnOutcome(outcome, turn.Phase)
	}
	if err != nil {
		note := fmt.Sprintf("No valid outcome from this %s turn on %s: %v. Werkrby returned the task to todo; the attempt was spent.", turn.phaseLabel(), turn.Staff, err)
		target := statusPath(base, "todo", name)
		update := func(frontMatter map[string]any) { delete(frontMatter, resumeCreditField) }
		if moveErr := moveDocumentWithUpdate(lease.File, target, "todo", now, note, update); moveErr != nil {
			m.log("explicit outcome requeue " + lease.File + ": " + moveErr.Error())
			return
		}
		_ = os.Remove(m.leasePath(lease.ID))
		m.finishRuntimeJob(lease.ID)
		m.log(note + " " + target)
		return
	}
	target := statusPath(base, transition.Status, name)
	edit := func(document *Document) { transition.apply(document, turn.Phase, source, now) }
	if err := moveDocumentWithEdit(lease.File, target, transition.Status, now, edit); err != nil {
		m.log("explicit outcome " + lease.File + ": " + err.Error())
		return
	}
	m.log(fmt.Sprintf("explicit outcome %s from %s (%s): %s moved to %s", transition.Outcome, turn.Staff, source, name, transition.Status))
}
