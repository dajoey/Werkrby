package orchestrator

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/agent0ai/spynel/internal/fsx"
	"github.com/agent0ai/spynel/internal/roster"
)

// Explicit turn outcomes. On a roster seat with `explicit_outcome: true` (headless only), a task
// implementation or review turn does not write its task document: the worker returns the outcome as
// data, in result.json in its run directory or a werkrby-outcome block that ends its final reply,
// and the owner reads it after the worker process exits, validates it, applies the front matter and
// moves the file under the document lock. Every other seat, and every goal phase, keeps the
// file-move contract.
//
// What a bad outcome costs follows what the file-move path forgives today: a park with no routable
// wait returns to todo without spending the attempt; a completion summary with only a format
// problem gets one repair per attempt; any other missing or invalid outcome gets one free retry per
// attempt and phase (a review goes back to review, an implementation to todo with the attempt
// continued), and the next one in the same attempt spends it.

const (
	turnResultFileName      = "result.json"
	outcomeFenceInfo        = "werkrby-outcome"
	maxTurnOutcomeBytes     = 256 << 10
	maxOutcomeProgressRunes = 16000
	// outcomeMissField records the phase and attempt whose free retry after a missing or invalid
	// outcome was used ("task_review:2"), so the next miss in that attempt spends it.
	outcomeMissField = "outcome_miss"
)

var errNoTurnOutcome = errors.New("no outcome returned (no result.json in the run directory and no " + outcomeFenceInfo + " block ending the final reply)")

// safeLeaseID is the only lease id shape that may name a run directory: leaseID's hex digest.
var safeLeaseID = regexp.MustCompile(`^[0-9a-f]{1,64}$`)

type outcomeErrorKind int

const (
	outcomeMissed       outcomeErrorKind = iota // missing or malformed: one free retry per attempt and phase
	outcomeParkRejected                         // a park with no routable wait: todo, attempt not spent
	outcomeFormatOnly                           // completion summary format only: one repair per attempt
)

type outcomeError struct {
	kind outcomeErrorKind
	err  error
}

func (e *outcomeError) Error() string { return e.err.Error() }
func (e *outcomeError) Unwrap() error { return e.err }

func outcomeFail(kind outcomeErrorKind, format string, args ...any) error {
	return &outcomeError{kind: kind, err: fmt.Errorf(format, args...)}
}

func outcomeKind(err error) outcomeErrorKind {
	var typed *outcomeError
	if errors.As(err, &typed) {
		return typed.kind
	}
	return outcomeMissed
}

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
	Refused  []string
}

// workerOutcomeKeys are the front matter keys a worker may set or drop through its outcome, with the
// value shape each needs. Every other key is owned by Werkrby or another writer and is ignored.
// `risk` may only go up, and `staff` changes only on an implementation todo hand-off.
var workerOutcomeKeys = map[string]string{
	"completion_summary":    "mapping",
	"wake_at":               "time",
	"waiting_for":           "string",
	"joey_ask":              "mapping",
	"wake_check":            "string",
	"wake_check_interval_s": "number",
	"risk":                  "risk",
	"staff":                 "string",
	"cancelled_reason":      "string",
	"ffxiv_build":           "bool",
}

// ownedFrontMatterKeys are restored to their value at dispatch before an outcome is applied, so a
// worker that edits the file in place against the contract changes nothing that survives the move.
// Keys other writers maintain while a task is claimed (provider_iterations, the Helm and parks
// fields) are not listed.
var ownedFrontMatterKeys = []string{
	"id", "title", "created_at", "first_assigned_at", "attempt", "review_attempt", "review_required",
	"goal_id", "goal_round", "notify", "agent", "model", "implementation_thread", "implementation_session",
	resumeCreditField, resumeCreditsAttemptField, resumeCreditsUsedField, formatCreditAttemptField,
	acceptanceField, acceptanceRoundsField, outcomeMissField,
	"completion_summary", "wake_at", "waiting_for", "joey_ask", "wake_check", "wake_check_interval_s",
	"risk", "staff", "cancelled_reason", "ffxiv_build",
}

// codeManagedSummaryKeys are completion_summary fields Werkrby writes; a worker's values are dropped.
var codeManagedSummaryKeys = []string{"rework_count", "completed_at", "reviewed_at"}

var riskRank = map[string]int{"low": 1, "medium": 2, "high": 3}

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

// explicitTurn is one dispatch on an explicit-outcome seat: where its worker writes the outcome, and
// the task document as it stood when the turn started.
type explicitTurn struct {
	Staff      string
	Phase      string
	Dir        string
	ResultPath string
	file       string
	raw        []byte
	digest     [sha256.Size]byte
	owned      map[string]any
}

// explicitOutcomeTurn reports whether this dispatch runs on an explicit-outcome seat: a task
// implementation or review phase routed to roster staff with headless and explicit_outcome set.
func (m *Manager) explicitOutcomeTurn(route workflowRoute, lease Lease, targetModel string) *explicitTurn {
	if route.Name != "tasks" || !safeLeaseID.MatchString(lease.ID) {
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
		Dir: dir, ResultPath: filepath.Join(dir, turnResultFileName), file: lease.File,
	}
}

// prepare creates the run directory, removes an outcome left by an earlier turn of this lease, and
// records the task document as it stands before the worker starts.
func (t *explicitTurn) prepare() error {
	raw, err := os.ReadFile(t.file)
	if err != nil {
		return err
	}
	document, err := ParseDocument(raw)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(t.Dir, 0o700); err != nil {
		return err
	}
	if err := os.Remove(t.ResultPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	t.raw, t.digest, t.owned = raw, sha256.Sum256(raw), map[string]any{}
	for _, key := range ownedFrontMatterKeys {
		if value, ok := document.FrontMatter[key]; ok {
			t.owned[key] = value
		}
	}
	return nil
}

// changedOnDisk reports whether the task file differs from the bytes the turn started with.
func (t *explicitTurn) changedOnDisk() bool {
	raw, err := os.ReadFile(t.file)
	return err != nil || sha256.Sum256(raw) != t.digest
}

// restoreOwned puts every owned key back to its value at dispatch and names the keys it changed.
func (t *explicitTurn) restoreOwned(frontMatter map[string]any) []string {
	var restored []string
	for _, key := range ownedFrontMatterKeys {
		before, had := t.owned[key]
		now, has := frontMatter[key]
		switch {
		case had && (!has || !reflect.DeepEqual(before, now)):
			frontMatter[key] = before
			restored = append(restored, key)
		case !had && has:
			delete(frontMatter, key)
			restored = append(restored, key)
		}
	}
	return restored
}

func (t *explicitTurn) ownedAttempt() int {
	return numberValue(t.owned["attempt"])
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
	fmt.Fprintf(&b, "- Read `%s` as often as you need. Do not edit, move, rename or delete it; Werkrby undoes edits to it.\n", file)
	fmt.Fprintf(&b, "- As the last action of your turn, write one JSON object to `%s` (the folder exists; replace the file if it is there). Werkrby reads it after your process exits, validates it, adds your progress entry with the exact UTC time, sets `status` and `updated_at`, applies your front matter and moves the file.\n", t.ResultPath)
	fmt.Fprintf(&b, "- If you cannot write that file, end your final reply with the same JSON object in a fenced code block whose info string is exactly `%s`, with nothing after the closing fence. The file wins when both exist.\n", outcomeFenceInfo)
	b.WriteString("- A missing or invalid outcome sends the task back with the reason in `## Progress`: once per attempt for free, the next time the attempt is spent.\n\n")
	b.WriteString("Fields:\n\n")
	b.WriteString("- `outcome` (required): one of the outcomes listed below.\n")
	b.WriteString("- `progress` (required): your `## Progress` entry in Markdown: what you did, the evidence and verification, and for a park, failure or cancellation the exact condition, reason and next action. Add no timestamp.\n")
	b.WriteString("- `reason`: required for `failed`, `blocked` and `cancelled`; one or two plain sentences.\n")
	fmt.Fprintf(&b, "- `front_matter` (optional): an object of keys to set. Allowed keys: %s. `wake_at` is a future RFC 3339 UTC time, `wake_check_interval_s` a number, `ffxiv_build` a boolean, `risk` one of `low`, `medium`, `high` (it can only go up), `completion_summary` and `joey_ask` objects, and `staff` only on a `todo` hand-off. Werkrby ignores any other key and names it in Progress; it owns `status`, `updated_at`, the attempt counters, and the `completed_at`, `reviewed_at` and `rework_count` fields inside `completion_summary`.\n", strings.Join(keys, ", "))
	b.WriteString("- `drop` (optional): a list of allowed keys to remove.\n\n")
	b.WriteString("A `joey_ask` object has `subject` (at most 40 characters), `recap` (at most 280 characters), `prompt` (the one question, in plain words), `options` (a list of objects, each with `label` (at most 60 characters), `value` (at most 40) and `effect` (one sentence, at most 200 characters)) and `context` (detail, at most 1500 characters), plus optional `visual` and `visual_caption`. End `progress` with the `NEEDS JOEY BECAUSE: <one sentence>` line the workspace instructions ask for.\n\n")
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
	b.WriteString("- `ask_joey`: wait on Joey's decision. Needs `joey_ask` (the whole ask, shaped as above) and `waiting_for`.\n")
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

// extractOutcomeBlock returns the werkrby-outcome block that ends a final reply: the last opener at
// the start of a line with exactly that info string, one JSON value decoded right after it, and
// nothing but the closing fence after that value.
func extractOutcomeBlock(finalText string) ([]byte, bool) {
	text := strings.TrimRight(finalText, " \t\r\n")
	opener := "```" + outcomeFenceInfo
	start := -1
	for end := len(text); end > 0; {
		index := strings.LastIndex(text[:end], opener)
		if index < 0 {
			break
		}
		if index == 0 || text[index-1] == '\n' {
			start = index
			break
		}
		end = index
	}
	if start < 0 {
		return nil, false
	}
	lineEnd := strings.IndexByte(text[start:], '\n')
	if lineEnd < 0 || strings.TrimSpace(text[start+len(opener):start+lineEnd]) != "" {
		return nil, false
	}
	body := text[start+lineEnd+1:]
	decoder := json.NewDecoder(strings.NewReader(body))
	var value json.RawMessage
	if err := decoder.Decode(&value); err != nil {
		// Not one JSON value. When the reply still ends with the block, hand its text to the
		// parser so the outcome is reported invalid rather than missing.
		if strings.HasSuffix(body, "```") {
			return []byte(strings.TrimSuffix(body, "```")), true
		}
		return nil, false
	}
	if strings.TrimSpace(body[decoder.InputOffset():]) != "```" {
		return nil, false
	}
	return value, true
}

// readResultFile reads result.json only when it is a regular file: never through a symlink, never a
// FIFO or device (which could block the dispatch), and never past the size bound.
func readResultFile(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", turnResultFileName)
	}
	file, err := openResultFile(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", turnResultFileName, err)
	}
	defer file.Close()
	if opened, err := file.Stat(); err != nil || !opened.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", turnResultFileName)
	}
	data, err := io.ReadAll(io.LimitReader(file, maxTurnOutcomeBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxTurnOutcomeBytes {
		return nil, fmt.Errorf("%s is larger than %d bytes", turnResultFileName, maxTurnOutcomeBytes)
	}
	return data, nil
}

// readTurnOutcome reads the outcome a turn returned: result.json first, then the final reply.
func readTurnOutcome(resultPath, finalText string) (turnOutcome, string, error) {
	data, err := readResultFile(resultPath)
	switch {
	case err == nil:
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
	parking := name == "park" || name == "ask_joey"
	ignored := map[string]bool{}
	for key, value := range outcome.FrontMatter {
		shape, known := workerOutcomeKeys[key]
		if !known {
			ignored[key] = true
			continue
		}
		normalized, err := checkOutcomeValue(key, shape, value)
		if err != nil {
			kind := outcomeMissed
			switch {
			case parking && (key == "wake_at" || key == "joey_ask"):
				kind = outcomeParkRejected
			case key == "completion_summary" && phase == phaseTaskImplementation:
				kind = outcomeFormatOnly
			}
			return outcomeTransition{}, &outcomeError{kind: kind, err: err}
		}
		if key == "staff" && !(phase == phaseTaskImplementation && name == "todo") {
			transition.Refused = append(transition.Refused, "staff (only an implementation todo hand-off may change it)")
			continue
		}
		transition.Set[key] = normalized
	}
	for _, key := range outcome.Drop {
		if _, known := workerOutcomeKeys[key]; known && key != "staff" && key != "risk" {
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
		wake, ok := transition.Set["wake_at"].(string)
		if !ok {
			return outcomeTransition{}, outcomeFail(outcomeParkRejected, "outcome park needs front_matter.wake_at (a future RFC 3339 UTC time)")
		}
		if when, _ := time.Parse(time.RFC3339, wake); !when.After(time.Now().UTC()) {
			return outcomeTransition{}, outcomeFail(outcomeParkRejected, "front_matter.wake_at %s is not in the future", wake)
		}
	case "ask_joey":
		if ask, _ := transition.Set["joey_ask"].(map[string]any); len(ask) == 0 {
			return outcomeTransition{}, outcomeFail(outcomeParkRejected, "outcome ask_joey needs front_matter.joey_ask (the whole ask as an object)")
		}
	case "failed", "blocked", "cancelled":
		if transition.Reason == "" {
			return outcomeTransition{}, fmt.Errorf("outcome %s needs a reason", name)
		}
	}
	if phase == phaseTaskReview {
		if err := checkReviewVerdict(name, transition.Set); err != nil {
			return outcomeTransition{}, err
		}
	}
	return transition, nil
}

// checkReviewVerdict requires a review's completion summary verdict to match its outcome: an
// acceptance carries an accepted summary, and any summary a review sends says what the outcome says.
func checkReviewVerdict(outcome string, set map[string]any) error {
	want := ""
	switch outcome {
	case "accepted", "done", "park", "ask_joey":
		want = "accepted"
	case "rejected", "todo":
		want = "rejected"
	default:
		return nil
	}
	summary, ok := set["completion_summary"].(map[string]any)
	if !ok {
		if outcome == "accepted" || outcome == "done" {
			return fmt.Errorf("outcome %s needs front_matter.completion_summary with verdict %q", outcome, want)
		}
		return nil
	}
	verdict, _ := summary["verdict"].(string)
	if strings.ToLower(strings.TrimSpace(verdict)) != want {
		return fmt.Errorf("outcome %s needs completion_summary verdict %q, not %q", outcome, want, verdict)
	}
	return nil
}

func checkOutcomeValue(key, shape string, value any) (any, error) {
	switch shape {
	case "string":
		if text, ok := value.(string); ok {
			return text, nil
		}
		return nil, fmt.Errorf("front_matter.%s must be a string", key)
	case "risk":
		text, _ := value.(string)
		text = strings.ToLower(strings.TrimSpace(text))
		if _, ok := riskRank[text]; !ok {
			return nil, fmt.Errorf("front_matter.%s must be low, medium or high", key)
		}
		return text, nil
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
			for _, managed := range codeManagedSummaryKeys {
				delete(mapping, managed)
			}
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

// staffChange decides whether a todo hand-off may move a task from the seat it is pinned to (current,
// possibly empty) to target.
type staffChange func(current, target string) error

// apply makes the outcome's edits to a document that is about to move to t.Status: drops, sets, the
// risk and staff rules, the completion summary stamps, the cancellation reason and one Progress entry.
func (t outcomeTransition) apply(document *Document, phase, source string, now time.Time, restored []string, staffOK staffChange) {
	frontMatter := document.FrontMatter
	refused := append([]string(nil), t.Refused...)
	for _, key := range t.Drop {
		delete(frontMatter, key)
	}
	for key, value := range t.Set {
		switch key {
		case "risk":
			current, _ := frontMatter["risk"].(string)
			if riskRank[value.(string)] < riskRank[strings.ToLower(strings.TrimSpace(current))] {
				refused = append(refused, fmt.Sprintf("risk %s (risk only goes up; it stays %s)", value, current))
				continue
			}
		case "staff":
			current, _ := frontMatter["staff"].(string)
			if staffOK == nil {
				refused = append(refused, "staff (no roster to check the hand-off against)")
				continue
			}
			if err := staffOK(strings.TrimSpace(current), strings.TrimSpace(value.(string))); err != nil {
				refused = append(refused, "staff "+value.(string)+" ("+err.Error()+")")
				continue
			}
		}
		frontMatter[key] = value
	}
	stamp := now.UTC().Format(time.RFC3339)
	if summary, ok := t.Set["completion_summary"].(map[string]any); ok {
		if phase == phaseTaskReview {
			summary["reviewed_at"] = stamp
		} else if t.Status == "done" {
			summary["completed_at"] = stamp
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
	if len(refused) > 0 {
		entry += "\n\nWerkrby refused: " + strings.Join(refused, "; ") + "."
	}
	if len(restored) > 0 {
		entry += "\n\nThe task file was edited in place during the turn; Werkrby restored " + strings.Join(restored, ", ") + "."
	}
	appendProgressBlock(document, now, entry)
}

// staffHandOff checks a todo hand-off against the roster: the target must be defined, and neither the
// seat the task is pinned to nor the target may be a no_escalate (private-domain) seat.
func (m *Manager) staffHandOff(current, target string) error {
	staffing, err := roster.Load(m.Config.StatePath())
	if err != nil || staffing == nil {
		return errors.New("no valid roster")
	}
	if !staffing.Has(target) {
		return errors.New("not a roster seat")
	}
	if s, ok := staffing.Staff[current]; ok && s.NoEscalate {
		return fmt.Errorf("the task is pinned to %s, a no_escalate seat", current)
	}
	if staffing.Staff[target].NoEscalate {
		return errors.New("a no_escalate seat is never a hand-off target")
	}
	return nil
}

// applyExplicitOutcome runs after the worker process of an explicit-outcome turn exited. It reads
// and validates the outcome and makes the transition itself; the ordinary transition reconcile then
// validates the new status exactly as it would a worker's move. Every way this can go wrong ends in
// an explicit requeue, never a lease left in awaiting_transition for a detector.
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
	changed := turn.changedOnDisk()
	outcome, source, err := readTurnOutcome(turn.ResultPath, finalText)
	var transition outcomeTransition
	if err == nil {
		transition, err = validateTurnOutcome(outcome, turn.Phase)
	}
	if err != nil {
		m.requeueOutcomeFailure(base, name, lease, turn, err, changed, now)
		return
	}
	target := statusPath(base, transition.Status, name)
	var restored []string
	edit := func(document *Document) {
		restored = turn.restoreOwned(document.FrontMatter)
		transition.apply(document, turn.Phase, source, now, restored, m.staffHandOff)
	}
	if err := moveDocumentWithEdit(lease.File, target, transition.Status, now, edit); err != nil {
		m.requeueUnappliable(base, name, lease, turn, err, now)
		return
	}
	m.logInPlaceEdit(name, turn, changed, restored)
	m.log(fmt.Sprintf("explicit outcome %s from %s (%s): %s moved to %s", transition.Outcome, turn.Staff, source, name, transition.Status))
}

// applyWrittenOutcomeAfterSendError applies a valid result.json the worker wrote before its run ended
// with an error (for example a quota or overload refusal of its final reply). An operator stop or a
// goal preemption still wins, and anything less than a valid written outcome leaves the error to the
// ordinary error path. It reports whether it handled the turn.
func (m *Manager) applyWrittenOutcomeAfterSendError(route workflowRoute, lease Lease, turn *explicitTurn, sendErr error) bool {
	if m.operatorStopPending(lease.ID) || m.preemptionPendingFor(lease.ID) {
		return false
	}
	outcome, _, err := readTurnOutcome(turn.ResultPath, "")
	if err != nil {
		return false
	}
	if _, err := validateTurnOutcome(outcome, turn.Phase); err != nil {
		return false
	}
	if current, loadErr := m.loadLease(lease.ID); loadErr == nil {
		lease = current
	}
	m.log(fmt.Sprintf("explicit outcome: the run on %s ended with an error after writing a valid outcome (%v); applying it", turn.Staff, sendErr))
	m.applyExplicitOutcome(route, lease, turn, "")
	return true
}

// requeueOutcomeFailure sends a task back after a missing or invalid outcome, charging what the
// file-move path charges for the same mistake.
func (m *Manager) requeueOutcomeFailure(base, name string, lease Lease, turn *explicitTurn, cause error, changed bool, now time.Time) {
	if _, readErr := ReadDocument(lease.File); readErr != nil {
		m.requeueUnappliable(base, name, lease, turn, readErr, now)
		return
	}
	attempt := turn.ownedAttempt()
	status, credit := "todo", false
	var mark func(map[string]any)
	var note string
	switch kind := outcomeKind(cause); {
	case kind == outcomeParkRejected:
		credit = true
		note = fmt.Sprintf("Park rejected: %v. Werkrby returned this task to todo without spending an attempt.", cause)
	case kind == outcomeFormatOnly && turn.Phase == phaseTaskImplementation && attempt >= 1 && numberValue(turn.owned[formatCreditAttemptField]) != attempt:
		credit = true
		mark = func(frontMatter map[string]any) { frontMatter[formatCreditAttemptField] = attempt }
		note = fmt.Sprintf("Outcome rejected: %v. Only the completion summary format is wrong, so the repair turn continues attempt %d; a second format rejection in this attempt spends it.", cause, attempt)
	default:
		key := fmt.Sprintf("%s:%d", turn.Phase, attempt)
		if used, _ := turn.owned[outcomeMissField].(string); used != key {
			mark = func(frontMatter map[string]any) { frontMatter[outcomeMissField] = key }
			if turn.Phase == phaseTaskReview {
				status = "review"
				note = fmt.Sprintf("No valid outcome from this review turn on %s: %v. Werkrby returned the task to review for a fresh review; the attempt was not spent. A second miss in this attempt returns it to todo and spends the attempt.", turn.Staff, cause)
			} else {
				credit = true
				note = fmt.Sprintf("No valid outcome from this implementation turn on %s: %v. Werkrby returned the task to todo; the attempt was not spent. A second miss in this attempt spends it.", turn.Staff, cause)
			}
		} else {
			note = fmt.Sprintf("No valid outcome from this %s turn on %s, the second in attempt %d: %v. Werkrby returned the task to todo; the attempt was spent.", turn.phaseLabel(), turn.Staff, attempt, cause)
		}
	}
	var restored []string
	update := func(frontMatter map[string]any) {
		restored = turn.restoreOwned(frontMatter)
		if credit {
			frontMatter[resumeCreditField] = true
		} else {
			delete(frontMatter, resumeCreditField)
		}
		if mark != nil {
			mark(frontMatter)
		}
	}
	target := statusPath(base, status, name)
	if err := moveDocumentWithUpdate(lease.File, target, status, now, note, update); err != nil {
		m.requeueUnappliable(base, name, lease, turn, err, now)
		return
	}
	m.logInPlaceEdit(name, turn, changed, restored)
	_ = os.Remove(m.leasePath(lease.ID))
	m.finishRuntimeJob(lease.ID)
	m.log(note + " " + target)
}

// requeueUnappliable returns a task to todo when its outcome cannot be applied (the document no
// longer parses, or the write failed). An unreadable document is first put back to the bytes the
// turn started with. The attempt is spent.
func (m *Manager) requeueUnappliable(base, name string, lease Lease, turn *explicitTurn, cause error, now time.Time) {
	target := statusPath(base, "todo", name)
	note := fmt.Sprintf("Werkrby could not apply this turn's outcome (%v) and returned the task to todo; the attempt was spent.", cause)
	update := func(frontMatter map[string]any) {
		turn.restoreOwned(frontMatter)
		delete(frontMatter, resumeCreditField)
	}
	err := moveDocumentWithUpdate(lease.File, target, "todo", now, note, update)
	if err != nil && len(turn.raw) > 0 {
		if lock, lockErr := lockProviderTurn(lease.File); lockErr == nil {
			writeErr := fsx.AtomicWriteFile(lease.File, turn.raw, 0o600)
			unlockProviderTurn(lock)
			if writeErr == nil {
				note = fmt.Sprintf("Werkrby could not apply this turn's outcome (%v); it put the task file back as it was when the turn started and returned it to todo; the attempt was spent.", cause)
				err = moveDocumentWithUpdate(lease.File, target, "todo", now, note, update)
			}
		}
	}
	if err != nil {
		if renameErr := os.Rename(lease.File, target); renameErr != nil {
			m.log(fmt.Sprintf("explicit outcome: could not requeue %s (%v, then %v)", name, err, renameErr))
			return
		}
	}
	_ = os.Remove(m.leasePath(lease.ID))
	m.finishRuntimeJob(lease.ID)
	m.log(note + " " + target)
}

func (m *Manager) logInPlaceEdit(name string, turn *explicitTurn, changed bool, restored []string) {
	if !changed {
		return
	}
	what := "no Werkrby-owned key needed restoring"
	if len(restored) > 0 {
		what = "restored Werkrby-owned keys: " + strings.Join(restored, ", ")
	}
	m.log(fmt.Sprintf("explicit outcome: %s changed on disk during the turn on %s; %s", name, turn.Staff, what))
}
