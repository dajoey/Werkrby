// Package roster owns the optional workspace staffing file .spynel/roster.yaml:
// named staff (runner kind, launch arguments, daily cap, fallback) and the
// workflow-role to staff mapping. A missing file means legacy routing; a
// malformed file is an error the caller logs before falling back to legacy.
package roster

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/agent0ai/spynel/internal/fsx"
	"gopkg.in/yaml.v3"
)

const (
	FileName = "roster.yaml"
	// StateDirName mirrors config.StateDirectoryName for harnesses that only know the workspace root.
	StateDirName  = ".spynel"
	usageFileName = "roster-usage.json"
	// StaffPrefix marks a harness model string that names roster staff.
	StaffPrefix = "@"
	maxFallback = 8
)

var namePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)

type Staff struct {
	Runner string   `yaml:"runner"`
	Args   []string `yaml:"args,omitempty"`
	// NotifyAt asks Joey once per day when the count reaches it; routing is unchanged.
	NotifyAt int `yaml:"notify_at,omitempty"`
	// DailyCap halts the staff for the day unless Joey answered "continue".
	DailyCap int    `yaml:"daily_cap,omitempty"`
	Fallback string `yaml:"fallback,omitempty"`
	// NoEscalate keeps a pinned task on this staff past the top ladder rung
	// instead of promoting it. Opt-in for private-domain seats.
	NoEscalate bool `yaml:"no_escalate,omitempty"`
	// EscalateAfter and EscalateTo move a task that has used up attempts onto another seat: once a
	// task's attempt number is above EscalateAfter it runs on EscalateTo instead. Unlike the global
	// Escalation ladder this is per seat, so a retry stays inside the job's own lane (the ladder
	// sent every attempt-3 task to one seat whatever its job and was removed 2026-09-28). Both are
	// set together; the target may escalate again on its own thresholds.
	EscalateAfter int    `yaml:"escalate_after,omitempty"`
	EscalateTo    string `yaml:"escalate_to,omitempty"`
	// Family names the model family behind the staff (glm, gemini, claude, kimi, ...). Empty means
	// derive it from the --model launch argument; see FamilyOf. Review picks a reviewer from a
	// different family than the seat that wrote the work.
	Family string `yaml:"family,omitempty"`
	// Headless runs each turn of a `runner: claude` staff as `claude -p` (stream-json) in the workspace
	// instead of an interactive Herdr pane. Anthropic bills the Max plan's monthly API credit only for
	// headless (Agent SDK) Claude Code runs, so a seat paid by that credit must set it.
	Headless bool `yaml:"headless,omitempty"`
	// ExplicitOutcome makes a headless seat return its task turn's outcome as data (result.json in
	// its run directory, or a werkrby-outcome block in its final reply) instead of editing and moving
	// the task file; the orchestrator applies the transition. Off unless set; needs headless, because
	// only a process exit says the turn is over.
	ExplicitOutcome bool   `yaml:"explicit_outcome,omitempty"`
	Note            string `yaml:"note,omitempty"`
}

type Escalation struct {
	AfterAttempt int    `yaml:"after_attempt"`
	Staff        string `yaml:"staff"`
}

// Ladder is the attempt escalation: one mapping, or a list of rungs.
type Ladder []Escalation

func (l *Ladder) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.SequenceNode {
		var rungs []Escalation
		if err := node.Decode(&rungs); err != nil {
			return err
		}
		*l = rungs
		return nil
	}
	var one Escalation
	if err := node.Decode(&one); err != nil {
		return err
	}
	*l = Ladder{one}
	return nil
}

type Roster struct {
	Staff      map[string]Staff  `yaml:"staff"`
	Roles      map[string]string `yaml:"roles"`
	Escalation Ladder            `yaml:"escalation,omitempty"`
	// RoleAlternates lists, per role, other staff a review may use when the role's own staff is the
	// same model family as the seat that wrote the work. See PickDifferentFamily.
	RoleAlternates map[string][]string `yaml:"role_alternates,omitempty"`
	// Routing is the machine-readable domain routing table (Joey 2026-09-18:
	// "routing is by work type, and the filer sets it — not a classifier, not
	// Joey"). Each entry names a work type; filers stamp front matter
	// `staff:` from it. An empty Staff means omit the pin and fall through to
	// the role default. A non-empty HeldUntil marks a HELD domain: Staff is
	// the current fallback and HeldUntil is the unblock condition.
	Routing []RouteEntry `yaml:"routing,omitempty"`
}

// RouteEntry is one work-type -> seat row. HeldUntil set means HELD.
type RouteEntry struct {
	Domain    string `yaml:"domain"`
	Staff     string `yaml:"staff,omitempty"`
	HeldUntil string `yaml:"held_until,omitempty"`
	Note      string `yaml:"note,omitempty"`
}

// EscalationFor returns the staff for an implementation attempt: the rung with
// the highest after_attempt below it. A task's own staff pin outranks every
// rung except the top one, so a handed-off task reaches its specialist while a
// task that keeps failing still reaches the top.
func (r *Roster) EscalationFor(attempt int, pinned bool) string {
	if r == nil {
		return ""
	}
	best, top := -1, -1
	for i, rung := range r.Escalation {
		if top < 0 || rung.AfterAttempt > r.Escalation[top].AfterAttempt {
			top = i
		}
		if attempt > rung.AfterAttempt && (best < 0 || rung.AfterAttempt > r.Escalation[best].AfterAttempt) {
			best = i
		}
	}
	if best < 0 || (pinned && best != top) {
		return ""
	}
	return r.Escalation[best].Staff
}

// EscalatedStaff returns the seat a task runs on once it has reached `attempt` after starting on
// `name`: it follows each seat's own escalate_to while the attempt is above that seat's
// escalate_after. A name that is unknown, or has no escalation, comes back unchanged.
func (r *Roster) EscalatedStaff(name string, attempt int) string {
	if r == nil {
		return name
	}
	cur := name
	for hop := 0; hop < maxFallback; hop++ {
		s, ok := r.Staff[cur]
		if !ok || s.EscalateTo == "" || attempt <= s.EscalateAfter {
			break
		}
		cur = s.EscalateTo
	}
	return cur
}

var familyPatterns = []struct {
	name string
	re   *regexp.Regexp
}{
	{"glm", regexp.MustCompile(`(?i)glm|zai`)},
	{"gemini", regexp.MustCompile(`(?i)gemini|antigravity`)},
	{"claude", regexp.MustCompile(`(?i)claude|sonnet|haiku|opus`)},
	{"kimi", regexp.MustCompile(`(?i)kimi|(^|/)k[0-9]`)},
}

func modelArg(args []string) string {
	for i, a := range args {
		if a == "--model" && i+1 < len(args) {
			return args[i+1]
		}
		if strings.HasPrefix(a, "--model=") {
			return strings.TrimPrefix(a, "--model=")
		}
	}
	return ""
}

// FamilyOf returns the model family behind a staff: its explicit `family:` (lower-cased), else the
// family its --model launch argument names, else "" when it cannot be told.
func (r *Roster) FamilyOf(name string) string {
	if r == nil {
		return ""
	}
	s, ok := r.Staff[name]
	if !ok {
		return ""
	}
	if f := strings.ToLower(strings.TrimSpace(s.Family)); f != "" {
		return f
	}
	model := modelArg(s.Args)
	for _, p := range familyPatterns {
		if p.re.MatchString(model) {
			return p.name
		}
	}
	return ""
}

// PickDifferentFamily chooses the reviewer for work written on seat `implementer`: `main` when it
// is a different family (or either family is unknown, which is never second-guessed), otherwise the
// first alternate whose family is known and differs, otherwise `main` again.
func (r *Roster) PickDifferentFamily(main string, alternates []string, implementer string) string {
	implFamily := r.FamilyOf(implementer)
	mainFamily := r.FamilyOf(main)
	if implFamily == "" || mainFamily == "" || mainFamily != implFamily {
		return main
	}
	for _, alt := range alternates {
		if f := r.FamilyOf(alt); f != "" && f != implFamily {
			return alt
		}
	}
	return main
}

// Path returns the roster file inside a workspace state directory.
func Path(stateDir string) string { return filepath.Join(stateDir, FileName) }

// pathFlags are launch flags whose value is a file the runner opens at start-up.
var pathFlags = map[string]bool{
	"--mcp-config":                true,
	"--settings":                  true,
	"--plugin-dir":                true,
	"--system-prompt-file":        true,
	"--append-system-prompt-file": true,
}

var (
	refusalMu     sync.Mutex
	refusalLogger = func(msg string) { log.Print(msg) }
	lastGood      = map[string][]byte{}
	lastRefusal   = map[string]string{}
)

// SetRefusalLogger sets where a refused roster reload is reported (nil restores the standard
// logger). Each distinct refusal of one roster file is reported once, not once per dispatch.
func SetRefusalLogger(fn func(string)) {
	refusalMu.Lock()
	defer refusalMu.Unlock()
	if fn == nil {
		fn = func(msg string) { log.Print(msg) }
	}
	refusalLogger = fn
}

// Load reads and validates the roster. It returns (nil, nil) when no roster exists. A roster that
// does not parse, validate, or name only files that exist is refused: the last roster this process
// loaded cleanly stays in force and one error line says why. A process with no earlier roster
// gets the error instead.
func Load(stateDir string) (*Roster, error) {
	path := Path(stateDir)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	r, err := parse(data, stateDir, true)
	refusalMu.Lock()
	defer refusalMu.Unlock()
	if err == nil {
		lastGood[path] = data
		delete(lastRefusal, path)
		return r, nil
	}
	previous, ok := lastGood[path]
	if !ok {
		return nil, err
	}
	kept, keptErr := parse(previous, stateDir, false)
	if keptErr != nil {
		return nil, err
	}
	if lastRefusal[path] != err.Error() {
		lastRefusal[path] = err.Error()
		refusalLogger("roster reload refused, keeping the previous roster: " + err.Error())
	}
	return kept, nil
}

func parse(data []byte, stateDir string, checkPaths bool) (*Roster, error) {
	var r Roster
	if err := yaml.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("parse %s: %w", FileName, err)
	}
	if err := r.validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", FileName, err)
	}
	if checkPaths {
		if err := r.checkLaunchPaths(filepath.Dir(stateDir)); err != nil {
			return nil, fmt.Errorf("%s: %w", FileName, err)
		}
	}
	return &r, nil
}

// launchPath reports whether a launch argument names a file (an absolute path, or the value of a
// path flag) and returns that path; flag is the previous argument.
func launchPath(flag, arg string) (string, bool) {
	if name, value, ok := strings.Cut(arg, "="); ok && pathFlags[name] {
		return value, true
	}
	if pathFlags[flag] || filepath.IsAbs(arg) {
		return arg, true
	}
	return "", false
}

// checkLaunchPaths refuses a roster whose launch arguments name a file that does not exist: the
// seat would fail every launch. Relative paths resolve from the workspace root.
func (r *Roster) checkLaunchPaths(root string) error {
	names := make([]string, 0, len(r.Staff))
	for name := range r.Staff {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		prev := ""
		for _, arg := range r.Staff[name].Args {
			path, isPath := launchPath(prev, arg)
			prev = arg
			if !isPath {
				continue
			}
			if !filepath.IsAbs(path) {
				path = filepath.Join(root, path)
			}
			if _, err := os.Stat(path); err != nil {
				return fmt.Errorf("staff %s: launch path %s does not exist", name, path)
			}
		}
	}
	return nil
}

func (r *Roster) validate() error {
	if len(r.Staff) == 0 {
		return errors.New("no staff defined")
	}
	for name, s := range r.Staff {
		if !namePattern.MatchString(name) {
			return fmt.Errorf("staff name %q is invalid", name)
		}
		if !namePattern.MatchString(s.Runner) {
			return fmt.Errorf("staff %s: runner %q is invalid", name, s.Runner)
		}
		if s.Headless && s.Runner != "claude" {
			return fmt.Errorf("staff %s: headless needs runner claude, not %q", name, s.Runner)
		}
		if s.ExplicitOutcome && !s.Headless {
			return fmt.Errorf("staff %s: explicit_outcome needs headless: true", name)
		}
		if s.DailyCap < 0 {
			return fmt.Errorf("staff %s: daily_cap cannot be negative", name)
		}
		if s.NotifyAt < 0 {
			return fmt.Errorf("staff %s: notify_at cannot be negative", name)
		}
		if s.NotifyAt > 0 && s.DailyCap > 0 && s.NotifyAt >= s.DailyCap {
			return fmt.Errorf("staff %s: notify_at must be below daily_cap", name)
		}
		if s.Fallback != "" {
			if _, ok := r.Staff[s.Fallback]; !ok {
				return fmt.Errorf("staff %s: fallback %q is not defined", name, s.Fallback)
			}
		}
		if s.DailyCap > 0 && s.Fallback == "" {
			return fmt.Errorf("staff %s: daily_cap needs a fallback", name)
		}
		if s.EscalateAfter != 0 || s.EscalateTo != "" {
			if s.EscalateAfter < 1 || s.EscalateTo == "" {
				return fmt.Errorf("staff %s: escalate_after (at least 1) and escalate_to must be set together", name)
			}
			if s.EscalateTo == name {
				return fmt.Errorf("staff %s: escalate_to cannot be the staff itself", name)
			}
			if _, ok := r.Staff[s.EscalateTo]; !ok {
				return fmt.Errorf("staff %s: escalate_to %q is not defined", name, s.EscalateTo)
			}
		}
		for _, arg := range s.Args {
			if strings.ContainsAny(arg, "\n\r\x00") {
				return fmt.Errorf("staff %s: args must be single-line", name)
			}
		}
	}
	for name := range r.Staff {
		visited := map[string]bool{}
		for cur := name; r.Staff[cur].EscalateTo != ""; cur = r.Staff[cur].EscalateTo {
			if visited[cur] {
				return fmt.Errorf("staff %s: escalate_to forms a cycle", name)
			}
			visited[cur] = true
		}
	}
	for role, alternates := range r.RoleAlternates {
		for _, alt := range alternates {
			if _, ok := r.Staff[alt]; !ok {
				return fmt.Errorf("role_alternates %s: staff %q is not defined", role, alt)
			}
		}
	}
	for role, name := range r.Roles {
		if _, ok := r.Staff[name]; !ok {
			return fmt.Errorf("role %s: staff %q is not defined", role, name)
		}
	}
	seen := map[int]bool{}
	for _, rung := range r.Escalation {
		if rung.AfterAttempt < 1 {
			return errors.New("escalation.after_attempt must be at least 1")
		}
		if seen[rung.AfterAttempt] {
			return fmt.Errorf("escalation: after_attempt %d is listed twice", rung.AfterAttempt)
		}
		seen[rung.AfterAttempt] = true
		if _, ok := r.Staff[rung.Staff]; !ok {
			return fmt.Errorf("escalation: staff %q is not defined", rung.Staff)
		}
	}
	for i, entry := range r.Routing {
		if strings.TrimSpace(entry.Domain) == "" {
			return fmt.Errorf("routing[%d]: domain must not be empty", i)
		}
		for j := 0; j < i; j++ {
			if r.Routing[j].Domain == entry.Domain {
				return fmt.Errorf("routing: domain %q is listed twice", entry.Domain)
			}
		}
		if entry.Staff != "" {
			if _, ok := r.Staff[entry.Staff]; !ok {
				return fmt.Errorf("routing %q: staff %q is not defined", entry.Domain, entry.Staff)
			}
		}
		if entry.HeldUntil != "" && entry.Staff == "" {
			return fmt.Errorf("routing %q: a held domain must name its fallback staff", entry.Domain)
		}
		for _, field := range []string{entry.Domain, entry.Staff, entry.HeldUntil, entry.Note} {
			if strings.ContainsAny(field, "\n\r\x00") {
				return fmt.Errorf("routing %q: fields must be single-line", entry.Domain)
			}
		}
	}
	return nil
}

// Has reports whether name is defined staff.
func (r *Roster) Has(name string) bool {
	if r == nil {
		return false
	}
	_, ok := r.Staff[name]
	return ok
}

// ForRole returns the staff name mapped to a workflow role, or "".
func (r *Roster) ForRole(role string) string {
	if r == nil {
		return ""
	}
	return r.Roles[role]
}

// Model renders the harness model string for staff: the bare runner when no
// launch arguments or headless mode are needed, otherwise the "@name" form a
// roster-aware harness expands.
func (r *Roster) Model(name string) string {
	s := r.Staff[name]
	if len(s.Args) == 0 && !s.Headless {
		return s.Runner
	}
	return StaffPrefix + name
}

// Expand resolves a harness model string to a runner kind and launch arguments.
// Strings without the staff prefix pass through unchanged.
func Expand(stateDir, model string) (string, []string, error) {
	if !strings.HasPrefix(model, StaffPrefix) {
		return model, nil, nil
	}
	name := strings.TrimPrefix(model, StaffPrefix)
	r, err := Load(stateDir)
	if err != nil {
		return "", nil, err
	}
	if !r.Has(name) {
		return "", nil, fmt.Errorf("roster staff %q is not defined", name)
	}
	s := r.Staff[name]
	return s.Runner, append([]string(nil), s.Args...), nil
}

// ExpandStaff is Expand for callers that need the whole staff entry: ok is false when model does not
// name roster staff.
func ExpandStaff(stateDir, model string) (s Staff, ok bool, err error) {
	if !strings.HasPrefix(model, StaffPrefix) {
		return Staff{}, false, nil
	}
	name := strings.TrimPrefix(model, StaffPrefix)
	r, err := Load(stateDir)
	if err != nil {
		return Staff{}, true, err
	}
	if !r.Has(name) {
		return Staff{}, true, fmt.Errorf("roster staff %q is not defined", name)
	}
	s = r.Staff[name]
	s.Args = append([]string(nil), s.Args...)
	return s, true, nil
}

type usage struct {
	Date   string            `json:"date"`
	Counts map[string]int    `json:"counts"`
	Seen   map[string]string `json:"seen"`
	// Notified lists staff whose notify_at ask was already filed today.
	Notified map[string]bool `json:"notified,omitempty"`
}

// decisions is Joey's answer to a notify_at ask, written by the agent that
// resumes the ask task: "continue" lifts daily_cap for that staff today,
// "stop" halts the staff now. Anything else, or another date, means no answer.
type decisions struct {
	Date  string            `json:"date"`
	Staff map[string]string `json:"staff"`
}

const (
	decisionsFileName = "roster-decisions.json"
	DecisionContinue  = "continue"
	DecisionStop      = "stop"
)

func loadDecisions(stateDir, today string) map[string]string {
	var d decisions
	data, err := os.ReadFile(filepath.Join(stateDir, "runtime", decisionsFileName))
	if err != nil || json.Unmarshal(data, &d) != nil || d.Date != today {
		return nil
	}
	return d.Staff
}

var usageMu sync.Mutex

// Assign applies daily caps. A session key keeps the staff it was first given
// today, so retries and recoveries of one session are counted once. Capped
// staff fall through their fallback chain; the last link is used even when it
// is itself capped, so work is never left without a runner. Reaching notify_at
// files one ask for Joey and changes no routing; his "continue" lifts daily_cap
// for the day and his "stop" halts the staff at once.
// routesThrough reports whether staff `to` is `from` itself or is reachable
// from it by following fallback links -- i.e. whether a session already
// assigned to `to` is still consistent with the roster naming `from` for its
// role.
func (r *Roster) routesThrough(from, to string) bool {
	cur := from
	for hop := 0; hop <= maxFallback; hop++ {
		if cur == to {
			return true
		}
		s, ok := r.Staff[cur]
		if !ok || s.Fallback == "" {
			return false
		}
		cur = s.Fallback
	}
	return false
}

func (r *Roster) Assign(stateDir, name, sessionKey string, now time.Time) (string, error) {
	if !r.Has(name) {
		return "", fmt.Errorf("roster staff %q is not defined", name)
	}
	usageMu.Lock()
	defer usageMu.Unlock()

	path := filepath.Join(stateDir, "runtime", usageFileName)
	today := now.Local().Format("2006-01-02")
	u := usage{}
	if data, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(data, &u)
	}
	if u.Date != today || u.Counts == nil || u.Seen == nil {
		u = usage{Date: today, Counts: map[string]int{}, Seen: map[string]string{}}
	}
	// Seen exists so one session counts once against daily_cap. It must not
	// also pin routing across a roster edit: roster.yaml promises "edit, save,
	// done (no restart)", and before 2026-09-18 this check accepted any prior
	// staff, so a conversation already seen that day kept the staff it first
	// got and silently ignored a mid-day reseat until midnight. Accept the
	// prior choice only while the roster still routes this role through it --
	// a tripped daily_cap still leaves a running session where it is, but a
	// reseat changes `name` and lets the session move.
	// A "stop" decision halts the staff at once, so a session already on a stopped staff
	// re-derives instead of keeping it (found 2026-09-27: a recovery session went back to a
	// Cursor seat stopped for a spent plan and lost an attempt). A tripped daily_cap alone
	// still leaves a running session where it is.
	decided := loadDecisions(stateDir, today)
	if prior, ok := u.Seen[sessionKey]; ok && sessionKey != "" && r.Has(prior) && r.routesThrough(name, prior) && decided[prior] != DecisionStop {
		return prior, nil
	}

	chosen := r.follow(name, u.Counts, decided)
	u.Counts[chosen]++
	if s := r.Staff[chosen]; s.NotifyAt > 0 && u.Counts[chosen] >= s.NotifyAt && !u.Notified[chosen] && decided[chosen] == "" {
		if err := writeCapAsk(stateDir, chosen, s, u.Counts[chosen], now); err == nil {
			if u.Notified == nil {
				u.Notified = map[string]bool{}
			}
			u.Notified[chosen] = true
		}
	}
	if sessionKey != "" {
		u.Seen[sessionKey] = chosen
	}
	data, err := json.MarshalIndent(u, "", "  ")
	if err != nil {
		return chosen, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return chosen, err
	}
	return chosen, fsx.AtomicWriteFile(path, data, 0o600)
}

// writeCapAsk parks one task on Joey (joey_ask choice). The workspace pump puts
// it on Helm and returns his answer into the same document, which resumes and
// records the decision file Assign reads.
func writeCapAsk(stateDir, name string, s Staff, count int, now time.Time) error {
	day := now.Local().Format("2006-01-02")
	id := "tasks-" + now.Local().Format("20060102") + "-roster-cap-" + name
	stamp := now.UTC().Format(time.RFC3339)
	halt := "There is no higher limit set, so it keeps going until you say stop."
	stopEffect := name + " stops for the rest of " + day + " and its work goes to " + s.Fallback + "."
	if s.DailyCap > 0 {
		halt = fmt.Sprintf("If you do not answer, %s keeps working until %d sessions today, then stops and its work goes to %s.", name, s.DailyCap, s.Fallback)
	}
	if s.Fallback == "" {
		stopEffect = name + " has no fallback staff, so choosing this changes nothing until one is set in roster.yaml."
	}
	text := fmt.Sprintf(`---
id: %[1]s
title: "Roster: %[2]s reached %[3]d sessions on %[4]s"
status: waiting
created_at: "%[5]s"
updated_at: "%[5]s"
review_required: false
attempt: 0
notify:
  enabled: false
waiting_for: "Joey's call on %[2]s usage for %[4]s"
joey_ask:
  kind: choice
  prompt: "Werkrby staff '%[2]s' (%[6]s) has used %[3]d sessions today, the point where you asked to be told. Let it keep going today, or stop it for today?"
  context: "Nothing is paused: %[2]s is still taking work while this card is open. %[7]s This card is only so you know what is going on and can overrule it."
  how_to: "Tap one option."
  options:
    - label: "Keep going today"
      value: "continue"
      effect: "%[2]s keeps working for the rest of %[4]s with no halt at the higher limit."
    - label: "Stop it for today"
      value: "stop"
      effect: "%[8]s"
---

# Roster: %[2]s reached %[3]d sessions on %[4]s

## Objective

Record Joey's answer so the roster applies it. Filed automatically by Werkrby when roster staff %[2]s reached its notify_at count.

## Scope

- Joey's answer is in the newest JOEY ANSWERED entry under Progress: the tapped value is continue or stop.
- If today's local date is still %[4]s: write the file runtime/roster-decisions.json inside this workspace's .spynel directory with exactly {"date": "%[4]s", "staff": {"%[2]s": "<continue or stop>"}}. If that file already holds date %[4]s, keep its other staff entries and set only %[2]s.
- If the date has passed, change nothing; the count already reset.
- Do nothing else. Do not edit roster.yaml.

## Acceptance criteria

- The decision file holds Joey's value for %[2]s on %[4]s, or Progress says the date had passed.

## Context

- Joey, 2026-09-17: "Instead have it escalate to me when that cap happen so I know what's going on.. but y'all keep working. And then a higher cap to halt if it's hit and I haven't responded."

## Progress

- %[5]s - Filed by the Werkrby roster: %[2]s at %[3]d sessions (notify_at %[9]d, daily_cap %[10]d).
`, id, name, count, day, stamp, s.Runner, halt, stopEffect, s.NotifyAt, s.DailyCap)
	dir := filepath.Join(stateDir, "tasks", "waiting")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	return fsx.AtomicWriteFile(filepath.Join(dir, id+".md"), []byte(text), 0o600)
}
