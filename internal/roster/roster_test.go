package roster

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

const sample = `
staff:
  implementer: {runner: opencode}
  thinker: {runner: pi}
  principal:
    runner: claude
    args: ["--model", "opus", "--effort", "xhigh"]
    daily_cap: 2
    fallback: thinker
roles:
  task_implementation: implementer
  task_review: thinker
  goal_planning: principal
escalation: {after_attempt: 3, staff: principal}
`

func write(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(Path(dir), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestLoadMissingIsLegacy(t *testing.T) {
	r, err := Load(t.TempDir())
	if r != nil || err != nil {
		t.Fatalf("got %v, %v", r, err)
	}
}

func TestLoadRejectsBadRosters(t *testing.T) {
	for name, body := range map[string]string{
		"no staff":         "roles: {}",
		"unknown role":     "staff: {a: {runner: pi}}\nroles: {task_review: b}",
		"unknown fallback": "staff: {a: {runner: pi, fallback: b}}",
		"cap no fallback":  "staff: {a: {runner: pi, daily_cap: 1}}",
		"bad runner":       "staff: {a: {runner: \"pi; rm\"}}",
		"bad escalation":   "staff: {a: {runner: pi}}\nescalation: {after_attempt: 0, staff: a}",
	} {
		if _, err := Load(write(t, body)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestModelAndExpand(t *testing.T) {
	dir := write(t, sample)
	r, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Model("implementer"); got != "opencode" {
		t.Fatalf("plain staff model = %q", got)
	}
	if got := r.Model("principal"); got != "@principal" {
		t.Fatalf("staff with args model = %q", got)
	}
	kind, args, err := Expand(dir, "@principal")
	if err != nil || kind != "claude" || len(args) != 4 || args[1] != "opus" {
		t.Fatalf("expand = %q %v %v", kind, args, err)
	}
	if kind, args, err := Expand(dir, "kimi"); kind != "kimi" || args != nil || err != nil {
		t.Fatalf("passthrough = %q %v %v", kind, args, err)
	}
	if _, _, err := Expand(dir, "@nobody"); err == nil {
		t.Fatal("unknown staff must fail")
	}
}

func TestAssignCapsCountOncePerSessionAndResetDaily(t *testing.T) {
	dir := write(t, sample)
	r, _ := Load(dir)
	day := time.Date(2026, 9, 17, 10, 0, 0, 0, time.Local)
	for _, step := range []struct{ key, want string }{
		{"s1", "principal"},
		{"s1", "principal"}, // retry of the same session is not a second use
		{"s2", "principal"},
		{"s3", "thinker"}, // cap of 2 reached
		{"s1", "principal"},
	} {
		got, err := r.Assign(dir, "principal", step.key, day)
		if err != nil || got != step.want {
			t.Fatalf("%s: got %q, %v; want %q", step.key, got, err, step.want)
		}
	}
	if got, _ := r.Assign(dir, "principal", "s4", day.Add(24*time.Hour)); got != "principal" {
		t.Fatalf("next day = %q", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "runtime", usageFileName)); err != nil {
		t.Fatal(err)
	}
}

// A mid-day roster reseat must reach a conversation that already ran today.
// Before 2026-09-18 the Seen map was consulted before the role was resolved,
// so the live Telegram chat kept the staff it first got that day and ignored
// Joey's reseat until midnight.
func TestAssignFollowsAMidDayReseat(t *testing.T) {
	dir := write(t, sample)
	r, _ := Load(dir)
	day := time.Date(2026, 9, 17, 10, 0, 0, 0, time.Local)
	key := "chat:telegram:TG-1"

	if got, err := r.Assign(dir, "thinker", key, day); err != nil || got != "thinker" {
		t.Fatalf("first assign = %q, %v", got, err)
	}
	// The role is reseated to implementer; the same session must follow.
	if got, err := r.Assign(dir, "implementer", key, day); err != nil || got != "implementer" {
		t.Fatalf("after reseat = %q, %v; want implementer", got, err)
	}
	// And it stays put while the roster keeps naming that staff.
	if got, _ := r.Assign(dir, "implementer", key, day); got != "implementer" {
		t.Fatalf("stickiness lost: %q", got)
	}
}

// A session already moved to a fallback stays there while the roster still
// routes its role through that fallback, so a tripped cap never yanks it back
// and never double-counts it.
func TestAssignKeepsASessionOnItsFallback(t *testing.T) {
	dir := write(t, sample)
	r, _ := Load(dir)
	day := time.Date(2026, 9, 17, 10, 0, 0, 0, time.Local)

	for _, k := range []string{"s1", "s2"} {
		if got, _ := r.Assign(dir, "principal", k, day); got != "principal" {
			t.Fatalf("%s = %q", k, got)
		}
	}
	// Cap of 2 is now spent, so a new session falls back.
	if got, _ := r.Assign(dir, "principal", "s3", day); got != "thinker" {
		t.Fatalf("s3 = %q, want thinker", got)
	}
	// s3 repeats: principal still falls back to thinker, so s3 is unchanged
	// and is not counted a second time.
	before := readCount(t, dir, "thinker")
	if got, _ := r.Assign(dir, "principal", "s3", day); got != "thinker" {
		t.Fatalf("s3 repeat = %q", got)
	}
	if after := readCount(t, dir, "thinker"); after != before {
		t.Fatalf("thinker double-counted: %d -> %d", before, after)
	}
}

func readCount(t *testing.T, dir, staff string) int {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "runtime", usageFileName))
	if err != nil {
		t.Fatal(err)
	}
	var u struct {
		Counts map[string]int `json:"counts"`
	}
	if err := json.Unmarshal(data, &u); err != nil {
		t.Fatal(err)
	}
	return u.Counts[staff]
}

const notifySample = `
staff:
  thinker:
    runner: opencode
  principal:
    runner: claude
    notify_at: 2
    daily_cap: 4
    fallback: thinker
roles:
  goal_planning: principal
`

func TestNotifyAtAsksOnceAndJoeyDecides(t *testing.T) {
	day := time.Date(2026, 9, 17, 10, 0, 0, 0, time.Local)
	ask := func(dir string) string {
		return filepath.Join(dir, "tasks", "waiting", "tasks-20260917-roster-cap-principal.md")
	}
	decide := func(dir, value string) {
		data := `{"date":"2026-09-17","staff":{"principal":"` + value + `"}}`
		if err := os.WriteFile(filepath.Join(dir, "runtime", decisionsFileName), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	run := func(dir string, r *Roster, keys []string, want string) {
		t.Helper()
		for _, key := range keys {
			if got, err := r.Assign(dir, "principal", key, day); err != nil || got != want {
				t.Fatalf("%s: got %q, %v; want %q", key, got, err, want)
			}
		}
	}

	// No answer: keeps working past notify_at, halts at daily_cap.
	dir := write(t, notifySample)
	r, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	run(dir, r, []string{"a1"}, "principal")
	if _, err := os.Stat(ask(dir)); err == nil {
		t.Fatal("asked before notify_at")
	}
	run(dir, r, []string{"a2"}, "principal")
	data, err := os.ReadFile(ask(dir))
	if err != nil || !strings.Contains(string(data), "joey_ask:") || !strings.Contains(string(data), `value: "stop"`) {
		t.Fatalf("ask not filed: %v", err)
	}
	var front map[string]any
	if err := yaml.Unmarshal([]byte(strings.SplitN(string(data), "---\n", 3)[1]), &front); err != nil {
		t.Fatalf("ask front matter: %v", err)
	}
	os.Remove(ask(dir))
	run(dir, r, []string{"a3", "a4"}, "principal")
	if _, err := os.Stat(ask(dir)); err == nil {
		t.Fatal("asked twice in one day")
	}
	run(dir, r, []string{"a5"}, "thinker")

	// "continue" lifts daily_cap; "stop" halts at once.
	decide(dir, DecisionContinue)
	run(dir, r, []string{"a6"}, "principal")
	decide(dir, DecisionStop)
	run(dir, r, []string{"a7"}, "thinker")
	// A session already given principal today (a6) must move too: "stop" halts the staff at
	// once. Found live 2026-09-27: a recovery session for an attempt that began on a Cursor seat
	// went back to that seat after its stop decision, hit the spent plan and lost an attempt.
	run(dir, r, []string{"a6"}, "thinker")

	if _, err := Load(write(t, strings.Replace(notifySample, "notify_at: 2", "notify_at: 4", 1))); err == nil {
		t.Fatal("notify_at at or above daily_cap must fail")
	}
}

func TestRoutingBlock(t *testing.T) {
	base := `
staff:
  implementer: {runner: cursor}
  architect: {runner: agy}
`
	valid := base + `routing:
  - {domain: "fleet ops / infra", staff: implementer}
  - {domain: "FFXIV plugin C#", note: "omit, default implementer"}
  - {domain: "research / long-context", staff: architect, held_until: "Kimi quota returns"}
`
	if _, err := Load(write(t, valid)); err != nil {
		t.Fatalf("valid routing rejected: %v", err)
	}
	for name, body := range map[string]string{
		"unknown staff":       base + "routing:\n  - {domain: x, staff: ghost}\n",
		"held needs fallback": base + "routing:\n  - {domain: x, held_until: \"someday\"}\n",
		"duplicate domain":    base + "routing:\n  - {domain: x, staff: implementer}\n  - {domain: x, staff: architect}\n",
		"empty domain":        base + "routing:\n  - {domain: \"\", staff: implementer}\n",
	} {
		if _, err := Load(write(t, body)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	// Missing block stays legacy-compatible.
	r, err := Load(write(t, base))
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Routing) != 0 {
		t.Fatalf("missing routing block = %v, want empty", r.Routing)
	}
	// Unknown future fields are ignored so an un-rebuilt binary tolerates the block.
	legacy := `
staff:
  implementer: {runner: cursor}
routing:
  - {domain: x, staff: implementer, future_field: 1}
`
	if _, err := Load(write(t, legacy)); err != nil {
		t.Fatalf("forward-compat routing rejected: %v", err)
	}
}

func TestEscalationLadder(t *testing.T) {
	base := `
staff:
  implementer: {runner: cursor}
  architect: {runner: agy}
  principal: {runner: claude}
`
	ladder, err := Load(write(t, base+`escalation:
  - {after_attempt: 1, staff: architect}
  - {after_attempt: 2, staff: principal}
`))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		attempt int
		pinned  bool
		want    string
	}{
		{1, false, ""}, {2, false, "architect"}, {3, false, "principal"}, {9, false, "principal"},
		{2, true, ""}, // a staff pin outranks the lower rung
		{3, true, "principal"},
	} {
		if got := ladder.EscalationFor(c.attempt, c.pinned); got != c.want {
			t.Fatalf("attempt %d pinned %v = %q, want %q", c.attempt, c.pinned, got, c.want)
		}
	}
	single, err := Load(write(t, base+"escalation:\n  after_attempt: 2\n  staff: principal\n"))
	if err != nil {
		t.Fatal(err)
	}
	if single.EscalationFor(2, false) != "" || single.EscalationFor(3, true) != "principal" {
		t.Fatal("single mapping form must keep its old behavior")
	}
	if _, err := Load(write(t, base+"escalation:\n  - {after_attempt: 2, staff: architect}\n  - {after_attempt: 2, staff: principal}\n")); err == nil {
		t.Fatal("duplicate rung must fail")
	}
}

// A seat can name where its tasks go once an attempt has failed (escalate_after / escalate_to).
// Unlike the global ladder it is per seat, so a retry stays inside the job's own lane: the
// 2026-09-28 ladder sent every attempt-3 task to kimi whatever its job, and Joey took it out.
func TestSeatEscalation(t *testing.T) {
	r, err := Load(write(t, `
staff:
  ops: {runner: agy, escalate_after: 1, escalate_to: architect}
  architect: {runner: pi}
  implementer: {runner: pi}
  a: {runner: pi, escalate_after: 1, escalate_to: b}
  b: {runner: pi, escalate_after: 2, escalate_to: c}
  c: {runner: pi}
`))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name    string
		attempt int
		want    string
	}{
		{"ops", 1, "ops"}, {"ops", 2, "architect"}, {"ops", 9, "architect"},
		{"architect", 5, "architect"}, {"implementer", 3, "implementer"},
		{"a", 1, "a"}, {"a", 2, "b"}, {"a", 3, "c"}, {"b", 2, "b"}, {"b", 3, "c"},
		{"nobody", 7, "nobody"}, // unknown names pass through untouched
	} {
		if got := r.EscalatedStaff(c.name, c.attempt); got != c.want {
			t.Errorf("EscalatedStaff(%s, %d) = %q, want %q", c.name, c.attempt, got, c.want)
		}
	}
}

func TestSeatEscalationValidation(t *testing.T) {
	for name, body := range map[string]string{
		"target without after": "staff: {a: {runner: pi, escalate_to: b}, b: {runner: pi}}",
		"after without target": "staff: {a: {runner: pi, escalate_after: 1}, b: {runner: pi}}",
		"after below one":      "staff: {a: {runner: pi, escalate_after: 0, escalate_to: b}, b: {runner: pi}}",
		"negative after":       "staff: {a: {runner: pi, escalate_after: -1, escalate_to: b}, b: {runner: pi}}",
		"undefined target":     "staff: {a: {runner: pi, escalate_after: 1, escalate_to: nobody}}",
		"escalates to itself":  "staff: {a: {runner: pi, escalate_after: 1, escalate_to: a}}",
		"cycle":                "staff: {a: {runner: pi, escalate_after: 1, escalate_to: b}, b: {runner: pi, escalate_after: 2, escalate_to: a}}",
	} {
		if _, err := Load(write(t, body)); err == nil {
			t.Errorf("%s: want a load error", name)
		}
	}
}
