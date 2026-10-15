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
		"no staff":            "roles: {}",
		"unknown role":        "staff: {a: {runner: pi}}\nroles: {task_review: b}",
		"unknown fallback":    "staff: {a: {runner: pi, fallback: b}}",
		"cap no fallback":     "staff: {a: {runner: pi, daily_cap: 1}}",
		"bad runner":          "staff: {a: {runner: \"pi; rm\"}}",
		"bad escalation":      "staff: {a: {runner: pi}}\nescalation: {after_attempt: 0, staff: a}",
		"headless not claude": "staff: {a: {runner: pi, headless: true}}",
		// An explicit outcome is read when the worker process exits; a pane seat has no process exit
		// to read it at, only a screen that looks idle.
		"explicit outcome not headless": "staff: {a: {runner: claude, explicit_outcome: true}}",
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

// A seat's family comes from the roster (`family:`) or, failing that, from its --model launch
// argument. Review is meant to be a different family from the implementer's.
func TestFamilyOf(t *testing.T) {
	r, err := Load(write(t, `
staff:
  flash: {runner: pi, args: ["--model", "zai/glm-5.3-flash", "--thinking", "high"]}
  bal: {runner: pi, args: ["--model", "zai-balance/glm-5.3-flash"]}
  gem: {runner: agy, args: ["--model", "gemini-3.8-flash-high"]}
  son: {runner: claude, args: ["--model", "sonnet", "--effort", "high"]}
  hai: {runner: claude, args: ["--model=haiku"]}
  kim: {runner: kimi, args: ["--model", "kimi-code/k3"]}
  explicit: {runner: hermes, family: Gemini}
  bare: {runner: hermes}
`))
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{
		"flash": "glm", "bal": "glm", "gem": "gemini", "son": "claude", "hai": "claude", "kim": "kimi",
		"explicit": "gemini", "bare": "", "missing": "",
	} {
		if got := r.FamilyOf(name); got != want {
			t.Errorf("FamilyOf(%s) = %q, want %q", name, got, want)
		}
	}
}

func TestPickDifferentFamily(t *testing.T) {
	r, err := Load(write(t, `
staff:
  gem: {runner: agy, args: ["--model", "gemini-3.8-flash-high"]}
  glm: {runner: pi, args: ["--model", "zai/glm-5.3-flash"]}
  son: {runner: claude, args: ["--model", "sonnet"]}
  bare: {runner: hermes}
role_alternates:
  task_review_medium: [glm, son]
roles:
  task_review_medium: gem
`))
	if err != nil {
		t.Fatal(err)
	}
	alts := r.RoleAlternates["task_review_medium"]
	for _, c := range []struct{ name, main, impl, want string }{
		{"different family keeps the main", "gem", "glm", "gem"},
		{"same family takes the first different alternate", "gem", "gem", "glm"},
		{"an alternate of the same family is skipped", "glm", "glm", "son"}, // main glm, impl glm: alternates are glm (same, skipped) then son
		{"unknown implementer keeps the main", "gem", "", "gem"},
		{"implementer with no family keeps the main", "gem", "bare", "gem"},
		{"unknown implementer seat keeps the main", "gem", "nobody", "gem"},
	} {
		if got := r.PickDifferentFamily(c.main, alts, c.impl); got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
	// Nothing differs: stay on the main rather than invent a seat.
	if got := r.PickDifferentFamily("gem", []string{"gem"}, "gem"); got != "gem" {
		t.Errorf("no different alternate: got %q, want gem", got)
	}
}

func TestRoleAlternatesValidation(t *testing.T) {
	for name, body := range map[string]string{
		"undefined alternate": "staff: {a: {runner: pi}}\nrole_alternates: {task_review: [nobody]}",
		"empty alternate":     "staff: {a: {runner: pi}}\nrole_alternates: {task_review: [\"\"]}",
	} {
		if _, err := Load(write(t, body)); err == nil {
			t.Errorf("%s: want a load error", name)
		}
	}
	if _, err := Load(write(t, "staff: {a: {runner: pi}}\nrole_alternates: {task_review: [a]}")); err != nil {
		t.Errorf("valid alternates rejected: %v", err)
	}
}

// The seat that actually ran an implementation is remembered across days (the daily usage count
// forgets at midnight), so a review the next morning still knows who wrote the work.
func TestImplementerIsRemembered(t *testing.T) {
	dir := t.TempDir()
	if _, ok := ImplementedBy(dir, "t1.md"); ok {
		t.Fatal("nothing recorded yet")
	}
	day1 := time.Date(2026, 10, 5, 12, 0, 0, 0, time.Local)
	if err := RecordImplementer(dir, "t1.md", "ops", day1); err != nil {
		t.Fatal(err)
	}
	if err := RecordImplementer(dir, "t2.md", "architect", day1); err != nil {
		t.Fatal(err)
	}
	if seat, ok := ImplementedBy(dir, "t1.md"); !ok || seat != "ops" {
		t.Fatalf("t1 = %q %v", seat, ok)
	}
	// A retry on another seat replaces the record; the latest implementer is who wrote the work.
	if err := RecordImplementer(dir, "t1.md", "architect", day1.Add(26*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if seat, _ := ImplementedBy(dir, "t1.md"); seat != "architect" {
		t.Fatalf("after retry t1 = %q", seat)
	}
	// Old records are pruned so the file cannot grow forever.
	if err := RecordImplementer(dir, "t3.md", "ops", day1.Add(40*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, ok := ImplementedBy(dir, "t2.md"); ok {
		t.Fatal("a 40-day-old record should be pruned")
	}
	if seat, ok := ImplementedBy(dir, "t3.md"); !ok || seat != "ops" {
		t.Fatalf("t3 = %q %v", seat, ok)
	}
}

func TestHeadlessStaffKeepsItsNameAndExpands(t *testing.T) {
	dir := write(t, "staff:\n  credits: {runner: claude, headless: true}\n  plain: {runner: claude}\n  argsed: {runner: claude, headless: true, args: [\"--model\", \"sonnet\"]}\n")
	r, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Model("credits"); got != "@credits" {
		t.Fatalf("headless staff without args = %q, want @credits so the harness can see it is headless", got)
	}
	if got := r.Model("plain"); got != "claude" {
		t.Fatalf("plain staff = %q", got)
	}
	s, ok, err := ExpandStaff(dir, "@argsed")
	if err != nil || !ok || !s.Headless || s.Runner != "claude" || len(s.Args) != 2 || s.Args[1] != "sonnet" {
		t.Fatalf("ExpandStaff = %+v %v %v", s, ok, err)
	}
	if _, ok, err := ExpandStaff(dir, "claude"); ok || err != nil {
		t.Fatalf("a bare runner is not staff: ok=%v err=%v", ok, err)
	}
	if _, ok, err := ExpandStaff(dir, "@nobody"); !ok || err == nil {
		t.Fatalf("unknown staff: ok=%v err=%v", ok, err)
	}
}

func TestExplicitOutcomeIsPerSeatAndOffByDefault(t *testing.T) {
	dir := write(t, "staff:\n  pilot: {runner: claude, headless: true, explicit_outcome: true}\n  credits: {runner: claude, headless: true}\n")
	pilot, ok, err := ExpandStaff(dir, "@pilot")
	if err != nil || !ok || !pilot.ExplicitOutcome {
		t.Fatalf("pilot seat = %+v %v %v, want explicit_outcome on", pilot, ok, err)
	}
	credits, ok, err := ExpandStaff(dir, "@credits")
	if err != nil || !ok || credits.ExplicitOutcome {
		t.Fatalf("unflagged headless seat = %+v %v %v, want explicit_outcome off", credits, ok, err)
	}
}

func TestLoadRefusesMissingLaunchPath(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-mcp.json")
	body := "staff:\n  implementer:\n    runner: claude\n    args: [\"--model\", \"sonnet\", \"--mcp-config\", \"" + missing + "\"]\nroles:\n  task_implementation: implementer\n"
	dir := write(t, body)
	_, err := Load(dir)
	if err == nil || !strings.Contains(err.Error(), missing) || !strings.Contains(err.Error(), "implementer") {
		t.Fatalf("Load with a missing --mcp-config = %v, want an error naming staff and %s", err, missing)
	}

	// A path in the flag=value form, and a bare absolute argument, are checked too.
	for _, arg := range []string{`"--settings=` + missing + `"`, `"` + missing + `"`} {
		dir := write(t, "staff:\n  implementer:\n    runner: claude\n    args: ["+arg+"]\n")
		if _, err := Load(dir); err == nil || !strings.Contains(err.Error(), missing) {
			t.Errorf("Load with arg %s = %v, want an error naming %s", arg, err, missing)
		}
	}

	// An existing file, and non-path values such as a model id, load fine.
	present := filepath.Join(t.TempDir(), "mcp.json")
	if err := os.WriteFile(present, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	dir = write(t, "staff:\n  implementer:\n    runner: claude\n    args: [\"--model\", \"zai/glm-5.3\", \"--mcp-config\", \""+present+"\"]\n")
	if _, err := Load(dir); err != nil {
		t.Fatalf("Load with an existing path = %v", err)
	}
}

func TestReloadKeepsPreviousRosterWhenNewOneIsRefused(t *testing.T) {
	var logged []string
	SetRefusalLogger(func(msg string) { logged = append(logged, msg) })
	t.Cleanup(func() { SetRefusalLogger(nil) })

	present := filepath.Join(t.TempDir(), "mcp.json")
	if err := os.WriteFile(present, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	good := "staff:\n  implementer:\n    runner: claude\n    args: [\"--mcp-config\", \"" + present + "\"]\nroles:\n  task_implementation: implementer\n"
	dir := write(t, good)
	if _, err := Load(dir); err != nil {
		t.Fatal(err)
	}

	missing := filepath.Join(t.TempDir(), "gone.json")
	bad := map[string]string{
		"missing path":  strings.Replace(good, present, missing, 1),
		"unknown staff": strings.Replace(good, "task_implementation: implementer", "task_implementation: nobody", 1),
	}
	for name, body := range bad {
		logged = nil
		if err := os.WriteFile(Path(dir), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			r, err := Load(dir)
			if err != nil || r == nil || !r.Has("implementer") || r.Staff["implementer"].Args[1] != present {
				t.Fatalf("%s: reload = %+v, %v; want the previous roster", name, r, err)
			}
		}
		if len(logged) != 1 {
			t.Fatalf("%s: want exactly one refusal line for three reloads, got %d: %v", name, len(logged), logged)
		}
		if name == "missing path" && !strings.Contains(logged[0], missing) {
			t.Errorf("refusal line %q does not name the missing path %s", logged[0], missing)
		}
	}
}

func TestHeadlessKimiStaffLoadsAndExpands(t *testing.T) {
	dir := write(t, "staff:\n  researcher: {runner: kimi, headless: true, args: [\"--model\", \"kimi-code/k3\"]}\n")
	s, ok, err := ExpandStaff(dir, "@researcher")
	if err != nil || !ok || !s.Headless || s.Runner != "kimi" {
		t.Fatalf("ExpandStaff = %+v %v %v, want a headless kimi seat", s, ok, err)
	}
}
