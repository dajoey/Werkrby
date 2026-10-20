package admission

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/roster"
)

var testNow = time.Date(2026, 10, 14, 15, 0, 0, 0, time.UTC)

const testRoster = `
staff:
  impl: {runner: pi, args: ["--model", "zai-coding-plan/glm-5.3"], fallback: backup}
  backup: {runner: opencode, args: ["--model", "kimi-for-coding/kimi-k3"]}
  lone: {runner: pi, args: ["--model", "zai-coding-plan/glm-5.3"]}
  venice: {runner: hermes}
  veniced: {runner: pi, args: ["--model", "zai-coding-plan/glm-5.3"], fallback: venice}
  claude-main: {runner: claude, args: ["--model", "sonnet"]}
roles:
  task_implementation: impl
`

func ts(offset time.Duration) string { return testNow.Add(offset).UTC().Format(time.RFC3339) }

// plan builds one snapshot plan entry from window name -> pct (nil pct = unknown) and a reset in two hours.
func plan(asOf time.Time, windows map[string]any) map[string]any {
	entry := map[string]any{"ok": true, "as_of": asOf.UTC().Format(time.RFC3339)}
	for name, pct := range windows {
		entry[name] = map[string]any{"pct": pct, "resets_at": ts(2 * time.Hour)}
	}
	return entry
}

type fixture struct {
	t    *testing.T
	dir  string
	snap string
	a    *Admission
}

func newFixture(t *testing.T, rosterYAML string, plans map[string]any, extra map[string]any) *fixture {
	t.Helper()
	dir := t.TempDir()
	if rosterYAML != "" {
		if err := os.WriteFile(roster.Path(dir), []byte(rosterYAML), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	f := &fixture{t: t, dir: dir, snap: filepath.Join(dir, "snapshot.json")}
	f.setSnapshot(plans, extra)
	f.a = New(dir, f.snap)
	return f
}

func (f *fixture) setSnapshot(plans map[string]any, extra map[string]any) {
	f.t.Helper()
	snap := map[string]any{"generated_at": testNow.UTC().Format(time.RFC3339), "plans": plans}
	for k, v := range extra {
		snap[k] = v
	}
	data, _ := json.Marshal(snap)
	if err := os.WriteFile(f.snap, data, 0o600); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) writeRuntime(name string, v any) {
	f.t.Helper()
	if err := os.MkdirAll(filepath.Join(f.dir, "runtime"), 0o700); err != nil {
		f.t.Fatal(err)
	}
	data, _ := json.Marshal(v)
	if err := os.WriteFile(filepath.Join(f.dir, "runtime", name), data, 0o600); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) decide(seat string, constraints ...string) Decision {
	return f.a.Decide(Request{Kind: KindTaskImplementation, Doc: "tasks-1", Asked: seat, Now: testNow, Constraints: constraints})
}

func wantRun(t *testing.T, d Decision, seat string) {
	t.Helper()
	if d.Verdict != Run || d.Seat != seat {
		t.Fatalf("want run on %q, got %s on %q: %s (trail %+v)", seat, d.Verdict, d.Seat, d.Reason, d.Trail)
	}
}

func wantWait(t *testing.T, d Decision, reasonPart string) {
	t.Helper()
	if d.Verdict != Wait || !strings.Contains(d.Reason, reasonPart) {
		t.Fatalf("want wait mentioning %q, got %s on %q: %s", reasonPart, d.Verdict, d.Seat, d.Reason)
	}
}

func TestDecideRunsWhenPlanHasRoom(t *testing.T) {
	f := newFixture(t, testRoster, map[string]any{"zai": plan(testNow, map[string]any{"weekly": 40})}, nil)
	d := f.decide("impl")
	wantRun(t, d, "impl")
	if d.Plan != "zai" || d.Inputs["zai"] == "" {
		t.Fatalf("plan %q inputs %v", d.Plan, d.Inputs)
	}
}

func TestDecideWaitsForResetWhenPlanFullWithoutFallback(t *testing.T) {
	f := newFixture(t, testRoster, map[string]any{"zai": plan(testNow, map[string]any{"weekly": 100})}, nil)
	d := f.decide("lone")
	wantWait(t, d, "full")
	if !d.Until.Equal(testNow.Add(2 * time.Hour)) {
		t.Fatalf("until %v, want the window reset", d.Until)
	}
}

func TestDecideFallsBackFromAFullPlanToASeatWithRoom(t *testing.T) {
	f := newFixture(t, testRoster, map[string]any{
		"zai":         plan(testNow, map[string]any{"weekly": 100}),
		"kimi_coding": plan(testNow, map[string]any{"rolling": 10}),
	}, nil)
	d := f.decide("impl")
	wantRun(t, d, "backup")
	if len(d.Trail) == 0 || d.Trail[0].Seat != "impl" || !strings.Contains(d.Trail[0].Note, "full") {
		t.Fatalf("trail %+v", d.Trail)
	}
}

func TestDecideNeverFallsBackToAnUnmeteredSeat(t *testing.T) {
	f := newFixture(t, testRoster, map[string]any{"zai": plan(testNow, map[string]any{"weekly": 100})}, nil)
	wantWait(t, f.decide("veniced"), "unmetered")
}

func TestDecideWaitsWhenTheFallbackPlanIsFullToo(t *testing.T) {
	f := newFixture(t, testRoster, map[string]any{
		"zai":         map[string]any{"ok": true, "as_of": ts(0), "weekly": map[string]any{"pct": 100, "resets_at": ts(3 * time.Hour)}},
		"kimi_coding": map[string]any{"ok": true, "as_of": ts(0), "monthly": map[string]any{"pct": 100, "resets_at": ts(time.Hour)}},
	}, nil)
	d := f.decide("impl")
	wantWait(t, d, "full")
	if !d.Until.Equal(testNow.Add(time.Hour)) {
		t.Fatalf("until %v, want the earliest reset in the chain", d.Until)
	}
}

func TestDecideHoldWaitsAndDoesNotFallBack(t *testing.T) {
	f := newFixture(t, testRoster, map[string]any{
		"zai":         plan(testNow, map[string]any{"weekly": 10}),
		"kimi_coding": plan(testNow, map[string]any{"rolling": 10}),
	}, nil)
	f.writeRuntime("roster-holds.json", map[string]any{
		"updated_at": ts(-time.Minute),
		"staff":      map[string]any{"impl": map[string]string{"until": ts(time.Hour), "reason": "zai-pace"}},
	})
	d := f.decide("impl")
	wantWait(t, d, "hold")
	if !d.Until.Equal(testNow.Add(time.Hour)) || !strings.Contains(d.Reason, "zai-pace") {
		t.Fatalf("until %v reason %q", d.Until, d.Reason)
	}
}

func TestDecideFollowsAStopDecisionToTheFallback(t *testing.T) {
	f := newFixture(t, testRoster, map[string]any{
		"zai":         plan(testNow, map[string]any{"weekly": 10}),
		"kimi_coding": plan(testNow, map[string]any{"rolling": 10}),
	}, nil)
	f.writeRuntime("roster-decisions.json", map[string]any{
		"date":  testNow.Local().Format("2006-01-02"),
		"staff": map[string]string{"impl": "stop"},
	})
	d := f.decide("impl")
	wantRun(t, d, "backup")
	if len(d.Trail) != 1 || d.Trail[0].Seat != "impl" || !strings.Contains(d.Trail[0].Note, "stopped") {
		t.Fatalf("trail %+v", d.Trail)
	}
}

func TestDecideKeepsAFullWindowFullUntilItReadsUnderNinety(t *testing.T) {
	f := newFixture(t, testRoster, map[string]any{"zai": plan(testNow, map[string]any{"weekly": 100})}, nil)
	wantWait(t, f.decide("lone"), "full")
	f.setSnapshot(map[string]any{"zai": plan(testNow, map[string]any{"weekly": 95})}, nil)
	wantWait(t, f.decide("lone"), "full")
	f.setSnapshot(map[string]any{"zai": plan(testNow, map[string]any{"weekly": 85})}, nil)
	wantRun(t, f.decide("lone"), "lone")
}

func TestDecideUnknownReadingIsReportedNotAWall(t *testing.T) {
	for name, entry := range map[string]map[string]any{
		"collector failed": {"ok": false, "as_of": ts(0), "weekly": map[string]any{"pct": nil, "resets_at": nil}},
		"stale reading":    plan(testNow.Add(-3*time.Hour), map[string]any{"weekly": 100}),
		"missing plan":     nil,
	} {
		plans := map[string]any{}
		if entry != nil {
			plans["zai"] = entry
		}
		f := newFixture(t, testRoster, plans, nil)
		d := f.decide("lone")
		if d.Verdict != Run || len(d.Unknown) != 1 || d.Unknown[0] != "zai" {
			t.Fatalf("%s: verdict %s unknown %v reason %q", name, d.Verdict, d.Unknown, d.Reason)
		}
	}
}

func TestDecideClosedStateIsAWall(t *testing.T) {
	settings := filepath.Join(t.TempDir(), "claude-api-credits-sonnet.json") // roster launch paths must exist
	if err := os.WriteFile(settings, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	creditsRoster := fmt.Sprintf(`
staff:
  credits: {runner: claude, args: ["--model", "sonnet", "--settings", %q]}
roles:
  task_implementation: credits
`, settings)
	f := newFixture(t, creditsRoster, map[string]any{
		"anthropic_api_credits": map[string]any{"ok": false, "state": "closed: credit spent", "as_of": ts(0),
			"credits": map[string]any{"pct": 100, "resets_at": nil}},
	}, nil)
	d := f.decide("credits")
	wantWait(t, d, "closed")
	if d.Plan != "anthropic_api_credits" {
		t.Fatalf("plan %q", d.Plan)
	}
}

func TestDecideReadsTheClaudePlanFromTheTopLevelAndCountsNinetyFiveAsFull(t *testing.T) {
	claude := map[string]any{"ok": true, "as_of": ts(0),
		"five_hour": map[string]any{"pct": 96.0, "resets_at": ts(time.Hour)},
		"seven_day": map[string]any{"pct": 10.0, "resets_at": ts(48 * time.Hour)}}
	f := newFixture(t, testRoster, map[string]any{}, map[string]any{"claude": claude})
	d := f.decide("claude-main")
	wantWait(t, d, "full")
	if !d.Until.Equal(testNow.Add(time.Hour)) {
		t.Fatalf("until %v", d.Until)
	}
	claude["five_hour"] = map[string]any{"pct": 94.0, "resets_at": ts(time.Hour)}
	f.setSnapshot(map[string]any{}, map[string]any{"claude": claude})
	wantWait(t, f.decide("claude-main"), "full") // a window that read full stays full until it reads under 90
	claude["five_hour"] = map[string]any{"pct": 85.0, "resets_at": ts(time.Hour)}
	f.setSnapshot(map[string]any{}, map[string]any{"claude": claude})
	wantRun(t, f.decide("claude-main"), "claude-main")
}

func TestDecideClaudeOnlyDocumentsWaitOnOtherPlans(t *testing.T) {
	f := newFixture(t, testRoster, map[string]any{
		"zai": plan(testNow, map[string]any{"weekly": 10}),
	}, map[string]any{"claude": map[string]any{"ok": true, "as_of": ts(0), "five_hour": map[string]any{"pct": 10.0, "resets_at": ts(time.Hour)}}})
	wantWait(t, f.decide("impl", "claude_only"), "claude_only")
	wantRun(t, f.decide("claude-main", "claude_only"), "claude-main")
}

func TestDecideWithoutARosterRunsOnTheAskedSeat(t *testing.T) {
	f := newFixture(t, "", map[string]any{}, nil)
	d := f.decide("anything")
	wantRun(t, d, "anything")
	if !strings.Contains(d.Reason, "no roster") {
		t.Fatalf("reason %q", d.Reason)
	}
}

func TestDecideIsReadOnly(t *testing.T) {
	f := newFixture(t, testRoster, map[string]any{"zai": plan(testNow, map[string]any{"weekly": 10})}, nil)
	f.decide("impl")
	if _, err := os.Stat(filepath.Join(f.dir, "runtime", "roster-usage.json")); !os.IsNotExist(err) {
		t.Fatal("Decide counted a session; admission is read-only")
	}
}

func TestPlanOfMatchesTheQuotaGate(t *testing.T) {
	cases := []struct {
		name string
		s    roster.Staff
		want string
	}{
		{"zai pin", roster.Staff{Runner: "pi", Args: []string{"--model", "zai-coding-plan/glm-5.3"}}, "zai"},
		{"zai balance pin", roster.Staff{Runner: "pi", Args: []string{"--model", "zai-balance/glm-5.3"}}, "zai_balance"},
		{"retired opencode-go pin is unmetered", roster.Staff{Runner: "opencode", Args: []string{"--model", "opencode-go/glm-5.3"}}, ""},
		{"unpinned opencode is unmetered", roster.Staff{Runner: "opencode"}, ""},
		{"kimi code pin", roster.Staff{Runner: "kimi", Args: []string{"--model", "kimi-code/k3"}}, "kimi_coding"},
		{"agy", roster.Staff{Runner: "agy", Args: []string{"--model", "gemini-3.8-flash-high"}}, "antigravity"},
		{"unpinned pi", roster.Staff{Runner: "pi"}, "zai"},
		{"claude plan", roster.Staff{Runner: "claude", Args: []string{"--model", "sonnet"}}, "claude"},
		{"claude credits", roster.Staff{Runner: "claude", Args: []string{"--settings", "/h/claude-api-credits-sonnet.json"}}, "anthropic_api_credits"},
		{"hermes", roster.Staff{Runner: "hermes"}, ""},
		{"unknown provider", roster.Staff{Runner: "pi", Args: []string{"--model", "mystery/x"}}, ""},
	}
	for _, c := range cases {
		if got := PlanOf(c.s); got != c.want {
			t.Errorf("%s: PlanOf = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestExplainShowsVerdictAndTrail(t *testing.T) {
	f := newFixture(t, testRoster, map[string]any{
		"zai":         plan(testNow, map[string]any{"weekly": 100}),
		"kimi_coding": plan(testNow, map[string]any{"rolling": 10}),
	}, nil)
	d := f.decide("impl")
	text := d.Explain("impl")
	for _, part := range []string{"impl", "RUN", "backup", "kimi_coding", "zai", "full"} {
		if !strings.Contains(text, part) {
			t.Fatalf("explain output lacks %q:\n%s", part, text)
		}
	}
	data, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	var back map[string]any
	if err := json.Unmarshal(data, &back); err != nil || back["verdict"] != "run" || back["seat"] != "backup" {
		t.Fatalf("json %s err %v", data, err)
	}
	_ = fmt.Sprint(back)
}
