package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/harness"
	"github.com/agent0ai/spynel/internal/roster"
)

// rosterShadowFixture adds the chat, notification and heartbeat roles to the shadow fixture's roster.
func rosterShadowFixture(t *testing.T, mode string) *shadowFixture {
	t.Helper()
	f := newShadowFixture(t, mode)
	body := `
staff:
  implementer: {runner: pi, args: ["--model", "zai-coding-plan/glm-5.3"]}
roles:
  task_implementation: implementer
  chat: implementer
  notification: implementer
  heartbeat: implementer
`
	if err := os.WriteFile(roster.Path(f.state), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return f
}

// Off (the default) asks admission nothing for chat, notification and heartbeat turns.
func TestAdmissionShadowRosterOffAsksNothing(t *testing.T) {
	f := rosterShadowFixture(t, "")
	f.writeZai(100)
	m := f.manager()
	if record := m.shadowRoster("chat", "chat:telegram:TG-1", "implementer"); record != nil {
		t.Fatal("admission_mode off returned a recorder")
	}
	if m.shadow.decider != nil || len(m.shadow.sampled) != 0 {
		t.Fatal("admission_mode off built a decider or sampled a turn")
	}
}

// A chat turn on a seat whose plan reads full is a disagreement: the old path never holds a chat turn.
func TestAdmissionShadowRosterLogsDisagreementPerRole(t *testing.T) {
	f := rosterShadowFixture(t, "shadow")
	f.writeZai(100)
	m := f.manager()
	for role, key := range map[string]string{
		"chat":         "chat:telegram:TG-1",
		"notification": "orchestrator:notification:t1:done:1",
		"heartbeat":    "orchestrator:semantic-heartbeat",
	} {
		record := m.shadowRoster(role, key, "implementer")
		if record == nil {
			t.Fatalf("%s: no recorder", role)
		}
		record("implementer")
	}
	joined := strings.Join(f.shadowLines(), "\n")
	for _, want := range []string{
		"site=chat doc=chat:telegram:TG-1 phase=chat asked=implementer old=run:implementer new=wait:implementer",
		"site=notification doc=notification phase=notification",
		"site=heartbeat doc=orchestrator:semantic-heartbeat phase=heartbeat",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("shadow log lacks %q:\n%s", want, joined)
		}
	}
}

// With room on the plan the verdicts agree and only the hourly summary counts it.
func TestAdmissionShadowRosterAgreesWithRoom(t *testing.T) {
	f := rosterShadowFixture(t, "shadow")
	m := f.manager()
	m.shadowRoster("chat", "chat:tui:local-1", "implementer")("implementer")
	m.shadowSummary(true)
	joined := strings.Join(f.shadowLines(), "\n")
	if strings.Contains(joined, "DISAGREE") || !strings.Contains(joined, "site=chat agree=1 disagree=0") {
		t.Fatalf("agreement not counted:\n%s", joined)
	}
}

// A turn is compared once a minute per conversation; another conversation is its own sample.
func TestAdmissionShadowRosterSamplesOncePerMinutePerKey(t *testing.T) {
	f := rosterShadowFixture(t, "shadow")
	m := f.manager()
	now := time.Now()
	m.shadow.now = func() time.Time { return now }
	if m.shadowRoster("chat", "chat:tui:a", "implementer") == nil {
		t.Fatal("first turn not sampled")
	}
	if m.shadowRoster("chat", "chat:tui:a", "implementer") != nil {
		t.Fatal("second turn inside the minute was sampled again")
	}
	if m.shadowRoster("chat", "chat:tui:b", "implementer") == nil {
		t.Fatal("another conversation was not sampled")
	}
	now = now.Add(time.Minute + time.Second)
	if m.shadowRoster("chat", "chat:tui:a", "implementer") == nil {
		t.Fatal("turn a minute later was not sampled")
	}
}

// A chat turn through the real Herdr harness reaches the manager's observer: the harness asks before
// the roster counts the session, the roster still counts it, and the verdict is logged beside it.
// The stand-in herdr CLI reports a running server and refuses everything else, so the turn stops
// after staffing, at target resolution.
func TestAdmissionShadowChatTurnThroughHerdr(t *testing.T) {
	f := rosterShadowFixture(t, "shadow")
	f.writeZai(100)
	m := f.manager()
	script := filepath.Join(t.TempDir(), "herdr")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nif [ \"$1\" = status ]; then echo 'status: running'; exit 0; fi\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	h, err := harness.NewHerdr(harness.HarnessConfig{Name: "herdr", Command: script, Cwd: f.cfg.Root, SessionsFile: filepath.Join(t.TempDir(), "sessions.json")})
	if err != nil {
		t.Fatal(err)
	}
	harness.SetRosterObserver(m.shadowRoster)
	t.Cleanup(func() { harness.SetRosterObserver(nil) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, _, err := h.Send(ctx, "chat:telegram:TG-1", "hello", nil); err == nil || !strings.Contains(err.Error(), "target resolution") {
		t.Fatalf("turn did not reach target resolution after staffing: %v", err)
	}
	joined := strings.Join(f.shadowLines(), "\n")
	if want := "site=chat doc=chat:telegram:TG-1 phase=chat asked=implementer old=run:implementer new=wait:implementer"; !strings.Contains(joined, want) {
		t.Fatalf("shadow log lacks %q:\n%s", want, joined)
	}
	usage, err := os.ReadFile(filepath.Join(f.state, "runtime", "roster-usage.json"))
	if err != nil || !strings.Contains(string(usage), "chat:telegram:TG-1") {
		t.Fatalf("the roster did not count the chat session (%v): %s", err, usage)
	}
}

// Every notification has its own session key; the comparison and its log line are per role, so a full
// plan does not log one line per event.
func TestAdmissionShadowRosterComparesNotificationsAsOneRole(t *testing.T) {
	f := rosterShadowFixture(t, "shadow")
	f.writeZai(100)
	m := f.manager()
	if m.shadowRoster("notification", "orchestrator:notification:t1:done:1", "implementer") == nil {
		t.Fatal("first notification not sampled")
	}
	if m.shadowRoster("notification", "orchestrator:notification:t2:done:1", "implementer") != nil {
		t.Fatal("a second notification inside the minute was sampled")
	}
}

// Run installs the observer and, on exit, removes only its own: a newer owner's stays.
func TestManagerRunInstallsAndRemovesTheRosterObserver(t *testing.T) {
	f := rosterShadowFixture(t, "shadow")
	f.writeZai(100)
	m := f.manager()
	script := filepath.Join(t.TempDir(), "herdr")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nif [ \"$1\" = status ]; then echo 'status: running'; exit 0; fi\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	h, err := harness.NewHerdr(harness.HarnessConfig{Name: "herdr", Command: script, Cwd: f.cfg.Root, SessionsFile: filepath.Join(t.TempDir(), "sessions.json")})
	if err != nil {
		t.Fatal(err)
	}
	send := func(key string) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _, _ = h.Send(ctx, key, "hello", nil)
	}
	count := func() int { return len(f.shadowLines()) }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()
	deadline := time.Now().Add(20 * time.Second)
	for before := count(); count() == before; {
		send("chat:tui:run-hook")
		if time.Now().After(deadline) {
			t.Fatal("Run did not install the roster observer")
		}
		time.Sleep(20 * time.Millisecond)
		m.shadow.mu.Lock()
		m.shadow.sampled = nil // let the retry be sampled again
		m.shadow.mu.Unlock()
	}
	var asked atomic.Int32
	remove := harness.SetRosterObserver(func(string, string, string) func(string) { asked.Add(1); return nil })
	t.Cleanup(remove)
	cancel()
	<-done
	send("chat:tui:after-run")
	if asked.Load() != 1 {
		t.Fatalf("Run's exit removed an observer another owner had installed (asked %d times)", asked.Load())
	}
	remove()
	send("chat:tui:after-remove")
	if asked.Load() != 1 {
		t.Fatal("remove left the observer installed")
	}
}

// A role admission has no kind for is not compared.
func TestAdmissionShadowRosterIgnoresUnknownRole(t *testing.T) {
	f := rosterShadowFixture(t, "shadow")
	m := f.manager()
	if m.shadowRoster("planner", "chat:tui:a", "implementer") != nil {
		t.Fatal("unknown role was compared")
	}
}
