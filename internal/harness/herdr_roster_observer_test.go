package harness

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/agent0ai/spynel/internal/roster"
)

type rosterObservation struct {
	role, key, asked string
	assigned         []string
	countedAtAsk     bool
}

func watchRoster(t *testing.T, state string, panicking bool) *[]*rosterObservation {
	t.Helper()
	var seen []*rosterObservation
	SetRosterObserver(func(role, key, asked string) func(string) {
		o := &rosterObservation{role: role, key: key, asked: asked}
		// Assign counts the session in runtime/roster-usage.json; the question is asked before that.
		_, err := os.Stat(filepath.Join(state, "runtime", "roster-usage.json"))
		o.countedAtAsk = err == nil
		seen = append(seen, o)
		if panicking {
			panic("observer fault")
		}
		return func(assigned string) { o.assigned = append(o.assigned, assigned) }
	})
	t.Cleanup(func() { SetRosterObserver(nil) })
	return &seen
}

func rosterObserverFixture(t *testing.T) (*Herdr, HarnessConfig, string) {
	t.Helper()
	root := t.TempDir()
	state := filepath.Join(root, roster.StateDirName)
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}
	body := "staff:\n  thinker: {runner: pi}\n  boss: {runner: claude, args: [\"--model\", \"opus\"]}\nroles:\n  chat: thinker\n  heartbeat: boss\n"
	if err := os.WriteFile(roster.Path(state), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return &Herdr{config: HarnessConfig{Model: "opencode", Cwd: root}}, HarnessConfig{Model: "opencode", Cwd: root}, state
}

// The observer is asked before Assign counts the session and told which seat Assign chose; the model
// the turn gets does not change.
func TestRosterModelForKeyTellsTheObserverBeforeAssign(t *testing.T) {
	h, cfg, state := rosterObserverFixture(t)
	seen := watchRoster(t, state, false)

	if got := h.rosterModelForKey("chat:telegram:TG-1", cfg); got != "pi" {
		t.Fatalf("observer changed the model: %q", got)
	}
	if got := h.rosterModelForKey("orchestrator:semantic-heartbeat", cfg); got != "@boss" {
		t.Fatalf("observer changed the heartbeat model: %q", got)
	}
	if len(*seen) != 2 {
		t.Fatalf("observer asked %d times, want 2", len(*seen))
	}
	chat, heartbeat := (*seen)[0], (*seen)[1]
	if chat.role != "chat" || chat.key != "chat:telegram:TG-1" || chat.asked != "thinker" || chat.countedAtAsk || len(chat.assigned) != 1 || chat.assigned[0] != "thinker" {
		t.Errorf("chat observation: %+v", chat)
	}
	if heartbeat.role != "heartbeat" || heartbeat.asked != "boss" || len(heartbeat.assigned) != 1 || heartbeat.assigned[0] != "boss" {
		t.Errorf("heartbeat observation: %+v", heartbeat)
	}
}

// An explicit model choice, an unmapped role and a key the roster does not route never reach the observer.
func TestRosterModelForKeySkipsTheObserverWhereRosterDoesNotStaff(t *testing.T) {
	h, cfg, state := rosterObserverFixture(t)
	seen := watchRoster(t, state, false)

	cfg.Model = "kimi"
	h.rosterModelForKey("chat:telegram:TG-1", cfg)
	cfg.Model = "opencode"
	h.rosterModelForKey("orchestrator:notification:t1:done:1", cfg)
	h.rosterModelForKey("orchestrator:tasks:task_review:t1:1", cfg)
	if len(*seen) != 0 {
		t.Fatalf("observer asked for turns the roster does not staff: %+v", *seen)
	}
}

// A fault in the observer never reaches the turn it observes.
func TestRosterModelForKeyIgnoresAnObserverPanic(t *testing.T) {
	h, cfg, state := rosterObserverFixture(t)
	watchRoster(t, state, true)
	if got := h.rosterModelForKey("chat:telegram:TG-1", cfg); got != "pi" {
		t.Fatalf("observer panic changed the model: %q", got)
	}
}
