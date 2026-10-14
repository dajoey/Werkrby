package harness

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent0ai/spynel/internal/roster"
	"github.com/agent0ai/spynel/internal/statedir"
)

func TestRosterModelForKey(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, statedir.Legacy)
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}
	h := &Herdr{config: HarnessConfig{Model: "opencode", Cwd: root}}
	cfg := HarnessConfig{Model: "opencode", Cwd: root}

	if got := h.rosterModelForKey("chat:telegram:TG-1", cfg); got != "opencode" {
		t.Fatalf("no roster = %q", got)
	}
	body := "staff:\n  thinker: {runner: pi}\n  boss: {runner: claude, args: [\"--model\", \"opus\"]}\nroles:\n  chat: thinker\n  heartbeat: boss\n"
	if err := os.WriteFile(roster.Path(state), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ key, model, want string }{
		{"chat:telegram:TG-1", "opencode", "pi"},
		{"chat:tui:local-1", "", "pi"},
		{"chat:telegram:TG-1", "kimi", "kimi"}, // explicit /model choice wins
		{"orchestrator:semantic-heartbeat", "opencode", "@boss"},
		{"orchestrator:notification:t1:done:1", "opencode", "opencode"}, // role not mapped
		{"orchestrator:tasks:task_review:t1:1", "opencode", "opencode"}, // document routing owns these
	} {
		cfg.Model = c.model
		if got := h.rosterModelForKey(c.key, cfg); got != c.want {
			t.Errorf("%s (%q): got %q, want %q", c.key, c.model, got, c.want)
		}
	}
}

// The orchestrator loads the roster from the active state directory
// (.werkrby once it exists); the harness must read the same file.
func TestHarnessReadsRosterFromActiveStateDir(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, ".werkrby")
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}
	// A stale roster in a real .spynel directory must not win over .werkrby.
	stale := filepath.Join(root, ".spynel")
	if err := os.MkdirAll(stale, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(roster.Path(stale), []byte("staff:\n  oldseat: {runner: pi}\nroles:\n  chat: oldseat\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	body := "staff:\n  newseat: {runner: pi}\nroles:\n  chat: newseat\n"
	if err := os.WriteFile(roster.Path(state), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	h := &Herdr{config: HarnessConfig{Model: "opencode", Cwd: root}}
	// Expansion happens before any herdr call; whatever fails afterwards (no herdr
	// server in the test) must not be the roster lookup.
	if _, _, _, _, _, err := h.resolveTarget(context.Background(), "chat:tui:local-1", "@newseat", root); err != nil && strings.Contains(err.Error(), "roster") {
		t.Fatalf("resolveTarget(@newseat) = %v; the .werkrby roster defines newseat", err)
	}
	if got := h.rosterModelForKey("chat:tui:local-1", HarnessConfig{Model: "opencode", Cwd: root}); got != "pi" {
		t.Fatalf("rosterModelForKey = %q, want the role mapping from the .werkrby roster", got)
	}
}
