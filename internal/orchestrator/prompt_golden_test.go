package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/roster"
	"github.com/agent0ai/spynel/internal/workspace"
)

const flagOffGolden = "testdata/flag_off_task_prompt.golden"

// flagOffDispatchPrompt is the prompt a fixed task gets from a real dispatch on a headless seat
// without explicit_outcome, with the temporary workspace root replaced by <ROOT> and the test binary by <EXE>. This file only
// uses APIs that exist on main, so the golden was captured by running it there
// (WERKRBY_GOLDEN_UPDATE=1).
func flagOffDispatchPrompt(t *testing.T, seat string) string {
	t.Helper()
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Orchestrator.TaskNotifications = config.TaskNotificationsOff
	body := "staff:\n  pilot: " + seat + "\nroles:\n  task_implementation: pilot\n"
	if err := os.WriteFile(roster.Path(cfg.StatePath()), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	task := "---\nid: tasks-golden-probe\ntitle: golden probe\nstatus: todo\ncreated_at: \"2026-01-01T00:00:00Z\"\nupdated_at: \"2026-01-01T00:00:00Z\"\nattempt: 0\nreview_required: true\n---\nProbe body.\n"
	if err := os.WriteFile(filepath.Join(cfg.StatePath("tasks", "todo"), "golden-probe.md"), []byte(task), 0o600); err != nil {
		t.Fatal(err)
	}
	fake := newFakeRecipient()
	manager := New(cfg, fake, extensions.Runner{Directory: filepath.Join(root, "missing")})
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.prompts) != 1 {
		t.Fatalf("dispatches = %d, want 1", len(fake.prompts))
	}
	prompt := strings.ReplaceAll(fake.prompts[0], root, "<ROOT>")
	if executable, err := os.Executable(); err == nil {
		prompt = strings.ReplaceAll(prompt, executable, "<EXE>")
	}
	return prompt
}

// TestFlagOffTaskPromptMatchesMainGolden pins the unflagged prompt to main's renderPrompt output.
func TestFlagOffTaskPromptMatchesMainGolden(t *testing.T) {
	got := flagOffDispatchPrompt(t, "{runner: claude, headless: true}")
	if os.Getenv("WERKRBY_GOLDEN_UPDATE") == "1" {
		if err := os.MkdirAll(filepath.Dir(flagOffGolden), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(flagOffGolden, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(flagOffGolden)
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Fatalf("unflagged prompt differs from main's golden (%d vs %d bytes)", len(got), len(want))
	}
}
