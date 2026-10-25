package orchestrator

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/roster"
)

// A task front-matter agent:/model: pin that names a retired runner (agy, kimi, opencode,
// pi, claude) must never launch that runner: the roster routes the document on its role's
// Hermes seat instead, exactly as if the pin were absent.
func TestRetiredRunnerPinDispatchesOnRosterHermesSeat(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, config.StateDirectoryName)
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}
	body := `
staff:
  implementer: {runner: hermes, args: ["-p", "wk-code", "--model", "zai/glm-5.3-flash"]}
roles:
  task_implementation: implementer
  task_review: implementer
`
	if err := os.WriteFile(roster.Path(state), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{Root: root, Harness: config.Harness{Name: "herdr", Model: "opencode", DeveloperModel: "opencode", ReviewerModel: "kimi"}}
	manager := New(cfg, newFakeRecipient(), extensions.Runner{})
	for _, pin := range []string{"agy", "kimi", "opencode", "pi", "claude"} {
		for _, field := range []string{"agent", "model"} {
			name := field + "-pin-" + pin
			path := filepath.Join(root, name+".md")
			if err := os.WriteFile(path, []byte("---\nid: "+name+"\n"+field+": "+pin+"\nstatus: working\n---\n# T\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			lease := Lease{File: path, Phase: phaseTaskImplementation, SessionKey: "orchestrator:tasks:x:" + name + ":1"}
			if got := manager.resolveTargetModel(workflowRoute{Name: "tasks"}, lease); got != "@implementer" {
				t.Errorf("%s: pin in %s resolves to %q, want @implementer (the role's Hermes seat), never the retired runner", pin, field, got)
			}
		}
	}
}

// An empty reviewer_model falls back to Hermes, the only runner Werkrby code may launch.
func TestEmptyReviewerModelFallsBackToHermes(t *testing.T) {
	dir := t.TempDir()
	taskPath := filepath.Join(dir, "task.md")
	if err := os.WriteFile(taskPath, []byte("---\nid: test-task\nstatus: reviewing\n---\n# T\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{Harness: config.Harness{Name: "herdr"}}
	manager := New(cfg, newFakeRecipient(), extensions.Runner{})
	lease := Lease{File: taskPath, Phase: phaseTaskReview}
	if got := manager.resolveTargetModel(workflowRoute{Name: "tasks"}, lease); got != "hermes" {
		t.Fatalf("review with empty reviewer_model resolves to %q, want hermes", got)
	}
}

// With no roster (legacy routing), a front-matter agent:/model: pin naming a retired runner is
// skipped as if absent: the document runs on the configured developer model, never on the
// retired runner, and never hands the harness a name it refuses to launch.
func TestRetiredRunnerPinWithoutRosterUsesDeveloperModel(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, config.StateDirectoryName), 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{Root: root, Harness: config.Harness{Name: "herdr", Model: "hermes", DeveloperModel: "hermes"}}
	manager := New(cfg, newFakeRecipient(), extensions.Runner{})
	for _, pin := range []string{"agy", "kimi", "opencode", "pi", "claude"} {
		for _, field := range []string{"agent", "model"} {
			name := field + "-legacy-pin-" + pin
			path := filepath.Join(root, name+".md")
			if err := os.WriteFile(path, []byte("---\nid: "+name+"\n"+field+": "+pin+"\nstatus: working\n---\n# T\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			lease := Lease{File: path, Phase: phaseTaskImplementation, SessionKey: "orchestrator:tasks:x:" + name + ":1"}
			if got := manager.resolveTargetModel(workflowRoute{Name: "tasks"}, lease); got != "hermes" {
				t.Errorf("%s: legacy pin in %s resolves to %q, want hermes (the developer model), never the retired runner", pin, field, got)
			}
		}
	}
	// A pin that names no retired runner is still honoured on the legacy path.
	path := filepath.Join(root, "agent-pin-hermes.md")
	if err := os.WriteFile(path, []byte("---\nid: agent-pin-hermes\nagent: \"@implementer\"\nstatus: working\n---\n# T\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	lease := Lease{File: path, Phase: phaseTaskImplementation, SessionKey: "orchestrator:tasks:x:agent-pin-hermes:1"}
	if got := manager.resolveTargetModel(workflowRoute{Name: "tasks"}, lease); got != "@implementer" {
		t.Errorf("legacy non-retired pin resolves to %q, want @implementer", got)
	}
}
