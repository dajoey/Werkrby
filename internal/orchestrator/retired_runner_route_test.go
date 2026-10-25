package orchestrator

import (
	"os"
	"path/filepath"
	"strings"
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
	// A staff pin names no roster staff while there is no roster, so it is skipped too; a hermes
	// pin is honoured.
	pinned := func(name, pin string) Lease {
		t.Helper()
		path := filepath.Join(root, name+".md")
		if err := os.WriteFile(path, []byte("---\nid: "+name+"\nagent: \""+pin+"\"\nstatus: working\n---\n# T\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return Lease{File: path, Phase: phaseTaskImplementation, SessionKey: "orchestrator:tasks:x:" + name + ":1"}
	}
	if got := manager.resolveTargetModel(workflowRoute{Name: "tasks"}, pinned("agent-pin-staff", "@implementer")); got != "hermes" {
		t.Errorf("legacy pin of undefined staff resolves to %q, want hermes (the developer model)", got)
	}
	if got := manager.resolveTargetModel(workflowRoute{Name: "tasks"}, pinned("agent-pin-hermes", "Hermes")); got != "hermes" {
		t.Errorf("legacy hermes pin resolves to %q, want hermes", got)
	}
	// With roster staff defined, a pin naming that staff is honoured as written.
	body := "staff:\n  implementer: {runner: hermes, args: [\"-p\", \"wk-code\"]}\n"
	if err := os.WriteFile(roster.Path(filepath.Join(root, config.StateDirectoryName)), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := manager.resolveTargetModel(workflowRoute{Name: "tasks"}, pinned("agent-pin-roster-staff", "@implementer")); got != "@implementer" {
		t.Errorf("pin of roster staff resolves to %q, want @implementer", got)
	}
}

// A front-matter agent:/model: pin is honoured only when it names hermes or roster staff. Any other
// runner herdr supports (codex, cursor) is logged and routed by role, on the roster path and the
// legacy path alike, never handed to the harness.
func TestUnsanctionedRunnerPinRoutesByRole(t *testing.T) {
	for _, tc := range []struct {
		name   string
		roster bool
		want   string
	}{
		{name: "roster path", roster: true, want: "@implementer"},
		{name: "legacy path", roster: false, want: "hermes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			state := filepath.Join(root, config.StateDirectoryName)
			if err := os.MkdirAll(state, 0o700); err != nil {
				t.Fatal(err)
			}
			if tc.roster {
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
			}
			cfg := config.Config{Root: root, Harness: config.Harness{Name: "herdr", Model: "hermes", DeveloperModel: "hermes", ReviewerModel: "hermes"}}
			manager := New(cfg, newFakeRecipient(), extensions.Runner{})
			var logs []string
			manager.Log = func(s string) { logs = append(logs, s) }
			for _, pin := range []string{"codex", "cursor"} {
				for _, field := range []string{"agent", "model"} {
					name := field + "-pin-" + pin
					path := filepath.Join(root, name+".md")
					if err := os.WriteFile(path, []byte("---\nid: "+name+"\n"+field+": "+pin+"\nstatus: working\n---\n# T\n"), 0o600); err != nil {
						t.Fatal(err)
					}
					lease := Lease{File: path, Phase: phaseTaskImplementation, SessionKey: "orchestrator:tasks:x:" + name + ":1"}
					if got := manager.resolveTargetModel(workflowRoute{Name: "tasks"}, lease); got != tc.want {
						t.Errorf("%s pin in %s resolves to %q, want %q: only hermes or roster staff may be pinned", pin, field, got, tc.want)
					}
				}
			}
			logged := 0
			for _, line := range logs {
				if strings.Contains(line, "neither hermes nor roster staff") {
					logged++
				}
			}
			if logged == 0 {
				t.Errorf("no log line names the refused pins: %q", logs)
			}
		})
	}
}
