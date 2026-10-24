package harness

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// hermesWorkerLedger is the ledger of the Werkrby code-worker profile under a Hermes home: where
// a hermes launch that names no profile records its turns.
func hermesWorkerLedger(home string) string {
	return filepath.Join(home, "profiles", "wk-code", "state.db")
}

type hermesStart struct {
	kind string
	args []string // the runner's launch arguments, after herdr's "--"
}

// hermesStarts parses the `agent start` lines the fake herdr recorded in dir/starts.txt.
func hermesStarts(t *testing.T, dir string) []hermesStart {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "starts.txt"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var starts []hermesStart
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		fields := strings.Fields(line)
		var s hermesStart
		for i := 0; i < len(fields); i++ {
			switch fields[i] {
			case "--kind":
				if i+1 < len(fields) {
					s.kind = fields[i+1]
					i++
				}
			case "--":
				s.args = append([]string{}, fields[i+1:]...)
				i = len(fields)
			}
		}
		starts = append(starts, s)
	}
	return starts
}

const hermesDefaultsRoster = `staff:
  chatseat:
    runner: hermes
    args: ["-p", "wk-chat", "--model", "zai/glm-5.3"]
  longflag:
    runner: hermes
    args: ["--model", "zai/glm-5.3", "--profile", "wk-notify"]
  eqflag:
    runner: hermes
    args: ["--profile=wk-media"]
  bare:
    runner: hermes
    args: ["--model", "zai/glm-5.3", "--reasoning", "high"]
  piseat:
    runner: pi
    args: ["--model", "zai/glm-5.3"]
`

func writeHermesDefaultsRoster(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, ".werkrby"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".werkrby", "roster.yaml"), []byte(hermesDefaultsRoster), 0o644); err != nil {
		t.Fatal(err)
	}
}

// Every Werkrby seat runs Hermes, so a model that names no runner starts Hermes, and a Hermes
// launch that names no profile runs the Werkrby code-worker profile, never the root profile
// (the front-of-house persona). A launch that names its profile keeps it. The ledger reader
// resolves the same profile the launch runs.
func TestHerdrRunnerLaunchDefaultsToHermesCodeProfile(t *testing.T) {
	dir := t.TempDir()
	writeHermesDefaultsRoster(t, dir)
	h, err := NewHerdr(HarnessConfig{Name: "herdr", Cwd: dir})
	if err != nil {
		t.Fatal(err)
	}
	home := filepath.Join(dir, "hermes-home")
	for _, tc := range []struct {
		model      string
		wantKind   string
		wantArgs   []string
		wantLedger string // "" when the launch is not hermes
	}{
		{model: "", wantKind: "hermes", wantArgs: []string{"-p", "wk-code"}, wantLedger: hermesWorkerLedger(home)},
		{model: "default", wantKind: "hermes", wantArgs: []string{"-p", "wk-code"}, wantLedger: hermesWorkerLedger(home)},
		{model: " hermes ", wantKind: "hermes", wantArgs: []string{"-p", "wk-code"}, wantLedger: hermesWorkerLedger(home)},
		{model: "@bare", wantKind: "hermes", wantArgs: []string{"-p", "wk-code", "--model", "zai/glm-5.3", "--reasoning", "high"}, wantLedger: hermesWorkerLedger(home)},
		{model: "@chatseat", wantKind: "hermes", wantArgs: []string{"-p", "wk-chat", "--model", "zai/glm-5.3"}, wantLedger: filepath.Join(home, "profiles", "wk-chat", "state.db")},
		{model: "@longflag", wantKind: "hermes", wantArgs: []string{"--model", "zai/glm-5.3", "--profile", "wk-notify"}, wantLedger: filepath.Join(home, "profiles", "wk-notify", "state.db")},
		{model: "@eqflag", wantKind: "hermes", wantArgs: []string{"--profile=wk-media"}, wantLedger: filepath.Join(home, "profiles", "wk-media", "state.db")},
		{model: "@piseat", wantKind: "pi", wantArgs: []string{"--model", "zai/glm-5.3"}},
		{model: "claude", wantKind: "claude"},
		{model: "w8:p2", wantKind: "w8:p2"},
	} {
		t.Run(tc.model, func(t *testing.T) {
			kind, args := h.runnerLaunch(HarnessConfig{Cwd: dir, Model: tc.model})
			if kind != tc.wantKind || !reflect.DeepEqual(args, tc.wantArgs) {
				t.Fatalf("runnerLaunch(%q) = %q %q, want %q %q", tc.model, kind, args, tc.wantKind, tc.wantArgs)
			}
			if tc.wantLedger == "" {
				return
			}
			ledger, err := hermesLedgerPath(home, args)
			if err != nil || ledger != tc.wantLedger {
				t.Fatalf("ledger for %q = %q, %v, want %q", tc.model, ledger, err, tc.wantLedger)
			}
		})
	}
}

// The catalog default, and the runner an empty or "default" model names a worker after, is
// hermes: OpenCode is retired from Werkrby.
func TestHerdrDefaultModelIsHermes(t *testing.T) {
	h, err := NewHerdr(HarnessConfig{Name: "herdr"})
	if err != nil {
		t.Fatal(err)
	}
	models, err := h.Models(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var defaults []string
	for _, m := range models {
		if m.Default {
			defaults = append(defaults, m.ID)
		}
		if m.ID != "hermes" && strings.Contains(m.Description, "(default)") {
			t.Errorf("model %q still describes itself as the default: %q", m.ID, m.Description)
		}
	}
	if !reflect.DeepEqual(defaults, []string{"hermes"}) {
		t.Fatalf("default models = %q, want only hermes", defaults)
	}
	for _, model := range []string{"", "default"} {
		name, label := workerNameAndLabel("orchestrator:semantic-heartbeat", model)
		if name != "wk-hb-hermes" || label != "Heartbeat (hermes)" {
			t.Errorf("heartbeat worker for model %q = %q %q, want wk-hb-hermes \"Heartbeat (hermes)\"", model, name, label)
		}
		if _, label := workerNameAndLabel("chat:tui:local-71f92e03b302", model); label != "Chat: 71f92e03b302 (hermes)" {
			t.Errorf("chat tab label for model %q = %q, want \"Chat: 71f92e03b302 (hermes)\"", model, label)
		}
	}
	if got := WorkerNameForKey("heartbeat"); got != "wk-hb-hermes" {
		t.Errorf("WorkerNameForKey(heartbeat) = %q, want wk-hb-hermes", got)
	}
}

// A turn whose model names no runner, or names bare hermes, starts `hermes -p wk-code` in its new
// pane and is judged from wk-code's ledger, never the root ledger (a decoy here answers the same
// prompt from the root profile).
func TestHerdrHermesBareLaunchRunsTheCodeProfileAndReadsItsLedger(t *testing.T) {
	for _, tc := range []struct {
		name        string
		configModel string
		sendModel   string
		wantArgs    string
	}{
		{name: "configured hermes", configModel: "hermes", wantArgs: "-p wk-code"},
		{name: "empty configured model", configModel: "", wantArgs: "-p wk-code"},
		{name: "default model", configModel: "hermes", sendModel: "default", wantArgs: "-p wk-code"},
		{name: "hermes seat with no profile", configModel: "hermes", sendModel: "@bare", wantArgs: "-p wk-code --model zai/glm-5.3 --reasoning high"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeHermesDefaultsRoster(t, dir)
			hermesHome := filepath.Join(dir, "hermes-home")
			t.Setenv("HERMES_HOME", hermesHome)
			root := newHermesTestLedger(t, filepath.Join(hermesHome, "state.db"))
			root.session("local-root", "", "", time.Now())
			worker := newHermesTestLedger(t, hermesWorkerLedger(hermesHome))
			worker.session("local-code", "", "", time.Now())
			script := hermesFakeHerdr(t, dir)
			writeHermesScreen(t, dir, hermesTestTurn("You are running from the Werkrby orchestrator system.", hermesTestBox("Screen text, not the ledger."))+hermesTestChrome)
			startHermesSim(t, dir, func(n int, prompt string) {
				root.add("local-root",
					hermesTestRow{role: "user", content: prompt},
					hermesTestRow{role: "assistant", content: "From the root profile.", finishReason: "stop"},
				)
				worker.add("local-code",
					hermesTestRow{role: "user", content: prompt},
					hermesTestRow{role: "assistant", content: "From the wk-code profile.", finishReason: "stop"},
				)
			})

			got := runHermesTurnConfigured(t, dir, script, tc.configModel, "orchestrator:tasks:task_implementation:tasks-20261024-bare-hermes-01:1", tc.sendModel, hermesTestTaskPrompt("tasks-20261024-bare-hermes-01"))

			starts := hermesStarts(t, dir)
			if len(starts) != 1 {
				t.Fatalf("agent start ran %d times, want 1: %q", len(starts), starts)
			}
			if kind, args := starts[0].kind, strings.Join(starts[0].args, " "); kind != "hermes" || args != tc.wantArgs {
				t.Fatalf("agent start --kind %q -- %q, want --kind hermes -- %q", kind, args, tc.wantArgs)
			}
			if got.err != nil {
				t.Fatalf("Send returned %v, want the wk-code ledger's answer", got.err)
			}
			if finals := got.finals(); len(finals) != 1 || finals[0] != "From the wk-code profile." {
				t.Fatalf("EventFinal texts = %q, want the wk-code ledger's answer", finals)
			}
			if sent := hermesPromptsSent(t, dir); sent != 1 {
				t.Fatalf("prompt sent %d times, want 1", sent)
			}
		})
	}
}
