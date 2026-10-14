package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func admissionWorkspace(t *testing.T, zaiPct int) (stateDir, snapshot string) {
	t.Helper()
	stateDir = t.TempDir()
	roster := `
staff:
  impl: {runner: pi, args: ["--model", "zai-coding-plan/glm-5.3"], fallback: backup}
  backup: {runner: opencode, args: ["--model", "opencode-go/glm-5.3"]}
roles:
  task_implementation: impl
`
	if err := os.WriteFile(filepath.Join(stateDir, "roster.yaml"), []byte(roster), 0o600); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	reset := now.Add(2 * time.Hour).Format(time.RFC3339)
	snap := map[string]any{
		"generated_at": now.Format(time.RFC3339),
		"plans": map[string]any{
			"zai":         map[string]any{"ok": true, "as_of": now.Format(time.RFC3339), "weekly": map[string]any{"pct": zaiPct, "resets_at": reset}},
			"opencode_go": map[string]any{"ok": true, "as_of": now.Format(time.RFC3339), "rolling": map[string]any{"pct": 5, "resets_at": reset}},
		},
	}
	data, _ := json.Marshal(snap)
	snapshot = filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(snapshot, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return stateDir, snapshot
}

func TestAdmissionExplainOneSeat(t *testing.T) {
	stateDir, snapshot := admissionWorkspace(t, 100)
	var out bytes.Buffer
	if err := runAdmissionCommand([]string{"explain", "--state", stateDir, "--snapshot", snapshot, "--seat", "impl"}, &out); err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{"impl: RUN on backup", "opencode_go", "passed over", "zai"} {
		if !strings.Contains(out.String(), part) {
			t.Fatalf("output lacks %q:\n%s", part, out.String())
		}
	}
}

func TestAdmissionExplainEverySeatAsJSON(t *testing.T) {
	stateDir, snapshot := admissionWorkspace(t, 10)
	var out bytes.Buffer
	if err := runAdmissionCommand([]string{"explain", "--state", stateDir, "--snapshot", snapshot, "--json"}, &out); err != nil {
		t.Fatal(err)
	}
	var seats map[string]map[string]any
	if err := json.Unmarshal(out.Bytes(), &seats); err != nil {
		t.Fatalf("not json: %v\n%s", err, out.String())
	}
	if len(seats) != 2 || seats["impl"]["verdict"] != "run" || seats["backup"]["seat"] != "backup" {
		t.Fatalf("seats %v", seats)
	}
}

func TestAdmissionExplainRejectsBadInput(t *testing.T) {
	stateDir, snapshot := admissionWorkspace(t, 10)
	for name, args := range map[string][]string{
		"no subcommand":   {},
		"unknown command": {"decide"},
		"unknown seat":    {"explain", "--state", stateDir, "--snapshot", snapshot, "--seat", "nobody"},
		"unknown kind":    {"explain", "--state", stateDir, "--snapshot", snapshot, "--kind", "lunch"},
		"no roster":       {"explain", "--state", t.TempDir(), "--snapshot", snapshot},
	} {
		if err := runAdmissionCommand(args, &bytes.Buffer{}); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}
