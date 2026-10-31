package finite

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestFiniteSupervisorProtocolStartsSendsAndClosesOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	spec := newFiniteFixtureSpec(t, "complete")
	handle, err := StartSupervisor(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Wait(); err != nil {
		t.Fatalf("supervisor: %v", err)
	}
	calls := readFixtureCallKinds(t, spec.RunDir)
	if !reflect.DeepEqual(calls, []string{"start", "send", "close"}) {
		t.Fatalf("harness calls = %v, want exactly [start send close]", calls)
	}
	observation, err := InspectRun(ctx, spec.RunID, spec.ProcessToken)
	if err != nil || observation.Liveness != LivenessAbsent {
		t.Fatalf("completed run boundary = %#v, %v", observation, err)
	}
	prompt, err := os.ReadFile(filepath.Join(spec.RunDir, "prompt.txt"))
	if err != nil || string(prompt) != spec.Prompt {
		t.Fatalf("run prompt = %q, %v", prompt, err)
	}
	events, err := os.ReadFile(filepath.Join(spec.RunDir, "events.jsonl"))
	if err != nil || !strings.Contains(string(events), `"done":true`) {
		t.Fatalf("provider events = %q, %v", events, err)
	}
	factoryData, err := os.ReadFile(filepath.Join(spec.RunDir, "factory.json"))
	if err != nil {
		t.Fatal(err)
	}
	var factory struct {
		SessionsFile string `json:"sessions_file"`
	}
	if err := json.Unmarshal(factoryData, &factory); err != nil {
		t.Fatal(err)
	}
	relative, err := filepath.Rel(spec.RunDir, factory.SessionsFile)
	if err != nil || relative == "." || strings.HasPrefix(relative, "..") {
		t.Fatalf("session state escaped run directory: %q", factory.SessionsFile)
	}
	if fileExists(filepath.Join(spec.RunDir, "engine.db")) {
		t.Fatal("supervisor opened a workflow database")
	}
	completionData, err := os.ReadFile(filepath.Join(spec.RunDir, "completion.json"))
	if err != nil || !strings.Contains(string(completionData), `"outcome":"ok"`) {
		t.Fatalf("completion = %s, %v", completionData, err)
	}

	command := exec.Command(spec.Executable, "supervise", "--run-id", "42", "--run-dir", spec.RunDir, "--process-token", spec.ProcessToken)
	if output, err := command.CombinedOutput(); err == nil {
		t.Fatalf("duplicate supervisor executed successfully: %s", output)
	}
	if after := readFixtureCallKinds(t, spec.RunDir); !reflect.DeepEqual(after, calls) {
		t.Fatalf("duplicate supervisor repeated provider calls: before=%v after=%v", calls, after)
	}
}
