package finite

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/harness"
)

type compositionStubHarness struct{}

func (compositionStubHarness) Start(context.Context) error { return nil }
func (compositionStubHarness) Close() error                { return nil }
func (compositionStubHarness) Send(context.Context, string, string, core.Emit) (string, bool, error) {
	return "fake-thread", false, nil
}
func (compositionStubHarness) Interrupt(context.Context, string) (bool, error) { return true, nil }
func (compositionStubHarness) ResetSession(string) error                       { return nil }
func (compositionStubHarness) ThreadID(string) string                          { return "fake-thread" }
func (compositionStubHarness) IsActive(string) bool                            { return false }

func writeCompositionSupervisor(t *testing.T, name string) (string, []string) {
	t.Helper()
	runDir := filepath.Join(t.TempDir(), "run")
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		t.Fatal(err)
	}
	token := strings.Repeat("a", 43)
	request := supervisorRequest{
		SchemaVersion: supervisorRequestVersion, RunID: 99, RunDir: runDir, ProcessToken: token,
		SessionKey: "finite:composition", Harness: harness.HarnessConfig{Name: name, Command: filepath.Join(runDir, "no-such-herdr"), Cwd: runDir, Model: "hermes"},
	}
	data, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "supervisor.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "prompt.txt"), []byte("composition prompt"), 0o600); err != nil {
		t.Fatal(err)
	}
	return runDir, []string{"supervise", "--run-id", "99", "--run-dir", runDir, "--process-token", token}
}

func TestFiniteProductionSupervisorRequiresHeadlessHermesStaff(t *testing.T) {
	_, args := writeCompositionSupervisor(t, "herdr")
	err := defaultCLIDependencies().runSupervisor(context.Background(), args, NewRunner)
	if err == nil || !strings.Contains(err.Error(), "requires configured headless Hermes staff") {
		t.Fatalf("production supervisor error = %v, want headless-staff-only refusal", err)
	}
}

func TestFiniteProductionSupervisorKeepsFakeProvidersSupported(t *testing.T) {
	_, args := writeCompositionSupervisor(t, "finite-composition-fake")
	err := defaultCLIDependencies().runSupervisor(context.Background(), args[1:], func(harness.HarnessConfig) (harness.Harness, error) {
		return compositionStubHarness{}, nil
	})
	if err != nil {
		t.Fatalf("fake provider supervisor: %v", err)
	}
}
