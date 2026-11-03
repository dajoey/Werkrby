package harness

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/agent0ai/spynel/internal/roster"
)

// The staff expansion that HermesRunHasAdmission performs must return exactly
// today's values for every input, so refactoring it behind HeadlessHermesStaff
// cannot change any admission decision. Each case pins the pre-refactor
// contract: a supervisor.json read or decode error propagates, a run whose
// harness entry is not an expandable headless Hermes staff seat is (false, nil),
// and a headless Hermes seat fails closed without an explicit HERMES_HOME.
func TestHermesRunHasAdmissionUnchangedByStaffRefactor(t *testing.T) {
	ctx := context.Background()
	writeRunDir := func(t *testing.T, cwd, model string, harnessJSON string) string {
		t.Helper()
		runDir := t.TempDir()
		var payload []byte
		if harnessJSON == "" {
			request := struct {
				Harness HarnessConfig `json:"harness"`
			}{Harness: HarnessConfig{Name: "herdr", Cwd: cwd, Model: model}}
			data, err := json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			payload = data
		} else {
			payload = []byte(harnessJSON)
		}
		if err := os.WriteFile(filepath.Join(runDir, "supervisor.json"), payload, 0o600); err != nil {
			t.Fatal(err)
		}
		return runDir
	}
	writeRoster := func(t *testing.T, cwd, content string) {
		t.Helper()
		dir := filepath.Join(cwd, roster.StateDirName)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "roster.yaml"), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	check := func(t *testing.T, runDir string, wantAdmitted bool, wantErr error) {
		t.Helper()
		admitted, err := HermesRunHasAdmission(ctx, RunBoundarySpec{RunID: 1, ProcessToken: "token", RunDir: runDir})
		if wantErr != nil {
			if !errors.Is(err, wantErr) {
				t.Fatalf("HermesRunHasAdmission error = %v, want %v", err, wantErr)
			}
			return
		}
		if err != nil {
			t.Fatalf("HermesRunHasAdmission = %v, want no error", err)
		}
		if admitted != wantAdmitted {
			t.Fatalf("HermesRunHasAdmission = %v, want %v", admitted, wantAdmitted)
		}
	}

	t.Run("missing_supervisor", func(t *testing.T) {
		check(t, t.TempDir(), false, fs.ErrNotExist)
	})

	t.Run("bad_json", func(t *testing.T) {
		runDir := t.TempDir()
		if err := os.WriteFile(filepath.Join(runDir, "supervisor.json"), []byte("{not json"), 0o600); err != nil {
			t.Fatal(err)
		}
		admitted, err := HermesRunHasAdmission(ctx, RunBoundarySpec{RunID: 1, ProcessToken: "token", RunDir: runDir})
		if err == nil {
			t.Fatalf("bad supervisor.json admitted=%v with no error", admitted)
		}
	})

	t.Run("non_staff_model", func(t *testing.T) {
		cwd := t.TempDir()
		writeRoster(t, cwd, "staff:\n  pilot:\n    runner: hermes\n    headless: true\n")
		check(t, writeRunDir(t, cwd, "zai/glm-5.3-flash", ""), false, nil)
	})

	t.Run("expand_staff_error", func(t *testing.T) {
		// @pilot with no roster at all: expansion fails, admission is (false, nil).
		cwd := t.TempDir()
		check(t, writeRunDir(t, cwd, "@pilot", ""), false, nil)
	})

	t.Run("not_headless", func(t *testing.T) {
		cwd := t.TempDir()
		writeRoster(t, cwd, "staff:\n  pilot:\n    runner: hermes\n")
		check(t, writeRunDir(t, cwd, "@pilot", ""), false, nil)
	})

	t.Run("other_runner", func(t *testing.T) {
		cwd := t.TempDir()
		writeRoster(t, cwd, "staff:\n  pilot:\n    runner: claude\n    headless: true\n")
		check(t, writeRunDir(t, cwd, "@pilot", ""), false, nil)
	})

	t.Run("headless_requires_home", func(t *testing.T) {
		t.Setenv("HERMES_HOME", "")
		cwd := t.TempDir()
		writeRoster(t, cwd, "staff:\n  pilot:\n    runner: hermes\n    headless: true\n    args: [\"-p\", \"wk-finite-test\", \"--model\", \"zai/glm-5.3-flash\"]\n")
		runDir := writeRunDir(t, cwd, "@pilot", "")
		if err := os.WriteFile(filepath.Join(runDir, "prompt.txt"), []byte("prompt"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := HermesRunHasAdmission(ctx, RunBoundarySpec{RunID: 1, ProcessToken: "token", RunDir: runDir})
		if err == nil || err.Error() != "Hermes admission check requires an explicit disposable HERMES_HOME" {
			t.Fatalf("headless Hermes without HERMES_HOME = %v, want the disposable-home error", err)
		}
	})
}
