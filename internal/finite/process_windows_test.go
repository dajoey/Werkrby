package finite

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFiniteWindowsJobNameIsGlobal(t *testing.T) {
	token, err := NewProcessToken()
	if err != nil {
		t.Fatal(err)
	}
	name := finiteJobObjectName(token)
	if !strings.HasPrefix(name, `Global\`) {
		t.Fatalf("job object name %q is not in the global namespace; an owner restarted in another logon session cannot reopen it", name)
	}
}

func TestFiniteWindowsSignalRefusesProcessOutsideRunJob(t *testing.T) {
	command, release := startFiniteSleeper(t)
	token, err := NewProcessToken()
	if err != nil {
		t.Fatal(err)
	}
	identity, err := IdentifyProcess(command.Process.Pid, 91, token)
	if err != nil {
		t.Fatal(err)
	}
	if err := signalProcess(identity, true); !errors.Is(err, ErrProcessIdentityMismatch) {
		t.Fatalf("signal outside run job = %v, want ErrProcessIdentityMismatch", err)
	}
	if err := os.WriteFile(release, []byte("cleanup"), 0o600); err != nil {
		t.Fatal(err)
	}
	waitErr, _ := waitForCommand(command, 5*time.Second)
	if waitErr != nil {
		t.Fatalf("refused signal killed the sleeper: %v", waitErr)
	}
}

func TestFiniteWindowsGracefulStopLetsProviderComplete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	spec := newFiniteFixtureSpec(t, "graceful")
	handle, err := StartSupervisor(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	startedPath := spec.Harness.Env[2][len(fixtureStartedEnv)+1:]
	releasePath := spec.Harness.Env[1][len(fixtureReleaseEnv)+1:]
	markerPath := filepath.Join(filepath.Dir(releasePath), "provider-graceful-exit")
	waitForFixture(t, func() bool { return fileExists(startedPath) }, "Windows provider start")
	begin := time.Now()
	state, err := StopRun(ctx, spec.RunID, spec.ProcessToken, 15*time.Second)
	if err != nil || state != LivenessAbsent {
		t.Fatalf("graceful run stop = %s, %v", state, err)
	}
	if elapsed := time.Since(begin); elapsed >= 15*time.Second {
		t.Fatalf("graceful stop consumed the whole grace (%s): the provider was terminated, not drained", elapsed)
	}
	if !fileExists(markerPath) {
		t.Fatal("provider left no graceful-exit marker: it was terminated instead of exiting on the stop signal")
	}
	data, err := os.ReadFile(filepath.Join(spec.RunDir, "completion.json"))
	if err != nil {
		t.Fatalf("completion.json missing after graceful stop: %v", err)
	}
	var completion supervisorCompletion
	if err := json.Unmarshal(data, &completion); err != nil {
		t.Fatal(err)
	}
	if completion.Outcome != RunOK || completion.RunID != spec.RunID {
		t.Fatalf("completion = %+v, want run %d outcome %v", completion, spec.RunID, RunOK)
	}
	_ = handle.Wait()
}

// fixtureInterruptSignal is the console stop signal a graceful stop delivers.
// Go maps both console events to Interrupt on Windows.
func fixtureInterruptSignal() os.Signal { return os.Interrupt }
