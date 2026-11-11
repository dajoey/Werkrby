package finite

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/windows"
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

// killWindowsProcess terminates pid and waits for confirmed exit so later job
// and identity observations never race the dying process.
func killWindowsProcess(t *testing.T, pid int) {
	t.Helper()
	process, err := windows.OpenProcess(windows.PROCESS_TERMINATE|windows.SYNCHRONIZE, false, uint32(pid))
	if err != nil {
		t.Fatalf("open process %d for termination: %v", pid, err)
	}
	defer windows.CloseHandle(process)
	if err := windows.TerminateProcess(process, 1); err != nil {
		t.Fatalf("terminate process %d: %v", pid, err)
	}
	if event, err := windows.WaitForSingleObject(process, 10000); err != nil || event != windows.WAIT_OBJECT_0 {
		t.Fatalf("process %d did not exit after termination: event %d, %v", pid, event, err)
	}
}

// TestFiniteWindowsProviderOutlivesOwnerAndSupervisorDeath proves the provider
// inheritance handoff: a harness-spawned provider holds the run job's query
// handle, so the job object survives as long as any run member lives even when
// BOTH the owner fixture process and the supervisor are killed. Before the
// handle carriage existed, the last job handle closed with the wrappers, the
// job object was destroyed, and this test observed a false LivenessAbsent
// while the uncontained provider kept running.
func TestFiniteWindowsProviderOutlivesOwnerAndSupervisorDeath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	spec := newFiniteFixtureSpec(t, "inherit")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	releasePath := spec.Harness.Env[1][len(fixtureReleaseEnv)+1:]
	startedPath := spec.Harness.Env[2][len(fixtureStartedEnv)+1:]
	root := filepath.Dir(spec.RunDir)
	// newFiniteFixtureSpec names root and runDir but creates nothing: the run
	// directory appears only when the owner spawns the supervisor. The owner
	// fixture input needs the root to exist first.
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	input, err := json.Marshal(finiteOwnerFixtureInput{Root: root, Spec: spec})
	if err != nil {
		t.Fatal(err)
	}
	inputPath := filepath.Join(root, "owner fixture.json")
	if err := os.WriteFile(inputPath, input, 0o600); err != nil {
		t.Fatal(err)
	}
	// The owner fixture is the owner: it holds the run job's launch handle
	// until it dies, exactly like the production serve process.
	owner := exec.Command(executable, "finite-owner-fixture", inputPath)
	owner.Env = append(os.Environ(), spec.Harness.Env...)
	if err := owner.Start(); err != nil {
		t.Fatal(err)
	}
	ownerGone := false
	t.Cleanup(func() {
		if !ownerGone && owner.Process != nil {
			_ = owner.Process.Kill()
			_, _ = waitForCommand(owner, 5*time.Second)
		}
		_ = os.WriteFile(releasePath, []byte("cleanup"), 0o600)
	})
	var spawnedPath string
	waitForFixture(t, func() bool {
		matches, _ := filepath.Glob(filepath.Join(root, "owner-spawned-*"))
		if len(matches) > 0 {
			spawnedPath = matches[0]
			return true
		}
		return false
	}, "owner fixture supervisor spawn")
	data, err := os.ReadFile(spawnedPath)
	if err != nil {
		t.Fatal(err)
	}
	var supervisor ProcessIdentity
	if err := json.Unmarshal(data, &supervisor); err != nil {
		t.Fatal(err)
	}
	waitForFixture(t, func() bool { return fileExists(startedPath) }, "harness-spawned provider start")
	pidText, err := os.ReadFile(startedPath)
	if err != nil {
		t.Fatal(err)
	}
	providerPID, err := strconv.Atoi(strings.TrimSpace(string(pidText)))
	if err != nil {
		t.Fatal(err)
	}

	// Kill the supervisor, then the owner, while the provider lives. Nothing
	// but the provider can keep the run job open afterwards.
	killWindowsProcess(t, supervisor.PID)
	if err := owner.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if _, _ = waitForCommand(owner, 5*time.Second); owner.ProcessState == nil {
		t.Fatal("owner fixture outlived its kill")
	}
	ownerGone = true

	observation, err := InspectRun(ctx, spec.RunID, spec.ProcessToken)
	if err != nil || observation.Liveness != LivenessLive || observation.Supervisor != nil || len(observation.Processes) == 0 {
		t.Fatalf("provider-only boundary after both wrappers died = %#v, %v; want the harness provider %d still observable through the run job", observation, err, providerPID)
	}
	if identity, idErr := IdentifyProcess(providerPID, spec.RunID, spec.ProcessToken); idErr != nil {
		t.Fatalf("harness provider %d did not survive its wrappers: %v", providerPID, idErr)
	} else if identity.PID != providerPID {
		t.Fatalf("provider identity = %#v, want PID %d", identity, providerPID)
	}

	state, err := StopRun(ctx, spec.RunID, spec.ProcessToken, 0)
	if err != nil || state != LivenessAbsent {
		t.Fatalf("provider-only run stop = %s, %v", state, err)
	}
	// Absence must be proven termination, never a vanished job: before the
	// inheritance handoff, StopRun reported absent here while the provider
	// kept running uncontained.
	if _, idErr := IdentifyProcess(providerPID, spec.RunID, spec.ProcessToken); !errors.Is(idErr, os.ErrProcessDone) {
		t.Fatalf("StopRun reported %s but provider %d is still live: %v", state, providerPID, idErr)
	}
}

// fixtureInterruptSignal is the console stop signal a graceful stop delivers.
// Go maps both console events to Interrupt on Windows.
func fixtureInterruptSignal() os.Signal { return os.Interrupt }
