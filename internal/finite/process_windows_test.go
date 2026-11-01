//go:build windows

package finite

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFiniteWindowsIdentifyChildProcess(t *testing.T) {
	command, _ := startFiniteSleeper(t)
	token, err := NewProcessToken()
	if err != nil {
		t.Fatal(err)
	}
	identity, err := IdentifyProcess(command.Process.Pid, 73, token)
	if err != nil {
		t.Fatalf("identify live child process: %v", err)
	}
	if identity.PID != command.Process.Pid || identity.RunID != 73 || identity.Token != token || !strings.HasPrefix(identity.Start, "windows:") {
		t.Fatalf("child identity = %#v", identity)
	}
}

func TestFiniteWindowsProviderJoinsRunJob(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	spec := newFiniteFixtureSpec(t, "block")
	handle, err := StartSupervisor(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	startedPath := spec.Harness.Env[2][len(fixtureStartedEnv)+1:]
	waitForFixture(t, func() bool { return fileExists(startedPath) }, "Windows provider start")
	observation, err := InspectRun(ctx, spec.RunID, spec.ProcessToken)
	if err != nil || observation.Liveness != LivenessLive || len(observation.Processes) < 2 {
		detail, _ := os.ReadFile(filepath.Join(spec.RunDir, "supervisor.log"))
		t.Fatalf("live provider job membership = %#v, %v; supervisor log:\n%s", observation, err, detail)
	}
	state, err := StopRun(ctx, spec.RunID, spec.ProcessToken, 25*time.Millisecond)
	if err != nil || state != LivenessAbsent {
		t.Fatalf("stop provider job = %s, %v", state, err)
	}
	_ = handle.Wait()
}
