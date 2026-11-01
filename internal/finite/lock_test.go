package finite

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestFiniteWorkspaceLockIsNonblocking(t *testing.T) {
	root := t.TempDir()
	first, err := AcquireWorkspaceLock(root)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	started := time.Now()
	second, err := AcquireWorkspaceLock(root)
	if second != nil {
		defer second.Close()
	}
	if !errors.Is(err, ErrWorkspaceLocked) {
		t.Fatalf("second owner error = %v, want ErrWorkspaceLocked", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("contending lock blocked for %s", elapsed)
	}
}

func TestFiniteOwnerExclusion_O1(t *testing.T) {
	spec := newFiniteFixtureSpec(t, "block")
	root := filepath.Dir(spec.RunDir)
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
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	release := os.Getenv(fixtureReleaseEnv)
	for _, pair := range spec.Harness.Env {
		if len(pair) > len(fixtureReleaseEnv) && pair[:len(fixtureReleaseEnv)+1] == fixtureReleaseEnv+"=" {
			release = pair[len(fixtureReleaseEnv)+1:]
		}
	}
	commands := []*exec.Cmd{exec.Command(executable, "finite-owner-fixture", inputPath), exec.Command(executable, "finite-owner-fixture", inputPath)}
	for _, command := range commands {
		command.Env = append(os.Environ(), spec.Harness.Env...)
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
	}
	finished := false
	defer func() {
		if !finished {
			_ = os.WriteFile(release, []byte("cleanup"), 0o600)
			for _, command := range commands {
				if command.Process != nil {
					_ = command.Process.Kill()
				}
				_, _ = waitForCommand(command, 5*time.Second)
			}
		}
	}()
	waitForFixture(t, func() bool {
		acquired, _ := filepath.Glob(filepath.Join(root, "owner-acquired-*"))
		losers, _ := filepath.Glob(filepath.Join(root, "owner-loser-*"))
		errors, _ := filepath.Glob(filepath.Join(root, "owner-error-*"))
		return len(acquired)+len(losers) == 2 || len(errors) != 0
	}, "both owner decisions")
	acquired, _ := filepath.Glob(filepath.Join(root, "owner-acquired-*"))
	losers, _ := filepath.Glob(filepath.Join(root, "owner-loser-*"))
	if len(acquired) != 1 || len(losers) != 1 {
		_ = os.WriteFile(release, []byte("cleanup"), 0o600)
		for _, command := range commands {
			_, _ = waitForCommand(command, 5*time.Second)
		}
		finished = true
		t.Fatalf("owner decisions: acquired=%d lock-losers=%d, want exactly one of each", len(acquired), len(losers))
	}
	waitForFixture(t, func() bool {
		spawned, _ := filepath.Glob(filepath.Join(root, "owner-spawned-*"))
		return len(spawned) == 1
	}, "one supervisor spawn")
	observation, err := InspectRun(context.Background(), spec.RunID, spec.ProcessToken)
	if err != nil || observation.Liveness != LivenessLive || observation.Supervisor == nil {
		t.Fatalf("owned run observation = %#v, %v", observation, err)
	}
	if err := os.WriteFile(release, []byte("finish"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, command := range commands {
		waitErr, timeoutErr := waitForCommand(command, 10*time.Second)
		if timeoutErr != nil {
			t.Fatal(timeoutErr)
		}
		if waitErr != nil {
			t.Fatalf("owner fixture: %v", waitErr)
		}
	}
	finished = true
	waitForFixture(t, func() bool {
		observation, err := InspectRun(context.Background(), spec.RunID, spec.ProcessToken)
		return err == nil && observation.Liveness == LivenessAbsent
	}, "provider absence after owner release")
}

func TestFiniteWorkspaceLockDescriptorNotInherited(t *testing.T) {
	root := t.TempDir()
	release := filepath.Join(root, "child-release")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	parent := exec.Command(executable, "finite-lock-parent", root)
	parent.Env = append(os.Environ(), fixtureReleaseEnv+"="+release)
	if err := parent.Start(); err != nil {
		t.Fatal(err)
	}
	parentWaited := false
	defer func() {
		_ = os.WriteFile(filepath.Join(root, "parent-release"), nil, 0o600)
		_ = os.WriteFile(release, nil, 0o600)
		if !parentWaited {
			if parent.Process != nil {
				_ = parent.Process.Kill()
			}
			_, _ = waitForCommand(parent, 5*time.Second)
		}
	}()
	waitForFixture(t, func() bool {
		children, _ := filepath.Glob(filepath.Join(root, "lock-child-started-*"))
		return fileExists(filepath.Join(root, "lock-parent-ready")) && len(children) == 1
	}, "lock parent and live child")
	childPIDData, err := os.ReadFile(filepath.Join(root, "lock-parent-ready"))
	if err != nil {
		t.Fatal(err)
	}
	childPID, err := strconv.Atoi(string(childPIDData))
	if err != nil {
		t.Fatal(err)
	}
	childToken, err := NewProcessToken()
	if err != nil {
		t.Fatal(err)
	}
	childIdentity, err := IdentifyProcess(childPID, 1, childToken)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = signalProcess(childIdentity, true)
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := IdentifyProcess(childPID, 1, childToken); errors.Is(err, os.ErrProcessDone) {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
	contender, err := AcquireWorkspaceLock(root)
	if err == nil {
		_ = contender.Close()
		_ = os.WriteFile(filepath.Join(root, "parent-release"), nil, 0o600)
		_ = os.WriteFile(release, nil, 0o600)
		_, _ = waitForCommand(parent, 5*time.Second)
		parentWaited = true
		t.Fatal("a second owner acquired the lock while the original owner was live")
	}
	if !errors.Is(err, ErrWorkspaceLocked) {
		t.Fatalf("contending lock error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "parent-release"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	waitErr, timeoutErr := waitForCommand(parent, 5*time.Second)
	parentWaited = true
	if timeoutErr != nil || waitErr != nil {
		t.Fatalf("lock parent exit: %v, %v", waitErr, timeoutErr)
	}
	inherited, err := AcquireWorkspaceLock(root)
	if err != nil {
		t.Fatalf("exited parent's live child retained the lock: %v", err)
	}
	if err := inherited.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	waitForFixture(t, func() bool {
		exited, _ := filepath.Glob(filepath.Join(root, "lock-child-exited-*"))
		return len(exited) == 1
	}, "lock child shutdown marker")
	waitForFixture(t, func() bool {
		_, err := IdentifyProcess(childPID, 1, childToken)
		return errors.Is(err, os.ErrProcessDone)
	}, "lock child OS absence")
}
