//go:build linux

package finite

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func readFixturePID(t *testing.T, path string) int {
	t.Helper()
	var pid int
	waitForFixture(t, func() bool {
		data, err := os.ReadFile(path)
		if err != nil {
			return false
		}
		pid, err = strconv.Atoi(strings.TrimSpace(string(data)))
		return err == nil && pid > 0
	}, "fixture child PID "+path)
	return pid
}

func linuxPIDAlive(pid int) bool {
	identity, err := IdentifyProcess(pid, 1, strings.Repeat("a", 43))
	return err == nil && identity.PID == pid
}

func killPIDOnCleanup(t *testing.T, pid int) {
	t.Helper()
	identity, err := IdentifyProcess(pid, 1, strings.Repeat("a", 43))
	if err != nil {
		return
	}
	t.Cleanup(func() { _ = signalProcess(identity, true) })
}

func observedPIDs(observation RunProcesses) map[int]bool {
	pids := map[int]bool{}
	for _, process := range observation.Processes {
		pids[process.PID] = true
	}
	return pids
}

func startGroupChildSupervisor(t *testing.T, ctx context.Context) (SupervisorSpec, *SupervisorHandle, int) {
	t.Helper()
	spec := newFiniteFixtureSpec(t, "group-child")
	handle, err := StartSupervisor(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	startedPath := strings.TrimPrefix(spec.Harness.Env[2], fixtureStartedEnv+"=")
	child := readFixturePID(t, startedPath)
	killPIDOnCleanup(t, child)
	return spec, handle, child
}

// A supervisor killed outright (for example by the OOM killer) must not make
// its surviving process-group members invisible.
func TestFiniteSupervisorDeathKeepsGroupMembersLive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	spec, handle, child := startGroupChildSupervisor(t, ctx)
	boundary := RunBoundary{RunID: spec.RunID, Token: spec.ProcessToken, RunDir: spec.RunDir, Supervisor: handle.Identity}
	observation, err := InspectRunBoundary(ctx, boundary)
	if err != nil || observation.Liveness != LivenessLive || observation.Supervisor == nil || !observation.Supervisor.Equal(handle.Identity) {
		t.Fatalf("live supervisor boundary = %#v, %v; want the supervisor", observation, err)
	}
	if err := syscall.Kill(handle.Identity.PID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = handle.Wait()
	if !linuxPIDAlive(child) {
		t.Fatal("fixture child exited with its supervisor")
	}
	observation, err = InspectRunBoundary(ctx, boundary)
	if err != nil || observation.Liveness != LivenessLive || observation.Supervisor != nil || !observedPIDs(observation)[child] {
		t.Fatalf("boundary after supervisor death = %#v, %v; want live group member %d without supervisor", observation, err, child)
	}
	if _, err := AdoptRun(ctx, handle.Identity); err == nil {
		t.Fatal("a dead supervisor was adopted")
	}
	grace := 300 * time.Millisecond
	started := time.Now()
	state, err := StopRunBoundary(ctx, boundary, grace)
	elapsed := time.Since(started)
	if err != nil || state != LivenessAbsent {
		t.Fatalf("orphaned group stop = %s, %v", state, err)
	}
	if linuxPIDAlive(child) {
		t.Fatalf("stop reported absence while group member %d still runs", child)
	}
	if elapsed < grace {
		t.Fatalf("SIGTERM-ignoring member was gone after %v, before the %v grace", elapsed, grace)
	}
	observation, err = InspectRunBoundary(ctx, boundary)
	if err != nil || observation.Liveness != LivenessAbsent {
		t.Fatalf("post-stop boundary = %#v, %v", observation, err)
	}
}

// StopRun signals the whole group and escalates to SIGKILL after the grace;
// absence is reported only when no member remains.
func TestFiniteStopRunEscalatesForTermIgnoringGroupMember(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	spec, handle, child := startGroupChildSupervisor(t, ctx)
	grace := 600 * time.Millisecond
	started := time.Now()
	state, err := StopRun(ctx, spec.RunID, spec.ProcessToken, grace)
	elapsed := time.Since(started)
	alive := linuxPIDAlive(child)
	if err != nil || state != LivenessAbsent || alive {
		t.Fatalf("StopRun = %s, %v after %v; SIGTERM-ignoring child %d alive=%v", state, err, elapsed, child, alive)
	}
	if elapsed < grace {
		t.Fatalf("absence after %v, before the %v grace: the child cannot have needed SIGKILL", elapsed, grace)
	}
	_ = handle.Wait()
}

// Recovery must keep a run whose supervisor died open while a process-group
// member survives; it must not settle the run as lost or dispatch it again.
func TestFiniteRecoveryKeepsOrphanedGroupOpen(t *testing.T) {
	engine := newEngineFixture(t, engineFixturePlan{Runs: map[string]engineFixtureBehavior{"1": {Result: ResultDone, Block: true}}})
	ctx := context.Background()
	task := intakeTestTask(t, engine.store, "orphaned-group", false)
	if err := engine.ScanOnce(ctx); err != nil {
		t.Fatal(err)
	}
	waitEngineInvocationCount(t, engine, 1)
	run, err := engine.store.GetRun(ctx, 1)
	if err != nil || run.SupervisorPID == nil {
		t.Fatalf("run lacks a recorded supervisor: %+v, %v", run, err)
	}
	supervisor := *run.SupervisorPID
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	started := filepath.Join(base, "member started")
	release := filepath.Join(base, "member release")
	member := exec.Command(executable, "finite-sleeper")
	member.Env = append(os.Environ(), fixtureStartedEnv+"="+started, fixtureReleaseEnv+"="+release)
	member.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pgid: supervisor}
	if err := member.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.WriteFile(release, nil, 0o600)
		_ = member.Process.Kill()
		_, _ = waitForCommand(member, 5*time.Second)
	})
	readFixturePID(t, started)
	if err := syscall.Kill(supervisor, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	waitForFixture(t, func() bool { return !linuxPIDAlive(supervisor) }, "supervisor exit")
	for pass := 0; pass < 3; pass++ {
		if err := engine.ScanOnce(ctx); !errors.Is(err, ErrDetachedProvider) {
			t.Fatalf("scan %d with a surviving group member = %v, want detached provider", pass, err)
		}
	}
	current, _ := engine.store.GetTask(ctx, task.ID)
	open, _ := engine.store.GetRun(ctx, run.ID)
	if current.State != StateWorking || open.EndedAt != nil || engineInvocationCount(t, engine) != 1 {
		t.Fatalf("surviving group member was treated as absent: task=%+v run=%+v invocations=%d", current, open, engineInvocationCount(t, engine))
	}
	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := waitForCommand(member, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := engine.ScanOnce(ctx); err != nil {
		t.Fatalf("scan after the last member exited: %v", err)
	}
	settled, _ := engine.store.GetRun(ctx, run.ID)
	if settled.EndedAt == nil {
		t.Fatalf("run stayed open after its whole group exited: %+v", settled)
	}
	assertEngineInvariants(t, engine.store, task.ID)
}

// A recorded supervisor PID that now belongs to an unrelated process names a
// different process group; nothing in it is part of the run or signalled.
func TestFiniteReusedSupervisorPIDIsNotTheRunGroup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	release := filepath.Join(base, "release")
	leaderStarted := filepath.Join(base, "leader")
	leader := exec.Command(executable, "finite-sleeper")
	leader.Env = append(os.Environ(), fixtureStartedEnv+"="+leaderStarted, fixtureReleaseEnv+"="+release)
	leader.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := leader.Start(); err != nil {
		t.Fatal(err)
	}
	readFixturePID(t, leaderStarted)
	memberStarted := filepath.Join(base, "member")
	member := exec.Command(executable, "finite-sleeper")
	member.Env = append(os.Environ(), fixtureStartedEnv+"="+memberStarted, fixtureReleaseEnv+"="+release)
	member.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pgid: leader.Process.Pid}
	if err := member.Start(); err != nil {
		t.Fatal(err)
	}
	readFixturePID(t, memberStarted)
	t.Cleanup(func() {
		_ = os.WriteFile(release, nil, 0o600)
		_ = leader.Process.Kill()
		_ = member.Process.Kill()
		_, _ = waitForCommand(leader, 5*time.Second)
		_, _ = waitForCommand(member, 5*time.Second)
	})
	leaderIdentity, _ := IdentifyProcess(leader.Process.Pid, 1, strings.Repeat("a", 43))
	memberIdentity, _ := IdentifyProcess(member.Process.Pid, 1, strings.Repeat("a", 43))
	token, err := NewProcessToken()
	if err != nil {
		t.Fatal(err)
	}
	for name, recorded := range map[string]ProcessIdentity{
		"stale_start": {PID: leader.Process.Pid, Start: "linux:1"},
		"pid_only":    {PID: leader.Process.Pid},
	} {
		t.Run(name, func(t *testing.T) {
			boundary := RunBoundary{RunID: 77, Token: token, RunDir: filepath.Join(base, "runs", "77"), Supervisor: recorded}
			observation, err := InspectRunBoundary(ctx, boundary)
			if err != nil || observation.Liveness != LivenessAbsent || len(observation.Processes) != 0 {
				t.Fatalf("reused-PID boundary = %#v, %v; want reliable absence", observation, err)
			}
			if state, err := StopRunBoundary(ctx, boundary, 0); err != nil || state != LivenessAbsent {
				t.Fatalf("reused-PID stop = %s, %v", state, err)
			}
			for _, identity := range []ProcessIdentity{leaderIdentity, memberIdentity} {
				current, err := IdentifyProcess(identity.PID, 1, strings.Repeat("a", 43))
				if err != nil || !current.Equal(identity) {
					t.Fatalf("unrelated process %d was signalled: %#v, %v", identity.PID, current, err)
				}
			}
		})
	}
}

// Only processes running the finite executable are interpreted as
// supervisors or marked providers. Unrelated argv cannot make a scan unknown.
func TestFiniteUnrelatedArgvDoesNotPoisonScan(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash is unavailable")
	}
	token, err := NewProcessToken()
	if err != nil {
		t.Fatal(err)
	}
	other, err := NewProcessToken()
	if err != nil {
		t.Fatal(err)
	}
	scriptDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(scriptDir, "supervise"), []byte("sleep 30; true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runDir := filepath.Join(t.TempDir(), "runs", "55555")
	for name, args := range map[string][]string{
		"grep_marker":       {"bash", "-c", "sleep 30; true", "grep", "--finite-run-id"},
		"marker_with_token": {"bash", "-c", "sleep 30; true", "x", finiteProviderRunIDArg, "55555", finiteProviderTokenArg, token},
		"other_marker":      {"bash", "-c", "sleep 30; true", "x", finiteProviderRunIDArg, "55555", finiteProviderTokenArg, other},
		"supervise_script":  {"bash", "supervise", "--run-id", "55555", "--run-dir", runDir, "--process-token", other},
		"supervise_ours":    {"bash", "supervise", "--run-id", "55555", "--run-dir", runDir, "--process-token", token},
		"supervise_garbage": {"bash", "supervise", "--bogus"},
	} {
		t.Run(name, func(t *testing.T) {
			command := &exec.Cmd{Path: bash, Args: args, Dir: scriptDir}
			command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
				_, _ = waitForCommand(command, 5*time.Second)
			}()
			waitForFixture(t, func() bool {
				data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(command.Process.Pid), "cmdline"))
				return err == nil && strings.Contains(string(data), args[len(args)-1])
			}, "unrelated process argv")
			for _, boundary := range []RunBoundary{
				{RunID: 55555, Token: token},
				{RunID: 55555, Token: token, RunDir: runDir},
			} {
				observation, err := InspectRunBoundary(ctx, boundary)
				if err != nil || observation.Liveness != LivenessAbsent {
					t.Fatalf("unrelated %v made the scan %#v, %v", args, observation, err)
				}
			}
		})
	}
}

// Run IDs restart at 1 in every database: another state root's genuine
// supervisor with the same run ID is not this run, while a second supervisor
// claiming this run's directory under another token stays fail-closed.
func TestFiniteOtherStateRootSameRunIDDoesNotPoison(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	foreign := newFiniteFixtureSpec(t, "block")
	handle, err := StartSupervisor(ctx, foreign)
	if err != nil {
		t.Fatal(err)
	}
	waitForFixture(t, func() bool { return fileExists(filepath.Join(foreign.RunDir, "supervisor-started.json")) }, "foreign supervisor start")
	if observation, err := InspectRun(ctx, foreign.RunID, foreign.ProcessToken); err != nil || observation.Supervisor == nil {
		t.Fatalf("foreign supervisor not found by its own run: %#v, %v", observation, err)
	}
	token, err := NewProcessToken()
	if err != nil {
		t.Fatal(err)
	}
	ownRunDir := filepath.Join(t.TempDir(), "other state", "runs", strconv.FormatInt(foreign.RunID, 10))
	for name, boundary := range map[string]RunBoundary{
		"with_run_dir":    {RunID: foreign.RunID, Token: token, RunDir: ownRunDir},
		"without_run_dir": {RunID: foreign.RunID, Token: token},
	} {
		observation, err := InspectRunBoundary(ctx, boundary)
		if err != nil || observation.Liveness != LivenessAbsent {
			t.Fatalf("%s: another root's run %d poisoned this one: %#v, %v", name, foreign.RunID, observation, err)
		}
	}
	if _, err := InspectRunBoundary(ctx, RunBoundary{RunID: foreign.RunID, Token: token, RunDir: foreign.RunDir}); !errors.Is(err, ErrProcessTokenMismatch) {
		t.Fatalf("a second token claiming the same run directory = %v, want token mismatch", err)
	}
	if _, err := InspectRun(ctx, foreign.RunID+1, foreign.ProcessToken); !errors.Is(err, ErrProcessTokenMismatch) {
		t.Fatalf("a run token bound to another run ID = %v, want token mismatch", err)
	}
	if state, err := StopRun(ctx, foreign.RunID, foreign.ProcessToken, 0); err != nil || state != LivenessAbsent {
		t.Fatalf("foreign cleanup stop = %s, %v", state, err)
	}
	_ = handle.Wait()
}

// After an upgrade replaces the finite binary, /proc/<pid>/exe of a running
// supervisor reads "<path> (deleted)"; it is still the finite executable.
func TestFiniteDeletedSupervisorBinaryStillRecognized(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	spec := newFiniteFixtureSpec(t, "block")
	source, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	installed := filepath.Join(t.TempDir(), "werkrby-finite")
	copyExecutable(t, source, installed)
	spec.Executable = installed
	handle, err := StartSupervisor(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	waitForFixture(t, func() bool { return fileExists(filepath.Join(spec.RunDir, "supervisor-started.json")) }, "supervisor start")
	if err := os.Remove(installed); err != nil {
		t.Fatal(err)
	}
	copyExecutable(t, source, installed)
	link, err := os.Readlink(filepath.Join("/proc", strconv.Itoa(handle.Identity.PID), "exe"))
	if err != nil || !strings.HasSuffix(link, " (deleted)") {
		t.Fatalf("upgrade fixture exe link = %q, %v", link, err)
	}
	boundary := RunBoundary{RunID: spec.RunID, Token: spec.ProcessToken, RunDir: spec.RunDir, Executable: installed}
	observation, err := InspectRunBoundary(ctx, boundary)
	if err != nil || observation.Supervisor == nil || !observation.Supervisor.Equal(handle.Identity) {
		t.Fatalf("supervisor of a replaced binary = %#v, %v", observation, err)
	}
	// Without the configured path, the recorded supervisor identity still
	// recognises the exact generation.
	observation, err = InspectRunBoundary(ctx, RunBoundary{RunID: spec.RunID, Token: spec.ProcessToken, RunDir: spec.RunDir, Supervisor: handle.Identity})
	if err != nil || observation.Supervisor == nil || !observation.Supervisor.Equal(handle.Identity) {
		t.Fatalf("recorded supervisor of a moved binary = %#v, %v", observation, err)
	}
	if state, err := StopRunBoundary(ctx, boundary, 0); err != nil || state != LivenessAbsent {
		t.Fatalf("stop = %s, %v", state, err)
	}
	_ = handle.Wait()
}

func copyExecutable(t *testing.T, source, destination string) {
	t.Helper()
	input, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o700)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(output, input); err != nil {
		_ = output.Close()
		t.Fatal(err)
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}
}
