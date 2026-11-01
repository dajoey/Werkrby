package finite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/harness"
)

const (
	fixtureBehaviorEnv = "WERKRBY_FINITE_FIXTURE_BEHAVIOR"
	fixtureReleaseEnv  = "WERKRBY_FINITE_FIXTURE_RELEASE"
	fixtureStartedEnv  = "WERKRBY_FINITE_FIXTURE_STARTED"
)

func TestMain(m *testing.M) {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "supervise":
			os.Exit(runFiniteSupervisorFixture())
		case "finite-fake-provider":
			os.Exit(runFiniteFakeProvider())
		case "finite-owner-fixture":
			os.Exit(runFiniteOwnerFixture())
		case "finite-lock-parent":
			os.Exit(runFiniteLockParent())
		case "finite-lock-child":
			os.Exit(runFiniteLockChild())
		case "finite-sleeper":
			os.Exit(runFiniteSleeper())
		case "finite-term-ignoring-sleeper":
			signal.Ignore(syscall.SIGTERM)
			os.Exit(runFiniteSleeper())
		}
	}
	os.Exit(m.Run())
}

func TestFiniteProcessTokenAndOSStartIdentity(t *testing.T) {
	first, err := NewProcessToken()
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewProcessToken()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("per-run process tokens are not unique")
	}
	identity, err := IdentifyProcess(os.Getpid(), 41, first)
	if err != nil {
		t.Fatal(err)
	}
	if identity.PID != os.Getpid() || identity.RunID != 41 || identity.Token != first {
		t.Fatalf("identity binding = %#v", identity)
	}
	if !strings.HasPrefix(identity.Start, runtime.GOOS+":") {
		t.Fatalf("OS process-start identity = %q, want %s-native generation", identity.Start, runtime.GOOS)
	}
}

func TestFiniteProcessBindingValidation(t *testing.T) {
	if _, err := InspectRun(context.Background(), 0, strings.Repeat("a", 43)); err == nil {
		t.Fatal("zero run ID was accepted")
	}
	if _, err := InspectRun(context.Background(), 1, "not-a-token"); err == nil {
		t.Fatal("malformed process token was accepted")
	}
}

func TestFiniteUnsupportedPlatformsRefuseDispatch(t *testing.T) {
	if runtime.GOOS == "linux" || runtime.GOOS == "windows" {
		if !SupportsProcessBoundary() {
			t.Fatalf("%s must support the finite process boundary", runtime.GOOS)
		}
		return
	}
	if SupportsProcessBoundary() {
		t.Fatalf("%s unexpectedly reports a supported boundary", runtime.GOOS)
	}
	spec := SupervisorSpec{RunID: 1, RunDir: t.TempDir(), ProcessToken: strings.Repeat("a", 43)}
	if _, err := StartSupervisor(context.Background(), spec); !errors.Is(err, ErrUnsupportedPlatform) {
		t.Fatalf("dispatch error = %v, want ErrUnsupportedPlatform", err)
	}
	observation, err := InspectRun(context.Background(), 1, strings.Repeat("a", 43))
	if !errors.Is(err, ErrUnsupportedPlatform) || observation.Liveness != LivenessUnknown {
		t.Fatalf("unsupported liveness = %#v, %v", observation, err)
	}
}

func newFiniteFixtureSpec(t *testing.T, behavior string) SupervisorSpec {
	t.Helper()
	root := filepath.Join(t.TempDir(), "finite runs 世界")
	runDir := filepath.Join(root, "run 42 λ")
	token, err := NewProcessToken()
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	release := filepath.Join(root, "release provider")
	started := filepath.Join(root, "provider started")
	spec := SupervisorSpec{
		RunID:        42,
		RunDir:       runDir,
		ProcessToken: token,
		Executable:   executable,
		SessionKey:   "finite:test:42",
		Prompt:       "synthetic finite prompt",
		Harness: harness.HarnessConfig{
			Name: "finite-fixture",
			Cwd:  runDir,
			Env: []string{
				fixtureBehaviorEnv + "=" + behavior,
				fixtureReleaseEnv + "=" + release,
				fixtureStartedEnv + "=" + started,
			},
		},
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = StopRun(ctx, spec.RunID, spec.ProcessToken, 0)
		_ = os.WriteFile(release, []byte("cleanup"), 0o600)
	})
	return spec
}

func runFiniteSupervisorFixture() int {
	if err := RunSupervisor(context.Background(), os.Args[1:], newFiniteFixtureHarness); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

type finiteFixtureHarness struct {
	cfg harness.HarnessConfig
}

func newFiniteFixtureHarness(cfg harness.HarnessConfig) (harness.Harness, error) {
	runDir := os.Getenv(finiteRunDirEnv)
	record, _ := json.Marshal(map[string]string{
		"run_dir":       runDir,
		"sessions_file": cfg.SessionsFile,
		"cwd":           cfg.Cwd,
	})
	if err := os.WriteFile(filepath.Join(runDir, "factory.json"), record, 0o600); err != nil {
		return nil, err
	}
	return &finiteFixtureHarness{cfg: cfg}, nil
}

func (h *finiteFixtureHarness) Start(context.Context) error {
	return h.recordCall("start")
}

func (h *finiteFixtureHarness) Send(ctx context.Context, key, prompt string, emit core.Emit) (string, bool, error) {
	if err := h.recordCall("send"); err != nil {
		return "", false, err
	}
	behavior := os.Getenv(fixtureBehaviorEnv)
	if behavior == "group-child" {
		// An unmarked provider child inherits the supervisor's process group
		// and ignores SIGTERM, as a real provider tool may.
		executable, err := os.Executable()
		if err != nil {
			return "", false, err
		}
		command := exec.Command(executable, "finite-term-ignoring-sleeper")
		command.Env = mergeEnvironment(os.Environ(), h.cfg.Env)
		if err := command.Start(); err != nil {
			return "", false, err
		}
		if err := waitForReleaseFile(ctx, os.Getenv(fixtureReleaseEnv)); err != nil {
			return "", false, err
		}
	}
	if behavior == "block" || behavior == "orphan" {
		executable, err := os.Executable()
		if err != nil {
			return "", false, err
		}
		command := exec.Command(executable, "finite-fake-provider",
			finiteProviderRunIDArg, os.Getenv(finiteRunIDEnv),
			finiteProviderTokenArg, os.Getenv(finiteRunTokenEnv))
		command.Env = mergeEnvironment(os.Environ(), h.cfg.Env)
		if err := command.Start(); err != nil {
			return "", false, err
		}
		if behavior == "block" {
			if err := waitForReleaseFile(ctx, os.Getenv(fixtureReleaseEnv)); err != nil {
				return "", false, err
			}
		}
	}
	emit(core.Event{Kind: core.EventDelta, Text: "fixture " + prompt})
	emit(core.Event{Kind: core.EventFinal, Text: "fixture complete", Done: true})
	return "fixture-thread", false, nil
}

func (h *finiteFixtureHarness) Close() error { return h.recordCall("close") }

func (h *finiteFixtureHarness) Interrupt(context.Context, string) (bool, error) { return true, nil }
func (h *finiteFixtureHarness) ResetSession(string) error                       { return nil }
func (h *finiteFixtureHarness) ThreadID(string) string                          { return "fixture-thread" }
func (h *finiteFixtureHarness) IsActive(string) bool                            { return false }

func (h *finiteFixtureHarness) recordCall(kind string) error {
	path := filepath.Join(os.Getenv(finiteRunDirEnv), "calls.jsonl")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	return json.NewEncoder(file).Encode(map[string]string{"kind": kind})
}

func runFiniteFakeProvider() int {
	// A provider admitted beyond its supervisor's lifetime explicitly holds
	// the query-only containment handle. Linux uses argv markers instead.
	containment, err := openSupervisorContainment(os.Getenv(finiteRunTokenEnv))
	if err != nil {
		return 1
	}
	defer containment.Close()
	if path := os.Getenv(fixtureStartedEnv); path != "" {
		if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
			return 1
		}
	}
	if err := waitForReleaseFile(context.Background(), os.Getenv(fixtureReleaseEnv)); err != nil {
		return 1
	}
	return 0
}

type finiteOwnerFixtureInput struct {
	Root string
	Spec SupervisorSpec
}

func runFiniteOwnerFixture() int {
	data, err := os.ReadFile(os.Args[2])
	if err != nil {
		return 2
	}
	var input finiteOwnerFixtureInput
	if json.Unmarshal(data, &input) != nil {
		return 2
	}
	lock, err := AcquireWorkspaceLock(input.Root)
	if errors.Is(err, ErrWorkspaceLocked) {
		_ = os.WriteFile(filepath.Join(input.Root, fmt.Sprintf("owner-loser-%d", os.Getpid())), nil, 0o600)
		return 0
	}
	if err != nil {
		return 2
	}
	defer lock.Close()
	if err := os.WriteFile(filepath.Join(input.Root, fmt.Sprintf("owner-acquired-%d", os.Getpid())), nil, 0o600); err != nil {
		return 2
	}
	handle, err := StartSupervisor(context.Background(), input.Spec)
	if err != nil {
		_ = os.WriteFile(filepath.Join(input.Root, fmt.Sprintf("owner-error-%d", os.Getpid())), []byte(err.Error()), 0o600)
		return 3
	}
	identity, _ := json.Marshal(handle.Identity)
	if err := os.WriteFile(filepath.Join(input.Root, fmt.Sprintf("owner-spawned-%d", os.Getpid())), identity, 0o600); err != nil {
		return 2
	}
	if err := waitForReleaseFile(context.Background(), os.Getenv(fixtureReleaseEnv)); err != nil {
		return 2
	}
	if err := handle.Wait(); err != nil {
		return 4
	}
	return 0
}

func runFiniteLockParent() int {
	root := os.Args[2]
	lock, err := AcquireWorkspaceLock(root)
	if err != nil {
		return 2
	}
	if lock == nil {
		return 2
	}
	executable, err := os.Executable()
	if err != nil {
		return 2
	}
	child := exec.Command(executable, "finite-lock-child", root)
	child.Env = os.Environ()
	if err := child.Start(); err != nil {
		return 2
	}
	if err := os.WriteFile(filepath.Join(root, "lock-parent-ready"), []byte(strconv.Itoa(child.Process.Pid)), 0o600); err != nil {
		return 2
	}
	if err := waitForReleaseFile(context.Background(), filepath.Join(root, "parent-release")); err != nil {
		return 2
	}
	// Deliberately exit without Close. The waiting child must not inherit the
	// descriptor that keeps owner.lock held.
	return 0
}

func runFiniteLockChild() int {
	root := os.Args[2]
	if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("lock-child-started-%d", os.Getpid())), nil, 0o600); err != nil {
		return 1
	}
	if err := waitForReleaseFile(context.Background(), os.Getenv(fixtureReleaseEnv)); err != nil {
		return 1
	}
	if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("lock-child-exited-%d", os.Getpid())), nil, 0o600); err != nil {
		return 1
	}
	return 0
}

func runFiniteSleeper() int {
	if err := os.WriteFile(os.Getenv(fixtureStartedEnv), []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		return 1
	}
	if err := waitForReleaseFile(context.Background(), os.Getenv(fixtureReleaseEnv)); err != nil {
		return 1
	}
	return 0
}

func startFiniteSleeper(t *testing.T) (*exec.Cmd, string) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	release := filepath.Join(root, "release")
	started := filepath.Join(root, "started")
	command := exec.Command(executable, "finite-sleeper")
	command.Env = append(os.Environ(), fixtureReleaseEnv+"="+release, fixtureStartedEnv+"="+started)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.WriteFile(release, []byte("cleanup"), 0o600)
		if command.Process != nil {
			_ = command.Process.Kill()
		}
		_, _ = waitForCommand(command, 5*time.Second)
	})
	waitForFixture(t, func() bool { return fileExists(started) }, "unrelated process start")
	return command, release
}

func waitForReleaseFile(ctx context.Context, path string) error {
	if path == "" {
		return fmt.Errorf("fixture release path is empty")
	}
	for {
		if fileExists(path) {
			return nil
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func waitForFixture(t *testing.T, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func waitForCommand(command *exec.Cmd, timeout time.Duration) (error, error) {
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	select {
	case err := <-done:
		return err, nil
	case <-time.After(timeout):
		if command.Process != nil {
			_ = command.Process.Kill()
		}
		return nil, fmt.Errorf("process %d did not exit", command.Process.Pid)
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func readFixtureCallKinds(t *testing.T, runDir string) []string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(runDir, "calls.jsonl"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var kinds []string
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		var record map[string]string
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal(err)
		}
		kinds = append(kinds, record["kind"])
	}
	return kinds
}
