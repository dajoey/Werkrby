package finite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
)

type projectionCrashCase struct {
	Root       string
	Worker     string
	TaskID     string
	Point      string
	MarkerPath string
}

type projectionCrashMarker struct {
	OwnerPID int    `json:"owner_pid"`
	Point    string `json:"point"`
}

type projectionFixture struct {
	cfg       Config
	taskID    string
	old       []byte
	new       []byte
	runs      int
	events    int
	outbox    int
	doneState Task
}

func prepareProjectionFixture(t *testing.T, id string) projectionFixture {
	t.Helper()
	cfg := Config{Root: t.TempDir(), WorkerDir: t.TempDir()}
	s, err := OpenStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	task := intakeTestTask(t, s, id, false)
	run := dispatchTestRun(t, s, task, PhaseImplementation)
	if err := s.ProjectTask(context.Background(), task.ID); err != nil {
		t.Fatal(err)
	}
	old, err := os.ReadFile(task.ProjectionPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.SettleRun(context.Background(), SettlementRequest{RunID: run.ID, ExpectedTaskVersion: task.Version + 1, ExecutionOutcome: RunOK, Result: validTestResult(run, ResultDone)}); err != nil {
		t.Fatal(err)
	}
	current, err := s.GetTask(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	persistedRun, err := s.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	fixture := projectionFixture{
		cfg: cfg, taskID: task.ID, old: old, new: []byte(RenderProjection(current, []Run{persistedRun})),
		runs: countRows(t, s, `SELECT COUNT(*) FROM runs`), events: countRows(t, s, `SELECT COUNT(*) FROM task_events`),
		outbox: countRows(t, s, `SELECT COUNT(*) FROM outbox`), doneState: current,
	}
	if bytes.Equal(fixture.old, fixture.new) {
		t.Fatal("projection fixture did not create distinct old and new complete files")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func (f projectionFixture) projectionPath(t *testing.T) string {
	t.Helper()
	s, err := OpenStore(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	task, err := s.GetTask(context.Background(), f.taskID)
	if err != nil {
		t.Fatal(err)
	}
	return task.ProjectionPath
}

func (f projectionFixture) requireUnchangedAndRegenerate(t *testing.T) {
	t.Helper()
	s, err := OpenStore(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	current, err := s.GetTask(context.Background(), f.taskID)
	if err != nil || current.State != f.doneState.State || current.Version != f.doneState.Version {
		t.Fatalf("projection fault rolled back settlement: %+v, %v", current, err)
	}
	if got := countRows(t, s, `SELECT COUNT(*) FROM runs`); got != f.runs {
		t.Fatalf("projection changed run count: %d", got)
	}
	if got := countRows(t, s, `SELECT COUNT(*) FROM task_events`); got != f.events {
		t.Fatalf("projection changed event count: %d", got)
	}
	if got := countRows(t, s, `SELECT COUNT(*) FROM outbox`); got != f.outbox {
		t.Fatalf("projection changed outbox count: %d", got)
	}
	if err := s.ProjectTask(context.Background(), f.taskID); err != nil {
		t.Fatalf("regenerate projection: %v", err)
	}
	contents, err := os.ReadFile(current.ProjectionPath)
	if err != nil || !bytes.Equal(contents, f.new) {
		t.Fatalf("regenerated projection is not complete-new: err=%v", err)
	}
	if err := s.CheckInvariants(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := countRows(t, s, `SELECT COUNT(*) FROM runs`); got != f.runs {
		t.Fatalf("regeneration created a provider run: %d", got)
	}
	if got := countRows(t, s, `SELECT COUNT(*) FROM task_events`); got != f.events {
		t.Fatalf("regeneration created an event: %d", got)
	}
	if got := countRows(t, s, `SELECT COUNT(*) FROM outbox`); got != f.outbox {
		t.Fatalf("regeneration created an outbox row: %d", got)
	}
}

func TestFiniteProjectionRecovery_R5(t *testing.T) {
	for _, point := range []projectionPublicationFaultPoint{projectionAfterTempWrite, projectionBeforeRename, projectionAfterRename} {
		t.Run("owner_crash/"+string(point), func(t *testing.T) {
			fixture := prepareProjectionFixture(t, "r5-crash-"+strings.ReplaceAll(string(point), "_", "-"))
			ownerCase := projectionCrashCase{Root: fixture.cfg.Root, Worker: fixture.cfg.WorkerDir, TaskID: fixture.taskID, Point: string(point), MarkerPath: filepath.Join(t.TempDir(), "crash.json")}
			runProjectionCrashOwner(t, ownerCase)
			contents, err := os.ReadFile(fixture.projectionPath(t))
			if err != nil {
				t.Fatal(err)
			}
			completeOld := bytes.Equal(contents, fixture.old)
			completeNew := bytes.Equal(contents, fixture.new)
			if completeOld == completeNew {
				t.Fatalf("crash left a torn projection: old=%t new=%t bytes=%d", completeOld, completeNew, len(contents))
			}
			if point == projectionAfterRename && !completeNew {
				t.Fatal("post-rename crash did not preserve the complete-new projection")
			}
			if point != projectionAfterRename && !completeOld {
				t.Fatal("pre-publication crash replaced the complete-old projection")
			}
			fixture.requireUnchangedAndRegenerate(t)
		})
	}

	t.Run("ENOSPC/injected_publication_error", func(t *testing.T) {
		fixture := prepareProjectionFixture(t, "r5-enospc")
		s, err := OpenStore(fixture.cfg)
		if err != nil {
			t.Fatal(err)
		}
		restore := registerProjectionPublicationFault(projectionAfterTempWrite, func() error { return syscall.ENOSPC })
		err = s.ProjectTask(context.Background(), fixture.taskID)
		restore()
		_ = s.Close()
		if !errors.Is(err, syscall.ENOSPC) {
			t.Fatalf("injected publication error = %v, want ENOSPC", err)
		}
		contents, readErr := os.ReadFile(fixture.projectionPath(t))
		if readErr != nil || !bytes.Equal(contents, fixture.old) {
			t.Fatalf("ENOSPC replaced complete-old projection: err=%v", readErr)
		}
		fixture.requireUnchangedAndRegenerate(t)
	})

	t.Run("read_only_directory/real_filesystem_fault", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("POSIX directory permissions are required for this real read-only-directory fault")
		}
		fixture := prepareProjectionFixture(t, "r5-read-only")
		path := fixture.projectionPath(t)
		directory := filepath.Dir(path)
		if err := os.Chmod(directory, 0o500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(directory, 0o700) })
		s, err := OpenStore(fixture.cfg)
		if err != nil {
			t.Fatal(err)
		}
		err = s.ProjectTask(context.Background(), fixture.taskID)
		_ = s.Close()
		if !errors.Is(err, os.ErrPermission) {
			t.Fatalf("read-only projection directory error = %v, want permission failure", err)
		}
		contents, readErr := os.ReadFile(path)
		if readErr != nil || !bytes.Equal(contents, fixture.old) {
			t.Fatalf("read-only directory replaced complete-old projection: err=%v", readErr)
		}
		if err := os.Chmod(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		fixture.requireUnchangedAndRegenerate(t)
	})
}

const projectionCrashConfigEnv = "WERKRBY_FINITE_PROJECTION_CRASH_CONFIG"

func TestFiniteProjectionCrashHelperProcess(t *testing.T) {
	path := os.Getenv(projectionCrashConfigEnv)
	if path == "" {
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var ownerCase projectionCrashCase
	if err := json.Unmarshal(data, &ownerCase); err != nil {
		t.Fatal(err)
	}
	lock, err := AcquireWorkspaceLock(ownerCase.Root)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	s, err := OpenStore(Config{Root: ownerCase.Root, WorkerDir: ownerCase.Worker})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	restore := registerProjectionPublicationFault(projectionPublicationFaultPoint(ownerCase.Point), func() error {
		marker, _ := json.Marshal(projectionCrashMarker{OwnerPID: os.Getpid(), Point: ownerCase.Point})
		_ = os.WriteFile(ownerCase.MarkerPath, marker, 0o600)
		os.Exit(86)
		return nil
	})
	defer restore()
	if err := s.ProjectTask(context.Background(), ownerCase.TaskID); err != nil {
		t.Fatal(err)
	}
}

func runProjectionCrashOwner(t *testing.T, ownerCase projectionCrashCase) {
	t.Helper()
	configPath := filepath.Join(t.TempDir(), "projection-crash-config.json")
	writeEngineJSONFile(t, configPath, ownerCase)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(t.TempDir(), "projection-owner.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run=^TestFiniteProjectionCrashHelperProcess$")
	command.Env = append(os.Environ(), projectionCrashConfigEnv+"="+configPath)
	command.Stdout = logFile
	command.Stderr = logFile
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		t.Fatal(err)
	}
	ownerPID := command.Process.Pid
	err = command.Wait()
	_ = logFile.Close()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 86 {
		output, _ := os.ReadFile(logPath)
		t.Fatalf("projection owner %d did not crash at %s: err=%v output=%s", ownerPID, ownerCase.Point, err, output)
	}
	marker, err := os.ReadFile(ownerCase.MarkerPath)
	if err != nil {
		t.Fatal(err)
	}
	var identity projectionCrashMarker
	if err := json.Unmarshal(marker, &identity); err != nil || identity.OwnerPID != ownerPID || identity.Point != ownerCase.Point {
		t.Fatalf("invalid projection crash marker: %s, err=%v", marker, err)
	}
	t.Logf("projection owner_pid=%d failpoint=%s complete_old_or_new=true", ownerPID, ownerCase.Point)
}
