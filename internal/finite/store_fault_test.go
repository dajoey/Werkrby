package finite

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ncruces/go-sqlite3"
	"github.com/ncruces/go-sqlite3/util/vfsutil"
	"github.com/ncruces/go-sqlite3/vfs"
)

type sqliteWriteFaults struct {
	code atomic.Int32
	hits atomic.Int64
}

func (f *sqliteWriteFaults) arm(code sqlite3.ErrorCode) { f.code.Store(int32(code)) }
func (f *sqliteWriteFaults) disarm()                    { f.code.Store(0) }

type sqliteFaultVFS struct {
	base   vfs.VFS
	faults *sqliteWriteFaults
}

func (v *sqliteFaultVFS) Open(name string, flags vfs.OpenFlag) (vfs.File, vfs.OpenFlag, error) {
	file, flags, err := v.base.Open(name, flags)
	if err != nil {
		return nil, flags, err
	}
	return &sqliteFaultFile{File: file, name: name, faults: v.faults}, flags, nil
}

func (v *sqliteFaultVFS) OpenFilename(name *vfs.Filename, flags vfs.OpenFlag) (vfs.File, vfs.OpenFlag, error) {
	file, flags, err := vfsutil.WrapOpenFilename(v.base, name, flags)
	if err != nil {
		return nil, flags, err
	}
	return &sqliteFaultFile{File: file, name: name.String(), faults: v.faults}, flags, nil
}

func (v *sqliteFaultVFS) Delete(name string, syncDir bool) error { return v.base.Delete(name, syncDir) }
func (v *sqliteFaultVFS) Access(name string, flags vfs.AccessFlag) (bool, error) {
	return v.base.Access(name, flags)
}
func (v *sqliteFaultVFS) FullPathname(name string) (string, error) {
	return v.base.FullPathname(name)
}

type sqliteFaultFile struct {
	vfs.File
	name   string
	faults *sqliteWriteFaults
}

func (f *sqliteFaultFile) WriteAt(data []byte, offset int64) (int, error) {
	code := sqlite3.ErrorCode(f.faults.code.Load())
	if code != 0 && strings.Contains(f.name, "engine.db") {
		f.faults.hits.Add(1)
		return 0, code
	}
	return f.File.WriteAt(data, offset)
}

func (f *sqliteFaultFile) Unwrap() vfs.File { return f.File }

func (f *sqliteFaultFile) SharedMemory() vfs.SharedMemory {
	if file, ok := f.File.(vfs.FileSharedMemory); ok {
		return file.SharedMemory()
	}
	return nil
}

var sqliteFaultVFSSequence atomic.Uint64

func newF1Store(t *testing.T, faults *sqliteWriteFaults) *Store {
	t.Helper()
	originalDSN := storeSQLiteDSN
	vfsName := ""
	if faults != nil {
		vfsName = fmt.Sprintf("finite_fault_%d", sqliteFaultVFSSequence.Add(1))
		vfs.Register(vfsName, &sqliteFaultVFS{base: vfs.Find(""), faults: faults})
	}
	storeSQLiteDSN = func(cfg Config) string {
		dsn := strings.Replace(originalDSN(cfg), "busy_timeout(5000)", "busy_timeout(50)", 1)
		if vfsName != "" {
			dsn += "&vfs=" + vfsName
		}
		return dsn
	}
	s, err := OpenStore(Config{Root: t.TempDir(), WorkerDir: t.TempDir()})
	if err != nil {
		storeSQLiteDSN = originalDSN
		if vfsName != "" {
			vfs.Unregister(vfsName)
		}
		t.Fatalf("open F1 store: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
		storeSQLiteDSN = originalDSN
		if vfsName != "" {
			vfs.Unregister(vfsName)
		}
	})
	return s
}

func holdF1WriteLock(t *testing.T, s *Store) *sql.Tx {
	t.Helper()
	db, err := sql.Open("sqlite3", storeSQLiteDSN(s.cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatalf("acquire competing WAL write lock: %v", err)
	}
	return tx
}

func TestFiniteDatabaseFaults_F1(t *testing.T) {
	for _, testCase := range []struct {
		name string
		code sqlite3.ErrorCode
	}{
		{"busy_real_wal_lock", sqlite3.BUSY},
		{"full_vfs_write", sqlite3.FULL},
		{"ioerr_vfs_write", sqlite3.IOERR},
	} {
		t.Run("admission/"+testCase.name, func(t *testing.T) {
			var faults *sqliteWriteFaults
			if testCase.code != sqlite3.BUSY {
				faults = &sqliteWriteFaults{}
			}
			s := newF1Store(t, faults)
			task := intakeTestTask(t, s, "f1-admission-"+strings.ReplaceAll(testCase.name, "_", "-"), false)
			var lock *sql.Tx
			if testCase.code == sqlite3.BUSY {
				lock = holdF1WriteLock(t, s)
			} else {
				faults.arm(testCase.code)
			}
			launches := 0
			_, err := s.Dispatch(context.Background(), DispatchRequest{
				TaskID: task.ID, ExpectedVersion: task.Version, Phase: PhaseImplementation,
				Seat: "seat-a", SessionKey: "finite:f1", LaunchReason: "initial", PromptHash: "abc",
			}, func(context.Context, Run) error { launches++; return nil })
			if lock != nil {
				_ = lock.Rollback()
			}
			if faults != nil {
				faults.disarm()
				if faults.hits.Load() == 0 {
					t.Fatal("VFS write fault was not reached")
				}
			}
			if !errors.Is(err, testCase.code) {
				t.Fatalf("admission fault = %v, want SQLite code %v", err, testCase.code)
			}
			if launches != 0 {
				t.Fatalf("admission fault spawned %d runs", launches)
			}
			current, getErr := s.GetTask(context.Background(), task.ID)
			if getErr != nil || current.State != StateTodo || current.Version != 0 {
				t.Fatalf("admission fault changed task: %+v, %v", current, getErr)
			}
			if got := countRows(t, s, `SELECT COUNT(*) FROM runs`); got != 0 {
				t.Fatalf("admission fault committed %d runs", got)
			}
			if got := countRows(t, s, `SELECT COUNT(*) FROM task_events`); got != 0 {
				t.Fatalf("admission fault committed %d events", got)
			}
			if err := s.CheckInvariants(context.Background()); err != nil {
				t.Fatal(err)
			}
			t.Logf("fault=%s launches=%d runs=0 events=0", testCase.code, launches)
		})

		t.Run("settlement/"+testCase.name, func(t *testing.T) {
			var faults *sqliteWriteFaults
			if testCase.code != sqlite3.BUSY {
				faults = &sqliteWriteFaults{}
			}
			s := newF1Store(t, faults)
			task := intakeTestTask(t, s, "f1-settlement-"+strings.ReplaceAll(testCase.name, "_", "-"), false)
			run := dispatchTestRun(t, s, task, PhaseImplementation)
			req := SettlementRequest{RunID: run.ID, ExpectedTaskVersion: task.Version + 1, ExecutionOutcome: RunOK, Result: validTestResult(run, ResultDone), Usage: &Usage{Source: "f1", CollectedAt: s.now()}}
			var lock *sql.Tx
			if testCase.code == sqlite3.BUSY {
				lock = holdF1WriteLock(t, s)
			} else {
				faults.arm(testCase.code)
			}
			_, err := s.SettleRun(context.Background(), req)
			if lock != nil {
				_ = lock.Rollback()
			}
			if faults != nil {
				faults.disarm()
				if faults.hits.Load() == 0 {
					t.Fatal("VFS write fault was not reached")
				}
			}
			if !errors.Is(err, testCase.code) {
				t.Fatalf("settlement fault = %v, want SQLite code %v", err, testCase.code)
			}
			current, getErr := s.GetTask(context.Background(), task.ID)
			if getErr != nil || current.State != StateWorking || current.Version != 1 {
				t.Fatalf("settlement fault changed task: %+v, %v", current, getErr)
			}
			persisted, getErr := s.GetRun(context.Background(), run.ID)
			if getErr != nil || persisted.EndedAt != nil {
				t.Fatalf("settlement fault partially ended run: %+v, %v", persisted, getErr)
			}
			assertSettlementWriteCounts(t, s, run.ID, 0)
			if err := s.CheckInvariants(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, err := s.SettleRun(context.Background(), req); err != nil {
				t.Fatalf("settlement after fault readback: %v", err)
			}
			assertSettlementWriteCounts(t, s, run.ID, 1)
		})
	}

	t.Run("admission/commit_unknown_explicit_readback", func(t *testing.T) {
		s := newF1Store(t, nil)
		task := intakeTestTask(t, s, "f1-admission-commit-unknown", false)
		sentinel := errors.New("post-commit result is ambiguous")
		restore := registerStoreTransactionFault(storeTransactionAfterCommit, func() error { return sentinel })
		launches := 0
		request := DispatchRequest{TaskID: task.ID, ExpectedVersion: task.Version, Phase: PhaseImplementation, Seat: "seat-a", SessionKey: "finite:f1", LaunchReason: "initial", PromptHash: "abc"}
		_, err := s.Dispatch(context.Background(), request, func(context.Context, Run) error { launches++; return nil })
		restore()
		if !errors.Is(err, sentinel) || launches != 0 {
			t.Fatalf("ambiguous admission = %v, launches=%d", err, launches)
		}
		// The committed result is read back before any retry or spawn decision.
		current, getErr := s.GetTask(context.Background(), task.ID)
		if getErr != nil || current.State != StateWorking || current.Version != 1 {
			t.Fatalf("admission commit readback = %+v, %v", current, getErr)
		}
		if got := countRows(t, s, `SELECT COUNT(*) FROM runs`); got != 1 {
			t.Fatalf("admission commit readback runs = %d, want 1", got)
		}
		_, err = s.Dispatch(context.Background(), request, func(context.Context, Run) error { launches++; return nil })
		if !errors.Is(err, ErrVersionConflict) || launches != 0 {
			t.Fatalf("retry did not honor admission readback: %v, launches=%d", err, launches)
		}
		if got := countRows(t, s, `SELECT COUNT(*) FROM runs`); got != 1 {
			t.Fatalf("ambiguous admission retry duplicated runs: %d", got)
		}
		if err := s.CheckInvariants(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("settlement/commit_unknown_explicit_readback", func(t *testing.T) {
		s := newF1Store(t, nil)
		task := intakeTestTask(t, s, "f1-settlement-commit-unknown", false)
		run := dispatchTestRun(t, s, task, PhaseImplementation)
		req := SettlementRequest{RunID: run.ID, ExpectedTaskVersion: task.Version + 1, ExecutionOutcome: RunOK, Result: validTestResult(run, ResultDone), Usage: &Usage{Source: "f1", CollectedAt: s.now()}}
		sentinel := errors.New("post-commit result is ambiguous")
		restore := registerStoreTransactionFault(storeTransactionAfterCommit, func() error { return sentinel })
		_, err := s.SettleRun(context.Background(), req)
		restore()
		if !errors.Is(err, sentinel) {
			t.Fatalf("ambiguous settlement = %v", err)
		}
		current, getErr := s.GetTask(context.Background(), task.ID)
		if getErr != nil || current.State != StateDone || current.Version != 2 {
			t.Fatalf("settlement commit readback = %+v, %v", current, getErr)
		}
		assertSettlementWriteCounts(t, s, run.ID, 1)
		beforeEvents := countRows(t, s, `SELECT COUNT(*) FROM task_events`)
		beforeOutbox := countRows(t, s, `SELECT COUNT(*) FROM outbox`)
		if _, err := s.SettleRun(context.Background(), req); err != nil {
			t.Fatalf("settlement replay after readback: %v", err)
		}
		if got := countRows(t, s, `SELECT COUNT(*) FROM task_events`); got != beforeEvents {
			t.Fatalf("commit-unknown replay added events: %d", got)
		}
		if got := countRows(t, s, `SELECT COUNT(*) FROM outbox`); got != beforeOutbox {
			t.Fatalf("commit-unknown replay added outbox rows: %d", got)
		}
		if err := s.CheckInvariants(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("open/corrupt_database_globally_fail_closed", func(t *testing.T) {
		s := newF1Store(t, nil)
		for index := range 8 {
			intakeTestTask(t, s, fmt.Sprintf("f1-corrupt-db-%d", index), false)
		}
		cfg := s.cfg
		var corruptPage int
		if err := s.db.QueryRow(`SELECT rootpage FROM sqlite_master WHERE name='tasks'`).Scan(&corruptPage); err != nil || corruptPage < 2 {
			t.Fatalf("locate allocated tasks page: page=%d err=%v", corruptPage, err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(cfg.DatabasePath())
		if err != nil || len(data) < 3*512 {
			t.Fatalf("read database for corruption: bytes=%d err=%v", len(data), err)
		}
		pageSize := int(binary.BigEndian.Uint16(data[16:18]))
		if pageSize == 1 {
			pageSize = 65536
		}
		if pageSize < 512 || len(data) < corruptPage*pageSize {
			t.Fatalf("unexpected database layout: page=%d bytes=%d", pageSize, len(data))
		}
		offset := (corruptPage - 1) * pageSize
		for index := offset; index < offset+16; index++ {
			data[index] ^= 0xff
		}
		if err := os.WriteFile(cfg.DatabasePath(), data, 0o600); err != nil {
			t.Fatal(err)
		}
		assertF1OpenFailsClosed(t, cfg)
	})

	t.Run("open/corrupt_wal_globally_fail_closed", func(t *testing.T) {
		ownerCase := prepareF1WALCrash(t)
		runF1WALCrashOwner(t, ownerCase)
		walPath := ownerCase.Root + string(os.PathSeparator) + "engine.db-wal"
		data, err := os.ReadFile(walPath)
		if err != nil || len(data) < 32 {
			t.Fatalf("real crashed WAL is unavailable: bytes=%d err=%v", len(data), err)
		}
		data[0] ^= 0xff
		data[7] ^= 0xff
		if err := os.WriteFile(walPath, data, 0o600); err != nil {
			t.Fatal(err)
		}
		assertF1OpenFailsClosed(t, Config{Root: ownerCase.Root, WorkerDir: ownerCase.Worker})
	})

	t.Run("open/unavailable_database_globally_fail_closed", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("POSIX directory permissions are required for this real unavailable-DB fault")
		}
		s := newF1Store(t, nil)
		cfg := s.cfg
		intakeTestTask(t, s, "f1-unavailable-db", false)
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(cfg.Root, 0o500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(cfg.Root, 0o700) })
		assertF1OpenFailsClosed(t, cfg)
	})
}

func assertF1OpenFailsClosed(t *testing.T, cfg Config) {
	t.Helper()
	if s, err := OpenStore(cfg); err == nil {
		_ = s.Close()
		t.Fatal("corrupt or unavailable SQLite opened instead of failing closed")
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(cfg, EngineOptions{Executable: executable})
	if err == nil {
		_ = engine.Close()
		t.Fatal("engine acquired a corrupt or unavailable database")
	}
}

const f1WALCrashConfigEnv = "WERKRBY_FINITE_F1_WAL_CRASH_CONFIG"

type f1WALCrashCase struct {
	Root       string
	Worker     string
	BriefPath  string
	MarkerPath string
}

type f1WALCrashMarker struct {
	OwnerPID int   `json:"owner_pid"`
	RunID    int64 `json:"run_id"`
}

func prepareF1WALCrash(t *testing.T) f1WALCrashCase {
	t.Helper()
	base := t.TempDir()
	ownerCase := f1WALCrashCase{Root: filepath.Join(base, "state"), Worker: filepath.Join(base, "worker"), BriefPath: writeTestBrief(t, "f1-corrupt-wal", false), MarkerPath: filepath.Join(base, "crash.json")}
	if err := os.MkdirAll(ownerCase.Worker, 0o700); err != nil {
		t.Fatal(err)
	}
	return ownerCase
}

func TestFiniteDatabaseFaultHelperProcess(t *testing.T) {
	path := os.Getenv(f1WALCrashConfigEnv)
	if path == "" {
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var ownerCase f1WALCrashCase
	if err := json.Unmarshal(data, &ownerCase); err != nil {
		t.Fatal(err)
	}
	s, err := OpenStore(Config{Root: ownerCase.Root, WorkerDir: ownerCase.Worker})
	if err != nil {
		t.Fatal(err)
	}
	task, err := s.IntakeBrief(context.Background(), ownerCase.BriefPath)
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.Dispatch(context.Background(), DispatchRequest{TaskID: task.ID, ExpectedVersion: task.Version, Phase: PhaseImplementation, Seat: "seat-a", SessionKey: "finite:f1", LaunchReason: "initial"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	restore := registerStoreTransactionFault(storeTransactionAfterCommit, func() error {
		marker, _ := json.Marshal(f1WALCrashMarker{OwnerPID: os.Getpid(), RunID: run.ID})
		_ = os.WriteFile(ownerCase.MarkerPath, marker, 0o600)
		os.Exit(86)
		return nil
	})
	defer restore()
	_, err = s.SettleRun(context.Background(), SettlementRequest{RunID: run.ID, ExpectedTaskVersion: task.Version + 1, ExecutionOutcome: RunOK, Result: validTestResult(run, ResultDone)})
	if err != nil {
		t.Fatal(err)
	}
}

func runF1WALCrashOwner(t *testing.T, ownerCase f1WALCrashCase) {
	t.Helper()
	base := filepath.Dir(ownerCase.MarkerPath)
	configPath := filepath.Join(base, "config.json")
	writeEngineJSONFile(t, configPath, ownerCase)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(base, "owner.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run=^TestFiniteDatabaseFaultHelperProcess$")
	command.Env = append(os.Environ(), f1WALCrashConfigEnv+"="+configPath)
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
		t.Fatalf("WAL owner %d did not crash after commit: err=%v output=%s", ownerPID, err, output)
	}
	marker, err := os.ReadFile(ownerCase.MarkerPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("commit crash owner_pid=%d marker=%s wal=%s", ownerPID, marker, walPathForLog(ownerCase.Root))
}

func walPathForLog(root string) string { return filepath.Join(root, "engine.db-wal") }
