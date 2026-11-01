package finite

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The WAL validator follows SQLite's own recovery contract: a bad WAL header
// is refused, while a frame from before a log restart (different salt), a
// trailing partial frame, or a checksum mismatch is the normal end of the log.
// Each of those shapes is produced by an ordinary checkpoint or a process kill
// and must never brick the store.

type walShape struct {
	pageSize  int
	frames    int
	sameSalt  int
	staleSalt int
	partial   int
}

func inspectWALShape(t *testing.T, data []byte) walShape {
	t.Helper()
	if len(data) < 32 {
		t.Fatalf("WAL too short for a header: %d bytes", len(data))
	}
	shape := walShape{pageSize: int(binary.BigEndian.Uint32(data[8:12]))}
	frameSize := 24 + shape.pageSize
	salt := data[16:24]
	offset := 32
	for ; offset+frameSize <= len(data); offset += frameSize {
		shape.frames++
		if bytes.Equal(data[offset+8:offset+16], salt) {
			shape.sameSalt++
		} else {
			shape.staleSalt++
		}
	}
	shape.partial = len(data) - offset
	return shape
}

// snapshotCrashState copies the live database and WAL while the owner still
// holds them open, which is exactly the on-disk state a killed owner leaves.
func snapshotCrashState(t *testing.T, cfg Config, dir string) Config {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"", "-wal"} {
		data, err := os.ReadFile(cfg.DatabasePath() + suffix)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "engine.db"+suffix), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return Config{Root: dir, WorkerDir: filepath.Join(filepath.Dir(dir), filepath.Base(dir)+"-worker")}
}

func TestFiniteWALRestartedLogWithStaleFramesOpens(t *testing.T) {
	base := t.TempDir()
	cfg := Config{Root: filepath.Join(base, "state"), WorkerDir: filepath.Join(base, "worker")}
	s, err := OpenStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	big := strings.Repeat("x", 3000)
	// Enough committed page writes to pass the 1000-page autocheckpoint, so the
	// next writer restarts the log under a new salt without truncating it.
	for batch := 0; batch < 15; batch++ {
		if err := s.withImmediateTx(ctx, func(tx txRunner) error {
			for i := batch * 100; i < (batch+1)*100; i++ {
				if _, err := tx.ExecContext(ctx, `INSERT INTO tasks(id,title,state,risk,review_required,brief_path,brief_sha256,projection_path,version,created_at,updated_at)
				VALUES(?,?,'todo','low',0,'b',?,'p',0,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`, fmt.Sprintf("t%04d", i), big, strings.Repeat("a", 64)); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.withImmediateTx(ctx, func(tx txRunner) error {
		_, err := tx.ExecContext(ctx, `UPDATE tasks SET title='after-restart' WHERE id='t0000'`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	crash := snapshotCrashState(t, cfg, filepath.Join(base, "crash"))
	wal, err := os.ReadFile(crash.DatabasePath() + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	shape := inspectWALShape(t, wal)
	if shape.sameSalt == 0 || shape.staleSalt == 0 {
		t.Fatalf("setup did not produce a restarted WAL with stale frames: %+v", shape)
	}
	if err := validateFiniteWAL(crash.DatabasePath() + "-wal"); err != nil {
		t.Fatalf("restarted WAL with stale old-salt frames rejected: %v (shape %+v)", err, shape)
	}
	reopened, err := OpenStore(crash)
	if err != nil {
		t.Fatalf("crash-state store with a restarted WAL did not open: %v", err)
	}
	defer reopened.Close()
	var rows int
	var title string
	if err := reopened.db.QueryRow(`SELECT COUNT(*) FROM tasks`).Scan(&rows); err != nil || rows != 1500 {
		t.Fatalf("reopened rows = %d, %v; want 1500", rows, err)
	}
	if err := reopened.db.QueryRow(`SELECT title FROM tasks WHERE id='t0000'`).Scan(&title); err != nil || title != "after-restart" {
		t.Fatalf("committed post-restart write lost: title=%q err=%v", title, err)
	}
}

func TestFiniteWALRealDispatchSettleCyclesStayOpenable(t *testing.T) {
	base := t.TempDir()
	cfg := Config{Root: filepath.Join(base, "state"), WorkerDir: filepath.Join(base, "worker")}
	s, err := OpenStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	brief, err := os.ReadFile(writeTestBrief(t, "wal-cycles", false))
	if err != nil {
		t.Fatal(err)
	}
	// A generous run budget keeps the task schedulable for every cycle.
	brief = bytes.Replace(brief, []byte("review_required: false\n"), []byte("review_required: false\nmax_runs: 1000\n"), 1)
	briefPath := filepath.Join(base, "wal-cycles.md")
	if err := os.WriteFile(briefPath, brief, 0o600); err != nil {
		t.Fatal(err)
	}
	task, err := s.IntakeBrief(ctx, briefPath)
	if err != nil {
		t.Fatal(err)
	}
	restarted := false
	for cycle := 1; cycle <= 400 && !restarted; cycle++ {
		current, err := s.GetTask(ctx, task.ID)
		if err != nil {
			t.Fatal(err)
		}
		run, err := s.Dispatch(ctx, DispatchRequest{TaskID: task.ID, ExpectedVersion: current.Version, Phase: PhaseImplementation,
			Seat: "seat-a", SessionKey: "finite:wal", LaunchReason: "retry", PromptHash: "h"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.SettleRun(ctx, SettlementRequest{RunID: run.ID, ExpectedTaskVersion: current.Version + 1,
			ExecutionOutcome: RunOK, Result: validTestResult(run, ResultTodo)}); err != nil {
			t.Fatal(err)
		}
		wal, err := os.ReadFile(cfg.DatabasePath() + "-wal")
		if err != nil {
			t.Fatal(err)
		}
		shape := inspectWALShape(t, wal)
		copyPath := filepath.Join(base, "copy-wal")
		if err := os.WriteFile(copyPath, wal, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := validateFiniteWAL(copyPath); err != nil {
			t.Fatalf("cycle %d: crash-state WAL rejected: %v (shape %+v)", cycle, err, shape)
		}
		if shape.staleSalt == 0 {
			continue
		}
		restarted = true
		crash := snapshotCrashState(t, cfg, filepath.Join(base, "crash"))
		reopened, err := OpenStore(crash)
		if err != nil {
			t.Fatalf("cycle %d: crash-state store did not open: %v (shape %+v)", cycle, err, shape)
		}
		runs, err := reopened.ListRuns(ctx, task.ID)
		_ = reopened.Close()
		if err != nil || len(runs) != cycle || runs[len(runs)-1].Outcome != RunOK {
			t.Fatalf("cycle %d: reopened runs = %d (%v), want every settled run", cycle, len(runs), err)
		}
	}
	if !restarted {
		t.Fatal("400 dispatch/settle cycles never restarted the WAL; the stale-frame case was not exercised")
	}
}

// liveWALCrashCopy returns a crash-state copy whose WAL holds only frames of
// the current salt, and the number of those frames.
func liveWALCrashCopy(t *testing.T) (Config, []byte) {
	t.Helper()
	base := t.TempDir()
	cfg := Config{Root: filepath.Join(base, "state"), WorkerDir: filepath.Join(base, "worker")}
	s, err := OpenStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 3; i++ {
		intakeTestTask(t, s, fmt.Sprintf("wal-tail-%d", i), false)
	}
	crash := snapshotCrashState(t, cfg, filepath.Join(base, "crash"))
	wal, err := os.ReadFile(crash.DatabasePath() + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	shape := inspectWALShape(t, wal)
	if shape.frames < 2 || shape.staleSalt != 0 || shape.partial != 0 {
		t.Fatalf("unexpected fresh WAL shape %+v", shape)
	}
	return crash, wal
}

func TestFiniteWALNormalEndOfLogShapesOpen(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(wal []byte, pageSize int) []byte
		tasks  int
		frames int // valid frames relative to the unmutated log
	}{
		{"trailing_partial_frame", func(wal []byte, pageSize int) []byte {
			// A kill during a frame append leaves part of a frame at the end.
			return append(wal, bytes.Repeat([]byte{0xa5}, 24+pageSize/2)...)
		}, 3, 0},
		{"last_frame_checksum_mismatch", func(wal []byte, pageSize int) []byte {
			// A torn last frame fails its checksum; SQLite drops that
			// uncommitted tail, here the last intake's commit frame.
			wal[len(wal)-1] ^= 0xff
			return wal
		}, 2, -1},
		{"stale_salt_frame_after_valid_frames", func(wal []byte, pageSize int) []byte {
			// A frame left from before a log restart carries the old salt.
			frameSize := 24 + pageSize
			stale := append([]byte(nil), wal[len(wal)-frameSize:]...)
			stale[8] ^= 0xff
			return append(wal, stale...)
		}, 3, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			crash, wal := liveWALCrashCopy(t)
			shape := inspectWALShape(t, wal)
			mutated := tc.mutate(wal, shape.pageSize)
			if err := os.WriteFile(crash.DatabasePath()+"-wal", mutated, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := validateFiniteWAL(crash.DatabasePath() + "-wal"); err != nil {
				t.Fatalf("normal end-of-log WAL rejected: %v", err)
			}
			if frames, err := finiteWALValidFrames(mutated); err != nil || frames != shape.frames+tc.frames {
				t.Fatalf("valid frames = %d, %v; want %d", frames, err, shape.frames+tc.frames)
			}
			reopened, err := OpenStore(crash)
			if err != nil {
				t.Fatalf("crash-state store did not open: %v", err)
			}
			defer reopened.Close()
			if got := countRows(t, reopened, `SELECT COUNT(*) FROM tasks`); got != tc.tasks {
				t.Fatalf("recovered tasks = %d, want %d", got, tc.tasks)
			}
		})
	}
}

func TestFiniteWALBadHeaderFailsClosed(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(wal []byte)
	}{
		{"magic", func(wal []byte) { wal[0] ^= 0xff }},
		{"version", func(wal []byte) { wal[7] ^= 0xff }},
		{"page_size", func(wal []byte) { binary.BigEndian.PutUint32(wal[8:12], 1000) }},
		{"header_checksum", func(wal []byte) { wal[24] ^= 0xff }},
		{"header_field_under_checksum", func(wal []byte) { wal[12] ^= 0x01 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			crash, wal := liveWALCrashCopy(t)
			tc.mutate(wal)
			if err := os.WriteFile(crash.DatabasePath()+"-wal", wal, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := validateFiniteWAL(crash.DatabasePath() + "-wal"); err == nil {
				t.Fatal("WAL with a bad header was accepted")
			}
			assertF1OpenFailsClosed(t, crash)
		})
	}
	t.Run("truncated_header", func(t *testing.T) {
		crash, wal := liveWALCrashCopy(t)
		if err := os.WriteFile(crash.DatabasePath()+"-wal", wal[:20], 0o600); err != nil {
			t.Fatal(err)
		}
		assertF1OpenFailsClosed(t, crash)
	})
}
