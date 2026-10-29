package orchestrator

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSweepStaleProviderTurnLocksRemovesOnlyOldEmptyLocks(t *testing.T) {
	dir := t.TempDir()
	oldLock := filepath.Join(dir, ".tasks-old.provider-turn.lock")
	freshLock := filepath.Join(dir, ".tasks-fresh.provider-turn.lock")
	fatLock := filepath.Join(dir, ".tasks-fat.provider-turn.lock")
	for _, p := range []string{oldLock, freshLock, fatLock} {
		if err := os.WriteFile(p, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	stale := time.Now().Add(-25 * time.Hour)
	if err := os.Chtimes(oldLock, stale, stale); err != nil {
		t.Fatal(err)
	}
	// A lock that somehow carries content is never swept: only empty leftovers are.
	if err := os.WriteFile(fatLock, []byte("held"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(fatLock, stale, stale); err != nil {
		t.Fatal(err)
	}

	removed := SweepStaleProviderTurnLocks(dir)

	if removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}
	if _, err := os.Lstat(oldLock); !os.IsNotExist(err) {
		t.Fatalf("stale empty lock still present: %v", err)
	}
	if _, err := os.Lstat(freshLock); err != nil {
		t.Fatalf("fresh lock was swept: %v", err)
	}
	if _, err := os.Lstat(fatLock); err != nil {
		t.Fatalf("non-empty lock was swept: %v", err)
	}
}

func TestSweepStaleProviderTurnLocksOnAMissingDirectoryIsQuiet(t *testing.T) {
	if removed := SweepStaleProviderTurnLocks(filepath.Join(t.TempDir(), "absent")); removed != 0 {
		t.Fatalf("removed = %d, want 0", removed)
	}
}
