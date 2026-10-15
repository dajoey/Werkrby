package runs

import (
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func openTemp(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "runtime", "runs.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, path
}

func begin(t *testing.T, s *Store, doc, phase string, start time.Time) int64 {
	t.Helper()
	id, err := s.Begin(Run{Doc: doc, Phase: phase, Seat: "seat-a", SessionKey: doc + "-" + phase, Charged: 1, PID: 42, Start: start})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// A run is a provider turn that happened: only a finished turn that ended ok, failed or timed out
// is an attempt. A run still open, one an owner restart lost, and one a restart or an operator
// stop interrupted never are.
func TestAttemptsDeriveFromFinishedRunsOnly(t *testing.T) {
	s, _ := openTemp(t)
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	for i, outcome := range []Outcome{OK, Fail, Timeout, Lost, Interrupted, Wall} {
		id := begin(t, s, "task-a.md", "task_implementation", now.Add(time.Duration(i)*time.Minute))
		if err := s.End(id, outcome, 0, now.Add(time.Duration(i)*time.Minute+time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	begin(t, s, "task-a.md", "task_implementation", now.Add(time.Hour)) // still open
	other := begin(t, s, "task-a.md", "task_review", now)
	if err := s.End(other, OK, 0, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := s.End(begin(t, s, "task-b.md", "task_implementation", now), OK, 0, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	got, err := s.Attempts("task-a.md", "task_implementation")
	if err != nil || got != 3 {
		t.Fatalf("attempts for task-a implementation = %d, %v; want 3 (ok, fail, timeout)", got, err)
	}
	if got, _ := s.Attempts("task-a.md", "task_review"); got != 1 {
		t.Fatalf("review attempts = %d, want 1", got)
	}
	if got, _ := s.Attempts("task-missing.md", "task_implementation"); got != 0 {
		t.Fatalf("unknown document attempts = %d, want 0", got)
	}
}

func TestEndRecordsTheOutcomeOnceAndOpenRunsAreReported(t *testing.T) {
	s, _ := openTemp(t)
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	id := begin(t, s, "task-a.md", "task_implementation", now)
	runs, err := s.Runs("task-a.md")
	if err != nil || len(runs) != 1 || runs[0].Outcome != Running || !runs[0].End.IsZero() || runs[0].PID != 42 || runs[0].Charged != 1 {
		t.Fatalf("open run = %+v, %v", runs, err)
	}
	if err := s.End(id, Fail, 3, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := s.End(id, OK, 0, now.Add(2*time.Minute)); err == nil {
		t.Fatal("a finished run's outcome was overwritten")
	}
	runs, _ = s.Runs("task-a.md")
	if runs[0].Outcome != Fail || runs[0].Exit != 3 || !runs[0].End.Equal(now.Add(time.Minute)) {
		t.Fatalf("finished run = %+v", runs[0])
	}
	if err := s.End(id+99, OK, 0, now); err == nil {
		t.Fatal("ending an unknown run succeeded")
	}
	if err := s.End(id, Running, 0, now); err == nil {
		t.Fatal("ending a run with the open outcome succeeded")
	}
}

// The owner that started a run is gone when a new owner opens the store: whatever is still open
// is lost, which costs no attempt.
func TestLostOpenClosesRunsFromAPreviousOwner(t *testing.T) {
	s, path := openTemp(t)
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	begin(t, s, "task-a.md", "task_implementation", now)
	done := begin(t, s, "task-a.md", "task_implementation", now.Add(time.Minute))
	if err := s.End(done, OK, 0, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	again, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	n, err := again.LostOpen(now.Add(time.Hour))
	if err != nil || n != 1 {
		t.Fatalf("LostOpen = %d, %v; want 1", n, err)
	}
	runs, _ := again.Runs("task-a.md")
	if len(runs) != 2 || runs[0].Outcome != Lost || !runs[0].End.Equal(now.Add(time.Hour)) || runs[1].Outcome != OK {
		t.Fatalf("runs after LostOpen = %+v", runs)
	}
	if got, _ := again.Attempts("task-a.md", "task_implementation"); got != 1 {
		t.Fatalf("attempts after LostOpen = %d, want 1", got)
	}
}

// Intents are an append-only inbox other writers can fill while the owner is down.
func TestIntentsAppendAndListInOrder(t *testing.T) {
	s, _ := openTemp(t)
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	first, err := s.AppendIntent(Intent{Kind: "park", Doc: "task-a.md", Payload: `{"wake_at":"2026-10-16T09:00:00Z"}`, At: now})
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.AppendIntent(Intent{Kind: "cancel", Doc: "task-b.md", At: now.Add(time.Second)})
	if err != nil || second <= first {
		t.Fatalf("intent ids %d then %d, %v", first, second, err)
	}
	pending, err := s.PendingIntents()
	if err != nil || len(pending) != 2 || pending[0].Kind != "park" || pending[0].Payload == "" || pending[1].Doc != "task-b.md" {
		t.Fatalf("pending = %+v, %v", pending, err)
	}
	if err := s.ApplyIntent(first, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyIntent(first, now.Add(time.Minute)); err == nil {
		t.Fatal("an intent was applied twice")
	}
	pending, _ = s.PendingIntents()
	if len(pending) != 1 || pending[0].ID != second {
		t.Fatalf("pending after apply = %+v", pending)
	}
}

func TestConcurrentWritersAndPersistenceAcrossOpen(t *testing.T) {
	s, path := openTemp(t)
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, err := s.Begin(Run{Doc: "task-a.md", Phase: "task_implementation", Start: now.Add(time.Duration(i) * time.Second)})
			if err != nil {
				t.Error(err)
				return
			}
			if err := s.End(id, OK, 0, now.Add(time.Hour)); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	other, err := Open(path) // a second handle on the same file, as a reader beside the owner
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if got, err := other.Attempts("task-a.md", "task_implementation"); err != nil || got != 16 {
		t.Fatalf("attempts through a second handle = %d, %v; want 16", got, err)
	}
}
