package runs

import (
	"testing"
	"time"
)

// The attempt a claim should charge follows from how the previous run of that document and phase
// ended: a run that counted makes the next claim the next attempt, one that did not leaves the
// attempt where it was. A claim that charged otherwise is a refund (the previous run counted but the
// files did not advance, the shape of every park, format-only and high-risk credit) or a
// disagreement.
func TestCheckComparesTheChargedAttemptWithTheOneTheRunsImply(t *testing.T) {
	for _, tc := range []struct {
		name        string
		prev        *Run
		charged     int
		wantDerived int
		want        Verdict
	}{
		{"no history", nil, 1, 0, Unseeded},
		{"previous run still open", &Run{Charged: 2, Outcome: Running}, 2, 0, PreviousOpen},
		{"ok then the next attempt", &Run{Charged: 1, Outcome: OK}, 2, 2, Match},
		{"fail then the next attempt", &Run{Charged: 2, Outcome: Fail}, 3, 3, Match},
		{"timeout then the next attempt", &Run{Charged: 1, Outcome: Timeout}, 2, 2, Match},
		{"ok but the files did not advance", &Run{Charged: 2, Outcome: OK}, 2, 3, Refund},
		{"ok but the files skipped one", &Run{Charged: 1, Outcome: OK}, 3, 2, Disagree},
		{"lost run costs nothing", &Run{Charged: 2, Outcome: Lost}, 2, 2, Match},
		{"interrupted run costs nothing", &Run{Charged: 2, Outcome: Interrupted}, 2, 2, Match},
		{"wall costs nothing", &Run{Charged: 1, Outcome: Wall}, 1, 1, Match},
		{"wall but the files charged it", &Run{Charged: 1, Outcome: Wall}, 2, 1, Disagree},
		{"interrupted but the files charged it", &Run{Charged: 2, Outcome: Interrupted}, 3, 2, Disagree},
		{"files went backwards", &Run{Charged: 3, Outcome: Fail}, 2, 4, Disagree},
	} {
		t.Run(tc.name, func(t *testing.T) {
			derived, verdict := Check(tc.prev, tc.charged)
			if verdict != tc.want || derived != tc.wantDerived {
				t.Fatalf("Check = derived %d, %s; want derived %d, %s", derived, verdict, tc.wantDerived, tc.want)
			}
		})
	}
}

// LastRun is the latest run of one document and phase, found before a new run begins.
func TestLastRunIsTheLatestRunOfTheDocumentAndPhase(t *testing.T) {
	s, _ := openTemp(t)
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	if _, ok, err := s.LastRun("task-a.md", "task_implementation"); err != nil || ok {
		t.Fatalf("LastRun on an empty store = %v, %v; want none", ok, err)
	}
	first := begin(t, s, "task-a.md", "task_implementation", now)
	if err := s.End(first, Fail, 0, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	second := begin(t, s, "task-a.md", "task_implementation", now.Add(time.Minute))
	if err := s.End(second, OK, 0, now.Add(time.Minute+time.Second)); err != nil {
		t.Fatal(err)
	}
	other := begin(t, s, "task-a.md", "task_review", now.Add(time.Hour)) // another phase
	if err := s.End(other, Fail, 0, now.Add(time.Hour+time.Second)); err != nil {
		t.Fatal(err)
	}
	begin(t, s, "task-b.md", "task_implementation", now.Add(2*time.Hour)) // another document
	run, ok, err := s.LastRun("task-a.md", "task_implementation")
	if err != nil || !ok || run.ID != second || run.Outcome != OK {
		t.Fatalf("LastRun = %+v, %v, %v; want the second implementation run", run, ok, err)
	}
}
