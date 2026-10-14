package admission

import (
	"testing"
	"time"
)

func TestDecideHeldWindowSurvivesADroppedOrStaleReadingUntilItsReset(t *testing.T) {
	f := newFixture(t, testRoster, map[string]any{"zai": plan(testNow, map[string]any{"weekly": 100})}, nil)
	wantWait(t, f.decide("lone"), "full")
	// the next reading no longer lists the window
	f.setSnapshot(map[string]any{"zai": plan(testNow, map[string]any{"five_hour": 3})}, nil)
	wantWait(t, f.decide("lone"), "full")
	// the collector goes stale: the known-full window still holds until its reset
	f.setSnapshot(map[string]any{"zai": plan(testNow.Add(-3*time.Hour), map[string]any{"five_hour": 3})}, nil)
	wantWait(t, f.decide("lone"), "full")
	// after the reset a stale reading is just unknown
	later := f.a.Decide(Request{Kind: KindTaskImplementation, Asked: "lone", Now: testNow.Add(3 * time.Hour)})
	if later.Verdict != Run || len(later.Unknown) != 1 {
		t.Fatalf("after reset: %s %v %s", later.Verdict, later.Unknown, later.Reason)
	}
}

func TestDecideContinuationIsNeverStopped(t *testing.T) {
	f := newFixture(t, testRoster, map[string]any{"zai": plan(testNow, map[string]any{"weekly": 100})}, nil)
	wantRun(t, f.a.Decide(Request{Kind: KindContinuation, Asked: "lone", Now: testNow}), "lone")
}

func TestDecideClaudeOnlyWaitsWhenTheSeatCannotBeClassified(t *testing.T) {
	f := newFixture(t, testRoster, map[string]any{}, nil)
	wantWait(t, f.decide("nobody", "claude_only"), "claude_only")
	g := newFixture(t, "", map[string]any{}, nil)
	wantWait(t, g.decide("nobody", "claude_only"), "claude_only")
}

func TestDecideConcurrentCallsAreSafe(t *testing.T) {
	f := newFixture(t, testRoster, map[string]any{"zai": plan(testNow, map[string]any{"weekly": 100})}, nil)
	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 20; j++ {
				f.decide("lone")
			}
		}()
	}
	for i := 0; i < 8; i++ {
		<-done
	}
}
