package roster

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeHolds(t *testing.T, dir string, updated time.Time, staff map[string]string) {
	t.Helper()
	entries := map[string]holdEntry{}
	for name, until := range staff {
		entries[name] = holdEntry{Until: until, Reason: "zai-pace"}
	}
	data, _ := json.Marshal(holdsFile{UpdatedAt: updated.UTC().Format(time.RFC3339), Staff: entries})
	if err := os.MkdirAll(filepath.Join(dir, "runtime"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "runtime", holdsFileName), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestHeldUntil(t *testing.T) {
	now := time.Date(2026, 10, 11, 15, 0, 0, 0, time.UTC)
	later := now.Add(2 * time.Hour).Format(time.RFC3339)
	earlier := now.Add(-time.Minute).Format(time.RFC3339)

	dir := t.TempDir()
	if _, _, held := HeldUntil(dir, "implementer", now); held || AnyHeld(dir, now) {
		t.Fatal("a missing holds file held a seat")
	}

	writeHolds(t, dir, now.Add(-time.Minute), map[string]string{"implementer": later, "thinker": earlier})
	until, reason, held := HeldUntil(dir, "implementer", now)
	if !held || until.Format(time.RFC3339) != later || reason != "zai-pace" {
		t.Fatalf("fresh hold: held=%v until=%v reason=%q", held, until, reason)
	}
	if _, _, held := HeldUntil(dir, "thinker", now); held {
		t.Fatal("a hold whose until has passed still held")
	}
	if _, _, held := HeldUntil(dir, "ops", now); held {
		t.Fatal("an unlisted seat was held")
	}
	if !AnyHeld(dir, now) {
		t.Fatal("AnyHeld missed a live hold")
	}

	writeHolds(t, dir, now.Add(-31*time.Minute), map[string]string{"implementer": later})
	if _, _, held := HeldUntil(dir, "implementer", now); held || AnyHeld(dir, now) {
		t.Fatal("a stale holds file (gate stopped writing) still held a seat")
	}

	if err := os.WriteFile(filepath.Join(dir, "runtime", holdsFileName), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, held := HeldUntil(dir, "implementer", now); held {
		t.Fatal("an unreadable holds file held a seat")
	}
}

func TestLandingTrailNamesWhySeatsWerePassed(t *testing.T) {
	dir := write(t, `
staff:
  a: {runner: pi, daily_cap: 2, fallback: b}
  b: {runner: pi, fallback: c}
  c: {runner: agy}
roles:
  task_implementation: a
`)
	r, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	today := now.Local().Format("2006-01-02")
	if err := os.MkdirAll(filepath.Join(dir, "runtime"), 0o700); err != nil {
		t.Fatal(err)
	}
	usageData, _ := json.Marshal(usage{Date: today, Counts: map[string]int{"a": 2}, Seen: map[string]string{}})
	if err := os.WriteFile(filepath.Join(dir, "runtime", usageFileName), usageData, 0o600); err != nil {
		t.Fatal(err)
	}
	decisionData, _ := json.Marshal(decisions{Date: today, Staff: map[string]string{"b": DecisionStop}})
	if err := os.WriteFile(filepath.Join(dir, "runtime", decisionsFileName), decisionData, 0o600); err != nil {
		t.Fatal(err)
	}
	landing, passed := r.LandingTrail(dir, "a", now)
	if landing != "c" || landing != r.Landing(dir, "a", now) {
		t.Fatalf("landing %q, want c (same as Landing)", landing)
	}
	if len(passed) != 2 || passed[0].Seat != "a" || passed[0].Reason != "daily cap 2 reached" || passed[1].Seat != "b" || passed[1].Reason != "stopped for today" {
		t.Fatalf("passed hops %+v", passed)
	}
	if landing, passed := r.LandingTrail(dir, "c", now); landing != "c" || len(passed) != 0 {
		t.Fatalf("a seat that runs passed nothing: %q %+v", landing, passed)
	}
}

func TestLandingFollowsStopsWithoutCounting(t *testing.T) {
	dir := write(t, `
staff:
  implementer: {runner: pi, fallback: implementer-bal}
  implementer-bal: {runner: pi, fallback: gemini}
  gemini: {runner: agy}
roles:
  task_implementation: implementer
`)
	r, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if got := r.Landing(dir, "implementer", now); got != "implementer" {
		t.Fatalf("no decisions: landing %q", got)
	}
	today := now.Local().Format("2006-01-02")
	data, _ := json.Marshal(decisions{Date: today, Staff: map[string]string{"implementer-bal": DecisionStop}})
	if err := os.MkdirAll(filepath.Join(dir, "runtime"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "runtime", decisionsFileName), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if got := r.Landing(dir, "implementer-bal", now); got != "gemini" {
		t.Fatalf("stopped seat: landing %q, want gemini", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "runtime", usageFileName)); !os.IsNotExist(err) {
		t.Fatal("Landing wrote roster usage; it must not count a session")
	}
}
