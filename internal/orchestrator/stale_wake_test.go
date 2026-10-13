package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A joey_ask park waits for Joey's answer, never for a clock. A wake_at left
// over from an earlier quota or operator-stop park, or a past not_before, used
// to win scheduledWake's first-date-field rule: the next scan returned the task
// to todo/ and Joey's question was lost.

func askParkDocument(fields map[string]any) Document {
	front := map[string]any{"joey_ask": map[string]any{"kind": "choice", "prompt": "Which vendor?"}}
	for key, value := range fields {
		front[key] = value
	}
	return Document{FrontMatter: front}
}

func TestScheduledWakeIgnoresDatesWhileJoeyAskIsSet(t *testing.T) {
	now := time.Now().UTC()
	past := now.Add(-time.Hour).Format(time.RFC3339)
	for _, field := range []string{"wake_at", "not_before", "next_dispatch_at"} {
		due, err := scheduledWake(askParkDocument(map[string]any{field: past}), now)
		if err != nil || due {
			t.Fatalf("past %s with joey_ask set: due=%v err=%v, want not due", field, due, err)
		}
	}
	// A joey_ask that is empty or nil is not a park on Joey.
	for _, ask := range []any{nil, "", map[string]any{}} {
		document := Document{FrontMatter: map[string]any{"joey_ask": ask, "wake_at": past}}
		if due, err := scheduledWake(document, now); err != nil || !due {
			t.Fatalf("past wake_at with empty joey_ask %#v: due=%v err=%v, want due", ask, due, err)
		}
	}
}

func TestWakeClearsTheDatesThatWokeTheTask(t *testing.T) {
	cfg, _, manager := workflowTestManager(t)
	route := workflowRoutes()[0]
	task, err := Create(cfg, "tasks", "wake and drop the stale dates", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	base := filepath.Dir(cfg.Resolve(route.Source))
	past := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	future := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	editFrontMatter(t, task, func(fm map[string]any) {
		fm["waiting_for"] = "quota reset"
		fm["wake_at"] = past
		fm["not_before"] = past
		fm["next_dispatch_at"] = future
	})
	if err := moveDocument(task, filepath.Join(base, "waiting", name), "waiting", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := manager.wakeWaitingDocuments(context.Background()); err != nil {
		t.Fatal(err)
	}
	document, err := ReadDocument(filepath.Join(base, "todo", name))
	if err != nil {
		t.Fatalf("due task did not return to todo/: %v", err)
	}
	for _, field := range []string{"wake_at", "not_before"} {
		if _, present := document.FrontMatter[field]; present {
			t.Fatalf("%s survived the wake: %#v", field, document.FrontMatter)
		}
	}
	if _, present := document.FrontMatter["next_dispatch_at"]; !present {
		t.Fatalf("next_dispatch_at is a dispatch gate, not a park date, and must survive a wake: %#v", document.FrontMatter)
	}
}

func TestPastNotBeforeDoesNotWakeJoeyAskPark(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	route := workflowRoutes()[0]
	task, err := Create(cfg, "tasks", "park on Joey with an old not_before", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	base := filepath.Dir(cfg.Resolve(route.Source))
	parkDuringTurn(t, fake, base, name, func(fm map[string]any) {
		fm["waiting_for"] = "Joey to pick a vendor"
		fm["joey_ask"] = map[string]any{"kind": "choice", "prompt": "Which vendor?"}
		fm["not_before"] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	})
	scanAndWait(t, manager) // the turn parks on Joey
	scanAndWait(t, manager)
	scanAndWait(t, manager)
	if _, err := os.Stat(filepath.Join(base, "waiting", name)); err != nil {
		t.Fatalf("a park on Joey left waiting/ because of a past not_before: %v", err)
	}
}

// The reported path end to end: a clock park (quota or operator stop) wakes,
// the resumed attempt parks on Joey alone, and the question must stay put.
func TestClockWakeThenJoeyAskParkKeepsTheQuestion(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	route := workflowRoutes()[0]
	task, err := Create(cfg, "tasks", "quota park, then a question for Joey", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	base := filepath.Dir(cfg.Resolve(route.Source))
	turn := 0
	fake.beforeEmit = func() {
		turn++
		working := filepath.Join(base, "working", name)
		switch turn {
		case 1: // an earlier park on a clock
			editFrontMatter(t, working, func(fm map[string]any) {
				fm["waiting_for"] = "quota reset"
				fm["wake_at"] = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
			})
		case 2: // the resumed attempt asks Joey and touches no date
			editFrontMatter(t, working, func(fm map[string]any) {
				fm["waiting_for"] = "Joey to pick a vendor"
				fm["joey_ask"] = map[string]any{"kind": "choice", "prompt": "Which vendor?"}
			})
		default:
			return
		}
		if err := moveDocument(working, filepath.Join(base, "waiting", name), "waiting", time.Now().UTC()); err != nil {
			t.Errorf("park: %v", err)
		}
	}
	scanAndWait(t, manager) // turn 1 parks on the clock
	if err := manager.reconcileTransitions(context.Background()); err != nil {
		t.Fatal(err)
	}
	editFrontMatter(t, filepath.Join(base, "waiting", name), func(fm map[string]any) {
		fm["wake_at"] = time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	})
	scanAndWait(t, manager) // wakes, claims, turn 2 parks on Joey
	if turn != 2 {
		t.Fatalf("turns = %d, want 2", turn)
	}
	for i := 0; i < 3; i++ {
		scanAndWait(t, manager)
	}
	parked, err := ReadDocument(filepath.Join(base, "waiting", name))
	if err != nil {
		t.Fatalf("Joey's question left waiting/: %v", err)
	}
	if _, present := parked.FrontMatter["wake_at"]; present {
		t.Fatalf("the clock park's wake_at survived into the Joey park: %#v", parked.FrontMatter)
	}
	if !strings.Contains(parked.Body, "scheduled wake condition became due") {
		t.Fatalf("the first wake was not journaled: %s", parked.Body)
	}
}
