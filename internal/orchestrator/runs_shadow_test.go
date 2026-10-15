package orchestrator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/runs"
)

func (f *shadowFixture) runsPath() string { return filepath.Join(f.state, "runtime", "runs.db") }

// managerWith builds a manager on the fixture around a harness the test controls.
func (f *shadowFixture) managerWith(h *fakeHarness) *Manager {
	m := New(f.cfg, h, extensions.Runner{Directory: filepath.Join(f.t.TempDir(), "missing")})
	f.t.Cleanup(m.Wait)
	return m
}

func (f *shadowFixture) runsOf(name string) []runs.Run {
	f.t.Helper()
	store, err := runs.Open(f.runsPath())
	if err != nil {
		f.t.Fatal(err)
	}
	defer store.Close()
	all, err := store.Runs(name)
	if err != nil {
		f.t.Fatal(err)
	}
	return all
}

// Off (the default) writes no run record and creates no store.
func TestRunsShadowOffWritesNothing(t *testing.T) {
	f := newShadowFixture(t, "")
	name := f.newTask("off", nil)
	m := f.manager()
	f.scanTodo(m)
	m.Wait()
	if _, err := os.Stat(f.runsPath()); !os.IsNotExist(err) {
		t.Fatalf("runs_mode off created the runs store: %v", err)
	}
	if !claimed(f, name) {
		t.Fatal("the task was not claimed")
	}
}

// Shadow records one run per dispatch with the attempt the file charged, ends it when the turn
// ends, and derives the same attempt the claim charged.
func TestRunsShadowRecordsTheDispatchAndDerivesTheChargedAttempt(t *testing.T) {
	f := newShadowFixture(t, "")
	f.cfg.Orchestrator.RunsMode = "shadow"
	name := f.newTask("recorded", nil)
	m := f.manager()
	f.scanTodo(m)
	m.Wait()
	all := f.runsOf(name)
	if len(all) != 1 {
		t.Fatalf("runs = %+v, want one", all)
	}
	run := all[0]
	if run.Phase != phaseTaskImplementation || run.Charged != 1 || run.Outcome != runs.OK || run.End.IsZero() || run.SessionKey == "" {
		t.Fatalf("run = %+v", run)
	}
	store, err := runs.Open(f.runsPath())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	derived, err := store.Attempts(name, phaseTaskImplementation)
	if err != nil || derived != run.Charged {
		t.Fatalf("derived attempts %d, %v; the claim charged %d", derived, err, run.Charged)
	}
}

// A refused or failed send ends its run by what happened, and only a failure is an attempt.
func TestRunsShadowClassifiesHowTheTurnEnded(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		outcome runs.Outcome
		counts  int
	}{
		{"failure", errors.New("exit status 1"), runs.Fail, 1},
		{"quota wall", errors.New("429 Too Many Requests"), runs.Wall, 0},
		{"interrupted", context.Canceled, runs.Interrupted, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newShadowFixture(t, "")
			f.cfg.Orchestrator.RunsMode = "shadow"
			name := f.newTask("ends "+tc.name, nil)
			h := newFakeRecipient()
			h.sendErrs = []error{tc.err}
			m := f.managerWith(h)
			f.scanTodo(m)
			m.Wait()
			all := f.runsOf(name)
			if len(all) != 1 || all[0].Outcome != tc.outcome || all[0].End.IsZero() {
				t.Fatalf("runs = %+v, want one %s run", all, tc.outcome)
			}
			store, err := runs.Open(f.runsPath())
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if got, _ := store.Attempts(name, phaseTaskImplementation); got != tc.counts {
				t.Fatalf("derived attempts %d, want %d", got, tc.counts)
			}
		})
	}
}

// asyncHarness returns from Send as soon as the turn has started, like the ACP harness; the turn's
// terminal event arrives later, when the test releases it.
type asyncHarness struct {
	*fakeHarness
	release chan core.Event
}

func (h *asyncHarness) Send(_ context.Context, key, _ string, emit core.Emit) (string, bool, error) {
	h.fakeHarness.mu.Lock()
	h.fakeHarness.calls++
	h.fakeHarness.mu.Unlock()
	go func() { emit(<-h.release) }()
	return "thread-" + key, false, nil
}

// A turn that is still running when Send returns has not ended: the run closes on the terminal
// event, by what that event says.
func TestRunsShadowEndsAnAsyncTurnOnItsTerminalEvent(t *testing.T) {
	for _, tc := range []struct {
		name    string
		event   core.Event
		outcome runs.Outcome
	}{
		{"final", core.Event{Kind: core.EventFinal, Text: "done", Done: true}, runs.OK},
		{"error", core.Event{Kind: core.EventError, Text: "exit status 2", Done: true}, runs.Fail},
		{"quota wall", core.Event{Kind: core.EventError, Text: "429 Too Many Requests", Done: true}, runs.Wall},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newShadowFixture(t, "")
			f.cfg.Orchestrator.RunsMode = "shadow"
			name := f.newTask("async "+tc.name, nil)
			h := &asyncHarness{fakeHarness: newFakeRecipient(), release: make(chan core.Event, 1)}
			m := New(f.cfg, h, extensions.Runner{Directory: filepath.Join(t.TempDir(), "missing")})
			t.Cleanup(m.Wait)
			f.scanTodo(m)
			m.Wait() // the dispatch goroutine has returned; the turn has not ended
			if all := f.runsOf(name); len(all) != 1 || all[0].Outcome != runs.Running {
				t.Fatalf("runs before the terminal event = %+v, want one open run", all)
			}
			h.release <- tc.event
			deadline := time.Now().Add(5 * time.Second)
			for {
				all := f.runsOf(name)
				if len(all) == 1 && all[0].Outcome == tc.outcome && !all[0].End.IsZero() {
					return
				}
				if time.Now().After(deadline) {
					t.Fatalf("runs after the terminal event = %+v, want one %s run", all, tc.outcome)
				}
				time.Sleep(20 * time.Millisecond)
			}
		})
	}
}

// retryHarness refuses the first send as a transient provider refusal (the error event comes with
// the error, as real harnesses emit it) and starts the retry like an ACP seat: Send returns, the
// terminal event comes later.
type retryHarness struct {
	*fakeHarness
	release chan core.Event
	sends   int
}

func (h *retryHarness) Send(_ context.Context, key, _ string, emit core.Emit) (string, bool, error) {
	h.sends++
	if h.sends == 1 {
		err := errors.New("prompt_not_delivered")
		emit(core.Event{Kind: core.EventError, Text: err.Error(), Done: true})
		return "", false, err
	}
	go func() { emit(<-h.release) }()
	return "thread-" + key, false, nil
}

// The refusal an in-place retry recovers from is not how the run ended: the retry's own terminal
// event is.
func TestRunsShadowRetryInPlaceDoesNotEndTheRunOnTheRefusal(t *testing.T) {
	f := newShadowFixture(t, "")
	f.cfg.Orchestrator.RunsMode = "shadow"
	name := f.newTask("retried", nil)
	h := &retryHarness{fakeHarness: newFakeRecipient(), release: make(chan core.Event, 1)}
	m := New(f.cfg, h, extensions.Runner{Directory: filepath.Join(t.TempDir(), "missing")})
	t.Cleanup(m.Wait)
	f.scanTodo(m)
	m.Wait()
	if h.sends != 2 {
		t.Fatalf("sends = %d, want the refusal and one retry", h.sends)
	}
	if all := f.runsOf(name); len(all) != 1 || all[0].Outcome != runs.Running {
		t.Fatalf("runs while the retry runs = %+v, want one open run", all)
	}
	h.release <- core.Event{Kind: core.EventFinal, Text: "done", Done: true}
	deadline := time.Now().Add(5 * time.Second)
	for {
		all := f.runsOf(name)
		if len(all) == 1 && all[0].Outcome == runs.OK {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("runs after the retry's final event = %+v, want one ok run", all)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A turn that emitted its final event and then failed on the way back completed: the first
// terminal event ends the run.
func TestRunsShadowFinalThenSendErrorIsOK(t *testing.T) {
	f := newShadowFixture(t, "")
	f.cfg.Orchestrator.RunsMode = "shadow"
	name := f.newTask("final then error", nil)
	h := newFakeRecipient()
	h.sendErrs = []error{errors.New("write reply: broken pipe")}
	h.sendErrFinal = true
	m := f.managerWith(h)
	f.scanTodo(m)
	m.Wait()
	if all := f.runsOf(name); len(all) != 1 || all[0].Outcome != runs.OK {
		t.Fatalf("runs = %+v, want one ok run", all)
	}
}

// A run the previous owner never saw end is lost when the next owner opens the store, and
// costs no attempt.
func TestRunsShadowClosesTheOpenRunsOfAPreviousOwner(t *testing.T) {
	f := newShadowFixture(t, "")
	f.cfg.Orchestrator.RunsMode = "shadow"
	name := f.newTask("after a crash", nil)
	old, err := runs.Open(f.runsPath())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.Begin(runs.Run{Doc: name, Phase: phaseTaskImplementation, Charged: 1, Start: time.Now().Add(-time.Hour)}); err != nil {
		t.Fatal(err)
	}
	_ = old.Close()
	m := f.manager()
	f.scanTodo(m)
	m.Wait()
	all := f.runsOf(name)
	if len(all) != 2 || all[0].Outcome != runs.Lost || all[1].Outcome != runs.OK {
		t.Fatalf("runs = %+v, want the old run lost and the new one ok", all)
	}
}
