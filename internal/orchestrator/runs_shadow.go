package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/runs"
)

// Runs shadow (orchestrator.runs_mode: shadow): every dispatch also records the provider turn it
// sends in the runs store (<state>/runtime/runs.db) and ends it by how the turn ended. The task
// files stay the source of truth: nothing here changes a claim, a transition or an attempt, and a
// fault in the store is logged and ignored. Stage B derives attempts from these runs and compares
// them with the attempts the files charge before any switch.
//
// One dispatch is one run, however many times the send is retried in place. The harness does not
// expose the process, so pid and exit stay 0 until a headless seat reports them.
type runsShadowState struct {
	mu     sync.Mutex
	store  *runs.Store
	failed bool // opening the store failed once; it is not retried until the next start
}

// runRecorder is one open run; nil records nothing. A run ends once, by whichever of its terminal
// event and its send error arrives first.
//
// Send returning is not the end of a turn: an ACP seat returns as soon as the turn has started and
// reports the end later as a terminal event, while a headless seat reports it before Send returns.
// So a terminal final event ends the run ok, a send error ends it by its class, and a terminal
// error event that arrives before Send returns waits for the send error (the real error, which
// restartInterrupted can classify) and only ends the run when Send returns without one.
type runRecorder struct {
	m  *Manager
	id int64

	mu           sync.Mutex
	ended        bool
	sendReturned bool
	pendingErr   string // a terminal error event seen while Send was still running
}

func (m *Manager) runsShadowOn() bool {
	return m.runtimeSnapshot().Orchestrator.RunsModeOrOff() == config.RunsModeShadow
}

// runsStore opens the store on first use. Opening closes the runs a previous owner left open as
// lost, once: no run of this owner exists yet.
func (m *Manager) runsStore() *runs.Store {
	m.runsShadow.mu.Lock()
	defer m.runsShadow.mu.Unlock()
	if m.runsShadow.store != nil || m.runsShadow.failed {
		return m.runsShadow.store
	}
	path := filepath.Join(m.Config.StatePath(), "runtime", "runs.db")
	store, err := runs.Open(path)
	if err != nil {
		m.runsShadow.failed = true
		m.log("runs-shadow: store unavailable, runs not recorded: " + err.Error())
		return nil
	}
	if lost, err := store.LostOpen(time.Now()); err != nil {
		m.log("runs-shadow: close runs left open by a previous owner: " + err.Error())
	} else if lost > 0 {
		m.log(fmt.Sprintf("runs-shadow: %d run(s) a previous owner never saw end recorded as lost", lost))
	}
	m.runsShadow.store = store
	return store
}

// runBegin records the start of the turn about to be sent. It returns nil when shadow is off.
func (m *Manager) runBegin(lease Lease, seat string) (rec *runRecorder) {
	if !m.runsShadowOn() {
		return nil
	}
	defer m.runsRecover()
	store := m.runsStore()
	if store == nil {
		return nil
	}
	id, err := store.Begin(runs.Run{
		Doc: filepath.Base(lease.File), Phase: lease.Phase, Seat: seat, SessionKey: lease.SessionKey,
		Charged: lease.ClaimAttempt, Start: time.Now(),
	})
	if err != nil {
		m.log("runs-shadow: record run start: " + err.Error())
		return nil
	}
	return &runRecorder{m: m, id: id}
}

// endLocked records how the turn ended, once. r.mu is held.
func (r *runRecorder) endLocked(outcome runs.Outcome) {
	if r.ended {
		return
	}
	r.ended = true
	defer r.m.runsRecover()
	if err := r.m.runsStore().End(r.id, outcome, 0, time.Now()); err != nil {
		r.m.log("runs-shadow: record run end: " + err.Error())
	}
}

// terminal records a terminal event of the turn.
func (r *runRecorder) terminal(ctx context.Context, lease Lease, event core.Event) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	switch {
	case event.Kind == core.EventFinal:
		r.endLocked(runs.OK)
	case !r.sendReturned:
		r.pendingErr = event.Text
	default:
		r.endLocked(r.m.runOutcome(ctx, lease, errors.New(event.Text)))
	}
}

// sent records that Send returned with err. A send error ends the run; a clean return leaves it
// to the terminal event, unless that event already came with an error.
func (r *runRecorder) sent(ctx context.Context, lease Lease, err error) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sendReturned = true
	switch {
	case err != nil:
		r.endLocked(r.m.runOutcome(ctx, lease, err))
	case r.pendingErr != "":
		r.endLocked(r.m.runOutcome(ctx, lease, errors.New(r.pendingErr)))
	}
}

// runOutcome classifies an error that ended a turn: an interruption (service stop, cancelled
// context, an operator stop recorded for the lease) and a quota or rate wall are not failures of
// the worker; anything else is a failure.
func (m *Manager) runOutcome(ctx context.Context, lease Lease, err error) runs.Outcome {
	switch {
	case m.restartInterrupted(ctx, err) || m.operatorStopPending(lease.ID):
		return runs.Interrupted
	case isQuotaError(err):
		return runs.Wall
	}
	return runs.Fail
}

func (m *Manager) runsRecover() {
	if r := recover(); r != nil {
		m.log(fmt.Sprintf("runs-shadow: panic recording a run, ignored: %v", r))
	}
}
