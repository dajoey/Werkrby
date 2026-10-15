package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Goal work is the top priority (Joey, 2026-10-03, 2026-10-10). That means it
// claims first, and when it is queued with every slot taken a running
// standalone task is paused so the goal work gets the slot. It does not mean
// standalone work waits while slots are idle: an earlier build held every
// standalone claim for as long as any goal work existed, which left slots empty
// for hours.

// preemptWindow is how long a recorded pause may wait for the dispatch error
// handler to see it; an older record belongs to some other cancellation.
const preemptWindow = operatorStopWindow

// preemptStopTimeout bounds the interrupt of the paused job.
const preemptStopTimeout = 45 * time.Second

// markPreemption records that a lease is being paused for goal work. It reports
// false when a pause is already in progress, so one scan cannot pause a second
// task for the same waiting work.
func (m *Manager) markPreemption(leaseID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dropStalePreemptionsLocked()
	if len(m.preemptions) > 0 {
		return false
	}
	m.preemptions[leaseID] = time.Now()
	return true
}

func (m *Manager) preemptionPending() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dropStalePreemptionsLocked()
	return len(m.preemptions) > 0
}

// dropStalePreemptionsLocked forgets pauses that can no longer complete: the
// record is older than preemptWindow, or the job it names already ended without
// passing through recordError (it finished on its own while being stopped).
// The caller holds m.mu.
func (m *Manager) dropStalePreemptionsLocked() {
	now := time.Now()
	for id, at := range m.preemptions {
		if now.Sub(at) > preemptWindow || !m.inflight[id] {
			delete(m.preemptions, id)
		}
	}
}

func (m *Manager) forgetPreemption(leaseID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.preemptions, leaseID)
}

// preemptionPendingFor reports whether a goal preemption was recorded for this lease, without
// consuming it.
func (m *Manager) preemptionPendingFor(leaseID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	at, ok := m.preemptions[leaseID]
	return ok && time.Since(at) <= preemptWindow
}

// consumePreemption reports whether the lease was paused for goal work within
// preemptWindow, and forgets the record.
func (m *Manager) consumePreemption(leaseID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	at, ok := m.preemptions[leaseID]
	delete(m.preemptions, leaseID)
	return ok && time.Since(at) <= preemptWindow
}

// pauseStandaloneForGoalWork pauses the standalone task implementation that has
// run the shortest, the one that loses the least work. Reviews, recoveries and
// goal-derived work are never paused. The interrupt runs off the scan so a slow
// pane teardown cannot stall claiming.
func (m *Manager) pauseStandaloneForGoalWork(ctx context.Context) {
	if m.preemptionPending() {
		return
	}
	leases, err := m.loadLeases()
	if err != nil {
		return
	}
	var victim *Lease
	for i := range leases {
		lease := leases[i]
		if lease.Route != "tasks" || lease.Phase != phaseTaskImplementation || lease.State != "processing" || !m.isInflight(lease.ID) {
			continue
		}
		// A work Claude card is never the one paused for goal work.
		if lease.WorkCard {
			continue
		}
		document, readErr := ReadDocument(lease.File)
		if readErr != nil || documentIsGoalDerived(document) || documentIsWorkCard(document) {
			continue
		}
		if victim == nil || lease.StartedAt.After(victim.StartedAt) {
			victim = &leases[i]
		}
	}
	if victim == nil || !m.markPreemption(victim.ID) {
		return
	}
	lease := *victim
	m.log(fmt.Sprintf("goal work is waiting for a slot: pausing standalone task %s", lease.File))
	m.jobs.Add(1)
	go func() {
		defer m.jobs.Done()
		stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), preemptStopTimeout)
		defer cancel()
		if err := m.stopForPreemption(stopCtx, lease); err != nil {
			m.forgetPreemption(lease.ID)
			m.log(fmt.Sprintf("could not pause %s for goal work (will retry on a later scan): %v", lease.File, err))
		}
	}()
}

func (m *Manager) stopForPreemption(ctx context.Context, lease Lease) error {
	if m.PreemptJob != nil {
		return m.PreemptJob(ctx, lease.SessionKey)
	}
	cancellationLeaseID := m.MarkControlCancellation(lease.SessionKey)
	stopped, err := m.Harness.Interrupt(ctx, lease.SessionKey)
	if err == nil && !stopped {
		err = errors.New("the provider did not accept the interrupt request")
	}
	if err != nil {
		m.RestoreControlCancellation(cancellationLeaseID)
	}
	return err
}

// recordPreemption lands a task that was paused for goal work: back in todo/
// with its attempt unspent and no timer, so it runs again as soon as a slot is
// free. It handles only task implementation, like recordOperatorStop, and
// reports whether it handled the error.
func (m *Manager) recordPreemption(lease Lease) bool {
	if lease.Phase != phaseTaskImplementation || !m.consumePreemption(lease.ID) {
		return false
	}
	// MarkControlCancellation also recorded an operator stop for this lease;
	// this pause is not one.
	m.consumeOperatorStop(lease.ID)
	route, ok := routeByName(lease.Route)
	if !ok {
		return false
	}
	now := time.Now().UTC()
	base := filepath.Dir(m.Config.Resolve(route.Source))
	todoTarget := filepath.Join(base, "todo", filepath.Base(lease.File))
	note := fmt.Sprintf("paused at %s so goal work could take this slot; returned to todo; attempt not spent", now.Format(time.RFC3339))
	update := func(fm map[string]any) {
		fm["paused_for"] = "goal work"
		fm["paused_at"] = now.Format(time.RFC3339)
		fm[resumeCreditField] = true
	}
	if moveErr := moveDocumentWithUpdate(lease.File, todoTarget, "todo", now, note, update); moveErr != nil {
		m.log("return paused task to todo: " + moveErr.Error())
		return false
	}
	_ = os.Remove(m.leasePath(lease.ID))
	m.finishRuntimeJob(lease.ID)
	m.log(fmt.Sprintf("%s: %s", note, todoTarget))
	return true
}
