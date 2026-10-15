package orchestrator

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/agent0ai/spynel/internal/config"
)

// Ops lane: goal-derived work claims ahead of standalone work, so a goal that files more tasks
// than it settles can hold every slot and leave normal operations (Helm alert cards, fixes,
// maintenance) waiting for hours. The lane keeps slots for them. While at least one ops document
// is waiting and claimable, goal-lane claims (goal planning and review, goal-derived tasks)
// may not take a slot if that would leave the lane short of its reserve. With no ops work
// waiting nothing is held, so no slot sits idle. Running work is never stopped, and a running ops
// task is not paused for goal work while the lane is within its reserve.

const (
	slotClassGoal = "goal"
	slotClassOps  = "ops"
)

// documentSlotClass is the lane a claim counts against: "goal" for goal planning and review and
// for goal-derived tasks, "ops" for any other task, "" for a repair task (which has its own
// reserved capacity).
func documentSlotClass(route workflowRoute, document Document) string {
	switch {
	case documentIsRepair(document):
		return ""
	case route.Name == "goals" || documentIsGoalDerived(document):
		return slotClassGoal
	default:
		return slotClassOps
	}
}

// opsLaneReserve is how many slots the lane keeps for waiting ops work. It never takes the last
// slot, so a tiny max_parallel cannot starve goal work.
func (m *Manager) opsLaneReserve(cfg config.Config) int {
	if !cfg.Orchestrator.OpsLane.Enabled {
		return 0
	}
	m.capacityMu.Lock()
	limit := m.capacityLimit
	m.capacityMu.Unlock()
	return max(0, min(cfg.Orchestrator.OpsLane.Reserved(), limit-1))
}

// leaseSlotClass is the lane of an in-flight lease. A lease from before the class was recorded
// falls back to its document.
func (m *Manager) leaseSlotClass(lease Lease) string {
	if lease.Lane == repairLane {
		return ""
	}
	if lease.SlotClass != "" {
		return lease.SlotClass
	}
	if lease.Route == "goals" {
		return slotClassGoal
	}
	// A document that cannot be read is unknown, not ops: counting it would lower the hold and
	// switch on pause protection for a lease that may be goal work.
	document, err := ReadDocument(lease.File)
	switch {
	case err != nil:
		return ""
	case documentIsGoalDerived(document):
		return slotClassGoal
	}
	return slotClassOps
}

// opsInflight counts in-flight claims in the ops lane.
func (m *Manager) opsInflight() int {
	m.mu.Lock()
	running := make(map[string]bool, len(m.inflight))
	for key := range m.inflight {
		running[key] = true
	}
	m.mu.Unlock()
	leases, err := m.loadLeases()
	if err != nil {
		return 0
	}
	count := 0
	for _, lease := range leases {
		if running[lease.ID] && m.leaseSlotClass(lease) == slotClassOps {
			count++
		}
	}
	return count
}

// refreshOpsWaiting records, once per scan, whether an ops document is waiting for a slot.
func (m *Manager) refreshOpsWaiting(cfg config.Config) {
	if m.opsLaneReserve(cfg) == 0 {
		m.opsWaiting.Store(false)
		return
	}
	m.opsWaiting.Store(m.opsDocumentWaiting(cfg))
}

// opsDocumentWaiting reports whether any standalone task in todo/ or review/ could be claimed
// now. A document that is not yet due, or whose seat is held, is not waiting: holding goal work
// for it would leave slots idle.
func (m *Manager) opsDocumentWaiting(cfg config.Config) bool {
	route, ok := routeByName("tasks")
	if !ok {
		return false
	}
	todo := cfg.Resolve(route.Source)
	base := filepath.Dir(todo)
	now := time.Now()
	for _, queue := range []struct{ dir, phase string }{
		{filepath.Join(base, "review"), phaseTaskReview},
		{todo, phaseTaskImplementation},
	} {
		entries, err := os.ReadDir(queue.dir)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".md") || entry.Name() == "AGENTS.md" {
				continue
			}
			document, err := ReadDocument(filepath.Join(queue.dir, entry.Name()))
			if err != nil || documentSlotClass(route, document) != slotClassOps {
				continue
			}
			if !documentDue(document, now, queue.phase) || m.rosterHeldNow(route, queue.phase, entry.Name(), document) {
				continue
			}
			// The claim loop skips a document a lease or running job still owns (a same-named copy
			// refiled while the original runs); counting it would hold goal work for nothing.
			documentID := documentID(document)
			if documentID == "" {
				documentID = leaseID(route.Name+":"+queue.phase, filepath.Join(queue.dir, entry.Name()))
			}
			if leased, err := m.claimBlockedByLease(route, queue.phase, entry.Name(), leaseID(route.Name+":"+queue.phase, documentID)); err != nil || leased {
				continue
			}
			return true
		}
	}
	return false
}

// opsLaneHolds reports whether a claim in this route and document must wait because ops work is
// waiting and taking the slot would leave the lane short of its reserve.
func (m *Manager) opsLaneHolds(route workflowRoute, document Document) bool {
	if documentSlotClass(route, document) != slotClassGoal || !m.opsWaiting.Load() {
		return false
	}
	reserve := m.opsLaneReserve(m.runtimeSnapshot())
	shortfall := reserve - m.opsInflight()
	if shortfall <= 0 {
		return false
	}
	m.capacityMu.Lock()
	limit := m.capacityLimit
	m.capacityMu.Unlock()
	m.mu.Lock()
	count := len(m.inflight)
	m.mu.Unlock()
	return count+1+shortfall > limit
}

// opsLaneProtects reports whether the lane is within its reserve, so none of its running tasks
// may be paused to make room for goal work.
func (m *Manager) opsLaneProtects() bool {
	reserve := m.opsLaneReserve(m.runtimeSnapshot())
	return reserve > 0 && m.opsInflight() <= reserve
}

// logOpsLaneHold notes that goal work is being held for the ops lane, at most once every five
// minutes so a ten-second scan does not flood the log.
func (m *Manager) logOpsLaneHold(name string) {
	now := time.Now()
	m.mu.Lock()
	quiet := now.Sub(m.opsLaneLoggedAt) < 5*time.Minute
	if !quiet {
		m.opsLaneLoggedAt = now
	}
	m.mu.Unlock()
	if !quiet {
		m.log("ops lane: holding goal work " + name + " so waiting ops tasks get their " +
			strconv.Itoa(m.opsLaneReserve(m.runtimeSnapshot())) + " reserved slots")
	}
}
