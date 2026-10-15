package orchestrator

import (
	"strconv"
	"strings"
	"time"

	"github.com/agent0ai/spynel/internal/config"
)

// Work window: during working hours one slot is kept free for work Claude
// cards. Personal claims (everything that is not a work card: goal planning and
// review, task implementation and review) may fill at most max_parallel minus
// reserved_slots slots, and work cards claim ahead of everything else. Running
// work is never stopped; if the window opens with every slot full of personal
// work, the first slot that frees goes to a work card. Outside the window, or
// with the window disabled, none of this applies.

// documentIsWorkCard reports whether a document is a work Claude card. The Helm
// pump writes claude_only: true on those tasks and on nothing else.
func documentIsWorkCard(document Document) bool {
	switch value := document.FrontMatter["claude_only"].(type) {
	case bool:
		return value
	case string:
		return strings.EqualFold(strings.TrimSpace(value), "true")
	}
	return false
}

// workWindowActive reports whether the work window is open right now.
func (m *Manager) workWindowActive(cfg config.Config) bool {
	now := time.Now()
	if m.workClock != nil {
		now = m.workClock()
	}
	return cfg.Orchestrator.WorkWindow.Active(now)
}

// personalSlotCap is how many slots personal claims may fill while the window is
// open. It never drops below one, so a tiny max_parallel cannot starve personal work.
func (m *Manager) personalSlotCap(cfg config.Config) int {
	m.capacityMu.Lock()
	limit := m.capacityLimit
	m.capacityMu.Unlock()
	return max(1, limit-cfg.Orchestrator.WorkWindow.Reserved())
}

// personalInflight counts in-flight claims that are not work cards. A claim
// whose document cannot be read counts as personal, the safe side for a reserve.
func (m *Manager) personalInflight() int {
	m.mu.Lock()
	running := make(map[string]bool, len(m.inflight))
	for key := range m.inflight {
		running[key] = true
	}
	m.mu.Unlock()
	leases, err := m.loadLeases()
	if err != nil {
		return len(running)
	}
	work := 0
	for _, lease := range leases {
		if !running[lease.ID] {
			continue
		}
		document, readErr := ReadDocument(lease.File)
		if readErr == nil && documentIsWorkCard(document) {
			work++
		}
	}
	return len(running) - work
}

// logWorkReserveHold notes that personal work is being held for the reserved
// slot, at most once every five minutes so a ten-second scan does not flood the log.
func (m *Manager) logWorkReserveHold(name string, personal, cap int) {
	now := time.Now()
	m.mu.Lock()
	quiet := now.Sub(m.workReserveLoggedAt) < 5*time.Minute
	if !quiet {
		m.workReserveLoggedAt = now
	}
	m.mu.Unlock()
	if !quiet {
		m.log("work window: holding " + name + " for the reserved slot (personal work " + strconv.Itoa(personal) + "/" + strconv.Itoa(cap) + ")")
	}
}
