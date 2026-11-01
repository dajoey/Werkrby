package orchestrator

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/agent0ai/spynel/internal/roster"
)

// A work Claude card (claude_only, which the Helm pump writes on work Claude threads) carries work
// data that may reach a Claude model and nothing else. It runs on a Claude seat (roster staff whose
// runner is claude) or not at all: no role, staff pin, escalation, fallback or legacy model may move
// it onto another runner. With no Claude seat to land on it waits unclaimed in its queue folder,
// and a claimed one is returned there unsent. Legacy routing (no roster) names no Claude seat, since
// Werkrby launches only Hermes there, so on that path a work card always waits.

func isClaudeStaff(staffing *roster.Roster, name string) bool {
	if staffing == nil || name == "" {
		return false
	}
	staff, ok := staffing.Staff[name]
	return ok && strings.EqualFold(strings.TrimSpace(staff.Runner), "claude")
}

func frontMatterIsWorkCard(frontMatter map[string]any) bool {
	return documentIsWorkCard(Document{FrontMatter: frontMatter})
}

// workCardSeat is the Claude seat a work Claude card asks for: its staff pin when that names a
// Claude seat, else the roster's first Claude seat by name, else "" (the roster has none).
func workCardSeat(staffing *roster.Roster, frontMatter map[string]any, fileName string, logf func(string)) string {
	if pinned := honoredStaffPin(staffing, frontMatter, fileName, logf); isClaudeStaff(staffing, pinned) {
		return pinned
	}
	if staffing == nil {
		return ""
	}
	names := make([]string, 0, len(staffing.Staff))
	for name := range staffing.Staff {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if isClaudeStaff(staffing, name) {
			return name
		}
	}
	return ""
}

// workCardSeatAvailable reports whether a work Claude card has a Claude seat to run on now: the
// seat workCardSeat asks for, after the fallback walk past caps and stops (Landing), is a Claude
// seat.
func (m *Manager) workCardSeatAvailable(document Document, fileName string) bool {
	stateDir := m.Config.StatePath()
	staffing, err := roster.Load(stateDir)
	if err != nil || staffing == nil {
		return false
	}
	seat := workCardSeat(staffing, document.FrontMatter, fileName, nil)
	return seat != "" && isClaudeStaff(staffing, staffing.Landing(stateDir, seat, time.Now()))
}

// workCardHeld reports whether a queued document is a work Claude card with no Claude seat to run
// on; it waits unclaimed, logged at most once an hour per document.
func (m *Manager) workCardHeld(name string, document Document) bool {
	if !documentIsWorkCard(document) || m.workCardSeatAvailable(document, name) {
		return false
	}
	now := time.Now()
	m.holdLogMu.Lock()
	if m.workCardLogged == nil {
		m.workCardLogged = map[string]time.Time{}
	}
	due := now.Sub(m.workCardLogged[name]) >= time.Hour
	if due {
		m.workCardLogged[name] = now
	}
	m.holdLogMu.Unlock()
	if due {
		m.log("claude_only: " + name + " waits unclaimed: no Claude seat in the roster can take it, and a work Claude card never runs on another model")
	}
	return true
}

// leaseIsWorkCard reports whether a claimed document is a work Claude card, by its lease or its
// document (the conservative side for the fence).
func leaseIsWorkCard(lease Lease) bool {
	if lease.WorkCard {
		return true
	}
	document, err := ReadDocument(lease.File)
	return err == nil && documentIsWorkCard(document)
}

// claudeTarget reports whether a harness model string starts a Claude runner.
func (m *Manager) claudeTarget(model string) bool {
	if strings.TrimSpace(model) == "" {
		return false
	}
	kind, _, err := roster.Expand(m.Config.StatePath(), model)
	return err == nil && strings.EqualFold(strings.TrimSpace(kind), "claude")
}

// releaseUnseatedWorkCard runs before a dispatch: a claimed work Claude card with no Claude seat to
// run on is returned to its queue folder unsent, and the dispatch must not go ahead.
func (m *Manager) releaseUnseatedWorkCard(route workflowRoute, lease Lease) bool {
	if !leaseIsWorkCard(lease) {
		return false
	}
	if document, err := ReadDocument(lease.File); err == nil && m.workCardSeatAvailable(document, filepath.Base(lease.File)) {
		return false
	}
	m.releaseWorkCard(route, lease)
	return true
}

// releaseWorkCard returns a claimed work Claude card to the folder its phase claims from, with a
// resume credit so its next claim spends no attempt, and drops the claim's lease.
func (m *Manager) releaseWorkCard(route workflowRoute, lease Lease) {
	queue := "todo"
	switch normalizeLeasePhase(route.Name, lease.Phase) {
	case phaseTaskReview, phaseGoalReview:
		queue = "review"
	case phaseGoalPlanning:
		queue = "proposed"
	}
	base := filepath.Dir(m.Config.Resolve(route.Source))
	target := filepath.Join(base, queue, filepath.Base(lease.File))
	note := "Werkrby returned this work Claude card (claude_only) unsent: no Claude seat can take it, and it never runs on another model; attempt not spent."
	update := func(frontMatter map[string]any) { frontMatter[resumeCreditField] = true }
	if err := moveDocumentWithUpdate(lease.File, target, queue, time.Now().UTC(), note, update); err != nil {
		m.log("claude_only: return " + lease.File + " unsent: " + err.Error())
		return
	}
	_ = os.Remove(m.leasePath(lease.ID))
	m.finishRuntimeJob(lease.ID)
	m.log("claude_only: returned " + filepath.Base(lease.File) + " to " + queue + "/ unsent: no Claude seat can take it")
}
