package orchestrator

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/agent0ai/spynel/internal/admission"
	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/roster"
)

// Shadow admission (orchestrator.admission_mode: shadow): at each claim site the manager also asks
// internal/admission what it would decide and logs the verdict beside the old path's outcome. It
// never changes the outcome, writes no file and spends no attempt. Sites:
//   - queue: scanPhaseQueue, the goal-planning start in startExistingClaim, and wakeWaitingDocuments
//     compare admission with rosterHeld (held = wait, not held = run).
//   - dispatch: resolveRosterModel compares admission with the seat Assign gave the claim.
//
// A disagreement is logged once an hour per document and verdict pair; a summary of agreements and
// disagreements per site follows hourly.
const shadowLogEvery = time.Hour

type admissionShadowState struct {
	mu        sync.Mutex
	decider   *admission.Admission
	logged    map[string]time.Time
	agree     map[string]int
	disagree  map[string]int
	summaryAt time.Time
}

type shadowOld struct {
	Verdict admission.Verdict
	Seat    string
}

func (o shadowOld) String() string { return string(o.Verdict) + ":" + o.Seat }

func (m *Manager) admissionShadowOn() bool {
	return m.runtimeSnapshot().Orchestrator.AdmissionModeOrOff() == config.AdmissionModeShadow
}

func admissionKind(phase string) (admission.Kind, bool) {
	switch phase {
	case phaseTaskImplementation:
		return admission.KindTaskImplementation, true
	case phaseTaskReview:
		return admission.KindTaskReview, true
	case phaseGoalPlanning:
		return admission.KindGoalPlanning, true
	case phaseGoalReview:
		return admission.KindGoalReview, true
	}
	return "", false
}

func (m *Manager) shadowDecider() *admission.Admission {
	m.shadow.mu.Lock()
	defer m.shadow.mu.Unlock()
	if m.shadow.decider == nil {
		snapshot := m.admissionSnapshotPath
		if snapshot == "" {
			snapshot = admission.DefaultSnapshotPath()
		}
		m.shadow.decider = admission.New(m.Config.StatePath(), snapshot)
	}
	return m.shadow.decider
}

// shadowQueued records the queue-site comparison for a document rosterHeld just judged.
func (m *Manager) shadowQueued(site string, route workflowRoute, phase, name string, document Document, held bool) {
	if !m.admissionShadowOn() {
		return
	}
	defer m.shadowRecover()
	stateDir := m.Config.StatePath()
	staffing, err := roster.Load(stateDir)
	if err != nil || staffing == nil {
		return
	}
	phase = normalizeLeasePhase(route.Name, phase)
	// Route on the attempt the claim would record, as rosterHeld does.
	frontMatter := make(map[string]any, len(document.FrontMatter)+1)
	scratch := make(map[string]any, len(document.FrontMatter))
	for key, value := range document.FrontMatter {
		frontMatter[key] = value
		scratch[key] = value
	}
	if field := phaseAttemptField(phase); field != "" {
		frontMatter[field], _ = claimAttempt(scratch, field)
	}
	asked, ok := rosterSeat(staffing, stateDir, phase, frontMatter, name, nil)
	if !ok {
		return
	}
	old := shadowOld{Verdict: admission.Run, Seat: staffing.Landing(stateDir, asked, time.Now())}
	if held {
		old.Verdict = admission.Wait
	}
	m.shadowCompare(site, phase, name, asked, frontMatter, old)
}

// shadowDispatched records the dispatch-site comparison: assigned is the seat the old path gave a
// claim that is already running.
func (m *Manager) shadowDispatched(phase, name, asked, assigned string, frontMatter map[string]any) {
	if !m.admissionShadowOn() {
		return
	}
	defer m.shadowRecover()
	m.shadowCompare("dispatch", phase, name, asked, frontMatter, shadowOld{Verdict: admission.Run, Seat: assigned})
}

func (m *Manager) shadowCompare(site, phase, doc, asked string, frontMatter map[string]any, old shadowOld) {
	kind, ok := admissionKind(phase)
	if !ok {
		return
	}
	var constraints []string
	switch v := frontMatter["claude_only"].(type) {
	case bool:
		if v {
			constraints = append(constraints, admission.ConstraintClaudeOnly)
		}
	case string:
		if strings.EqualFold(strings.TrimSpace(v), "true") {
			constraints = append(constraints, admission.ConstraintClaudeOnly)
		}
	}
	decision := m.shadowDecider().Decide(admission.Request{
		Kind: kind, Doc: doc, Asked: asked, Attempt: numberValue(frontMatter[phaseAttemptField(phase)]), Constraints: constraints,
	})
	agree := decision.Verdict == old.Verdict && (decision.Verdict == admission.Wait || decision.Seat == old.Seat)
	now := time.Now()
	var line string
	m.shadow.mu.Lock()
	if m.shadow.agree == nil {
		m.shadow.agree, m.shadow.disagree, m.shadow.logged = map[string]int{}, map[string]int{}, map[string]time.Time{}
	}
	if agree {
		m.shadow.agree[site]++
	} else {
		m.shadow.disagree[site]++
		key := strings.Join([]string{site, doc, phase, old.String(), string(decision.Verdict) + ":" + decision.Seat}, "\x00")
		if now.Sub(m.shadow.logged[key]) >= shadowLogEvery {
			m.shadow.logged[key] = now
			line = fmt.Sprintf("admission-shadow: DISAGREE site=%s doc=%s phase=%s asked=%s old=%s new=%s:%s reason=%q until=%s inputs=%s",
				site, doc, phase, asked, old, decision.Verdict, decision.Seat, decision.Reason, untilField(decision.Until), inputsField(decision.Inputs))
		}
	}
	m.shadow.mu.Unlock()
	if line != "" {
		m.log(line)
	}
	m.shadowSummary(now, false)
}

// shadowSummary logs the per-site counts since the last summary; force emits them now.
func (m *Manager) shadowSummary(now time.Time, force bool) {
	m.shadow.mu.Lock()
	if m.shadow.summaryAt.IsZero() {
		m.shadow.summaryAt = now
	}
	if !force && now.Sub(m.shadow.summaryAt) < shadowLogEvery {
		m.shadow.mu.Unlock()
		return
	}
	sites := map[string]bool{}
	for site := range m.shadow.agree {
		sites[site] = true
	}
	for site := range m.shadow.disagree {
		sites[site] = true
	}
	var lines []string
	for site := range sites {
		lines = append(lines, fmt.Sprintf("admission-shadow: summary site=%s agree=%d disagree=%d since=%s",
			site, m.shadow.agree[site], m.shadow.disagree[site], m.shadow.summaryAt.UTC().Format(time.RFC3339)))
	}
	m.shadow.agree, m.shadow.disagree, m.shadow.summaryAt = map[string]int{}, map[string]int{}, now
	m.shadow.mu.Unlock()
	sort.Strings(lines)
	for _, line := range lines {
		m.log(line)
	}
}

// shadowFlushSummary is the forced summary used by tests.
func (m *Manager) shadowFlushSummary(now time.Time) { m.shadowSummary(now, true) }

// shadowRecover keeps a fault in the comparison from reaching the claim it observes.
func (m *Manager) shadowRecover() {
	if r := recover(); r != nil {
		m.log(fmt.Sprintf("admission-shadow: panic in comparison, ignored: %v", r))
	}
}

func untilField(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.UTC().Format(time.RFC3339)
}

func inputsField(inputs map[string]string) string {
	if len(inputs) == 0 {
		return "-"
	}
	keys := make([]string, 0, len(inputs))
	for key := range inputs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, key := range keys {
		parts[i] = key + "@" + inputs[key]
	}
	return strings.Join(parts, ",")
}
