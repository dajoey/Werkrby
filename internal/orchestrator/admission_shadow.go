package orchestrator

import (
	"fmt"
	"path/filepath"
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
//   - queue, start, wake: scanPhaseQueue, the goal-planning start in startExistingClaim and
//     wakeWaitingDocuments compare admission with rosterHeld (held = wait, not held = run).
//   - dispatch: resolveRosterModel compares admission, asked before Assign counts the session, with
//     the seat Assign gave the claim. Recovery of running work is not compared: a continuation is
//     never stopped by a quota decision.
//   - chat, notification, heartbeat: harness.Herdr's rosterModelForKey tells shadowRoster (installed
//     by Run) the seat the roster maps the role to and the seat Assign chose.
//
// A comparison for one document and phase is made at most once a minute (a pass that reaches the same
// document through two sites counts once). A disagreement is logged once an hour per document and
// verdict pair; a summary of agreements and disagreements per site follows hourly.
const (
	shadowLogEvery    = time.Hour
	shadowSampleEvery = time.Minute
)

type admissionShadowState struct {
	mu        sync.Mutex
	now       func() time.Time // tests; nil means time.Now
	decider   *admission.Admission
	logged    map[string]time.Time
	sampled   map[string]time.Time
	agree     map[string]int
	disagree  map[string]int
	summaryAt time.Time
}

type shadowOld struct {
	Verdict admission.Verdict
	Seat    string
}

func (o shadowOld) String() string { return string(o.Verdict) + ":" + o.Seat }

func (s *admissionShadowState) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

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

// shadowDue reports whether a comparison for this document and phase is due, and marks it taken.
func (m *Manager) shadowDue(key string) bool {
	m.shadow.mu.Lock()
	defer m.shadow.mu.Unlock()
	now := m.shadow.clock()
	if m.shadow.sampled == nil {
		m.shadow.sampled = map[string]time.Time{}
	}
	if last, ok := m.shadow.sampled[key]; ok && now.Sub(last) < shadowSampleEvery {
		return false
	}
	m.shadow.sampled[key] = now
	return true
}

// shadowQueued records the queue-site comparison for a document rosterHeld just judged.
func (m *Manager) shadowQueued(site string, route workflowRoute, phase, name string, document Document, held bool) {
	if !m.admissionShadowOn() {
		return
	}
	defer m.shadowRecover()
	phase = normalizeLeasePhase(route.Name, phase)
	if !m.shadowDue(fmt.Sprintf("%s\x00%s\x00%s\x00%t", route.Name, name, phase, held)) {
		return
	}
	stateDir := m.Config.StatePath()
	staffing, err := roster.Load(stateDir)
	if err != nil || staffing == nil {
		return
	}
	// Route on the attempt the claim would record, as rosterHeld does, on copies so the caller's
	// document is untouched (claimAttempt deletes a key from the map it is given).
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
	old := shadowOld{Verdict: admission.Run, Seat: staffing.Landing(stateDir, asked, m.shadow.clock())}
	if held {
		old.Verdict = admission.Wait
	}
	decision, ok := m.shadowDecide(phase, name, asked, frontMatter)
	if ok {
		m.shadowRecord(site, phase, name, asked, old, decision)
	}
}

// shadowDispatchPending asks admission before Assign counts the session, so the question is the one
// Assign was asked; the returned function records the comparison once the old path has chosen a seat.
// It returns nil when shadow is off or the lease is a recovery of running work.
func (m *Manager) shadowDispatchPending(lease Lease, phase, name string, frontMatter map[string]any) func(assigned string) {
	if !m.admissionShadowOn() || lease.State == "recovering" {
		return nil
	}
	var decision admission.Decision
	var ok bool
	func() {
		defer m.shadowRecover()
		decision, ok = m.shadowDecide(phase, filepath.Base(lease.File), name, frontMatter)
	}()
	if !ok {
		return nil
	}
	return func(assigned string) {
		defer m.shadowRecover()
		m.shadowRecord("dispatch", phase, filepath.Base(lease.File), name, shadowOld{Verdict: admission.Run, Seat: assigned}, decision)
	}
}

// shadowRoster is the chat, notification and heartbeat site (harness.RosterObserver): the harness asks
// before the roster counts the session; the returned function records the comparison once the roster
// has chosen a seat. The old path never holds these turns, so its outcome is run on the assigned seat.
// A conversation is compared once a minute. It returns nil when shadow is off, the role has no
// admission kind or the turn was sampled in the last minute.
func (m *Manager) shadowRoster(role, key, asked string) func(assigned string) {
	if !m.admissionShadowOn() {
		return nil
	}
	var kind admission.Kind
	switch role {
	case "chat":
		kind = admission.KindChat
	case "notification":
		kind = admission.KindNotification
	case "heartbeat":
		kind = admission.KindHeartbeat
	default:
		return nil
	}
	var decision admission.Decision
	var ok bool
	func() {
		defer m.shadowRecover()
		if !m.shadowDue(role + "\x00" + key) {
			return
		}
		decision, ok = m.shadowDecider().Decide(admission.Request{Kind: kind, Doc: key, Asked: asked}), true
	}()
	if !ok {
		return nil
	}
	return func(assigned string) {
		defer m.shadowRecover()
		m.shadowRecord(role, role, key, asked, shadowOld{Verdict: admission.Run, Seat: assigned}, decision)
	}
}

func (m *Manager) shadowDecide(phase, doc, asked string, frontMatter map[string]any) (admission.Decision, bool) {
	kind, ok := admissionKind(phase)
	if !ok {
		return admission.Decision{}, false
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
	return m.shadowDecider().Decide(admission.Request{
		Kind: kind, Doc: doc, Asked: asked, Attempt: numberValue(frontMatter[phaseAttemptField(phase)]), Constraints: constraints,
	}), true
}

func (m *Manager) shadowRecord(site, phase, doc, asked string, old shadowOld, decision admission.Decision) {
	agree := decision.Verdict == old.Verdict && (decision.Verdict == admission.Wait || decision.Seat == old.Seat)
	var line string
	func() {
		m.shadow.mu.Lock()
		defer m.shadow.mu.Unlock()
		now := m.shadow.clock()
		m.shadow.initLocked()
		if agree {
			m.shadow.agree[site]++
			return
		}
		m.shadow.disagree[site]++
		key := strings.Join([]string{site, doc, phase, old.String(), string(decision.Verdict) + ":" + decision.Seat}, "\x00")
		if last, ok := m.shadow.logged[key]; !ok || now.Sub(last) >= shadowLogEvery {
			m.shadow.logged[key] = now
			line = fmt.Sprintf("admission-shadow: DISAGREE site=%s doc=%s phase=%s asked=%s old=%s new=%s:%s reason=%q until=%s inputs=%s",
				site, doc, phase, asked, old, decision.Verdict, decision.Seat, decision.Reason, untilField(decision.Until), inputsField(decision.Inputs))
		}
	}()
	if line != "" {
		m.log(line)
	}
	m.shadowSummary(false)
}

func (s *admissionShadowState) initLocked() {
	if s.agree == nil {
		s.agree = map[string]int{}
	}
	if s.disagree == nil {
		s.disagree = map[string]int{}
	}
	if s.logged == nil {
		s.logged = map[string]time.Time{}
	}
	if s.sampled == nil {
		s.sampled = map[string]time.Time{}
	}
}

// shadowSummary logs the per-site counts since the last summary and drops dedupe entries older than
// the window; force emits the counts now.
func (m *Manager) shadowSummary(force bool) {
	var lines []string
	func() {
		m.shadow.mu.Lock()
		defer m.shadow.mu.Unlock()
		now := m.shadow.clock()
		m.shadow.initLocked()
		if m.shadow.summaryAt.IsZero() {
			m.shadow.summaryAt = now
		}
		if !force && now.Sub(m.shadow.summaryAt) < shadowLogEvery {
			return
		}
		sites := map[string]bool{}
		for site := range m.shadow.agree {
			sites[site] = true
		}
		for site := range m.shadow.disagree {
			sites[site] = true
		}
		for site := range sites {
			lines = append(lines, fmt.Sprintf("admission-shadow: summary site=%s agree=%d disagree=%d since=%s",
				site, m.shadow.agree[site], m.shadow.disagree[site], m.shadow.summaryAt.UTC().Format(time.RFC3339)))
		}
		for key, at := range m.shadow.logged {
			if now.Sub(at) >= shadowLogEvery {
				delete(m.shadow.logged, key)
			}
		}
		for key, at := range m.shadow.sampled {
			if now.Sub(at) >= shadowSampleEvery {
				delete(m.shadow.sampled, key)
			}
		}
		m.shadow.agree, m.shadow.disagree, m.shadow.summaryAt = map[string]int{}, map[string]int{}, now
	}()
	sort.Strings(lines)
	for _, line := range lines {
		m.log(line)
	}
}

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
