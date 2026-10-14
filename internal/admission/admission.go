// Package admission answers one question: may this work start now, and on which seat? Q0 (shadow)
// reads what the old deciders read (roster.yaml, runtime/roster-decisions.json, runtime/roster-holds.json,
// the usage collector's snapshot) and decides without writing anything; the orchestrator logs the verdict
// beside its own outcome. Design: notebook Projects/Werkrby Structural Program/Quota Admission Design.
package admission

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/agent0ai/spynel/internal/roster"
)

type Kind string

const (
	KindTaskImplementation Kind = "task_implementation"
	KindTaskReview         Kind = "task_review"
	KindGoalPlanning       Kind = "goal_planning"
	KindGoalReview         Kind = "goal_review"
	KindChat               Kind = "chat"
	KindNotification       Kind = "notification"
	KindHeartbeat          Kind = "heartbeat"
	KindContinuation       Kind = "continuation"
)

type Verdict string

const (
	Run  Verdict = "run"
	Wait Verdict = "wait"
)

const maxHops = 8

// ConstraintClaudeOnly is a request constraint: only a Claude plan may spend it.
const ConstraintClaudeOnly = "claude_only"

// Request is one claim: Asked is the seat the existing routing chose (routing stays where it is).
type Request struct {
	Kind        Kind
	Doc         string // "" for turns with no document
	Asked       string
	Attempt     int
	Constraints []string
	Now         time.Time // zero means now
}

// Hop is a seat the decision passed over, and why.
type Hop struct {
	Seat string `json:"seat"`
	Plan string `json:"plan,omitempty"`
	Note string `json:"note"`
}

type Decision struct {
	Verdict Verdict
	Seat    string            // the seat that runs, or the seat the walk stopped at
	Plan    string            // usage plan of Seat; empty when unmetered
	Until   time.Time         // earliest time a Wait can change (a reset, a hold's end); zero when not known
	Reason  string            // one line for logs and explain
	Trail   []Hop             // seats passed over, in order
	Unknown []string          // plans whose reading was missing, failed or stale (Q0 treats them as open)
	Inputs  map[string]string // plan -> as_of of the reading used
}

func (d Decision) MarshalJSON() ([]byte, error) {
	out := struct {
		Verdict Verdict           `json:"verdict"`
		Seat    string            `json:"seat"`
		Plan    string            `json:"plan,omitempty"`
		Until   string            `json:"until,omitempty"`
		Reason  string            `json:"reason"`
		Trail   []Hop             `json:"trail,omitempty"`
		Unknown []string          `json:"unknown_readings,omitempty"`
		Inputs  map[string]string `json:"inputs,omitempty"`
	}{d.Verdict, d.Seat, d.Plan, "", d.Reason, d.Trail, d.Unknown, d.Inputs}
	if !d.Until.IsZero() {
		out.Until = d.Until.UTC().Format(time.RFC3339)
	}
	return json.Marshal(out)
}

// Admission decides from files; it keeps only the hysteresis of windows that read full.
type Admission struct {
	StateDir     string
	SnapshotPath string

	mu   sync.Mutex
	held map[string]time.Time
}

func New(stateDir, snapshotPath string) *Admission {
	return &Admission{StateDir: stateDir, SnapshotPath: snapshotPath, held: map[string]time.Time{}}
}

// Decide never writes: it does not count a session, claim a document or touch a file.
func (a *Admission) Decide(req Request) Decision {
	now := req.Now
	if now.IsZero() {
		now = time.Now()
	}
	d := Decision{Seat: req.Asked, Inputs: map[string]string{}}
	if req.Kind == KindContinuation {
		return d.run("continuation of running work is never stopped by a quota decision")
	}
	claudeOnly := hasConstraint(req.Constraints, ConstraintClaudeOnly)
	r, err := roster.Load(a.StateDir)
	var unclassified string
	switch {
	case err != nil:
		unclassified = "roster unreadable, legacy routing: " + err.Error()
	case r == nil:
		unclassified = "no roster: legacy routing"
	case !r.Has(req.Asked):
		unclassified = fmt.Sprintf("seat %q is not in the roster", req.Asked)
	}
	if unclassified != "" {
		if claudeOnly {
			return d.wait("claude_only: " + unclassified)
		}
		return d.run(unclassified)
	}

	// Load the readings under the lock so two calls cannot apply an older file to the held windows
	// after a newer one.
	a.mu.Lock()
	defer a.mu.Unlock()
	readings, _ := loadReadings(a.SnapshotPath) // an unreadable snapshot leaves every plan unknown

	seat, passed := r.LandingTrail(a.StateDir, req.Asked, now)
	for _, p := range passed {
		d.Trail = append(d.Trail, Hop{Seat: p.Seat, Plan: PlanOf(r.Staff[p.Seat]), Note: p.Reason})
	}
	var earliest time.Time
	fromFull := false
	for hop := 0; hop < maxHops; hop++ {
		staff := r.Staff[seat]
		plan := PlanOf(staff)
		d.Seat, d.Plan = seat, plan
		if until, why, held := roster.HeldUntil(a.StateDir, seat, now); held {
			d.Until = until
			return d.wait(fmt.Sprintf("hold on seat %s until %s: %s", seat, until.UTC().Format(time.RFC3339), why))
		}
		if claudeOnly && !claudePlan(plan) {
			return d.wait(fmt.Sprintf("claude_only: seat %s spends %s", seat, planName(plan)))
		}
		if plan == "" {
			if fromFull {
				d.Until = earliest
				return d.wait(fmt.Sprintf("fallback seat %s is unmetered; work never falls back to an unmetered seat", seat))
			}
			return d.run(fmt.Sprintf("seat %s spends no metered plan", seat))
		}
		st := state(plan, readings, a.held, now)
		if st.AsOf.IsZero() {
			d.Inputs[plan] = "none"
		} else {
			d.Inputs[plan] = st.AsOf.UTC().Format(time.RFC3339)
		}
		if st.Unknown {
			d.Unknown = append(d.Unknown, plan)
		}
		if !st.Full {
			note := fmt.Sprintf("seat %s: plan %s has room", seat, plan)
			if st.Unknown {
				note = fmt.Sprintf("seat %s: plan %s reading unknown, treated as open", seat, plan)
			}
			if len(d.Trail) > 0 {
				note += fmt.Sprintf(" (asked %s)", req.Asked)
			}
			return d.run(note)
		}
		d.Trail = append(d.Trail, Hop{Seat: seat, Plan: plan, Note: fmt.Sprintf("plan %s full until %s (%s)", plan, untilText(st.Until), st.Why)})
		if !st.Until.IsZero() && (earliest.IsZero() || st.Until.Before(earliest)) {
			earliest = st.Until
		}
		if staff.Fallback == "" {
			d.Until = earliest
			return d.wait(fmt.Sprintf("plan %s full until %s (%s); seat %s has no fallback", plan, untilText(st.Until), st.Why, seat))
		}
		next, more := r.LandingTrail(a.StateDir, staff.Fallback, now)
		for _, p := range more {
			d.Trail = append(d.Trail, Hop{Seat: p.Seat, Plan: PlanOf(r.Staff[p.Seat]), Note: p.Reason})
		}
		seat, fromFull = next, true
	}
	d.Until = earliest
	return d.wait(fmt.Sprintf("fallback chain from %s longer than %d seats", req.Asked, maxHops))
}

func (d Decision) run(reason string) Decision {
	d.Verdict, d.Reason = Run, reason
	return d
}

func (d Decision) wait(reason string) Decision {
	d.Verdict, d.Reason = Wait, reason
	return d
}

func hasConstraint(constraints []string, want string) bool {
	for _, c := range constraints {
		if strings.TrimSpace(c) == want {
			return true
		}
	}
	return false
}

func planName(plan string) string {
	if plan == "" {
		return "no metered plan"
	}
	return "plan " + plan
}

func untilText(t time.Time) string {
	if t.IsZero() {
		return "an unknown time"
	}
	return t.UTC().Format(time.RFC3339)
}
