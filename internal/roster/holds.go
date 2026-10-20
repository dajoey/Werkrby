package roster

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// A hold makes a seat's queued work wait instead of starting: no session, no
// attempt spent, no move down the fallback chain. The quota gate writes
// runtime/roster-holds.json when a plan is being paced (the Z.ai pace hold);
// the orchestrator skips a claim whose seat is held. A file whose updated_at
// is older than holdsMaxAge is ignored, so a gate that stops writing never
// freezes dispatch.
const (
	holdsFileName = "roster-holds.json"
	holdsMaxAge   = 30 * time.Minute
)

type holdEntry struct {
	Until  string `json:"until"`
	Reason string `json:"reason"`
}

type holdsFile struct {
	UpdatedAt string               `json:"updated_at"`
	Staff     map[string]holdEntry `json:"staff"`
}

// HeldUntil reports whether staff `name` is held at `now`, the time the hold
// lifts and its reason. A missing, unreadable or stale file holds nothing.
func HeldUntil(stateDir, name string, now time.Time) (time.Time, string, bool) {
	data, err := os.ReadFile(filepath.Join(stateDir, "runtime", holdsFileName))
	if err != nil {
		return time.Time{}, "", false
	}
	var h holdsFile
	if json.Unmarshal(data, &h) != nil {
		return time.Time{}, "", false
	}
	updated, err := time.Parse(time.RFC3339, strings.TrimSpace(h.UpdatedAt))
	if err != nil || now.Sub(updated) > holdsMaxAge {
		return time.Time{}, "", false
	}
	entry, ok := h.Staff[name]
	if !ok {
		return time.Time{}, "", false
	}
	until, err := time.Parse(time.RFC3339, strings.TrimSpace(entry.Until))
	if err != nil || !now.Before(until) {
		return time.Time{}, "", false
	}
	return until, entry.Reason, true
}

// Landing is the staff Assign would choose for `name` now, without counting a
// session or writing anything: it follows fallback past capped and stopped
// staff exactly as Assign does.
func (r *Roster) Landing(stateDir, name string, now time.Time) string {
	landing, _ := r.LandingTrail(stateDir, name, now)
	return landing
}

// Passed is one staff the fallback walk went past, and why.
type Passed struct {
	Seat   string
	Reason string
}

// LandingTrail is Landing plus the staff it went past, in order, each with the
// reason (daily cap, stopped). Read-only like Landing.
func (r *Roster) LandingTrail(stateDir, name string, now time.Time) (string, []Passed) {
	if !r.Has(name) {
		return name, nil
	}
	today := now.Local().Format("2006-01-02")
	u := usage{}
	if data, err := os.ReadFile(filepath.Join(stateDir, "runtime", usageFileName)); err == nil {
		_ = json.Unmarshal(data, &u)
	}
	if u.Date != today || u.Counts == nil {
		u = usage{Date: today, Counts: map[string]int{}}
	}
	return r.followTrail(name, u.Counts, loadDecisions(stateDir, today))
}

// follow walks the fallback chain from `name` past staff that are at their
// daily cap (without a "continue") or stopped, as Assign routes a new session.
func (r *Roster) follow(name string, counts map[string]int, decided map[string]string) string {
	chosen, _ := r.followTrail(name, counts, decided)
	return chosen
}

func (r *Roster) followTrail(name string, counts map[string]int, decided map[string]string) (string, []Passed) {
	chosen := name
	var passed []Passed
	for hop := 0; hop < maxFallback; hop++ {
		s := r.Staff[chosen]
		if s.Fallback == "" {
			break
		}
		halted := s.DailyCap > 0 && counts[chosen] >= s.DailyCap && decided[chosen] != DecisionContinue
		stopped := decided[chosen] == DecisionStop
		if !halted && !stopped {
			break
		}
		reason := "stopped for today"
		if !stopped {
			reason = fmt.Sprintf("daily cap %d reached", s.DailyCap)
		}
		passed = append(passed, Passed{Seat: chosen, Reason: reason})
		chosen = s.Fallback
	}
	return chosen, passed
}

// AnyHeld is a cheap pre-check: true when the holds file is fresh and lists at
// least one staff still held at `now`.
func AnyHeld(stateDir string, now time.Time) bool {
	data, err := os.ReadFile(filepath.Join(stateDir, "runtime", holdsFileName))
	if err != nil {
		return false
	}
	var h holdsFile
	if json.Unmarshal(data, &h) != nil || len(h.Staff) == 0 {
		return false
	}
	updated, err := time.Parse(time.RFC3339, strings.TrimSpace(h.UpdatedAt))
	if err != nil || now.Sub(updated) > holdsMaxAge {
		return false
	}
	for _, entry := range h.Staff {
		if until, err := time.Parse(time.RFC3339, strings.TrimSpace(entry.Until)); err == nil && now.Before(until) {
			return true
		}
	}
	return false
}
