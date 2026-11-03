package finite

import (
	"sort"
	"time"
)

// Admit makes a pure run/wait/seat decision.
func Admit(in AdmissionInput) AdmissionDecision {
	task := in.Task
	if task.CancelRequested || task.State == StateWaiting {
		return AdmissionDecision{Action: AdmissionWait, Reason: "stop_or_wait_pending"}
	}
	if in.Phase == PhaseImplementation && task.State != StateTodo {
		return AdmissionDecision{Action: AdmissionWait, Reason: "not_in_implementation_queue"}
	}
	if in.Phase == PhaseReview && task.State != StateReview {
		return AdmissionDecision{Action: AdmissionWait, Reason: "not_in_review_queue"}
	}
	if in.Phase != PhaseImplementation && in.Phase != PhaseReview {
		return AdmissionDecision{Action: AdmissionWait, Reason: "unknown_phase"}
	}
	if in.Accounting.Attempts >= task.MaxRuns || in.Accounting.Runs >= task.MaxRuns {
		return AdmissionDecision{Action: AdmissionFail, Reason: "max_runs"}
	}
	if in.Accounting.Failures >= task.MaxFailures {
		return AdmissionDecision{Action: AdmissionFail, Reason: "max_failures"}
	}
	if in.Accounting.ReviewCycles >= task.MaxReviewCycles {
		return AdmissionDecision{Action: AdmissionFail, Reason: "max_review_cycles"}
	}
	if in.RosterSeats {
		return admitRoster(in)
	}
	seats := append([]Seat(nil), in.Seats...)
	sort.SliceStable(seats, func(i, j int) bool { return seats[i].Name < seats[j].Name })
	var earliest *time.Time
	for _, seat := range seats {
		if seat.Available && seat.Name != "" {
			return AdmissionDecision{Action: AdmissionDispatch, Seat: seat.Name}
		}
		if seat.QuotaReset != nil && (earliest == nil || seat.QuotaReset.Before(*earliest)) {
			value := seat.QuotaReset.UTC()
			earliest = &value
		}
	}
	return AdmissionDecision{Action: AdmissionWait, Reason: "no_available_seat", WakeAt: earliest}
}

// expireSeats closes every seat whose available_until the instant now has
// reached or passed. The roster writer stamps each open balance seat with the
// expiry of its own reading, so a relay that stops rewriting the roster can
// never keep a paid seat open past its reading. The input slice is never
// modified; a copy comes back.
func expireSeats(seats []Seat, now time.Time) []Seat {
	expired := append([]Seat(nil), seats...)
	for i := range expired {
		if expired[i].AvailableUntil != nil && !now.Before(*expired[i].AvailableUntil) {
			expired[i].Available = false
		}
	}
	return expired
}

// maxFallbackHops bounds the fallback walk, mirroring the legacy roster chain
// limit: the start seat plus at most eight fallback links.
const maxFallbackHops = 8

// admitRoster walks the task's own chain — start seat, then its fallback, and
// so on — and dispatches the first seat that is available and not walled by
// the task's own quota wall. A start seat missing from the roster is a routing
// error the task must fail on once, never a silent run on an unrelated seat.
func admitRoster(in AdmissionInput) AdmissionDecision {
	byName := make(map[string]Seat, len(in.Seats))
	for _, seat := range in.Seats {
		byName[seat.Name] = seat
	}
	current, found := byName[in.StartSeat]
	if !found {
		return AdmissionDecision{Action: AdmissionFail, Reason: "staff_not_in_roster"}
	}
	visited := make(map[string]struct{}, maxFallbackHops+1)
	var earliest *time.Time
	for hops := 0; ; hops++ {
		if _, seen := visited[current.Name]; seen {
			break // cycle: the chain repeats without a runnable seat
		}
		visited[current.Name] = struct{}{}
		_, walled := in.WalledPools[current.Pool]
		if current.Available && current.Name != "" && !walled {
			return AdmissionDecision{Action: AdmissionDispatch, Seat: current.Name}
		}
		if current.QuotaReset != nil && (earliest == nil || current.QuotaReset.Before(*earliest)) {
			value := current.QuotaReset.UTC()
			earliest = &value
		}
		if hops >= maxFallbackHops {
			break
		}
		next, linked := byName[current.Fallback]
		if current.Fallback == "" || !linked {
			break // chain end: no seat of the chain can run
		}
		current = next
	}
	return AdmissionDecision{Action: AdmissionWait, Reason: "no_available_seat", WakeAt: earliest}
}
