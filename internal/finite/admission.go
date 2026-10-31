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
	if in.Accounting.Attempts >= task.MaxRuns {
		return AdmissionDecision{Action: AdmissionFail, Reason: "max_runs"}
	}
	if in.Accounting.Failures >= task.MaxFailures {
		return AdmissionDecision{Action: AdmissionFail, Reason: "max_failures"}
	}
	if in.Accounting.ReviewCycles >= task.MaxReviewCycles {
		return AdmissionDecision{Action: AdmissionFail, Reason: "max_review_cycles"}
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
