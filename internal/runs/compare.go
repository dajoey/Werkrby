package runs

// Verdict says how the attempt a claim charged compares with the attempt the runs imply.
type Verdict string

const (
	// Match: the files charged what the runs imply.
	Match Verdict = "match"
	// Refund: the previous run counted but the files did not advance. Every park credit, format-only
	// rejection and high-risk review handoff has this shape, and so does the recovery of a turn whose
	// file move never came (the live-path check shows it), so a refund is a known carve-out site,
	// not yet a disagreement; naming the site needs the run's outcome refined (parked, rejected).
	Refund Verdict = "refund"
	// Disagree: neither of the above. This is what the Stage B check counts.
	Disagree Verdict = "disagree"
	// Unseeded: the store has no earlier run of this document and phase, so there is nothing to
	// compare with (the store is newer than the document).
	Unseeded Verdict = "unseeded"
	// PreviousOpen: the earlier run never recorded an end, so its cost is unknown.
	PreviousOpen Verdict = "previous-open"
)

// LastRun is the latest run of a document's phase.
func (s *Store) LastRun(doc, phase string) (Run, bool, error) {
	all, err := s.Runs(doc)
	if err != nil {
		return Run{}, false, err
	}
	for i := len(all) - 1; i >= 0; i-- {
		if all[i].Phase == phase {
			return all[i], true, nil
		}
	}
	return Run{}, false, nil
}

// Check compares the attempt number a claim charged with the one the previous run of the same
// document and phase implies: a run that counted (ok, fail, timeout) makes the next claim the next
// attempt, any other run leaves the attempt where it was. It returns the derived attempt, which is
// 0 when there is nothing to derive it from.
func Check(prev *Run, charged int) (int, Verdict) {
	switch {
	case prev == nil:
		return 0, Unseeded
	case prev.Outcome == Running:
		return 0, PreviousOpen
	}
	derived := prev.Charged
	if prev.Outcome.Counts() {
		derived++
	}
	switch {
	case charged == derived:
		return derived, Match
	case prev.Outcome.Counts() && charged == prev.Charged:
		return derived, Refund
	}
	return derived, Disagree
}
