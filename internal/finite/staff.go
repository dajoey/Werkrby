package finite

import (
	"time"

	"github.com/agent0ai/spynel/internal/harness"
)

// Seat gate vocabulary: how a fallback rung may be used once it is reached.
const (
	// SeatGateBalance marks a rung that bills real money against the pool the
	// chain's subscription rungs use: it runs only while every unresolved wall
	// on the chain still resets more than balanceGateLead away, so money is
	// never spent against an imminent free reset.
	SeatGateBalance = "balance"
	// SeatGatePaidOK marks a rung on a paid per-token provider the roster
	// explicitly opted into; it carries no reset-proximity gate.
	SeatGatePaidOK = "paid-ok"
)

const (
	balanceGateLead  = 10 * time.Minute
	maxFallbackRungs = 8
)

// SeatRung is one seat of a phase fallback chain.
type SeatRung struct {
	Seat string
	Gate string
}

// SeatWall is one provider wall observed on a chain seat.
type SeatWall struct {
	Seat   string
	WakeAt time.Time
}

// PhaseChain is the ordered seats a run of the phase may use: the phase staff
// seat first, then its fallback rungs. A phase without a staff seat has no
// chain and its walls stay plain waits.
func (b Brief) PhaseChain(phase Phase) []SeatRung {
	if phase == PhaseReview {
		if b.ReviewStaff == "" {
			return nil
		}
		return append([]SeatRung{{Seat: b.ReviewStaff}}, b.ReviewFallbacks...)
	}
	if b.Staff == "" {
		return nil
	}
	return append([]SeatRung{{Seat: b.Staff}}, b.Fallbacks...)
}

// chooseSeatRung returns the index of the first chain rung that may run at
// now, or -1 and the earliest unresolved wall wake when none may. A wall is
// unresolved only while its wake is still in the future; a wall recorded
// against a seat outside the chain benches the primary rung, which keeps run
// rows dispatched before the chain existed on the primary.
func chooseSeatRung(chain []SeatRung, walls []SeatWall, now time.Time) (int, *time.Time) {
	if len(chain) == 0 {
		return -1, nil
	}
	primary := chain[0].Seat
	chainSeats := make(map[string]bool, len(chain))
	for _, rung := range chain {
		chainSeats[rung.Seat] = true
	}
	unresolved := make(map[string]time.Time, len(walls))
	var earliest *time.Time
	for _, wall := range walls {
		if !wall.WakeAt.After(now) {
			continue
		}
		seat := wall.Seat
		if !chainSeats[seat] {
			seat = primary
		}
		previous, seen := unresolved[seat]
		if !seen || wall.WakeAt.Before(previous) {
			previous = wall.WakeAt
			unresolved[seat] = previous
		}
		if earliest == nil || previous.Before(*earliest) {
			value := previous
			earliest = &value
		}
	}
	for index, rung := range chain {
		if _, walled := unresolved[rung.Seat]; walled {
			continue
		}
		if rung.Gate == SeatGateBalance {
			gated := false
			for _, wake := range unresolved {
				if !wake.After(now.Add(balanceGateLead)) {
					gated = true
					break
				}
			}
			if gated {
				continue
			}
		}
		return index, earliest
	}
	return -1, earliest
}

// wallSeat maps the seat a run was dispatched on to its chain rung: a seat in
// the chain is itself; anything else (admission labels, pre-chain rows) is the
// primary rung.
func wallSeat(chain []SeatRung, seat string) string {
	for _, rung := range chain {
		if rung.Seat == seat {
			return seat
		}
	}
	if len(chain) == 0 {
		return seat
	}
	return chain[0].Seat
}

// seatForDispatch records the staff seat a run is launched on; a task without
// a staff seat keeps the admission decision's label.
func seatForDispatch(admissionSeat, staff string) string {
	if staff != "" {
		return staff
	}
	return admissionSeat
}

// harnessForPhase returns the run's harness configuration: the engine default,
// or the staff entry the immutable brief names for this phase.
func harnessForPhase(base harness.HarnessConfig, brief Brief, phase Phase) harness.HarnessConfig {
	config := base
	staff := brief.Staff
	if phase == PhaseReview && brief.ReviewStaff != "" {
		staff = brief.ReviewStaff
	}
	if staff != "" {
		config.Model = "@" + staff
	}
	return config
}
