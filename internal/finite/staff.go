package finite

import "github.com/agent0ai/spynel/internal/harness"

// harnessForPhase returns the run's harness configuration. In roster mode the
// seat is the one the fallback walk chose; otherwise the staff entry the
// immutable brief names for this phase, else the engine default.
func harnessForPhase(base harness.HarnessConfig, brief Brief, phase Phase, seat string) harness.HarnessConfig {
	config := base
	staff := seat
	if staff == "" {
		staff = brief.Staff
		if phase == PhaseReview && brief.ReviewStaff != "" {
			staff = brief.ReviewStaff
		}
	}
	if staff != "" {
		config.Model = "@" + staff
	}
	return config
}
