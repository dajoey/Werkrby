package finite

import "github.com/agent0ai/spynel/internal/harness"

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
