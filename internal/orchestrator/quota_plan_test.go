package orchestrator

import "testing"

// The opencode runner spends the Z.ai coding plan since OpenCode Go was
// retired (2026-10-20); a retired provider pin resolves to no plan at all.
func TestQuotaPlanForOpencodeRunnerIsZai(t *testing.T) {
	if got := quotaPlanForModelString("opencode"); got != "zai" {
		t.Fatalf("quotaPlanForModelString(%q) = %q, want zai", "opencode", got)
	}
}

func TestQuotaPlanForRetiredOpencodeGoPinIsNone(t *testing.T) {
	if got := quotaPlanForModelString("opencode-go/glm-5.3"); got != "" {
		t.Fatalf("quotaPlanForModelString(%q) = %q, want no plan", "opencode-go/glm-5.3", got)
	}
}

// A bare hermes launch (no roster seat) runs the wk-code profile, whose models spend the
// Z.ai coding plan: the turn is metered, never unmetered.
func TestQuotaPlanForBareHermesIsZai(t *testing.T) {
	if got := quotaPlanForModelString("hermes"); got != "zai" {
		t.Fatalf("quotaPlanForModelString(%q) = %q, want zai", "hermes", got)
	}
	// The quota-wall park for a turn dispatched on bare hermes wakes on the Z.ai plan's reset.
	m := &Manager{}
	if name, plan := m.quotaWallNameAndPlan(Document{}, nil, Lease{TargetModel: "hermes"}); name != "hermes" || plan != "zai" {
		t.Fatalf("quotaWallNameAndPlan(bare hermes) = %q %q, want hermes zai", name, plan)
	}
}
