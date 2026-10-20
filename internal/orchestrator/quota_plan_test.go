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
