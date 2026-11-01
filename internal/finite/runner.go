package finite

import "github.com/agent0ai/spynel/internal/harness"

// NewRunner constructs an ordinary harness adapter. Finite fake providers are
// injected only by colocated tests through harness.Factory.
func NewRunner(cfg harness.HarnessConfig) (harness.Harness, error) {
	return harness.NewBuiltinRegistry().Create(cfg)
}
