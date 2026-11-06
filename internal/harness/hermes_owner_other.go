//go:build !linux

package harness

// hermesProcessGenerationGone cannot read Hermes's start-time fingerprint off
// Linux, so it never proves a gateway gone there.
func hermesProcessGenerationGone(int, int64) (bool, error) {
	return false, nil
}
