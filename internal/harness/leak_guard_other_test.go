//go:build !linux

package harness

import "testing"

// requireNoGatewayUnder mirrors leak_guard_linux_test.go: there it scans
// /proc for a Hermes gateway the test leaked from its disposable home and
// kills it before failing. Non-Linux platforms have no /proc scan; the guard
// is a no-op so the fixture compiles and vets everywhere.
func requireNoGatewayUnder(t *testing.T, _ string) {
	t.Helper()
}
