package harness

import (
	"os"
	"path/filepath"
	"testing"
)

// A finite run may use a real dedicated profile, but only one whose name
// claims the finite namespace and whose home carries the matching opt-in
// marker. Production worker profiles and the default home stay refused.
func TestFiniteHermesProfileFence(t *testing.T) {
	home := t.TempDir()
	write := func(profile string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(home, finiteHermesMarker), []byte(profile+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, profile := range []string{"wk-finite", "wk-finite-impl", "wk-finite-test"} {
		write(profile)
		if err := validateDisposableHermesHome(home, profile); err != nil {
			t.Errorf("profile %q with its marker was refused: %v", profile, err)
		}
	}
	for _, profile := range []string{"wk-code", "default", "wk-finitex", "finite", "wk-finite/../wk-code", "wk-finite-"} {
		write(profile)
		if err := validateDisposableHermesHome(home, profile); err == nil {
			t.Errorf("profile %q was admitted", profile)
		}
	}
	write("wk-finite")
	if err := validateDisposableHermesHome(home, "wk-finite-impl"); err == nil {
		t.Error("a marker naming another profile admitted this one")
	}
	userHome, err := os.UserHomeDir()
	if err == nil {
		if err := validateDisposableHermesHome(filepath.Join(userHome, ".hermes"), "wk-finite"); err == nil {
			t.Error("the default ~/.hermes home was admitted")
		}
	}
}
