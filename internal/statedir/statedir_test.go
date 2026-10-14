package statedir

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolvePrefersExistingWerkrbyDirectory(t *testing.T) {
	root := t.TempDir()
	if got, want := Resolve(root), filepath.Join(root, Legacy); got != want {
		t.Fatalf("empty workspace = %q, want %q", got, want)
	}
	if err := os.Mkdir(filepath.Join(root, Current), 0o700); err != nil {
		t.Fatal(err)
	}
	if got, want := Resolve(root), filepath.Join(root, Current); got != want {
		t.Fatalf("with .werkrby = %q, want %q", got, want)
	}
}
