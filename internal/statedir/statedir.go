// Package statedir resolves a workspace's active state directory. It is a leaf
// package so config, roster consumers and harnesses all derive the same path.
package statedir

import (
	"os"
	"path/filepath"
)

const (
	// Legacy is the original state directory name.
	Legacy = ".spynel"
	// Current is the state directory name once a workspace has switched.
	Current = ".werkrby"
)

// Resolve returns <root>/.werkrby when that directory exists, else <root>/.spynel.
func Resolve(root string) string {
	current := filepath.Join(root, Current)
	if fi, err := os.Stat(current); err == nil && fi.IsDir() {
		return current
	}
	return filepath.Join(root, Legacy)
}
