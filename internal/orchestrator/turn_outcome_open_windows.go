//go:build windows

package orchestrator

import "os"

// openResultFile opens result.json; readResultFile has already refused anything but a regular file.
func openResultFile(path string) (*os.File, error) {
	return os.Open(path)
}
