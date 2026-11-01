//go:build windows

package finite

import "golang.org/x/sys/windows"

func replaceProjectionFile(source, target string) error {
	return windows.Rename(source, target)
}
