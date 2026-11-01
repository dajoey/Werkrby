//go:build !windows

package finite

import "os"

func replaceProjectionFile(source, target string) error {
	return os.Rename(source, target)
}
