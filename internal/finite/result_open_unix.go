//go:build !windows

package finite

import "os"

func openResultFile(path string) (*os.File, error) { return nil, nil }
