//go:build !linux

package finite

import "syscall"

// acceptancePlatformSupported gates the check: only Linux runs declared
// commands today. Elsewhere the recorded verdict is a verifier failure, never
// a pass or a task failure.
const acceptancePlatformSupported = false

func acceptanceSysProcAttr() *syscall.SysProcAttr { return nil }

func killAcceptanceWorktreeProcesses(string) {}
