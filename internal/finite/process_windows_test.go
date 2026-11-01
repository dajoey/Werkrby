//go:build windows

package finite

import (
	"strings"
	"testing"
)

func TestFiniteWindowsIdentifyChildProcess(t *testing.T) {
	command, _ := startFiniteSleeper(t)
	token, err := NewProcessToken()
	if err != nil {
		t.Fatal(err)
	}
	identity, err := IdentifyProcess(command.Process.Pid, 73, token)
	if err != nil {
		t.Fatalf("identify live child process: %v", err)
	}
	if identity.PID != command.Process.Pid || identity.RunID != 73 || identity.Token != token || !strings.HasPrefix(identity.Start, "windows:") {
		t.Fatalf("child identity = %#v", identity)
	}
}
