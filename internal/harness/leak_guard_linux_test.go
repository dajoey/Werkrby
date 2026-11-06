//go:build linux

package harness

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// requireNoGatewayUnder fails t when a process owned by the current user still
// runs with HERMES_HOME under root: a Hermes gateway the test leaked from its
// disposable home. The home path lives only in the process environment, so
// pgrep cannot see it; /proc can. Each leaked process is killed (SIGTERM, then
// SIGKILL after five seconds) so one leak cannot outlive the suite, and the
// failure names the pid and command line.
func requireNoGatewayUnder(t *testing.T, root string) {
	t.Helper()
	self := os.Getpid()
	uid := strconv.Itoa(os.Getuid())
	type leakedGateway struct {
		pid     int
		cmdline string
	}
	var leaked []leakedGateway
	entries, err := os.ReadDir("/proc")
	if err != nil {
		t.Fatalf("gateway leak guard cannot scan /proc: %v", err)
	}
	for _, entry := range entries {
		pid, convErr := strconv.Atoi(entry.Name())
		if convErr != nil || pid <= 0 || pid == self {
			continue
		}
		status, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
		if err != nil {
			continue
		}
		owner := false
		for _, line := range strings.Split(string(status), "\n") {
			if fields := strings.Fields(line); len(fields) >= 2 && fields[0] == "Uid:" && fields[1] == uid {
				owner = true
				break
			}
		}
		if !owner {
			continue
		}
		environ, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", pid))
		if err != nil {
			continue
		}
		home := ""
		for _, setting := range strings.Split(string(environ), "\x00") {
			if after, ok := strings.CutPrefix(setting, "HERMES_HOME="); ok {
				home = after
			}
		}
		if home == "" || !strings.HasPrefix(home, root) {
			continue
		}
		cmdline, _ := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
		command := string(bytes.ReplaceAll(bytes.TrimRight(cmdline, "\x00"), []byte("\x00"), []byte(" ")))
		leaked = append(leaked, leakedGateway{pid: pid, cmdline: command})
	}
	if len(leaked) == 0 {
		return
	}
	detail := make([]string, 0, len(leaked))
	for _, p := range leaked {
		detail = append(detail, fmt.Sprintf("pid %d: %s", p.pid, p.cmdline))
		if syscall.Kill(p.pid, syscall.SIGTERM) == nil {
			if !waitProcGone(p.pid, 5*time.Second) {
				syscall.Kill(p.pid, syscall.SIGKILL)
				waitProcGone(p.pid, time.Second)
			}
		}
	}
	t.Fatalf("test leaked %d Hermes gateway(s) under %s: %s", len(leaked), root, strings.Join(detail, "; "))
}

// waitProcGone polls until the pid disappears from /proc or the limit passes.
func waitProcGone(pid int, limit time.Duration) bool {
	deadline := time.Now().Add(limit)
	for {
		if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); err != nil {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
}
