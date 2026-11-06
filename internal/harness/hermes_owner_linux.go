//go:build linux

package harness

import (
	"bytes"
	"errors"
	"os"
	"strconv"
)

// hermesProcessGenerationGone reads the same fingerprint Hermes records for a
// gateway (field 22 of /proc/<pid>/stat). known is false when the reading
// proves nothing: an unreadable or unparsable entry is doubt, not absence.
// This relies on the engine sharing the gateway's PID namespace and seeing its
// /proc entries (no hidepid), as the serve unit does today.
func hermesProcessGenerationGone(pid int, recordedStart int64) (gone, known bool) {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return true, true
		}
		return false, false
	}
	end := bytes.LastIndexByte(data, ')')
	if end < 0 {
		return false, false
	}
	fields := bytes.Fields(data[end+1:])
	if len(fields) < 20 {
		return false, false
	}
	// The state letter is the first field after the command name. Z marks a
	// gateway that ended but was never reaped: it can never run again.
	if string(fields[0]) == "Z" {
		return true, true
	}
	current, err := strconv.ParseInt(string(fields[19]), 10, 64)
	if err != nil {
		return false, false
	}
	diff := current - recordedStart
	if diff < 0 {
		diff = -diff
	}
	return diff > hermesStartTimeDriftTolerance, true
}
