//go:build linux

package harness

import (
	"bytes"
	"errors"
	"os"
	"strconv"
)

// hermesProcessGenerationGone reads the same fingerprint Hermes records for a
// gateway (field 22 of /proc/<pid>/stat) and reports whether that exact
// process incarnation has ended. An unreadable entry other than a missing one
// is doubt, not absence.
func hermesProcessGenerationGone(pid int, recordedStart int64) (bool, error) {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return true, nil
		}
		return false, nil
	}
	end := bytes.LastIndexByte(data, ')')
	if end < 0 {
		return false, nil
	}
	fields := bytes.Fields(data[end+1:])
	if len(fields) < 20 {
		return false, nil
	}
	current, err := strconv.ParseInt(string(fields[19]), 10, 64)
	if err != nil {
		return false, nil
	}
	diff := current - recordedStart
	if diff < 0 {
		diff = -diff
	}
	return diff > hermesStartTimeDriftTolerance, nil
}
