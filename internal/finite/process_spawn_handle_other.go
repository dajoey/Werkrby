//go:build !windows

package finite

// supervisorSpawnHandle is the non-Windows stub: Linux runs keep the
// supervisor's process group as their boundary and carry no job handle.
func supervisorSpawnHandle(supervisorContainment) uintptr { return 0 }
