package finite

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type projectionPublicationFaultPoint string

const (
	projectionAfterTempWrite projectionPublicationFaultPoint = "after_temp_write"
	projectionBeforeRename   projectionPublicationFaultPoint = "before_rename"
	projectionAfterRename    projectionPublicationFaultPoint = "after_rename"
)

var projectionPublicationFault struct {
	sync.RWMutex
	point projectionPublicationFaultPoint
	hook  func() error
}

// registerProjectionPublicationFault is package-private and leaves the normal
// fsx publication path untouched unless a same-package fault test arms it.
func registerProjectionPublicationFault(point projectionPublicationFaultPoint, hook func() error) func() {
	projectionPublicationFault.Lock()
	projectionPublicationFault.point = point
	projectionPublicationFault.hook = hook
	projectionPublicationFault.Unlock()
	return func() {
		projectionPublicationFault.Lock()
		projectionPublicationFault.point = ""
		projectionPublicationFault.hook = nil
		projectionPublicationFault.Unlock()
	}
}

func runProjectionPublicationFault(point projectionPublicationFaultPoint) error {
	projectionPublicationFault.RLock()
	hook := projectionPublicationFault.hook
	if projectionPublicationFault.point != point {
		hook = nil
	}
	projectionPublicationFault.RUnlock()
	if hook == nil {
		return nil
	}
	if err := hook(); err != nil {
		return fmt.Errorf("finite: projection publication fault %s: %w", point, err)
	}
	return nil
}

func writeProjectionWithFaults(path string, data []byte, mode os.FileMode) error {
	projectionPublicationFault.RLock()
	armed := projectionPublicationFault.hook != nil
	projectionPublicationFault.RUnlock()
	if !armed {
		return atomicWriteFile(path, data, mode)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".werkrby-write-*")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(mode); err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		return err
	}
	if err := runProjectionPublicationFault(projectionAfterTempWrite); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := runProjectionPublicationFault(projectionBeforeRename); err != nil {
		return err
	}
	if err := replaceProjectionFile(tempName, path); err != nil {
		return err
	}
	return runProjectionPublicationFault(projectionAfterRename)
}
