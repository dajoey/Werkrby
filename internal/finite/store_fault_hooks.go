package finite

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// storeSQLiteDSN keeps the production DSN in one replaceable boundary so tests
// can select a real SQLite VFS without replacing the store or its driver.
var storeSQLiteDSN = func(cfg Config) string {
	return "file:" + filepath.ToSlash(cfg.DatabasePath()) + "?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)&_pragma=journal_mode(wal)&_pragma=synchronous(full)"
}

type storeTransactionFaultPoint string

const (
	storeTransactionBeforeCommit storeTransactionFaultPoint = "before_commit"
	storeTransactionAfterCommit  storeTransactionFaultPoint = "after_commit"
)

var storeTransactionFault struct {
	sync.RWMutex
	point storeTransactionFaultPoint
	hook  func() error
}

// registerStoreTransactionFault is package-private and production-inert until
// a same-package fault test arms a transaction boundary.
func registerStoreTransactionFault(point storeTransactionFaultPoint, hook func() error) func() {
	storeTransactionFault.Lock()
	storeTransactionFault.point = point
	storeTransactionFault.hook = hook
	storeTransactionFault.Unlock()
	return func() {
		storeTransactionFault.Lock()
		storeTransactionFault.point = ""
		storeTransactionFault.hook = nil
		storeTransactionFault.Unlock()
	}
}

func runStoreTransactionFault(point storeTransactionFaultPoint) error {
	storeTransactionFault.RLock()
	hook := storeTransactionFault.hook
	if storeTransactionFault.point != point {
		hook = nil
	}
	storeTransactionFault.RUnlock()
	if hook == nil {
		return nil
	}
	if err := hook(); err != nil {
		return fmt.Errorf("finite: transaction fault %s: %w", point, err)
	}
	return nil
}

// validateFiniteWAL rejects malformed or incomplete WAL state before SQLite
// can silently treat a bad checksum as the end of the recoverable log.
func validateFiniteWAL(path string) error {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("finite: read WAL: %w", err)
	}
	if len(data) == 0 {
		return nil
	}
	corrupt := func(reason string) error { return fmt.Errorf("finite: corrupt WAL: %s", reason) }
	if len(data) < 32 {
		return corrupt("truncated header")
	}
	var order binary.ByteOrder = binary.BigEndian
	switch magic := binary.BigEndian.Uint32(data[:4]); magic {
	case 0x377f0682:
		order = binary.LittleEndian
	case 0x377f0683:
	default:
		return corrupt("invalid magic")
	}
	if version := binary.BigEndian.Uint32(data[4:8]); version != 3007000 {
		return corrupt("unsupported version")
	}
	pageSize := uint64(binary.BigEndian.Uint32(data[8:12]))
	if pageSize < 512 || pageSize > 65536 || pageSize&(pageSize-1) != 0 {
		return corrupt("invalid page size")
	}
	frameSize := 24 + int(pageSize)
	if (len(data)-32)%frameSize != 0 {
		return corrupt("truncated frame")
	}
	seed0, seed1 := finiteWALChecksum(order, 0, 0, data[:24])
	if seed0 != binary.BigEndian.Uint32(data[24:28]) || seed1 != binary.BigEndian.Uint32(data[28:32]) {
		return corrupt("header checksum")
	}
	for offset := 32; offset < len(data); offset += frameSize {
		header := data[offset : offset+24]
		if binary.BigEndian.Uint64(header[8:16]) != binary.BigEndian.Uint64(data[16:24]) {
			return corrupt("frame salt")
		}
		seed0, seed1 = finiteWALChecksum(order, seed0, seed1, header[:8])
		seed0, seed1 = finiteWALChecksum(order, seed0, seed1, data[offset+24:offset+frameSize])
		if seed0 != binary.BigEndian.Uint32(header[16:20]) || seed1 != binary.BigEndian.Uint32(header[20:24]) {
			return corrupt("frame checksum")
		}
	}
	return nil
}

func finiteWALChecksum(order binary.ByteOrder, seed0, seed1 uint32, data []byte) (uint32, uint32) {
	for len(data) >= 8 {
		seed0 += order.Uint32(data[:4]) + seed1
		seed1 += order.Uint32(data[4:8]) + seed0
		data = data[8:]
	}
	return seed0, seed1
}
