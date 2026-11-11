package finite

import (
	"bytes"
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

// validateFiniteWAL refuses a WAL whose header is malformed and otherwise
// follows SQLite's own recovery contract for the frames behind it. SQLite
// restarts the log under a new salt after a checkpoint without truncating it,
// and a killed process can leave a partial or torn last frame, so the valid
// log ends at the first frame with a different salt, a trailing partial
// frame, a zero page number, or the first checksum mismatch. None of those is
// corruption; SQLite recovers exactly the committed prefix before them.
func validateFiniteWAL(path string) error {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("finite: read WAL: %w", err)
	}
	_, err = finiteWALValidFrames(data)
	return err
}

// finiteWALValidFrames checks the WAL header and returns how many frames form
// the valid log SQLite would scan during recovery.
func finiteWALValidFrames(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	corrupt := func(reason string) error { return fmt.Errorf("finite: corrupt WAL: %s", reason) }
	if len(data) < 32 {
		return 0, corrupt("truncated header")
	}
	var order binary.ByteOrder = binary.BigEndian
	switch magic := binary.BigEndian.Uint32(data[:4]); magic {
	case 0x377f0682:
		order = binary.LittleEndian
	case 0x377f0683:
	default:
		return 0, corrupt("invalid magic")
	}
	if version := binary.BigEndian.Uint32(data[4:8]); version != 3007000 {
		return 0, corrupt("unsupported version")
	}
	pageSize := uint64(binary.BigEndian.Uint32(data[8:12]))
	if pageSize < 512 || pageSize > 65536 || pageSize&(pageSize-1) != 0 {
		return 0, corrupt("invalid page size")
	}
	seed0, seed1 := finiteWALChecksum(order, 0, 0, data[:24])
	if seed0 != binary.BigEndian.Uint32(data[24:28]) || seed1 != binary.BigEndian.Uint32(data[28:32]) {
		return 0, corrupt("header checksum")
	}
	salt := data[16:24]
	frameSize := 24 + int(pageSize)
	frames := 0
	for offset := 32; offset+frameSize <= len(data); offset += frameSize {
		header := data[offset : offset+24]
		if !bytes.Equal(header[8:16], salt) || binary.BigEndian.Uint32(header[:4]) == 0 {
			break
		}
		next0, next1 := finiteWALChecksum(order, seed0, seed1, header[:8])
		next0, next1 = finiteWALChecksum(order, next0, next1, data[offset+24:offset+frameSize])
		if next0 != binary.BigEndian.Uint32(header[16:20]) || next1 != binary.BigEndian.Uint32(header[20:24]) {
			break
		}
		seed0, seed1 = next0, next1
		frames++
	}
	return frames, nil
}

func finiteWALChecksum(order binary.ByteOrder, seed0, seed1 uint32, data []byte) (uint32, uint32) {
	for len(data) >= 8 {
		seed0 += order.Uint32(data[:4]) + seed1
		seed1 += order.Uint32(data[4:8]) + seed0
		data = data[8:]
	}
	return seed0, seed1
}
