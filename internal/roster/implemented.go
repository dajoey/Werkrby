package roster

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/agent0ai/spynel/internal/fsx"
)

const (
	implementedFileName = "roster-implemented.json"
	implementedKeep     = 30 * 24 * time.Hour
)

type implementedRecord struct {
	Seat string    `json:"seat"`
	At   time.Time `json:"at"`
}

// RecordImplementer remembers which staff an implementation of `task` (the task document's file
// name) was dispatched to. The daily usage count forgets at midnight, but a review can come the
// next day and still needs to know who wrote the work; the latest implementation wins, and records
// older than 30 days are dropped so the file stays small. Bookkeeping only, never workflow
// authority.
func RecordImplementer(stateDir, task, seat string, now time.Time) error {
	if task == "" || seat == "" {
		return nil
	}
	usageMu.Lock()
	defer usageMu.Unlock()
	path := filepath.Join(stateDir, "runtime", implementedFileName)
	records := map[string]implementedRecord{}
	if data, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(data, &records)
	}
	records[task] = implementedRecord{Seat: seat, At: now}
	for key, record := range records {
		if now.Sub(record.At) > implementedKeep {
			delete(records, key)
		}
	}
	data, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return fsx.AtomicWriteFile(path, data, 0o600)
}

// ImplementedBy returns the staff the latest implementation of `task` was dispatched to.
func ImplementedBy(stateDir, task string) (string, bool) {
	usageMu.Lock()
	defer usageMu.Unlock()
	data, err := os.ReadFile(filepath.Join(stateDir, "runtime", implementedFileName))
	if err != nil {
		return "", false
	}
	records := map[string]implementedRecord{}
	if json.Unmarshal(data, &records) != nil {
		return "", false
	}
	record, ok := records[task]
	return record.Seat, ok && record.Seat != ""
}
