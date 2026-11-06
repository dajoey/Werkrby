package harness

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
)

// hermesGatewayPIDFile is the gateway identity record Hermes writes under the
// HERMES_HOME of a running gateway: the root home for the shared gateway, or a
// profile's folder for a gateway that serves only that profile.
const hermesGatewayPIDFile = "gateway.pid"

// hermesStartTimeDriftTolerance matches Hermes's START_TIME_DRIFT_TOLERANCE:
// two start-time fingerprints within it are the same process incarnation.
const hermesStartTimeDriftTolerance = 200

type hermesGatewayRecord struct {
	PID        int    `json:"pid"`
	StartTime  *int64 `json:"start_time"`
	HermesHome string `json:"hermes_home"`
}

type hermesGatewayState int

const (
	hermesGatewayDoubt hermesGatewayState = iota
	hermesGatewayMissing
	hermesGatewayAlive
	hermesGatewayDead
)

// hermesOwnerGone reports whether the Hermes gateway generation that owned
// ownerEpoch provably no longer exists. Hermes never replays a started turn
// once its owner is gone: the next gateway marks it unknown. The owner is gone
// when a later epoch has taken over the ledger, or when the ledger is still at
// ownerEpoch, no gateway.pid record in any of homes names a live gateway, and
// at least one names a gateway process that has ended (a dead PID, or a PID
// reused with a different start time, as after a reboot). Every error or
// unreadable record is doubt and returns false, so the caller keeps failing
// closed exactly as it did before this check existed.
func hermesOwnerGone(ctx context.Context, db *sql.DB, homes []string, ownerEpoch int64) bool {
	var current int64
	if err := db.QueryRowContext(ctx, `SELECT epoch FROM runtime_epoch WHERE singleton=1`).Scan(&current); err != nil {
		return false
	}
	if current > ownerEpoch {
		return true
	}
	if current < ownerEpoch {
		return false
	}
	sawDead := false
	seen := map[string]bool{}
	for _, home := range homes {
		home = strings.TrimSpace(home)
		if home == "" || seen[filepath.Clean(home)] {
			continue
		}
		seen[filepath.Clean(home)] = true
		switch hermesGatewayRecordState(home) {
		case hermesGatewayMissing:
		case hermesGatewayDead:
			sawDead = true
		default:
			return false
		}
	}
	return sawDead
}

func hermesGatewayRecordState(home string) hermesGatewayState {
	data, err := os.ReadFile(filepath.Join(home, hermesGatewayPIDFile))
	if err != nil {
		if os.IsNotExist(err) {
			return hermesGatewayMissing
		}
		return hermesGatewayDoubt
	}
	var record hermesGatewayRecord
	if err := json.Unmarshal(data, &record); err != nil || record.PID <= 0 || record.StartTime == nil {
		return hermesGatewayDoubt
	}
	if record.HermesHome != "" && filepath.Clean(record.HermesHome) != filepath.Clean(home) {
		return hermesGatewayDoubt
	}
	switch gone, known := hermesProcessGenerationGone(record.PID, *record.StartTime); {
	case !known:
		return hermesGatewayDoubt
	case gone:
		return hermesGatewayDead
	default:
		return hermesGatewayAlive
	}
}

// hermesAdmissionOrphaned reports whether an admission's non-terminal status
// can no longer describe a running turn because its owning gateway is gone.
// A queued admission is never orphaned: the next gateway may still start it.
func hermesAdmissionOrphaned(ctx context.Context, db *sql.DB, homes []string, row hermesAdmissionRow) bool {
	switch row.status {
	case "started", "unknown":
		return hermesOwnerGone(ctx, db, homes, row.ownerEpoch)
	default:
		return false
	}
}
