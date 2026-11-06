package harness

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// hermesGatewayPIDFile is the gateway identity record Hermes writes under its
// HERMES_HOME while a gateway runs.
const hermesGatewayPIDFile = "gateway.pid"

// hermesStartTimeDriftTolerance matches Hermes's START_TIME_DRIFT_TOLERANCE:
// two start-time fingerprints within it are the same process incarnation.
const hermesStartTimeDriftTolerance = 200

type hermesGatewayRecord struct {
	PID        int    `json:"pid"`
	StartTime  *int64 `json:"start_time"`
	HermesHome string `json:"hermes_home"`
}

// hermesOwnerGone reports whether the Hermes gateway generation that owned
// ownerEpoch provably no longer exists. Hermes never replays a started turn
// once its owner is gone: the next gateway marks it unknown and blocks its
// session. The owner is gone when a later epoch has taken over the ledger, or
// when the ledger is still at ownerEpoch and the gateway recorded in
// gateway.pid is no longer that process (dead PID, or a PID reused by a
// different start time, as after a reboot). Any doubt returns false, so the
// caller keeps failing closed.
func hermesOwnerGone(ctx context.Context, db *sql.DB, home string, ownerEpoch int64) (bool, error) {
	var current int64
	err := db.QueryRowContext(ctx, `SELECT epoch FROM runtime_epoch WHERE singleton=1`).Scan(&current)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) || strings.Contains(err.Error(), "no such table") {
			return false, nil
		}
		return false, err
	}
	if current > ownerEpoch {
		return true, nil
	}
	if current < ownerEpoch || strings.TrimSpace(home) == "" {
		return false, nil
	}
	data, err := os.ReadFile(filepath.Join(home, hermesGatewayPIDFile))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	var record hermesGatewayRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return false, nil
	}
	if record.PID <= 0 || record.StartTime == nil {
		return false, nil
	}
	if record.HermesHome != "" && filepath.Clean(record.HermesHome) != filepath.Clean(home) {
		return false, nil
	}
	return hermesProcessGenerationGone(record.PID, *record.StartTime)
}

// hermesAdmissionOrphaned reports whether an admission's non-terminal status
// can no longer describe a running turn because its owning gateway is gone.
// A queued admission is never orphaned: the next gateway may still start it.
func hermesAdmissionOrphaned(ctx context.Context, db *sql.DB, home string, row hermesAdmissionRow) (bool, error) {
	switch row.status {
	case "started", "unknown":
		return hermesOwnerGone(ctx, db, home, row.ownerEpoch)
	default:
		return false, nil
	}
}
