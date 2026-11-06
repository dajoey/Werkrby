package harness

import (
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
// at least one names a gateway process that has ended (a dead PID, a PID
// reused with a different start time, or an unreaped zombie, as after a
// reboot). Every unreadable record is doubt and returns false, so the caller
// keeps failing closed exactly as it did before this check existed.
func hermesOwnerGone(epoch int64, known bool, homes []string, ownerEpoch int64) bool {
	if !known {
		return false
	}
	if epoch > ownerEpoch {
		return true
	}
	if epoch < ownerEpoch {
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
	if record.HermesHome != "" && !hermesSameHome(record.HermesHome, home) {
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

// hermesSameHome resolves both homes through symlinks before comparing them,
// so a relocated state root (a symlinked home) keeps its gateway record
// checkable. Any resolution failure is doubt, never a mismatch.
func hermesSameHome(recorded, home string) bool {
	resolvedRecorded, err := filepath.EvalSymlinks(recorded)
	if err != nil {
		return false
	}
	resolvedHome, err := filepath.EvalSymlinks(home)
	if err != nil {
		return false
	}
	return filepath.Clean(resolvedRecorded) == filepath.Clean(resolvedHome)
}

// hermesAdmissionOrphaned reports whether an admission's non-terminal status
// can no longer describe a running turn because its owning gateway is gone.
// A queued admission is never orphaned: the next gateway may still start it.
func hermesAdmissionOrphaned(epoch int64, known bool, homes []string, row hermesAdmissionRow) bool {
	switch row.status {
	case "started", "unknown":
		return hermesOwnerGone(epoch, known, homes, row.ownerEpoch)
	default:
		return false
	}
}
