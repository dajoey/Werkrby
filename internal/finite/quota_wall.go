package finite

import (
	"context"
	"encoding/json"
	"strings"
	"time"
)

// WallSource names where a settled quota wall's wake came from. It travels in
// the settled event's payload, so pool walls and replays can tell a corrected
// wake from one stored before the correction existed; a payload without one is
// a legacy row.
type WallSource string

const (
	WallSourceStructured WallSource = "structured" // roster quota_reset
	WallSourceText       WallSource = "text"       // provider text, parsed in the provider zone
	WallSourceHarness    WallSource = "harness"    // harness-provided wall_wake_at, kept as is
	WallSourceProbe      WallSource = "probe"      // no reset known: 15/30/60-minute ladder
)

// wallInputs carries everything one wall's wake resolution may read.
type wallInputs struct {
	Pool         string     // roster pool of the walled seat; "" if unknown
	ErrorText    string     // execution.json "error"
	RosterReset  *time.Time // roster QuotaReset of the walled seat; nil if none or unknown
	HarnessWake  time.Time  // execution.json "wall_wake_at"
	HarnessProbe bool       // execution.json "wall_source" == "probe"
	PriorProbes  int        // settled probe walls the pool already spent
	SettleAt     time.Time  // engine clock at resolution
}

// wallLookback bounds how far pool walls and probe ladders read back. Weekly
// windows are seven days; eight keeps a boundary run inside the window.
const wallLookback = 8 * 24 * time.Hour

// providerZone reads the pool's plan zone. Z.ai prints its quota reset in
// UTC+8 whatever the server clock reads; every other pool, and a pool unknown
// to the roster, reads as UTC.
func providerZone(pool string) *time.Location {
	if strings.HasPrefix(pool, "zai") {
		return time.FixedZone("UTC+8", 8*3600)
	}
	return time.UTC
}

// providerResetLayouts are the instant shapes the providers print after
// "reset at "; RFC 3339 keeps its own offset, the bare shapes read in the
// pool's zone.
var providerResetLayouts = []string{time.RFC3339Nano, "2006-01-02 15:04:05", "2006-01-02 15:04"}

// parseProviderReset reads the reset instant out of the provider's failure
// text, in the pool's own zone.
func parseProviderReset(pool, errorText string) (time.Time, bool) {
	marker := "reset at "
	lower := strings.ToLower(errorText)
	index := strings.Index(lower, marker)
	if index < 0 {
		return time.Time{}, false
	}
	text := strings.TrimSpace(errorText[index+len(marker):])
	zone := providerZone(pool)
	for _, layout := range providerResetLayouts {
		if parsed, err := time.ParseInLocation(layout, text, zone); err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}

// probeWallLadder steps an unparseable wall through 15, 30 and 60 minute
// re-probes; 60 minutes is the ceiling.
func probeWallLadder(prior int) time.Duration {
	switch {
	case prior <= 0:
		return 15 * time.Minute
	case prior == 1:
		return 30 * time.Minute
	default:
		return 60 * time.Minute
	}
}

// resolveWallWake applies the strict priority: the roster's own reset reading,
// then the provider text in the provider's zone, then the harness wake as it
// was written, then the probe ladder. It always names a source.
func resolveWallWake(in wallInputs) (time.Time, WallSource) {
	// 1. structured: the roster's collector read the epoch reset. A past
	// reading has no authority over a wall settled after it.
	if in.RosterReset != nil && in.SettleAt.Before(*in.RosterReset) {
		return in.RosterReset.UTC(), WallSourceStructured
	}
	// 2. text: the provider's own reset line, read in the provider's zone.
	if wake, ok := parseProviderReset(in.Pool, in.ErrorText); ok {
		return wake.UTC(), WallSourceText
	}
	// 3. harness: the boundary's wake, kept as given.
	if !in.HarnessProbe {
		return in.HarnessWake.UTC(), WallSourceHarness
	}
	// 4. probe: no reset is known; re-probe on the ladder.
	return in.SettleAt.Add(probeWallLadder(in.PriorProbes)).UTC(), WallSourceProbe
}

// legacyWallLift re-reads a wall settled before wakes carried a source: the
// provider text in the pool's zone when it parses, else the stored value. It
// reads no clock and no roster, so every read of the same row agrees.
func legacyWallLift(pool, errorText string, stored time.Time) time.Time {
	if wake, ok := parseProviderReset(pool, errorText); ok {
		return wake.UTC()
	}
	return stored
}

// wallPayload is the settled event payload a wall wrote.
type wallPayload struct {
	Pool       string `json:"pool"`
	Seat       string `json:"seat"`
	WallWakeAt string `json:"wall_wake_at"`
	WallSource string `json:"wall_source"`
	Error      string `json:"error"`
}

// wallPoolOf names the pool a settled wall row spends: the payload's own pool
// when it carries one, else the current roster's pool for the seat. A seat
// with no pool is unmetered and never walls.
func wallPoolOf(payload wallPayload, seat string, rosterPool map[string]string) string {
	if payload.Pool != "" {
		return payload.Pool
	}
	return rosterPool[seat]
}

// poolWalls derives, from SQLite and the current roster alone, when each plan
// pool's latest quota wall lifts. Every task's walls count, so one task's
// wall benches the whole pool. Per pool the newest-ended settled wall decides
// the lift: its stored wake when the row carries a source, else the legacy
// lift re-read from the provider text. Only lifts still in the future come
// back.
func (s *Store) poolWalls(ctx context.Context, seats []Seat, now time.Time) (map[string]time.Time, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT r.seat, r.ended_at, e.payload_json FROM task_events e
		JOIN runs r ON r.id = e.run_id
		WHERE e.kind='settled' AND r.outcome='wall' AND r.seat <> '' AND r.ended_at >= ?`,
		stamp(now.Add(-wallLookback)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	rosterPool := make(map[string]string, len(seats))
	for _, seat := range seats {
		rosterPool[seat.Name] = seat.Pool
	}
	lifts := make(map[string]time.Time)
	ended := make(map[string]time.Time)
	for rows.Next() {
		var seat, endedAt, payload string
		if err := rows.Scan(&seat, &endedAt, &payload); err != nil {
			return nil, err
		}
		var audit wallPayload
		if json.Unmarshal([]byte(payload), &audit) != nil {
			continue // a torn payload decides nothing
		}
		pool := wallPoolOf(audit, seat, rosterPool)
		if pool == "" {
			continue
		}
		rowEnded, err := time.Parse(stampLayout, endedAt)
		if err != nil {
			continue
		}
		if prior, exists := ended[pool]; exists && !rowEnded.After(prior) {
			continue // an older row of the pool never outranks a newer one
		}
		if audit.WallWakeAt == "" {
			continue
		}
		stored, err := time.Parse(time.RFC3339Nano, audit.WallWakeAt)
		if err != nil {
			continue
		}
		var lift time.Time
		if audit.WallSource != "" {
			lift = stored
		} else {
			lift = legacyWallLift(pool, audit.Error, stored)
		}
		ended[pool] = rowEnded
		lifts[pool] = lift
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for pool, lift := range lifts {
		if !lift.After(now) {
			delete(lifts, pool)
		}
	}
	return lifts, nil
}

// priorProbeWalls counts the settled probe walls a pool spent since its last
// productive run, so the uncharged re-probe ladder climbs 15, 30, then 60
// minutes. Only the pool's own rows count: a probe another pool spent never
// moves this pool's ladder. Every task counts. On a query error the count is 0.
func (s *Store) priorProbeWalls(ctx context.Context, pool string, seats []Seat) (int, error) {
	if pool == "" {
		return 0, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT r.seat, r.outcome, r.ended_at, e.payload_json FROM task_events e
		JOIN runs r ON r.id = e.run_id
		WHERE e.kind='settled' AND r.ended_at IS NOT NULL AND r.seat <> '' AND r.ended_at >= ?`,
		stamp(s.now().UTC().Add(-wallLookback)))
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	rosterPool := make(map[string]string, len(seats))
	for _, seat := range seats {
		rosterPool[seat.Name] = seat.Pool
	}
	var boundary time.Time
	probes := make([]time.Time, 0, 4)
	for rows.Next() {
		var seat, outcome, endedAt, payload string
		if err := rows.Scan(&seat, &outcome, &endedAt, &payload); err != nil {
			return 0, err
		}
		rowEnded, err := time.Parse(stampLayout, endedAt)
		if err != nil {
			continue
		}
		probe := false
		rowPool := ""
		if RunOutcome(outcome) == RunWall {
			var audit wallPayload
			if json.Unmarshal([]byte(payload), &audit) != nil {
				continue
			}
			rowPool = wallPoolOf(audit, seat, rosterPool)
			probe = audit.WallSource == string(WallSourceProbe)
		} else {
			rowPool = rosterPool[seat]
		}
		if rowPool != pool {
			continue // another pool's rows never move this pool's ladder
		}
		if probe {
			probes = append(probes, rowEnded)
		}
		if RunOutcome(outcome) != RunWall && rowEnded.After(boundary) {
			boundary = rowEnded
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	count := 0
	for _, probe := range probes {
		if probe.After(boundary) {
			count++
		}
	}
	return count, nil
}
