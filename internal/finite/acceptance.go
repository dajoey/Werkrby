package finite

import (
	"context"
	"strconv"
)

// acceptanceEventID is the deterministic event id of one run's shadow
// acceptance verdict, so the once-per-run record is idempotent.
func acceptanceEventID(runID int64) string {
	return "acceptance:" + strconv.FormatInt(runID, 10)
}

// startAcceptance runs one declared acceptance check detached from the
// worker's run and records its verdict. Scaffolding stub: no behavior yet.
func (e *Engine) startAcceptance(taskID string, runID int64, check AcceptanceCheck) {}

// cleanupAcceptanceLeftovers removes state a crashed acceptance check left
// behind. Scaffolding stub: no behavior yet.
func cleanupAcceptanceLeftovers(ctx context.Context, root string) error { return nil }
