package finite

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/agent0ai/spynel/internal/harness"
)

// Failure classes are the single settlement-side vocabulary that decides, for
// every settlement, the stored run outcome, whether the run is charged, and
// the retry policy. The chosen class is recorded on the settled task event.
const (
	failureResult         = "result"
	failureProviderFail   = "provider-fail"
	failureTimeout        = "timeout"
	failureSupervisorDied = "supervisor-died"
	failureHermesNoSpend  = "hermes-no-spend"
	failureQuotaWall      = "quota-wall"
	failureLost           = "lost"
	failureInterrupted    = "interrupted"
	failureCancelled      = "cancelled"
)

type retryPolicy int

const (
	retryNow retryPolicy = iota
	retryBackoff
	retryProviderReset
	retryByResult
)

type failureClassRow struct {
	Outcome RunOutcome
	Charged bool
	Retry   retryPolicy
}

// failureClassTable is the one failure-class table. Every row's Charged
// matches its stored outcome's Charged(), so runs.outcome alone keeps
// derivedAccounting and the charged_attempts view correct.
var failureClassTable = map[string]failureClassRow{
	failureResult:         {Outcome: RunOK, Charged: true, Retry: retryByResult},
	failureProviderFail:   {Outcome: RunFail, Charged: true, Retry: retryNow},
	failureTimeout:        {Outcome: RunTimeout, Charged: true, Retry: retryNow},
	failureSupervisorDied: {Outcome: RunFail, Charged: true, Retry: retryNow},
	failureHermesNoSpend:  {Outcome: RunInterrupted, Charged: false, Retry: retryBackoff},
	failureQuotaWall:      {Outcome: RunWall, Charged: false, Retry: retryProviderReset},
	failureLost:           {Outcome: RunLost, Charged: false, Retry: retryNow},
	failureInterrupted:    {Outcome: RunInterrupted, Charged: false, Retry: retryNow},
	failureCancelled:      {Outcome: RunInterrupted, Charged: false, Retry: retryNow},
}

// classifySettlement applies the settlement precedence: a cancel ends the task
// as cancelled, a valid result is the run's own verdict, an explicit class is
// used as is, and anything else follows the execution outcome. The returned
// outcome is always the class row's stored outcome.
func classifySettlement(task Task, result *Result, requested string, executionOutcome RunOutcome) (string, RunOutcome, error) {
	class := ""
	switch {
	case task.CancelRequested:
		class = failureCancelled
	case result != nil:
		class = failureResult
	case requested != "":
		if _, ok := failureClassTable[requested]; !ok {
			return "", "", fmt.Errorf("finite: unknown failure class %q", requested)
		}
		class = requested
	default:
		switch executionOutcome {
		case "", RunOK:
			class = failureResult
		case RunFail:
			class = failureProviderFail
		case RunTimeout:
			class = failureTimeout
		case RunWall:
			class = failureQuotaWall
		case RunLost:
			class = failureLost
		case RunInterrupted:
			class = failureInterrupted
		default:
			return "", "", fmt.Errorf("finite: invalid execution outcome %q", executionOutcome)
		}
	}
	return class, failureClassTable[class].Outcome, nil
}

// noTokenSpend reports that a usage record exists and proves no spend: every
// one of the seven counters is null or zero. A missing usage record is not
// decided here; it goes through the exact Hermes admission check instead.
func noTokenSpend(u *Usage) bool {
	if u == nil {
		return false
	}
	spent := func(value *int64) bool { return value != nil && *value != 0 }
	return !spent(u.FreshInputTokens) && !spent(u.CacheReadTokens) && !spent(u.CacheWriteTokens) &&
		!spent(u.OutputTokens) && !spent(u.ReasoningTokens) && !spent(u.APICalls) && !spent(u.ToolCalls)
}

// backoffDelay is the durable retry backoff for a no-spend Hermes failure:
// 30 s doubled per consecutive no-spend settlement, bounded at 10 min.
func backoffDelay(k int) time.Duration {
	const base = 30 * time.Second
	const max = 10 * time.Minute
	delay := base
	for i := 1; i < k; i++ {
		delay *= 2
		if delay >= max {
			return max
		}
	}
	return delay
}

// hermesNoSpendStreak counts, newest first, the consecutive hermes-no-spend
// settlements already recorded for the task and phase. The first settlement of
// any other class — or one written before classes existed, whose payload has
// no failure_class — stops the count.
func hermesNoSpendStreak(ctx context.Context, tx txRunner, taskID string, phase Phase) (int, error) {
	rows, err := tx.QueryContext(ctx, `SELECT json_extract(e.payload_json,'$.failure_class') FROM task_events e
		JOIN runs r ON r.id=e.run_id WHERE e.task_id=? AND r.phase=? AND e.kind='settled' ORDER BY r.id DESC`,
		taskID, phase)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	streak := 0
	for rows.Next() {
		var class *string
		if err := rows.Scan(&class); err != nil {
			return 0, err
		}
		if class == nil || *class != failureHermesNoSpend {
			break
		}
		streak++
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return streak, nil
}

func firstLine(text string) string {
	line, _, _ := strings.Cut(text, "\n")
	return line
}

// hermesPromptAdmitted reports whether the disposable Hermes gateway admitted
// this run's exact prompt. It is a package seam so tests can pin the answer;
// production uses defaultHermesPromptAdmitted.
var hermesPromptAdmitted = defaultHermesPromptAdmitted

// defaultHermesPromptAdmitted mirrors inspectFiniteHermesRecovery's two
// boundary routes: with hermes-provider.json the exact boundary observation
// carries the admission identity; without it the ledger is queried directly.
// A missing admission is proven absence; any other answer is an error the
// caller must treat as unproven (and therefore charged).
func defaultHermesPromptAdmitted(ctx context.Context, run Run) (bool, error) {
	spec := harness.RunBoundarySpec{RunID: run.ID, ProcessToken: run.ProcessToken, RunDir: run.RunDir}
	_, err := os.Lstat(filepath.Join(run.RunDir, "hermes-provider.json"))
	if err == nil {
		observation, err := harness.InspectHermesRunBoundary(ctx, spec)
		if err != nil {
			return false, err
		}
		return observation.Identity != nil, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	return harness.HermesRunHasAdmission(ctx, spec)
}
