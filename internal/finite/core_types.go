package finite

import (
	"context"
	"time"
)

// Brief is the closed metadata and Markdown body accepted at intake.
type Brief struct {
	ID              string
	Title           string
	Risk            string
	ReviewRequired  bool
	MaxRuns         int
	MaxFailures     int
	MaxReviewCycles int
	Staff           string
	ReviewStaff     string
	Body            string
	// AcceptanceCheck is the optional declared acceptance check. The engine
	// runs it outside the worker's run and records a shadow verdict; it never
	// appears in a prompt.
	AcceptanceCheck *AcceptanceCheck
}

// AcceptanceCheck is the command a brief declares as its acceptance check.
// Command appears in prompts, so it must hold no secret.
type AcceptanceCheck struct {
	Command        string
	Repo           string
	TimeoutSeconds int
}

// JoeyAsk is the closed human-question contract used by a Joey wait.
type JoeyAsk struct {
	Question string          `json:"question"`
	Options  []JoeyAskOption `json:"options"`
}

// JoeyAskOption is one stable answer choice.
type JoeyAskOption struct {
	ID    string `json:"id"`
	Label string `json:"label"`
}

// Accounting is derived from execution records, never stored on a task.
// Attempts and Failures count charged provider turns only; Runs counts every
// run of the phase whatever its outcome and is what MaxRuns bounds.
type Accounting struct {
	Attempts     int
	Failures     int
	ReviewCycles int
	Runs         int
}

// Seat describes one provider seat at the instant of a pure admission decision.
type Seat struct {
	Name       string
	Available  bool
	QuotaReset *time.Time
	// Fallback names the next seat of the chain when this one cannot run
	// (roster mode only); empty ends the chain.
	Fallback string
	// Pool labels the metered plan a seat spends, so a quota wall can bench
	// every seat of the same pool. Empty means unmetered. The engine never
	// maps providers to plans; the availability roster carries the label.
	Pool string
	// AvailableUntil is the instant the roster reading that marked this seat
	// available goes stale. At or after that instant the engine reads the
	// seat unavailable, so a roster that stops being rewritten cannot keep
	// an open seat open. Nil means the seat never expires.
	AvailableUntil *time.Time
}

// AdmissionAction is the pure admission result vocabulary.
type AdmissionAction string

const (
	AdmissionDispatch AdmissionAction = "dispatch"
	AdmissionWait     AdmissionAction = "wait"
	AdmissionFail     AdmissionAction = "fail"
)

// AdmissionInput contains every input to an admission decision.
type AdmissionInput struct {
	Task       Task
	Phase      Phase
	Accounting Accounting
	Seats      []Seat
	// RosterSeats reports that Seats came from a fallback roster; admission
	// then walks the chain from StartSeat instead of taking any seat.
	RosterSeats bool
	// StartSeat is the walk origin: the brief's staff (review staff in review),
	// else the engine default seat.
	StartSeat string
	// WalledPools are the plan pools this task's own quota-wall runs still
	// wall; a seat of such a pool is unavailable even if the roster disagrees.
	WalledPools map[string]struct{}
}

// AdmissionDecision selects a seat or explains why no run may start.
type AdmissionDecision struct {
	Action AdmissionAction
	Seat   string
	Reason string
	WakeAt *time.Time
}

// DispatchRequest fences creation of one immutable provider run.
type DispatchRequest struct {
	TaskID          string
	ExpectedVersion int64
	Phase           Phase
	Seat            string
	SessionKey      string
	LaunchReason    string
	PromptHash      string
}

// LaunchFunc starts supervision only after the dispatch transaction commits.
type LaunchFunc func(ctx context.Context, run Run) error

// SettlementRequest converges normal, invalid, failed, recovered, and stopped runs.
type SettlementRequest struct {
	RunID               int64
	ExpectedTaskVersion int64
	ExecutionOutcome    RunOutcome
	ExitCode            *int
	Result              *Result
	ResultError         error
	Usage               *Usage
	WallWakeAt          *time.Time
	Error               string
}

// PromptRequest separates the static provider bundle from dynamic turn data.
type PromptRequest struct {
	TaskID             string
	RunID              int64
	Phase              Phase
	Domains            []string
	Dynamic            string
	RunDir             string
	PreviousBundleHash string
	ResetContext       bool
	UncertainContext   bool
}

// Prompt is a rendered turn and the hash of its static bundle.
type Prompt struct {
	Text       string
	BundleHash string
	FullBundle bool
}

// UsageConvention declares whether adapter samples are totals or increments.
type UsageConvention string

const (
	UsageCumulative UsageConvention = "cumulative"
	UsageDelta      UsageConvention = "delta"
)

// UsageSample binds one adapter report to a run and one convention.
type UsageSample struct {
	RunID      int64
	Convention UsageConvention
	Value      Usage
}

// UsageAccumulator normalizes one run without mixing sample conventions.
type UsageAccumulator struct {
	runID      int64
	convention UsageConvention
	total      Usage
}

// OutboxEntry is the durable delivery record produced by settlement.
type OutboxEntry struct {
	DeliveryID  string
	EventID     string
	Channel     string
	Kind        string
	PayloadJSON string
	CreatedAt   time.Time
	AvailableAt time.Time
	SendCount   int
	DeliveredAt *time.Time
	LastError   string
}
