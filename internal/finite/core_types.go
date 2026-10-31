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
	Body            string
}

// Accounting is derived from execution records, never stored on a task.
type Accounting struct {
	Attempts     int
	Failures     int
	ReviewCycles int
}

// Seat describes one provider seat at the instant of a pure admission decision.
type Seat struct {
	Name       string
	Available  bool
	QuotaReset *time.Time
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
	Error               string
}

// PromptRequest separates the static provider bundle from dynamic turn data.
type PromptRequest struct {
	TaskID             string
	RunID              int64
	Phase              Phase
	Domains            []string
	Dynamic            string
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
