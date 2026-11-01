package finite

import (
	"encoding/json"
	"time"
)

// State is the authoritative task workflow state.
type State string

const (
	StateTodo      State = "todo"
	StateWorking   State = "working"
	StateReview    State = "review"
	StateReviewing State = "reviewing"
	StateWaiting   State = "waiting"
	StateDone      State = "done"
	StateFailed    State = "failed"
	StateCancelled State = "cancelled"
)

// Phase identifies the independent provider role for a run.
type Phase string

const (
	PhaseImplementation Phase = "implementation"
	PhaseReview         Phase = "review"
)

// RunOutcome describes execution, never the task transition.
type RunOutcome string

const (
	RunOK          RunOutcome = "ok"
	RunFail        RunOutcome = "fail"
	RunTimeout     RunOutcome = "timeout"
	RunLost        RunOutcome = "lost"
	RunInterrupted RunOutcome = "interrupted"
	RunWall        RunOutcome = "wall"
)

// Charged reports whether a finished provider turn counts as an attempt.
func (o RunOutcome) Charged() bool { return o == RunOK || o == RunFail || o == RunTimeout }

// ResultOutcome is the closed worker result vocabulary.
type ResultOutcome string

const (
	ResultDone     ResultOutcome = "done"
	ResultReview   ResultOutcome = "review"
	ResultTodo     ResultOutcome = "todo"
	ResultWait     ResultOutcome = "wait"
	ResultFailed   ResultOutcome = "failed"
	ResultAccepted ResultOutcome = "accepted"
	ResultRejected ResultOutcome = "rejected"
)

// WaitKind distinguishes a clock wait from a human-answer wait.
type WaitKind string

const (
	WaitTime WaitKind = "time"
	WaitJoey WaitKind = "joey"
)

// Task is the SQLite-backed workflow projection used by the engine.
type Task struct {
	ID              string
	Title           string
	State           State
	Risk            string
	ReviewRequired  bool
	CancelRequested bool
	MaxRuns         int
	MaxFailures     int
	MaxReviewCycles int
	BriefPath       string
	BriefSHA256     string
	ProjectionPath  string
	ResumeState     State
	WaitKind        WaitKind
	WakeAt          *time.Time
	WaitingFor      string
	JoeyAskJSON     string
	Version         int64
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// Run is one provider turn. PID is recovery metadata, never proof by itself.
type Run struct {
	ID            int64
	TaskID        string
	Phase         Phase
	TaskVersion   int64
	Seat          string
	SessionKey    string
	LaunchReason  string
	PromptHash    string
	RunDir        string
	SupervisorPID *int
	ProcessToken  string
	CreatedAt     time.Time
	StartedAt     *time.Time
	EndedAt       *time.Time
	ExitCode      *int
	Outcome       RunOutcome // empty while open
	Error         string
}

// Summary carries bounded worker evidence rather than arbitrary front matter.
type Summary struct {
	Outcome     string `json:"outcome"`
	Evidence    string `json:"evidence"`
	Uncertainty string `json:"uncertainty"`
}

// Wait is the typed waiting instruction in a worker result.
type Wait struct {
	Kind   WaitKind        `json:"kind"`
	WakeAt string          `json:"wake_at,omitempty"`
	Reason string          `json:"reason"`
	Ask    json.RawMessage `json:"ask,omitempty"`
}

// Result is bound to exactly one task, run, and phase.
type Result struct {
	SchemaVersion int           `json:"schema_version"`
	RunID         int64         `json:"run_id"`
	TaskID        string        `json:"task_id"`
	Phase         Phase         `json:"phase"`
	Outcome       ResultOutcome `json:"outcome"`
	Progress      string        `json:"progress"`
	Summary       *Summary      `json:"summary,omitempty"`
	Findings      []string      `json:"findings,omitempty"`
	Wait          *Wait         `json:"wait,omitempty"`
}

// Usage is adapter-normalized telemetry. Nil means unreported, never zero.
type Usage struct {
	FreshInputTokens *int64
	CacheReadTokens  *int64
	CacheWriteTokens *int64
	OutputTokens     *int64
	ReasoningTokens  *int64
	APICalls         *int64
	ToolCalls        *int64
	Source           string
	CollectedAt      time.Time
}

// Config identifies an isolated finite state root and its worker target.
type Config struct {
	Root        string
	WorkerDir   string
	ControlAddr string
	RosterPath  string
}
