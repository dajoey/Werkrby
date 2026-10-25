package harness

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/fsx"
	"github.com/agent0ai/spynel/internal/history"
	"github.com/agent0ai/spynel/internal/roster"
)

// Herdr adapts the Herdr terminal workspace manager as a coding harness.
// It can route tasks to warm existing agent panes (Claude, Kimi, Hermes, OpenCode, Codex)
// or dynamically spawn and supervise new agent panes inside Herdr.
type Herdr struct {
	config HarnessConfig

	keyMu    sync.Mutex
	keyLocks map[string]*sync.Mutex
	wsMu     sync.Mutex
	mu       sync.Mutex
	sessions map[string]string
	active   map[string]*herdrTurn
	closed   bool

	idleStabilityCount    int
	idleStabilityInterval time.Duration
	composerWaitTimeout   time.Duration
	// headlessCommand is the Claude Code executable for `headless: true` staff (default "claude").
	headlessCommand string
	// headlessKimiCommand is the Kimi Code executable for `headless: true` kimi staff (default:
	// kimiExecutable).
	headlessKimiCommand string
	// headlessAgyCommand is the agy executable for `headless: true` agy staff (default: agyExecutable).
	headlessAgyCommand string

	cliChatIdleThreshold time.Duration

	// hermesHome overrides the Hermes home whose profile ledgers judge hermes turns (tests);
	// empty means HERMES_HOME, else ~/.hermes.
	hermesHome string
	// hermesLedgerSettle bounds the re-reads of a delivered Hermes turn whose answer row has not
	// landed yet; zero means defaultHermesLedgerSettle, negative means no re-read.
	hermesLedgerSettle time.Duration
	// hermesBlockedWait bounds how long a turn waits on a Hermes worker pane herdr reports blocked
	// (zero means defaultHermesBlockedWait); hermesBlockedPoll is the re-read interval (zero means
	// defaultHermesBlockedPoll).
	hermesBlockedWait time.Duration
	hermesBlockedPoll time.Duration

	// submitted is the last prompt herdr accepted for each worker and when it was sent, so a turn
	// that finds the worker still running that prompt's turn can judge it from the ledger. A
	// successful turn forgets its worker. Guarded by mu.
	submitted map[string]hermesSubmitted
}

// hermesSubmitted is a prompt herdr accepted for a worker, and when it was sent.
type hermesSubmitted struct {
	prompt string
	since  time.Time
}

// maxRememberedSubmissions bounds Herdr.submitted: past it the oldest entry is dropped.
const maxRememberedSubmissions = 64

const (
	// defaultHermesBlockedWait is how long a turn waits on a blocked Hermes worker pane before it
	// errors: the prompt watcher (cron, every minute) answers a pane blocked on two consecutive
	// runs, so a pane it can clear clears well inside this.
	defaultHermesBlockedWait = 5 * time.Minute
	defaultHermesBlockedPoll = 5 * time.Second
)

// HermesBlockedCode marks the error a Hermes turn returns when its worker pane stayed blocked on a
// prompt past the wait. It is a harness condition, not a task failure; the orchestrator matches
// this code and does not count the error toward a lease's recovery fail-out.
const HermesBlockedCode = "hermes_blocked"

// hermesBlockedError is a Hermes worker pane still blocked when the wait ran out.
type hermesBlockedError struct {
	target string
	waited time.Duration
}

func (e *hermesBlockedError) Error() string {
	return fmt.Sprintf("herdr agent %s blocked: %s: the hermes worker pane stayed blocked on a prompt for %s", e.target, HermesBlockedCode, e.waited.Round(time.Second))
}

type herdrTurn struct {
	target    string
	paneID    string
	tabID     string
	isDynamic bool
	cmd       *exec.Cmd
	cancel    context.CancelFunc
	done      chan struct{}
	// headless marks a `claude -p` turn (sendHeadless): there is no pane to type into or close.
	headless bool
	// notSent: the prompt was never typed, because the worker's earlier turn moved the claimed
	// document out of its folder while this turn waited on the blocked pane. earlier is that
	// turn's prompt when this process sent it, so the ledger can judge it.
	notSent       bool
	movedDocument string
	earlier       *hermesSubmitted
}

type herdrAgentItem struct {
	Name          string `json:"name"`
	Agent         string `json:"agent"`
	AgentStatus   string `json:"agent_status"`
	PaneID        string `json:"pane_id"`
	TabID         string `json:"tab_id"`
	WorkspaceID   string `json:"workspace_id"`
	Cwd           string `json:"cwd"`
	TerminalTitle string `json:"terminal_title_stripped"`
	AgentSession  struct {
		Value string `json:"value"`
	} `json:"agent_session"`
}

type herdrAgentListResponse struct {
	Result struct {
		Agents []herdrAgentItem `json:"agents"`
	} `json:"result"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

type herdrAgentGetResponse struct {
	Result struct {
		Agent herdrAgentItem `json:"agent"`
	} `json:"result"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

type herdrTabCreateResponse struct {
	Result struct {
		RootPane struct {
			PaneID string `json:"pane_id"`
		} `json:"root_pane"`
		Tab struct {
			TabID string `json:"tab_id"`
		} `json:"tab"`
	} `json:"result"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

type herdrPaneSplitResponse struct {
	Result struct {
		Pane struct {
			PaneID string `json:"pane_id"`
		} `json:"pane"`
	} `json:"result"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func NewHerdr(cfg HarnessConfig) (*Herdr, error) {
	if cfg.Command == "" {
		cfg.Command = "herdr"
	}
	if cfg.Cwd == "" {
		cfg.Cwd = "."
	}
	idleThreshold := 6 * time.Hour
	if cfg.CLIChatIdleThresholdSet {
		idleThreshold = cfg.CLIChatIdleThreshold
	} else if cfg.CLIChatIdleThreshold != 0 {
		idleThreshold = cfg.CLIChatIdleThreshold
	}
	h := &Herdr{
		config:                cfg,
		keyLocks:              map[string]*sync.Mutex{},
		sessions:              map[string]string{},
		active:                map[string]*herdrTurn{},
		idleStabilityCount:    3,
		idleStabilityInterval: 2 * time.Second,
		cliChatIdleThreshold:  idleThreshold,
	}
	_ = h.loadSessions()
	return h, nil
}

// SetIdleStability configures the number of consecutive idle readings required
// and the interval between checks before considering a worker turn complete.
func (h *Herdr) SetIdleStability(count int, interval time.Duration) {
	h.idleStabilityCount = count
	h.idleStabilityInterval = interval
}

// SetComposerWaitTimeout configures the maximum time to wait for a runner's
// composer to become visible after launch or when recovering from an undelivered prompt.
func (h *Herdr) SetComposerWaitTimeout(d time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.composerWaitTimeout = d
}

func (h *Herdr) Start(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return errors.New("Herdr harness is closed")
	}
	if _, err := exec.LookPath(h.config.Command); err != nil {
		return fmt.Errorf("herdr command %q not found in PATH: %w", h.config.Command, err)
	}
	// Check Herdr server status
	cmd := exec.Command(h.config.Command, "status")
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "status: running") {
		return fmt.Errorf("herdr server is not running (output: %s)", strings.TrimSpace(string(out)))
	}
	go h.sweepOrphanedWorkers(ctx)
	if h.cliChatIdleThreshold > 0 {
		go h.runCLIChatIdleSweepLoop(ctx)
	}
	return nil
}

func (h *Herdr) Available() (bool, string) {
	if _, err := exec.LookPath(h.config.Command); err != nil {
		return false, "herdr binary not found in PATH"
	}
	cmd := exec.Command(h.config.Command, "status")
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "status: running") {
		return false, "herdr daemon is not running"
	}
	return true, "Herdr daemon running"
}

func (h *Herdr) ReadyEvents() <-chan struct{} {
	ch := make(chan struct{}, 1)
	ch <- struct{}{}
	return ch
}

func (h *Herdr) Models(_ context.Context) ([]Model, error) {
	allEfforts := []string{"low", "medium", "high"}
	return []Model{
		{ID: "hermes", DisplayName: "Hermes", Description: "Dedicated Hermes worker in Herdr pane (default)", Default: true, Efforts: allEfforts},
		{ID: "opencode", DisplayName: "OpenCode", Description: "Dedicated OpenCode worker in Herdr pane", Efforts: allEfforts},
		{ID: "claude", DisplayName: "Claude Code", Description: "Dedicated Claude Code worker in Herdr pane", Efforts: allEfforts},
		{ID: "codex", DisplayName: "Codex", Description: "Dedicated Codex worker in Herdr pane", Efforts: allEfforts},
		{ID: "kimi", DisplayName: "Kimi", Description: "Dedicated Kimi CLI worker in Herdr pane", Efforts: allEfforts},
		{ID: "agy", DisplayName: "Antigravity", Description: "Dedicated Antigravity CLI worker in Herdr pane", Efforts: allEfforts},
		{ID: "pi", DisplayName: "Pi", Description: "Dedicated Pi worker in Herdr pane", Efforts: allEfforts},
		{ID: "cursor", DisplayName: "Cursor", Description: "Dedicated Cursor worker in Herdr pane", Efforts: allEfforts},
	}, nil
}

func (h *Herdr) SetModel(model string) {
	h.mu.Lock()
	h.config.Model = model
	h.mu.Unlock()
}

func (h *Herdr) SendWithModel(ctx context.Context, key, prompt, model string, emit core.Emit) (string, bool, error) {
	h.mu.Lock()
	cfg := h.config
	cfg.Model = model
	h.mu.Unlock()
	return h.sendInternal(ctx, key, prompt, cfg, emit)
}

func (h *Herdr) Send(ctx context.Context, key, prompt string, emit core.Emit) (string, bool, error) {
	h.mu.Lock()
	cfg := h.config
	h.mu.Unlock()
	return h.sendInternal(ctx, key, prompt, cfg, emit)
}

func (h *Herdr) lockForKey(key string) *sync.Mutex {
	h.keyMu.Lock()
	defer h.keyMu.Unlock()
	lock, ok := h.keyLocks[key]
	if !ok {
		lock = &sync.Mutex{}
		h.keyLocks[key] = lock
	}
	return lock
}

func (h *Herdr) resolveCallerContext(ctx context.Context, cwd string) (string, string) {
	callerPane := os.Getenv("HERDR_PANE_ID")
	workspaceID := os.Getenv("HERDR_WORKSPACE_ID")

	if callerPane != "" && workspaceID != "" {
		return callerPane, workspaceID
	}

	out, err := exec.CommandContext(ctx, h.config.Command, "pane", "current").Output()
	if err == nil {
		var resp struct {
			Result struct {
				Pane struct {
					PaneID      string `json:"pane_id"`
					WorkspaceID string `json:"workspace_id"`
				} `json:"pane"`
			} `json:"result"`
		}
		if json.Unmarshal(out, &resp) == nil && resp.Result.Pane.PaneID != "" {
			if callerPane == "" {
				callerPane = resp.Result.Pane.PaneID
			}
			if workspaceID == "" {
				workspaceID = resp.Result.Pane.WorkspaceID
			}
		}
	}

	if workspaceID == "" {
		if outWorkspaces, err := exec.CommandContext(ctx, h.config.Command, "workspace", "list").Output(); err == nil {
			var wResp struct {
				Result struct {
					Workspaces []struct {
						WorkspaceID string `json:"workspace_id"`
						Label       string `json:"label"`
					} `json:"workspaces"`
				} `json:"result"`
			}
			if json.Unmarshal(outWorkspaces, &wResp) == nil {
				for _, ws := range wResp.Result.Workspaces {
					if isWorkerWorkspaceLabel(ws.Label) {
						workspaceID = ws.WorkspaceID
						break
					}
				}
				if workspaceID == "" && len(wResp.Result.Workspaces) > 0 {
					workspaceID = wResp.Result.Workspaces[0].WorkspaceID
				}
			}
		}
	}

	if workspaceID == "" {
		workspaceID = "w6"
	}
	return callerPane, workspaceID
}

func workerSubject(key string) string {
	cleanKey := strings.ToLower(strings.TrimSpace(key))
	if strings.Contains(cleanKey, "helm") {
		return "helm"
	}
	if strings.HasPrefix(cleanKey, "orchestrator:") || cleanKey == "heartbeat" {
		parts := strings.Split(cleanKey, ":")
		if len(parts) >= 4 {
			id := parts[3]
			if strings.Contains(id, "helm") {
				return "helm"
			}
		}
		return "tasks"
	}
	if strings.HasPrefix(cleanKey, "chat:") {
		parts := strings.Split(cleanKey, ":")
		if len(parts) >= 2 && parts[1] != "" {
			return parts[1]
		}
		return "chat"
	}
	return "tasks"
}

func workerWorkspaceLabel(subject string) string {
	subject = strings.TrimSpace(strings.ToLower(subject))
	if subject == "" {
		subject = "tasks"
	}
	return fmt.Sprintf("[wk-worker] %s", subject)
}

func isWorkerWorkspaceLabel(label string) bool {
	lower := strings.ToLower(label)
	return strings.Contains(lower, "[sj-worker]") || strings.Contains(lower, "[wk-worker]")
}

// workerLabelSubjectPrefixes lists both workspace-label forms that address a
// dedicated worker workspace: the legacy pre-rename "[sj-worker]" form and the
// current "[wk-worker]" form.
var workerLabelSubjectPrefixes = []string{"[sj-worker] ", "[wk-worker] "}

// workspaceLabelMatchesSubject reports whether a dedicated worker workspace
// label addresses the given subject under either label form, so workspaces
// labeled before the wk- writer switch keep resolving instead of duplicating.
func workspaceLabelMatchesSubject(label, subject string) bool {
	subject = strings.TrimSpace(strings.ToLower(subject))
	if subject == "" {
		subject = "tasks"
	}
	lower := strings.ToLower(strings.TrimSpace(label))
	for _, prefix := range workerLabelSubjectPrefixes {
		if strings.HasPrefix(lower, prefix) {
			return strings.EqualFold(strings.TrimSpace(strings.TrimPrefix(lower, prefix)), subject)
		}
	}
	return false
}

func hasWorkerPrefix(name string) bool {
	return strings.HasPrefix(name, "sj-") || strings.HasPrefix(name, "wk-") || strings.HasPrefix(name, "spyjo-")
}

func isOrphanWorkerName(name string) bool {
	return strings.HasPrefix(name, "sj-t-") ||
		strings.HasPrefix(name, "sj-r-") ||
		strings.HasPrefix(name, "sj-p-") ||
		strings.HasPrefix(name, "sj-o-") ||
		strings.HasPrefix(name, "sj-hb-") ||
		strings.HasPrefix(name, "wk-t-") ||
		strings.HasPrefix(name, "wk-r-") ||
		strings.HasPrefix(name, "wk-p-") ||
		strings.HasPrefix(name, "wk-o-") ||
		strings.HasPrefix(name, "wk-hb-")
}

func (h *Herdr) resolveDedicatedWorkspace(ctx context.Context, subject string, cwd string) (string, error) {
	h.wsMu.Lock()
	defer h.wsMu.Unlock()

	targetLabel := workerWorkspaceLabel(subject)

	absCwd := cwd
	if abs, err := filepath.Abs(cwd); err == nil {
		absCwd = abs
	}

	// 1. Check existing workspaces for exact label match (case-insensitive)
	outWorkspaces, err := exec.CommandContext(ctx, h.config.Command, "workspace", "list").Output()
	if err == nil {
		var wResp struct {
			Result struct {
				Workspaces []struct {
					WorkspaceID string `json:"workspace_id"`
					Label       string `json:"label"`
				} `json:"workspaces"`
			} `json:"result"`
		}
		if json.Unmarshal(outWorkspaces, &wResp) == nil {
			for _, ws := range wResp.Result.Workspaces {
				if workspaceLabelMatchesSubject(ws.Label, subject) {
					return ws.WorkspaceID, nil
				}
			}
		}
	}

	// 2. Not found: create dedicated workspace with --no-focus
	createArgs := []string{"workspace", "create", "--label", targetLabel, "--cwd", absCwd, "--no-focus"}
	outCreate, err := exec.CommandContext(ctx, h.config.Command, createArgs...).Output()
	if err == nil {
		var cResp struct {
			Result struct {
				WorkspaceID string `json:"workspace_id"`
				Workspace   struct {
					WorkspaceID string `json:"workspace_id"`
				} `json:"workspace"`
			} `json:"result"`
		}
		if json.Unmarshal(outCreate, &cResp) == nil {
			if cResp.Result.Workspace.WorkspaceID != "" {
				return cResp.Result.Workspace.WorkspaceID, nil
			}
			if cResp.Result.WorkspaceID != "" {
				return cResp.Result.WorkspaceID, nil
			}
		}
	}

	// 3. Fallback: list workspaces again after create attempt
	if outWorkspaces, err := exec.CommandContext(ctx, h.config.Command, "workspace", "list").Output(); err == nil {
		var wResp struct {
			Result struct {
				Workspaces []struct {
					WorkspaceID string `json:"workspace_id"`
					Label       string `json:"label"`
				} `json:"workspaces"`
			} `json:"result"`
		}
		if json.Unmarshal(outWorkspaces, &wResp) == nil {
			for _, ws := range wResp.Result.Workspaces {
				if workspaceLabelMatchesSubject(ws.Label, subject) {
					return ws.WorkspaceID, nil
				}
			}
		}
	}

	return "", err
}

var reLeadingDate = regexp.MustCompile(`^\d{8}(?:-\d{6})?-`)

func sanitizeTag(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
		} else if r == '_' || r == ':' || r == '/' || r == '.' {
			b.WriteRune('-')
		}
	}
	res := strings.Trim(b.String(), "-")
	for strings.Contains(res, "--") {
		res = strings.ReplaceAll(res, "--", "-")
	}
	return res
}

// fitHerdrWorkerName builds a herdr agent name of at most 32 characters matching
// [a-z0-9_-]. When prefix+body fits, the result is that concatenation (trimmed).
// When it does not, a readable head of body is kept and a stable 6-hex sha1 of
// hashSource is appended so IDs that differ only past the old head-truncate point
// stay unique and deterministic across restarts (re-adoption looks panes up by name).
func fitHerdrWorkerName(prefix, body, hashSource string) string {
	return fitHerdrWorkerNameHashed(prefix, body, hashSource, false)
}

// fitHerdrWorkerNameHashed is fitHerdrWorkerName with an option to append the
// hash even when prefix+body fits, for bodies derived lossily from hashSource.
func fitHerdrWorkerNameHashed(prefix, body, hashSource string, alwaysHash bool) string {
	const maxLen = 32
	body = strings.Trim(body, "-_")
	name := prefix + body
	if !alwaysHash && len(name) <= maxLen {
		return strings.Trim(name, "-_")
	}
	sum := sha1.Sum([]byte(hashSource))
	hash6 := hex.EncodeToString(sum[:])[:6]
	suffix := "-" + hash6
	maxBody := maxLen - len(prefix) - len(suffix)
	if maxBody < 1 {
		fallback := prefix + hash6
		if len(fallback) > maxLen {
			fallback = fallback[:maxLen]
		}
		return strings.Trim(fallback, "-_")
	}
	head := body
	if len(head) > maxBody {
		head = head[:maxBody]
	}
	head = strings.Trim(head, "-_")
	result := prefix + head + suffix
	if len(result) > maxLen {
		result = result[:maxLen]
	}
	return strings.Trim(result, "-_")
}

// WorkerNameForKey returns the deterministic Herdr worker agent name for a session key.
func WorkerNameForKey(key string) string {
	name, _ := workerNameAndLabel(key, "")
	return name
}

func workerNameAndLabel(key, model string) (workerName string, tabLabel string) {
	model = herdrRunnerModel(model)

	cleanKey := strings.TrimSpace(key)

	// Background orchestrator jobs
	if strings.HasPrefix(cleanKey, "orchestrator:") || cleanKey == "heartbeat" {
		if cleanKey == "orchestrator:semantic-heartbeat" || cleanKey == "heartbeat" {
			modelTag := sanitizeTag(model)
			if modelTag == "" {
				modelTag = defaultHerdrRunner
			}
			workerName = fitHerdrWorkerName("wk-hb-", modelTag, modelTag)
			tabLabel = fmt.Sprintf("Heartbeat (%s)", model)
			return
		}

		// Format: orchestrator:<route>:<phase>:<id>[:<attempt>]
		parts := strings.Split(cleanKey, ":")
		if len(parts) >= 4 {
			phase := parts[2]
			id := parts[3]
			attempt := "1"
			if len(parts) >= 5 && parts[4] != "" {
				attempt = parts[4]
			}

			shortID := id
			shortID = strings.TrimPrefix(shortID, "tasks-")
			shortID = strings.TrimPrefix(shortID, "goals-")
			shortID = reLeadingDate.ReplaceAllString(shortID, "")
			shortID = sanitizeTag(shortID)
			shortID = strings.Trim(shortID, "-_")
			if shortID == "" {
				shortID = "job"
			}
			// The short form drops the tasks-/goals- prefix and the leading date,
			// so distinct ids can share it; hash the full id whenever it was altered.
			idAltered := shortID != id

			phaseCode := "t"
			phaseLabel := "Task"
			switch {
			case strings.Contains(phase, "impl"):
				phaseCode = "t"
				phaseLabel = "Task"
			case strings.Contains(phase, "rev"):
				phaseCode = "r"
				phaseLabel = "Review"
			case strings.Contains(phase, "plan"):
				phaseCode = "p"
				phaseLabel = "Plan"
			default:
				phaseCode = "o"
				phaseLabel = "Task"
			}

			// Herdr limits agent names to 1-32 lowercase characters [a-z0-9_-]
			prefix := fmt.Sprintf("wk-%s-a%s-", phaseCode, attempt)
			workerName = fitHerdrWorkerNameHashed(prefix, shortID, id, idAltered)
			tabLabel = fmt.Sprintf("%s: %s (%s)", phaseLabel, shortID, model)
			return
		}

		sanitized := sanitizeTag(cleanKey)
		if sanitized == "" {
			sanitized = "job"
		}
		workerName = fitHerdrWorkerName("wk-o-", sanitized, sanitized)
		tabLabel = fmt.Sprintf("Task (%s)", model)
		return
	}

	// Interactive chat (Front-of-House communication agent)
	if strings.HasPrefix(cleanKey, "chat:") {
		parts := strings.Split(cleanKey, ":")
		channel := "tui"
		conv := ""
		if len(parts) >= 2 {
			channel = parts[1]
		}
		if len(parts) >= 3 {
			conv = parts[2]
		}

		channel = sanitizeTag(channel)
		if channel == "" {
			channel = "tui"
		}
		shortConv := conv
		shortConv = strings.TrimPrefix(shortConv, "local-")
		shortConv = sanitizeTag(shortConv)
		if shortConv == "" {
			shortConv = "main"
		}

		labelConv := shortConv
		if len(labelConv) > 12 {
			labelConv = labelConv[:12]
		}
		workerName = fitHerdrWorkerName(fmt.Sprintf("wk-c-%s-", channel), shortConv, shortConv)
		tabLabel = fmt.Sprintf("Chat: %s (%s)", labelConv, model)
		return
	}

	// Generic fallback
	sanitized := sanitizeTag(cleanKey)
	if sanitized == "" {
		sanitized = "worker"
	}
	workerName = fitHerdrWorkerName("wk-", sanitized, sanitized)
	tabLabel = fmt.Sprintf("Werkrby (%s)", model)
	return
}

func (h *Herdr) resolveTarget(ctx context.Context, key string, model string, cwd string) (target string, paneID string, tabID string, isDynamic bool, threadID string, err error) {
	// Roster staff ("@name") expand to a runner kind plus launch arguments.
	model, agentArgs, expandErr := herdrLaunch(cwd, key, model)
	if expandErr != nil {
		return "", "", "", false, "", expandErr
	}

	// 1. Direct explicit pane target (e.g. "w8:p2")
	if strings.Contains(model, ":") {
		return model, model, "", false, "", nil
	}

	cleanKey := strings.TrimSpace(key)
	absCwd := cwd
	if abs, err := filepath.Abs(cwd); err == nil {
		absCwd = abs
	}

	var callerPane string
	var workspaceID string

	if isOrchestratorKey(cleanKey) || strings.HasPrefix(cleanKey, "chat:telegram") {
		subj := workerSubject(cleanKey)
		wsID, err := h.resolveDedicatedWorkspace(ctx, subj, absCwd)
		if err == nil && wsID != "" {
			workspaceID = wsID
		}
	}

	if workspaceID == "" {
		callerPane, workspaceID = h.resolveCallerContext(ctx, cwd)
	}

	expectedWorkerName, tabLabel := workerNameAndLabel(key, model)

	// 2. Search for existing dedicated worker agent in this workspace
	cmd := exec.CommandContext(ctx, h.config.Command, "agent", "list")
	out, err := cmd.Output()
	if err == nil {
		var listResp herdrAgentListResponse
		if json.Unmarshal(out, &listResp) == nil {
			for _, a := range listResp.Result.Agents {
				inWorkspace := workspaceID == "" || a.WorkspaceID == workspaceID || strings.HasPrefix(a.PaneID, workspaceID+":")
				isWorker := a.Name == expectedWorkerName || hasWorkerPrefix(a.Name)

				// STRICT IMMUNITY: Never touch external fleet panes (w1, w2, w4, w5, etc.)!
				if !inWorkspace && !isWorker {
					continue
				}

				// If matching worker agent exists in this workspace
				if a.Name == expectedWorkerName && inWorkspace {
					// A worker left over from different staffing (another runner kind)
					// is retired so the conversation continues on the assigned runner.
					if a.Agent != "" && a.Agent != model && a.AgentStatus != "working" {
						if a.TabID != "" {
							_ = exec.CommandContext(ctx, h.config.Command, "tab", "close", a.TabID).Run()
						}
						break
					}
					if a.AgentStatus == "blocked" {
						_ = exec.CommandContext(ctx, h.config.Command, "agent", "send-keys", a.Name, "esc").Run()
					}
					return a.Name, a.PaneID, a.TabID, false, a.AgentSession.Value, nil
				}
			}
		}
	}

	// 3. Spawn a dedicated worker in a full-width dedicated tab
	tabArgs := []string{"tab", "create", "--label", tabLabel, "--cwd", absCwd, "--no-focus"}
	if workspaceID != "" {
		tabArgs = append(tabArgs, "--workspace", workspaceID)
	}

	var newPaneID string
	var createdTabID string
	tabCmd := exec.CommandContext(ctx, h.config.Command, tabArgs...)
	tabOut, tabErr := tabCmd.Output()
	if tabErr == nil {
		var tabResp herdrTabCreateResponse
		if json.Unmarshal(tabOut, &tabResp) == nil && tabResp.Result.RootPane.PaneID != "" {
			newPaneID = tabResp.Result.RootPane.PaneID
			createdTabID = tabResp.Result.Tab.TabID
		}
	}

	// Fallback to pane split if tab creation failed
	if newPaneID == "" {
		splitArgs := []string{"pane", "split", "--direction", "right", "--ratio", "0.5", "--cwd", absCwd, "--no-focus"}
		if callerPane != "" {
			splitArgs = append(splitArgs, "--pane", callerPane)
		}
		splitCmd := exec.CommandContext(ctx, h.config.Command, splitArgs...)
		splitOut, splitErr := splitCmd.Output()
		if splitErr != nil {
			return "", "", "", false, "", fmt.Errorf("failed to spawn dedicated worker (tab and split failed): %w (out: %s)", splitErr, string(splitOut))
		}
		var splitResp herdrPaneSplitResponse
		if err := json.Unmarshal(splitOut, &splitResp); err != nil || splitResp.Result.Pane.PaneID == "" {
			return "", "", "", false, "", fmt.Errorf("failed to parse new pane ID from herdr: %s", string(splitOut))
		}
		newPaneID = splitResp.Result.Pane.PaneID
	}

	// 4. Start the agent in the new pane (clear any stale registration of the name first)
	_ = exec.CommandContext(ctx, h.config.Command, "agent", "rename", expectedWorkerName, "--clear").Run()

	startArgs := []string{"agent", "start", expectedWorkerName, "--kind", model, "--pane", newPaneID, "--timeout", "45000"}
	if len(agentArgs) > 0 {
		startArgs = append(append(startArgs, "--"), agentArgs...)
	}
	startCmd := exec.CommandContext(ctx, h.config.Command, startArgs...)
	startOut, startErr := startCmd.CombinedOutput()
	if startErr != nil {
		if createdTabID != "" {
			_ = exec.Command(h.config.Command, "tab", "close", createdTabID).Run()
		} else {
			_ = exec.Command(h.config.Command, "pane", "close", newPaneID).Run()
		}
		return "", "", "", false, "", fmt.Errorf("failed to start agent %s in pane %s: %w (out: %s)", model, newPaneID, startErr, string(startOut))
	}

	getCmd := exec.CommandContext(ctx, h.config.Command, "agent", "get", expectedWorkerName)
	if getOut, getErr := getCmd.Output(); getErr == nil {
		var getResp herdrAgentGetResponse
		if json.Unmarshal(getOut, &getResp) == nil {
			threadID = getResp.Result.Agent.AgentSession.Value
		}
	}
	composerTimeout := h.composerWaitTimeout
	if composerTimeout <= 0 {
		composerTimeout = 30 * time.Second
	}
	if sid := h.waitForRunnerComposer(ctx, expectedWorkerName, model, composerTimeout); sid != "" {
		threadID = sid
	}

	return expectedWorkerName, newPaneID, createdTabID, true, threadID, nil
}

func (h *Herdr) sendInternal(ctx context.Context, key, prompt string, cfg HarnessConfig, emit core.Emit) (string, bool, error) {
	if strings.TrimSpace(prompt) == "" {
		return "", false, errors.New("harness prompt is empty")
	}

	lock := h.lockForKey(key)
	lock.Lock()
	defer lock.Unlock()

	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return "", false, errors.New("Herdr harness is closed")
	}
	h.mu.Unlock()

	cfg.Model = h.rosterModelForKey(key, cfg)
	// A `headless: true` staff runs as `claude -p`, `kimi -p` or `agy -p`, not in a pane. A roster error falls through to
	// resolveTarget, which reports it.
	if staff, ok, err := roster.ExpandStaff(filepath.Join(cfg.Cwd, roster.StateDirName), strings.TrimSpace(cfg.Model)); ok && err == nil && staff.Headless {
		name := strings.TrimPrefix(strings.TrimSpace(cfg.Model), roster.StaffPrefix)
		return h.sendHeadless(ctx, key, prompt, name, staff, cfg, emit)
	}
	target, paneID, tabID, isDynamic, threadID, err := h.resolveTarget(ctx, key, cfg.Model, cfg.Cwd)
	if err != nil {
		return "", false, fmt.Errorf("herdr target resolution: %w", err)
	}

	if emit != nil {
		statusMsg := fmt.Sprintf("Herdr: routing work to dedicated worker %s in %s", target, paneID)
		if isDynamic {
			statusMsg = fmt.Sprintf("Herdr: spawned dedicated worker %s in %s (cwd: %s)", target, paneID, cfg.Cwd)
		}
		emit(core.Event{
			Kind: core.EventStatus,
			Text: statusMsg,
		})
	}

	turnContext, cancel := context.WithCancel(ctx)
	turn := &herdrTurn{
		target:    target,
		paneID:    paneID,
		tabID:     tabID,
		isDynamic: isDynamic,
		cancel:    cancel,
		done:      make(chan struct{}),
	}

	h.mu.Lock()
	h.active[key] = turn
	h.mu.Unlock()

	var turnSuccess bool
	cleanKey := strings.TrimSpace(key)
	defer func() {
		cancel()
		h.mu.Lock()
		delete(h.active, key)
		h.mu.Unlock()
		close(turn.done)
		if turnSuccess {
			// The worker is idle after a completed turn, so no later turn needs this one's prompt.
			h.forgetSubmitted(target)
		}
		if turnSuccess && isOrchestratorKey(cleanKey) {
			go h.cleanupCompletedTab(key, tabID, paneID, target)
		}
	}()

	// Background status ticker while agent is processing prompt. Chat panes keep
	// a 2s cadence for the live UI. Orchestrator workers use 30s: every status
	// event also rewrites the job lease, and at 2s these lines were 82-99% of all
	// durable job output (spyjo-observer 2026-09-17). 30s stays far inside the
	// 30-minute task / 2-hour goal stale thresholds that the lease heartbeat feeds.
	streamTicker := time.NewTicker(herdrStatusInterval(cleanKey))
	streamDone := make(chan struct{})
	go func() {
		defer streamTicker.Stop()
		startTime := time.Now()
		for {
			select {
			case <-streamDone:
				return
			case <-turnContext.Done():
				return
			case <-streamTicker.C:
				elapsed := time.Since(startTime).Truncate(time.Second)
				if emit != nil {
					emit(core.Event{
						Kind: core.EventStatus,
						Text: fmt.Sprintf("Herdr: %s working (%s)...", target, elapsed),
					})
				}
			}
		}
	}()

	runner := func(ctx context.Context, args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, h.config.Command, args...)
		turn.cmd = cmd
		return cmd.CombinedOutput()
	}
	status := func(text string) {
		if emit != nil {
			emit(core.Event{Kind: core.EventStatus, Text: text})
		}
	}
	// The runner kind decides, before the send, whether a blocked pane is waited on (hermes) and
	// which profile ledger judges the turn afterwards.
	runnerKind, launchArgs := h.runnerLaunch(cfg, key)

	// promptStart bounds the transcript read below to THIS turn: without it the
	// reader returns the newest assistant text anywhere in the file, so a turn
	// that errored replays the previous answer as if it were fresh.
	promptStart := time.Now()
	promptOut, promptErr := h.promptWorker(turnContext, turn, target, prompt, runnerKind == "hermes", runner, status)

	close(streamDone)

	if turnContext.Err() != nil {
		return threadID, false, turnContext.Err()
	}

	// Inspect final agent state
	getCmd := exec.Command(h.config.Command, "agent", "get", target)
	getOut, getErr := getCmd.Output()
	var agentStatus string
	var agentKind string
	var hasRealSession bool
	if getErr == nil {
		var getResp herdrAgentGetResponse
		if json.Unmarshal(getOut, &getResp) == nil {
			agentStatus = getResp.Result.Agent.AgentStatus
			agentKind = getResp.Result.Agent.Agent
			if getResp.Result.Agent.AgentSession.Value != "" {
				threadID = getResp.Result.Agent.AgentSession.Value
				hasRealSession = true
			}
		}
	}

	// Fetch and clean final response text
	var finalText string
	if agentKind == "opencode" || strings.HasPrefix(threadID, "ses_") {
		if text, err := extractOpencodeMessage(threadID); err == nil && strings.TrimSpace(text) != "" {
			finalText = text
		}
	}

	if finalText == "" {
		if path := transcriptPath(agentKind, threadID, cfg.Cwd); path != "" {
			text, err := extractTranscriptMessage(path, promptStart)
			var turnErr *transcriptTurnError
			if errors.As(err, &turnErr) {
				return threadID, false, fmt.Errorf("runner turn failed: %s", turnErr.Detail)
			}
			if err == nil {
				finalText = text
			}
		}
	}

	// A Hermes turn runs in the root gateway, so herdr never reports a session for it and there
	// is no transcript: the launch profile's ledger proves delivery and holds the answer.
	isHermes := agentKind == "hermes" || runnerKind == "hermes"
	var hermes hermesVerdict
	if turn.notSent {
		// This prompt was never typed: the worker's earlier turn moved the claimed document while
		// this turn waited on its blocked pane, so that document's transition owns the outcome.
		finalText = h.earlierTurnReport(turnContext, target, launchArgs, turn)
		if turnContext.Err() != nil {
			return threadID, false, turnContext.Err()
		}
	} else if isHermes && promptErr == nil && finalText == "" {
		hermes = h.judgeHermesTurn(turnContext, target, launchArgs, prompt, promptStart)
		if turnContext.Err() != nil {
			return threadID, false, turnContext.Err()
		}
		if hermes.err != nil {
			return threadID, false, hermes.err
		}
		finalText = hermes.final
	}

	// Guard against undelivered prompt on newly spawned pane:
	// If prompt --wait returned ok on a pane spawned for this turn, but there is
	// still no agent session and no new transcript, the prompt was typed into a
	// terminal that the worker was not yet reading. A Hermes turn the ledger or
	// screen shows was delivered is never re-sent.
	if promptErr == nil && !turn.notSent && turn.isDynamic && !hasRealSession && finalText == "" && !hermes.delivered {
		if emit != nil {
			emit(core.Event{
				Kind:      core.EventStatus,
				Text:      fmt.Sprintf("prompt_not_delivered: worker %s produced no session or transcript; waiting for composer before re-sending", target),
				Execution: &core.ExecutionStatus{State: "running", Detail: "prompt_not_delivered: waiting for composer"},
			})
		}

		// Re-send once after the runner's composer is visible
		composerTimeout := h.composerWaitTimeout
		if composerTimeout <= 0 {
			composerTimeout = 30 * time.Second
		}
		if sid := h.waitForRunnerComposer(turnContext, target, cfg.Model, composerTimeout); sid != "" {
			threadID = sid
			hasRealSession = true
		}

		rePromptStart := time.Now()
		promptOut, promptErr = h.promptWorker(turnContext, turn, target, prompt, runnerKind == "hermes", runner, status)

		getCmd = exec.Command(h.config.Command, "agent", "get", target)
		if getOut, getErr = getCmd.Output(); getErr == nil {
			var getResp herdrAgentGetResponse
			if json.Unmarshal(getOut, &getResp) == nil {
				agentStatus = getResp.Result.Agent.AgentStatus
				agentKind = getResp.Result.Agent.Agent
				if getResp.Result.Agent.AgentSession.Value != "" {
					threadID = getResp.Result.Agent.AgentSession.Value
					hasRealSession = true
				}
			}
		}

		if agentKind == "opencode" || strings.HasPrefix(threadID, "ses_") {
			if text, err := extractOpencodeMessage(threadID); err == nil && strings.TrimSpace(text) != "" {
				finalText = text
			}
		}
		if finalText == "" {
			if path := transcriptPath(agentKind, threadID, cfg.Cwd); path != "" {
				if text, err := extractTranscriptMessage(path, rePromptStart); err == nil {
					finalText = text
				}
			}
		}
		if isHermes && promptErr == nil && finalText == "" {
			hermes = h.judgeHermesTurn(turnContext, target, launchArgs, prompt, rePromptStart)
			if turnContext.Err() != nil {
				return threadID, false, turnContext.Err()
			}
			if hermes.err != nil {
				return threadID, false, hermes.err
			}
			finalText = hermes.final
		}

		// If that also yields nothing, return an error that Werkrby retries in place, never EventFinal.
		if promptErr != nil || (!hasRealSession && finalText == "" && !hermes.delivered) {
			notDeliveredErr := fmt.Errorf("prompt_not_delivered: worker %s never received prompt, please retry", target)
			if emit != nil {
				emit(core.Event{
					Kind:      core.EventError,
					Text:      notDeliveredErr.Error(),
					ThreadID:  threadID,
					Done:      true,
					Execution: &core.ExecutionStatus{State: "error", Detail: "prompt_not_delivered"},
				})
			}
			return threadID, false, notDeliveredErr
		}
	}

	if threadID == "" {
		threadID = fmt.Sprintf("herdr:%s:%d", target, time.Now().Unix())
	}
	h.rememberSession(key, threadID)

	if finalText == "" {
		finalOut, _ := exec.Command(h.config.Command, "agent", "read", target, "--source", "recent-unwrapped", "--lines", "250").Output()
		raw := strings.TrimSpace(string(finalOut))
		if raw == "" {
			raw = strings.TrimSpace(string(promptOut))
		}
		if strings.Contains(raw, "monthly spending limit") || strings.Contains(raw, "spending limit") {
			return threadID, false, fmt.Errorf("provider spending limit reached: %s", raw)
		}
		finalText = cleanHerdrTerminalOutput(raw)
		if finalText == "" {
			finalText = raw
		}
	}

	// Evaluate completion
	if promptErr != nil {
		errMsg := strings.TrimSpace(string(promptOut))
		if errMsg == "" {
			errMsg = promptErr.Error()
		}
		var stuck *hermesBlockedError
		if errors.As(promptErr, &stuck) || agentStatus == "blocked" || strings.Contains(errMsg, "agent_blocked") {
			readOut, _ := exec.Command(h.config.Command, "agent", "read", target, "--source", "recent-unwrapped", "--lines", "30").Output()
			blockedDetail := strings.TrimSpace(string(readOut))
			if blockedDetail == "" {
				blockedDetail = errMsg
			}
			if emit != nil {
				emit(core.Event{
					Kind:      core.EventStatus,
					Text:      fmt.Sprintf("Herdr worker %s is blocked in %s: %s", target, paneID, blockedDetail),
					ThreadID:  threadID,
					Execution: &core.ExecutionStatus{State: "blocked", Detail: blockedDetail},
				})
			}
			if stuck != nil {
				return threadID, false, stuck
			}
			return threadID, false, fmt.Errorf("herdr agent %s blocked: %s", target, errMsg)
		}
		if emit != nil {
			emit(core.Event{
				Kind:      core.EventError,
				Text:      errMsg,
				ThreadID:  threadID,
				Done:      true,
				Execution: &core.ExecutionStatus{State: "error", Detail: errMsg},
			})
		}
		return threadID, false, fmt.Errorf("herdr prompt failed: %w (out: %s)", promptErr, errMsg)
	}

	if emit != nil {
		emit(core.Event{
			Kind:      core.EventFinal,
			Text:      finalText,
			FinalText: &finalText,
			ThreadID:  threadID,
			Done:      true,
			Execution: &core.ExecutionStatus{State: "finishing"},
		})
	}

	turnSuccess = true
	return threadID, false, nil
}

var (
	herdrFooterPatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?m)^\s*▣\s+Build.*$`),
		regexp.MustCompile(`(?m)^\s*╹[▀─=]+.*$`),
		regexp.MustCompile(`(?m)^\s*✻\s+Churned for.*$`),
		regexp.MustCompile(`(?m)^.*•\s+OpenCode.*$`),
		regexp.MustCompile(`(?m)^.*ctrl\+p commands.*$`),
		regexp.MustCompile(`(?m)^\s*●\s+Login expired.*$`),
		regexp.MustCompile(`(?m)^\s*⏵⏵\s+bypass permissions.*$`),
		regexp.MustCompile(`(?m)^\s*Ask When Needed.*$`),
		regexp.MustCompile(`(?m)^\s*context:\s*\d+%.*$`),
		regexp.MustCompile(`(?m)^\s*⚕\s+k\d+.*$`),
		regexp.MustCompile(`(?m)^\s*❯\s+Ask anything.*$`),
		regexp.MustCompile(`(?m)^.*Message (?:Antigravity|agy|SpyJo|Werkrby).*$`),
		regexp.MustCompile(`(?m)^\s*▄▄[↵\^].*$`),
		regexp.MustCompile(`(?m)^\s*───{5,}.*$`),
	}
	herdrThoughtPattern   = regexp.MustCompile(`(?s)(?:^|\n)\s*Thought:\s*[^\n]+\n+(.*?)(?:\n\s*\n\s*([^\s].*)|$)`)
	hermesBoxPattern      = regexp.MustCompile(`(?s)╭─\s*[⚕☤]\s*Hermes[^\n]*\n(.*?)\n╰[─]+╯`)
	hermesTurnRule        = regexp.MustCompile(`(?m)^─{40}[ \t\r]*$`)
	hermesReasoningBox    = regexp.MustCompile(`(?s)┌─\s*Reasoning[^\n]*\n.*?└[─]+┘\n*`)
	kimiInputBoxPattern   = regexp.MustCompile(`(?s)╭[─]+╮\s*\n\s*│\s*>\s*\n\s*╰[─]+╯`)
	agyInputBoxPattern    = regexp.MustCompile(`(?s)(?:╭─+╮\s*\n\s*│\s*Message (?:Antigravity|agy|SpyJo|Werkrby)[^\n]*\n\s*╰─+╯|─{5,}\s*\n[ \t]*>[^\n]*\n[ \t]*─{5,}(?:\s*\n[^\n]*\? for shortcuts[^\n]*)?)`)
	hermesComposerPattern = regexp.MustCompile(`(?s)(?:─{5,}\s*\n[ \t]*(?:[\w.-]+[ \t]+)?❯.*?\n[ \t]*─{5,}|[⚕☤]\s+\S+\s+│[^\n]*\n[ \t]*─{5,}\s*\n.*?\n[ \t]*─{5,})`)
)

// cleanHerdrTerminalOutput strips prompt echoes, terminal footers, and internal thought blocks
// from raw terminal screen dumps across OpenCode, Kimi, and Hermes agents.
func cleanHerdrTerminalOutput(text string) string {
	// 0. Hermes: the reply is the LAST response box. A warm pane keeps earlier turns' boxes on
	// screen, and one turn can draw a box before a tool call and another for the answer.
	// Hermes rules off every submitted message with a 40-wide rule; one after the last box
	// means the newest turn drew no box, so clean only that turn and never serve an older reply.
	if boxes := hermesBoxPattern.FindAllStringSubmatchIndex(text, -1); len(boxes) > 0 {
		last := boxes[len(boxes)-1]
		if rules := hermesTurnRule.FindAllStringIndex(text[last[1]:], -1); len(rules) > 0 {
			text = hermesDropPromptPreview(text[last[1]+rules[len(rules)-1][1]:])
		} else if body := strings.TrimSpace(text[last[2]:last[3]]); body != "" {
			return body
		}
	}

	// Strip Hermes reasoning boxes if present
	text = hermesReasoningBox.ReplaceAllString(text, "")

	// Strip Kimi bottom input box if present
	text = kimiInputBoxPattern.ReplaceAllString(text, "")

	// Strip Antigravity / Werkrby bottom input box if present
	text = agyInputBoxPattern.ReplaceAllString(text, "")

	// Strip Hermes bottom composer if present
	text = hermesComposerPattern.ReplaceAllString(text, "")

	lines := strings.Split(text, "\n")
	var cleanedLines []string

	// 1. Strip prompt echo lines (vertical borders or Kimi prompt marker ✨)
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "┃") || strings.HasPrefix(trimmed, "│") || strings.HasPrefix(trimmed, "|") {
			continue
		}
		if strings.HasPrefix(trimmed, "✨ ") || strings.HasPrefix(trimmed, "● respond in one sentence") {
			continue
		}
		cleanedLines = append(cleanedLines, line)
	}
	text = strings.TrimSpace(strings.Join(cleanedLines, "\n"))

	// 2. Cut off bottom status chrome
	for _, pat := range herdrFooterPatterns {
		loc := pat.FindStringIndex(text)
		if loc != nil {
			text = strings.TrimSpace(text[:loc[0]])
		}
	}

	// 3. If there is a Thought block followed by an answer, extract the actual answer
	if m := herdrThoughtPattern.FindStringSubmatch(text); m != nil && len(m) >= 3 && strings.TrimSpace(m[2]) != "" {
		text = strings.TrimSpace(m[2])
	}

	return strings.TrimSpace(text)
}

// promptWorker submits prompt to target with --wait and follows the turn to its end: a stalled
// submission is retried once after a short delay, a wait that herdr gives up on while the agent
// still works is extended (herdr's --wait stops after 15 minutes; treating that as a failure
// closed the worker tab mid-task and sent a recovery agent to redo the work), and an idle reading
// must hold (checkIdleStability). For a hermes worker a pane blocked on a prompt is waited on, not
// failed: herdr rejects a submission to an already blocked agent before any input is sent, so the
// prompt is submitted again once the pane clears, and a pane still blocked after the wait returns
// a *hermesBlockedError.
//
// A resubmission is typed only into a pane that holds idle with the claimed document still in its
// folder. A pane that clears into working has resumed the turn it was blocked in, and a Hermes
// worker queues a prompt typed into a busy pane (busy_input_mode: queue) and runs it as a second
// full turn after that one, so the in-flight turn is followed to its end first. When that turn,
// or any earlier one, moved the claimed document (ClaimedDocument) out of its folder, the prompt
// is not typed at all: turn.notSent is set and sendInternal judges the earlier turn instead.
func (h *Herdr) promptWorker(ctx context.Context, turn *herdrTurn, target, prompt string, hermes bool, runner herdrRunner, status func(string)) ([]byte, error) {
	submit := func() ([]byte, error) {
		since := time.Now()
		cmd := exec.CommandContext(ctx, h.config.Command, "agent", "prompt", target, prompt, "--wait", "--timeout", "900000")
		turn.cmd = cmd
		out, err := cmd.CombinedOutput()
		if err != nil && strings.Contains(string(out), "agent_prompt_stalled") {
			time.Sleep(2000 * time.Millisecond)
			cmd = exec.CommandContext(ctx, h.config.Command, "agent", "prompt", target, prompt, "--wait", "--timeout", "900000")
			turn.cmd = cmd
			out, err = cmd.CombinedOutput()
		}
		if hermes && (err == nil || !strings.Contains(string(out), "agent_blocked")) {
			h.rememberSubmitted(target, prompt, since)
		}
		return out, err
	}
	out, err := submit()
	if hermes && err != nil && strings.Contains(string(out), "agent_blocked") {
		// One wait, from the first rejection, covers every resubmission, so a pane that clears and
		// blocks again before the prompt lands cannot keep the turn here past the bound. Time spent
		// following a resumed (working) turn to its end is not blocked time and moves the bound out
		// by as much; an idle pane's settle reads count toward it.
		first := time.Now()
		deadline := first.Add(h.hermesBlockedBound())
	resubmit:
		for err != nil && strings.Contains(string(out), "agent_blocked") {
			if !time.Now().Before(deadline) {
				return out, &hermesBlockedError{target: target, waited: time.Since(first)}
			}
			state, waitErr := h.waitHermesUnblocked(ctx, target, deadline, runner, status)
			if waitErr != nil {
				return out, waitErr
			}
			switch state {
			case "":
				break resubmit // herdr could not report the agent: the rejection stands, as before
			case "idle", "done", "working":
				// Type only into a pane whose idle reading holds; checkIdleStability follows a
				// resumed turn to its end on the way.
				settling := time.Now()
				if state == "working" {
					status(fmt.Sprintf("Herdr: hermes worker %s resumed its earlier turn; following it to its end before typing this prompt", target))
				}
				if settleErr := h.checkIdleStability(ctx, target, true, runner, status); settleErr != nil {
					return out, settleErr
				}
				if state == "working" {
					deadline = deadline.Add(time.Since(settling))
				}
			default:
				// A status this harness does not know is no evidence the pane takes input: re-read it
				// after one poll.
				select {
				case <-ctx.Done():
					return out, ctx.Err()
				case <-time.After(min(h.hermesBlockedPollEvery(), max(time.Until(deadline), 0))):
				}
				continue
			}
			if path, moved := claimedDocumentMoved(ctx); moved {
				status(fmt.Sprintf("Herdr: hermes worker %s's earlier turn moved %s out of its claimed folder; this prompt is not sent", target, path))
				turn.notSent = true
				turn.movedDocument = path
				turn.earlier = h.lastSubmitted(target)
				return nil, nil
			}
			out, err = submit()
		}
	}
	if err != nil && isHerdrWaitTimeout(string(out)) {
		out, err = extendHerdrWait(ctx, target, out, err, herdrWaitCeiling, runner, status)
	}
	if err == nil {
		err = h.checkIdleStability(ctx, target, hermes, runner, status)
	}
	return out, err
}

// claimedDocumentMoved reports whether the claimed document ctx names (WithClaimedDocument) is gone
// from its path: the worker's turn moved it to its next folder. A ctx that names none, or a path
// that cannot be read for another reason, is not a move.
func claimedDocumentMoved(ctx context.Context) (string, bool) {
	path := ClaimedDocument(ctx)
	if path == "" {
		return "", false
	}
	_, err := os.Stat(path)
	return path, os.IsNotExist(err)
}

// rememberSubmitted records the prompt herdr accepted for target.
func (h *Herdr) rememberSubmitted(target, prompt string, since time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.submitted == nil {
		h.submitted = map[string]hermesSubmitted{}
	}
	if _, ok := h.submitted[target]; !ok && len(h.submitted) >= maxRememberedSubmissions {
		oldest := ""
		for name, entry := range h.submitted {
			if oldest == "" || entry.since.Before(h.submitted[oldest].since) {
				oldest = name
			}
		}
		delete(h.submitted, oldest)
	}
	h.submitted[target] = hermesSubmitted{prompt: prompt, since: since}
}

// lastSubmitted is the last prompt herdr accepted for target in this process, or nil.
func (h *Herdr) lastSubmitted(target string) *hermesSubmitted {
	h.mu.Lock()
	defer h.mu.Unlock()
	entry, ok := h.submitted[target]
	if !ok {
		return nil
	}
	return &entry
}

func (h *Herdr) forgetSubmitted(target string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.submitted, target)
}

func (h *Herdr) hermesBlockedPollEvery() time.Duration {
	if h.hermesBlockedPoll > 0 {
		return h.hermesBlockedPoll
	}
	return defaultHermesBlockedPoll
}

// earlierTurnReport is the final text of a turn whose prompt was never typed (turn.notSent): the
// earlier turn's answer as its profile ledger records it, when this process sent that turn's
// prompt, else a note that this prompt was not sent. A failed earlier turn is reported in the
// note, not returned as this turn's error: the document it moved already settled the outcome.
func (h *Herdr) earlierTurnReport(ctx context.Context, target string, launchArgs []string, turn *herdrTurn) string {
	note := fmt.Sprintf("Werkrby did not send this prompt: the worker's earlier turn moved %s out of its claimed folder while the worker's pane was blocked.", turn.movedDocument)
	if turn.earlier == nil {
		return note
	}
	verdict := h.judgeHermesTurn(ctx, target, launchArgs, turn.earlier.prompt, turn.earlier.since)
	switch {
	case verdict.final != "":
		return verdict.final
	case verdict.err != nil:
		return note + " That turn ended with an error: " + verdict.err.Error()
	}
	return note
}

func (h *Herdr) hermesBlockedBound() time.Duration {
	if h.hermesBlockedWait > 0 {
		return h.hermesBlockedWait
	}
	return defaultHermesBlockedWait
}

// waitHermesUnblocked re-reads a hermes worker herdr reports blocked until it shows another state
// or deadline passes. It returns the state the worker left blocked for ("" when herdr could not
// report one, which the caller treats as before), the context's error when the turn is cancelled,
// or a *hermesBlockedError once deadline passes with the pane still blocked.
func (h *Herdr) waitHermesUnblocked(ctx context.Context, target string, deadline time.Time, run herdrRunner, status func(string)) (string, error) {
	poll := h.hermesBlockedPollEvery()
	started := time.Now()
	status(fmt.Sprintf("Herdr: hermes worker %s is blocked on a prompt; waiting up to %s for it to clear", target, time.Until(deadline).Round(time.Second)))
	for {
		getOut, getErr := run(ctx, "agent", "get", target)
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		if getErr != nil {
			return "", nil
		}
		var resp herdrAgentGetResponse
		if json.Unmarshal(getOut, &resp) != nil {
			return "", nil
		}
		if state := resp.Result.Agent.AgentStatus; state != "blocked" {
			status(fmt.Sprintf("Herdr: hermes worker %s cleared its prompt after %s (%s)", target, time.Since(started).Round(time.Second), state))
			return state, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return "blocked", &hermesBlockedError{target: target, waited: time.Since(started)}
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(min(poll, remaining)):
		}
	}
}

// checkIdleStability re-reads the agent after its prompt wait ended: herdr screen detection can
// momentarily classify an in-flight agent frame as idle mid-turn, so an idle reading must hold for
// several consecutive checks, and any check that reports working re-enters extendHerdrWait. A
// blocked agent is an agent_blocked error, except for a hermes worker, which is waited on
// (waitHermesUnblocked) for up to the blocked-pane bound per blocked episode.
func (h *Herdr) checkIdleStability(turnContext context.Context, target string, hermes bool, runner herdrRunner, status func(string)) error {
	stabilityCount := h.idleStabilityCount
	if stabilityCount <= 0 {
		stabilityCount = 3
	}
	interval := h.idleStabilityInterval
	if interval <= 0 && h.idleStabilityCount == 0 {
		interval = 2 * time.Second
	}
	consecutiveIdle := 0
	for consecutiveIdle < stabilityCount {
		if turnContext.Err() != nil {
			return turnContext.Err()
		}
		getOut, getErr := runner(turnContext, "agent", "get", target)
		if getErr != nil {
			break
		}
		var getResp herdrAgentGetResponse
		if json.Unmarshal(getOut, &getResp) != nil {
			break
		}
		agentSt := getResp.Result.Agent.AgentStatus
		switch agentSt {
		case "idle", "done":
			consecutiveIdle++
			if consecutiveIdle < stabilityCount && interval > 0 {
				select {
				case <-turnContext.Done():
					return turnContext.Err()
				case <-time.After(interval):
				}
				if turnContext.Err() != nil {
					return turnContext.Err()
				}
			}
		case "working":
			consecutiveIdle = 0
			var promptErr error
			_, promptErr = extendHerdrWait(turnContext, target, nil, errors.New("agent still working"), herdrWaitCeiling, runner, status)
			if promptErr != nil {
				return promptErr
			}
		case "blocked":
			if !hermes {
				return errors.New("agent_blocked")
			}
			consecutiveIdle = 0
			state, err := h.waitHermesUnblocked(turnContext, target, time.Now().Add(h.hermesBlockedBound()), runner, status)
			if err != nil {
				return err
			}
			if state == "" {
				return nil // herdr could not report the agent: the same as a failed read above
			}
		default:
			// Unknown status (a future herdr value): neither evidence of idle
			// nor of work. Do not count it and do not busy-loop; wait one
			// interval before re-reading (reviewer hardening note 2026-09-27).
			if interval > 0 {
				select {
				case <-turnContext.Done():
					return turnContext.Err()
				case <-time.After(interval):
				}
				if turnContext.Err() != nil {
					return turnContext.Err()
				}
			}
		}
	}
	return nil
}

// isRunnerComposerVisible reports whether the screen contains the interactive composer
// or ready input line for the specified runner kind.
func isRunnerComposerVisible(model, screen string) bool {
	clean := strings.TrimSpace(screen)
	if clean == "" {
		return false
	}
	model = strings.ToLower(strings.TrimSpace(model))
	switch {
	case strings.Contains(model, "agy") || strings.Contains(model, "antigravity") || strings.Contains(model, "gemini"):
		if agyInputBoxPattern.MatchString(screen) {
			return true
		}
		if strings.Contains(screen, "? for shortcuts") && !strings.Contains(screen, "esc to cancel") {
			return true
		}
		return false
	case strings.Contains(model, "kimi"):
		if kimiInputBoxPattern.MatchString(screen) || strings.Contains(screen, "│ >") {
			return true
		}
		return false
	case strings.Contains(model, "hermes"):
		if strings.Contains(screen, "msg=interrupt") || strings.Contains(screen, "☤ ❯") {
			return false
		}
		if hermesComposerPattern.MatchString(screen) || hermesBoxPattern.MatchString(screen) {
			return true
		}
		return false
	case strings.Contains(model, "opencode"):
		if strings.Contains(screen, "ctrl+p commands") || strings.Contains(screen, "• OpenCode") || strings.Contains(screen, "Ask anything...") {
			return true
		}
		return false
	case strings.Contains(model, "pi"):
		if strings.Contains(screen, "π -") || strings.Contains(screen, "❯") {
			return true
		}
		return false
	default:
		if agyInputBoxPattern.MatchString(screen) || kimiInputBoxPattern.MatchString(screen) || hermesComposerPattern.MatchString(screen) || hermesBoxPattern.MatchString(screen) {
			return true
		}
		if strings.Contains(screen, "? for shortcuts") && !strings.Contains(screen, "esc to cancel") {
			return true
		}
		if strings.Contains(screen, "ctrl+p commands") {
			return true
		}
		return false
	}
}

// waitForRunnerComposer waits up to maxWait for the runner's composer to be visible
// on the terminal screen, returning any active agent session ID discovered during the wait.
func (h *Herdr) waitForRunnerComposer(ctx context.Context, target, model string, maxWait time.Duration) string {
	if maxWait <= 0 {
		maxWait = 30 * time.Second
	}
	deadline := time.Now().Add(maxWait)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var sessionID string
	for {
		// 1. Check agent state and session
		getCmd := exec.CommandContext(ctx, h.config.Command, "agent", "get", target)
		if getOut, getErr := getCmd.Output(); getErr == nil {
			var getResp herdrAgentGetResponse
			if json.Unmarshal(getOut, &getResp) == nil {
				if getResp.Result.Agent.AgentSession.Value != "" {
					sessionID = getResp.Result.Agent.AgentSession.Value
				}
			}
		}

		// 2. Read recent terminal output to inspect composer
		readCmd := exec.CommandContext(ctx, h.config.Command, "agent", "read", target, "--source", "recent-unwrapped", "--lines", "100")
		if readOut, readErr := readCmd.Output(); readErr == nil {
			if isRunnerComposerVisible(model, string(readOut)) {
				time.Sleep(100 * time.Millisecond)
				return sessionID
			}
		}

		if sessionID != "" && !strings.Contains(strings.ToLower(model), "agy") && !strings.Contains(strings.ToLower(model), "antigravity") && !strings.Contains(strings.ToLower(model), "hermes") {
			return sessionID
		}

		if time.Now().After(deadline) {
			break
		}

		select {
		case <-ctx.Done():
			return sessionID
		case <-ticker.C:
		}
	}
	return sessionID
}

// extractOpencodeMessage queries OpenCode's local database for the latest assistant message in a session.
var claudeProjectSlug = regexp.MustCompile(`[^A-Za-z0-9]`)

// transcriptPath locates the runner's own JSONL session transcript, which holds
// the exact final reply; a terminal scrape is only the last resort. Pi reports
// the transcript path as its session; Claude Code reports a session UUID stored
// under its per-directory project folder.
func transcriptPath(agentKind, threadID, cwd string) string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	switch {
	case strings.HasSuffix(threadID, ".jsonl") && strings.HasPrefix(threadID, filepath.Join(home, ".pi")+string(filepath.Separator)):
		return filepath.Clean(threadID)
	case agentKind == "claude" && threadID != "" && !strings.ContainsAny(threadID, "/\\:"):
		abs, err := filepath.Abs(cwd)
		if err != nil {
			return ""
		}
		return filepath.Join(home, ".claude", "projects", claudeProjectSlug.ReplaceAllString(abs, "-"), threadID+".jsonl")
	}
	return ""
}

// transcriptTurnError reports that the runner produced a turn for THIS prompt
// but that turn carried no usable text -- a provider error, a refusal, an empty
// completion. It exists so the caller fails the turn loudly instead of serving
// an older reply: on 2026-09-18 a Venice 402 on the chat seat made SpyJo answer
// three unrelated questions with the same stale paragraph, twice verbatim.
type transcriptTurnError struct {
	Detail string
}

func (e *transcriptTurnError) Error() string { return e.Detail }

// extractTranscriptMessage returns the text of the last assistant message that
// carries text AND belongs to the turn started at or after since. Pi and Claude
// Code share the {"message":{"role","content":[{"type":"text"}]}} line shape and
// both stamp each line with a top-level RFC3339 timestamp.
//
// Messages older than since are never returned: they belong to a previous turn.
func extractTranscriptMessage(path string, since time.Time) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	var last string
	var failure string
	var sawTurn bool
	reader := bufio.NewReaderSize(file, 1<<20)
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(line) > 0 {
			var entry struct {
				Timestamp string `json:"timestamp"`
				Message   struct {
					Role         string          `json:"role"`
					Content      json.RawMessage `json:"content"`
					StopReason   string          `json:"stopReason"`
					ErrorMessage string          `json:"errorMessage"`
				} `json:"message"`
			}
			if json.Unmarshal(line, &entry) == nil && entry.Message.Role == "assistant" {
				// Drop anything written before this turn's prompt was sent.
				stamp, stampErr := time.Parse(time.RFC3339, entry.Timestamp)
				inTurn := stampErr == nil && !stamp.Before(since)
				if inTurn {
					sawTurn = true
					var blocks []struct {
						Type string `json:"type"`
						Text string `json:"text"`
					}
					var parts []string
					if json.Unmarshal(entry.Message.Content, &blocks) == nil {
						for _, block := range blocks {
							if block.Type == "text" && strings.TrimSpace(block.Text) != "" {
								parts = append(parts, strings.TrimSpace(block.Text))
							}
						}
					}
					if len(parts) > 0 {
						last = strings.Join(parts, "\n\n")
						failure = ""
					} else if detail := strings.TrimSpace(entry.Message.ErrorMessage); detail != "" {
						failure = detail
					} else if entry.Message.StopReason == "error" {
						failure = "runner reported an error with no message"
					}
				}
			}
		}
		if readErr != nil {
			break
		}
	}
	if last != "" {
		return last, nil
	}
	if failure != "" {
		return "", &transcriptTurnError{Detail: failure}
	}
	if sawTurn {
		return "", &transcriptTurnError{Detail: "runner produced an empty reply"}
	}
	return "", errors.New("no assistant text in transcript for this turn")
}

func extractOpencodeMessage(sessionID string) (string, error) {
	if !strings.HasPrefix(sessionID, "ses_") {
		return "", errors.New("not an opencode session")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	dbPath := filepath.Join(home, ".local", "share", "opencode", "opencode.db")
	if _, err := os.Stat(dbPath); err != nil {
		return "", err
	}

	// 1. Find the latest assistant message ID for this session
	msgCmd := exec.Command("sqlite3", fmt.Sprintf("file:%s?mode=ro", dbPath),
		fmt.Sprintf("SELECT m.id FROM message m WHERE m.session_id = '%s' AND json_extract(m.data, '$.role') = 'assistant' ORDER BY m.time_created DESC LIMIT 1;", sessionID),
	)
	msgOut, err := msgCmd.Output()
	if err != nil {
		return "", fmt.Errorf("sqlite message query: %w", err)
	}
	msgID := strings.TrimSpace(string(msgOut))
	if msgID == "" {
		return "", errors.New("no assistant message found")
	}

	// 2. Fetch all text parts for this message in order
	partCmd := exec.Command("sqlite3", fmt.Sprintf("file:%s?mode=ro", dbPath),
		fmt.Sprintf("SELECT json_extract(p.data, '$.text') FROM part p WHERE p.message_id = '%s' AND json_extract(p.data, '$.type') = 'text' ORDER BY p.time_created ASC;", msgID),
	)
	partOut, err := partCmd.Output()
	if err != nil {
		return "", fmt.Errorf("sqlite part query: %w", err)
	}

	text := strings.TrimSpace(string(partOut))
	if text == "" {
		return "", errors.New("no text in message parts")
	}
	return text, nil
}

func (h *Herdr) rememberSession(key, threadID string) {
	h.mu.Lock()
	h.sessions[key] = threadID
	_ = h.saveSessionsLocked()
	h.mu.Unlock()
}

func (h *Herdr) loadSessions() error {
	if h.config.SessionsFile == "" {
		return nil
	}
	data, err := os.ReadFile(h.config.SessionsFile)
	if err != nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return json.Unmarshal(data, &h.sessions)
}

func (h *Herdr) saveSessionsLocked() error {
	if h.config.SessionsFile == "" {
		return nil
	}
	data, err := json.MarshalIndent(h.sessions, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(h.config.SessionsFile), 0700); err != nil {
		return err
	}
	return fsx.AtomicWriteFile(h.config.SessionsFile, data, 0600)
}

func (h *Herdr) Interrupt(ctx context.Context, key string) (bool, error) {
	h.mu.Lock()
	turn := h.active[key]
	h.mu.Unlock()

	if turn == nil {
		return false, nil
	}

	turn.cancel()
	if turn.headless {
		return true, nil
	}
	_ = exec.CommandContext(ctx, h.config.Command, "agent", "send-keys", turn.target, "ctrl+c").Run()
	go h.teardownWorker(key, turn.tabID, turn.paneID, turn.target, true)
	return true, nil
}

// PromptLive submits a prompt into the live worker pane of an active turn
// without waiting for completion. It is the interrupt primitive for
// pane-backed workers: the text reaches the running agent session immediately;
// whether the runner steers the active turn or buffers it in its input is the
// runner's native behavior. Rejections (for example a blocked agent) surface
// as ordinary errors so callers can fall back to queued delivery.
func (h *Herdr) PromptLive(ctx context.Context, key, prompt string) error {
	if strings.TrimSpace(prompt) == "" {
		return errors.New("live prompt is empty")
	}
	h.mu.Lock()
	turn := h.active[key]
	h.mu.Unlock()
	if turn == nil {
		return errors.New("herdr turn is no longer active")
	}
	if turn.headless {
		return errors.New("a headless turn takes no live prompt; deliver it as the next turn")
	}
	promptCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(promptCtx, h.config.Command, "agent", "prompt", turn.target, prompt)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("herdr live prompt to %s: %w (%s)", turn.target, err, strings.TrimSpace(string(output)))
	}
	return nil
}

func (h *Herdr) ResetSession(key string) error {
	lock := h.lockForKey(key)
	lock.Lock()
	defer lock.Unlock()

	h.mu.Lock()
	delete(h.sessions, key)
	_ = h.saveSessionsLocked()
	h.mu.Unlock()
	return nil
}

func (h *Herdr) ThreadID(key string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sessions[key]
}

func (h *Herdr) IsActive(key string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.active[key] != nil
}

func (h *Herdr) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	for _, turn := range h.active {
		turn.cancel()
	}
	return nil
}

// herdrWaitCeiling bounds one worker turn including the initial 15-minute
// prompt wait. A genuinely hung agent that herdr still reports as working is
// left to the ops watchdog's liveness checks until then.
const herdrWaitCeiling = 2 * time.Hour

func isHerdrWaitTimeout(output string) bool {
	return strings.Contains(output, "timed out waiting for agent status")
}

type herdrPaneProcessInfoResponse struct {
	Result struct {
		ProcessInfo struct {
			ForegroundProcesses []struct {
				PID int `json:"pid"`
			} `json:"foreground_processes"`
		} `json:"process_info"`
	} `json:"result"`
}

// paneOutputBytes sums /proc rchar over the foreground process trees of a
// herdr worker pane: every read the runner and its tool children make
// (terminal output, model streaming, sockets) advances the counters, so the
// sum grows while the pane does work even when the agent sits inside one long
// tool call. Same signal and semantics as the ops runner-watchdog's pane
// liveness check.
func paneOutputBytes(ctx context.Context, run herdrRunner, paneID string) (int64, error) {
	out, err := run(ctx, "pane", "process-info", "--pane", paneID)
	if err != nil {
		return 0, err
	}
	var resp herdrPaneProcessInfoResponse
	if err := json.Unmarshal(out, &resp); err != nil {
		return 0, err
	}
	var total int64
	for _, proc := range resp.Result.ProcessInfo.ForegroundProcesses {
		total += treeReadBytes(proc.PID)
	}
	return total, nil
}

// treeReadBytes sums rchar over one pid and all descendants via
// /proc/<pid>/task/<tid>/children, tolerating processes that exit mid-walk
// (a pane's tool children come and go).
var treeReadBytes = func(pid int) int64 {
	total, seen, todo := int64(0), map[int]bool{}, []int{pid}
	for len(todo) > 0 {
		p := todo[len(todo)-1]
		todo = todo[:len(todo)-1]
		if p <= 0 || seen[p] {
			continue
		}
		seen[p] = true
		ioBytes, err := os.ReadFile(fmt.Sprintf("/proc/%d/io", p))
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(ioBytes), "\n") {
			if value, found := strings.CutPrefix(line, "rchar:"); found {
				if n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil {
					total += n
				}
			}
		}
		tasks, err := os.ReadDir(fmt.Sprintf("/proc/%d/task", p))
		if err != nil {
			continue
		}
		for _, task := range tasks {
			children, err := os.ReadFile(fmt.Sprintf("/proc/%d/task/%s/children", p, task.Name()))
			if err != nil {
				continue
			}
			for _, field := range strings.Fields(string(children)) {
				if n, err := strconv.Atoi(field); err == nil {
					todo = append(todo, n)
				}
			}
		}
	}
	return total
}

type herdrRunner func(ctx context.Context, args ...string) ([]byte, error)

// herdrNow is the clock extendHerdrWait waits on; tests replace it.
var herdrNow = time.Now

// extendHerdrWait keeps waiting on a worker whose prompt wait timed out while
// herdr still reports it working. It returns success once the agent reaches
// idle/done, hands blocked or vanished agents back with the original failure so
// the caller's blocked/error handling runs, and gives up only when the worker
// pane stops producing output. A fixed wall from turn start retired healthy
// turns that sat inside one long tool call (job j-20261010T004721Z: two 2-hour
// timeouts and two recovery attempts charged to one worker that was working
// continuously and finished fine afterwards), so the deadline re-arms whenever
// the pane's foreground processes read more bytes -- the same /proc rchar
// liveness signal the ops runner-watchdog applies as "bytes flowing -- not a
// stall". A pane that produces no output for a whole ceiling window, or a
// pane-output probe that fails, keeps the original bounded behavior.
func extendHerdrWait(ctx context.Context, target string, out []byte, err error, ceiling time.Duration, run herdrRunner, status func(string)) ([]byte, error) {
	started := herdrNow()
	deadline := started.Add(ceiling - 15*time.Minute)
	lastPaneBytes := int64(-1)
	for {
		getOut, getErr := run(ctx, "agent", "get", target)
		if getErr != nil {
			return out, err
		}
		var resp herdrAgentGetResponse
		if json.Unmarshal(getOut, &resp) != nil {
			return out, err
		}
		switch resp.Result.Agent.AgentStatus {
		case "idle", "done":
			return getOut, nil
		case "working":
		default:
			return out, err
		}
		if paneID := resp.Result.Agent.PaneID; paneID != "" {
			if paneBytes, probeErr := paneOutputBytes(ctx, run, paneID); probeErr == nil {
				if paneBytes > lastPaneBytes {
					deadline = herdrNow().Add(ceiling - 15*time.Minute)
				}
				lastPaneBytes = paneBytes
			}
		}
		remaining := deadline.Sub(herdrNow())
		if remaining <= 0 || ctx.Err() != nil {
			return out, err
		}
		step := min(15*time.Minute, remaining)
		status(fmt.Sprintf("Herdr: %s still working past the prompt wait; waiting up to %s more", target, step.Round(time.Second)))
		waitOut, waitErr := run(ctx, "agent", "wait", target, "--timeout", strconv.FormatInt(step.Milliseconds(), 10))
		if waitErr == nil {
			return waitOut, nil
		}
		if ctx.Err() != nil {
			return waitOut, waitErr
		}
		if !isHerdrWaitTimeout(string(waitOut)) {
			return waitOut, waitErr
		}
		out, err = waitOut, waitErr
	}
}

func herdrStatusInterval(key string) time.Duration {
	if isOrchestratorKey(key) {
		return 30 * time.Second
	}
	return 2 * time.Second
}

// rosterRoleForKey maps a harness session key to a roster role for turns that
// the orchestrator's document routing does not cover.
func rosterRoleForKey(key string) string {
	key = strings.TrimSpace(key)
	switch {
	case strings.HasPrefix(key, "chat:"):
		return "chat"
	case strings.HasPrefix(key, "orchestrator:notification:"):
		return "notification"
	case key == "heartbeat" || key == "orchestrator:semantic-heartbeat":
		return "heartbeat"
	}
	return ""
}

// rosterModelForKey applies roster staffing to chat, notification, and
// heartbeat turns that arrive with the configured default model. An explicit
// per-conversation model choice, a missing role, or any roster problem keeps
// the requested model.
func (h *Herdr) rosterModelForKey(key string, cfg HarnessConfig) string {
	role := rosterRoleForKey(key)
	if role == "" {
		return cfg.Model
	}
	h.mu.Lock()
	configured := strings.TrimSpace(h.config.Model)
	h.mu.Unlock()
	if requested := strings.TrimSpace(cfg.Model); requested != "" && requested != "default" && requested != configured {
		return cfg.Model
	}
	stateDir := filepath.Join(cfg.Cwd, roster.StateDirName)
	staffing, err := roster.Load(stateDir)
	if err != nil || staffing == nil {
		return cfg.Model
	}
	name := staffing.ForRole(role)
	if name == "" {
		return cfg.Model
	}
	record := observeRoster(role, key, name)
	assigned, err := staffing.Assign(stateDir, name, key, time.Now())
	if assigned == "" || (err != nil && !staffing.Has(assigned)) {
		return cfg.Model
	}
	recordRoster(record, assigned)
	return staffing.Model(assigned)
}

const (
	// defaultHerdrRunner is the runner an empty or "default" model starts. Every Werkrby seat
	// runs Hermes.
	defaultHerdrRunner = "hermes"
	// defaultHermesProfile is the profile a hermes launch that names none runs: the Werkrby code
	// worker. Without it Hermes runs the root profile, which is the front-of-house persona.
	defaultHermesProfile = "wk-code"
)

// herdrRunnerModel is model with an empty or "default" value replaced by the default runner.
func herdrRunnerModel(model string) string {
	model = strings.TrimSpace(model)
	if model == "" || model == "default" {
		return defaultHerdrRunner
	}
	return model
}

// herdrLaunch is the runner kind and launch arguments a harness model starts in a worker pane
// under the workspace cwd: an empty or "default" model is the default runner, roster staff
// ("@name") expand to their runner and args, and a hermes launch whose args carry no -p,
// --profile or --profile= gets the session key's worker profile in front (wk-chat for chat
// turns, wk-notify for notification and heartbeat turns, wk-code otherwise). Every other bare
// runner name (a retired one, or one herdr supports such as codex or cursor) is refused: only
// roster staff may name another runner. resolveTarget
// launches what this returns and runnerLaunch reads the turn's ledger from it, so the two
// cannot name different profiles.
func herdrLaunch(cwd, key, model string) (string, []string, error) {
	requested := herdrRunnerModel(model)
	switch {
	case strings.HasPrefix(requested, roster.StaffPrefix):
	case strings.Contains(requested, ":"):
		// A direct pane address ("w8:p2") launches nothing: resolveTarget prompts that pane.
		return requested, nil, nil
	case strings.EqualFold(requested, defaultHerdrRunner):
		requested = defaultHerdrRunner
	default:
		return "", nil, fmt.Errorf("runner %q is not hermes or roster staff; Werkrby launches Hermes workers only (roster staff or a hermes model)", requested)
	}
	kind, args, err := roster.Expand(filepath.Join(cwd, roster.StateDirName), requested)
	if err != nil {
		return "", nil, err
	}
	if kind == "hermes" {
		if _, explicit, _ := hermesLaunchProfile(args); !explicit {
			args = append([]string{"-p", defaultHermesProfileForKey(key)}, args...)
		}
	}
	return kind, args, nil
}

// defaultHermesProfileForKey is the profile a hermes launch that names none runs for a
// session key: the role's own worker profile for chat, notification and heartbeat turns,
// the code worker for everything else.
func defaultHermesProfileForKey(key string) string {
	switch rosterRoleForKey(key) {
	case "chat":
		return "wk-chat"
	case "notification", "heartbeat":
		return "wk-notify"
	default:
		return defaultHermesProfile
	}
}

// runnerLaunch is the runner kind and launch arguments cfg.Model starts, as resolveTarget
// launches them; a staff entry that no longer expands reports neither.
func (h *Herdr) runnerLaunch(cfg HarnessConfig, key string) (string, []string) {
	kind, args, err := herdrLaunch(cfg.Cwd, key, cfg.Model)
	if err != nil {
		return "", nil
	}
	return kind, args
}

func isOrchestratorKey(key string) bool {
	return strings.HasPrefix(key, "orchestrator:") || key == "heartbeat" || strings.HasPrefix(key, "test-")
}

func (h *Herdr) tabExists(tabID string) bool {
	if tabID == "" {
		return false
	}
	cmd := exec.Command(h.config.Command, "tab", "get", tabID)
	return cmd.Run() == nil
}

func (h *Herdr) isTabFocused(tabID string) bool {
	if tabID == "" {
		return false
	}
	out, err := exec.Command(h.config.Command, "tab", "get", tabID).Output()
	if err != nil {
		return false
	}
	var resp struct {
		Result struct {
			Tab struct {
				Focused bool `json:"focused"`
			} `json:"tab"`
		} `json:"result"`
	}
	if err := json.Unmarshal(out, &resp); err != nil {
		return false
	}
	return resp.Result.Tab.Focused
}

// cleanupCompletedTab closes a dynamic worker tab once its turn finishes.
// If the user is currently focused on the tab, it waits until they switch away
// before closing so that output is not pulled out from under their eyes.
// A later turn on the same key reuses this same registered worker and tab (an
// orchestrator retry re-sends immediately), so the close happens only while
// this key's send lock is free and no turn is active; otherwise the newer turn
// owns the worker and runs its own cleanup when it ends.
func (h *Herdr) cleanupCompletedTab(key, tabID, paneID, agentName string) {
	h.teardownWorker(key, tabID, paneID, agentName, false)
}

func (h *Herdr) teardownWorker(key, tabID, paneID, agentName string, force bool) {
	if tabID == "" && paneID == "" {
		return
	}

	if !force {
		// Brief initial settle pause before checking focus or closing
		time.Sleep(1500 * time.Millisecond)

		if tabID != "" {
			deadline := time.Now().Add(5 * time.Minute)
			for time.Now().Before(deadline) {
				h.mu.Lock()
				closed := h.closed
				h.mu.Unlock()
				if closed {
					return
				}

				// A newer turn already claimed this worker: it owns the tab.
				if h.IsActive(key) {
					return
				}

				// If tab was already closed or removed, nothing more to do
				if !h.tabExists(tabID) {
					return
				}

				// If tab is no longer focused, break and proceed to close
				if !h.isTabFocused(tabID) {
					break
				}
				time.Sleep(1500 * time.Millisecond)
			}
		}
	}

	// Hold the key's send lock across the close so a turn that is resolving its
	// target right now cannot adopt the tab while it is being torn down. A lock
	// already held means a newer turn is running on this worker: leave it alone.
	lock := h.lockForKey(key)
	deadline := time.Now().Add(5 * time.Second)
	acquired := false
	for time.Now().Before(deadline) {
		if lock.TryLock() {
			acquired = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !acquired {
		return
	}
	defer lock.Unlock()
	if !force && h.IsActive(key) {
		return
	}

	if tabID != "" {
		_ = exec.Command(h.config.Command, "tab", "close", tabID).Run()
	} else if paneID != "" {
		_ = exec.Command(h.config.Command, "pane", "close", paneID).Run()
	}

	if agentName != "" {
		_ = exec.Command(h.config.Command, "agent", "rename", agentName, "--clear").Run()
	}
}

// canonicalDir returns an absolute, symlink-free form of dir for comparing workspace roots, or ""
// when dir is empty.
func canonicalDir(dir string) string {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return ""
	}
	if abs, err := filepath.Abs(dir); err == nil {
		dir = abs
	}
	if resolved, err := filepath.EvalSymlinks(dir); err == nil {
		dir = resolved
	}
	return filepath.Clean(dir)
}

// sweepOrphanedWorkers cleans up any abandoned worker tabs or agent registrations
// from previous crashed or aborted sessions in this workspace on startup.
func (h *Herdr) sweepOrphanedWorkers(ctx context.Context) {
	// Find all dedicated worker workspaces (worker label forms)
	workerWorkspaceIDs := make(map[string]bool)
	if outW, err := exec.CommandContext(ctx, h.config.Command, "workspace", "list").Output(); err == nil {
		var wResp struct {
			Result struct {
				Workspaces []struct {
					WorkspaceID string `json:"workspace_id"`
					Label       string `json:"label"`
				} `json:"workspaces"`
			} `json:"result"`
		}
		if json.Unmarshal(outW, &wResp) == nil {
			for _, ws := range wResp.Result.Workspaces {
				if isWorkerWorkspaceLabel(ws.Label) {
					workerWorkspaceIDs[ws.WorkspaceID] = true
				}
			}
		}
	}

	out, err := exec.CommandContext(ctx, h.config.Command, "agent", "list").Output()
	if err != nil {
		return
	}
	var listResp herdrAgentListResponse
	if err := json.Unmarshal(out, &listResp); err != nil {
		return
	}
	ownRoot := canonicalDir(h.config.Cwd)
	for _, a := range listResp.Result.Agents {
		// Only sweep background worker agents (not chat sj-c-*/wk-c-*, not external agents)
		if !isOrphanWorkerName(a.Name) {
			continue
		}

		// The herdr server is shared by every Werkrby workspace on the host: only a worker that
		// provably runs in this workspace's root is an orphan of this serve. A worker with another
		// or no cwd belongs to a live serve and is left alone.
		if ownRoot == "" || canonicalDir(a.Cwd) != ownRoot {
			continue
		}

		// Only sweep if the worker is in a dedicated worker workspace
		if len(workerWorkspaceIDs) > 0 && !workerWorkspaceIDs[a.WorkspaceID] {
			continue
		}
		if a.TabID != "" {
			_ = exec.Command(h.config.Command, "tab", "close", a.TabID).Run()
		} else if a.PaneID != "" {
			_ = exec.Command(h.config.Command, "pane", "close", a.PaneID).Run()
		}
		_ = exec.Command(h.config.Command, "agent", "rename", a.Name, "--clear").Run()
	}
}

func isCLIChatWorkerName(name string) bool {
	return strings.HasPrefix(name, "wk-c-cli-") || strings.HasPrefix(name, "sj-c-cli-")
}

func (h *Herdr) isAgentTurnInFlight(a herdrAgentItem) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for key, turn := range h.active {
		if turn == nil {
			continue
		}
		if turn.target == a.Name || (a.PaneID != "" && turn.paneID == a.PaneID) || (a.TabID != "" && turn.tabID == a.TabID) {
			return true
		}
		expectedName, _ := workerNameAndLabel(key, "")
		if expectedName == a.Name {
			return true
		}
	}
	return false
}

func (h *Herdr) resolveCLIChatActivity(agentName string) (time.Time, string, bool) {
	roots := []string{
		filepath.Join(h.config.Cwd, roster.StateDirName, "history"),
		filepath.Join(h.config.Cwd, ".spynel", "history"),
		filepath.Join(h.config.Cwd, "history"),
	}

	for _, root := range roots {
		cliDir := filepath.Join(root, "cli")
		entries, err := os.ReadDir(cliDir)
		if err != nil {
			continue
		}
		store := history.New(root)
		for _, e := range entries {
			if e.IsDir() || filepath.Ext(e.Name()) != ".jsonl" {
				continue
			}
			conv := strings.TrimSuffix(e.Name(), ".jsonl")
			shortConv := conv
			shortConv = strings.TrimPrefix(shortConv, "local-")
			shortConv = sanitizeTag(shortConv)
			if shortConv == "" {
				shortConv = "main"
			}
			expectedWk := fitHerdrWorkerName("wk-c-cli-", shortConv, shortConv)
			expectedSj := fitHerdrWorkerName("sj-c-cli-", shortConv, shortConv)
			if expectedWk == agentName || expectedSj == agentName {
				key := "chat:cli:" + conv
				if act, found := store.LastActivity("cli", conv); found {
					return act, key, true
				}
				if info, err := e.Info(); err == nil {
					return info.ModTime(), key, true
				}
			}
		}
	}
	return time.Time{}, "", false
}

func (h *Herdr) sweepIdleCLIChatPanes(ctx context.Context) {
	h.mu.Lock()
	threshold := h.cliChatIdleThreshold
	closed := h.closed
	h.mu.Unlock()
	if closed || threshold <= 0 {
		return
	}

	out, err := exec.CommandContext(ctx, h.config.Command, "agent", "list").Output()
	if err != nil {
		return
	}
	var listResp herdrAgentListResponse
	if err := json.Unmarshal(out, &listResp); err != nil {
		return
	}

	ownRoot := canonicalDir(h.config.Cwd)
	now := time.Now()

	for _, a := range listResp.Result.Agents {
		// Only sweep CLI chat agent panes (not telegram, not tui, not worker agents)
		if !isCLIChatWorkerName(a.Name) {
			continue
		}

		// Must belong to this workspace
		if ownRoot == "" || canonicalDir(a.Cwd) != ownRoot {
			continue
		}

		// Only close when idle or done (working panes are never closed)
		if a.AgentStatus == "working" || (a.AgentStatus != "idle" && a.AgentStatus != "done") {
			continue
		}

		// Only close when no turn is in flight
		if h.isAgentTurnInFlight(a) {
			continue
		}

		lastActivity, convKey, ok := h.resolveCLIChatActivity(a.Name)
		if !ok {
			continue
		}

		if now.Sub(lastActivity) <= threshold {
			continue
		}

		// Idle past threshold: close using the same tab close / pane close path as orphan sweep
		if a.TabID != "" {
			_ = exec.CommandContext(ctx, h.config.Command, "tab", "close", a.TabID).Run()
		} else if a.PaneID != "" {
			_ = exec.CommandContext(ctx, h.config.Command, "pane", "close", a.PaneID).Run()
		}
		_ = exec.CommandContext(ctx, h.config.Command, "agent", "rename", a.Name, "--clear").Run()

		if convKey != "" {
			h.mu.Lock()
			delete(h.sessions, convKey)
			_ = h.saveSessionsLocked()
			h.mu.Unlock()
		}
	}
}

func (h *Herdr) runCLIChatIdleSweepLoop(ctx context.Context) {
	h.sweepIdleCLIChatPanes(ctx)

	threshold := h.cliChatIdleThreshold
	if threshold <= 0 {
		return
	}
	cadence := threshold / 4
	if cadence > time.Minute {
		cadence = time.Minute
	}
	if cadence < 5*time.Second {
		cadence = 5 * time.Second
	}

	ticker := time.NewTicker(cadence)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.mu.Lock()
			closed := h.closed
			curThreshold := h.cliChatIdleThreshold
			h.mu.Unlock()
			if closed || curThreshold <= 0 {
				return
			}
			h.sweepIdleCLIChatPanes(ctx)
		}
	}
}
