package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agent0ai/spynel/internal/agentdocs"
	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/fsx"
	"github.com/agent0ai/spynel/internal/harness"
	"github.com/agent0ai/spynel/internal/instructions"
	"github.com/agent0ai/spynel/internal/roster"
	"github.com/agent0ai/spynel/internal/shortid"
)

func defaultUsageSnapshotPath() string {
	home := os.Getenv("HOME")
	if home == "" {
		home = "/home/dajoey"
	}
	return filepath.Join(home, "ops", "state", "ai-usage", "snapshot.json")
}

var usageSnapshotPath = defaultUsageSnapshotPath()

type Lease struct {
	ID                string    `json:"id"`
	ClaimID           string    `json:"claim_id,omitempty"`
	DocumentType      string    `json:"document_type,omitempty"`
	OwnerID           string    `json:"owner_id,omitempty"`
	Route             string    `json:"route"`
	File              string    `json:"file"`
	SourceFile        string    `json:"source_file,omitempty"`
	SessionKey        string    `json:"session_key"`
	ThreadID          string    `json:"thread_id,omitempty"`
	State             string    `json:"state"`
	StartedAt         time.Time `json:"started_at"`
	HeartbeatAt       time.Time `json:"heartbeat_at"`
	RecoveryCount     int       `json:"recovery_count"`
	LastError         string    `json:"last_error,omitempty"`
	Phase             string    `json:"phase,omitempty"`
	ClaimAttempt      int       `json:"claim_attempt,omitempty"`
	ImplementerThread string    `json:"implementer_thread,omitempty"`
	// TargetModel is the model string this dispatch actually steered the
	// harness to (resolveTargetModel). recordQuotaError reads it to find the
	// usage plan the quota wall hit: roster-routed documents carry no
	// agent/runner pin, so without it every such park named "default" and
	// woke a blind hour later (found live 2026-09-28).
	TargetModel string `json:"target_model,omitempty"`
	// ReviewRisk is the task's risk when Spynel claimed it for review:
	// reviewRiskHigh or reviewRiskRoutine. Empty means unknown (an older
	// lease, or a review adopted without a claim) and never earns a credit.
	ReviewRisk             string          `json:"review_risk,omitempty"`
	TerminalHooksCompleted map[string]bool `json:"terminal_hooks_completed,omitempty"`
	// WorkCard marks a lease whose document was a work Claude card when it was claimed, so the
	// work window's personal count survives the card's file moving between folders.
	WorkCard bool `json:"work_card,omitempty"`
	// Lane is the document's front-matter lane (repairLane for lane: repair
	// tasks); it decides whether a dispatch may acquire the reserved repair
	// capacity beyond orchestrator.max_parallel (2026-10-15).
	Lane string `json:"lane,omitempty"`
	// SlotClass is the lane the claim counts against for the ops lane: slotClassGoal for goal
	// planning, review and goal-derived tasks, slotClassOps for any other task. Empty for repair
	// tasks and for leases from before the field existed.
	SlotClass string `json:"slot_class,omitempty"`
}

type ScheduledCheckpoint struct {
	ID     string    `json:"id"`
	Title  string    `json:"title"`
	At     time.Time `json:"at"`
	Reason string    `json:"reason,omitempty"`
}

type Manager struct {
	Config              config.Config
	Harness             harness.Harness
	Hooks               extensions.Runner
	Log                 func(string)
	LogError            func(component, event, message string)
	JobStarted          func(lease Lease, description string, firstAssignedAt time.Time, providerIterations, implementationAttempts int) (int, error)
	JobUpdated          func(id int, lease Lease)
	JobTimingUpdated    func(id int, firstAssignedAt time.Time, providerIterations int)
	JobExecutionUpdated func(id int, status core.ExecutionStatus)
	JobEvent            func(id int, event core.Event)
	JobFinished         func(id int)
	// PreemptJob stops the live job behind sessionKey the way an operator stop
	// does, so goal work can take its slot. Nil means the manager interrupts the
	// harness session itself.
	PreemptJob               func(ctx context.Context, sessionKey string) error
	Cleanup                  func(context.Context, int) (string, error)
	notificationAgentTimeout time.Duration
	runtimeConfigMu          sync.RWMutex
	runtimeConfig            config.Config

	mu                          sync.Mutex
	scanMu                      sync.Mutex
	inflight                    map[string]bool
	runtimeJobs                 map[string]int
	controlCancelled            map[string]int
	operatorStops               map[string]time.Time // lease id -> when an operator stopped it; survives finishRuntimeJob
	preemptions                 map[string]time.Time // lease id -> when it was paused for goal work; consumed by recordError
	workClock                   func() time.Time     // test clock for the work window; nil means time.Now
	workReserveLoggedAt         time.Time            // last "holding for the reserved slot" log line
	opsWaiting                  atomic.Bool          // an ops document is waiting for a slot (set once per scan)
	opsLaneLoggedAt             time.Time            // last "holding goal work for the ops lane" log line
	jobs                        sync.WaitGroup
	capacityMu                  sync.Mutex
	capacityActive              int
	capacityLimit               int
	repairActive                int // lane: repair dispatches holding capacity
	repairSlots                 int // reserved capacity beyond capacityLimit for lane: repair
	capacityChanged             chan struct{}
	Outbox                      *Outbox
	ownerID                     string
	scanNow                     chan struct{}
	scanTimerMu                 sync.Mutex
	holdLogMu                   sync.Mutex
	holdLogged                  map[string]time.Time
	shadow                      admissionShadowState
	runsShadow                  runsShadowState
	admissionSnapshotPath       string // tests; empty means the collector's default snapshot
	scanTimer                   *time.Timer
	scanTimerGeneration         uint64
	scanNext                    time.Time
	scanTimerChanged            chan struct{}
	heartbeatNow                func() time.Time
	heartbeatTicks              <-chan time.Time
	heartbeatTimeout            time.Duration
	heartbeatCommit             sync.Mutex
	heartbeatTerm               uint64
	heartbeatProviderActive     atomic.Bool
	heartbeatProviderMu         sync.Mutex
	heartbeatProviderDone       chan struct{}
	heartbeatProviderReleasedAt time.Time
	heartbeatRunningTerm        atomic.Uint64
	heartbeatSchedulerActive    atomic.Bool
	primaryOwned                atomic.Bool
	orchestratorEnabled         atomic.Bool
	heartbeatMinutes            atomic.Int64
	heartbeatConfigChanged      chan struct{}
	heartbeatManual             chan heartbeatManualRequest
	heartbeatConfigAcceptedAt   atomic.Int64
	heartbeatConfigGeneration   atomic.Uint64
	heartbeatAppliedGeneration  atomic.Uint64
	harnessPolicy               atomic.Value
	heartbeatTimerMu            sync.Mutex
	heartbeatTimer              *time.Timer
	heartbeatStatusMu           sync.RWMutex
	heartbeatOwned              bool
	heartbeatOwnedTerm          uint64
	heartbeatNext               time.Time
	cleanupDays                 atomic.Int64
	cleanupTicks                <-chan time.Time
	claimDocument               func(source, target, status, attemptField string, now time.Time) (Document, error)
	// startedAt bounds the post-restart window where a worker SIGTERM/SIGKILL
	// is attributed to the service lifecycle instead of the task.
	startedAt time.Time
}

// SetPrimaryOwned records whether this manager belongs to the elected
// workspace owner. The scheduler publishes its deadline separately, allowing
// status to distinguish a primary that is still starting from a secondary.
func (m *Manager) SetPrimaryOwned(owned bool) {
	m.primaryOwned.Store(owned)
}

// ApplyRuntimeConfig publishes one accepted runtime snapshot. Existing jobs
// retain their immutable admission snapshot; every subsequent scan, claim,
// notification decision, and hook dispatch observes the newest snapshot.
func (m *Manager) ApplyRuntimeConfig(cfg config.Config) {
	m.scanMu.Lock()
	defer m.scanMu.Unlock()
	m.runtimeConfigMu.Lock()
	previous := m.runtimeConfig
	// Extension controls are the sole intentional restart boundary. Keep the
	// process-start snapshot even when a transaction changes them alongside
	// live settings.
	cfg.Extensions = previous.Extensions
	m.runtimeConfig = cfg
	m.runtimeConfigMu.Unlock()
	m.harnessPolicy.Store(cfg.Harness)
	m.cleanupDays.Store(int64(cfg.Workspace.CleanupRetentionDays))
	wasEnabled := m.orchestratorEnabled.Load()
	heartbeatChanged := previous.Orchestrator.Enabled != cfg.Orchestrator.Enabled || previous.Orchestrator.SemanticHeartbeatMinutes != cfg.Orchestrator.SemanticHeartbeatMinutes
	if heartbeatChanged {
		acceptedAt := m.semanticHeartbeatNow()
		m.heartbeatCommit.Lock()
		m.orchestratorEnabled.Store(cfg.Orchestrator.Enabled)
		m.heartbeatMinutes.Store(int64(cfg.Orchestrator.SemanticHeartbeatMinutes))
		m.heartbeatConfigAcceptedAt.Store(acceptedAt.UnixNano())
		m.heartbeatConfigGeneration.Add(1)
		// Fence result commit, deadline publication, timer ownership, and tick
		// dispatch synchronously with the accepted configuration change.
		m.heartbeatTerm++
		m.stopSemanticHeartbeatTimer(nil)
		m.setSemanticHeartbeatSchedule(false, time.Time{})
		m.heartbeatCommit.Unlock()
		select {
		case m.heartbeatConfigChanged <- struct{}{}:
		default:
		}
	}
	if previous.Orchestrator.IntervalSec != cfg.Orchestrator.IntervalSec {
		// Reset the production timer synchronously. ApplyRuntimeConfig holds the
		// scan lock, so an old timer event either finishes before acceptance or
		// observes the new generation and is discarded afterward.
		m.resetScanTimer(time.Now())
	}
	if previous.Orchestrator.MaxParallel != cfg.Orchestrator.MaxParallel {
		m.capacityMu.Lock()
		m.capacityLimit = max(1, cfg.Orchestrator.MaxParallel)
		m.capacityMu.Unlock()
		m.signalCapacityChanged()
		if cfg.Orchestrator.MaxParallel > previous.Orchestrator.MaxParallel {
			m.requestScan()
		}
	}
	if previous.Orchestrator.RepairSlots != cfg.Orchestrator.RepairSlots {
		m.capacityMu.Lock()
		m.repairSlots = max(0, cfg.Orchestrator.RepairSlots)
		m.capacityMu.Unlock()
		m.signalCapacityChanged()
		if cfg.Orchestrator.RepairSlots > previous.Orchestrator.RepairSlots {
			m.requestScan()
		}
	}
	if cfg.Orchestrator.Enabled && !wasEnabled {
		m.requestScan()
	}
}

// ApplyHarnessConfig publishes prompt prefixes and task-review policy without
// perturbing the semantic-heartbeat timer. Runtime adapter settings are still
// applied separately by the application harness supervisor.
func (m *Manager) ApplyHarnessConfig(settings config.Harness) {
	m.harnessPolicy.Store(settings)
}

// stopSemanticHeartbeatTimer stops the registered production timer. Callers
// that accept configuration hold heartbeatCommit so acceptance, publication,
// and dispatch have one ordering boundary. A nil expected value stops any
// timer.
func (m *Manager) stopSemanticHeartbeatTimer(expected *time.Timer) {
	m.heartbeatTimerMu.Lock()
	defer m.heartbeatTimerMu.Unlock()
	if expected != nil && m.heartbeatTimer != expected {
		return
	}
	if m.heartbeatTimer != nil {
		if !m.heartbeatTimer.Stop() {
			select {
			case <-m.heartbeatTimer.C:
			default:
			}
		}
		m.heartbeatTimer = nil
	}
}

func New(cfg config.Config, target harness.Harness, hooks extensions.Runner) *Manager {
	parallel := cfg.Orchestrator.MaxParallel
	if parallel <= 0 {
		parallel = 1
	}
	manager := &Manager{
		Config: cfg, runtimeConfig: cfg, Harness: target, Hooks: hooks, inflight: map[string]bool{}, runtimeJobs: map[string]int{}, controlCancelled: map[string]int{}, operatorStops: map[string]time.Time{}, preemptions: map[string]time.Time{}, capacityLimit: parallel, repairSlots: max(0, cfg.Orchestrator.RepairSlots),
		Outbox:                 &Outbox{Directory: cfg.StatePath("runtime", "outbox")},
		ownerID:                fmt.Sprintf("%d-%d-%s", os.Getpid(), time.Now().UTC().UnixNano(), randomSuffix()),
		scanNow:                make(chan struct{}, 1),
		capacityChanged:        make(chan struct{}, 1),
		scanTimerChanged:       make(chan struct{}, 1),
		heartbeatConfigChanged: make(chan struct{}, 1),
		heartbeatManual:        make(chan heartbeatManualRequest),
		heartbeatNow:           time.Now, heartbeatTimeout: 5 * time.Minute,
		startedAt: time.Now().UTC(),
	}
	manager.orchestratorEnabled.Store(cfg.Orchestrator.Enabled)
	manager.heartbeatMinutes.Store(int64(cfg.Orchestrator.SemanticHeartbeatMinutes))
	manager.cleanupDays.Store(int64(cfg.Workspace.CleanupRetentionDays))
	manager.harnessPolicy.Store(cfg.Harness)
	return manager
}

func (m *Manager) SetNotificationDelivery(deliver func(context.Context, Origin, string, string) error) {
	m.Outbox.Deliver = deliver
}

func (m *Manager) Run(ctx context.Context) error {
	defer harness.SetRosterObserver(m.shadowRoster)()
	if err := m.ScanOnce(ctx); err != nil {
		m.log("orchestrator scan: " + err.Error())
	}
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		m.runSemanticHeartbeat(ctx)
	}()
	defer func() { <-heartbeatDone }()
	cleanupDone := make(chan struct{})
	go func() {
		defer close(cleanupDone)
		m.runAutomaticCleanup(ctx)
	}()
	defer func() { <-cleanupDone }()
	timer := m.startScanTimer()
	defer m.stopScanTimer(timer)
	for {
		m.scanTimerMu.Lock()
		timer = m.scanTimer
		generation := m.scanTimerGeneration
		m.scanTimerMu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-m.scanNow:
			if err := m.ScanOnce(ctx); err != nil {
				m.log("orchestrator event scan: " + err.Error())
			}
		case <-m.scanTimerChanged:
			continue
		case <-timer.C:
			if err := m.scanScheduled(ctx, generation); err != nil {
				m.log("orchestrator scan: " + err.Error())
			}
		}
	}
}

const automaticCleanupCadence = 8 * time.Hour

func (m *Manager) runAutomaticCleanup(ctx context.Context) {
	var ticker *time.Ticker
	ticks := m.cleanupTicks
	if ticks == nil {
		ticker = time.NewTicker(automaticCleanupCadence)
		defer ticker.Stop()
		ticks = ticker.C
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks:
			if !m.primaryOwned.Load() || m.Cleanup == nil {
				continue
			}
			days := int(m.cleanupDays.Load())
			if days <= 0 {
				continue
			}
			result, err := m.Cleanup(ctx, days)
			if err != nil {
				m.log("automatic cleanup skipped or failed: " + err.Error())
				continue
			}
			m.log("automatic cleanup: " + result)
		}
	}
}

func (m *Manager) startScanTimer() *time.Timer {
	m.scanTimerMu.Lock()
	defer m.scanTimerMu.Unlock()
	now := time.Now()
	if m.scanNext.IsZero() {
		m.scanNext = now.Add(m.scanInterval())
		m.scanTimerGeneration++
	}
	delay := time.Until(m.scanNext)
	if delay < 0 {
		delay = 0
	}
	m.scanTimer = time.NewTimer(delay)
	return m.scanTimer
}

func (m *Manager) stopScanTimer(_ *time.Timer) {
	m.scanTimerMu.Lock()
	defer m.scanTimerMu.Unlock()
	if m.scanTimer == nil {
		return
	}
	if !m.scanTimer.Stop() {
		select {
		case <-m.scanTimer.C:
		default:
		}
	}
	m.scanTimer = nil
}

// resetScanTimer arms the next route scan from the accepted configuration
// instant and returns only after the production timer observes the new
// generation. When Run has not started yet, startScanTimer preserves the same
// accepted deadline.
func (m *Manager) resetScanTimer(acceptedAt time.Time) {
	m.scanTimerMu.Lock()
	defer m.scanTimerMu.Unlock()
	m.scanNext = acceptedAt.Add(m.scanInterval())
	m.scanTimerGeneration++
	if m.scanTimer == nil {
		return
	}
	if !m.scanTimer.Stop() {
		select {
		case <-m.scanTimer.C:
		default:
		}
	}
	delay := time.Until(m.scanNext)
	if delay < 0 {
		delay = 0
	}
	m.scanTimer = time.NewTimer(delay)
	select {
	case m.scanTimerChanged <- struct{}{}:
	default:
	}
}

func (m *Manager) scanScheduled(ctx context.Context, generation uint64) error {
	m.scanMu.Lock()
	defer m.scanMu.Unlock()
	m.scanTimerMu.Lock()
	current := m.scanTimerGeneration
	m.scanTimerMu.Unlock()
	if current != generation {
		return nil
	}
	err := m.scanOnce(ctx)
	m.resetScanTimer(time.Now())
	return err
}

func (m *Manager) runtimeSnapshot() config.Config {
	m.runtimeConfigMu.RLock()
	defer m.runtimeConfigMu.RUnlock()
	return m.runtimeConfig
}

func (m *Manager) scanInterval() time.Duration {
	seconds := m.runtimeSnapshot().Orchestrator.IntervalSec
	if seconds < 1 {
		seconds = 1
	}
	return time.Duration(seconds) * time.Second
}

func (m *Manager) signalCapacityChanged() {
	select {
	case m.capacityChanged <- struct{}{}:
	default:
	}
}

func (m *Manager) acquireCapacity(ctx context.Context, isRepair bool) bool {
	for {
		m.capacityMu.Lock()
		limit := m.capacityLimit
		if isRepair {
			limit += m.repairSlots
		}
		if m.capacityActive < limit && (!isRepair || m.repairActive < m.repairSlots) {
			m.capacityActive++
			if isRepair {
				m.repairActive++
			}
			m.capacityMu.Unlock()
			return true
		}
		m.capacityMu.Unlock()
		select {
		case <-ctx.Done():
			return false
		case <-m.capacityChanged:
		}
	}
}

func (m *Manager) releaseCapacity(isRepair bool) {
	m.capacityMu.Lock()
	if m.capacityActive > 0 {
		m.capacityActive--
	}
	if isRepair && m.repairActive > 0 {
		m.repairActive--
	}
	m.capacityMu.Unlock()
	m.signalCapacityChanged()
}

func (m *Manager) requestScan() {
	select {
	case m.scanNow <- struct{}{}:
	default:
	}
}

func (m *Manager) ScanOnce(ctx context.Context) error {
	m.scanMu.Lock()
	defer m.scanMu.Unlock()
	return m.scanOnce(ctx)
}

func (m *Manager) scanOnce(ctx context.Context) error {
	cfg := m.runtimeSnapshot()
	if !m.orchestratorEnabled.Load() {
		return nil
	}
	if err := os.MkdirAll(m.leaseDirectory(), 0o700); err != nil {
		return err
	}
	if err := m.ensureRouteDirectories(); err != nil {
		return err
	}
	m.refreshOpsWaiting(cfg)
	if err := m.resumeInterruptedClaims(ctx); err != nil {
		return err
	}
	if err := m.reconcileTransitions(ctx); err != nil {
		return err
	}
	if err := m.recoverStale(ctx); err != nil {
		return err
	}
	if err := m.recoverOrphanClaims(ctx); err != nil {
		return err
	}
	if err := m.wakeWaitingDocuments(ctx); err != nil {
		return err
	}
	if err := m.advanceActiveGoals(); err != nil {
		return err
	}
	// Work window: work Claude cards claim before any personal claim in any queue.
	if m.workWindowActive(cfg) {
		taskRoute, _ := routeByName("tasks")
		taskBase := filepath.Dir(cfg.Resolve(taskRoute.Source))
		if err := m.scanPhaseQueueMode(ctx, taskRoute, filepath.Join(taskBase, "review"), filepath.Join(taskBase, "reviewing"), phaseTaskReview, true); err != nil {
			return err
		}
		if err := m.scanPhaseQueueMode(ctx, taskRoute, cfg.Resolve(taskRoute.Source), cfg.Resolve(taskRoute.Working), phaseTaskImplementation, true); err != nil {
			return fmt.Errorf("route tasks (work cards): %w", err)
		}
	}
	// Finish before starting: review queues claim free capacity ahead of new
	// implementation and planning. Implementation-first starved reviews behind
	// a whole batch (spyjo-observer 2026-09-17: cronaudit-01/02 sat in review
	// while 03/04 started), so nothing finished until the batch drained,
	// reviewer findings could not inform later tasks, and reviews verified
	// systems that later tasks had already changed.
	for _, route := range workflowRoutesGoalsFirst() {
		base := filepath.Dir(cfg.Resolve(route.Source))
		var phase string
		switch route.Name {
		case "tasks":
			phase = phaseTaskReview
		case "goals":
			phase = phaseGoalReview
		default:
			continue
		}
		if err := m.scanPhaseQueue(ctx, route, filepath.Join(base, "review"), filepath.Join(base, "reviewing"), phase); err != nil {
			return err
		}
	}
	for _, route := range workflowRoutesGoalsFirst() {
		var err error
		switch route.Name {
		case "tasks":
			err = m.scanPhaseQueue(ctx, route, cfg.Resolve(route.Source), cfg.Resolve(route.Working), phaseTaskImplementation)
		case "goals":
			err = m.scanPhaseQueue(ctx, route, cfg.Resolve(route.Source), cfg.Resolve(route.Working), phaseGoalPlanning)
		}
		if err != nil {
			return fmt.Errorf("route %s: %w", route.Name, err)
		}
	}
	if err := m.Outbox.Process(ctx); err != nil {
		m.log("notification delivery deferred: " + err.Error())
	}
	return nil
}

func (m *Manager) scanPhaseQueue(ctx context.Context, route workflowRoute, sourceDir, claimedDir, phase string) error {
	return m.scanPhaseQueueMode(ctx, route, sourceDir, claimedDir, phase, false)
}

// scanPhaseQueueMode is scanPhaseQueue; workOnly restricts it to work Claude cards, which the
// work window scans ahead of every other queue.
func (m *Manager) scanPhaseQueueMode(ctx context.Context, route workflowRoute, sourceDir, claimedDir, phase string, workOnly bool) error {
	entries, err := os.ReadDir(sourceDir)
	if os.IsNotExist(err) {
		return os.MkdirAll(sourceDir, 0o700)
	}
	if err != nil {
		return err
	}
	type queueCandidate struct {
		name     string
		document Document
	}
	cfg := m.runtimeSnapshot()
	windowOpen := m.workWindowActive(cfg)
	var candidates []queueCandidate
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".md") || entry.Name() == "AGENTS.md" {
			continue
		}
		source := filepath.Join(sourceDir, entry.Name())
		document, readErr := ReadDocument(source)
		if readErr != nil {
			m.reportUnparseableDocument(source, readErr)
			continue
		}
		if !documentDue(document, time.Now(), phase) {
			continue
		}
		if workOnly && !documentIsWorkCard(document) {
			continue
		}
		candidates = append(candidates, queueCandidate{name: entry.Name(), document: document})
	}
	// Claim oldest first. ReadDir returns filename order, which made claim
	// priority an accident of naming (2026-10-03: date-named tasks-20261003-*
	// filed 15:18Z-15:23Z overtook Helm-filed tasks waiting since 14:06Z
	// because digits sort before letters). Front-matter created_at decides,
	// equal timestamps fall back to filename order, and a document without a
	// usable created_at sorts last so one bad document cannot stall the board.
	sort.SliceStable(candidates, func(i, j int) bool {
		// While the work window is open, work Claude cards claim before
		// everything else, goal work included.
		if windowOpen {
			wi, wj := documentIsWorkCard(candidates[i].document), documentIsWorkCard(candidates[j].document)
			if wi != wj {
				return wi
			}
		}
		// Goal-derived documents (goal_id) claim before standalone ones: the
		// current goal is paramount (Joey, 2026-10-03), including tasks the
		// goal files after today. Within a class the order is unchanged.
		gi, gj := documentIsGoalDerived(candidates[i].document), documentIsGoalDerived(candidates[j].document)
		if gi != gj {
			return gi
		}
		ti, oki := documentCreatedAt(candidates[i].document)
		tj, okj := documentCreatedAt(candidates[j].document)
		switch {
		case oki && !okj:
			return true
		case !oki && okj:
			return false
		case oki && okj && !ti.Equal(tj):
			return ti.Before(tj)
		default:
			return candidates[i].name < candidates[j].name
		}
	})
	personalClaims := -1 // in-flight personal claims; counted on first use while the window is open
	goalPauseAttempted := false // one standalone pause per scan, as the break below used to promise
	for _, candidate := range candidates {
		source := filepath.Join(sourceDir, candidate.name)
		document := candidate.document
		documentID := documentID(document)
		if documentID == "" {
			documentID = leaseID(route.Name+":"+phase, source)
		}
		key := leaseID(route.Name+":"+phase, documentID)
		leased, leaseErr := m.claimBlockedByLease(route, phase, candidate.name, key)
		if leaseErr != nil {
			return leaseErr
		}
		if leased {
			continue
		}
		// A paced seat's work waits where it is (no claim, no attempt, no fallback) until
		// its hold lifts. Checked before capacity so held goal work never pauses a running
		// standalone task for a slot it will not use.
		if m.rosterHeld("queue", route, phase, candidate.name, document) {
			continue
		}
		// Work window: personal claims may not take the slot reserved for work
		// Claude cards. Skipped, not claimed; a later work card in this queue
		// still can be.
		if windowOpen && !documentIsWorkCard(document) {
			if personalClaims < 0 {
				personalClaims = m.personalInflight()
			}
			if personalCap := m.personalSlotCap(cfg); personalClaims >= personalCap {
				m.logWorkReserveHold(candidate.name, personalClaims, personalCap)
				continue
			}
		}
		// Ops lane: goal claims may not take the slots waiting ops work is short of. Skipped, not
		// claimed, and no running task is paused for it; the ops candidates after it still can be.
		if m.opsLaneHolds(route, document) {
			m.logOpsLaneHold(candidate.name)
			continue
		}
		isRepair := documentIsRepair(document)
		if !m.canAdmitClaim(isRepair) {
			// Every slot (including the repair reserve for repair work) is
			// taken. Goal work is the top priority: it claims first (sort order
			// above) and, when it cannot get a slot, a running standalone task
			// is paused so the next scan can claim it (at most one pause per
			// scan). The scan itself keeps walking: idle slots are never held
			// back for goal work, and a later lane: repair candidate must still
			// reach the reserved slot while the house is stuck (2026-10-15).
			if route.Name == "goals" || documentIsGoalDerived(document) {
				if !goalPauseAttempted {
					goalPauseAttempted = true
					m.pauseStandaloneForGoalWork(ctx)
				}
				continue
			}
			if isRepair {
				break
			}
			// A normal task cannot claim while the normal slots are full, but a
			// later lane: repair candidate may still take the reserved slot, so
			// keep scanning instead of stopping at the first full house
			// (2026-10-15).
			continue
		}
		if phase == phaseTaskReview && m.harnessSettings().Reviews == config.TaskReviewsNever {
			target := filepath.Join(filepath.Dir(sourceDir), "todo", candidate.name)
			note := "Independent task review is disabled by harness.reviews=never; Werkrby returned this task to todo so an implementation session can record the required direct-completion evidence."
			if err := moveDocumentWithProgress(source, target, "todo", time.Now().UTC(), note); err != nil {
				m.log("bypass disabled task review " + source + ": " + err.Error())
			}
			continue
		}
		target := filepath.Join(claimedDir, candidate.name)
		now := time.Now().UTC()
		attemptField := phaseAttemptField(phase)
		// Same rule the claim applies under the provider-turn lock below; the
		// journaled lease must carry the attempt the document will record.
		attempt, _ := claimAttempt(document.FrontMatter, attemptField)
		lease := Lease{
			ID: key, ClaimID: key, DocumentType: strings.TrimSuffix(route.Name, "s"), Route: route.Name,
			OwnerID: m.ownerID,
			File:    target, SourceFile: source, SessionKey: phaseSessionKey(route.Name, documentID, phase, attempt),
			State: "claiming", Phase: phase, ClaimAttempt: attempt, StartedAt: now, HeartbeatAt: now,
			WorkCard: documentIsWorkCard(document), SlotClass: documentSlotClass(route, document),
		}
		if isRepair {
			lease.Lane = repairLane
		}
		if phase == phaseTaskReview {
			lease.ImplementerThread, _ = document.FrontMatter["implementation_thread"].(string)
			lease.ReviewRisk = reviewRiskAtClaim(document)
		}
		if err := m.saveLease(lease); err != nil {
			return err
		}
		claimed, claimErr := m.claimPhaseDocument(source, target, phaseClaimedStatus(phase), attemptField, now)
		if claimErr != nil {
			_, targetErr := os.Stat(target)
			_, sourceErr := os.Stat(source)
			if targetErr != nil || !os.IsNotExist(sourceErr) {
				_ = os.Remove(m.leasePath(key))
			}
			m.log("claim " + source + ": " + claimErr.Error())
			continue
		}
		claimed.FrontMatter[attemptField] = attempt
		if err := WriteDocument(target, claimed); err != nil {
			return err
		}
		if personalClaims >= 0 && !documentIsWorkCard(document) {
			personalClaims++
		}
		lease.State = "processing"
		lease.SourceFile = ""
		if err := m.saveLease(lease); err != nil {
			return err
		}
		if phase == phaseTaskImplementation && m.runtimeSnapshot().Extensions.Enabled {
			output, hookErr := m.Hooks.Run(ctx, "task.claimed", map[string]any{"route": route.Name, "phase": phase, "file": target, "id": documentID})
			if hookErr != nil {
				m.recordError(ctx, lease, hookErr)
				continue
			}
			if output.Cancel {
				lease.State = "hook_cancelled"
				lease.LastError = output.Message
				if err := m.saveLease(lease); err != nil {
					m.log("save hook-cancelled lease: " + err.Error())
				}
				continue
			}
		}
		m.dispatch(ctx, route, lease, false)
	}
	return nil
}

// claimBlockedByLease reports whether a queued document cannot be claimed because a lease or a
// running job still owns it. A provider can move a document after the reconciliation pass but
// before the queue is scanned; the next phase must not be claimed while the preceding phase still
// owns the same durable document, so its lease reconciles the move first. The ops lane's waiting
// check shares this rule so it never counts a document the claim loop will skip.
func (m *Manager) claimBlockedByLease(route workflowRoute, phase, name, key string) (bool, error) {
	_, documentLeased, err := m.leaseForDocument(route.Name, name, "", "")
	if err != nil {
		return false, err
	}
	return m.isInflight(key) || m.leaseExists(key) || documentLeased, nil
}

// planningHoldDeferredField marks a goal whose fresh planning round was deferred by a pace hold.
const planningHoldDeferredField = "planning_hold_deferred"

func (m *Manager) startExistingClaim(ctx context.Context, route workflowRoute, path, phase string, recovery, incrementAttempt bool) error {
	document, err := ReadDocument(path)
	if err != nil {
		return m.skipUnreadableDocument(path, err)
	}
	id := documentID(document)
	if id == "" {
		id = leaseID(route.Name+":"+phase, path)
	}
	key := leaseID(route.Name+":"+phase, id)
	if m.leaseExists(key) || m.hasLeaseForFile(path) || m.isInflight(key) {
		return nil
	}
	// A goal reaching planning outside scanPhaseQueue (review -> planning, a planning wake, an
	// orphaned planning claim) honors the pace hold here: no attempt, no lease, no session. A
	// deferred fresh round is marked so the start that follows once the hold lifts
	// (recoverOrphanClaims, every scan) is that fresh round, not a recovery.
	if phase == phaseGoalPlanning {
		deferred, _ := document.FrontMatter[planningHoldDeferredField].(bool)
		if deferred {
			recovery, incrementAttempt = false, true
		}
		paced := m.rosterHeld("start", route, phase, filepath.Base(path), document)
		// Work window: a fresh planning round is personal work and may not take the slot kept for
		// work Claude cards. A recovery of a round already running is never held.
		windowHeld := !paced && incrementAttempt && m.workWindowHolds(document)
		// Ops lane: a fresh planning round may not take the slots waiting ops work is short of.
		opsHeld := !paced && !windowHeld && incrementAttempt && m.opsLaneHolds(route, document)
		if paced || windowHeld || opsHeld {
			if incrementAttempt && !deferred {
				note := "Spynel deferred this goal's planning round: its seat is on a quota pace hold. No attempt was spent; planning starts when the hold lifts."
				if windowHeld {
					note = "Spynel deferred this goal's planning round: the work window is open and the slot left for personal work is taken. No attempt was spent; planning starts when a slot frees or the window closes."
				}
				if opsHeld {
					note = "Spynel deferred this goal's planning round: normal-operations tasks are waiting and the ops lane is short of its reserved slots. No attempt was spent; planning starts when a slot frees."
				}
				if err := updateDocumentProgressWithUpdate(path, time.Now().UTC(), note, func(frontMatter map[string]any) {
					frontMatter[planningHoldDeferredField] = true
				}); err != nil {
					return err
				}
			}
			return nil
		}
		delete(document.FrontMatter, planningHoldDeferredField)
	}
	field := phaseAttemptField(phase)
	attempt := numberValue(document.FrontMatter[field])
	if incrementAttempt || attempt == 0 {
		attempt++
		document.FrontMatter[field] = attempt
		document.FrontMatter["status"] = phaseClaimedStatus(phase)
		document.FrontMatter["updated_at"] = time.Now().UTC().Format(time.RFC3339)
		if err := WriteDocument(path, document); err != nil {
			return err
		}
	}
	now := time.Now().UTC()
	state := "processing"
	if recovery {
		state = "recovering"
	}
	lease := Lease{
		ID: key, ClaimID: key, DocumentType: strings.TrimSuffix(route.Name, "s"), Route: route.Name,
		OwnerID: m.ownerID,
		File:    path, SessionKey: phaseSessionKey(route.Name, id, phase, attempt), State: state,
		Phase: phase, ClaimAttempt: attempt, StartedAt: now, HeartbeatAt: now,
		WorkCard: documentIsWorkCard(document), SlotClass: documentSlotClass(route, document),
	}
	if documentIsRepair(document) {
		lease.Lane = repairLane
	}
	if phase == phaseTaskReview {
		lease.ImplementerThread, _ = document.FrontMatter["implementation_thread"].(string)
	}
	if err := m.saveLease(lease); err != nil {
		return err
	}
	m.dispatch(ctx, route, lease, recovery)
	return nil
}

func numberValue(value any) int {
	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	}
	return 0
}

func (m *Manager) ensureRouteDirectories() error {
	for _, path := range m.WorkflowDirectories() {
		if err := os.MkdirAll(path, 0700); err != nil {
			return err
		}
	}

	return nil
}

func (m *Manager) dispatch(ctx context.Context, route workflowRoute, lease Lease, recovery bool) {
	m.setInflight(lease.ID, true)
	isRepair := lease.Lane == repairLane
	m.jobs.Add(1)
	go func() {
		defer m.jobs.Done()
		defer func() {
			m.setInflight(lease.ID, false)
			// Harness completion is the event that makes agent-authored durable
			// transitions observable. Wake the manager immediately instead of
			// waiting for the periodic recovery scan.
			m.requestScan()
		}()
		if !m.acquireCapacity(ctx, isRepair) {
			return
		}
		defer m.releaseCapacity(isRepair)
		promptPath := route.Prompt
		if recovery {
			promptPath = route.RecoveryPrompt
		} else if lease.Phase == phaseTaskReview || lease.Phase == phaseGoalReview || lease.Phase == "review" {
			promptPath = route.ReviewPrompt
		}
		if recovery {
			// Recovery owns a new execution; retire any ended local handle so
			// its terminal status cannot fence the resumed job. The archive
			// preserves this workflow dispatch's public number and history.
			m.finishRuntimeJob(lease.ID)
			lease.State = "recovering"
			lease.LastError = ""
			lease.HeartbeatAt = time.Now().UTC()
			if err := m.saveLease(lease); err != nil {
				m.recordError(ctx, lease, err)
				return
			}
			recoveryAttempt := lease.RecoveryCount + 1
			note := fmt.Sprintf("Spynel started recovery attempt %d for %s after its durable execution ownership required reconciliation; the recovery agent must record its findings and outcome here.", recoveryAttempt, strings.ReplaceAll(normalizeLeasePhase(route.Name, lease.Phase), "_", " "))
			if err := updateDocumentProgress(lease.File, time.Now().UTC(), note); err != nil {
				m.recordError(ctx, lease, fmt.Errorf("record recovery progress: %w", err))
				return
			}
		}
		prompt, err := m.renderPrompt(route, lease, promptPath)
		if err != nil {
			m.recordError(ctx, lease, err)
			return
		}
		harnessSettings := m.harnessSettings()
		prompt = config.PrependAgentPrefix(m.agentPrefix(lease.Phase, harnessSettings), prompt)
		firstAssignedAt, providerIterations, err := ReserveProviderTurn(lease.File, time.Now().UTC())
		if err != nil {
			m.recordError(ctx, lease, err)
			return
		}
		jobID := 0
		if m.JobStarted != nil {
			implementationAttempts := 0
			if route.Name == "tasks" {
				if document, readErr := ReadDocument(lease.File); readErr == nil {
					implementationAttempts = numberValue(document.FrontMatter["attempt"])
				}
			}
			jobID, err = m.JobStarted(lease, filepath.Base(lease.File), firstAssignedAt, providerIterations, implementationAttempts)
			if err != nil {
				m.recordError(ctx, lease, err)
				return
			}
			m.setRuntimeJob(lease.ID, jobID)
		}
		finish := func() { m.finishRuntimeJob(lease.ID) }
		// Admission and asynchronous events must not overwrite each other's
		// lease state (especially a terminal event racing Send's return).
		var lifecycleMu sync.Mutex
		// Only a terminal FINAL event proves the turn actually ran to
		// completion. A terminal error is what a harness emits immediately
		// beside the very send error an in-place retry classifies, so it must
		// not veto that retry.
		var terminalFinalSeen bool
		// finalText is the turn's final reply; an explicit-outcome turn may carry its outcome there.
		var finalText string
		// run records this dispatch's provider turn when orchestrator.runs_mode is shadow; it is set
		// just before the first send, which is the only thing that makes emit run.
		var run *runRecorder
		emit := func(event core.Event) {
			lifecycleMu.Lock()
			defer lifecycleMu.Unlock()
			if jobID > 0 && m.runtimeJob(lease.ID) != jobID {
				return
			}
			terminal := event.Done && (event.Kind == core.EventFinal || event.Kind == core.EventError)
			if terminal {
				run.terminal(ctx, lease, event)
			}
			if terminal && event.Kind == core.EventFinal {
				terminalFinalSeen = true
				finalText = event.Text
				if event.FinalText != nil {
					finalText = *event.FinalText
				}
			}
			if jobID > 0 && m.JobEvent != nil {
				m.JobEvent(jobID, event)
			}
			// Runtime job bookkeeping is process-local and must not depend on
			// the durable lease still existing. A fast agent can move its task,
			// then a concurrent recovery scan can remove the obsolete lease
			// before the provider emits its final event.
			current, err := m.loadLease(lease.ID)
			if err != nil {
				if terminal {
					finish()
				}
				return
			}
			current.HeartbeatAt = time.Now().UTC()
			if event.ThreadID != "" {
				current.ThreadID = event.ThreadID
			}
			if event.Kind == core.EventError {
				current.LastError = event.Text
			}
			if terminal {
				current.State = "awaiting_transition"
			} else if current.State == "recovering" && event.Execution != nil && event.Execution.State == "running" {
				current.State = "processing"
			}
			if err := m.saveLease(current); err != nil {
				m.log("save lease event state: " + err.Error())
			}
			if jobID > 0 && m.JobUpdated != nil {
				m.JobUpdated(jobID, current)
			}
			if jobID > 0 && event.Execution != nil && !(terminal && event.Execution.State == "finishing") && m.JobExecutionUpdated != nil {
				m.JobExecutionUpdated(jobID, *event.Execution)
			}
			// Terminal provider completion remains visible as awaiting_transition
			// until reconciliation observes the agent-authored durable file move.
		}
		targetModel := m.resolveTargetModel(route, lease)
		// Record what this dispatch actually runs on before the send: a quota
		// park must wake at the plan's real reset, and the model string (not
		// the document, which roster-routed work never pins) is the only
		// place the chosen seat is named. The send has not started, so no
		// event save can race this one.
		if lease.TargetModel != targetModel {
			lease.TargetModel = targetModel
			if saveErr := m.saveLease(lease); saveErr != nil {
				m.log("save lease target model: " + saveErr.Error())
			}
		}
		// A seat with explicit_outcome returns this turn's outcome as data and Werkrby applies it,
		// so its prompt carries that contract instead of the file-move steps. Any problem preparing
		// it keeps the ordinary prompt and the ordinary file-move reconcile for this turn.
		turn := m.explicitOutcomeTurn(route, lease, targetModel)
		if turn != nil {
			explicitPrompt, prepareErr := m.renderPromptFor(route, lease, promptPath, turn)
			if prepareErr == nil {
				prepareErr = turn.prepare()
			}
			if prepareErr != nil {
				m.log("explicit outcome off for this turn of " + lease.File + ": " + prepareErr.Error())
				turn = nil
			} else {
				prompt = config.PrependAgentPrefix(m.agentPrefix(lease.Phase, harnessSettings), explicitPrompt)
				// The run directory lives for this dispatch only, whatever way it ends.
				defer func() { _ = os.RemoveAll(turn.Dir) }()
			}
		}
		var threadID string
		var steered bool
		run = m.runBegin(lease, targetModel)
		for sendAttempt := 0; ; sendAttempt++ {
			if ms, ok := m.Harness.(interface {
				SendWithModel(context.Context, string, string, string, core.Emit) (string, bool, error)
			}); ok && targetModel != "" {
				threadID, steered, err = ms.SendWithModel(ctx, lease.SessionKey, prompt, targetModel, emit)
			} else {
				threadID, steered, err = m.Harness.Send(ctx, lease.SessionKey, prompt, emit)
			}
			if err == nil || sendAttempt >= 1 || ctx.Err() != nil {
				break
			}
			// A provider refusing work right now (backend overload, "please
			// retry") is not a failed worker: retry once in place so it costs
			// neither a re-claim (attempt++ and an escalation rung) nor a
			// fallback seat. A dead runner (signal/exit-code errors) never
			// matches here and fails over immediately, and a turn that already
			// produced its final response belongs to transition
			// reconciliation, not to a retry.
			lifecycleMu.Lock()
			eligible := !terminalFinalSeen && transientProviderError(err)
			if eligible {
				// The refused attempt emitted its own terminal error, which
				// parked the lease in awaiting_transition under that text. The
				// retry is the same turn, so hand the lease back to live work
				// before dispatching it.
				m.restoreLeaseForRetry(lease.ID, recovery)
				run.retrying()
			}
			lifecycleMu.Unlock()
			if !eligible {
				break
			}
			retryText := "The provider refused this turn as busy; retrying once on the same worker."
			retryDetail := "transient provider refusal: retrying once in place"
			if strings.Contains(strings.ToLower(err.Error()), "prompt_not_delivered") {
				m.log(fmt.Sprintf("prompt_not_delivered on %s, retrying once in place: %v", lease.File, err))
				retryText = "Prompt was not delivered to worker; retrying once in place."
				retryDetail = "prompt_not_delivered: retrying once in place"
			} else {
				m.log(fmt.Sprintf("transient provider refusal on %s, retrying once in place: %v", lease.File, err))
			}
			emit(core.Event{
				Kind:      core.EventStatus,
				Text:      retryText,
				Execution: &core.ExecutionStatus{State: "running", Detail: retryDetail},
			})
		}
		run.sent(ctx, lease, err)
		lifecycleMu.Lock()
		defer lifecycleMu.Unlock()
		if err != nil {
			// A worker on an explicit-outcome seat may have written its outcome before the run
			// failed (a refused final reply); that outcome is the turn's result.
			if turn != nil && m.applyWrittenOutcomeAfterSendError(route, lease, turn, err) {
				return
			}
			if jobID > 0 && m.JobEvent != nil {
				m.JobEvent(jobID, core.Event{Kind: core.EventError, Text: err.Error(), Done: true})
			}
			if jobID > 0 && m.JobExecutionUpdated != nil {
				m.JobExecutionUpdated(jobID, core.ExecutionStatus{State: "error", Detail: err.Error()})
			}
			finish()
			m.recordError(ctx, lease, err)
			return
		}
		current, loadErr := m.loadLease(lease.ID)
		if os.IsNotExist(loadErr) {
			return
		}
		if loadErr == nil {
			lease = current
		}
		lease.ThreadID = threadID
		lease.HeartbeatAt = time.Now().UTC()
		if recovery {
			lease.RecoveryCount++
			if lease.State == "recovering" {
				lease.State = "processing"
			}
		}
		if err := m.saveLease(lease); err != nil {
			m.log("save lease dispatch state: " + err.Error())
		}
		if jobID > 0 && m.JobUpdated != nil {
			m.JobUpdated(jobID, lease)
		}
		verb := "started"
		if steered {
			verb = "steered"
		}
		m.log(fmt.Sprintf("%s %s in thread %s", verb, lease.File, shortid.Display(threadID)))
		if turn != nil && !steered {
			m.applyExplicitOutcome(route, lease, turn, finalText)
		}
	}()
}

func (m *Manager) reconcileTransitions(ctx context.Context) error {
	leases, err := m.loadLeases()
	if err != nil {
		return err
	}
	for _, lease := range leases {
		if lease.State == "claiming" {
			continue
		}
		if _, statErr := os.Stat(lease.File); statErr == nil {
			continue
		} else if !os.IsNotExist(statErr) {
			return statErr
		}
		route, ok := routeByName(lease.Route)
		if !ok {
			continue
		}
		base := filepath.Dir(m.Config.Resolve(route.Source))
		name := filepath.Base(lease.File)
		status, path := "", ""
		for _, candidate := range route.AllowedNext {
			test := filepath.Join(base, candidate, name)
			if _, e := os.Stat(test); e == nil {
				status, path = candidate, test
				break
			}
		}
		if status == "" {
			_ = os.Remove(m.leasePath(lease.ID))
			m.finishRuntimeJob(lease.ID)
			continue
		}
		phase := normalizeLeasePhase(route.Name, lease.Phase)
		switch route.Name {
		case "tasks":
			status, path, err = m.reconcileTaskTransition(ctx, route, lease, phase, status, path)
		case "goals":
			status, path, err = m.reconcileGoalTransition(ctx, route, lease, phase, status, path)
		}
		if err != nil {
			// Keep the lease so a later scan can finish the transition (once
			// the document is repaired or the hook recovers); do not abort the
			// rest of the board. An unreadable document is already logged. A
			// cancelled context (shutdown) still aborts the scan.
			if ctx.Err() != nil {
				return err
			}
			if !errors.Is(err, errUnreadableDocument) {
				m.log(fmt.Sprintf("transition for %s failed, lease kept for the next scan: %v", lease.File, err))
			}
			continue
		}
		_ = os.Remove(m.leasePath(lease.ID))
		m.finishRuntimeJob(lease.ID)
		if route.Name == "goals" && phase == phaseGoalReview && status == "planning" {
			if err := m.startExistingClaim(ctx, route, path, phaseGoalPlanning, false, true); err != nil {
				if errors.Is(err, errUnreadableDocument) {
					continue
				}
				return err
			}
		}
	}
	return nil
}

func normalizeLeasePhase(routeName, phase string) string {
	if phase == "review" {
		if routeName == "goals" {
			return phaseGoalReview
		}
		return phaseTaskReview
	}
	if phase == "implementation" {
		if routeName == "goals" {
			return phaseGoalPlanning
		}
		return phaseTaskImplementation
	}
	return phase
}

func (m *Manager) reconcileTaskTransition(ctx context.Context, route workflowRoute, lease Lease, phase, status, path string) (string, string, error) {
	base := filepath.Dir(m.Config.Resolve(route.Source))
	name := filepath.Base(path)
	// Whatever this turn wrote, its transition carries no resume credit
	// unless Spynel grants one below for a park or a format-only rejection.
	if err := setResumeCredit(path, false); err != nil {
		m.log("clear resume credit " + path + ": " + err.Error())
	}
	if phase == phaseTaskImplementation {
		document, readErr := ReadDocument(path)
		if readErr != nil {
			return status, path, m.skipUnreadableDocument(path, readErr)
		}
		policy, policyErr := TaskPolicyFromDocument(document)
		if policyErr != nil {
			// Persist the conservative interpretation so retries and inspection
			// see the same unambiguous policy.
			document.FrontMatter["review_required"] = true
			if err := WriteDocument(path, document); err != nil {
				return status, path, err
			}
			m.log("normalized unsafe task review policy in " + path + ": " + policyErr.Error())
		}
		// Older managers, or an in-flight scan that crossed a provider move,
		// may already have claimed review before this implementation lease is
		// reconciled. Once a review lease exists it owns every subsequent
		// transition for the document. Preserve its file and backfill the
		// implementer identity instead of stealing the claim back to review.
		reviewLease, claimed, leaseErr := m.leaseForDocument(route.Name, name, phaseTaskReview, lease.ID)
		if leaseErr != nil {
			return status, path, leaseErr
		}
		if claimed {
			document.FrontMatter["implementation_thread"] = lease.ThreadID
			document.FrontMatter["implementation_session"] = lease.SessionKey
			if err := WriteDocument(path, document); err != nil {
				return status, path, err
			}
			reviewLease.ImplementerThread = lease.ThreadID
			if err := m.saveLease(reviewLease); err != nil {
				return status, path, err
			}
			return status, path, nil
		}
		harnessSettings := m.harnessSettings()
		reviewRequired := harnessSettings.EffectiveTaskReviewRequired(policy.ReviewRequired)
		if harnessSettings.Reviews == config.TaskReviewsNever && (status == "review" || status == "reviewing") {
			var err error
			status, path, err = m.redirectTransition(path, statusPath(base, "todo", name), "todo", "Independent task review is disabled; Werkrby returned this task to todo to record direct-completion evidence.")
			if err != nil {
				return status, path, err
			}
		}
		if status == "reviewing" || (status == "done" && reviewRequired) {
			var err error
			status, path, err = m.redirectTransition(path, statusPath(base, "review", name), "review", "Implementation cannot bypass independent review; Werkrby redirected this task to review.")
			if err != nil {
				return status, path, err
			}
		}
		if !map[string]bool{"todo": true, "review": true, "waiting": true, "done": true, "failed": true, "cancelled": true}[status] {
			var err error
			status, path, err = m.redirectTransition(path, statusPath(base, "todo", name), "todo", "Invalid task implementation transition; Werkrby returned this task to todo.")
			if err != nil {
				return status, path, err
			}
		}
		if status == "waiting" {
			document, err := ReadDocument(path)
			if err != nil {
				return status, path, m.skipUnreadableDocument(path, err)
			}
			if !hasRoutableWaitingPath(document, time.Now().UTC()) {
				note := "Park rejected: a park in waiting/ must carry a future RFC 3339 wake_at or a joey_ask in front matter; Werkrby returned this task to todo without spending an attempt."
				credit := func(frontMatter map[string]any) {
					frontMatter[resumeCreditField] = true
				}
				now := time.Now().UTC()
				target := statusPath(base, "todo", name)
				if err := moveDocumentWithUpdate(path, target, "todo", now, note, credit); err != nil {
					return "todo", path, err
				}
				m.log(note + " " + target)
				return "todo", target, nil
			}
		}
		if status == "review" {
			document, err := ReadDocument(path)
			if err != nil {
				return status, path, m.skipUnreadableDocument(path, err)
			}
			document.FrontMatter["implementation_thread"] = lease.ThreadID
			document.FrontMatter["implementation_session"] = lease.SessionKey
			if err := WriteDocument(path, document); err != nil {
				return status, path, err
			}
		}
		if status == "done" && !reviewRequired {
			if evidenceErr := validateDirectCompletionEvidence(document); evidenceErr != nil {
				note := "Direct completion rejected: " + evidenceErr.Error()
				var credit func(map[string]any)
				// A misshaped summary of recorded work gets one repair per attempt
				// without spending it; a second one, or missing content, spends it.
				attempt := numberValue(document.FrontMatter["attempt"])
				if completionFormatOnly(evidenceErr) && attempt >= 1 && numberValue(document.FrontMatter[formatCreditAttemptField]) != attempt {
					note += fmt.Sprintf(" Only the summary format is wrong, so the repair turn continues attempt %d; a second format rejection in this attempt spends it.", attempt)
					credit = func(frontMatter map[string]any) {
						frontMatter[resumeCreditField] = true
						frontMatter[formatCreditAttemptField] = attempt
					}
				}
				now := time.Now().UTC()
				target := statusPath(base, "todo", name)
				if err := moveDocumentWithUpdate(path, target, "todo", now, note, credit); err != nil {
					return "todo", path, err
				}
				m.log(note + " " + target)
				return "todo", target, nil
			}
		}
		if status == "review" || status == "done" {
			// The pre-registered acceptance check gates this hand-off; without
			// an armed or registered check this is a no-op, byte for byte.
			gateStatus, gatePath, gateHandled, gateErr := m.runAcceptanceGate(ctx, base, name, status, path)
			if gateErr != nil {
				return gateStatus, gatePath, gateErr
			}
			if gateHandled {
				return gateStatus, gatePath, nil
			}
			status, path = gateStatus, gatePath
		}
		if status == "done" {
			m.finalizeTaskCompletionSummary(path, status)
		}
		if status == "waiting" {
			m.grantParkCredit(path)
		}
		if status == "done" || status == "waiting" || status == "failed" || status == "cancelled" {
			if err := m.completeTransition(ctx, route, lease, status, path); err != nil {
				return status, path, err
			}
		}
		return status, path, nil
	}

	if status == "done" && lease.ImplementerThread != "" && lease.ThreadID == lease.ImplementerThread {
		var err error
		status, path, err = m.redirectTransition(path, statusPath(base, "todo", name), "todo", "Independent review rejected automatically because the reviewer reused the implementation harness thread.")
		return status, path, err
	}
	// A review may also park the task in waiting: the submitted work is accepted
	// but the acceptance criteria need a confirmation only a person can give
	// (e.g. an in-game grade of a testing build). Returning it to todo instead
	// burns a full implementation turn and an attempt whose only act is to park
	// it again (observed 2026-09-17 on an attempt-capped task).
	// An operator may cancel the task while under review; honour that decision.
	if status != "done" && status != "todo" && status != "waiting" && status != "cancelled" {
		var err error
		status, path, err = m.redirectTransition(path, statusPath(base, "todo", name), "todo", "Invalid task-review transition; review may accept into done, return findings to todo, or park an accepted attempt in waiting on a human confirmation.")
		if err != nil {
			return status, path, err
		}
	}
	if status == "cancelled" && m.Harness.IsActive(lease.SessionKey) {
		_, _ = m.Harness.Interrupt(ctx, lease.SessionKey)
	}
	if status == "waiting" {
		document, err := ReadDocument(path)
		if err != nil {
			return status, path, m.skipUnreadableDocument(path, err)
		}
		if !hasRoutableWaitingPath(document, time.Now().UTC()) {
			note := "Park rejected: a park in waiting/ must carry a future RFC 3339 wake_at or a joey_ask in front matter; Werkrby returned this task to todo without spending an attempt."
			credit := func(frontMatter map[string]any) {
				frontMatter[resumeCreditField] = true
			}
			now := time.Now().UTC()
			target := statusPath(base, "todo", name)
			if err := moveDocumentWithUpdate(path, target, "todo", now, note, credit); err != nil {
				return "todo", path, err
			}
			m.log(note + " " + target)
			return "todo", target, nil
		}
	}
	if status != "waiting" {
		m.finalizeTaskCompletionSummary(path, status)
	} else {
		m.grantParkCredit(path)
	}
	if status == "todo" && lease.ReviewRisk == reviewRiskRoutine && taskRiskHigh(path) {
		// A routine reviewer that finds the task should have been risk: high
		// gives no verdict, stamps the risk, and returns it for the risk-high
		// reviewer. That hand-off is not rework, and it can only make review
		// stricter, so the implementer's pass-through continues the attempt.
		m.grantParkCredit(path)
	}
	if status == "done" || status == "waiting" || status == "cancelled" {
		if err := m.completeTransition(ctx, route, lease, status, path); err != nil {
			return status, path, err
		}
	}
	return status, path, nil
}

func (m *Manager) reconcileGoalTransition(_ context.Context, route workflowRoute, lease Lease, phase, status, path string) (string, string, error) {
	base := filepath.Dir(m.Config.Resolve(route.Source))
	name := filepath.Base(path)
	document, err := ReadDocument(path)
	if err != nil {
		return status, path, m.skipUnreadableDocument(path, err)
	}
	if phase == phaseGoalPlanning {
		if status == "active" {
			if err := m.validateGoalPlanningTransition(document); err == nil {
				return status, path, nil
			} else {
				return m.redirectTransition(path, statusPath(base, "proposed", name), "proposed", "Goal activation rejected: "+err.Error())
			}
		}
		if status == "waiting" {
			if !hasRoutableWaitingPath(document, time.Now().UTC()) {
				note := "Park rejected: a park in waiting/ must carry a future RFC 3339 wake_at or a joey_ask in front matter; Werkrby returned this goal to proposed."
				credit := func(frontMatter map[string]any) {
					frontMatter[resumeCreditField] = true
				}
				target := statusPath(base, "proposed", name)
				now := time.Now().UTC()
				if err := moveDocumentWithUpdate(path, target, "proposed", now, note, credit); err != nil {
					return "proposed", path, err
				}
				m.log(note + " " + target)
				return "proposed", target, nil
			}
			return status, path, nil
		}
		if status == "abandoned" {
			return status, path, nil
		}
		return m.redirectTransition(path, statusPath(base, "proposed", name), "proposed", "Invalid goal-planning transition; planning must create a valid active round, wait on a precise condition, or record explicit abandonment.")
	}

	if status == "done" {
		if err := goalReviewProvesDone(document); err != nil {
			return m.redirectTransition(path, statusPath(base, "review", name), "review", "Goal completion rejected: "+err.Error())
		}
		return status, path, nil
	}
	if status == "waiting" {
		if !hasRoutableWaitingPath(document, time.Now().UTC()) {
			note := "Park rejected: a park in waiting/ must carry a future RFC 3339 wake_at or a joey_ask in front matter; Werkrby returned this goal to review."
			credit := func(frontMatter map[string]any) {
				frontMatter[resumeCreditField] = true
			}
			target := statusPath(base, "review", name)
			now := time.Now().UTC()
			if err := moveDocumentWithUpdate(path, target, "review", now, note, credit); err != nil {
				return "review", path, err
			}
			m.log(note + " " + target)
			return "review", target, nil
		}
		return status, path, nil
	}
	if status == "planning" || status == "abandoned" {
		return status, path, nil
	}
	return m.redirectTransition(path, statusPath(base, "review", name), "review", "Invalid goal-review transition; review must choose planning, waiting, done, or abandoned.")
}

// hasRoutableWaitingPath checks whether a document entering waiting/ carries
// an explicit routable path: either a non-empty joey_ask or a future RFC 3339 wake_at.
func hasRoutableWaitingPath(document Document, now time.Time) bool {
	if hasJoeyAsk(document) {
		return true
	}
	if due, ok := frontMatterTime(document.FrontMatter["wake_at"]); ok && due.After(now) {
		return true
	}
	return false
}

// grantParkCredit marks a task a turn just parked in waiting/, before the
// notification agent can read or edit it. A failed write only costs the task
// the credit (its next claim spends an attempt, as before), so it is logged.
func (m *Manager) grantParkCredit(path string) {
	if err := setResumeCredit(path, true); err != nil {
		m.log("record resume credit " + path + ": " + err.Error())
	}
}

func (m *Manager) redirectTransition(path, target, status, note string) (string, string, error) {
	now := time.Now().UTC()
	if err := moveDocumentWithProgress(path, target, status, now, note); err != nil {
		return status, path, err
	}
	m.log(note + " " + target)
	return status, target, nil
}

func (m *Manager) completeTransition(ctx context.Context, route workflowRoute, lease Lease, status, path string) error {
	document, err := ReadDocument(path)
	if err != nil {
		return m.skipUnreadableDocument(path, err)
	}
	runtimeCfg := m.runtimeSnapshot()
	if runtimeCfg.Extensions.Enabled {
		if lease.TerminalHooksCompleted == nil {
			lease.TerminalHooksCompleted = map[string]bool{}
		}
		// Enqueue first, then persist a receipt after each extension completes
		// successfully. A crash or receipt-write failure retries with the same
		// event ID; extensions must persistently deduplicate visible effects.
		var receiptErr error
		if _, hookErr := m.Hooks.RunTracked(ctx, "task.completed", map[string]any{
			"route": route.Name, "file": path, "thread_id": lease.ThreadID,
			"outcome": status, "event_id": lease.ID + ":" + status,
		}, lease.TerminalHooksCompleted, func(extensionID string) error {
			lease.TerminalHooksCompleted[extensionID] = true
			receiptErr = m.saveLease(lease)
			return receiptErr
		}); hookErr != nil {
			m.log("task.completed hook: " + hookErr.Error())
			return hookErr
		}
	}
	if requiresTaskNotificationDecision(document, status, time.Now().UTC()) {
		m.startTaskNotificationAgent(ctx, lease, status, path)
	}
	return nil
}

// SettleTaskByOperator records an operator-initiated terminal settle of a
// claimed task document and moves it to the terminal folder. It is the
// control-path twin of the honored operator review-cancel: the note must
// document the operator authority, and a done settle's summary map must carry
// the checker-accepted direct-completion shape (verdict, outcome, evidence,
// uncertainty, completed_at) with completed_at equal to the updated_at this
// move stamps. The caller remains responsible for stopping the run; the
// ordinary reconcile then validates the transition and runs terminal hooks
// and the notification decision.
func (m *Manager) SettleTaskByOperator(lease Lease, status string, note string, summary map[string]any, extra func(map[string]any)) (string, error) {
	switch status {
	case "done", "cancelled", "failed":
	default:
		return "", fmt.Errorf("unsupported operator settle status %q", status)
	}
	if _, err := os.Stat(lease.File); err != nil {
		return "", fmt.Errorf("claimed document is unavailable: %w", err)
	}
	route, ok := routeByName(lease.Route)
	if !ok {
		return "", fmt.Errorf("unknown route %q", lease.Route)
	}
	base := filepath.Dir(m.Config.Resolve(route.Source))
	target := statusPath(base, status, filepath.Base(lease.File))
	now := time.Now().UTC()
	update := func(frontMatter map[string]any) {
		if summary != nil {
			frontMatter["completion_summary"] = summary
		}
		if extra != nil {
			extra(frontMatter)
		}
	}
	if err := moveDocumentWithUpdate(lease.File, target, status, now, note, update); err != nil {
		return "", err
	}
	m.requestScan()
	return target, nil
}

func requiresTaskNotificationDecision(document Document, status string, now time.Time) bool {
	switch status {
	case "done", "failed", "cancelled":
		return true
	case "waiting":
		// A park on a joey_ask is owned by the Helm card, never by a waiting notice.
		if hasJoeyAsk(document) {
			return false
		}
		due, ok := frontMatterTime(document.FrontMatter["wake_at"])
		return !ok || !due.After(now)
	default:
		return false
	}
}

func (m *Manager) resumeInterruptedClaims(ctx context.Context) error {
	leases, err := m.loadLeases()
	if err != nil {
		return err
	}
	for _, lease := range leases {
		if lease.State != "claiming" || m.isInflight(lease.ID) || m.Harness.IsActive(lease.SessionKey) {
			continue
		}
		if _, err := os.Stat(lease.File); os.IsNotExist(err) && lease.SourceFile != "" {
			if _, sourceErr := os.Stat(lease.SourceFile); sourceErr == nil {
				phase := normalizeLeasePhase(lease.Route, lease.Phase)
				field := phaseAttemptField(phase)
				document, claimErr := m.claimPhaseDocument(lease.SourceFile, lease.File, phaseClaimedStatus(phase), field, time.Now())
				if claimErr != nil {
					if _, readErr := ReadDocument(lease.SourceFile); readErr != nil {
						_ = m.skipUnreadableDocument(lease.SourceFile, readErr)
						continue
					}
					return claimErr
				}
				if lease.ClaimAttempt == 0 {
					lease.ClaimAttempt = numberValue(document.FrontMatter[field])
				}
			}
		}
		if _, err := os.Stat(lease.File); err == nil {
			route, ok := routeByName(lease.Route)
			if !ok {
				continue
			}
			phase := normalizeLeasePhase(lease.Route, lease.Phase)
			field := phaseAttemptField(phase)
			document, readErr := ReadDocument(lease.File)
			if readErr != nil {
				_ = m.skipUnreadableDocument(lease.File, readErr)
				continue
			}
			attempt := lease.ClaimAttempt
			if attempt < 1 {
				return fmt.Errorf("claim lease %s has no phase attempt", lease.ID)
			}
			document.FrontMatter["status"] = phaseClaimedStatus(phase)
			document.FrontMatter["updated_at"] = lease.StartedAt.UTC().Format(time.RFC3339)
			if first, ok := frontMatterTime(document.FrontMatter["first_assigned_at"]); !ok || first.After(lease.StartedAt) {
				document.FrontMatter["first_assigned_at"] = lease.StartedAt.UTC().Format(time.RFC3339)
			}
			// A claim whose write never landed left the credit in place. The
			// journaled attempt was computed from it, so apply the same rule
			// here to record it against the attempt's budget and remove it.
			if _, marked := document.FrontMatter[resumeCreditField]; marked {
				claimAttempt(document.FrontMatter, field)
			}
			document.FrontMatter[field] = attempt
			if err := WriteDocument(lease.File, document); err != nil {
				return err
			}
			lease.State = "recovering"
			lease.OwnerID = m.ownerID
			lease.SourceFile = ""
			lease.HeartbeatAt = time.Now().UTC()
			if err := m.saveLease(lease); err != nil {
				return err
			}
			m.dispatch(ctx, route, lease, true)
			continue
		}
		// Neither side of the interrupted claim remains. Let ordinary
		// transition reconciliation locate a moved status file.
		lease.State = "processing"
		if err := m.saveLease(lease); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) claimPhaseDocument(source, target, status, attemptField string, now time.Time) (Document, error) {
	if m.claimDocument != nil {
		return m.claimDocument(source, target, status, attemptField, now)
	}
	return claimDocument(source, target, status, attemptField, now)
}

func (m *Manager) recoverOrphanClaims(ctx context.Context) error {
	cfg := m.runtimeSnapshot()
	for _, route := range workflowRoutes() {
		base := filepath.Dir(cfg.Resolve(route.Source))
		var phases map[string]string
		switch route.Name {
		case "tasks":
			phases = map[string]string{"working": phaseTaskImplementation, "reviewing": phaseTaskReview}
		case "goals":
			phases = map[string]string{"planning": phaseGoalPlanning, "reviewing": phaseGoalReview}
		default:
			continue
		}
		for status, phase := range phases {
			directory := filepath.Join(base, status)
			entries, err := os.ReadDir(directory)
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				return err
			}
			for _, entry := range entries {
				if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".md") || entry.Name() == "AGENTS.md" {
					continue
				}
				path := filepath.Join(directory, entry.Name())
				if m.hasLeaseForFile(path) {
					continue
				}
				m.log("recovering claimed document without lease: " + path)
				if err := m.startExistingClaim(ctx, route, path, phase, true, false); err != nil {
					if errors.Is(err, errUnreadableDocument) {
						continue
					}
					return err
				}
			}
		}
	}
	return nil
}

func (m *Manager) hasLeaseForFile(path string) bool {
	leases, err := m.loadLeases()
	if err != nil {
		return false
	}
	want := filepath.Clean(path)
	for _, lease := range leases {
		if filepath.Clean(lease.File) == want {
			return true
		}
	}
	return false
}

// HasLeaseForFile reports whether any persisted lease claims the file path.
func (m *Manager) HasLeaseForFile(path string) bool {
	if m == nil {
		return false
	}
	return m.hasLeaseForFile(path)
}

func (m *Manager) leaseForDocument(routeName, name, phase, exceptID string) (Lease, bool, error) {
	leases, err := m.loadLeases()
	if err != nil {
		return Lease{}, false, err
	}
	for _, lease := range leases {
		if lease.ID == exceptID || lease.Route != routeName || filepath.Base(lease.File) != name {
			continue
		}
		if phase != "" && normalizeLeasePhase(routeName, lease.Phase) != phase {
			continue
		}
		return lease, true, nil
	}
	return Lease{}, false, nil
}

func (m *Manager) wakeWaitingDocuments(ctx context.Context) error {
	now := time.Now().UTC()
	cfg := m.runtimeSnapshot()
	for _, route := range workflowRoutes() {
		if route.Name != "tasks" && route.Name != "goals" {
			continue
		}
		base := filepath.Dir(cfg.Resolve(route.Source))
		directory := filepath.Join(base, "waiting")
		entries, err := os.ReadDir(directory)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".md") || entry.Name() == "AGENTS.md" {
				continue
			}
			path := filepath.Join(directory, entry.Name())
			document, err := ReadDocument(path)
			if err != nil {
				m.reportUnparseableDocument(path, err)
				continue
			}
			due, err := scheduledWake(document, now)
			if err != nil {
				m.log("waiting schedule " + path + ": " + err.Error())
				continue
			}
			if !due {
				continue
			}
			targetStatus := "todo"
			if route.Name == "tasks" && stringField(document, "resume_status") == "review" {
				// A quota wall parked the task during review: its implementation
				// is done, so it resumes review rather than re-running the work.
				targetStatus = "review"
			}
			if route.Name == "goals" {
				targetStatus = stringField(document, "resume_status")
				if targetStatus != "planning" && targetStatus != "review" {
					targetStatus = "review"
				}
				// A goal resuming planning on a paced seat stays parked: no move, no note, no
				// attempt. It is due again every scan and wakes once the hold lifts.
				if targetStatus == "planning" && m.rosterHeld("wake", route, phaseGoalPlanning, entry.Name(), document) {
					continue
				}
				// Same for the work window: a woken planning round is personal work.
				if targetStatus == "planning" && m.workWindowHolds(document) {
					continue
				}
				// And for the ops lane: a woken planning round waits for the lane's slots.
				if targetStatus == "planning" && m.opsLaneHolds(route, document) {
					continue
				}
			}
			target := filepath.Join(base, targetStatus, entry.Name())
			note := fmt.Sprintf("Spynel resumed this %s from waiting because its scheduled wake condition became due; it returned to %s for a fresh agent decision.", strings.TrimSuffix(route.Name, "s"), targetStatus)
			// The dates that woke it are spent; left behind they would wake the
			// document's next park early. A not_before still in the future is a
			// dispatch gate rather than a park date, so it stays.
			if err := moveDocumentWithUpdate(path, target, targetStatus, now, note, func(fm map[string]any) {
				delete(fm, "wake_at")
				if when, ok := frontMatterTime(fm["not_before"]); !ok || !when.After(now) {
					delete(fm, "not_before")
				}
				delete(fm, "resume_status")
			}); err != nil {
				return err
			}
			if route.Name == "goals" && targetStatus == "planning" {
				if err := m.startExistingClaim(ctx, route, target, phaseGoalPlanning, false, true); err != nil {
					if errors.Is(err, errUnreadableDocument) {
						continue
					}
					return err
				}
			}
		}
	}
	return nil
}

func (m *Manager) advanceActiveGoals() error {
	route, ok := routeByName("goals")
	if !ok {
		return nil
	}
	base := filepath.Dir(m.Config.Resolve(route.Source))
	directory := filepath.Join(base, "active")
	entries, err := os.ReadDir(directory)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".md") || entry.Name() == "AGENTS.md" {
			continue
		}
		path := filepath.Join(directory, entry.Name())
		document, err := ReadDocument(path)
		if err != nil {
			m.reportUnparseableDocument(path, err)
			continue
		}
		if err := m.validateGoalActivation(document); err != nil {
			if m.skipUnreadableRoundTask(err) {
				continue
			}
			target := filepath.Join(base, "proposed", entry.Name())
			if _, _, moveErr := m.redirectTransition(path, target, "proposed", "Invalid active goal returned to planning queue: "+err.Error()); moveErr != nil {
				return moveErr
			}
			continue
		}
		tasks, err := m.goalRoundTasks(document)
		if err != nil {
			if m.skipUnreadableRoundTask(err) {
				continue
			}
			return err
		}
		settled := len(tasks) > 0
		for _, task := range tasks {
			settled = settled && taskSettledStatuses[task.Status]
		}
		trigger := stringField(document, "review_trigger")
		checkpoint := false
		if trigger == "scheduled" || trigger == "all_round_tasks_settled_or_checkpoint" {
			checkpoint, err = checkpointDue(document, now)
			if err != nil {
				m.log("goal checkpoint " + path + ": " + err.Error())
				continue
			}
		}
		ready := false
		switch trigger {
		case "all_round_tasks_settled":
			ready = settled
		case "all_round_tasks_settled_or_checkpoint":
			ready = settled || checkpoint
		case "scheduled":
			ready = checkpoint
		}
		if !ready {
			continue
		}
		target := filepath.Join(base, "review", entry.Name())
		reason := "all current-round tasks settled"
		if checkpoint && !settled {
			reason = "the configured evidence checkpoint became due"
		}
		note := "Spynel queued this goal for independent review because " + reason + "."
		if err := moveDocumentWithProgress(path, target, "review", now, note); err != nil {
			return err
		}
		m.log("goal round ready for review: " + target)
	}
	return nil
}

// skipUnreadableRoundTask files the repair for an unreadable round task and reports whether err
// was that case: the goal then keeps its status and is read again on the next scan.
func (m *Manager) skipUnreadableRoundTask(err error) bool {
	var unreadable *unreadableRoundTaskError
	if !errors.As(err, &unreadable) {
		return false
	}
	m.reportUnparseableDocument(unreadable.Path, unreadable.Err)
	return true
}

func (m *Manager) validateGoalActivation(document Document) error {
	if err := validSuccessCriteria(document); err != nil {
		return err
	}
	trigger := stringField(document, "review_trigger")
	switch trigger {
	case "all_round_tasks_settled":
	case "all_round_tasks_settled_or_checkpoint":
		if _, exists := document.FrontMatter["next_review_at"]; exists {
			if _, err := checkpointDue(document, time.Now()); err != nil {
				return err
			}
		}
	case "scheduled":
		if _, exists := document.FrontMatter["next_review_at"]; !exists {
			return errors.New("scheduled review_trigger requires next_review_at")
		}
		if _, err := checkpointDue(document, time.Now()); err != nil {
			return err
		}
	default:
		return errors.New("review_trigger must be all_round_tasks_settled, all_round_tasks_settled_or_checkpoint, or scheduled")
	}
	if numberValue(document.FrontMatter["round"]) <= 0 {
		return errors.New("round must be positive before activation")
	}
	tasks, err := m.goalRoundTasks(document)
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		return errors.New("the current round has no linked tasks")
	}
	rawIDs, ok := document.FrontMatter["round_task_ids"].([]any)
	if !ok || len(rawIDs) == 0 {
		return errors.New("round_task_ids must declare the complete current-round task batch")
	}
	declared := map[string]bool{}
	for _, raw := range rawIDs {
		id, ok := raw.(string)
		id = strings.TrimSpace(id)
		if !ok || id == "" || declared[id] {
			return errors.New("round_task_ids must contain unique non-empty task IDs")
		}
		declared[id] = true
	}
	linked := map[string]bool{}
	for _, task := range tasks {
		if task.ID == "" {
			return errors.New("every linked task requires an id")
		}
		linked[task.ID] = true
	}
	if len(declared) != len(linked) {
		return errors.New("round_task_ids does not match the linked current-round tasks")
	}
	for id := range declared {
		if !linked[id] {
			return fmt.Errorf("round_task_ids references missing task %q", id)
		}
	}
	return nil
}

func (m *Manager) validateGoalPlanningTransition(document Document) error {
	if err := m.validateGoalActivation(document); err != nil {
		return err
	}
	if _, exists := document.FrontMatter["next_review_at"]; exists && stringField(document, "checkpoint_reason") == "" {
		return errors.New("next_review_at requires checkpoint_reason")
	}
	return nil
}

func (m *Manager) goalRoundTasks(document Document) ([]linkedTask, error) {
	id := documentID(document)
	if id == "" {
		return nil, errors.New("goal id is required")
	}
	return linkedRoundTasks(m.Config.StatePath("tasks"), id, numberValue(document.FrontMatter["round"]))
}

func (m *Manager) recoverStale(ctx context.Context) error {
	leases, err := m.loadLeases()
	if err != nil {
		return err
	}
	now := time.Now()
	for _, lease := range leases {
		if lease.State == "hook_cancelled" {
			continue
		}
		if _, err := os.Stat(lease.File); os.IsNotExist(err) {
			// A missing working path is a durable status transition. Leave the
			// lease for reconcileTransitions so review enforcement, hooks, and
			// notification enqueueing cannot be skipped by a concurrent scan.
			continue
		}
		route, ok := routeByName(lease.Route)
		if !ok {
			continue
		}
		foreignOwner := lease.OwnerID != "" && lease.OwnerID != m.ownerID
		if lease.State == "awaiting_transition" {
			if (!foreignOwner && now.Sub(lease.HeartbeatAt) < awaitingTransitionStaleAfter) || m.isInflight(lease.ID) || m.Harness.IsActive(lease.SessionKey) {
				continue
			}
			if lease.RecoveryCount >= quickAwaitingTransitionRecoveries {
				requeueStatus := "todo"
				if route.Name == "goals" {
					if normalizeLeasePhase(route.Name, lease.Phase) == phaseGoalReview {
						requeueStatus = "review"
					} else {
						requeueStatus = "proposed"
					}
				}
				base := filepath.Dir(m.Config.Resolve(route.Source))
				target := filepath.Join(base, requeueStatus, filepath.Base(lease.File))
				note := fmt.Sprintf("Spynel requeued this %s to %s because turn ended without moving the document after %d recovery attempt(s); attempt was spent.", strings.TrimSuffix(route.Name, "s"), requeueStatus, lease.RecoveryCount)
				update := func(fm map[string]any) {
					delete(fm, resumeCreditField)
				}
				if moveErr := moveDocumentWithUpdate(lease.File, target, requeueStatus, now.UTC(), note, update); moveErr != nil {
					m.log("requeue stranded document: " + moveErr.Error())
					continue
				}
				_ = os.Remove(m.leasePath(lease.ID))
				m.finishRuntimeJob(lease.ID)
				m.log(fmt.Sprintf("requeued stranded %s to %s: %s", route.Name, requeueStatus, target))
				continue
			}
		}
		staleThreshold := route.StaleAfter
		switch {
		case lease.State == "error":
			staleThreshold = 10 * time.Second
		case lease.State == "awaiting_transition":
			staleThreshold = awaitingTransitionStaleAfter
		}
		if (!foreignOwner && now.Sub(lease.HeartbeatAt) < staleThreshold) || m.isInflight(lease.ID) || m.Harness.IsActive(lease.SessionKey) {
			continue
		}
		lease.OwnerID = m.ownerID
		lease.HeartbeatAt = time.Now().UTC()
		if err := m.saveLease(lease); err != nil {
			return err
		}
		m.dispatch(ctx, route, lease, true)
	}
	return nil
}

func (m *Manager) renderPrompt(route workflowRoute, lease Lease, promptPath string) (string, error) {
	return m.renderPromptFor(route, lease, promptPath, nil)
}

// renderPromptFor renders a workflow prompt. A non-nil turn is an explicit-outcome seat: the status
// folder list gives way to a pointer at the turn-end contract, which is appended after every other
// section so it is the last word on how the turn ends.
func (m *Manager) renderPromptFor(route workflowRoute, lease Lease, promptPath string, turn *explicitTurn) (string, error) {
	file := lease.File
	data, err := os.ReadFile(m.Config.Resolve(promptPath))
	if err != nil {
		return "", err
	}
	statuses := make([]string, 0, len(route.AllowedNext))
	base := filepath.Dir(m.Config.Resolve(route.Source))
	for _, status := range route.AllowedNext {
		statuses = append(statuses, fmt.Sprintf("- %s: %s", status, filepath.Join(base, status)))
	}
	workerName := harness.WorkerNameForKey(lease.SessionKey)
	if workerName == "" {
		docID := strings.TrimSuffix(filepath.Base(file), filepath.Ext(file))
		if doc, err := ReadDocument(file); err == nil && documentID(doc) != "" {
			docID = documentID(doc)
		}
		attempt := lease.ClaimAttempt
		if attempt < 1 {
			attempt = 1
		}
		phase := lease.Phase
		if phase == "" {
			phase = phaseForFile(route.Name, file)
		}
		workerName = harness.WorkerNameForKey(phaseSessionKey(route.Name, docID, phase, attempt))
	}
	statusFolders := strings.Join(statuses, "\n")
	if turn != nil {
		statusFolders = "- On this seat Werkrby moves the task file; see \"Turn end on this seat\" at the end of this prompt."
	}
	replacements := map[string]string{
		"{{FILE}}": file, "{{ROUTE}}": route.Name,
		"{{ALLOWED_NEXT}}":   strings.Join(route.AllowedNext, ", "),
		"{{STATUS_FOLDERS}}": statusFolders,
		"{{STALE_AFTER}}":    route.StaleAfter.String(),
		"{{PHASE}}":          phaseForFile(route.Name, file),
		"{{RELATED_TASKS}}":  m.relatedTasksForGoal(file),
		"{{TASK_SOURCE}}":    m.Config.StatePath("tasks", "todo"),
		"{{GOAL_SOURCE}}":    m.Config.StatePath("goals", "proposed"),
		"{{WORKER_NAME}}":    workerName,
		"{{WORKER_PANE}}":    workerName,
	}
	prompt := string(data)
	prompt = agentdocs.InjectPromptGuidance(prompt)
	for from, to := range replacements {
		prompt = strings.ReplaceAll(prompt, from, to)
	}
	prompt = appendTaskReviewModeInstruction(prompt, m.harnessSettings().Reviews)
	role := instructions.Developer
	phase := normalizeLeasePhase(route.Name, phaseForFile(route.Name, file))
	if phase == phaseTaskReview || phase == phaseGoalReview {
		role = instructions.Reviewer
	}
	prompt = instructions.InjectRoleScopeDiscipline(prompt, role)
	prompt, err = instructions.Append(prompt, m.Config.StatePath(), role)
	if err != nil || turn == nil {
		return prompt, err
	}
	return strings.TrimRight(prompt, "\n") + "\n\n" + turn.contract(file), nil
}

func (m *Manager) harnessSettings() config.Harness {
	if value := m.harnessPolicy.Load(); value != nil {
		return value.(config.Harness)
	}
	return m.runtimeSnapshot().Harness
}

func (m *Manager) agentPrefix(phase string, settings config.Harness) string {
	switch normalizeLeasePhase("", phase) {
	case phaseTaskReview, phaseGoalReview:
		return settings.ReviewerAgentPrefix
	default:
		return settings.DeveloperAgentPrefix
	}
}

func (m *Manager) resolveTargetModel(route workflowRoute, lease Lease) string {
	if model, ok := m.resolveRosterModel(route, lease); ok {
		return model
	}
	return m.resolveLegacyTargetModel(route, lease)
}

// resolveRosterModel routes through the optional .spynel/roster.yaml. The file
// is read fresh per dispatch so staffing edits apply at the next dispatch
// boundary. Any roster problem is logged and legacy routing is used instead.
func (m *Manager) resolveRosterModel(route workflowRoute, lease Lease) (string, bool) {
	stateDir := m.Config.StatePath()
	staffing, err := roster.Load(stateDir)
	if err != nil {
		m.log("roster ignored: " + err.Error())
		return "", false
	}
	if staffing == nil {
		return "", false
	}
	phase := normalizeLeasePhase(route.Name, lease.Phase)
	if phase == "review" {
		phase = phaseTaskReview
	}
	frontMatter := map[string]any{}
	if doc, err := ReadDocument(lease.File); err == nil {
		frontMatter = doc.FrontMatter
	}
	name, ok := rosterSeat(staffing, stateDir, phase, frontMatter, filepath.Base(lease.File), m.log)
	if !ok {
		return "", false
	}
	shadowDone := m.shadowDispatchPending(lease, phase, name, frontMatter)
	assigned, err := staffing.Assign(stateDir, name, lease.SessionKey, time.Now())
	if err != nil {
		m.log("roster usage: " + err.Error())
		if assigned == "" {
			return "", false
		}
	}
	if assigned != name {
		m.log("roster: " + name + " is at its daily cap; " + filepath.Base(lease.File) + " goes to " + assigned)
	}
	if phase == phaseTaskImplementation {
		// The seat that actually runs the work, after caps and stops, is who wrote it: a later
		// review picks a different model family from this one.
		if err := roster.RecordImplementer(stateDir, filepath.Base(lease.File), assigned, time.Now()); err != nil {
			m.log("roster implementer: " + err.Error())
		}
	}
	if shadowDone != nil {
		shadowDone(assigned)
	}
	return staffing.Model(assigned), true
}

// rosterSeat is the staff a document's dispatch asks the roster for, before daily caps and
// stops: the role's staff, a review's cross-family alternate, a task's staff pin, the attempt
// ladder and a seat's own escalation. ok is false when the roster does not route this document
// (an explicit legacy runner pin, or no staff for the role). logf may be nil.
func rosterSeat(staffing *roster.Roster, stateDir, phase string, frontMatter map[string]any, fileName string, logf func(string)) (string, bool) {
	if logf == nil {
		logf = func(string) {}
	}
	text := func(key string) string {
		value, _ := frontMatter[key].(string)
		return strings.TrimSpace(value)
	}

	name := ""
	switch phase {
	case phaseTaskReview:
		role := phaseTaskReview
		switch {
		case strings.EqualFold(text("risk"), "high"):
			name = staffing.ForRole(phaseTaskReview + "_high_risk")
			role = phaseTaskReview + "_high_risk"
		case strings.EqualFold(text("risk"), "medium"):
			// Optional role; unmapped, a medium-risk review is an ordinary review as before.
			name = staffing.ForRole(phaseTaskReview + "_medium")
			role = phaseTaskReview + "_medium"
		}
		if name == "" {
			name = staffing.ForRole(phaseTaskReview)
			role = phaseTaskReview
		}
		// A role that lists alternates (role_alternates) is reviewed by a different model family
		// than the seat that wrote the work: use the first alternate when the role's own staff
		// shares the implementer's family. An unknown implementer keeps the role's staff.
		if alternates := staffing.RoleAlternates[role]; name != "" && len(alternates) > 0 {
			if implementer, ok := roster.ImplementedBy(stateDir, fileName); ok {
				if picked := staffing.PickDifferentFamily(name, alternates, implementer); picked != name {
					logf("roster: " + role + " review of " + fileName + " moves from " + name + " to " + picked + " (the work was written on " + implementer + ", the same model family)")
					name = picked
				}
			}
		}
	case phaseTaskImplementation:
		pinnedStaff := honoredStaffPin(staffing, frontMatter, fileName, logf)
		if pinnedStaff != "" {
			if s, ok := staffing.Staff[pinnedStaff]; ok && s.NoEscalate {
				name = ""
			} else {
				name = staffing.EscalationFor(int(numberValue(frontMatter["attempt"])), staffing.Has(pinnedStaff))
			}
		} else {
			name = staffing.EscalationFor(int(numberValue(frontMatter["attempt"])), false)
		}
		if name == "" {
			if staff := pinnedStaff; staff != "" {
				if staffing.Has(staff) {
					name = staff
				} else {
					logf("roster: unknown staff " + strconv.Quote(staff) + " in " + fileName)
				}
			}
		}
		if name == "" && (text("agent") != "" || text("model") != "") {
			return "", false // explicit legacy runner pin
		}
		if name == "" {
			name = staffing.ForRole(phase)
		}
		// A seat's own escalation (escalate_after / escalate_to) moves a retry onto the seat it
		// names, whether the task was pinned here or reached this seat as the role default.
		if name != "" {
			attempt := int(numberValue(frontMatter["attempt"]))
			if escalated := staffing.EscalatedStaff(name, attempt); escalated != name {
				logf("roster: " + name + " escalates to " + escalated + " at attempt " + strconv.Itoa(attempt) + ": " + fileName)
				name = escalated
			}
		}
	default:
		name = staffing.ForRole(phase)
	}
	if name == "" {
		return "", false
	}
	return name, true
}

// rosterHeld reports whether a queued document's session would start on a seat the quota gate
// is holding (runtime/roster-holds.json, roster.HeldUntil). The seat is found the way dispatch
// finds it (rosterSeat, then Assign's fallback walk past caps and stops) without counting a
// session. A held document is logged at most once an hour.
func (m *Manager) rosterHeld(site string, route workflowRoute, phase, name string, document Document) bool {
	held := m.rosterHeldNow(route, phase, name, document)
	m.shadowQueued(site, route, phase, name, document, held)
	return held
}

func (m *Manager) rosterHeldNow(route workflowRoute, phase, name string, document Document) bool {
	stateDir := m.Config.StatePath()
	now := time.Now()
	if !roster.AnyHeld(stateDir, now) {
		return false
	}
	staffing, err := roster.Load(stateDir)
	if err != nil || staffing == nil {
		return false
	}
	phase = normalizeLeasePhase(route.Name, phase)
	// Route on the attempt the claim would record (claimAttempt), as dispatch does after the
	// claim: a retry that escalates onto a held seat must wait too.
	frontMatter := make(map[string]any, len(document.FrontMatter)+1)
	scratch := make(map[string]any, len(document.FrontMatter))
	for key, value := range document.FrontMatter {
		frontMatter[key] = value
		scratch[key] = value
	}
	if field := phaseAttemptField(phase); field != "" {
		frontMatter[field], _ = claimAttempt(scratch, field)
	}
	asked, ok := rosterSeat(staffing, stateDir, phase, frontMatter, name, nil)
	if !ok {
		return false
	}
	seat := staffing.Landing(stateDir, asked, now)
	until, reason, held := roster.HeldUntil(stateDir, seat, now)
	if !held {
		return false
	}
	// The gate rewrites `until` every tick, so log a held document at most once an hour.
	key := name + "\x00" + phase
	m.holdLogMu.Lock()
	if m.holdLogged == nil {
		m.holdLogged = map[string]time.Time{}
	}
	due := now.Sub(m.holdLogged[key]) >= time.Hour
	if due {
		m.holdLogged[key] = now
	}
	m.holdLogMu.Unlock()
	if due {
		m.log("roster: " + seat + " is held (" + reason + ") until " + until.UTC().Format(time.RFC3339) + "; " + name + " (" + phase + ") waits unclaimed")
	}
	return true
}

func (m *Manager) resolveLegacyTargetModel(route workflowRoute, lease Lease) string {
	harnessSettings := m.harnessSettings()
	phase := normalizeLeasePhase(route.Name, lease.Phase)

	// 1. If this is a review phase, default to reviewer model
	if phase == phaseTaskReview || phase == phaseGoalReview || phase == "review" {
		if harnessSettings.ReviewerModel != "" {
			return harnessSettings.ReviewerModel
		}
		return "kimi"
	}

	// 2. Check frontmatter of the claimed task / goal file
	if doc, err := ReadDocument(lease.File); err == nil {
		if agent, ok := doc.FrontMatter["agent"].(string); ok && strings.TrimSpace(agent) != "" {
			return strings.TrimSpace(agent)
		}
		if model, ok := doc.FrontMatter["model"].(string); ok && strings.TrimSpace(model) != "" {
			return strings.TrimSpace(model)
		}
	}

	// 3. Fallback to developer model
	if harnessSettings.DeveloperModel != "" {
		return harnessSettings.DeveloperModel
	}

	// 4. Default harness model
	return harnessSettings.Model
}

func phaseForFile(routeName, file string) string {
	status := filepath.Base(filepath.Dir(file))
	switch routeName + ":" + status {
	case "tasks:working":
		return phaseTaskImplementation
	case "tasks:reviewing":
		return phaseTaskReview
	case "goals:planning":
		return phaseGoalPlanning
	case "goals:reviewing":
		return phaseGoalReview
	default:
		return status
	}
}

func (m *Manager) relatedTasksForGoal(file string) string {
	if filepath.Base(filepath.Dir(filepath.Dir(file))) != "goals" {
		return "- Not applicable."
	}
	document, err := ReadDocument(file)
	if err != nil {
		return "- Unable to read linked tasks: " + err.Error()
	}
	tasks, err := m.goalRoundTasks(document)
	if err != nil {
		return "- Unable to resolve linked tasks: " + err.Error()
	}
	if len(tasks) == 0 {
		return "- No tasks are linked to the current round."
	}
	lines := make([]string, 0, len(tasks))
	for _, task := range tasks {
		lines = append(lines, "- ["+task.Status+"] "+task.Path)
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

func (m *Manager) Wait() { m.jobs.Wait() }

func (m *Manager) WaitForIdle(ctx context.Context) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		leases, err := m.loadLeases()
		if err != nil {
			return err
		}
		busy := false
		for _, lease := range leases {
			if m.Harness.IsActive(lease.SessionKey) || m.isInflight(lease.ID) {
				busy = true
				break
			}
		}
		if !busy {
			m.jobs.Wait()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (m *Manager) Status() (int, int, error) {
	leases, err := m.loadLeases()
	if err != nil {
		return 0, 0, err
	}
	m.mu.Lock()
	active := len(m.inflight)
	m.mu.Unlock()
	return len(leases), active, nil
}

func (m *Manager) ScheduledCheckpoints(now time.Time) ([]ScheduledCheckpoint, error) {
	route, ok := routeByName("goals")
	if !ok {
		return nil, nil
	}
	directory := filepath.Join(filepath.Dir(m.Config.Resolve(route.Source)), "active")
	entries, truncated, err := readDirectoryEntries(directory, maxStatusDirectoryEntries)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	waits := make([]ScheduledCheckpoint, 0)
	inspected := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".md") || entry.Name() == "AGENTS.md" {
			continue
		}
		if inspected >= maxStatusDocuments {
			return waits, fmt.Errorf("scheduled goal checkpoints are incomplete: document limit reached")
		}
		inspected++
		data, err := readStatusDocument(filepath.Join(directory, entry.Name()))
		if err != nil {
			continue
		}
		document, err := ParseDocument(data)
		if err != nil {
			m.reportUnparseableDocument(filepath.Join(directory, entry.Name()), err)
			continue
		}
		trigger := stringField(document, "review_trigger")
		if trigger != "scheduled" && trigger != "all_round_tasks_settled_or_checkpoint" {
			continue
		}
		value, ok := document.FrontMatter["next_review_at"]
		if !ok {
			continue
		}
		var at time.Time
		switch typed := value.(type) {
		case string:
			at, err = time.Parse(time.RFC3339, typed)
		case time.Time:
			at = typed
		}
		if err != nil || at.IsZero() || !at.After(now) {
			continue
		}
		waits = append(waits, ScheduledCheckpoint{
			ID: documentID(document), Title: stringField(document, "title"), At: at.UTC(),
			Reason: stringField(document, "checkpoint_reason"),
		})
	}
	if truncated {
		return waits, fmt.Errorf("scheduled goal checkpoints are incomplete: directory entry limit reached")
	}
	sort.Slice(waits, func(i, j int) bool { return waits[i].At.Before(waits[j].At) })
	return waits, nil
}

func (m *Manager) leaseDirectory() string     { return m.Config.StatePath("runtime", "leases") }
func (m *Manager) leasePath(id string) string { return filepath.Join(m.leaseDirectory(), id+".json") }

func (m *Manager) leaseExists(id string) bool {
	_, err := os.Stat(m.leasePath(id))
	return err == nil
}

func (m *Manager) saveLease(lease Lease) error {
	if err := os.MkdirAll(m.leaseDirectory(), 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(lease, "", "  ")
	if err != nil {
		return err
	}
	return fsx.AtomicWriteFile(m.leasePath(lease.ID), append(data, '\n'), 0o600)
}

func (m *Manager) loadLease(id string) (Lease, error) {
	data, err := os.ReadFile(m.leasePath(id))
	if err != nil {
		return Lease{}, err
	}
	var lease Lease
	err = json.Unmarshal(data, &lease)
	return lease, err
}

func (m *Manager) loadLeases() ([]Lease, error) {
	entries, err := os.ReadDir(m.leaseDirectory())
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var leases []Lease
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		id := strings.TrimSuffix(entry.Name(), ".json")
		lease, err := m.loadLease(id)
		if err != nil {
			m.log("invalid lease " + entry.Name() + ": " + err.Error())
			continue
		}
		leases = append(leases, lease)
	}
	sort.Slice(leases, func(i, j int) bool { return leases[i].StartedAt.Before(leases[j].StartedAt) })
	return leases, nil
}

// LeaseForSession returns the newest persisted lease for an orchestrator
// session. The value copy remains safe if the lease is concurrently updated.
func (m *Manager) LeaseForSession(sessionKey string) (Lease, bool) {
	leases, err := m.loadLeases()
	if err != nil {
		return Lease{}, false
	}
	for index := len(leases) - 1; index >= 0; index-- {
		if leases[index].SessionKey == sessionKey {
			return leases[index], true
		}
	}
	return Lease{}, false
}

// PrepareControlContinuation performs the one durable gate immediately before
// a control-attributable continuation. It refuses moved documents, changed
// owners/sessions, cancellation/error states, and executions no longer owned
// by this manager. A successful gate returns the lease to processing; the
// provider's subsequent events remain responsible for heartbeat activity.
func (m *Manager) PrepareControlContinuation(expected Lease, expectedDocumentID string) bool {
	if !m.ControlStillValid(expected, expectedDocumentID) || !m.Harness.IsActive(expected.SessionKey) {
		return false
	}
	current, err := m.loadLease(expected.ID)
	if err != nil {
		return false
	}
	if current.State != "awaiting_transition" && current.State != "processing" && current.State != "recovering" {
		return false
	}
	current.State = "processing"
	current.LastError = ""
	if err := m.saveLease(current); err != nil {
		m.log("save control continuation lease: " + err.Error())
		return false
	}
	if jobID := m.runtimeJob(expected.ID); jobID > 0 && m.JobUpdated != nil {
		m.JobUpdated(jobID, current)
	}
	return true
}

// ReserveControlProviderTurn performs the same durable provider-turn
// reservation used by ordinary dispatch immediately before a native control,
// queued control, or guarded continuation reaches the harness.
func (m *Manager) ReserveControlProviderTurn(expected Lease, expectedDocumentID string) bool {
	if !m.ControlStillValid(expected, expectedDocumentID) {
		return false
	}
	first, iterations, err := ReserveProviderTurn(expected.File, time.Now().UTC())
	if err != nil {
		m.log("reserve control provider turn: " + err.Error())
		return false
	}
	if jobID := m.runtimeJob(expected.ID); jobID > 0 && m.JobTimingUpdated != nil {
		m.JobTimingUpdated(jobID, first, iterations)
	}
	return true
}

// ControlStillValid checks immutable execution identity and durable claimed
// state without advancing heartbeats or workflow status.
func (m *Manager) ControlStillValid(expected Lease, expectedDocumentID string) bool {
	if expected.ID == "" || expectedDocumentID == "" || m.runtimeJob(expected.ID) == 0 || m.isControlCancelled(expected.ID) {
		return false
	}
	current, err := m.loadLease(expected.ID)
	if err != nil || current.OwnerID != expected.OwnerID || current.SessionKey != expected.SessionKey || current.File != expected.File || current.Phase != expected.Phase {
		return false
	}
	if current.State != "awaiting_transition" && current.State != "processing" && current.State != "recovering" {
		return false
	}
	document, err := ReadDocument(current.File)
	if err != nil {
		return false
	}
	if documentID(document) != expectedDocumentID {
		return false
	}
	status, _ := document.FrontMatter["status"].(string)
	expectedStatus := ""
	switch normalizeLeasePhase(expected.Route, expected.Phase) {
	case phaseTaskImplementation:
		expectedStatus = "working"
	case phaseTaskReview, phaseGoalReview:
		expectedStatus = "reviewing"
	case phaseGoalPlanning:
		expectedStatus = "planning"
	}
	return expectedStatus != "" && status == expectedStatus
}

// MarkControlCancellation fences automatic control continuation before a job
// interrupt races the provider's terminal event. It is process-local: a later
// owner may still use ordinary durable stale recovery for unfinished work.
func (m *Manager) MarkControlCancellation(sessionKey string) string {
	lease, ok := m.LeaseForSession(sessionKey)
	if !ok {
		return ""
	}
	m.mu.Lock()
	m.controlCancelled[lease.ID]++
	for id, at := range m.operatorStops {
		if time.Since(at) > operatorStopWindow {
			delete(m.operatorStops, id)
		}
	}
	m.operatorStops[lease.ID] = time.Now()
	m.mu.Unlock()
	return lease.ID
}

// RestoreControlCancellation releases one cancellation reservation after the
// provider rejects the matching interrupt. Counting reservations prevents one
// failed concurrent request from clearing another accepted cancellation.
func (m *Manager) RestoreControlCancellation(leaseID string) {
	if leaseID == "" {
		return
	}
	m.mu.Lock()
	if m.controlCancelled[leaseID] <= 1 {
		delete(m.controlCancelled, leaseID)
		delete(m.operatorStops, leaseID)
	} else {
		m.controlCancelled[leaseID]--
	}
	m.mu.Unlock()
}

// operatorStopPending reports whether an operator stop was recorded for the lease
// within operatorStopWindow. Unlike controlCancelled it is NOT cleared by
// finishRuntimeJob, which runs before recordError after a failed send.
func (m *Manager) operatorStopPending(leaseID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	at, ok := m.operatorStops[leaseID]
	return ok && time.Since(at) <= operatorStopWindow
}

// consumeOperatorStop is operatorStopPending that also forgets the record.
func (m *Manager) consumeOperatorStop(leaseID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	at, ok := m.operatorStops[leaseID]
	delete(m.operatorStops, leaseID)
	return ok && time.Since(at) <= operatorStopWindow
}

func (m *Manager) isControlCancelled(leaseID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.controlCancelled[leaseID] > 0
}

// transientProviderError reports whether a harness send error is a provider
// refusing work right now (backend overload, "please retry") rather than a
// dead or misbehaving runner. Only the harness control-plane error text is
// matched, never the provider event stream, and quota refusals (429 plan
// limits) deliberately do not match: those wait for reset on the same runner
// without spending an attempt.
func transientProviderError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "please retry") || strings.Contains(msg, "overloaded") || strings.Contains(msg, "prompt_not_delivered")
}

// restoreLeaseForRetry returns a lease parked by a refused attempt's terminal
// error to live-work state, so an in-place retry is neither observed as a turn
// awaiting transition reconciliation nor left carrying the refusal it recovered
// from.
func (m *Manager) restoreLeaseForRetry(leaseID string, recovery bool) {
	current, err := m.loadLease(leaseID)
	if err != nil {
		return
	}
	current.LastError = ""
	if current.State == "awaiting_transition" {
		current.State = "processing"
		if recovery {
			current.State = "recovering"
		}
	}
	current.HeartbeatAt = time.Now().UTC()
	if err := m.saveLease(current); err != nil {
		m.log("save lease retry state: " + err.Error())
	}
}

// operatorStopHold is how long a task an operator stopped stays parked before
// its wake_at brings it back. A stop is "stop doing this now", not "never".
const operatorStopHold = 12 * time.Hour

// operatorStopWindow is how long a recorded stop may wait for the dispatch error handler to
// see it; a record older than this belongs to some other cancellation and is ignored.
const operatorStopWindow = 10 * time.Minute

// restartInterrupted reports whether a harness/dispatch error is the
// orchestrator's own lifecycle (service stop or the settling window after a
// start) rather than a defect of the task being dispatched. Those kills must
// not increment RecoveryCount or the consecutive-error path into failed/.
func (m *Manager) restartInterrupted(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if ctx != nil && ctx.Err() != nil {
		return true
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if !signalLifecycleError(err) {
		return false
	}
	started := m.startedAt
	if started.IsZero() {
		return false
	}
	return time.Since(started) < restartInterruptGrace
}

func signalLifecycleError(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "signal: terminated") || strings.Contains(msg, "signal: killed")
}

// recordRestartInterrupt parks the lease for ordinary re-adoption without
// spending RecoveryCount, phase attempt, or the consecutive-error fail path.
// Drain-on-shutdown is deliberately not used: re-adoption already preserves
// the claim, and delaying a binary install for in-flight turns is what made
// operators force-kill tonight.
func (m *Manager) recordRestartInterrupt(lease Lease, err error) {
	now := time.Now().UTC()
	if current, loadErr := m.loadLease(lease.ID); loadErr == nil {
		lease = current
	}
	lease.LastError = err.Error()
	lease.State = "error"
	lease.HeartbeatAt = now
	if saveErr := m.saveLease(lease); saveErr != nil {
		m.log("save restart-interrupted lease: " + saveErr.Error())
	}
	note := fmt.Sprintf("interrupted by a service restart at %s; attempt not spent", now.Format(time.RFC3339))
	if progressErr := updateDocumentProgress(lease.File, now, note); progressErr != nil {
		m.log("record restart interrupt progress: " + progressErr.Error())
	}
	msg := fmt.Sprintf("restart interrupted %s: %v", lease.File, err)
	if m.LogError != nil {
		m.LogError("orchestrator", "restart_interrupt", msg)
	} else {
		m.log(msg)
	}
}

// recordOperatorStop parks a task whose job an operator deliberately stopped
// (MarkControlCancellation recorded it before the interrupt, in operatorStops, which
// survives finishRuntimeJob: after a failed send finish() runs BEFORE recordError and
// clears controlCancelled, so the first version of this check never fired live). The job's context is
// cancelled by that stop, which restartInterrupted cannot tell from a service
// restart, so the stop used to be filed as "interrupted by a service restart",
// the lease was kept for re-adoption, and Spynel dispatched a recovery agent on
// work the operator had just stopped (found live 2026-10-03). Parking in
// waiting/ with a wake_at is the same shape recordQuotaError uses: the lease is
// removed, the runtime job ends, no attempt is spent, and a timer owns the task.
// Only task implementation is handled; reviews and goals keep their own
// transitions and fall through to the old behaviour. It reports whether it
// handled the error.
func (m *Manager) recordOperatorStop(lease Lease) bool {
	if lease.Phase != phaseTaskImplementation || !m.consumeOperatorStop(lease.ID) {
		return false
	}
	route, ok := routeByName(lease.Route)
	if !ok {
		return false
	}
	now := time.Now().UTC()
	wake := now.Add(operatorStopHold).Format(time.RFC3339)
	base := filepath.Dir(m.Config.Resolve(route.Source))
	waitingTarget := filepath.Join(base, "waiting", filepath.Base(lease.File))
	note := fmt.Sprintf("stopped by an operator at %s; parked until %s; attempt not spent", now.Format(time.RFC3339), wake)
	update := func(fm map[string]any) {
		fm["wake_at"] = wake
		fm["waiting_for"] = "stopped by an operator; resumes at wake_at, or re-queue or cancel it sooner"
		fm["stopped_by"] = "operator"
		fm[resumeCreditField] = true
	}
	if moveErr := moveDocumentWithUpdate(lease.File, waitingTarget, "waiting", now, note, update); moveErr != nil {
		m.log("park operator-stopped task in waiting: " + moveErr.Error())
		return false
	}
	_ = os.Remove(m.leasePath(lease.ID))
	m.finishRuntimeJob(lease.ID)
	m.log(fmt.Sprintf("%s: %s", note, waitingTarget))
	return true
}

func (m *Manager) recordError(ctx context.Context, lease Lease, err error) {
	if m.recordPreemption(lease) {
		return
	}
	if m.recordOperatorStop(lease) {
		return
	}
	if m.restartInterrupted(ctx, err) {
		m.recordRestartInterrupt(lease, err)
		return
	}
	if isQuotaError(err) {
		m.recordQuotaError(lease, err)
		return
	}
	lease.LastError = err.Error()
	lease.RecoveryCount++
	lease.State = "error"
	lease.HeartbeatAt = time.Now().UTC()

	route, ok := routeByName(lease.Route)
	if ok && lease.RecoveryCount >= 3 && route.Name == "tasks" {
		base := filepath.Dir(m.Config.Resolve(route.Source))
		failedTarget := filepath.Join(base, "failed", filepath.Base(lease.File))
		note := fmt.Sprintf("Terminal failure: task moved to failed/ after %d consecutive dispatch/execution errors: %v", lease.RecoveryCount, err)
		if moveErr := moveDocumentWithProgress(lease.File, failedTarget, "failed", time.Now().UTC(), note); moveErr == nil {
			_ = os.Remove(m.leasePath(lease.ID))
			m.finishRuntimeJob(lease.ID)
			m.log(fmt.Sprintf("abandoned repeatedly failing task %s into failed/: %v", lease.File, err))
			go m.captureAutopsyAndAlert(lease, fmt.Errorf("task failed after %d attempts: %w", lease.RecoveryCount, err))
			return
		}
	}

	if saveErr := m.saveLease(lease); saveErr != nil {
		m.log("save failed lease: " + saveErr.Error())
	}
	msg := fmt.Sprintf("dispatch %s: %v", lease.File, err)
	if m.LogError != nil {
		m.LogError("orchestrator", "dispatch_error", msg)
	} else {
		m.log(msg)
	}
	go m.captureAutopsyAndAlert(lease, err)
}

func (m *Manager) captureAutopsyAndAlert(lease Lease, err error) {
	if err == nil {
		return
	}
	diagDir := m.Config.StatePath("diagnostics")
	if mkErr := os.MkdirAll(diagDir, 0o700); mkErr != nil {
		return
	}
	timestamp := time.Now().UTC().Format("20060102T150405Z")
	filename := fmt.Sprintf("err-%s-%s.json", timestamp, lease.ID)
	diagPath := filepath.Join(diagDir, filename)

	autopsy := map[string]any{
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
		"lease_id":    lease.ID,
		"claim_id":    lease.ClaimID,
		"file":        lease.File,
		"route":       lease.Route,
		"session_key": lease.SessionKey,
		"thread_id":   lease.ThreadID,
		"error":       err.Error(),
		"state":       lease.State,
	}

	if data, mErr := json.MarshalIndent(autopsy, "", "  "); mErr == nil {
		_ = os.WriteFile(diagPath, data, 0o600)
	}

	alerterPath := "/home/dajoey/ops/telemetry_alerter.py"
	if _, statErr := os.Stat(alerterPath); statErr == nil {
		taskName := filepath.Base(lease.File)
		cmd := exec.Command("python3", alerterPath,
			"--title", "Runner Dispatch Error",
			"--component", "spyjo-orchestrator",
			"--error", err.Error(),
			"--severity", "P1",
			"--task-id", taskName,
			"--runner", lease.SessionKey,
		)
		_ = cmd.Run()
	}
}

func (m *Manager) isInflight(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.inflight[key]
}

// canAdmitClaim reports whether one more claim fits. A lane: repair claim may
// use the reserved capacity beyond capacityLimit (orchestrator.repair_slots),
// so a repair for stuck workers can be claimed while those workers hold every
// normal slot; a normal claim still stops at capacityLimit. repairActive also
// bounds concurrent repair dispatches at repairSlots.
func (m *Manager) canAdmitClaim(isRepair bool) bool {
	m.capacityMu.Lock()
	limit := m.capacityLimit
	repairSlots := m.repairSlots
	repairActive := m.repairActive
	m.capacityMu.Unlock()
	if isRepair {
		limit += repairSlots
		if repairActive >= repairSlots {
			return false
		}
	}
	m.mu.Lock()
	count := len(m.inflight)
	m.mu.Unlock()
	return count < limit
}

func (m *Manager) setInflight(key string, value bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if value {
		m.inflight[key] = true
	} else {
		delete(m.inflight, key)
	}
}

func (m *Manager) setRuntimeJob(leaseID string, jobID int) {
	if jobID <= 0 {
		return
	}
	m.mu.Lock()
	m.runtimeJobs[leaseID] = jobID
	m.mu.Unlock()
}

func (m *Manager) runtimeJob(leaseID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.runtimeJobs[leaseID]
}

func (m *Manager) finishRuntimeJob(leaseID string) {
	m.mu.Lock()
	jobID := m.runtimeJobs[leaseID]
	delete(m.runtimeJobs, leaseID)
	delete(m.controlCancelled, leaseID)
	m.mu.Unlock()
	if jobID > 0 && m.JobFinished != nil {
		m.JobFinished(jobID)
	}
}

func (m *Manager) log(message string) {
	if m.Log != nil {
		m.Log(message)
	}
}

func leaseID(route, path string) string {
	hash := sha256.Sum256([]byte(route + "\x00" + filepath.Clean(path)))
	return hex.EncodeToString(hash[:12])
}

// httpTooManyRequestsRegex matches 429 only as a whole token (the HTTP status), never as digits
// inside a path ("fix-issue-429.md"), a date ("20260429"), a version ("1.429.0") or an id.
// kimiUsageLimitRegex matches Kimi's wall wording: "reached your 5-hour usage limit",
// "reached your weekly (7-day) usage limit".
var kimiUsageLimitRegex = regexp.MustCompile(`reached your [a-z0-9() -]*usage limit`)

var httpTooManyRequestsRegex = regexp.MustCompile(`(?:^|[^0-9a-z_./-])429(?:$|[^0-9a-z_/-]|\.(?:$|[^0-9a-z]))`)

func isQuotaError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return httpTooManyRequestsRegex.MatchString(msg) ||
		strings.Contains(msg, "rate limit") ||
		strings.Contains(msg, "rate_limit") ||
		strings.Contains(msg, "quota limit") ||
		strings.Contains(msg, "resource_exhausted") ||
		strings.Contains(msg, "too many requests") ||
		// A prepaid API credit that is spent ("Credit balance is too low") walls the seat too.
		strings.Contains(msg, "credit balance is too low") ||
		strings.Contains(msg, "credit balance too low") ||
		// Kimi's 5-hour and weekly walls: "403 You've reached your 5-hour usage limit. Your quota will
		// reset ..." carries none of the words above. Matched on Kimi's own wording, not a bare
		// "usage limit", which other errors can carry (a parked quota wall never reaches failed/).
		kimiUsageLimitRegex.MatchString(msg) ||
		strings.Contains(msg, "quota will reset")
}

func resolveRunnerName(document Document, lease Lease) string {
	if a := stringField(document, "agent"); a != "" {
		return a
	}
	if r := stringField(document, "runner"); r != "" {
		return r
	}
	return "default"
}

var (
	rfc3339Regex     = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})`)
	relativeSecRegex = regexp.MustCompile(`resets? in (\d+)\s*(?:s|sec|seconds?)`)
	runnerPlanMap    = map[string]string{
		"opencode": "opencode_go",
		"kimi":     "kimi_coding",
		"agy":      "antigravity",
		"pi":       "zai",
	}
	// providerPlanMap is the provider-prefix half of the plan lookup, the
	// same table ~/ops/spyjo-quota-gate.py's PLAN_BY_PROVIDER uses, so the
	// orchestrator and the gate can never disagree about which plan a
	// provider/model pin spends.
	providerPlanMap = map[string]string{
		"opencode-go":     "opencode_go",
		"zai":             "zai",
		"zai-coding-plan": "zai",
		"kimi":            "kimi_coding",
		"kimi-for-coding": "kimi_coding",
		"kimi-code":       "kimi_coding",
		"antigravity":     "antigravity",
	}
)

// quotaPlanForModelString maps a harness model string to the usage plan it
// spends: the provider prefix of a "provider/model" pin through
// providerPlanMap, else the legacy runner-name map for bare runner strings.
func quotaPlanForModelString(model string) string {
	if p := strings.IndexByte(model, '/'); p > 0 {
		return providerPlanMap[model[:p]]
	}
	return runnerPlanMap[model]
}

// quotaPlanForTargetModel resolves the usage plan a dispatch's target model
// spends. A roster seat ("@name") is looked up read-only; everything else
// goes through quotaPlanForModelString. An empty result means unmetered or
// unknown, and the caller falls back to the legacy document path.
func (m *Manager) quotaPlanForTargetModel(targetModel string) string {
	if seat, ok := strings.CutPrefix(targetModel, roster.StaffPrefix); ok {
		return m.quotaPlanForStaff(seat)
	}
	return quotaPlanForModelString(targetModel)
}

// quotaPlanForStaff resolves the usage plan a roster seat spends, read-only
// by design: roster.Load plus a staff lookup, never staffing.Assign, which
// counts a usage session — the dispatch already paid that count when it
// chose the seat. Mirrors the quota gate: agy always spends the antigravity
// plan; a pinned "--model" names the plan through its provider prefix; a
// seat without a pinned model spends its runner's default plan.
func (m *Manager) quotaPlanForStaff(seat string) string {
	staffing, loadErr := roster.Load(m.Config.StatePath())
	if loadErr != nil || staffing == nil {
		return ""
	}
	s, ok := staffing.Staff[seat]
	if !ok {
		return ""
	}
	if s.Runner == "agy" {
		return "antigravity"
	}
	for i := 0; i+1 < len(s.Args); i++ {
		if s.Args[i] == "--model" {
			return quotaPlanForModelString(s.Args[i+1])
		}
	}
	return runnerPlanMap[s.Runner]
}

// quotaNameForTargetModel names what hit the wall in the park note: the seat
// for a roster seat, the model string itself for runner and provider/model
// pins (the legacy note already named those verbatim).
func quotaNameForTargetModel(targetModel string) string {
	if seat, ok := strings.CutPrefix(targetModel, roster.StaffPrefix); ok {
		return seat
	}
	return targetModel
}

// quotaWallNameAndPlan names what hit the quota wall and the usage plan the
// dispatch actually spent. The lease's recorded target model is
// authoritative: roster-routed documents carry no agent/runner pin, so
// before it every such park named "default" and woke a blind hour later.
// The document pin stays the legacy path for leases dispatched before the
// field was recorded.
func (m *Manager) quotaWallNameAndPlan(document Document, readErr error, lease Lease) (name, planKey string) {
	if lease.TargetModel != "" {
		name = quotaNameForTargetModel(lease.TargetModel)
		planKey = m.quotaPlanForTargetModel(lease.TargetModel)
	} else if readErr != nil {
		return "default", "default"
	} else {
		name = resolveRunnerName(document, lease)
		planKey = runnerPlanMap[name]
	}
	if planKey == "" {
		// Same shape as the legacy lookup: an unmetered or unknown name is
		// still looked up verbatim, so it misses the snapshot and parks the
		// blind hour instead of guessing a plan.
		planKey = name
	}
	return name, planKey
}

func parseQuotaResetTime(errMsg string, now time.Time) (time.Time, bool) {
	if match := rfc3339Regex.FindString(errMsg); match != "" {
		if t, err := time.Parse(time.RFC3339, match); err == nil && t.After(now) {
			return t, true
		}
	}
	if m := relativeSecRegex.FindStringSubmatch(strings.ToLower(errMsg)); len(m) == 2 {
		if sec, err := strconv.Atoi(m[1]); err == nil && sec > 0 {
			return now.Add(time.Duration(sec) * time.Second), true
		}
	}
	return time.Time{}, false
}

// readSnapshotResetTime reads the wake time for a usage plan from the quota
// gate's snapshot: the LATEST reset among its exhausted (100%) windows — the
// plan is unusable until every exhausted window clears, so the earliest would
// wake at a window that is still full and re-hit the wall (found live
// 2026-09-28). With no exhausted window it keeps the legacy earliest-future-
// reset heuristic for a stale snapshot. planKey is the plan's snapshot key,
// already resolved by the caller.
func readSnapshotResetTime(planKey string, now time.Time) (time.Time, bool) {
	if usageSnapshotPath == "" {
		return time.Time{}, false
	}
	data, err := os.ReadFile(usageSnapshotPath)
	if err != nil {
		return time.Time{}, false
	}
	var snap struct {
		Plans map[string]map[string]any `json:"plans"`
	}
	if err := json.Unmarshal(data, &snap); err != nil {
		return time.Time{}, false
	}
	planData, ok := snap.Plans[planKey]
	if !ok || planData == nil {
		return time.Time{}, false
	}

	var exhaustedReset time.Time
	var anyFutureReset time.Time

	for _, v := range planData {
		windowMap, ok := v.(map[string]any)
		if !ok {
			continue
		}
		resetsAtRaw, _ := windowMap["resets_at"].(string)
		if resetsAtRaw == "" {
			continue
		}
		t, err := time.Parse(time.RFC3339, resetsAtRaw)
		if err != nil || !t.After(now) {
			continue
		}

		pct := numberValue(windowMap["pct"])
		if pct >= 100 {
			if exhaustedReset.IsZero() || t.After(exhaustedReset) {
				exhaustedReset = t
			}
		}
		if anyFutureReset.IsZero() || t.Before(anyFutureReset) {
			anyFutureReset = t
		}
	}

	if !exhaustedReset.IsZero() {
		return exhaustedReset, true
	}
	if !anyFutureReset.IsZero() {
		return anyFutureReset, true
	}
	return time.Time{}, false
}

func resolveQuotaResetTime(errMsg, planKey string, now time.Time) time.Time {
	if t, ok := parseQuotaResetTime(errMsg, now); ok {
		return t
	}
	if t, ok := readSnapshotResetTime(planKey, now); ok {
		return t
	}
	return now.Add(1 * time.Hour)
}

func (m *Manager) recordQuotaError(lease Lease, err error) {
	now := time.Now().UTC()
	document, readErr := ReadDocument(lease.File)
	runner, planKey := m.quotaWallNameAndPlan(document, readErr, lease)
	resetTime := resolveQuotaResetTime(err.Error(), planKey, now)
	resetTimeStr := resetTime.UTC().Format(time.RFC3339)

	route, ok := routeByName(lease.Route)
	if !ok {
		return
	}
	base := filepath.Dir(m.Config.Resolve(route.Source))
	waitingTarget := filepath.Join(base, "waiting", filepath.Base(lease.File))
	note := fmt.Sprintf("quota wall on %s; parked until %s; attempt not spent", runner, resetTimeStr)
	// The wake returns the document to the phase the wall interrupted: review
	// and planning parks record it, an implementation park leaves it absent so
	// a stale value cannot misroute the wake.
	resumeStatus := ""
	switch normalizeLeasePhase(lease.Route, lease.Phase) {
	case phaseTaskReview, phaseGoalReview:
		resumeStatus = "review"
	case phaseGoalPlanning:
		resumeStatus = "planning"
	}
	update := func(fm map[string]any) {
		fm["quota_reset_at"] = resetTimeStr
		fm["wake_at"] = resetTimeStr
		fm[resumeCreditField] = true
		if resumeStatus != "" {
			fm["resume_status"] = resumeStatus
		} else {
			delete(fm, "resume_status")
		}
	}
	if moveErr := moveDocumentWithUpdate(lease.File, waitingTarget, "waiting", now, note, update); moveErr != nil {
		m.log("move quota error document to waiting: " + moveErr.Error())
		return
	}
	_ = os.Remove(m.leasePath(lease.ID))
	m.finishRuntimeJob(lease.ID)
	m.log(fmt.Sprintf("%s: %s", note, waitingTarget))
}
