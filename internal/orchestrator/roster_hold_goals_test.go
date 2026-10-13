package orchestrator

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/roster"
	"github.com/agent0ai/spynel/internal/workspace"
)

type goalHoldFixture struct {
	cfg        config.Config
	manager    *Manager
	fake       *fakeHarness
	goalsBase  string
	writeHolds func(staff ...string)
}

// newGoalHoldFixture builds a workspace whose goal_planning seat (thinker) can be put on a pace hold.
func newGoalHoldFixture(t *testing.T) *goalHoldFixture {
	t.Helper()
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(config.PathForRoot(root))
	state := cfg.StatePath()
	body := `
staff:
  implementer: {runner: opencode}
  thinker: {runner: pi}
roles:
  task_implementation: implementer
  goal_planning: thinker
`
	if err := os.WriteFile(roster.Path(state), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	fx := &goalHoldFixture{cfg: cfg, fake: newFakeRecipient()}
	fx.writeHolds = func(staff ...string) {
		t.Helper()
		entries := map[string]map[string]string{}
		for _, name := range staff {
			entries[name] = map[string]string{"until": time.Now().Add(time.Hour).UTC().Format(time.RFC3339), "reason": "zai-pace"}
		}
		data, _ := json.Marshal(map[string]any{"updated_at": time.Now().UTC().Format(time.RFC3339), "staff": entries})
		if err := os.MkdirAll(filepath.Join(state, "runtime"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(state, "runtime", "roster-holds.json"), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	fx.manager = New(cfg, fx.fake, extensions.Runner{Directory: filepath.Join(root, "missing")})
	t.Cleanup(fx.manager.Wait)
	fx.goalsBase = filepath.Dir(cfg.Resolve(workflowRoutes()[1].Source))
	return fx
}

func (fx *goalHoldFixture) sessions() int {
	fx.fake.mu.Lock()
	defer fx.fake.mu.Unlock()
	return fx.fake.calls
}

func (fx *goalHoldFixture) planningAttempt(t *testing.T, status, name string) int {
	t.Helper()
	document, err := ReadDocument(filepath.Join(fx.goalsBase, status, name))
	if err != nil {
		t.Fatalf("goal not in %s/: %v", status, err)
	}
	return numberValue(document.FrontMatter["planning_attempt"])
}

// finishedGoalReview puts a goal in planning/ with a goal-review lease whose file has moved on, the
// state reconcileTransitions sees when a goal review ends by choosing planning.
func (fx *goalHoldFixture) finishedGoalReview(t *testing.T) string {
	t.Helper()
	path, err := Create(fx.cfg, "goals", "paced re-planning goal", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(path)
	editFrontMatter(t, path, func(fm map[string]any) { fm["planning_attempt"] = 1 })
	reviewing := filepath.Join(fx.goalsBase, "reviewing", name)
	planning := filepath.Join(fx.goalsBase, "planning", name)
	if err := moveDocument(path, planning, "planning", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	lease := Lease{
		ID: leaseID("goals:"+phaseGoalReview, name), ClaimID: "review", DocumentType: "goal",
		OwnerID: fx.manager.ownerID, Route: "goals", File: reviewing, SessionKey: "test:goal:review",
		Phase: phaseGoalReview, ClaimAttempt: 1, StartedAt: now, HeartbeatAt: now, State: "processing",
	}
	if err := fx.manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	return name
}

// A goal review that ends in planning while its planning seat is held starts no session and spends
// no planning attempt; it is dispatched, as a fresh round, once the hold lifts.
func TestGoalReplanningWaitsUnderRosterHold(t *testing.T) {
	fx := newGoalHoldFixture(t)
	fx.writeHolds("thinker")
	name := fx.finishedGoalReview(t)

	for i := 0; i < 2; i++ { // the orphan-recovery pass that follows must not start it either
		if err := fx.manager.reconcileTransitions(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := fx.manager.recoverOrphanClaims(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	fx.manager.Wait()
	if got := fx.sessions(); got != 0 {
		t.Fatalf("a planning session started on held thinker: %d", got)
	}
	if got := fx.planningAttempt(t, "planning", name); got != 1 {
		t.Fatalf("planning_attempt changed under the hold: %d", got)
	}
	if leases, _ := fx.manager.loadLeases(); len(leases) != 0 {
		t.Fatalf("a lease was left behind: %d", len(leases))
	}

	fx.writeHolds() // the hold lifts
	if err := fx.manager.recoverOrphanClaims(context.Background()); err != nil {
		t.Fatal(err)
	}
	fx.manager.Wait()
	if got := fx.sessions(); got != 1 {
		t.Fatalf("planning did not start after the hold lifted: %d sessions", got)
	}
	if got := fx.planningAttempt(t, "planning", name); got != 2 {
		t.Fatalf("the deferred round did not spend its attempt when it started: planning_attempt = %d", got)
	}
	document, _ := ReadDocument(filepath.Join(fx.goalsBase, "planning", name))
	if _, marked := document.FrontMatter["planning_hold_deferred"]; marked {
		t.Error("the deferral marker outlived the start")
	}
}

// A goal due to resume planning from waiting/ while its seat is held stays where it is, untouched,
// and wakes normally after the hold lifts.
func TestWokenGoalWaitsUnderRosterHold(t *testing.T) {
	fx := newGoalHoldFixture(t)
	fx.writeHolds("thinker")
	path, err := Create(fx.cfg, "goals", "paced woken goal", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(path)
	editFrontMatter(t, path, func(fm map[string]any) {
		fm["planning_attempt"] = 1
		fm["resume_status"] = "planning"
		fm["wake_at"] = time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	})
	waiting := filepath.Join(fx.goalsBase, "waiting", name)
	if err := moveDocument(path, waiting, "waiting", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(waiting)

	if err := fx.manager.wakeWaitingDocuments(context.Background()); err != nil {
		t.Fatal(err)
	}
	fx.manager.Wait()
	after, err := os.ReadFile(waiting)
	if err != nil {
		t.Fatalf("the held goal left waiting/: %v", err)
	}
	if string(before) != string(after) {
		t.Error("the held goal's document was rewritten")
	}
	if got := fx.sessions(); got != 0 {
		t.Fatalf("a planning session started on held thinker: %d", got)
	}

	fx.writeHolds() // the hold lifts
	if err := fx.manager.wakeWaitingDocuments(context.Background()); err != nil {
		t.Fatal(err)
	}
	fx.manager.Wait()
	if got := fx.sessions(); got != 1 {
		t.Fatalf("planning did not start after the hold lifted: %d sessions", got)
	}
	if got := fx.planningAttempt(t, "planning", name); got != 2 {
		t.Fatalf("planning_attempt = %d after the woken start, want 2", got)
	}
}

// With no hold, both paths dispatch exactly as before.
func TestGoalPlanningPathsDispatchWithoutHold(t *testing.T) {
	fx := newGoalHoldFixture(t)
	name := fx.finishedGoalReview(t)
	if err := fx.manager.reconcileTransitions(context.Background()); err != nil {
		t.Fatal(err)
	}
	fx.manager.Wait()
	if got := fx.sessions(); got != 1 {
		t.Fatalf("review -> planning started %d sessions, want 1", got)
	}
	if got := fx.planningAttempt(t, "planning", name); got != 2 {
		t.Fatalf("planning_attempt = %d, want 2", got)
	}
}
