package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/roster"
	"github.com/agent0ai/spynel/internal/workspace"
)

// pumpWorkCard is the front matter the Helm pump writes for a work Claude thread when the bridge
// lane is off (helm-werkrby-pump.py build_task_markdown): agent claude, the Claude seat `second`,
// claude_only, and the Helm thread it came from.
func pumpWorkCard(id string) string {
	return `---
title: "Helm: work Claude request"
status: todo
attempt: 0
agent: claude
staff: second
claude_only: true
review_required: false
notify:
  enabled: false
id: "` + id + `"
created_at: "2026-10-24T20:00:00Z"
updated_at: "2026-10-24T20:00:00Z"
trace_id: "tr-thr-work-1-1761336000"
helm_thread_id: "thr-work-1"
helm_claim_id: "claim-work-1"
helm_reply_ts: 1761336000
---
# Helm: work Claude request

## Objective
Carry out the Helm instruction for thread ` + "`thr-work-1`" + `.
`
}

// hermesOnlyRoster is the live shape: Hermes seats only, with no Claude seat and no second seat.
const hermesOnlyRoster = `
staff:
  implementer: {runner: hermes, args: ["-p", "wk-code", "--model", "zai/glm-5.3-flash"]}
  reviewer: {runner: hermes, args: ["-p", "wk-code", "--model", "zai/glm-5.3"]}
roles:
  task_implementation: implementer
  task_review: reviewer
`

type fenceWorkspace struct {
	cfg     config.Config
	manager *Manager
	fake    *fakeHarness
	logs    func() []string
}

func newFenceWorkspace(t *testing.T, rosterBody string) fenceWorkspace {
	t.Helper()
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Harness.Model, cfg.Harness.DeveloperModel, cfg.Harness.ReviewerModel = "hermes", "hermes", "hermes"
	if rosterBody != "" {
		if err := os.WriteFile(roster.Path(cfg.StatePath()), []byte(rosterBody), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	fake := newFakeRecipient()
	manager := New(cfg, fake, extensions.Runner{})
	var mu sync.Mutex
	var logs []string
	manager.Log = func(s string) { mu.Lock(); logs = append(logs, s); mu.Unlock() }
	return fenceWorkspace{cfg: cfg, manager: manager, fake: fake, logs: func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), logs...)
	}}
}

func (w fenceWorkspace) write(t *testing.T, status, id string) string {
	t.Helper()
	path := w.cfg.StatePath("tasks", status, id+".md")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(pumpWorkCard(id)), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func (w fenceWorkspace) holdLines() int {
	n := 0
	for _, line := range w.logs() {
		if strings.Contains(line, "claude_only") && strings.Contains(line, "waits unclaimed") {
			n++
		}
	}
	return n
}

// A work Claude card (claude_only) never runs on a non-Claude seat. With no Claude seat it stays in
// todo/, unclaimed and never sent, and the hold is logged once an hour, not once a scan; this holds
// on the roster path (the live Hermes-only roster) and the legacy path (no roster).
func TestWorkCardWithNoClaudeSeatStaysUnclaimed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		roster string
	}{
		{name: "roster path", roster: hermesOnlyRoster},
		{name: "legacy path", roster: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newFenceWorkspace(t, tc.roster)
			queued := w.write(t, "todo", "helm-work-1")

			lease := Lease{File: queued, Phase: phaseTaskImplementation, SessionKey: "orchestrator:tasks:task_implementation:helm-work-1:1", WorkCard: true}
			if got := w.manager.resolveTargetModel(workflowRoute{Name: "tasks"}, lease); got != "" {
				t.Fatalf("resolveTargetModel = %q, want \"\": a claude_only document has no seat here and must never get a non-Claude one", got)
			}
			ctx := context.Background()
			for i := 0; i < 3; i++ {
				if err := w.manager.ScanOnce(ctx); err != nil {
					t.Fatal(err)
				}
				w.manager.Wait()
			}
			if w.fake.calls != 0 {
				t.Fatalf("harness sends = %d (%q), want none: a work Claude card reached a non-Claude seat", w.fake.calls, w.fake.keys)
			}
			if !fileExists(queued) {
				t.Fatalf("the work card left todo/ with no Claude seat to take it")
			}
			if leases, _ := w.manager.loadLeases(); len(leases) != 0 {
				t.Fatalf("leases = %#v, want none", leases)
			}
			if n := w.holdLines(); n != 1 {
				t.Fatalf("hold log lines over three scans = %d (%q), want one", n, w.logs())
			}
		})
	}
}

// A work Claude card already claimed (from before this rule, or adopted as an orphan) is never sent
// to a non-Claude seat either: its recovery dispatch returns it to todo/ with no attempt spent.
func TestClaimedWorkCardWithNoClaudeSeatIsReturnedUnsent(t *testing.T) {
	w := newFenceWorkspace(t, hermesOnlyRoster)
	working := w.write(t, "working", "helm-work-2")
	document, err := ReadDocument(working)
	if err != nil {
		t.Fatal(err)
	}
	document.FrontMatter["status"] = "working"
	document.FrontMatter["attempt"] = 1
	if err := WriteDocument(working, document); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	id := leaseID("tasks:"+phaseTaskImplementation, "helm-work-2")
	lease := Lease{
		ID: id, ClaimID: id, DocumentType: "task", Route: "tasks", OwnerID: w.manager.ownerID,
		File: working, SessionKey: "orchestrator:tasks:task_implementation:helm-work-2:1", State: "error",
		Phase: phaseTaskImplementation, ClaimAttempt: 1, StartedAt: now, HeartbeatAt: now.Add(-time.Minute), WorkCard: true,
	}
	if err := w.manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}

	if err := w.manager.recoverStale(context.Background()); err != nil {
		t.Fatal(err)
	}
	w.manager.Wait()

	if w.fake.calls != 0 {
		t.Fatalf("harness sends = %d, want none: a claimed work Claude card reached a non-Claude seat", w.fake.calls)
	}
	todo := w.cfg.StatePath("tasks", "todo", "helm-work-2.md")
	if fileExists(working) || !fileExists(todo) {
		t.Fatalf("the claimed work card was not returned to todo/")
	}
	if leases, _ := w.manager.loadLeases(); len(leases) != 0 {
		t.Fatalf("leases = %#v, want the claim dropped", leases)
	}
	returned, err := ReadDocument(todo)
	if err != nil {
		t.Fatal(err)
	}
	if credit, _ := returned.FrontMatter[resumeCreditField].(bool); !credit {
		t.Fatalf("returned work card carries no resume credit, so its next claim would spend an attempt: %v", returned.FrontMatter)
	}
}

// A roster that does have a Claude seat runs the work card there: on the pinned seat when the
// roster defines it, else on the roster's Claude seat; never on the role's Hermes seat.
func TestWorkCardRunsOnlyOnAClaudeSeat(t *testing.T) {
	for _, tc := range []struct {
		name   string
		extra  string
		want   string
		review bool
	}{
		{name: "pinned Claude seat", extra: "  second: {runner: claude, headless: true}\n", want: "@second"},
		{name: "another Claude seat", extra: "  sonnet: {runner: claude, headless: true}\n", want: "@sonnet"},
		{name: "review on a Claude seat", extra: "  sonnet: {runner: claude, headless: true}\n", want: "@sonnet", review: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := strings.Replace(hermesOnlyRoster, "roles:\n", tc.extra+"roles:\n", 1)
			w := newFenceWorkspace(t, body)
			status, phase := "working", phaseTaskImplementation
			if tc.review {
				status, phase = "reviewing", phaseTaskReview
			}
			path := w.write(t, status, "helm-work-3")
			lease := Lease{File: path, Phase: phase, SessionKey: "orchestrator:tasks:" + phase + ":helm-work-3:1", WorkCard: true}
			if got := w.manager.resolveTargetModel(workflowRoute{Name: "tasks"}, lease); got != tc.want {
				t.Fatalf("resolveTargetModel = %q, want %q", got, tc.want)
			}
		})
	}
}
