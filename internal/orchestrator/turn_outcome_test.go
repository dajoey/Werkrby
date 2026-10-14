package orchestrator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/roster"
	"github.com/agent0ai/spynel/internal/workspace"
)

func TestTurnOutcomeValidation(t *testing.T) {
	future := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	cases := []struct {
		name, phase, body string
		status            string // want this folder; empty means invalid
		errHas            string
	}{
		{"review hand-off", phaseTaskImplementation, `{"outcome":"review","progress":"Implemented."}`, "review", ""},
		{"direct done", phaseTaskImplementation, `{"outcome":"done","progress":"Done.","front_matter":{"completion_summary":{"verdict":"completed","outcome":"o","evidence":"e","uncertainty":"u"}}}`, "done", ""},
		{"todo hand-off", phaseTaskImplementation, `{"outcome":"todo","progress":"Hand-off.","front_matter":{"staff":"toolsmith"}}`, "todo", ""},
		{"time park", phaseTaskImplementation, `{"outcome":"park","progress":"Waiting on the build.","front_matter":{"wake_at":"` + future + `","waiting_for":"build","wake_check":"test -f /x","wake_check_interval_s":600}}`, "waiting", ""},
		{"ask joey", phaseTaskImplementation, `{"outcome":"ask_joey","progress":"Needs his decision.","front_matter":{"joey_ask":{"kind":"choice","prompt":"Which?","options":["A","B"]},"waiting_for":"his choice"}}`, "waiting", ""},
		{"failed", phaseTaskImplementation, `{"outcome":"failed","progress":"Could not.","reason":"the host is gone"}`, "failed", ""},
		{"blocked is failed", phaseTaskImplementation, `{"outcome":"blocked","progress":"Blocked.","reason":"no access"}`, "failed", ""},
		{"cancelled", phaseTaskImplementation, `{"outcome":"cancelled","progress":"Stopped.","reason":"superseded"}`, "cancelled", ""},
		{"review accepted", phaseTaskReview, `{"outcome":"accepted","progress":"Verified.","front_matter":{"completion_summary":{"verdict":"accepted","outcome":"o","evidence":"e"}}}`, "done", ""},
		{"review rejected", phaseTaskReview, `{"outcome":"rejected","progress":"Finding: x."}`, "todo", ""},
		{"review park", phaseTaskReview, `{"outcome":"ask_joey","progress":"Needs his grade.","front_matter":{"joey_ask":{"prompt":"Grade?"}}}`, "waiting", ""},

		{"not json", phaseTaskImplementation, `outcome: done`, "", "JSON"},
		{"unknown outcome", phaseTaskImplementation, `{"outcome":"finished","progress":"x"}`, "", "outcome"},
		{"missing outcome", phaseTaskImplementation, `{"progress":"x"}`, "", "outcome"},
		{"missing progress", phaseTaskImplementation, `{"outcome":"review"}`, "", "progress"},
		{"park without wake_at", phaseTaskImplementation, `{"outcome":"park","progress":"x"}`, "", "wake_at"},
		{"park with bad wake_at", phaseTaskImplementation, `{"outcome":"park","progress":"x","front_matter":{"wake_at":"tomorrow"}}`, "", "wake_at"},
		{"park with numeric wake_at", phaseTaskImplementation, `{"outcome":"park","progress":"x","front_matter":{"wake_at":5}}`, "", "wake_at"},
		{"ask without joey_ask", phaseTaskImplementation, `{"outcome":"ask_joey","progress":"x","front_matter":{"waiting_for":"him"}}`, "", "joey_ask"},
		{"failed without reason", phaseTaskImplementation, `{"outcome":"failed","progress":"x"}`, "", "reason"},
		{"summary not a mapping", phaseTaskImplementation, `{"outcome":"done","progress":"x","front_matter":{"completion_summary":"done"}}`, "", "completion_summary"},
		{"reviewer cannot hand to review", phaseTaskReview, `{"outcome":"review","progress":"x"}`, "", "review"},
		{"reviewer cannot fail", phaseTaskReview, `{"outcome":"failed","progress":"x","reason":"r"}`, "", "failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := parseTurnOutcome([]byte(tc.body))
			var transition outcomeTransition
			if err == nil {
				transition, err = validateTurnOutcome(parsed, tc.phase)
			}
			if tc.status == "" {
				if err == nil {
					t.Fatalf("want an error containing %q, got status %q", tc.errHas, transition.Status)
				}
				if !strings.Contains(err.Error(), tc.errHas) {
					t.Fatalf("error %q does not name %q", err, tc.errHas)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if transition.Status != tc.status {
				t.Fatalf("status = %q, want %q", transition.Status, tc.status)
			}
		})
	}
}

func TestTurnOutcomeIgnoresOwnerManagedAndUnknownKeys(t *testing.T) {
	parsed, err := parseTurnOutcome([]byte(`{"outcome":"review","progress":"x","front_matter":{"status":"done","attempt":9,"updated_at":"2026-01-01T00:00:00Z","risk":"high","note":"n"},"drop":["wake_at","review_required"]}`))
	if err != nil {
		t.Fatal(err)
	}
	transition, err := validateTurnOutcome(parsed, phaseTaskImplementation)
	if err != nil {
		t.Fatal(err)
	}
	if len(transition.Set) != 1 || transition.Set["risk"] != "high" {
		t.Fatalf("set = %#v, want only risk", transition.Set)
	}
	if strings.Join(transition.Drop, ",") != "wake_at" {
		t.Fatalf("drop = %v, want only wake_at", transition.Drop)
	}
	if got := strings.Join(transition.Ignored, ","); got != "attempt,note,review_required,status,updated_at" {
		t.Fatalf("ignored = %q", got)
	}
}

func TestTurnOutcomeReadPrefersResultFileThenFencedBlock(t *testing.T) {
	dir := t.TempDir()
	resultPath := filepath.Join(dir, "result.json")
	fenced := "All done.\n\n```werkrby-outcome\n{\"outcome\":\"failed\",\"progress\":\"from reply\",\"reason\":\"r\"}\n```\n"

	if _, _, err := readTurnOutcome(resultPath, "no block here"); err == nil || !strings.Contains(err.Error(), "no outcome") {
		t.Fatalf("missing result: err = %v, want a no-outcome error", err)
	}
	got, source, err := readTurnOutcome(resultPath, fenced)
	if err != nil || got.Outcome != "failed" || got.Progress != "from reply" || source != "final reply" {
		t.Fatalf("fenced block = %+v %q %v", got, source, err)
	}
	if err := os.WriteFile(resultPath, []byte(`{"outcome":"review","progress":"from file"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	got, source, err = readTurnOutcome(resultPath, fenced)
	if err != nil || got.Outcome != "review" || got.Progress != "from file" || source != "result.json" {
		t.Fatalf("result file = %+v %q %v", got, source, err)
	}
}

// explicitFixture is a workspace whose roster routes task implementation and review to one headless
// seat, with or without explicit_outcome, and one queued task.
func explicitFixture(t *testing.T, explicit bool, noReview bool) (config.Config, string) {
	t.Helper()
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Orchestrator.TaskNotifications = config.TaskNotificationsOff
	seat := "pilot: {runner: claude, headless: true}"
	if explicit {
		seat = "pilot: {runner: claude, headless: true, explicit_outcome: true}"
	}
	body := "staff:\n  " + seat + "\nroles:\n  task_implementation: pilot\n  task_review: pilot\n"
	if err := os.WriteFile(roster.Path(cfg.StatePath()), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	task, err := CreateWithOptions(cfg, "tasks", "explicit outcome probe", "Probe the explicit outcome path.", CreateOptions{NoReview: noReview})
	if err != nil {
		t.Fatal(err)
	}
	return cfg, filepath.Base(task)
}

var resultPathPattern = regexp.MustCompile("`([^`]*/result\\.json)`")

// writeResultFromPrompt makes a fake worker that writes body to the result path its prompt names.
func writeResultFromPrompt(t *testing.T, fake *fakeHarness, body string) {
	fake.beforeEmit = func() {
		fake.mu.Lock()
		prompt := fake.prompts[len(fake.prompts)-1]
		fake.mu.Unlock()
		match := resultPathPattern.FindStringSubmatch(prompt)
		if match == nil {
			t.Errorf("prompt names no result.json path")
			return
		}
		if err := os.WriteFile(match[1], []byte(body), 0o600); err != nil {
			t.Errorf("write result: %v", err)
		}
	}
}

func statusFile(cfg config.Config, status, name string) string {
	return filepath.Join(filepath.Dir(cfg.Resolve(workflowRoutes()[0].Source)), status, name)
}

func TestExplicitOutcomeSeatOwnerAppliesTransition(t *testing.T) {
	cfg, name := explicitFixture(t, true, true)
	fake := newFakeRecipient()
	writeResultFromPrompt(t, fake, `{"outcome":"done","progress":"Changed the probe.\nVerified: the probe test passes.","front_matter":{"completion_summary":{"verdict":"completed","outcome":"Probe changed","evidence":"Probe test passes","uncertainty":"None known"},"risk":"medium","attempt":7}}`)
	manager := New(cfg, fake, extensions.Runner{Directory: filepath.Join(cfg.Root, "missing")})
	for i := 0; i < 3; i++ {
		if err := manager.ScanOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		manager.Wait()
	}
	done := statusFile(cfg, "done", name)
	document, err := ReadDocument(done)
	if err != nil {
		t.Fatalf("task did not reach done/: %v", err)
	}
	if document.FrontMatter["status"] != "done" || document.FrontMatter["risk"] != "medium" {
		t.Fatalf("front matter = %#v", document.FrontMatter)
	}
	if numberValue(document.FrontMatter["attempt"]) != 1 {
		t.Fatalf("attempt = %v, want 1 (the worker cannot set it)", document.FrontMatter["attempt"])
	}
	summary, _ := document.FrontMatter["completion_summary"].(map[string]any)
	if summary["completed_at"] != document.FrontMatter["updated_at"] {
		t.Fatalf("completed_at %v != updated_at %v: Werkrby stamps both", summary["completed_at"], document.FrontMatter["updated_at"])
	}
	if !strings.Contains(document.Body, "Changed the probe.") || !strings.Contains(document.Body, "Verified: the probe test passes.") {
		t.Fatalf("progress entry missing from body:\n%s", document.Body)
	}
	if !strings.Contains(document.Body, "ignored front matter keys this seat may not set: attempt") {
		t.Fatalf("ignored owner-managed key is not named in progress:\n%s", document.Body)
	}
	leases, _ := manager.loadLeases()
	if len(leases) != 0 {
		t.Fatalf("leases left behind: %#v", leases)
	}
	if entries, _ := os.ReadDir(cfg.StatePath("runtime", "turns")); len(entries) != 0 {
		t.Fatalf("run directories left behind: %d", len(entries))
	}
	fake.mu.Lock()
	prompt := fake.prompts[0]
	fake.mu.Unlock()
	if !strings.Contains(prompt, "Turn end on this seat") || strings.Contains(prompt, filepath.Join(filepath.Dir(cfg.Resolve(workflowRoutes()[0].Source)), "review")+"\n") {
		t.Fatalf("flagged seat prompt lacks the explicit outcome contract or still lists status folders:\n%s", prompt)
	}
}

func TestExplicitOutcomeMissingResultRequeuesToTodo(t *testing.T) {
	cfg, name := explicitFixture(t, true, true)
	fake := newFakeRecipient()
	manager := New(cfg, fake, extensions.Runner{Directory: filepath.Join(cfg.Root, "missing")})
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	todo := statusFile(cfg, "todo", name)
	document, err := ReadDocument(todo)
	if err != nil {
		t.Fatalf("a turn without an outcome did not return the task to todo/: %v", err)
	}
	if !strings.Contains(document.Body, "no outcome") {
		t.Fatalf("progress does not say why:\n%s", document.Body)
	}
	if document.FrontMatter[resumeCreditField] != nil {
		t.Fatalf("a missing outcome must spend the attempt, got credit %#v", document.FrontMatter[resumeCreditField])
	}
	leases, _ := manager.loadLeases()
	if len(leases) != 0 {
		t.Fatalf("lease left for a detector: %#v", leases)
	}
}

func TestExplicitOutcomeInvalidResultRequeuesToTodo(t *testing.T) {
	cfg, name := explicitFixture(t, true, true)
	fake := newFakeRecipient()
	writeResultFromPrompt(t, fake, `{"outcome":"park","progress":"Waiting."}`)
	manager := New(cfg, fake, extensions.Runner{Directory: filepath.Join(cfg.Root, "missing")})
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	document, err := ReadDocument(statusFile(cfg, "todo", name))
	if err != nil {
		t.Fatalf("an invalid outcome did not return the task to todo/: %v", err)
	}
	if !strings.Contains(document.Body, "wake_at") {
		t.Fatalf("progress does not name the invalid field:\n%s", document.Body)
	}
	if leases, _ := manager.loadLeases(); len(leases) != 0 {
		t.Fatalf("lease left for a detector: %#v", leases)
	}
}

func TestExplicitOutcomeFlagOffKeepsLegacyBehaviour(t *testing.T) {
	cfg, name := explicitFixture(t, false, true)
	fake := newFakeRecipient()
	manager := New(cfg, fake, extensions.Runner{Directory: filepath.Join(cfg.Root, "missing")})
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	working := filepath.Join(cfg.Resolve(workflowRoutes()[0].Working), name)
	if _, err := os.Stat(working); err != nil {
		t.Fatalf("unflagged seat: the task left working/ without a worker move: %v", err)
	}
	leases, _ := manager.loadLeases()
	if len(leases) != 1 || leases[0].State != "awaiting_transition" {
		t.Fatalf("unflagged seat lease = %#v, want one awaiting_transition (legacy)", leases)
	}
	fake.mu.Lock()
	prompt := fake.prompts[0]
	fake.mu.Unlock()
	legacy, err := manager.renderPrompt(workflowRoutes()[0], leases[0], workflowRoutes()[0].Prompt)
	if err != nil {
		t.Fatal(err)
	}
	if prompt != legacy || strings.Contains(prompt, "Turn end on this seat") {
		t.Fatalf("unflagged seat prompt changed")
	}
}

func TestExplicitOutcomeSeatWorkerMoveFallsBackToLegacyReconcile(t *testing.T) {
	cfg, name := explicitFixture(t, true, false)
	fake := newFakeRecipient()
	working := filepath.Join(cfg.Resolve(workflowRoutes()[0].Working), name)
	review := statusFile(cfg, "review", name)
	var moved sync.Once
	fake.beforeEmit = func() {
		moved.Do(func() {
			if err := moveDocument(working, review, "review", time.Now().UTC()); err != nil {
				t.Errorf("worker move: %v", err)
			}
		})
	}
	manager := New(cfg, fake, extensions.Runner{Directory: filepath.Join(cfg.Root, "missing")})
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if _, err := os.Stat(statusFile(cfg, "todo", name)); err == nil {
		t.Fatal("a worker move on a flagged seat was treated as a missing outcome")
	}
	if err := manager.reconcileTransitions(context.Background()); err != nil {
		t.Fatal(err)
	}
	document, err := ReadDocument(review)
	if err != nil {
		t.Fatalf("legacy reconcile did not keep the worker's move to review: %v", err)
	}
	if document.FrontMatter["implementation_thread"] == nil {
		t.Fatalf("legacy reconcile did not run on the worker's move: %#v", document.FrontMatter)
	}
	if leases, _ := manager.loadLeases(); len(leases) != 0 {
		t.Fatalf("lease left after the legacy reconcile: %#v", leases)
	}
}

func TestExplicitOutcomeTurnScope(t *testing.T) {
	cfg, _ := explicitFixture(t, true, true)
	manager := New(cfg, newFakeRecipient(), extensions.Runner{})
	tasks, goals := workflowRoutes()[0], workflowRoutes()[1]
	if manager.explicitOutcomeTurn(tasks, Lease{ID: "a", Phase: phaseTaskImplementation}, "@pilot") == nil {
		t.Fatal("task implementation on the flagged seat must use an explicit outcome")
	}
	if manager.explicitOutcomeTurn(tasks, Lease{ID: "a", Phase: phaseTaskReview}, "@pilot") == nil {
		t.Fatal("task review on the flagged seat must use an explicit outcome")
	}
	if manager.explicitOutcomeTurn(goals, Lease{ID: "a", Phase: phaseGoalPlanning}, "@pilot") != nil {
		t.Fatal("goal phases keep the file-move contract in this stage")
	}
	if manager.explicitOutcomeTurn(tasks, Lease{ID: "a", Phase: phaseTaskImplementation}, "claude") != nil {
		t.Fatal("a model that is not roster staff is never an explicit-outcome seat")
	}
	if manager.explicitOutcomeTurn(tasks, Lease{ID: "a", Phase: phaseTaskImplementation}, "@nobody") != nil {
		t.Fatal("unknown staff is never an explicit-outcome seat")
	}
}

func TestExplicitOutcomeSendErrorKeepsTheErrorPath(t *testing.T) {
	cfg, name := explicitFixture(t, true, true)
	fake := newFakeRecipient()
	fake.sendErrs = []error{errors.New("runner turn failed: claude -p ended without a result")}
	manager := New(cfg, fake, extensions.Runner{Directory: filepath.Join(cfg.Root, "missing")})
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if _, err := os.Stat(filepath.Join(cfg.Resolve(workflowRoutes()[0].Working), name)); err != nil {
		t.Fatalf("a failed run is not an outcome; the task must stay claimed for the error path: %v", err)
	}
	leases, _ := manager.loadLeases()
	if len(leases) != 1 || leases[0].State != "error" {
		t.Fatalf("lease = %#v, want one in the ordinary error state", leases)
	}
	if entries, _ := os.ReadDir(cfg.StatePath("runtime", "turns")); len(entries) != 0 {
		t.Fatalf("run directory left behind after a failed run: %d", len(entries))
	}
}
