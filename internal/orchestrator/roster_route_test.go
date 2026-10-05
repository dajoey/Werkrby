package orchestrator

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/roster"
)

func TestResolveTargetModelThroughRoster(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, config.StateDirectoryName)
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{Root: root, Harness: config.Harness{Name: "herdr", Model: "opencode", DeveloperModel: "opencode", ReviewerModel: "pi"}}
	manager := New(cfg, newFakeRecipient(), extensions.Runner{})
	task := func(name, frontMatter string) Lease {
		path := filepath.Join(root, name+".md")
		if err := os.WriteFile(path, []byte("---\nid: "+name+"\n"+frontMatter+"---\n# T\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return Lease{File: path, SessionKey: "orchestrator:tasks:x:" + name + ":1"}
	}
	resolve := func(routeName, phase string, lease Lease) string {
		lease.Phase = phase
		return manager.resolveTargetModel(workflowRoute{Name: routeName}, lease)
	}

	// No roster file: legacy routing is untouched.
	if got := resolve("tasks", phaseTaskReview, task("legacy", "")); got != "pi" {
		t.Fatalf("legacy review = %q", got)
	}

	body := `
staff:
  implementer: {runner: opencode}
  architect: {runner: agy}
  thinker: {runner: pi}
  principal: {runner: claude, args: ["--model", "opus"], daily_cap: 1, fallback: thinker}
roles:
  task_implementation: implementer
  task_review: thinker
  task_review_high_risk: principal
  goal_planning: principal
escalation: {after_attempt: 3, staff: principal}
`
	if err := os.WriteFile(roster.Path(state), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ name, route, phase, frontMatter, want string }{
		{"role default", "tasks", phaseTaskImplementation, "", "opencode"},
		{"task staff", "tasks", phaseTaskImplementation, "staff: architect\n", "agy"},
		{"staff beats agent", "tasks", phaseTaskImplementation, "staff: architect\nagent: kimi\n", "agy"},
		{"legacy agent pin", "tasks", phaseTaskImplementation, "agent: hermes\n", "hermes"},
		{"unknown staff falls to role", "tasks", phaseTaskImplementation, "staff: nobody\n", "opencode"},
		{"ordinary review", "tasks", phaseTaskReview, "", "pi"},
		{"high-risk review", "tasks", phaseTaskReview, "risk: high\n", "@principal"},
		{"cap reached falls back", "goals", phaseGoalPlanning, "", "pi"},
		{"unmapped role is legacy", "goals", phaseGoalReview, "", "pi"},
		{"escalation over cap falls back", "tasks", phaseTaskImplementation, "attempt: 4\n", "pi"},
	} {
		if got := resolve(test.route, test.phase, task(test.name[:4]+test.phase+test.want, test.frontMatter)); got != test.want {
			t.Errorf("%s: got %q, want %q", test.name, got, test.want)
		}
	}

	// A broken roster is ignored, never fatal.
	if err := os.WriteFile(roster.Path(state), []byte("staff: ["), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := resolve("tasks", phaseTaskReview, task("broken", "")); got != "pi" {
		t.Fatalf("broken roster review = %q", got)
	}
}

func TestResolveTargetModelExemptStaffStaysPut(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, config.StateDirectoryName)
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{Root: root, Harness: config.Harness{Name: "herdr", Model: "opencode", DeveloperModel: "opencode", ReviewerModel: "pi"}}
	manager := New(cfg, newFakeRecipient(), extensions.Runner{})
	task := func(name, frontMatter string) Lease {
		path := filepath.Join(root, name+".md")
		if err := os.WriteFile(path, []byte("---\nid: "+name+"\n"+frontMatter+"---\n# T\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return Lease{File: path, SessionKey: "orchestrator:tasks:x:" + name + ":1"}
	}
	resolve := func(lease Lease) string {
		lease.Phase = phaseTaskImplementation
		return manager.resolveTargetModel(workflowRoute{Name: "tasks"}, lease)
	}

	body := `
staff:
  implementer: {runner: opencode}
  architect: {runner: agy}
  principal: {runner: claude, args: ["--model", "opus"], daily_cap: 12, fallback: thinker}
  thinker: {runner: pi}
  media:
    runner: hermes
    fallback: thinker
    no_escalate: true
roles:
  task_implementation: implementer
  task_review: thinker
escalation:
  - {after_attempt: 1, staff: architect}
  - {after_attempt: 2, staff: principal}
`
	if err := os.WriteFile(roster.Path(state), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	// Pinned + exempt stays put at every rung, including past the top.
	for _, attempt := range []string{"1", "2", "3", "4", "9"} {
		if got := resolve(task("exempt"+attempt, "staff: media\nattempt: "+attempt+"\n")); got != "hermes" {
			t.Errorf("exempt media attempt %s = %q, want hermes", attempt, got)
		}
	}
	// Pinned + non-exempt is unchanged: skips the middle rung, reaches the top.
	if got := resolve(task("nonexempt2", "staff: implementer\nattempt: 2\n")); got != "opencode" {
		t.Errorf("pinned non-exempt attempt 2 = %q, want opencode", got)
	}
	if got := resolve(task("nonexempt3", "staff: implementer\nattempt: 3\n")); got != "@principal" {
		t.Errorf("pinned non-exempt attempt 3 = %q, want @principal", got)
	}
	// Unpinned is unchanged at every rung.
	for _, c := range []struct{ attempt, want string }{
		{"1", "opencode"}, {"2", "agy"}, {"3", "@principal"}, {"4", "@principal"},
	} {
		if got := resolve(task("unpinned"+c.attempt, "attempt: "+c.attempt+"\n")); got != c.want {
			t.Errorf("unpinned attempt %s = %q, want %q", c.attempt, got, c.want)
		}
	}
}

// A seat's own escalation (escalate_after / escalate_to) moves its tasks on a retry, whether the
// task is pinned to the seat or reaches it as the role default, and leaves every other seat alone.
func TestResolveTargetModelSeatEscalation(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, config.StateDirectoryName)
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{Root: root, Harness: config.Harness{Name: "herdr", Model: "opencode", DeveloperModel: "opencode", ReviewerModel: "pi"}}
	manager := New(cfg, newFakeRecipient(), extensions.Runner{})
	task := func(name, frontMatter string) Lease {
		path := filepath.Join(root, name+".md")
		if err := os.WriteFile(path, []byte("---\nid: "+name+"\n"+frontMatter+"---\n# T\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return Lease{File: path, SessionKey: "orchestrator:tasks:x:" + name + ":1"}
	}
	resolve := func(lease Lease) string {
		lease.Phase = phaseTaskImplementation
		return manager.resolveTargetModel(workflowRoute{Name: "tasks"}, lease)
	}
	body := `
staff:
  ops: {runner: agy, escalate_after: 1, escalate_to: architect}
  architect: {runner: pi, args: ["--model", "zai/glm-5.3"]}
  implementer: {runner: opencode}
  media: {runner: hermes, escalate_after: 2, escalate_to: architect}
roles:
  task_implementation: ops
  task_review: architect
`
	if err := os.WriteFile(roster.Path(state), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ name, frontMatter, want string }{
		{"pinned first attempt stays", "staff: ops\nattempt: 1\n", "agy"},
		{"pinned retry escalates", "staff: ops\nattempt: 2\n", "@architect"},
		{"pinned later retry stays escalated", "staff: ops\nattempt: 4\n", "@architect"},
		{"unpinned role default escalates too", "attempt: 2\n", "@architect"},
		{"unpinned first attempt stays", "attempt: 1\n", "agy"},
		{"a seat with no escalation never moves", "staff: implementer\nattempt: 5\n", "opencode"},
		{"escalation honours its own threshold", "staff: media\nattempt: 2\n", "hermes"},
		{"escalation honours its own threshold, later", "staff: media\nattempt: 3\n", "@architect"},
		{"the target itself does not escalate", "staff: architect\nattempt: 9\n", "@architect"},
	} {
		if got := resolve(task(c.name[:5]+"-"+c.want+"-"+c.frontMatter[len(c.frontMatter)-3:len(c.frontMatter)-1], c.frontMatter)); got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

// A review picks a reviewer from a different model family than the seat that wrote the work, when
// the review role lists alternates (role_alternates). Spynel remembers the implementing seat.
func TestResolveTargetModelCrossFamilyReview(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, config.StateDirectoryName)
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{Root: root, Harness: config.Harness{Name: "herdr", Model: "opencode", DeveloperModel: "opencode", ReviewerModel: "pi"}}
	manager := New(cfg, newFakeRecipient(), extensions.Runner{})
	task := func(name, frontMatter string) Lease {
		path := filepath.Join(root, name+".md")
		if err := os.WriteFile(path, []byte("---\nid: "+name+"\n"+frontMatter+"---\n# T\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return Lease{File: path, SessionKey: "orchestrator:tasks:task_review:" + name + ":1"}
	}
	review := func(lease Lease) string {
		lease.Phase = phaseTaskReview
		return manager.resolveTargetModel(workflowRoute{Name: "tasks"}, lease)
	}
	body := `
staff:
  ops: {runner: agy, args: ["--model", "gemini-3.8-flash-high"]}
  analyst: {runner: agy, args: ["--model", "gemini-3.8-flash-high"]}
  glmflash: {runner: pi, args: ["--model", "zai/glm-5.3-flash"]}
  glmmax: {runner: pi, args: ["--model", "zai/glm-5.3"]}
  thinker: {runner: pi, args: ["--model", "zai/glm-5.3"]}
roles:
  task_implementation: ops
  task_review: analyst
  task_review_medium: analyst
  task_review_high_risk: thinker
role_alternates:
  task_review_medium: [glmflash]
`
	if err := os.WriteFile(roster.Path(state), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	record := func(file, seat string) {
		if err := roster.RecordImplementer(state, file, seat, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []struct{ name, frontMatter, implementer, want string }{
		{"medium risk, Gemini wrote it: GLM reviews", "risk: medium\n", "ops", "@glmflash"},
		{"medium risk, GLM wrote it: Gemini reviews", "risk: medium\n", "glmmax", "@analyst"},
		{"medium risk, writer unknown: the main reviewer", "risk: medium\n", "", "@analyst"},
		{"ordinary review has no alternates and is untouched", "", "ops", "@analyst"},
		{"high risk is untouched", "risk: high\n", "ops", "@thinker"},
	} {
		lease := task(strings.ReplaceAll(c.name[:8]+c.implementer+c.want, " ", "-"), c.frontMatter)
		if c.implementer != "" {
			record(filepath.Base(lease.File), c.implementer)
		}
		if got := review(lease); got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}

	// The implementation dispatch is what records the seat, so a review that follows it finds it.
	impl := task("recorded", "")
	impl.Phase = phaseTaskImplementation
	manager.resolveTargetModel(workflowRoute{Name: "tasks"}, impl)
	if seat, ok := roster.ImplementedBy(state, filepath.Base(impl.File)); !ok || seat != "ops" {
		t.Errorf("implementation dispatch recorded %q %v, want ops", seat, ok)
	}
}
