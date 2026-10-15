package orchestrator

import (
	"context"
	"encoding/json"
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

type shadowFixture struct {
	t        *testing.T
	cfg      config.Config
	state    string
	snapshot string
	tasks    workflowRoute
	base     string
	mu       sync.Mutex
	lines    []string
}

func newShadowFixture(t *testing.T, mode string) *shadowFixture {
	t.Helper()
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(config.PathForRoot(root))
	cfg.Orchestrator.AdmissionMode = mode
	f := &shadowFixture{t: t, cfg: cfg, state: cfg.StatePath(), tasks: workflowRoutes()[0]}
	f.base = filepath.Dir(cfg.Resolve(f.tasks.Source))
	body := `
staff:
  implementer: {runner: pi, args: ["--model", "zai-coding-plan/glm-5.3"]}
  reviewer: {runner: pi, args: ["--model", "zai-coding-plan/glm-5.3"]}
roles:
  task_implementation: implementer
  task_review: reviewer
`
	if err := os.WriteFile(roster.Path(f.state), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	f.snapshot = filepath.Join(t.TempDir(), "snapshot.json")
	f.writeZai(10)
	return f
}

// writeZai writes a usage snapshot whose zai plan reads pct on its weekly window.
func (f *shadowFixture) writeZai(pct int) {
	f.t.Helper()
	now := time.Now().UTC()
	snap := map[string]any{"generated_at": now.Format(time.RFC3339), "plans": map[string]any{
		"zai": map[string]any{"ok": true, "as_of": now.Format(time.RFC3339), "weekly": map[string]any{"pct": pct, "resets_at": now.Add(2 * time.Hour).Format(time.RFC3339)}},
	}}
	data, _ := json.Marshal(snap)
	if err := os.WriteFile(f.snapshot, data, 0o600); err != nil {
		f.t.Fatal(err)
	}
}

func (f *shadowFixture) writeHold(seat string) {
	f.t.Helper()
	until := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	data, _ := json.Marshal(map[string]any{"updated_at": time.Now().UTC().Format(time.RFC3339), "staff": map[string]any{seat: map[string]string{"until": until, "reason": "zai-pace"}}})
	if err := os.MkdirAll(filepath.Join(f.state, "runtime"), 0o700); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.state, "runtime", "roster-holds.json"), data, 0o600); err != nil {
		f.t.Fatal(err)
	}
}

func (f *shadowFixture) manager() *Manager {
	m := New(f.cfg, newFakeRecipient(), extensions.Runner{Directory: filepath.Join(f.t.TempDir(), "missing")})
	m.Log = func(line string) {
		f.mu.Lock()
		f.lines = append(f.lines, line)
		f.mu.Unlock()
	}
	m.admissionSnapshotPath = f.snapshot
	f.t.Cleanup(m.Wait)
	return m
}

func (f *shadowFixture) shadowLines() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, line := range f.lines {
		if strings.HasPrefix(line, "admission-shadow:") {
			out = append(out, line)
		}
	}
	return out
}

func (f *shadowFixture) newTask(title string, extra map[string]any) string {
	f.t.Helper()
	path, err := Create(f.cfg, "tasks", title, "")
	if err != nil {
		f.t.Fatal(err)
	}
	document, err := ReadDocument(path)
	if err != nil {
		f.t.Fatal(err)
	}
	for k, v := range extra {
		document.FrontMatter[k] = v
	}
	if err := WriteDocument(path, document); err != nil {
		f.t.Fatal(err)
	}
	return filepath.Base(path)
}

func (f *shadowFixture) scanTodo(m *Manager) {
	f.t.Helper()
	if err := m.scanPhaseQueue(context.Background(), f.tasks, filepath.Join(f.base, "todo"), filepath.Join(f.base, "working"), phaseTaskImplementation); err != nil {
		f.t.Fatal(err)
	}
}

func claimed(f *shadowFixture, name string) bool {
	_, err := os.Stat(filepath.Join(f.base, "todo", name))
	return err != nil
}

// Off (the default) asks admission nothing and logs nothing.
func TestAdmissionShadowOffLogsNothing(t *testing.T) {
	f := newShadowFixture(t, "")
	f.writeZai(100)
	name := f.newTask("off", nil)
	m := f.manager()
	f.scanTodo(m)
	if !claimed(f, name) {
		t.Fatal("the task was not claimed")
	}
	m.Wait() // the dispatch goroutine is where a dispatch-site line would come from
	if lines := f.shadowLines(); len(lines) != 0 {
		t.Fatalf("admission_mode off logged %v", lines)
	}
	if m.shadow.decider != nil || len(m.shadow.sampled) != 0 {
		t.Fatal("admission_mode off built a decider or sampled a document")
	}
}

// Shadow logs the verdict beside the claim and never changes it: a plan reading full is a
// disagreement admission reports while the old path still claims.
func TestAdmissionShadowLogsDisagreementAtClaimAndDispatchWithoutChangingIt(t *testing.T) {
	f := newShadowFixture(t, "shadow")
	f.writeZai(100)
	name := f.newTask("full plan", nil)
	m := f.manager()
	f.scanTodo(m)
	if !claimed(f, name) {
		t.Fatal("shadow mode changed the claim: the task stayed queued")
	}
	m.Wait()
	joined := strings.Join(f.shadowLines(), "\n")
	for _, want := range []string{"DISAGREE", "site=queue", "old=run:implementer", "new=wait:implementer", "doc=" + name} {
		if !strings.Contains(joined, want) {
			t.Errorf("shadow log lacks %q:\n%s", want, joined)
		}
	}
}

// A held document the old path leaves queued is an agreement when admission also waits.
func TestAdmissionShadowAgreesWithAHeldSeat(t *testing.T) {
	f := newShadowFixture(t, "shadow")
	f.writeHold("implementer")
	name := f.newTask("held", nil)
	m := f.manager()
	f.scanTodo(m)
	if claimed(f, name) {
		t.Fatal("the held task was claimed")
	}
	m.shadowSummary(true)
	joined := strings.Join(f.shadowLines(), "\n")
	if strings.Contains(joined, "DISAGREE") {
		t.Fatalf("hold agreement logged as a disagreement:\n%s", joined)
	}
	if !strings.Contains(joined, "summary") || !strings.Contains(joined, "site=queue agree=1 disagree=0") {
		t.Fatalf("summary lacks the agreement count:\n%s", joined)
	}
}

// The dispatch site compares the seat the old path assigns with admission's verdict.
func TestAdmissionShadowDispatchSiteComparesAssignedSeat(t *testing.T) {
	f := newShadowFixture(t, "shadow")
	f.writeZai(100)
	name := f.newTask("dispatch", nil)
	m := f.manager()
	file := filepath.Join(f.base, "todo", name)
	if _, ok := m.resolveRosterModel(f.tasks, Lease{File: file, Phase: phaseTaskImplementation, SessionKey: "s1"}); !ok {
		t.Fatal("roster did not route the task")
	}
	joined := strings.Join(f.shadowLines(), "\n")
	for _, want := range []string{"DISAGREE", "site=dispatch", "old=run:implementer", "new=wait:implementer"} {
		if !strings.Contains(joined, want) {
			t.Errorf("dispatch shadow lacks %q:\n%s", want, joined)
		}
	}
}

// Admission is asked before Assign counts the session: the session that reaches a seat's daily cap is
// not a disagreement about the cap.
func TestAdmissionShadowAsksBeforeAssignCountsTheSession(t *testing.T) {
	f := newShadowFixture(t, "shadow")
	body := `
staff:
  implementer: {runner: pi, args: ["--model", "zai-coding-plan/glm-5.3"], daily_cap: 1, fallback: backup}
  backup: {runner: opencode, args: ["--model", "opencode-go/glm-5.3"]}
roles:
  task_implementation: implementer
`
	if err := os.WriteFile(roster.Path(f.state), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	name := f.newTask("last session under the cap", nil)
	m := f.manager()
	lease := Lease{File: filepath.Join(f.base, "todo", name), Phase: phaseTaskImplementation, SessionKey: "cap-1"}
	if _, ok := m.resolveRosterModel(f.tasks, lease); !ok {
		t.Fatal("roster did not route the task")
	}
	m.shadowSummary(true)
	joined := strings.Join(f.shadowLines(), "\n")
	if strings.Contains(joined, "DISAGREE") || !strings.Contains(joined, "site=dispatch agree=1 disagree=0") {
		t.Fatalf("the session that reaches the cap was compared after it was counted:\n%s", joined)
	}
}

// claude_only front matter reaches admission: a zai seat cannot take it.
func TestAdmissionShadowPassesClaudeOnly(t *testing.T) {
	f := newShadowFixture(t, "shadow")
	name := f.newTask("claude only", map[string]any{"claude_only": true})
	m := f.manager()
	f.scanTodo(m)
	if !claimed(f, name) {
		t.Fatal("the task was not claimed")
	}
	m.Wait()
	if joined := strings.Join(f.shadowLines(), "\n"); !strings.Contains(joined, "claude_only") {
		t.Fatalf("claude_only not reflected:\n%s", joined)
	}
}

// Repeated scans of the same disagreement log it once an hour, not every tick.
func TestAdmissionShadowDedupesDisagreements(t *testing.T) {
	f := newShadowFixture(t, "shadow")
	f.writeZai(100)
	name := f.newTask("full but queued", nil)
	document, err := ReadDocument(filepath.Join(f.base, "todo", name))
	if err != nil {
		t.Fatal(err)
	}
	m := f.manager()
	for i := 0; i < 5; i++ {
		m.shadowQueued("queue", f.tasks, phaseTaskImplementation, name, document, false)
	}
	if got := f.disagreeLines(); got != 1 {
		t.Fatalf("same disagreement logged %d times, want 1", got)
	}
}

func (f *shadowFixture) disagreeLines() int {
	n := 0
	for _, line := range f.shadowLines() {
		if strings.Contains(line, "DISAGREE") {
			n++
		}
	}
	return n
}

// Samples are taken at most once a minute per document, and a disagreement repeats in the log only
// after an hour; the counts in the summary are samples.
func TestAdmissionShadowSamplesOncePerMinuteAndLogsOncePerHour(t *testing.T) {
	f := newShadowFixture(t, "shadow")
	f.writeZai(100)
	name := f.newTask("full but queued", nil)
	document, err := ReadDocument(filepath.Join(f.base, "todo", name))
	if err != nil {
		t.Fatal(err)
	}
	m := f.manager()
	clock := time.Now()
	m.shadow.now = func() time.Time { return clock }
	for i := 0; i < 5; i++ { // five calls in the same instant: one sample
		m.shadowQueued("queue", f.tasks, phaseTaskImplementation, name, document, false)
	}
	m.shadowSummary(true)
	if joined := strings.Join(f.shadowLines(), "\n"); !strings.Contains(joined, "site=queue agree=0 disagree=1") {
		t.Fatalf("five calls in one instant were not one sample:\n%s", joined)
	}
	for i := 0; i < 5; i++ { // five more samples within the hour: still one DISAGREE line
		clock = clock.Add(2 * time.Minute)
		m.shadowQueued("queue", f.tasks, phaseTaskImplementation, name, document, false)
	}
	if got := f.disagreeLines(); got != 1 {
		t.Fatalf("DISAGREE logged %d times within the hour, want 1", got)
	}
	clock = clock.Add(time.Hour)
	m.shadowQueued("queue", f.tasks, phaseTaskImplementation, name, document, false)
	if got := f.disagreeLines(); got != 2 {
		t.Fatalf("DISAGREE logged %d times after the hour, want 2", got)
	}
}

// One pass that reaches a document through two sites (wake, then start) is one sample, and a sample
// where both sides run and pick the same seat is an agreement.
func TestAdmissionShadowCountsOnceAcrossSitesAndAgreesOnRun(t *testing.T) {
	f := newShadowFixture(t, "shadow")
	name := f.newTask("room", nil)
	document, err := ReadDocument(filepath.Join(f.base, "todo", name))
	if err != nil {
		t.Fatal(err)
	}
	m := f.manager()
	m.rosterHeld("wake", f.tasks, phaseTaskImplementation, name, document)
	m.rosterHeld("start", f.tasks, phaseTaskImplementation, name, document)
	m.shadowSummary(true)
	joined := strings.Join(f.shadowLines(), "\n")
	if !strings.Contains(joined, "site=wake agree=1 disagree=0") || strings.Contains(joined, "site=start") {
		t.Fatalf("wake then start was not one agreeing sample:\n%s", joined)
	}
}

// The comparison reads and writes nothing of the old path's: the document file and the caller's front
// matter map (including a resume_credit key claimAttempt would delete from a map it is given) are
// untouched, and no attempt is spent.
func TestAdmissionShadowLeavesTheDocumentAndItsFrontMatterAlone(t *testing.T) {
	f := newShadowFixture(t, "shadow")
	f.writeZai(100)
	name := f.newTask("untouched", map[string]any{"attempt": 1, "resume_credit": true})
	path := filepath.Join(f.base, "todo", name)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	document, err := ReadDocument(path)
	if err != nil {
		t.Fatal(err)
	}
	m := f.manager()
	m.shadowQueued("queue", f.tasks, phaseTaskImplementation, name, document, false)
	if v, ok := document.FrontMatter["resume_credit"].(bool); !ok || !v {
		t.Fatalf("the caller's front matter lost resume_credit: %v", document.FrontMatter)
	}
	if numberValue(document.FrontMatter["attempt"]) != 1 {
		t.Fatalf("attempt changed: %v", document.FrontMatter["attempt"])
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("the shadow comparison rewrote the document")
	}
	if len(f.shadowLines()) == 0 {
		t.Fatal("no comparison was made")
	}
}

// A recovery of running work is never compared: a continuation is not stopped by a quota decision.
func TestAdmissionShadowSkipsRecoveryDispatch(t *testing.T) {
	f := newShadowFixture(t, "shadow")
	f.writeZai(100)
	m := f.manager()
	if done := m.shadowDispatchPending(Lease{File: "x.md", State: "recovering"}, phaseTaskImplementation, "implementer", map[string]any{}); done != nil {
		t.Fatal("a recovering lease was compared")
	}
	if done := m.shadowDispatchPending(Lease{File: "x.md", State: "processing"}, phaseTaskImplementation, "implementer", map[string]any{}); done == nil {
		t.Fatal("a new claim was not compared")
	}
}

// claude_only written as a string reaches admission as the constraint.
func TestAdmissionShadowPassesClaudeOnlyString(t *testing.T) {
	f := newShadowFixture(t, "shadow")
	f.writeZai(10)
	name := f.newTask("claude only", map[string]any{"claude_only": "true"})
	document, err := ReadDocument(filepath.Join(f.base, "todo", name))
	if err != nil {
		t.Fatal(err)
	}
	m := f.manager()
	m.shadowQueued("queue", f.tasks, phaseTaskImplementation, name, document, false)
	if joined := strings.Join(f.shadowLines(), "\n"); !strings.Contains(joined, "claude_only") {
		t.Fatalf("claude_only: \"true\" not reflected:\n%s", joined)
	}
}
