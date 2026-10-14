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

	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/extensions"
)

// scriptedHarness is a fake worker whose script sees each prompt and returns the outcome JSON to
// write to the result path the prompt names ("" writes nothing) and an optional send error.
type scriptedHarness struct {
	*fakeHarness
	script func(key, prompt string) (string, error)
}

func newScripted(script func(key, prompt string) (string, error)) *scriptedHarness {
	return &scriptedHarness{fakeHarness: newFakeRecipient(), script: script}
}

func (s *scriptedHarness) Send(_ context.Context, key, prompt string, emit core.Emit) (string, bool, error) {
	s.mu.Lock()
	s.calls++
	s.keys = append(s.keys, key)
	s.prompts = append(s.prompts, prompt)
	s.mu.Unlock()
	thread := "thread-" + key
	var body string
	var sendErr error
	if s.script != nil {
		body, sendErr = s.script(key, prompt)
	}
	if body != "" {
		if match := resultPathPattern.FindStringSubmatch(prompt); match != nil {
			_ = os.WriteFile(match[1], []byte(body), 0o600)
		}
	}
	if sendErr != nil {
		emit(core.Event{Kind: core.EventError, Text: sendErr.Error(), ThreadID: thread, Done: true})
		return thread, false, sendErr
	}
	emit(core.Event{Kind: core.EventFinal, Text: "done", ThreadID: thread, Done: true})
	return thread, false, nil
}

var claimedTaskPattern = regexp.MustCompile("`([^`]*/tasks/(?:working|reviewing)/[^`]*\\.md)`")

func isReviewPrompt(prompt string) bool { return strings.Contains(prompt, "independent task reviewer") }

func claimedTask(prompt string) string {
	if match := claimedTaskPattern.FindStringSubmatch(prompt); match != nil {
		return match[1]
	}
	return ""
}

func editTask(t *testing.T, path string, edit func(map[string]any)) {
	t.Helper()
	document, err := ReadDocument(path)
	if err != nil {
		t.Fatal(err)
	}
	edit(document.FrontMatter)
	if err := WriteDocument(path, document); err != nil {
		t.Fatal(err)
	}
}

func mustRead(t *testing.T, path string) Document {
	t.Helper()
	document, err := ReadDocument(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return document
}

type logSink struct {
	mu    sync.Mutex
	lines []string
}

func (l *logSink) log(line string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, line)
}

func (l *logSink) has(fragment string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.lines {
		if strings.Contains(line, fragment) {
			return true
		}
	}
	return false
}

// Finding 1: a reviewer's missing or invalid outcome is not an implementation failure. The first
// miss in an attempt goes back to review for a fresh review; a second one spends the attempt.
func TestExplicitOutcomeReviewMissReturnsToReviewOncePerAttempt(t *testing.T) {
	cfg, name := explicitFixture(t, true, false)
	harness := newScripted(func(_, prompt string) (string, error) {
		if isReviewPrompt(prompt) {
			return "", nil
		}
		return `{"outcome":"review","progress":"Implemented."}`, nil
	})
	manager := New(cfg, harness, extensions.Runner{Directory: filepath.Join(cfg.Root, "missing")})
	scanAndWait(t, manager) // implementation -> review/
	scanAndWait(t, manager) // review claimed; the reviewer returns nothing
	document := mustRead(t, statusFile(cfg, "review", name))
	if numberValue(document.FrontMatter["attempt"]) != 1 || document.FrontMatter[resumeCreditField] != nil {
		t.Fatalf("first review miss charged the implementation: %#v", document.FrontMatter)
	}
	if !strings.Contains(document.Body, "fresh review") {
		t.Fatalf("progress does not say the task went back for a fresh review:\n%s", document.Body)
	}
	if leases, _ := manager.loadLeases(); len(leases) != 0 {
		t.Fatalf("lease left for a detector: %#v", leases)
	}
	scanAndWait(t, manager) // second review, second miss in the same attempt
	document = mustRead(t, statusFile(cfg, "todo", name))
	if document.FrontMatter[resumeCreditField] != nil {
		t.Fatalf("a second review miss in one attempt must spend it: %#v", document.FrontMatter)
	}
}

// Findings 1 and 5: an implementation turn that returns no outcome gets one free retry per attempt,
// as today's awaiting_transition recovery gives it; the second miss spends the attempt.
func TestExplicitOutcomeImplementationMissCreditsOncePerAttempt(t *testing.T) {
	cfg, name := explicitFixture(t, true, true)
	harness := newScripted(nil)
	manager := New(cfg, harness, extensions.Runner{Directory: filepath.Join(cfg.Root, "missing")})
	scanAndWait(t, manager)
	document := mustRead(t, statusFile(cfg, "todo", name))
	if document.FrontMatter[resumeCreditField] != true || !strings.Contains(document.Body, "no outcome") {
		t.Fatalf("first miss = %#v\n%s\nwant todo with the attempt not spent", document.FrontMatter, document.Body)
	}
	scanAndWait(t, manager)
	document = mustRead(t, statusFile(cfg, "todo", name))
	if numberValue(document.FrontMatter["attempt"]) != 1 {
		t.Fatalf("attempt = %v, want 1: the credited retry continues the attempt", document.FrontMatter["attempt"])
	}
	if document.FrontMatter[resumeCreditField] != nil || !strings.Contains(document.Body, "attempt was spent") {
		t.Fatalf("second miss = %#v\n%s\nwant the attempt spent", document.FrontMatter, document.Body)
	}
}

// Finding 5: a park without a routable wait is forgiven today (todo, attempt not spent).
func TestExplicitOutcomeParkWithoutWakeAtIsNotCharged(t *testing.T) {
	for name, body := range map[string]string{
		"missing wake_at": `{"outcome":"park","progress":"Waiting."}`,
		"past wake_at":    `{"outcome":"park","progress":"Waiting.","front_matter":{"wake_at":"2020-01-01T00:00:00Z","waiting_for":"x"}}`,
		"missing ask":     `{"outcome":"ask_joey","progress":"Asking."}`,
	} {
		t.Run(name, func(t *testing.T) {
			cfg, task := explicitFixture(t, true, true)
			harness := newScripted(func(string, string) (string, error) { return body, nil })
			manager := New(cfg, harness, extensions.Runner{Directory: filepath.Join(cfg.Root, "missing")})
			scanAndWait(t, manager)
			document := mustRead(t, statusFile(cfg, "todo", task))
			if document.FrontMatter[resumeCreditField] != true || !strings.Contains(document.Body, "Park rejected") {
				t.Fatalf("park rejection = %#v\n%s\nwant todo without spending the attempt", document.FrontMatter, document.Body)
			}
		})
	}
}

// Finding 5: a completion summary that only has a format problem gets the one repair per attempt
// today's reconcile gives; a code-managed numeric rework_count is not a problem at all.
func TestExplicitOutcomeSummaryFormatGetsOneRepairPerAttempt(t *testing.T) {
	cfg, name := explicitFixture(t, true, true)
	harness := newScripted(func(string, string) (string, error) {
		return `{"outcome":"done","progress":"Done.","front_matter":{"completion_summary":{"verdict":"completed","outcome":"o","evidence":5,"uncertainty":"u"}}}`, nil
	})
	manager := New(cfg, harness, extensions.Runner{Directory: filepath.Join(cfg.Root, "missing")})
	scanAndWait(t, manager)
	document := mustRead(t, statusFile(cfg, "todo", name))
	if document.FrontMatter[resumeCreditField] != true || numberValue(document.FrontMatter[formatCreditAttemptField]) != 1 {
		t.Fatalf("format-only rejection = %#v, want a repair that continues attempt 1", document.FrontMatter)
	}

	cfg, name = explicitFixture(t, true, true)
	harness = newScripted(func(string, string) (string, error) {
		return `{"outcome":"done","progress":"Done.","front_matter":{"completion_summary":{"verdict":"completed","outcome":"o","evidence":"e","uncertainty":"u","rework_count":0}}}`, nil
	})
	manager = New(cfg, harness, extensions.Runner{Directory: filepath.Join(cfg.Root, "missing")})
	scanAndWait(t, manager)
	scanAndWait(t, manager)
	if _, err := os.Stat(statusFile(cfg, "done", name)); err != nil {
		t.Fatalf("a numeric rework_count blocked a valid completion: %v", err)
	}
}

// Finding 2: a valid result.json written before the send failed (a quota or overload error on
// the final reply) is the turn's outcome.
func TestExplicitOutcomeSendErrorAppliesWrittenOutcome(t *testing.T) {
	cfg, name := explicitFixture(t, true, false)
	harness := newScripted(func(string, string) (string, error) {
		return `{"outcome":"review","progress":"Implemented before the API refused the final reply."}`, errors.New("runner turn failed: Credit balance is too low")
	})
	manager := New(cfg, harness, extensions.Runner{Directory: filepath.Join(cfg.Root, "missing")})
	scanAndWait(t, manager)
	if _, err := os.Stat(statusFile(cfg, "waiting", name)); err == nil {
		t.Fatal("the written outcome was discarded and the task parked on the quota wall")
	}
	document := mustRead(t, statusFile(cfg, "review", name))
	if !strings.Contains(document.Body, "Implemented before the API refused the final reply.") {
		t.Fatalf("progress missing:\n%s", document.Body)
	}
}

// Finding 2: an operator stop still wins over an outcome the worker wrote.
func TestExplicitOutcomeOperatorStopWinsOverWrittenOutcome(t *testing.T) {
	cfg, name := explicitFixture(t, true, false)
	var manager *Manager
	harness := newScripted(func(key, _ string) (string, error) {
		manager.MarkControlCancellation(key)
		return `{"outcome":"review","progress":"Implemented."}`, context.Canceled
	})
	manager = New(cfg, harness, extensions.Runner{Directory: filepath.Join(cfg.Root, "missing")})
	scanAndWait(t, manager)
	document := mustRead(t, statusFile(cfg, "waiting", name))
	if document.FrontMatter["stopped_by"] != "operator" {
		t.Fatalf("operator stop lost to the written outcome: %#v", document.FrontMatter)
	}
}

// Finding 3: the werkrby-outcome block in the final reply is parsed as one JSON value after an exact
// opener at the start of a line, and it must be the last thing in the reply.
func TestOutcomeFenceRules(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "result.json")
	block := func(json string) string { return "```werkrby-outcome\n" + json + "\n```" }
	cases := []struct {
		name, reply, wantProgress string
	}{
		{"backticks inside a JSON string", "Done.\n\n" + block(`{"outcome":"review","progress":"ran `+"```go test```"+` ok"}`) + "\n", "ran ```go test``` ok"},
		{"quoted example before the real block", "Example:\n" + block(`{"outcome":"failed","progress":"quoted","reason":"r"}`) + "\nReal:\n" + block(`{"outcome":"review","progress":"real"}`), "real"},
		{"prose after the block", block(`{"outcome":"review","progress":"x"}`) + "\nThanks.", ""},
		{"block quoted inside an outer fence", "````markdown\n" + block(`{"outcome":"review","progress":"x"}`) + "\n````", ""},
		{"opener with a longer info string", "```werkrby-outcomes\n{\"outcome\":\"review\",\"progress\":\"x\"}\n```", ""},
		{"opener in the middle of a line", "see ```werkrby-outcome\n{\"outcome\":\"review\",\"progress\":\"x\"}\n```", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _, err := readTurnOutcome(missing, tc.reply)
			if tc.wantProgress == "" {
				if err == nil {
					t.Fatalf("accepted %+v from a reply that does not end with a werkrby-outcome block", got)
				}
				return
			}
			if err != nil || got.Progress != tc.wantProgress {
				t.Fatalf("got %+v, %v; want progress %q", got, err, tc.wantProgress)
			}
		})
	}
}

// Finding 4: a review verdict must match its outcome.
func TestReviewOutcomeNeedsAMatchingVerdict(t *testing.T) {
	for name, body := range map[string]string{
		"accepted without a summary":  `{"outcome":"accepted","progress":"x"}`,
		"accepted with a rejection":   `{"outcome":"accepted","progress":"x","front_matter":{"completion_summary":{"verdict":"rejected","outcome":"o","evidence":"e"}}}`,
		"rejected with an acceptance": `{"outcome":"rejected","progress":"x","front_matter":{"completion_summary":{"verdict":"accepted","outcome":"o","evidence":"e"}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			parsed, err := parseTurnOutcome([]byte(body))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := validateTurnOutcome(parsed, phaseTaskReview); err == nil || !strings.Contains(err.Error(), "verdict") {
				t.Fatalf("err = %v, want a verdict mismatch", err)
			}
		})
	}
}

// runOneImplementationTurn runs one implementation turn on the pilot seat and returns the document
// and the folder it ended in.
func runOneImplementationTurn(t *testing.T, staff string, prepare func(path string), result string) (Document, string) {
	t.Helper()
	cfg, name := explicitFixtureRoster(t, staff, true)
	if prepare != nil {
		prepare(statusFile(cfg, "todo", name))
	}
	harness := newScripted(func(string, string) (string, error) { return result, nil })
	manager := New(cfg, harness, extensions.Runner{Directory: filepath.Join(cfg.Root, "missing")})
	scanAndWait(t, manager)
	for _, folder := range []string{"todo", "review", "done", "waiting", "failed", "cancelled", "working"} {
		if document, err := ReadDocument(statusFile(cfg, folder, name)); err == nil {
			return document, folder
		}
	}
	t.Fatal("task document not found in any folder")
	return Document{}, ""
}

const pilotSeat = "  pilot: {runner: claude, headless: true, explicit_outcome: true}\n"

// Finding 4: risk only goes up through an outcome.
func TestExplicitOutcomeRiskOnlyGoesUp(t *testing.T) {
	document, _ := runOneImplementationTurn(t, pilotSeat, func(path string) {
		editTask(t, path, func(fm map[string]any) { fm["risk"] = "high" })
	}, `{"outcome":"review","progress":"Done.","front_matter":{"risk":"medium"}}`)
	if document.FrontMatter["risk"] != "high" {
		t.Fatalf("risk = %v, want high kept", document.FrontMatter["risk"])
	}
	if !strings.Contains(document.Body, "risk") {
		t.Fatalf("the refused lowering is not named in progress:\n%s", document.Body)
	}
}

// Finding 4: staff changes only on a todo hand-off, to a known seat, and never into or out of a
// no_escalate seat.
func TestExplicitOutcomeStaffOnlyOnTodoHandOff(t *testing.T) {
	staff := pilotSeat + "  toolsmith: {runner: claude}\n  private: {runner: claude, no_escalate: true}\n"
	document, folder := runOneImplementationTurn(t, staff, nil, `{"outcome":"todo","progress":"Tool work.","front_matter":{"staff":"toolsmith"}}`)
	if folder != "todo" || document.FrontMatter["staff"] != "toolsmith" {
		t.Fatalf("todo hand-off = %s %#v, want staff toolsmith", folder, document.FrontMatter["staff"])
	}
	document, _ = runOneImplementationTurn(t, staff, nil, `{"outcome":"review","progress":"Done.","front_matter":{"staff":"toolsmith"}}`)
	if document.FrontMatter["staff"] != nil {
		t.Fatalf("staff changed on a review hand-off: %#v", document.FrontMatter["staff"])
	}
	document, _ = runOneImplementationTurn(t, staff, nil, `{"outcome":"todo","progress":"x","front_matter":{"staff":"private"}}`)
	if document.FrontMatter["staff"] != nil {
		t.Fatalf("staff moved into a no_escalate seat: %#v", document.FrontMatter["staff"])
	}
	document, _ = runOneImplementationTurn(t, staff, nil, `{"outcome":"todo","progress":"x","front_matter":{"staff":"nobody"}}`)
	if document.FrontMatter["staff"] != nil {
		t.Fatalf("staff set to an unknown seat: %#v", document.FrontMatter["staff"])
	}
	pinnedPrivate := "  pilot: {runner: claude, headless: true, explicit_outcome: true, no_escalate: true}\n  toolsmith: {runner: claude}\n"
	document, _ = runOneImplementationTurn(t, pinnedPrivate, func(path string) {
		editTask(t, path, func(fm map[string]any) { fm["staff"] = "pilot" })
	}, `{"outcome":"todo","progress":"x","front_matter":{"staff":"toolsmith"}}`)
	if document.FrontMatter["staff"] != "pilot" {
		t.Fatalf("a task pinned to a no_escalate seat was moved off it: %#v", document.FrontMatter["staff"])
	}
}

// Finding 7: only a hex lease id builds a run directory path.
func TestExplicitOutcomeTurnRefusesUnsafeLeaseIDs(t *testing.T) {
	cfg, _ := explicitFixture(t, true, true)
	manager := New(cfg, newFakeRecipient(), extensions.Runner{})
	tasks := workflowRoutes()[0]
	for _, id := range []string{".", "..", "../..", "a/b", "ABCDEF", "abc-def", strings.Repeat("a", 65)} {
		if turn := manager.explicitOutcomeTurn(tasks, Lease{ID: id, Phase: phaseTaskImplementation}, "@pilot"); turn != nil {
			t.Fatalf("lease id %q built run directory %s", id, turn.Dir)
		}
	}
	if manager.explicitOutcomeTurn(tasks, Lease{ID: "0123456789abcdef01234567", Phase: phaseTaskImplementation}, "@pilot") == nil {
		t.Fatal("a real lease id must be accepted")
	}
}

// Finding 8: a worker that ignores the contract and edits Werkrby-owned keys in place does not get
// them through the move.
func TestExplicitOutcomeRestoresOwnedKeysEditedInPlace(t *testing.T) {
	cfg, name := explicitFixture(t, true, false)
	logs := &logSink{}
	harness := newScripted(func(_, prompt string) (string, error) {
		editTask(t, claimedTask(prompt), func(fm map[string]any) {
			fm["attempt"] = 9
			fm["review_required"] = false
			fm["risk"] = "low"
		})
		return `{"outcome":"review","progress":"Implemented."}`, nil
	})
	manager := New(cfg, harness, extensions.Runner{Directory: filepath.Join(cfg.Root, "missing")})
	manager.Log = logs.log
	scanAndWait(t, manager)
	document := mustRead(t, statusFile(cfg, "review", name))
	if numberValue(document.FrontMatter["attempt"]) != 1 || document.FrontMatter["review_required"] != true || document.FrontMatter["risk"] != nil {
		t.Fatalf("in-place edits survived the move: %#v", document.FrontMatter)
	}
	if !logs.has("changed") || !logs.has("attempt") {
		t.Fatalf("no log line names the in-place change: %v", logs.lines)
	}
}

// Nit: an outcome that cannot be applied (the worker broke the document) is requeued explicitly.
func TestExplicitOutcomeApplyFailureRequeuesExplicitly(t *testing.T) {
	cfg, name := explicitFixture(t, true, true)
	harness := newScripted(func(_, prompt string) (string, error) {
		if err := os.WriteFile(claimedTask(prompt), []byte("---\nid: [broken\n---\nbody\n"), 0o600); err != nil {
			t.Errorf("break document: %v", err)
		}
		return `{"outcome":"review","progress":"Implemented."}`, nil
	})
	manager := New(cfg, harness, extensions.Runner{Directory: filepath.Join(cfg.Root, "missing")})
	scanAndWait(t, manager)
	if _, err := os.Stat(statusFile(cfg, "todo", name)); err != nil {
		t.Fatalf("an unappliable outcome was not requeued: %v", err)
	}
	if leases, _ := manager.loadLeases(); len(leases) != 0 {
		t.Fatalf("lease left in %q for a detector", leases[0].State)
	}
}

// Nit: the turn-end contract spells out the joey_ask shape the Helm pump reads.
func TestExplicitOutcomeContractDescribesJoeyAsk(t *testing.T) {
	turn := &explicitTurn{Phase: phaseTaskImplementation, ResultPath: "/r/result.json"}
	contract := turn.contract("/t/task.md")
	for _, want := range []string{"`prompt`", "`options`", "`label`", "`effect`", "`context`", "NEEDS JOEY BECAUSE"} {
		if !strings.Contains(contract, want) {
			t.Fatalf("contract does not describe %s:\n%s", want, contract)
		}
	}
}

// Nit: a park must wake in the future.
func TestTurnOutcomeParkNeedsAFutureWakeAt(t *testing.T) {
	past := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
	parsed, err := parseTurnOutcome([]byte(`{"outcome":"park","progress":"x","front_matter":{"wake_at":"` + past + `"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := validateTurnOutcome(parsed, phaseTaskImplementation); err == nil || !strings.Contains(err.Error(), "future") {
		t.Fatalf("err = %v, want a past wake_at refused", err)
	}
}

// The golden comparison is sensitive: a flagged seat's prompt is not main's prompt.
func TestFlaggedTaskPromptDiffersFromMainGolden(t *testing.T) {
	want, err := os.ReadFile(flagOffGolden)
	if err != nil {
		t.Fatal(err)
	}
	if got := flagOffDispatchPrompt(t, "{runner: claude, headless: true, explicit_outcome: true}"); got == string(want) {
		t.Fatal("a flagged seat got main's prompt")
	}
}
