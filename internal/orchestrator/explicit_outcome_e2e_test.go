package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/harness"
)

// fakeHeadlessClaude stands in for `claude -p --output-format stream-json`: it reads the prompt on
// stdin, finds the claimed task file and the result path the prompt names, writes its outcome to
// result.json, prints stream-json and exits. With FAKE_CLAUDE_EDITS_TASK=1 its implementation turn
// also ignores the contract and edits Werkrby-owned keys of the task file in place. The reviewer turn
// reports a different session than the implementer, as a fresh review session does.
const fakeHeadlessClaude = `#!/bin/sh
prompt=$(cat)
here=$(dirname "$0")
result=$(printf '%s' "$prompt" | grep -oE '[^` + "`" + ` ]*/result\.json' | head -n 1)
task=$(printf '%s' "$prompt" | grep -oE '[^` + "`" + ` ]*/tasks/(working|reviewing)/[^` + "`" + ` ]*\.md' | head -n 1)
case "$prompt" in
  *"independent task reviewer"*)
    session=sess-review
    outcome='{"outcome":"accepted","progress":"Reviewed the probe change; the evidence holds.","front_matter":{"completion_summary":{"verdict":"accepted","outcome":"Probe change verified","evidence":"Re-ran the probe check"}}}'
    ;;
  *)
    session=sess-impl
    outcome='{"outcome":"review","progress":"Implemented the probe change.\nVerified with the probe check."}'
    if [ "$FAKE_CLAUDE_EDITS_TASK" = 1 ]; then
      sed -i -e 's/^attempt: 1$/attempt: 9/' -e 's/^review_required: true$/review_required: false/' "$task"
    fi
    ;;
esac
echo '{"type":"system","subtype":"init","session_id":"'$session'"}'
printf '%s' "$outcome" > "$result"
echo "$session $task" >> "$here/turns.log"
echo '{"type":"result","subtype":"success","is_error":false,"result":"Turn finished.","session_id":"'$session'"}'
`

// runHeadlessE2E runs the real orchestrator and the real Herdr headless harness against the fake
// `claude` on PATH until the task is done, and returns the done document and Werkrby's log.
func runHeadlessE2E(t *testing.T, editsTask bool) (Document, *logSink, string) {
	t.Helper()
	cfg, name := explicitFixture(t, true, false)
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "claude"), []byte(fakeHeadlessClaude), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	if editsTask {
		t.Setenv("FAKE_CLAUDE_EDITS_TASK", "1")
	} else {
		t.Setenv("FAKE_CLAUDE_EDITS_TASK", "0")
	}
	herdr, err := harness.NewHerdr(harness.HarnessConfig{
		Command: filepath.Join(cfg.Root, "no-such-herdr"), Cwd: cfg.Root,
		Sandbox: "danger-full-access", ApprovalPolicy: "never",
	})
	if err != nil {
		t.Fatal(err)
	}
	manager := New(cfg, herdr, extensions.Runner{Directory: filepath.Join(cfg.Root, "missing")})
	logs := &logSink{}
	manager.Log = logs.log

	done := statusFile(cfg, "done", name)
	deadline := time.Now().Add(30 * time.Second)
	for {
		if err := manager.ScanOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		manager.Wait()
		if _, err := os.Stat(done); err == nil {
			if leases, _ := manager.loadLeases(); len(leases) == 0 {
				break
			}
		}
		if time.Now().After(deadline) {
			leases, _ := manager.loadLeases()
			t.Fatalf("task never reached done/ under the owner; leases: %#v", leases)
		}
	}
	document, err := ReadDocument(done)
	if err != nil {
		t.Fatal(err)
	}
	turns, err := os.ReadFile(filepath.Join(bin, "turns.log"))
	if err != nil {
		t.Fatalf("the fake worker never ran: %v", err)
	}
	if entries, _ := os.ReadDir(cfg.StatePath("runtime", "turns")); len(entries) != 0 {
		t.Fatalf("run directories left behind: %d", len(entries))
	}
	return document, logs, string(turns)
}

// TestExplicitOutcomeEndToEndHeadlessClaude: the task goes todo -> working -> review -> reviewing ->
// done with every front-matter write and move made by Werkrby; the worker only writes result.json.
// Werkrby hashes the task file when it dispatches and again before it applies the outcome, and logs
// any change; TestExplicitOutcomeEndToEndRestoresAWorkerEdit proves that check fires.
func TestExplicitOutcomeEndToEndHeadlessClaude(t *testing.T) {
	document, logs, turns := runHeadlessE2E(t, false)
	if document.FrontMatter["status"] != "done" {
		t.Fatalf("status = %v", document.FrontMatter["status"])
	}
	summary, _ := document.FrontMatter["completion_summary"].(map[string]any)
	if summary["verdict"] != "accepted" || summary["reviewed_at"] != document.FrontMatter["updated_at"] {
		t.Fatalf("completion_summary = %#v (updated_at %v)", summary, document.FrontMatter["updated_at"])
	}
	if document.FrontMatter["implementation_thread"] != "sess-impl" {
		t.Fatalf("implementation_thread = %v, want the implementer's session", document.FrontMatter["implementation_thread"])
	}
	for _, want := range []string{"Implemented the probe change.", "Verified with the probe check.", "Reviewed the probe change; the evidence holds."} {
		if !strings.Contains(document.Body, want) {
			t.Fatalf("progress entry %q missing:\n%s", want, document.Body)
		}
	}
	if lines := strings.Split(strings.TrimSpace(turns), "\n"); len(lines) != 2 {
		t.Fatalf("worker turns = %d, want one implementation and one review:\n%s", len(lines), turns)
	}
	if logs.has("changed on disk during the turn") {
		t.Fatalf("Werkrby saw the task file change during a turn of a worker that never wrote it: %v", logs.lines)
	}
}

// TestExplicitOutcomeEndToEndRestoresAWorkerEdit: a worker that edits the task file in place is
// caught by the same hash check, and its edits to Werkrby-owned keys do not survive the move.
func TestExplicitOutcomeEndToEndRestoresAWorkerEdit(t *testing.T) {
	document, logs, _ := runHeadlessE2E(t, true)
	if !logs.has("changed on disk during the turn") {
		t.Fatalf("Werkrby did not see the worker's in-place edit: %v", logs.lines)
	}
	if numberValue(document.FrontMatter["attempt"]) != 1 || document.FrontMatter["review_required"] != true {
		t.Fatalf("the worker's in-place edits survived: attempt %v review_required %v", document.FrontMatter["attempt"], document.FrontMatter["review_required"])
	}
}
