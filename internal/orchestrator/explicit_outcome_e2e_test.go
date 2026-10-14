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
// stdin, finds the claimed task file and the result path the prompt names, records a hash of the
// task file before and after its "work", writes its outcome to result.json, prints stream-json and
// exits. It never writes the task file. The reviewer turn reports a different session than the
// implementer, as a fresh review session does.
const fakeHeadlessClaude = `#!/bin/sh
prompt=$(cat)
here=$(dirname "$0")
printf '%s' "$prompt" > "$here/prompt-$$.txt"
result=$(printf '%s' "$prompt" | grep -oE '[^` + "`" + ` ]*/result\.json' | head -n 1)
task=$(printf '%s' "$prompt" | grep -oE '[^` + "`" + ` ]*/tasks/(working|reviewing)/[^` + "`" + ` ]*\.md' | head -n 1)
before=$(sha256sum "$task" | cut -d ' ' -f 1)
case "$prompt" in
  *"independent task reviewer"*)
    session=sess-review
    outcome='{"outcome":"accepted","progress":"Reviewed the probe change; the evidence holds.","front_matter":{"completion_summary":{"verdict":"accepted","outcome":"Probe change verified","evidence":"Re-ran the probe check"}}}'
    ;;
  *)
    session=sess-impl
    outcome='{"outcome":"review","progress":"Implemented the probe change.\nVerified with the probe check."}'
    ;;
esac
echo '{"type":"system","subtype":"init","session_id":"'$session'"}'
printf '%s' "$outcome" > "$result"
after=$(sha256sum "$task" | cut -d ' ' -f 1)
echo "$task $before $after" >> "$here/touch.log"
echo '{"type":"result","subtype":"success","is_error":false,"result":"Turn finished.","session_id":"'$session'"}'
`

// TestExplicitOutcomeEndToEndHeadlessClaude runs the real orchestrator and the real Herdr headless
// harness against a fake `claude` on PATH. The task goes todo -> working -> review -> reviewing ->
// done with every front-matter write and move made by Werkrby; the worker only writes result.json.
func TestExplicitOutcomeEndToEndHeadlessClaude(t *testing.T) {
	cfg, name := explicitFixture(t, true, false)
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "claude"), []byte(fakeHeadlessClaude), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	herdr, err := harness.NewHerdr(harness.HarnessConfig{
		Command: filepath.Join(cfg.Root, "no-such-herdr"), Cwd: cfg.Root,
		Sandbox: "danger-full-access", ApprovalPolicy: "never",
	})
	if err != nil {
		t.Fatal(err)
	}
	manager := New(cfg, herdr, extensions.Runner{Directory: filepath.Join(cfg.Root, "missing")})

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

	touch, err := os.ReadFile(filepath.Join(bin, "touch.log"))
	if err != nil {
		t.Fatalf("the fake worker never ran: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(touch)), "\n")
	if len(lines) != 2 {
		t.Fatalf("worker turns = %d, want one implementation and one review:\n%s", len(lines), touch)
	}
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) != 3 || fields[1] == "" || fields[1] != fields[2] {
			t.Fatalf("the worker changed the task file during its turn: %q", line)
		}
	}
	if entries, _ := os.ReadDir(cfg.StatePath("runtime", "turns")); len(entries) != 0 {
		t.Fatalf("run directories left behind: %d", len(entries))
	}
}
