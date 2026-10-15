package harness

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// A headless Kimi run writes the Agent Wall's activity log (<dir>/<pid>.log), in the run-activity
// mod's line format, and closes it when the process exits.
func TestHeadlessKimiWritesTheWallActivityLog(t *testing.T) {
	h, root := kimiHeadlessFixture(t, `echo $$ > "$(dirname "$0")/pid"
env > "$(dirname "$0")/env"
`+kimiTurnOK)
	dir := filepath.Join(root, "activity")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGENT_ACTIVITY_DIR", dir)
	if _, _, err := h.SendWithModel(context.Background(), headlessKey, "do the task\nmore detail", "@researcher", nil); err != nil {
		t.Fatal(err)
	}
	pid, _ := os.ReadFile(filepath.Join(root, "pid"))
	body, err := os.ReadFile(filepath.Join(dir, strings.TrimSpace(string(pid))+".log"))
	if err != nil {
		t.Fatalf("activity log missing: %v", err)
	}
	log := string(body)
	for _, want := range []string{"● run started  " + root, "◆ task  do the task", "▶ Bash", "✓ Bash", "◼ turn done", "● run finished", "⏵ kimi-code/k3"} {
		if !strings.Contains(log, want) {
			t.Fatalf("activity log lacks %q:\n%s", want, log)
		}
	}
	if strings.Contains(log, "more detail") {
		t.Fatalf("activity log carries more than the prompt's first line:\n%s", log)
	}
	env, _ := os.ReadFile(filepath.Join(root, "env"))
	if !strings.Contains(string(env), "WERKRBY_TASK_ID=t1\n") {
		t.Fatalf("child env lacks WERKRBY_TASK_ID=t1")
	}
}

func TestHeadlessKimiQuotaWallReadsRedOnTheWall(t *testing.T) {
	h, root := kimiHeadlessFixture(t, `echo $$ > "$(dirname "$0")/pid"
echo "error: failed to run prompt: [provider.auth_error] 403 You've reached your 5-hour usage limit." >&2
exit 1
`)
	dir := filepath.Join(root, "activity")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGENT_ACTIVITY_DIR", dir)
	if _, _, err := h.SendWithModel(context.Background(), headlessKey, "x", "@researcher", nil); err == nil {
		t.Fatal("want a failed turn")
	}
	pid, _ := os.ReadFile(filepath.Join(root, "pid"))
	body, _ := os.ReadFile(filepath.Join(dir, strings.TrimSpace(string(pid))+".log"))
	if !strings.Contains(string(body), "✗ run failed  [provider.auth_error] 403 You've reached your 5-hour usage limit.") {
		t.Fatalf("activity log lacks the red failure line:\n%s", body)
	}
}

func TestHeadlessActivityWritesNothingWithoutADirectory(t *testing.T) {
	if a := newHeadlessActivity("", 123, "/w", "x", ""); a != nil {
		t.Fatal("an activity log with no directory")
	}
	var a *headlessActivity
	a.call("1", "Bash", `{"command":"ls"}`) // nil-safe
	a.result("1")
	a.finish("")
}

func TestActivitySummaryNeverCarriesSecretsOrContent(t *testing.T) {
	cases := map[string]string{
		`{"command":"curl -H 'Authorization: Bearer abc.def' https://x\nsecond line"}`: "curl -H 'Authorization: [redacted] [redacted] https://x",
		`{"command":"export API_KEY=sk-ant-REALLYSECRET123456"}`:                       "export API_KEY=[redacted]",
		`{"path":"/home/dajoey/ops/x.py","content":"SECRET BODY"}`:                     "/home/dajoey/ops/x.py",
		`{"url":"https://api.example.com/v1?token=zzz"}`:                               "api.example.com",
		`not json`: "",
	}
	for args, want := range cases {
		if got := activitySummary("Tool", args); got != want {
			t.Errorf("activitySummary(%s) = %q, want %q", args, got, want)
		}
	}
}

func TestHeadlessTaskIDFromSessionKey(t *testing.T) {
	for key, want := range map[string]string{
		"orchestrator:tasks:task_implementation:tasks-20261015-x-01:1": "tasks-20261015-x-01",
		"orchestrator:tasks:task_review:t2:3":                          "t2",
		"chat:telegram:TG-1":                                           "",
		"heartbeat":                                                    "",
	} {
		if got := headlessTaskID(key); got != want {
			t.Errorf("headlessTaskID(%q) = %q, want %q", key, got, want)
		}
	}
	_ = strconv.Itoa
}
