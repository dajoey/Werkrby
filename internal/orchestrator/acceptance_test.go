package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/workspace"
)

// Acceptance-gate tests. Every test drives the real runtime path: ScanOnce
// claims the queued task, the fake worker hands off by moving the document,
// and reconcileTransitions must enforce the pre-registered acceptance check.
// The tests compile against a tree without the gate and fail behaviorally
// there, proving they cover the live path rather than a helper in isolation.

func acceptanceTestWorkspace(t *testing.T) (config.Config, workflowRoute, string) {
	t.Helper()
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(config.PathForRoot(root))
	registry := filepath.Join(root, "acceptance-registry.jsonl")
	t.Setenv("SPYJO_ACCEPTANCE_REGISTRY", registry)
	return cfg, workflowRoutes()[0], registry
}

// writeAcceptanceCheck writes an executable check script and returns its path and sha256.
func writeAcceptanceCheck(t *testing.T, dir, body string) (string, string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "check.sh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return path, testFileSHA256(t, path)
}

func testFileSHA256(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func registerAcceptanceCheck(t *testing.T, registry, taskID, sha string) {
	t.Helper()
	entry := map[string]any{"task": taskID, "time": time.Now().UTC().Format(time.RFC3339), "sha256": sha}
	line, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(registry, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.Write(append(line, '\n')); err != nil {
		t.Fatal(err)
	}
}

// armAcceptanceCheck pins the acceptance block in the task's front matter.
func armAcceptanceCheck(t *testing.T, path, command, sha string, timeoutSeconds, maxRounds int) {
	t.Helper()
	document, err := ReadDocument(path)
	if err != nil {
		t.Fatal(err)
	}
	document.FrontMatter["acceptance"] = map[string]any{
		"command":         command,
		"sha256":          sha,
		"timeout_seconds": timeoutSeconds,
		"max_rounds":      maxRounds,
	}
	if err := WriteDocument(path, document); err != nil {
		t.Fatal(err)
	}
}

// handoffManager claims the task and simulates the worker handing off to the
// given status during its dispatch.
func handoffManager(t *testing.T, cfg config.Config, route workflowRoute, name, handoff string) *Manager {
	t.Helper()
	fake := newFakeRecipient()
	fake.beforeEmit = func() {
		working := filepath.Join(cfg.Resolve(route.Working), name)
		target := filepath.Join(filepath.Dir(cfg.Resolve(route.Source)), handoff, name)
		if _, err := os.Stat(working); err == nil {
			now := time.Now().UTC()
			if handoff == "done" {
				recordDirectCompletion(t, working, target)
				return
			}
			_ = moveDocument(working, target, handoff, now)
		}
	}
	return New(cfg, fake, extensions.Runner{Directory: filepath.Join(cfg.Root, "missing")})
}

func runClaimCycle(t *testing.T, manager *Manager) {
	t.Helper()
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if err := manager.reconcileTransitions(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func taskPath(cfg config.Config, route workflowRoute, status, name string) string {
	return filepath.Join(filepath.Dir(cfg.Resolve(route.Source)), status, name)
}

func readTaskDocument(t *testing.T, path string) Document {
	t.Helper()
	document, err := ReadDocument(path)
	if err != nil {
		t.Fatalf("cannot read %s: %v", path, err)
	}
	return document
}

func TestAcceptanceGatePassProceeds(t *testing.T) {
	cfg, route, registry := acceptanceTestWorkspace(t)
	task, err := Create(cfg, "tasks", "gate pass", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	check, sha := writeAcceptanceCheck(t, filepath.Join(cfg.Root, "checks", name), "exit 0")
	document := readTaskDocument(t, task)
	registerAcceptanceCheck(t, registry, documentID(document), sha)
	armAcceptanceCheck(t, task, check, sha, 10, 2)

	manager := handoffManager(t, cfg, route, name, "review")
	runClaimCycle(t, manager)

	path := taskPath(cfg, route, "review", name)
	after := readTaskDocument(t, path)
	if !strings.Contains(after.Body, "ACCEPTANCE CHECK PASSED") {
		t.Fatalf("passing check did not record its verdict: %q", after.Body)
	}
	if got := numberValue(after.FrontMatter["attempt"]); got != 1 {
		t.Fatalf("attempt = %d, want 1", got)
	}
	if _, ok := after.FrontMatter["acceptance_rounds"]; ok {
		t.Fatalf("passing check changed acceptance_rounds: %#v", after.FrontMatter["acceptance_rounds"])
	}
}

func TestAcceptanceGateFailsOnceThenPasses(t *testing.T) {
	cfg, route, registry := acceptanceTestWorkspace(t)
	task, err := Create(cfg, "tasks", "gate fail once", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	marker := filepath.Join(cfg.Root, "marker")
	check, sha := writeAcceptanceCheck(t, filepath.Join(cfg.Root, "checks", name),
		fmt.Sprintf("if [ ! -f %q ]; then echo 'the marker file is missing'; exit 1; fi", marker))
	document := readTaskDocument(t, task)
	registerAcceptanceCheck(t, registry, documentID(document), sha)
	armAcceptanceCheck(t, task, check, sha, 10, 2)

	manager := handoffManager(t, cfg, route, name, "review")
	runClaimCycle(t, manager)

	bounced := readTaskDocument(t, taskPath(cfg, route, "todo", name))
	if !strings.Contains(bounced.Body, "ACCEPTANCE CHECK FAILED (round 1 of 2)") {
		t.Fatalf("round-1 bounce note missing: %q", bounced.Body)
	}
	if !strings.Contains(bounced.Body, "the marker file is missing") {
		t.Fatalf("round-1 bounce does not carry the check output: %q", bounced.Body)
	}
	if got := numberValue(bounced.FrontMatter["acceptance_rounds"]); got != 1 {
		t.Fatalf("acceptance_rounds = %d, want 1", got)
	}
	if bounced.FrontMatter[resumeCreditField] == true {
		t.Fatal("a failed acceptance check must spend the attempt on the next claim")
	}

	if err := os.WriteFile(marker, []byte("ready"), 0o600); err != nil {
		t.Fatal(err)
	}
	runClaimCycle(t, manager)

	after := readTaskDocument(t, taskPath(cfg, route, "review", name))
	if !strings.Contains(after.Body, "ACCEPTANCE CHECK PASSED") {
		t.Fatalf("round-2 pass did not proceed to review: %q", after.Body)
	}
	if got := numberValue(after.FrontMatter["attempt"]); got != 2 {
		t.Fatalf("attempt = %d, want 2 (the failed round must cost one attempt)", got)
	}
	if got := numberValue(after.FrontMatter["acceptance_rounds"]); got != 1 {
		t.Fatalf("acceptance_rounds = %d, want 1", got)
	}
}

func TestAcceptanceGateFailsTwiceGoesToReviewWithBothOutputs(t *testing.T) {
	cfg, route, registry := acceptanceTestWorkspace(t)
	task, err := Create(cfg, "tasks", "gate fail twice", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	check, sha := writeAcceptanceCheck(t, filepath.Join(cfg.Root, "checks", name),
		"echo 'still not passing'; exit 1")
	document := readTaskDocument(t, task)
	registerAcceptanceCheck(t, registry, documentID(document), sha)
	armAcceptanceCheck(t, task, check, sha, 10, 2)

	manager := handoffManager(t, cfg, route, name, "review")
	runClaimCycle(t, manager)
	runClaimCycle(t, manager)

	path := taskPath(cfg, route, "review", name)
	after := readTaskDocument(t, path)
	if !strings.Contains(after.Body, "ACCEPTANCE CHECK FAILED (round 2 of 2)") {
		t.Fatalf("round-2 failure did not reach review: %q", after.Body)
	}
	if !strings.Contains(after.Body, "ACCEPTANCE CHECK FAILED (round 1 of 2)") {
		t.Fatalf("round-1 output missing from Progress: %q", after.Body)
	}
	if !strings.Contains(after.Body, "still not passing") {
		t.Fatalf("check output missing from review hand-off: %q", after.Body)
	}
	if !strings.Contains(after.Body, "check or the work is wrong") {
		t.Fatalf("reviewer guidance line missing: %q", after.Body)
	}
	if got := numberValue(after.FrontMatter["acceptance_rounds"]); got != 2 {
		t.Fatalf("acceptance_rounds = %d, want 2", got)
	}
}

func TestAcceptanceGateTimesOutFailsOpen(t *testing.T) {
	cfg, route, registry := acceptanceTestWorkspace(t)
	task, err := Create(cfg, "tasks", "gate timeout", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	check, sha := writeAcceptanceCheck(t, filepath.Join(cfg.Root, "checks", name), "sleep 3; exit 0")
	document := readTaskDocument(t, task)
	registerAcceptanceCheck(t, registry, documentID(document), sha)
	armAcceptanceCheck(t, task, check, sha, 1, 2)

	manager := handoffManager(t, cfg, route, name, "review")
	runClaimCycle(t, manager)

	path := taskPath(cfg, route, "review", name)
	after := readTaskDocument(t, path)
	if !strings.Contains(after.Body, "ACCEPTANCE CHECK INFRASTRUCTURE FAILURE") {
		t.Fatalf("timeout not recorded as an infrastructure failure: %q", after.Body)
	}
	if !strings.Contains(after.Body, "no attempt spent") {
		t.Fatalf("timeout note does not say no attempt was spent: %q", after.Body)
	}
	if _, ok := after.FrontMatter["acceptance_rounds"]; ok {
		t.Fatalf("timeout must not count a round: %#v", after.FrontMatter["acceptance_rounds"])
	}
	if got := numberValue(after.FrontMatter["attempt"]); got != 1 {
		t.Fatalf("attempt = %d, want 1", got)
	}
}

func TestAcceptanceGateMissingScriptFailsOpen(t *testing.T) {
	cfg, route, registry := acceptanceTestWorkspace(t)
	task, err := Create(cfg, "tasks", "gate missing script", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	missing := filepath.Join(cfg.Root, "checks", name, "check.sh")
	document := readTaskDocument(t, task)
	registerAcceptanceCheck(t, registry, documentID(document), "0000000000000000000000000000000000000000000000000000000000000000")
	armAcceptanceCheck(t, task, missing, "0000000000000000000000000000000000000000000000000000000000000000", 10, 2)

	manager := handoffManager(t, cfg, route, name, "review")
	runClaimCycle(t, manager)

	path := taskPath(cfg, route, "review", name)
	after := readTaskDocument(t, path)
	if !strings.Contains(after.Body, "ACCEPTANCE CHECK INFRASTRUCTURE FAILURE") {
		t.Fatalf("missing script not recorded as an infrastructure failure: %q", after.Body)
	}
	if _, ok := after.FrontMatter["acceptance_rounds"]; ok {
		t.Fatalf("missing script must not count a round: %#v", after.FrontMatter["acceptance_rounds"])
	}
}

func TestAcceptanceGateTamperedScriptFailsClosed(t *testing.T) {
	cfg, route, registry := acceptanceTestWorkspace(t)
	task, err := Create(cfg, "tasks", "gate tamper", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	check, sha := writeAcceptanceCheck(t, filepath.Join(cfg.Root, "checks", name), "exit 0")
	document := readTaskDocument(t, task)
	registerAcceptanceCheck(t, registry, documentID(document), sha)
	// The worker rewrites the check after registration.
	if err := os.WriteFile(check, []byte("#!/bin/sh\nexit 0  # quietly edited\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	armAcceptanceCheck(t, task, check, sha, 10, 2)

	manager := handoffManager(t, cfg, route, name, "review")
	runClaimCycle(t, manager)

	bounced := readTaskDocument(t, taskPath(cfg, route, "todo", name))
	if !strings.Contains(bounced.Body, "ACCEPTANCE CHECK TAMPERED") {
		t.Fatalf("hash mismatch not reported as tampering: %q", bounced.Body)
	}
	if _, ok := bounced.FrontMatter["acceptance_rounds"]; ok {
		t.Fatalf("tampering must never count as a check round: %#v", bounced.FrontMatter["acceptance_rounds"])
	}

	// A second hand-off of the same tampered task can never pass.
	runClaimCycle(t, manager)
	again := readTaskDocument(t, taskPath(cfg, route, "todo", name))
	if !strings.Contains(again.Body, "ACCEPTANCE CHECK TAMPERED") {
		t.Fatalf("tampering is not fail-closed on the second hand-off: %q", again.Body)
	}
}

func TestAcceptanceGateUnregisteredCheckFailsClosed(t *testing.T) {
	cfg, route, _ := acceptanceTestWorkspace(t)
	task, err := Create(cfg, "tasks", "gate unregistered", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	check, sha := writeAcceptanceCheck(t, filepath.Join(cfg.Root, "checks", name), "exit 0")
	armAcceptanceCheck(t, task, check, sha, 10, 2)

	manager := handoffManager(t, cfg, route, name, "review")
	runClaimCycle(t, manager)

	bounced := readTaskDocument(t, taskPath(cfg, route, "todo", name))
	if !strings.Contains(bounced.Body, "ACCEPTANCE CHECK TAMPERED") {
		t.Fatalf("an unregistered check must fail closed: %q", bounced.Body)
	}
}

func TestAcceptanceGateStrippedBlockFailsClosed(t *testing.T) {
	cfg, route, registry := acceptanceTestWorkspace(t)
	task, err := Create(cfg, "tasks", "gate stripped", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	check, sha := writeAcceptanceCheck(t, filepath.Join(cfg.Root, "checks", name), "exit 0")
	document := readTaskDocument(t, task)
	registerAcceptanceCheck(t, registry, documentID(document), sha)
	// The worker hands off after deleting the acceptance block entirely.
	_ = check

	manager := handoffManager(t, cfg, route, name, "review")
	runClaimCycle(t, manager)

	bounced := readTaskDocument(t, taskPath(cfg, route, "todo", name))
	if !strings.Contains(bounced.Body, "ACCEPTANCE CHECK TAMPERED") {
		t.Fatalf("deleting the acceptance block must fail closed: %q", bounced.Body)
	}
}

func TestAcceptanceGateAbsentKeyAndUnregisteredTaskUnchanged(t *testing.T) {
	cfg, route, _ := acceptanceTestWorkspace(t)
	task, err := Create(cfg, "tasks", "gate absent", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)

	manager := handoffManager(t, cfg, route, name, "review")
	runClaimCycle(t, manager)

	path := taskPath(cfg, route, "review", name)
	after := readTaskDocument(t, path)
	if strings.Contains(after.Body, "ACCEPTANCE CHECK") {
		t.Fatalf("ungated task must hand off exactly as before: %q", after.Body)
	}
	for key := range after.FrontMatter {
		if strings.HasPrefix(key, "acceptance") {
			t.Fatalf("ungated task gained front matter %q", key)
		}
	}
	if got := numberValue(after.FrontMatter["attempt"]); got != 1 {
		t.Fatalf("attempt = %d, want 1", got)
	}
}

func TestAcceptanceGateRestartDuringCheckSpendsNoAttempt(t *testing.T) {
	cfg, route, registry := acceptanceTestWorkspace(t)
	task, err := Create(cfg, "tasks", "gate restart", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	check, sha := writeAcceptanceCheck(t, filepath.Join(cfg.Root, "checks", name), "sleep 2; echo done; exit 1")
	document := readTaskDocument(t, task)
	registerAcceptanceCheck(t, registry, documentID(document), sha)
	armAcceptanceCheck(t, task, check, sha, 30, 2)

	fake := newFakeRecipient()
	fake.beforeEmit = func() {
		working := filepath.Join(cfg.Resolve(route.Working), name)
		target := filepath.Join(filepath.Dir(cfg.Resolve(route.Source)), "review", name)
		if _, err := os.Stat(working); err == nil {
			_ = moveDocument(working, target, "review", time.Now().UTC())
		}
	}
	manager := New(cfg, fake, extensions.Runner{Directory: filepath.Join(cfg.Root, "missing")})
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()

	// A service restart cancels the manager context while the check runs.
	interrupted, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(300 * time.Millisecond)
		cancel()
	}()
	reconcileErr := manager.reconcileTransitions(interrupted)

	path := taskPath(cfg, route, "review", name)
	after := readTaskDocument(t, path)
	if strings.Contains(after.Body, "ACCEPTANCE CHECK") {
		t.Fatalf("an interrupted check must not write any note: %q", after.Body)
	}
	if _, ok := after.FrontMatter["acceptance_rounds"]; ok {
		t.Fatalf("an interrupted check must not count a round: %#v", after.FrontMatter["acceptance_rounds"])
	}
	if got := numberValue(after.FrontMatter["attempt"]); got != 1 {
		t.Fatalf("attempt = %d, want 1 (restart must not spend an attempt)", got)
	}
	if reconcileErr == nil {
		t.Fatal("an interrupted check must surface an error so recovery re-runs the transition")
	}

	// After the restart, the recovered transition runs the check once.
	if err := manager.reconcileTransitions(context.Background()); err != nil {
		t.Fatal(err)
	}
	recovered := readTaskDocument(t, taskPath(cfg, route, "todo", name))
	if !strings.Contains(recovered.Body, "ACCEPTANCE CHECK FAILED (round 1 of 2)") {
		t.Fatalf("recovered hand-off did not run the check exactly once: %q", recovered.Body)
	}
	if strings.Count(recovered.Body, "ACCEPTANCE CHECK FAILED (round 1 of 2)") != 1 {
		t.Fatalf("the check ran more than once: %q", recovered.Body)
	}
}

func TestAcceptanceGateTrimsOutput(t *testing.T) {
	cfg, route, registry := acceptanceTestWorkspace(t)
	task, err := Create(cfg, "tasks", "gate trim", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	var script strings.Builder
	for i := 1; i <= 40; i++ {
		fmt.Fprintf(&script, "echo 'line %02d %s'", i, strings.Repeat("x", 60))
		if i < 40 {
			script.WriteString("; ")
		}
	}
	script.WriteString("; exit 1")
	check, sha := writeAcceptanceCheck(t, filepath.Join(cfg.Root, "checks", name), script.String())
	document := readTaskDocument(t, task)
	registerAcceptanceCheck(t, registry, documentID(document), sha)
	armAcceptanceCheck(t, task, check, sha, 10, 2)

	manager := handoffManager(t, cfg, route, name, "review")
	runClaimCycle(t, manager)

	bounced := readTaskDocument(t, taskPath(cfg, route, "todo", name))
	note := ""
	for _, line := range strings.Split(bounced.Body, "\n") {
		if strings.Contains(line, "ACCEPTANCE CHECK FAILED") {
			note = line
		}
	}
	if note == "" {
		t.Fatalf("failure note missing: %q", bounced.Body)
	}
	if !strings.Contains(note, "line 40") {
		t.Fatalf("trimmed output must keep the tail, got: %q", note)
	}
	if strings.Contains(note, "line 01") {
		t.Fatalf("trimmed output must drop the head: %q", note)
	}
	if len(note) > 1000 {
		t.Fatalf("note exceeds the framework progress cap: %d runes", len(note))
	}
}

func TestAcceptanceGateDirectDonePass(t *testing.T) {
	cfg, route, registry := acceptanceTestWorkspace(t)
	task, err := CreateWithOptions(cfg, "tasks", "gate direct done", "", CreateOptions{NoReview: true})
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	check, sha := writeAcceptanceCheck(t, filepath.Join(cfg.Root, "checks", name), "exit 0")
	document := readTaskDocument(t, task)
	registerAcceptanceCheck(t, registry, documentID(document), sha)
	armAcceptanceCheck(t, task, check, sha, 10, 2)

	manager := handoffManager(t, cfg, route, name, "done")
	runClaimCycle(t, manager)

	path := taskPath(cfg, route, "done", name)
	after := readTaskDocument(t, path)
	if !strings.Contains(after.Body, "ACCEPTANCE CHECK PASSED") {
		t.Fatalf("passing check did not complete the direct hand-off: %q", after.Body)
	}
}

func TestAcceptanceGateDirectDoneFailsOnceThenBounces(t *testing.T) {
	cfg, route, registry := acceptanceTestWorkspace(t)
	task, err := CreateWithOptions(cfg, "tasks", "gate direct fail", "", CreateOptions{NoReview: true})
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	marker := filepath.Join(cfg.Root, "direct-marker")
	check, sha := writeAcceptanceCheck(t, filepath.Join(cfg.Root, "checks", name),
		fmt.Sprintf("test -f %q || { echo 'direct marker missing'; exit 1; }", marker))
	document := readTaskDocument(t, task)
	registerAcceptanceCheck(t, registry, documentID(document), sha)
	armAcceptanceCheck(t, task, check, sha, 10, 2)

	manager := handoffManager(t, cfg, route, name, "done")
	runClaimCycle(t, manager)

	bounced := readTaskDocument(t, taskPath(cfg, route, "todo", name))
	if !strings.Contains(bounced.Body, "ACCEPTANCE CHECK FAILED (round 1 of 2)") {
		t.Fatalf("direct-done bounce note missing: %q", bounced.Body)
	}
	if !strings.Contains(bounced.Body, "direct marker missing") {
		t.Fatalf("direct-done bounce does not carry the output: %q", bounced.Body)
	}
}
