package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/workspace"
)

// corruptTaskFrontMatter and corruptGoalFrontMatter reproduce the 2026-09-19
// breakage: an orphan list item written between two top-level mapping keys.
const corruptTaskFrontMatter = `---
id: tasks-scratch-corrupt
title: "scratch corrupt task"
status: todo
created_at: "2026-09-19T18:00:00Z"
updated_at: "2026-09-19T18:00:00Z"
review_required: false
provider_iterations: 1
- id: SC6-orphan
  condition: "written outside success_criteria"
review_trigger: all_round_tasks_settled
---

# scratch corrupt task

## Progress

- keep me
`

const corruptGoalFrontMatter = `---
id: goals-scratch-corrupt
title: "scratch corrupt goal"
status: active
round: 1
created_at: "2026-09-19T18:00:00Z"
updated_at: "2026-09-19T18:00:00Z"
provider_iterations: 1
- id: SC6-orphan
  condition: "written outside success_criteria"
review_trigger: all_round_tasks_settled
---

# scratch corrupt goal
`

func newScratchWorkspace(t *testing.T) (string, config.Config) {
	t.Helper()
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	return root, cfg
}

func writeScratchDocument(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

// liveRepairTasks returns every repair task sitting in a live task folder. A
// repair task is ordinary queued work, so the same cycle that files one may
// already have claimed it into working/.
func liveRepairTasks(t *testing.T, cfg config.Config) map[string]string {
	t.Helper()
	found := map[string]string{}
	for _, status := range []string{"todo", "working"} {
		entries, err := os.ReadDir(cfg.StatePath("tasks", status))
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if !strings.HasPrefix(entry.Name(), unparseableRepairPrefix) {
				continue
			}
			data, err := os.ReadFile(cfg.StatePath("tasks", status, entry.Name()))
			if err != nil {
				t.Fatal(err)
			}
			found[strings.TrimSuffix(entry.Name(), ".md")] = string(data)
		}
	}
	return found
}

func readRepairTask(t *testing.T, repairs map[string]string, corrupt string) string {
	t.Helper()
	body, ok := repairs[unparseableRepairID(corrupt)]
	if !ok {
		t.Fatalf("no repair task for %s; found %v", filepath.Base(corrupt), keysOf(repairs))
	}
	return body
}

func keysOf(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// One ordinary orchestrator cycle over a scratch workspace must turn both a
// corrupt task and a corrupt goal into visible queued work plus a structured
// error record, and must leave the corrupt documents byte-identical.
func TestScanOnceFilesRepairTasksForUnparseableDocuments(t *testing.T) {
	root, cfg := newScratchWorkspace(t)

	corruptTask := cfg.StatePath("tasks", "todo", "tasks-scratch-corrupt.md")
	corruptGoal := cfg.StatePath("goals", "active", "goals-scratch-corrupt.md")
	writeScratchDocument(t, corruptTask, corruptTaskFrontMatter)
	writeScratchDocument(t, corruptGoal, corruptGoalFrontMatter)

	manager := New(cfg, newFakeRecipient(), extensions.Runner{Directory: filepath.Join(root, "missing")})
	var errorRecords []string
	manager.LogError = func(component, event, message string) {
		errorRecords = append(errorRecords, component+"|"+event+"|"+message)
	}
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()

	repairs := liveRepairTasks(t, cfg)
	taskRepair := readRepairTask(t, repairs, corruptTask)
	goalRepair := readRepairTask(t, repairs, corruptGoal)
	if !strings.Contains(taskRepair, "tasks-scratch-corrupt.md") {
		t.Fatalf("task repair does not name the corrupt document:\n%s", taskRepair)
	}
	if !strings.Contains(goalRepair, "this goal document's YAML front matter") {
		t.Fatalf("goal repair is not written as a goal repair:\n%s", goalRepair)
	}
	for _, repair := range []string{taskRepair, goalRepair} {
		document, err := ParseDocument([]byte(repair))
		if err != nil {
			t.Fatalf("filed repair task does not parse: %v\n%s", err, repair)
		}
		// Filed into todo; the same cycle may already have claimed it.
		if status, _ := document.FrontMatter["status"].(string); status != "todo" && status != "working" {
			t.Fatalf("repair task status = %q, want todo or working", status)
		}
		policy, err := TaskPolicyFromDocument(document)
		if err != nil || policy.ReviewRequired {
			t.Fatalf("repair task review policy = %#v, %v", policy, err)
		}
		notify, ok := document.FrontMatter["notify"].(map[string]any)
		if !ok {
			t.Fatalf("repair task has no notify mapping: %#v", document.FrontMatter["notify"])
		}
		if enabled, _ := notify["enabled"].(bool); !enabled {
			t.Fatalf("repair task notify.enabled = %#v", notify["enabled"])
		}
		outcomes, ok := notify["on"].([]any)
		if !ok || len(outcomes) != 3 {
			t.Fatalf(`repair task notify."on" = %#v (an unquoted "on" key is read as boolean true)`, notify)
		}
	}

	if len(errorRecords) != 2 {
		t.Fatalf("structured error records = %#v, want one per corrupt document", errorRecords)
	}
	for _, record := range errorRecords {
		if !strings.Contains(record, "|unparseable_document|") {
			t.Fatalf("error record is not the unparseable event: %s", record)
		}
	}

	// The corrupt documents are reported, never rewritten.
	for path, want := range map[string]string{corruptTask: corruptTaskFrontMatter, corruptGoal: corruptGoalFrontMatter} {
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != want {
			t.Fatalf("%s was rewritten by the report path", filepath.Base(path))
		}
	}

	// A second cycle must not file a duplicate or repeat the error record.
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if len(errorRecords) != 2 {
		t.Fatalf("second cycle repeated the error record: %#v", errorRecords)
	}
	if again := liveRepairTasks(t, cfg); len(again) != 2 {
		t.Fatalf("live repair tasks = %v, want exactly one per corrupt document", keysOf(again))
	}
}

// A repair task that has already been settled must not be filed a second time,
// and a corrupt repair task must never escalate into another repair task.
func TestUnparseableReportsDoNotLoopOrDuplicate(t *testing.T) {
	root, cfg := newScratchWorkspace(t)
	manager := New(cfg, newFakeRecipient(), extensions.Runner{Directory: filepath.Join(root, "missing")})

	corrupt := cfg.StatePath("tasks", "todo", "tasks-scratch-corrupt.md")
	writeScratchDocument(t, corrupt, corruptTaskFrontMatter)
	settled := cfg.StatePath("tasks", "done", unparseableRepairID(corrupt)+".md")
	writeScratchDocument(t, settled, "---\nid: settled\n---\n")

	_, parseErr := ReadDocument(corrupt)
	if parseErr == nil {
		t.Fatal("expected the corrupt fixture to fail ParseDocument")
	}
	manager.reportUnparseableDocument(corrupt, parseErr)
	if _, err := os.Stat(cfg.StatePath("tasks", "todo", unparseableRepairID(corrupt)+".md")); err == nil {
		t.Fatal("filed a second repair task while a settled one exists")
	}

	selfCorrupt := cfg.StatePath("tasks", "todo", unparseableRepairPrefix+"self.md")
	writeScratchDocument(t, selfCorrupt, corruptTaskFrontMatter)
	manager.reportUnparseableDocument(selfCorrupt, parseErr)
	if _, err := os.Stat(cfg.StatePath("tasks", "todo", unparseableRepairID(selfCorrupt)+".md")); err == nil {
		t.Fatal("a corrupt repair task escalated into another repair task")
	}
}

func TestUnparseableRepairIDIsAStableSlug(t *testing.T) {
	first := unparseableRepairID("/x/.spynel/goals/active/goals-20260919 Gluttony.md")
	if first != "tasks-unparseable-goals-20260919-gluttony" {
		t.Fatalf("repair id = %q", first)
	}
	if second := unparseableRepairID("/other/goals-20260919 Gluttony.md"); second != first {
		t.Fatalf("repair id is not stable across directories: %q vs %q", second, first)
	}
}
