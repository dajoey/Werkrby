package orchestrator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

	// The grace holds the first sighting; the backdated second scan files.
	backdateUnparseableSightings(t, manager)
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
		if _, err := NotificationFromDocument(document); err != nil {
			t.Fatalf("repair task notify is unusable: %v", err)
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
	first := unparseableRepairID("/x/.werkrby/goals/active/goals-20260919 Gluttony.md")
	if first != "tasks-unparseable-goals-20260919-gluttony" {
		t.Fatalf("repair id = %q", first)
	}
	if second := unparseableRepairID("/other/goals-20260919 Gluttony.md"); second != first {
		t.Fatalf("repair id is not stable across directories: %q vs %q", second, first)
	}
}

const validScratchTaskFrontMatter = `---
id: tasks-scratch-valid
title: "valid task"
status: todo
created_at: "2026-09-22T00:00:00Z"
updated_at: "2026-09-22T00:00:00Z"
review_required: false
---

# valid task
`

func assertValidTaskClaimed(t *testing.T, cfg config.Config) {
	t.Helper()
	if _, err := os.Stat(cfg.StatePath("tasks", "working", "tasks-scratch-valid.md")); err != nil {
		t.Fatalf("valid task was not claimed into working/: %v", err)
	}
	if _, err := os.Stat(cfg.StatePath("tasks", "todo", "tasks-scratch-valid.md")); !os.IsNotExist(err) {
		t.Fatalf("valid task still sits in todo/: %v", err)
	}
}

// A broken document in review/ whose implementation lease is still open used to
// abort reconcileTransitions and stop every later claim (2026-09-22 stall).
func TestScanOnceClaimsAlongsideUnparseableReviewDocument(t *testing.T) {
	root, cfg := newScratchWorkspace(t)
	writeScratchDocument(t, cfg.StatePath("tasks", "todo", "tasks-scratch-valid.md"), validScratchTaskFrontMatter)
	corrupt := cfg.StatePath("tasks", "review", "tasks-scratch-corrupt.md")
	writeScratchDocument(t, corrupt, corruptTaskFrontMatter)

	now := time.Now().UTC()
	key := leaseID("tasks:"+phaseTaskImplementation, "tasks-scratch-corrupt")
	manager := New(cfg, newFakeRecipient(), extensions.Runner{Directory: filepath.Join(root, "missing")})
	var errorRecords []string
	manager.LogError = func(component, event, message string) {
		errorRecords = append(errorRecords, component+"|"+event+"|"+message)
	}
	if err := manager.saveLease(Lease{
		ID: key, ClaimID: key, DocumentType: "task", Route: "tasks",
		OwnerID: manager.ownerID, File: cfg.StatePath("tasks", "working", "tasks-scratch-corrupt.md"),
		SessionKey: "sess", State: "awaiting_transition", Phase: phaseTaskImplementation,
		ClaimAttempt: 1, StartedAt: now, HeartbeatAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatalf("ScanOnce aborted the board on one unreadable review document: %v", err)
	}
	manager.Wait()
	assertValidTaskClaimed(t, cfg)

	// The grace holds the first sighting; the backdated second scan files.
	backdateUnparseableSightings(t, manager)
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()

	repairs := liveRepairTasks(t, cfg)
	if body := readRepairTask(t, repairs, corrupt); !strings.Contains(body, "tasks-scratch-corrupt.md") {
		t.Fatalf("repair task does not name the corrupt review document:\n%s", body)
	}
	if len(errorRecords) != 1 || !strings.Contains(errorRecords[0], "|unparseable_document|") {
		t.Fatalf("error records = %#v, want one unparseable_document", errorRecords)
	}
	// Lease retained so a repaired document can still finish reconciliation.
	if !manager.leaseExists(key) {
		t.Fatal("implementation lease was dropped before the document could be repaired")
	}
	got, err := os.ReadFile(corrupt)
	if err != nil || string(got) != corruptTaskFrontMatter {
		t.Fatal("corrupt review document was rewritten")
	}

	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if len(errorRecords) != 1 {
		t.Fatalf("second cycle repeated the error record: %#v", errorRecords)
	}
	if again := liveRepairTasks(t, cfg); len(again) != 1 {
		t.Fatalf("live repair tasks = %v, want exactly one", keysOf(again))
	}
}

// A broken orphan in working/ used to abort recoverOrphanClaims (startExistingClaim
// returned the YAML error) and stop every later claim.
func TestScanOnceClaimsAlongsideUnparseableWorkingDocument(t *testing.T) {
	root, cfg := newScratchWorkspace(t)
	writeScratchDocument(t, cfg.StatePath("tasks", "todo", "tasks-scratch-valid.md"), validScratchTaskFrontMatter)
	corrupt := cfg.StatePath("tasks", "working", "tasks-scratch-corrupt.md")
	writeScratchDocument(t, corrupt, corruptTaskFrontMatter)

	// Lease present on the broken working document: resumeInterruptedClaims must
	// soft-skip it instead of aborting the scan (State claiming + file present).
	now := time.Now().UTC()
	key := leaseID("tasks:"+phaseTaskImplementation, "tasks-scratch-corrupt")
	manager := New(cfg, newFakeRecipient(), extensions.Runner{Directory: filepath.Join(root, "missing")})
	var errorRecords []string
	manager.LogError = func(component, event, message string) {
		errorRecords = append(errorRecords, component+"|"+event+"|"+message)
	}
	if err := manager.saveLease(Lease{
		ID: key, ClaimID: key, DocumentType: "task", Route: "tasks",
		OwnerID: manager.ownerID, File: corrupt, SessionKey: "sess",
		State: "claiming", Phase: phaseTaskImplementation,
		ClaimAttempt: 1, StartedAt: now, HeartbeatAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatalf("ScanOnce aborted the board on one unreadable working document: %v", err)
	}
	manager.Wait()
	assertValidTaskClaimed(t, cfg)

	// The grace holds the first sighting; the backdated second scan files.
	backdateUnparseableSightings(t, manager)
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if body := readRepairTask(t, liveRepairTasks(t, cfg), corrupt); !strings.Contains(body, "tasks-scratch-corrupt.md") {
		t.Fatalf("repair task does not name the corrupt working document:\n%s", body)
	}
	if len(errorRecords) != 1 {
		t.Fatalf("error records = %#v, want one", errorRecords)
	}
}

// A broken active goal must cost only that goal: other task claims continue and
// exactly one repair task is filed (coverage already soft before this change).
func TestScanOnceClaimsAlongsideUnparseableActiveGoal(t *testing.T) {
	root, cfg := newScratchWorkspace(t)
	writeScratchDocument(t, cfg.StatePath("tasks", "todo", "tasks-scratch-valid.md"), validScratchTaskFrontMatter)
	corrupt := cfg.StatePath("goals", "active", "goals-scratch-corrupt.md")
	writeScratchDocument(t, corrupt, corruptGoalFrontMatter)

	manager := New(cfg, newFakeRecipient(), extensions.Runner{Directory: filepath.Join(root, "missing")})
	var errorRecords []string
	manager.LogError = func(component, event, message string) {
		errorRecords = append(errorRecords, component+"|"+event+"|"+message)
	}
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatalf("ScanOnce aborted the board on one unreadable active goal: %v", err)
	}
	manager.Wait()
	assertValidTaskClaimed(t, cfg)

	// The grace holds the first sighting; the backdated second scan files.
	backdateUnparseableSightings(t, manager)
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if body := readRepairTask(t, liveRepairTasks(t, cfg), corrupt); !strings.Contains(body, "this goal document's YAML front matter") {
		t.Fatalf("goal repair is not written as a goal repair:\n%s", body)
	}
	if len(errorRecords) != 1 {
		t.Fatalf("error records = %#v, want one", errorRecords)
	}
}

// A vanished document (e.g. deleted or moved between listing and reading, ENOENT)
// must not file a repair task and must log a skip message instead.
func TestReportUnparseableDocumentSkipsVanishedDocument(t *testing.T) {
	root, cfg := newScratchWorkspace(t)
	manager := New(cfg, newFakeRecipient(), extensions.Runner{Directory: filepath.Join(root, "missing")})

	var logLines []string
	manager.Log = func(line string) {
		logLines = append(logLines, line)
	}
	var errorRecords []string
	manager.LogError = func(component, event, message string) {
		errorRecords = append(errorRecords, component+"|"+event+"|"+message)
	}

	vanishedPath := cfg.StatePath("tasks", "todo", "tasks-scratch-vanished.md")
	_, readErr := os.ReadFile(vanishedPath)
	if readErr == nil {
		t.Fatal("expected ENOENT for non-existent file")
	}

	manager.reportUnparseableDocument(vanishedPath, readErr)

	repairs := liveRepairTasks(t, cfg)
	if len(repairs) != 0 {
		t.Fatalf("repair task was filed for vanished document: %v", keysOf(repairs))
	}
	if len(errorRecords) != 0 {
		t.Fatalf("unexpected error records: %v", errorRecords)
	}

	foundSkipLog := false
	for _, line := range logLines {
		if strings.Contains(line, "skipping vanished document") && strings.Contains(line, "tasks-scratch-vanished.md") {
			foundSkipLog = true
			break
		}
	}
	if !foundSkipLog {
		t.Fatalf("no log line recorded skip; log lines: %v", logLines)
	}
}

// The repair task's terminal notification is what reaches Joey when a repair
// cannot be made, so its notify block must be usable: enabled with an origin
// NotificationFromDocument accepts. Without an origin the notification was
// only logged as "prompt rejected".
func TestUnparseableRepairTaskNotifyIsUsable(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*config.Config)
		origin string
	}{
		{"no remote channel falls back to the local CLI origin", func(*config.Config) {}, "cli/local"},
		{"a single authorized Telegram user is the origin", func(cfg *config.Config) {
			cfg.Channels.Telegram.Enabled = true
			cfg.Channels.Telegram.AllowedUsers = []string{"1701167661"}
		}, "telegram/TG-1701167661"},
		{"a Telegram username cannot form an origin, so fall back", func(cfg *config.Config) {
			cfg.Channels.Telegram.Enabled = true
			cfg.Channels.Telegram.AllowedUsers = []string{"@someone"}
		}, "cli/local"},
		{"several authorized users are ambiguous, so fall back", func(cfg *config.Config) {
			cfg.Channels.Telegram.Enabled = true
			cfg.Channels.Telegram.AllowedUsers = []string{"1701167661", "42"}
		}, "cli/local"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, cfg := newScratchWorkspace(t)
			tc.mutate(&cfg)
			corrupt := cfg.StatePath("tasks", "todo", "tasks-scratch-corrupt.md")
			manager := New(cfg, newFakeRecipient(), extensions.Runner{Directory: t.TempDir()})
			filed, err := manager.fileUnparseableRepairTask(corrupt, errors.New("yaml: line 9: did not find expected key"))
			if err != nil || !filed {
				t.Fatalf("repair task not filed: %v, %v", filed, err)
			}
			data, err := os.ReadFile(cfg.StatePath("tasks", "todo", unparseableRepairID(corrupt)+".md"))
			if err != nil {
				t.Fatal(err)
			}
			document, err := ParseDocument(data)
			if err != nil {
				t.Fatal(err)
			}
			policy, err := NotificationFromDocument(document)
			if err != nil {
				t.Fatalf("repair task notify is unusable: %v\n%s", err, data)
			}
			if !policy.Enabled || !policy.Outcomes["failed"] || !policy.Outcomes["done"] || !policy.Outcomes["cancelled"] {
				t.Fatalf("repair task notify policy = %#v", policy)
			}
			if got := policy.Origin.Channel + "/" + policy.Origin.Conversation; got != tc.origin {
				t.Fatalf("repair task notify origin = %q, want %q", got, tc.origin)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Repair grace (2026-10-19 token-cut audit): about half of the sampled repair
// tasks were no-ops - the document parsed again by the time a worker opened
// it. A repair task now files only after the document has stayed unparseable
// across unparseableGracePeriod, and never while it parses on re-read.

// backdateUnparseableSightings pushes every recorded first sighting back two
// grace periods, so the next scan treats the breakages as persistent.
func backdateUnparseableSightings(t *testing.T, manager *Manager) {
	t.Helper()
	unparseableSightingsMu.Lock()
	defer unparseableSightingsMu.Unlock()
	sightings := manager.loadUnparseableSightings()
	for path, seen := range sightings {
		first, err := time.Parse(time.RFC3339, seen)
		if err != nil {
			continue
		}
		sightings[path] = first.Add(-2 * unparseableGracePeriod).UTC().Format(time.RFC3339)
	}
	manager.saveUnparseableSightings(sightings)
}

func TestRepairTaskWaitsOutTheGracePeriod(t *testing.T) {
	root, cfg := newScratchWorkspace(t)
	start := time.Date(2026, 10, 19, 10, 0, 0, 0, time.UTC)
	now := start
	manager := New(cfg, newFakeRecipient(), extensions.Runner{Directory: filepath.Join(root, "missing")})
	manager.unparseableNow = func() time.Time { return now }
	var errorRecords []string
	manager.LogError = func(component, event, message string) {
		errorRecords = append(errorRecords, component+"|"+event+"|"+message)
	}

	corrupt := cfg.StatePath("tasks", "todo", "tasks-scratch-corrupt.md")
	writeScratchDocument(t, corrupt, corruptTaskFrontMatter)

	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if repairs := liveRepairTasks(t, cfg); len(repairs) != 0 {
		t.Fatalf("first sighting filed repair tasks: %v", keysOf(repairs))
	}
	if len(errorRecords) != 0 {
		t.Fatalf("the grace period wrote an error record: %v", errorRecords)
	}
	unparseableSightingsMu.Lock()
	_, seen := manager.loadUnparseableSightings()[corrupt]
	unparseableSightingsMu.Unlock()
	if !seen {
		t.Fatal("first sighting was not recorded")
	}

	now = start.Add(unparseableGracePeriod + time.Minute)
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	repairs := liveRepairTasks(t, cfg)
	if len(repairs) != 1 {
		t.Fatalf("persistent breakage filed %d repair tasks, want 1: %v", len(repairs), keysOf(repairs))
	}
	if len(errorRecords) != 1 || !strings.Contains(errorRecords[0], "|unparseable_document|") {
		t.Fatalf("error records = %#v, want one on the filing scan", errorRecords)
	}
	unparseableSightingsMu.Lock()
	_, stillSeen := manager.loadUnparseableSightings()[corrupt]
	unparseableSightingsMu.Unlock()
	if stillSeen {
		t.Fatal("sighting was not cleared once the repair task filed")
	}
}

func TestRepairSkipsWhenTheDocumentParsesAgain(t *testing.T) {
	root, cfg := newScratchWorkspace(t)
	start := time.Date(2026, 10, 19, 10, 0, 0, 0, time.UTC)
	now := start
	manager := New(cfg, newFakeRecipient(), extensions.Runner{Directory: filepath.Join(root, "missing")})
	manager.unparseableNow = func() time.Time { return now }

	corrupt := cfg.StatePath("tasks", "todo", "tasks-scratch-corrupt.md")
	writeScratchDocument(t, corrupt, corruptTaskFrontMatter)

	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()

	// The document is repaired (rewritten validly) before the grace lapses.
	writeScratchDocument(t, corrupt, validScratchTaskFrontMatter)
	now = start.Add(2 * unparseableGracePeriod)
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if repairs := liveRepairTasks(t, cfg); len(repairs) != 0 {
		t.Fatalf("a repair task filed for a document that parses again: %v", keysOf(repairs))
	}
	unparseableSightingsMu.Lock()
	_, seen := manager.loadUnparseableSightings()[corrupt]
	unparseableSightingsMu.Unlock()
	if seen {
		t.Fatal("a healed document kept its unparseable sighting")
	}
}
