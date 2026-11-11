package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/harness"
	"github.com/agent0ai/spynel/internal/workspace"
	"gopkg.in/yaml.v3"
)

// writeStagingRegistryFixture writes a minimal retention registry: one covered tree and
// one staging root, both under the temp dir.
func writeStagingRegistryFixture(t *testing.T, root string) string {
	t.Helper()
	registryPath := filepath.Join(root, "retention-registry.yaml")
	data, err := yaml.Marshal(retentionRegistryFile{
		Entries: []retentionRegistryEntry{{
			Path:  filepath.Join(root, "covered"),
			Owner: "test owner",
			Rule:  "keep-forever",
		}},
		StagingRoots: []string{filepath.Join(root, "staging")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(registryPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WERKRBY_RETENTION_REGISTRY", registryPath)
	return registryPath
}

func TestParseStagingFrontMatter(t *testing.T) {
	missing := Document{FrontMatter: map[string]any{}}
	if entries, err := parseStagingFrontMatter(missing); err != nil || entries != nil {
		t.Fatalf("no staging key: entries=%v err=%v", entries, err)
	}
	valid := Document{FrontMatter: map[string]any{"staging": []any{
		map[string]any{"path": "/tmp/a", "delete_when": "2026-11-14"},
		map[string]any{"path": "/tmp/b"},
	}}}
	entries, err := parseStagingFrontMatter(valid)
	if err != nil || len(entries) != 2 || entries[0].Path != "/tmp/a" || entries[1].DeleteWhen != "" {
		t.Fatalf("valid list parsed wrong: %+v %v", entries, err)
	}
	notList := Document{FrontMatter: map[string]any{"staging": "/tmp/a"}}
	if _, err := parseStagingFrontMatter(notList); err == nil || !strings.Contains(err.Error(), "must be a YAML list") {
		t.Fatalf("scalar staging: err=%v", err)
	}
	noPath := Document{FrontMatter: map[string]any{"staging": []any{map[string]any{"delete_when": "x"}}}}
	if _, err := parseStagingFrontMatter(noPath); err == nil || !strings.Contains(err.Error(), "missing its path") {
		t.Fatalf("entry without path: err=%v", err)
	}
}

func TestRetentionRegistryCovers(t *testing.T) {
	entries := []retentionRegistryEntry{{Path: "/data/pile"}, {Path: "/data/glob-*"}}
	for _, target := range []string{"/data/pile", "/data/pile/child", "/data/glob-xyz"} {
		if !retentionRegistryCovers(entries, target) {
			t.Fatalf("%s should be covered", target)
		}
	}
	for _, target := range []string{"/data/pile-other", "/data/globs", "/data"} {
		if retentionRegistryCovers(entries, target) {
			t.Fatalf("%s should not be covered", target)
		}
	}
}

func TestValidateStagingCleanup(t *testing.T) {
	root := t.TempDir()
	writeStagingRegistryFixture(t, root)
	covered := filepath.Join(root, "covered")
	uncovered := filepath.Join(root, "uncovered")
	if err := os.MkdirAll(covered, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(uncovered, 0o700); err != nil {
		t.Fatal(err)
	}
	docWith := func(paths ...string) Document {
		list := make([]any, 0, len(paths))
		for _, p := range paths {
			list = append(list, map[string]any{"path": p})
		}
		return Document{FrontMatter: map[string]any{"staging": list}}
	}
	if err := validateStagingCleanup(Document{FrontMatter: map[string]any{}}); err != nil {
		t.Fatalf("no staging key must pass: %v", err)
	}
	if err := validateStagingCleanup(docWith(filepath.Join(root, "gone"))); err != nil {
		t.Fatalf("missing staging path must pass: %v", err)
	}
	if err := validateStagingCleanup(docWith(covered)); err != nil {
		t.Fatalf("registry-covered staging must pass: %v", err)
	}
	err := validateStagingCleanup(docWith(uncovered))
	if err == nil || !strings.Contains(err.Error(), "no retention-registry entry covers it") {
		t.Fatalf("uncovered staging must be refused, got: %v", err)
	}
	if !strings.Contains(err.Error(), uncovered) {
		t.Fatalf("refusal must name the path, got: %v", err)
	}
	// Unreadable registry fails closed and says so.
	t.Setenv("WERKRBY_RETENTION_REGISTRY", filepath.Join(root, "missing-registry.yaml"))
	err = validateStagingCleanup(docWith(covered))
	if err == nil || !strings.Contains(err.Error(), "unreadable") {
		t.Fatalf("unreadable registry must refuse with its reason, got: %v", err)
	}
}

// stagingGateFixture is a workspace whose task document sits in working/ with a staging
// list and a valid direct-completion summary.
func stagingGateFixture(t *testing.T, stagingPath string) (config.Config, *Manager, string) {
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
	writeStagingRegistryFixture(t, root)
	herdr, err := harness.NewHerdr(harness.HarnessConfig{
		Command: filepath.Join(root, "no-such-herdr"), Cwd: root,
		Sandbox: "danger-full-access", ApprovalPolicy: "never",
	})
	if err != nil {
		t.Fatal(err)
	}
	manager := New(cfg, herdr, extensions.Runner{Directory: filepath.Join(root, "missing")})
	logs := &logSink{}
	manager.Log = logs.log

	now := time.Now().UTC().Format(time.RFC3339)
	name := "staging-gate-probe.md"
	front := map[string]any{
		"id": "tasks-staging-gate-probe", "title": "staging gate probe",
		"status": "working", "created_at": now, "updated_at": now,
		"review_required": false, "attempt": 1,
		"staging":       []any{map[string]any{"path": stagingPath, "delete_when": "2026-11-14"}},
		"completion_summary": map[string]any{
			"verdict": "completed", "outcome": "probe", "evidence": "probe evidence",
			"uncertainty": "none", "completed_at": now,
		},
	}
	base := filepath.Dir(cfg.Resolve(workflowRoutes()[0].Source))
	doc := Document{FrontMatter: front, Body: "probe"}
	if err := WriteDocument(filepath.Join(base, "working", name), doc); err != nil {
		t.Fatal(err)
	}
	return cfg, manager, name
}

func TestStagingGateRefusesDoneUntilPathClears(t *testing.T) {
	root := t.TempDir()
	staging := filepath.Join(root, "staging", "pile")
	if err := os.MkdirAll(staging, 0o700); err != nil {
		t.Fatal(err)
	}
	cfg, manager, name := stagingGateFixture(t, staging)
	route, ok := routeByName("tasks")
	if !ok {
		t.Fatal("no tasks route")
	}
	base := filepath.Dir(cfg.Resolve(route.Source))
	lease := Lease{ID: "lease-probe", Route: "tasks", Phase: phaseTaskImplementation,
		File: filepath.Join(base, "working", name)}

	status, _, err := manager.reconcileTaskTransition(context.Background(), route, lease, phaseTaskImplementation, "done", lease.File)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if status != "todo" {
		t.Fatalf("staging still present must bounce done -> todo, got %q", status)
	}
	if _, err := os.Stat(filepath.Join(base, "todo", name)); err != nil {
		t.Fatalf("document must sit in todo/: %v", err)
	}
	if !strings.Contains(readNote(filepath.Join(base, "todo", name)), "staging") {
		t.Fatalf("the bounce note must name the staging rule")
	}

	// Once the staged path is gone the same document completes: the worker's move
	// into done/ is what the manager validates, so simulate it exactly.
	if err := os.RemoveAll(staging); err != nil {
		t.Fatal(err)
	}
	donePath := filepath.Join(base, "done", name)
	if err := os.Rename(filepath.Join(base, "todo", name), donePath); err != nil {
		t.Fatal(err)
	}
	status, _, err = manager.reconcileTaskTransition(context.Background(), route, lease, phaseTaskImplementation, "done", donePath)
	if err != nil {
		t.Fatalf("reconcile after cleanup: %v", err)
	}
	if status != "done" {
		t.Fatalf("cleared staging must complete, got status=%q", status)
	}
	if _, err := os.Stat(donePath); err != nil {
		t.Fatalf("accepted document must stay in done/: %v", err)
	}
}

func readNote(path string) string {
	document, err := ReadDocument(path)
	if err != nil {
		return ""
	}
	return document.Body
}

func TestCreateDefaultsStagingFromBody(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	writeStagingRegistryFixture(t, root)
	staging := filepath.Join(root, "staging", "takeout-zips")
	body := "Stage the export under `" + staging + "` then import it."
	path, err := CreateWithOptions(cfg, "tasks", "staging default probe", body, CreateOptions{NoReview: true})
	if err != nil {
		t.Fatal(err)
	}
	document, err := ReadDocument(path)
	if err != nil {
		t.Fatal(err)
	}
	raw, ok := document.FrontMatter["staging"].([]any)
	if !ok || len(raw) != 1 {
		t.Fatalf("creation must default staging from the brief, got: %#v", document.FrontMatter["staging"])
	}
	entry, _ := raw[0].(map[string]any)
	if entry["path"] != staging {
		t.Fatalf("staged path = %v, want %v", entry["path"], staging)
	}

	cleanPath, err := CreateWithOptions(cfg, "tasks", "no staging probe", "Nothing staged here.", CreateOptions{NoReview: true})
	if err != nil {
		t.Fatal(err)
	}
	clean, err := ReadDocument(cleanPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, present := clean.FrontMatter["staging"]; present {
		t.Fatalf("a brief naming no staging path must not carry staging front matter")
	}
}
