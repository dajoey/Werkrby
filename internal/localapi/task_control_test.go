package localapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/app"
	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/harness"
	"github.com/agent0ai/spynel/internal/orchestrator"
	"github.com/agent0ai/spynel/internal/workspace"
)

// taskControlFixtureHarness is the smallest harness a supervisor can start
// with; none of these tests reach it.
type taskControlFixtureHarness struct{}

func (h *taskControlFixtureHarness) Start(context.Context) error { return nil }
func (h *taskControlFixtureHarness) Close() error                { return nil }
func (h *taskControlFixtureHarness) Models(context.Context) ([]harness.Model, error) {
	return []harness.Model{{ID: "fixture", DisplayName: "Fixture"}}, nil
}
func (h *taskControlFixtureHarness) Send(_ context.Context, _, prompt string, _ core.Emit) (string, bool, error) {
	return "thread-fixture", false, nil
}
func (h *taskControlFixtureHarness) Interrupt(context.Context, string) (bool, error) {
	return false, nil
}
func (h *taskControlFixtureHarness) IsActive(string) bool      { return false }
func (h *taskControlFixtureHarness) ThreadID(string) string    { return "" }
func (h *taskControlFixtureHarness) ResetSession(string) error { return nil }

func newTaskControlServer(t *testing.T) (*Server, string, *httptest.Server) {
	t.Helper()
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	registry := harness.NewRegistry()
	registry.Register("fixture", func(harness.HarnessConfig) (harness.Harness, error) { return &taskControlFixtureHarness{}, nil })
	supervisor := harness.NewSupervisor(registry, harness.HarnessConfig{Name: "fixture"})
	if err := supervisor.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = supervisor.Close() })
	service := app.New(cfg, supervisor)
	token := "task-control-test-token"
	server := &Server{Service: service, Token: token}
	ts := httptest.NewServer(server.routes())
	t.Cleanup(ts.Close)
	return server, token, ts
}

func TestTaskControlRouteIsBearerGuarded(t *testing.T) {
	_, token, ts := newTaskControlServer(t)

	body, _ := json.Marshal(map[string]any{"task_id": "tasks-x", "action": "output"})
	for _, header := range []string{"", "Bearer wrong-token", "Bearer " + token} {
		req, _ := http.NewRequest("POST", ts.URL+"/v1/task-control", bytes.NewReader(body))
		if header != "" {
			req.Header.Set("Authorization", header)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		want := http.StatusUnauthorized
		if header == "Bearer "+token {
			want = http.StatusNotFound // valid token, unknown task id
		}
		if resp.StatusCode != want {
			t.Fatalf("auth %q status = %d, want %d", header, resp.StatusCode, want)
		}
	}
}

func TestTaskControlRouteReportsCodedErrorsAsJSON(t *testing.T) {
	_, token, ts := newTaskControlServer(t)

	for _, raw := range []string{"not json", `{"task_id":"tasks-x","action":"message","text":123}`} {
		req, _ := http.NewRequest("POST", ts.URL+"/v1/task-control", strings.NewReader(raw))
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("raw %q status = %d", raw, resp.StatusCode)
		}
	}

	req, _ := http.NewRequest("POST", ts.URL+"/v1/task-control", strings.NewReader(`{"task_id":"tasks-missing","action":"output"}`))
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown task status = %d", resp.StatusCode)
	}
	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if payload["code"] != "not_found" {
		t.Fatalf("coded payload = %#v", payload)
	}
}

func TestTaskControlRouteResolvesAClaimedTask(t *testing.T) {
	server, token, ts := newTaskControlServer(t)
	service := server.Service

	taskID := "tasks-20260925-control-route"
	path := filepath.Join(service.Config.StatePath("tasks", "working"), taskID+".md")
	document := orchestrator.Document{FrontMatter: map[string]any{
		"id": taskID, "title": "Route fixture", "status": "working", "review_required": true, "attempt": 1,
	}, Body: "# Task\n\n## Progress\n"}
	if err := orchestrator.WriteDocument(path, document); err != nil {
		t.Fatal(err)
	}
	lease := orchestrator.Lease{
		ID: "lease-route", OwnerID: "owner", SessionKey: "orchestrator:tasks:task_implementation:" + taskID + ":1",
		File: path, Route: "tasks", State: "processing", Phase: "task_implementation",
		StartedAt: time.Now().UTC(), HeartbeatAt: time.Now().UTC(),
	}
	directory := service.Config.StatePath("runtime", "leases")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	leaseData, _ := json.Marshal(lease)
	if err := os.WriteFile(filepath.Join(directory, lease.ID+".json"), leaseData, 0o600); err != nil {
		t.Fatal(err)
	}
	jobID := service.Runtime.BeginJobWithDetails(lease.SessionKey, "orchestrator", "markdown", taskID+".md", app.JobDetails{Kind: "task", Route: "tasks", DurableFile: path})
	service.Runtime.RecordJobEvent(jobID, core.Event{Kind: core.EventStatus, Text: "route captured output", Done: true})

	req, _ := http.NewRequest("POST", ts.URL+"/v1/task-control", strings.NewReader(`{"task_id":"`+taskID+`","action":"output"}`))
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("output status = %d", resp.StatusCode)
	}
	var payload struct {
		OK     bool   `json:"ok"`
		Action string `json:"action"`
		Output string `json:"output"`
		State  string `json:"state"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if !payload.OK || payload.Action != "output" || !strings.Contains(payload.Output, "route captured output") {
		t.Fatalf("payload = %#v", payload)
	}
}

func TestTaskControlRouteDecodesStaffAndWakeAtFields(t *testing.T) {
	server, token, ts := newTaskControlServer(t)
	service := server.Service

	taskID := "tasks-20260925-control-route-reassign"
	path := filepath.Join(service.Config.StatePath("tasks", "waiting"), taskID+".md")
	document := orchestrator.Document{FrontMatter: map[string]any{
		"id": taskID, "title": "Waiting fixture", "status": "waiting", "review_required": false, "attempt": 1,
		"wake_at": "2026-09-25T12:00:00Z",
	}, Body: "# Task\n\n## Progress\n"}
	if err := orchestrator.WriteDocument(path, document); err != nil {
		t.Fatal(err)
	}

	// Test reassign with staff field
	req, _ := http.NewRequest("POST", ts.URL+"/v1/task-control", strings.NewReader(`{"task_id":"`+taskID+`","action":"reassign","staff":"pi"}`))
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("reassign status = %d", resp.StatusCode)
	}

	// Test snooze with wake_at field
	future := time.Now().UTC().Add(2 * time.Hour).Format(time.RFC3339)
	req2, _ := http.NewRequest("POST", ts.URL+"/v1/task-control", strings.NewReader(`{"task_id":"`+taskID+`","action":"snooze","wake_at":"`+future+`"}`))
	req2.Header.Set("Authorization", "Bearer "+token)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("snooze status = %d", resp2.StatusCode)
	}
}

func TestTaskControlRouteReassignKeepsTaskParseable(t *testing.T) {
	server, token, ts := newTaskControlServer(t)
	service := server.Service

	taskID := "tasks-20260925-control-route-reassign-yaml"
	path := filepath.Join(service.Config.StatePath("tasks", "waiting"), taskID+".md")
	document := orchestrator.Document{FrontMatter: map[string]any{
		"id": taskID, "title": "Waiting fixture", "status": "waiting", "review_required": false, "attempt": 1,
		"wake_at": "2026-09-25T12:00:00Z",
	}, Body: "# Task\n\n## Progress\n"}
	if err := orchestrator.WriteDocument(path, document); err != nil {
		t.Fatal(err)
	}
	post := func(staff string) int {
		body := `{"task_id":"` + taskID + `","action":"reassign","staff":` + strconv.Quote(staff) + `}`
		req, _ := http.NewRequest("POST", ts.URL+"/v1/task-control", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	if code := post("x\nstatus: done"); code != http.StatusBadRequest {
		t.Errorf("injection reassign status = %d, want 400", code)
	}
	if code := post("Joey #2 backup"); code != http.StatusOK {
		t.Fatalf("reassign status = %d, want 200", code)
	}
	doc, err := orchestrator.ReadDocument(path)
	if err != nil {
		t.Fatalf("task unparseable after reassign: %v", err)
	}
	if got := doc.FrontMatter["staff"]; got != "Joey #2 backup" {
		t.Errorf("staff = %#v, want %q", got, "Joey #2 backup")
	}
	if got := doc.FrontMatter["status"]; got != "waiting" {
		t.Errorf("status = %#v, want waiting", got)
	}
}
