package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// An unquoted RFC 3339 wake_at is decoded by yaml.v3 as time.Time, not string.
// The park checks must treat it exactly like the quoted form, as scheduledWake does.
func parseWakeAtDocument(t *testing.T, wakeAtLine string) Document {
	t.Helper()
	document, err := ParseDocument([]byte("---\nid: t1\nstatus: waiting\n" + wakeAtLine + "\n---\n# body\n"))
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}
	return document
}

func TestBH_UnquotedWakeAtIsRoutable(t *testing.T) {
	now := time.Date(2026, time.October, 13, 23, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		line string
		want bool
	}{
		{"unquoted future", "wake_at: 2026-10-13T23:27:27Z", true},
		{"quoted future", `wake_at: "2026-10-13T23:27:27Z"`, true},
		{"unquoted past", "wake_at: 2026-10-13T22:00:00Z", false},
		{"quoted past", `wake_at: "2026-10-13T22:00:00Z"`, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document := parseWakeAtDocument(t, test.line)
			if got := hasRoutableWaitingPath(document, now); got != test.want {
				t.Fatalf("hasRoutableWaitingPath(%s) = %v, want %v (wake_at decoded as %T)", test.line, got, test.want, document.FrontMatter["wake_at"])
			}
		})
	}
}

func TestBH_UnquotedWakeAtNotificationDecision(t *testing.T) {
	now := time.Date(2026, time.October, 13, 23, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		line string
		want bool
	}{
		{"unquoted future", "wake_at: 2026-10-13T23:27:27Z", false},
		{"quoted future", `wake_at: "2026-10-13T23:27:27Z"`, false},
		{"unquoted past", "wake_at: 2026-10-13T22:00:00Z", true},
		{"quoted past", `wake_at: "2026-10-13T22:00:00Z"`, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document := parseWakeAtDocument(t, test.line)
			if got := requiresTaskNotificationDecision(document, "waiting", now); got != test.want {
				t.Fatalf("requiresTaskNotificationDecision(%s) = %v, want %v (wake_at decoded as %T)", test.line, got, test.want, document.FrontMatter["wake_at"])
			}
		})
	}
}

// End to end through the manager: an agent parks a task with an unquoted future
// wake_at (as written by hand or by a shell script). The park must be accepted
// into waiting/, not bounced to todo/ with a resume credit.
func TestBH_UnquotedWakeAtParkIsAccepted(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	route := workflowRoutes()[0]
	task, err := Create(cfg, "tasks", "task park with unquoted wake_at", "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(task)
	base := filepath.Dir(cfg.Resolve(route.Source))
	wake := time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339)
	var once sync.Once
	fake.beforeEmit = func() {
		once.Do(func() {
			working := filepath.Join(base, "working", name)
			editFrontMatter(t, working, func(fm map[string]any) {
				fm["waiting_for"] = "unquoted wake_at"
				fm["wake_at"] = wake
			})
			data, err := os.ReadFile(working)
			if err != nil {
				t.Errorf("read: %v", err)
				return
			}
			unquoted := strings.Replace(string(data), `wake_at: "`+wake+`"`, "wake_at: "+wake, 1)
			if unquoted == string(data) {
				t.Errorf("wake_at line was not unquoted in:\n%s", data)
				return
			}
			if err := os.WriteFile(working, []byte(unquoted), 0o644); err != nil {
				t.Errorf("write: %v", err)
				return
			}
			if err := moveDocument(working, filepath.Join(base, "waiting", name), "waiting", time.Now().UTC()); err != nil {
				t.Errorf("park: %v", err)
			}
		})
	}
	scanAndWait(t, manager)
	if err := manager.reconcileTransitions(context.Background()); err != nil {
		t.Fatal(err)
	}
	doc, err := ReadDocument(filepath.Join(base, "waiting", name))
	if err != nil {
		t.Fatalf("task with unquoted future wake_at was not accepted into waiting/: %v", err)
	}
	if strings.Contains(doc.Body, "Park rejected") {
		t.Fatalf("park was rejected: %s", doc.Body)
	}
}
