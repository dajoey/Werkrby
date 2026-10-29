package orchestrator

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A task or goal document claimed without a `title` front-matter key drew as an
// unlabelled row on every board surface (Helm finding 2026-10-24: four live
// documents). The claim writer is the one choke point every dispatched document
// passes through, so it backfills a missing or blank title from the body's
// first heading, falling back to the filename stem, and records one Progress
// note saying it did.
func TestClaimBackfillsMissingTitle(t *testing.T) {
	cases := []struct {
		name      string
		file      string
		front     map[string]any
		body      string
		wantTitle string
	}{
		{
			name:      "no title, heading present",
			file:      "tasks-20261024-example-01.md",
			front:     map[string]any{"id": "tasks-20261024-example-01", "status": "todo"},
			body:      "# Repair the night backup chain\n\n## Objective\n",
			wantTitle: "Repair the night backup chain",
		},
		{
			name:      "blank title counts as missing",
			file:      "tasks-20261024-example-02.md",
			front:     map[string]any{"id": "tasks-20261024-example-02", "status": "todo", "title": "   "},
			body:      "#  Trim and collapse  spacing \n",
			wantTitle: "Trim and collapse spacing",
		},
		{
			name:      "no title and no heading falls back to filename",
			file:      "tasks-20261024-example-03.md",
			front:     map[string]any{"id": "tasks-20261024-example-03", "status": "todo"},
			body:      "## Objective\n\nNo top heading here.\n",
			wantTitle: "tasks-20261024-example-03",
		},
		{
			name:      "overlong heading is bounded",
			file:      "tasks-20261024-example-04.md",
			front:     map[string]any{"id": "tasks-20261024-example-04", "status": "todo"},
			body:      "# " + strings.Repeat("word ", 100) + "\n",
			wantTitle: boundedWorkflowText(strings.Repeat("word ", 100), 240),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			directory := t.TempDir()
			source := filepath.Join(directory, "todo", tc.file)
			target := filepath.Join(directory, "working", tc.file)
			if err := os.MkdirAll(filepath.Dir(source), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := WriteDocument(source, Document{FrontMatter: tc.front, Body: tc.body}); err != nil {
				t.Fatal(err)
			}
			claimed, err := ClaimDocument(source, target, "working", time.Date(2026, 10, 24, 10, 0, 0, 0, time.UTC))
			if err != nil {
				t.Fatal(err)
			}
			if got := claimed.FrontMatter["title"]; got != tc.wantTitle {
				t.Fatalf("claimed title = %#v, want %#v", got, tc.wantTitle)
			}
			stored, err := ReadDocument(target)
			if err != nil {
				t.Fatal(err)
			}
			if got := stored.FrontMatter["title"]; got != tc.wantTitle {
				t.Fatalf("stored title = %#v, want %#v", got, tc.wantTitle)
			}
			if !strings.Contains(stored.Body, "title backfilled") {
				t.Fatalf("claimed body lacks the backfill Progress note:\n%s", stored.Body)
			}
		})
	}
}

func TestClaimKeepsExistingTitle(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "todo", "task.md")
	target := filepath.Join(directory, "working", "task.md")
	if err := os.MkdirAll(filepath.Dir(source), 0o700); err != nil {
		t.Fatal(err)
	}
	document := Document{FrontMatter: map[string]any{
		"id": "task", "status": "todo", "title": "A heading that must survive",
	}, Body: "# A different heading\n"}
	if err := WriteDocument(source, document); err != nil {
		t.Fatal(err)
	}
	claimed, err := ClaimDocument(source, target, "working", time.Date(2026, 10, 24, 10, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if got := claimed.FrontMatter["title"]; got != "A heading that must survive" {
		t.Fatalf("claimed title = %#v, want the author's title", got)
	}
	if strings.Contains(claimed.Body, "title backfilled") {
		t.Fatalf("backfill note written although the document had a title:\n%s", claimed.Body)
	}
}
