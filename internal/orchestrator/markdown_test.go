package orchestrator

import (
	"reflect"
	"testing"
	"time"
)

func TestParseDocumentSanitizesUnquotedColons(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantKey string
		wantSub string
		wantVal string
	}{
		{
			name: "waiting_for with internal colon",
			input: `---
status: waiting
waiting_for: Joey's picks (questions 1-4: picks stand vs changes)
---
Body text
`,
			wantKey: "waiting_for",
			wantVal: "Joey's picks (questions 1-4: picks stand vs changes)",
		},
		{
			name: "nested completion_summary outcome with Progress colon",
			input: `---
status: done
completion_summary:
  verdict: completed
  outcome: Progress: completed migration
---
Body text
`,
			wantKey: "completion_summary",
			wantSub: "outcome",
			wantVal: "Progress: completed migration",
		},
		{
			name: "evidence with timestamp colon",
			input: `---
status: done
evidence: 00:05Z: dsh-web tested
---
Body text
`,
			wantKey: "evidence",
			wantVal: "00:05Z: dsh-web tested",
		},
		{
			name: "title with multiple colons",
			input: `---
title: Fix: broken: really broken
status: todo
---
Body text
`,
			wantKey: "title",
			wantVal: "Fix: broken: really broken",
		},
		{
			name: "step ending with colon",
			input: `---
step: Note:
status: working
---
Body text
`,
			wantKey: "step",
			wantVal: "Note:",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			doc, err := ParseDocument([]byte(tc.input))
			if err != nil {
				t.Fatalf("ParseDocument failed: %v", err)
			}
			if tc.wantSub == "" {
				got := doc.FrontMatter[tc.wantKey]
				if !reflect.DeepEqual(got, tc.wantVal) {
					t.Fatalf("got %q = %v (%T), want %q", tc.wantKey, got, got, tc.wantVal)
				}
			} else {
				subMap, ok := doc.FrontMatter[tc.wantKey].(map[string]any)
				if !ok {
					t.Fatalf("expected map[string]any for %q, got %#v", tc.wantKey, doc.FrontMatter[tc.wantKey])
				}
				got := subMap[tc.wantSub]
				if !reflect.DeepEqual(got, tc.wantVal) {
					t.Fatalf("got %s.%s = %v, want %q", tc.wantKey, tc.wantSub, got, tc.wantVal)
				}
			}
			if doc.Body != "Body text\n" {
				t.Fatalf("got body %q, want %q", doc.Body, "Body text\n")
			}
		})
	}
}

func TestParseDocumentNormalYAMLUnchanged(t *testing.T) {
	input := `---
id: test-task-01
status: todo
round_task_ids:
  - task-a
  - task-b
review_required: true
notify:
  enabled: true
  origin: telegram/TG-123
  on: [done, waiting]
already_quoted: "this has: colons: but is quoted"
block_scalar: |
  hello: world
---
# Some Title
Markdown content here
`
	doc, err := ParseDocument([]byte(input))
	if err != nil {
		t.Fatalf("ParseDocument failed: %v", err)
	}
	if doc.FrontMatter["id"] != "test-task-01" {
		t.Fatalf("unexpected id: %v", doc.FrontMatter["id"])
	}
	if doc.FrontMatter["review_required"] != true {
		t.Fatalf("unexpected review_required: %v", doc.FrontMatter["review_required"])
	}
	if doc.FrontMatter["already_quoted"] != "this has: colons: but is quoted" {
		t.Fatalf("unexpected already_quoted: %v", doc.FrontMatter["already_quoted"])
	}
}

func TestMalformedScheduleIsNotDue(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	for _, field := range []string{"not_before", "next_dispatch_at"} {
		for _, value := range []string{"2099-01-01T09:00:00", "tomorrow"} {
			document := Document{FrontMatter: map[string]any{field: value}}
			due, err := documentDue(document, now, phaseTaskImplementation)
			if due || err == nil {
				t.Fatalf("%s=%q: due=%v err=%v, want not due with an error", field, value, due, err)
			}
		}
	}
	// A well-formed past date and a blank value stay due.
	for _, value := range []string{"2020-01-01T09:00:00Z", ""} {
		document := Document{FrontMatter: map[string]any{"not_before": value}}
		if due, err := documentDue(document, now, phaseTaskImplementation); !due || err != nil {
			t.Fatalf("not_before=%q: due=%v err=%v, want due", value, due, err)
		}
	}
}
