package orchestrator

import (
	"path/filepath"
	"reflect"
	"testing"
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

// A block scalar's content is data, not YAML: when the retry sanitizer fixes
// another key it must leave the scalar's lines byte-for-byte as written.
func TestParseDocumentRetryLeavesBlockScalarContentAlone(t *testing.T) {
	input := `---
id: test-task-02
title: Block scalar survives the retry
notes: |
  Result: 3 passed: 0 failed
  - step: done
folded: >-
  Outcome: fine: really

kept: |2
    Indented: text
waiting_for: Joey's decision (questions 1-4: picks stand)
status: waiting
---
body
`
	doc, err := ParseDocument([]byte(input))
	if err != nil {
		t.Fatalf("ParseDocument failed: %v", err)
	}
	if got, want := doc.FrontMatter["notes"], "Result: 3 passed: 0 failed\n- step: done\n"; got != want {
		t.Fatalf("notes = %q, want %q", got, want)
	}
	if got, want := doc.FrontMatter["folded"], "Outcome: fine: really"; got != want {
		t.Fatalf("folded = %q, want %q", got, want)
	}
	if got, want := doc.FrontMatter["kept"], "  Indented: text\n"; got != want {
		t.Fatalf("kept = %q, want %q", got, want)
	}
	if got, want := doc.FrontMatter["waiting_for"], "Joey's decision (questions 1-4: picks stand)"; got != want {
		t.Fatalf("waiting_for = %q, want %q (the genuinely broken key must still be repaired)", got, want)
	}
	if doc.FrontMatter["status"] != "waiting" {
		t.Fatalf("status = %v", doc.FrontMatter["status"])
	}

	// The retry result is what the next WriteDocument persists.
	path := filepath.Join(t.TempDir(), "task.md")
	if err := WriteDocument(path, doc); err != nil {
		t.Fatalf("WriteDocument failed: %v", err)
	}
	again, err := ReadDocument(path)
	if err != nil {
		t.Fatalf("ReadDocument after write failed: %v", err)
	}
	if got, want := again.FrontMatter["notes"], "Result: 3 passed: 0 failed\n- step: done\n"; got != want {
		t.Fatalf("persisted notes = %q, want %q", got, want)
	}
}

func TestSanitizeFrontMatterBlockScalarBoundaries(t *testing.T) {
	front := "completion_summary:\n  evidence: |\n    Result: 3 passed: 0 failed\n    # not a comment\n\n    more: lines\n  outcome: Progress: done\nitems:\n  - note: >\n      a: b\n    owner: Joey: yes\nlast: x: y"
	want := "completion_summary:\n  evidence: |\n    Result: 3 passed: 0 failed\n    # not a comment\n\n    more: lines\n  outcome: \"Progress: done\"\nitems:\n  - note: >\n      a: b\n    owner: \"Joey: yes\"\nlast: \"x: y\""
	if got := sanitizeFrontMatter(front); got != want {
		t.Fatalf("sanitizeFrontMatter =\n%s\nwant\n%s", got, want)
	}
}
