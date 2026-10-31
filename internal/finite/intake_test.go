package finite

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFiniteIntakeStrictBriefAndIsolation(t *testing.T) {
	valid := []byte("---\nid: task-intake\ntitle: Parse one brief\nrisk: medium\nreview_required: true\nmax_runs: 8\nmax_failures: 2\nmax_review_cycles: 4\n---\n\n# Objective\n\nImplement exactly one change.\n")
	brief, err := ParseBrief(valid)
	if err != nil {
		t.Fatalf("parse valid brief: %v", err)
	}
	if brief.ID != "task-intake" || brief.Title != "Parse one brief" || !brief.ReviewRequired || brief.MaxRuns != 8 || !strings.Contains(brief.Body, "# Objective") {
		t.Errorf("parsed brief = %+v", brief)
	}

	invalid := map[string]string{
		"missing_front_matter": "# Objective\n",
		"unknown_field":        strings.Replace(string(valid), "risk: medium", "risk: medium\nstate: done", 1),
		"missing_title":        strings.Replace(string(valid), "title: Parse one brief\n", "", 1),
		"missing_review":       strings.Replace(string(valid), "review_required: true\n", "", 1),
		"invalid_id":           strings.Replace(string(valid), "id: task-intake", "id: ../escape", 1),
		"invalid_risk":         strings.Replace(string(valid), "risk: medium", "risk: extreme", 1),
		"invalid_limit":        strings.Replace(string(valid), "max_runs: 8", "max_runs: 0", 1),
		"empty_body":           "---\nid: task-empty\ntitle: Empty\nrisk: low\nreview_required: false\n---\n",
	}
	for name, data := range invalid {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseBrief([]byte(data)); err == nil {
				t.Errorf("invalid brief was accepted")
			}
		})
	}

	t.Run("immutable_copy_hash_and_duplicate_fence", func(t *testing.T) {
		s := newTestStore(t)
		source := writeTestBrief(t, "task-copy", false)
		original, _ := os.ReadFile(source)
		task, err := s.IntakeBrief(context.Background(), source)
		if err != nil {
			t.Fatal(err)
		}
		copied, err := os.ReadFile(task.BriefPath)
		if err != nil || string(copied) != string(original) {
			t.Errorf("published brief differs: %v", err)
			return
		}
		wantHash := fmt.Sprintf("%x", sha256.Sum256(original))
		if task.BriefSHA256 != wantHash || task.State != StateTodo || task.Version != 0 {
			t.Errorf("intake task = %+v", task)
		}
		if err := os.WriteFile(source, []byte("changed"), 0o600); err != nil {
			t.Fatal(err)
		}
		after, _ := os.ReadFile(task.BriefPath)
		if string(after) != string(original) {
			t.Errorf("source edit changed immutable brief")
		}
		if _, err := s.IntakeBrief(context.Background(), source); err == nil {
			t.Errorf("duplicate task ID was accepted")
		}
	})

	t.Run("config_requires_distinct_disposable_root", func(t *testing.T) {
		worker := t.TempDir()
		for name, cfg := range map[string]Config{
			"same":        {Root: worker, WorkerDir: worker},
			"legacy":      {Root: filepath.Join(worker, ".werkrby"), WorkerDir: worker},
			"root_nested": {Root: filepath.Join(worker, "state"), WorkerDir: worker},
		} {
			t.Run(name, func(t *testing.T) {
				if err := cfg.Validate(); err == nil {
					t.Errorf("unsafe config was accepted: %+v", cfg)
				}
			})
		}
	})
}
