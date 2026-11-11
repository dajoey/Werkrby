package finite

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Intake publishes the immutable brief before the task commit. A crash or an
// ambiguous commit in between must neither block the ID forever nor leave a
// committed task without its brief.

func TestFiniteIntakeCommitUnknownKeepsBriefOfCommittedTask(t *testing.T) {
	s := newTestStore(t)
	source := writeTestBrief(t, "intake-ambiguous", false)
	original, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("post-commit result is ambiguous")
	restore := registerStoreTransactionFault(storeTransactionAfterCommit, func() error { return sentinel })
	_, err = s.IntakeBrief(context.Background(), source)
	restore()
	if !errors.Is(err, sentinel) {
		t.Fatalf("ambiguous intake = %v, want the commit-unknown error", err)
	}
	task, err := s.GetTask(context.Background(), "intake-ambiguous")
	if err != nil {
		t.Fatalf("committed task row is missing: %v", err)
	}
	assertBriefMatches(t, task, original)
	if _, err := s.IntakeBrief(context.Background(), source); err == nil {
		t.Fatal("duplicate intake of a committed task was accepted")
	}
	assertBriefMatches(t, task, original)
}

func TestFiniteIntakeFailedCommitRemovesBriefAndAllowsRetry(t *testing.T) {
	s := newTestStore(t)
	source := writeTestBrief(t, "intake-rolled-back", false)
	sentinel := errors.New("commit refused")
	restore := registerStoreTransactionFault(storeTransactionBeforeCommit, func() error { return sentinel })
	_, err := s.IntakeBrief(context.Background(), source)
	restore()
	if !errors.Is(err, sentinel) {
		t.Fatalf("failed intake = %v", err)
	}
	if _, err := s.GetTask(context.Background(), "intake-rolled-back"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rolled-back intake left a task row: %v", err)
	}
	if _, err := os.Stat(filepath.Join(s.cfg.BriefDir(), "intake-rolled-back.md")); !os.IsNotExist(err) {
		t.Fatalf("rolled-back intake left its brief: %v", err)
	}
	if _, err := s.IntakeBrief(context.Background(), source); err != nil {
		t.Fatalf("retry after rolled-back intake: %v", err)
	}
}

func TestFiniteIntakeReusesIdenticalOrphanBrief(t *testing.T) {
	s := newTestStore(t)
	source := writeTestBrief(t, "intake-orphan", false)
	original, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	// An owner crash after brief publication and before the task commit.
	if err := os.MkdirAll(s.cfg.BriefDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.cfg.BriefDir(), "intake-orphan.md"), original, 0o600); err != nil {
		t.Fatal(err)
	}
	task, err := s.IntakeBrief(context.Background(), source)
	if err != nil {
		t.Fatalf("re-intake after the crash window was refused: %v", err)
	}
	assertBriefMatches(t, task, original)
	if _, err := s.IntakeBrief(context.Background(), source); err == nil {
		t.Fatal("second intake of the now-committed task was accepted")
	}
}

func TestFiniteIntakeRejectsDifferentOrphanBrief(t *testing.T) {
	s := newTestStore(t)
	source := writeTestBrief(t, "intake-conflict", false)
	orphan := []byte("---\nid: intake-conflict\ntitle: Other bytes\nrisk: low\nreview_required: false\n---\n\nDifferent objective.\n")
	if err := os.MkdirAll(s.cfg.BriefDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	orphanPath := filepath.Join(s.cfg.BriefDir(), "intake-conflict.md")
	if err := os.WriteFile(orphanPath, orphan, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.IntakeBrief(context.Background(), source); err == nil {
		t.Fatal("intake over a brief with different bytes was accepted")
	}
	if _, err := s.GetTask(context.Background(), "intake-conflict"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("conflicting intake created a task: %v", err)
	}
	kept, err := os.ReadFile(orphanPath)
	if err != nil || string(kept) != string(orphan) {
		t.Fatalf("conflicting intake changed the existing brief: %q, %v", kept, err)
	}
}

func assertBriefMatches(t *testing.T, task Task, original []byte) {
	t.Helper()
	data, err := os.ReadFile(task.BriefPath)
	if err != nil {
		t.Fatalf("committed task %s has no immutable brief: %v", task.ID, err)
	}
	if string(data) != string(original) || fmt.Sprintf("%x", sha256.Sum256(data)) != task.BriefSHA256 {
		t.Fatalf("brief of task %s does not match its committed hash", task.ID)
	}
}
