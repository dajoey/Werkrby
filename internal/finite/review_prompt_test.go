package finite

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	reviewPromptImplProgressSentinel = "SENTINEL-IMPL-PROSE-5c1e"
	reviewPromptFindingSentinel      = "FINDING-KEEP-9a2b"
	reviewPromptWorkKeep             = "WORK-KEEP-11aa"
	reviewPromptJoeyKeep             = "JOEY-KEEP-4d19"
	reviewPromptBriefProse1          = "SENTINEL-BRIEF-PROSE-77d0"
	reviewPromptBriefProse2          = "SENTINEL-BRIEF-PROSE-77d1"
	reviewPromptEarlierProse         = "SENTINEL-EARLIER-PROSE-2e44"
)

// writeTestBriefBody writes an intake brief with extra front matter lines and
// a caller-provided body, so tests can shape exactly what a bridge-built
// brief looks like.
func writeTestBriefBody(t *testing.T, id, extraFront, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), id+".md")
	data := "---\nid: " + id + "\ntitle: Test task " + id + "\nrisk: low\nreview_required: true\n" +
		extraFront + "---\n\n" + body + "\n"
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatalf("write brief: %v", err)
	}
	return path
}

func countPromptLines(text, prefix string) int {
	count := 0
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, prefix) {
			count++
		}
	}
	return count
}

// A review-phase prompt must not carry what an implementer wrote about its own
// work: the reviewer sees findings and Joey's answers, never implementer
// progress, or it grades the implementer's story instead of the work.
func TestReviewPromptExcludesImplementerProgress(t *testing.T) {
	engine := newEngineFixture(t, engineFixturePlan{})
	s := engine.store
	ctx := context.Background()
	task := intakeTestTask(t, s, "review-prompt-prose", true)

	impl := dispatchTestRun(t, s, task, PhaseImplementation)
	implResult := validTestResult(impl, ResultReview)
	implResult.Progress = reviewPromptImplProgressSentinel
	toReview, err := s.SettleRun(ctx, SettlementRequest{
		RunID: impl.ID, ExpectedTaskVersion: task.Version + 1, ExecutionOutcome: RunOK, Result: implResult,
	})
	if err != nil {
		t.Fatal(err)
	}

	review := dispatchTestRun(t, s, toReview, PhaseReview)
	reviewResult := validTestResult(review, ResultRejected)
	reviewResult.Progress = "Two problems found."
	reviewResult.Findings = []string{reviewPromptFindingSentinel}
	toTodo, err := s.SettleRun(ctx, SettlementRequest{
		RunID: review.ID, ExpectedTaskVersion: toReview.Version + 1, ExecutionOutcome: RunOK, Result: reviewResult,
	})
	if err != nil {
		t.Fatal(err)
	}

	impl2 := dispatchTestRun(t, s, toTodo, PhaseImplementation)
	impl2Result := validTestResult(impl2, ResultReview)
	impl2Result.Progress = reviewPromptImplProgressSentinel
	if _, err := s.SettleRun(ctx, SettlementRequest{
		RunID: impl2.ID, ExpectedTaskVersion: toTodo.Version + 1, ExecutionOutcome: RunOK, Result: impl2Result,
	}); err != nil {
		t.Fatal(err)
	}

	brief, err := os.ReadFile(task.BriefPath)
	if err != nil {
		t.Fatal(err)
	}
	reviewText, err := engine.dispatchDynamic(ctx, task, PhaseReview, brief)
	if err != nil {
		t.Fatal(err)
	}
	implText, err := engine.dispatchDynamic(ctx, task, PhaseImplementation, brief)
	if err != nil {
		t.Fatal(err)
	}

	if strings.Contains(reviewText, reviewPromptImplProgressSentinel) {
		t.Errorf("review prompt carries implementer progress:\n%s", reviewText)
	}
	if !strings.Contains(reviewText, reviewPromptFindingSentinel) {
		t.Errorf("review prompt lost the review finding:\n%s", reviewText)
	}
	if got := countPromptLines(reviewText, "- Run "); got != 3 {
		t.Errorf("review prompt shows %d run lines, want 3:\n%s", got, reviewText)
	}
	if !strings.Contains(implText, reviewPromptImplProgressSentinel) {
		t.Errorf("implementation prompt lost its own progress:\n%s", implText)
	}
}

// The bridge appends the worker-written Progress log and Earlier attempt
// sections to the brief body it hands the engine. A review prompt must cut
// everything from the first such implementer heading to the end of the body:
// a worker-written heading inside that span must not leak text.
func TestReviewPromptDropsImplementerBriefSections(t *testing.T) {
	engine := newEngineFixture(t, engineFixturePlan{})
	s := engine.store
	ctx := context.Background()
	body := "## Objective\n\n" + reviewPromptWorkKeep + "\n\n" +
		"## Joey's instruction\n\n" + reviewPromptJoeyKeep + "\n\n" +
		"## Progress so far (earlier runs; read it first, verify current state, and do NOT " +
		"repeat steps already done, especially deletions, pruning or other destructive steps)\n\n" +
		reviewPromptBriefProse1 + "\n\n## Joey's instruction\n\n" + reviewPromptBriefProse2 + "\n\n" +
		"## Earlier attempt\n\n" + reviewPromptEarlierProse
	path := writeTestBriefBody(t, "review-prompt-body", "", body)
	task, err := s.IntakeBrief(ctx, path)
	if err != nil {
		t.Fatal(err)
	}

	brief, err := os.ReadFile(task.BriefPath)
	if err != nil {
		t.Fatal(err)
	}
	reviewText, err := engine.dispatchDynamic(ctx, task, PhaseReview, brief)
	if err != nil {
		t.Fatal(err)
	}
	implText, err := engine.dispatchDynamic(ctx, task, PhaseImplementation, brief)
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{reviewPromptWorkKeep, reviewPromptJoeyKeep} {
		if !strings.Contains(reviewText, want) {
			t.Errorf("review prompt lost %q:\n%s", want, reviewText)
		}
	}
	for _, banned := range []string{reviewPromptBriefProse1, reviewPromptBriefProse2, reviewPromptEarlierProse} {
		if strings.Contains(reviewText, banned) {
			t.Errorf("review prompt carries implementer brief prose %q:\n%s", banned, reviewText)
		}
	}
	for _, want := range []string{
		reviewPromptWorkKeep, reviewPromptJoeyKeep, reviewPromptBriefProse1,
		reviewPromptBriefProse2, reviewPromptEarlierProse,
	} {
		if !strings.Contains(implText, want) {
			t.Errorf("implementation prompt lost %q:\n%s", want, implText)
		}
	}
}
