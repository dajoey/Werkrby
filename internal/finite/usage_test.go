package finite

import (
	"context"
	"testing"
	"time"
)

func int64Pointer(value int64) *int64 { return &value }

func usageSample(runID int64, convention UsageConvention, fresh, output *int64) UsageSample {
	return UsageSample{RunID: runID, Convention: convention, Value: Usage{
		FreshInputTokens: fresh, OutputTokens: output, Source: "fake-adapter",
		CollectedAt: time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC),
	}}
}

func TestFiniteUsageNormalization_U1(t *testing.T) {
	t.Run("partial_cumulative", func(t *testing.T) {
		a, err := NewUsageAccumulator(42, UsageCumulative)
		if err != nil {
			t.Fatal(err)
		}
		if err := a.Add(usageSample(42, UsageCumulative, int64Pointer(100), int64Pointer(10))); err != nil {
			t.Fatal(err)
		}
		if err := a.Add(usageSample(42, UsageCumulative, int64Pointer(130), nil)); err != nil {
			t.Fatal(err)
		}
		got := a.Total()
		if got.FreshInputTokens == nil || *got.FreshInputTokens != 130 || got.OutputTokens == nil || *got.OutputTokens != 10 || got.CacheReadTokens != nil {
			t.Errorf("cumulative normalization = %+v", got)
		}
	})

	t.Run("delta_and_missing", func(t *testing.T) {
		a, err := NewUsageAccumulator(42, UsageDelta)
		if err != nil {
			t.Fatal(err)
		}
		_ = a.Add(usageSample(42, UsageDelta, int64Pointer(20), int64Pointer(2)))
		_ = a.Add(usageSample(42, UsageDelta, int64Pointer(7), nil))
		got := a.Total()
		if got.FreshInputTokens == nil || *got.FreshInputTokens != 27 || got.OutputTokens == nil || *got.OutputTokens != 2 || got.ReasoningTokens != nil {
			t.Errorf("delta normalization = %+v", got)
		}
		empty, _ := NewUsageAccumulator(43, UsageCumulative)
		if err := empty.Add(usageSample(43, UsageCumulative, nil, nil)); err != nil {
			t.Fatal(err)
		}
		if got := empty.Total(); got.FreshInputTokens != nil || got.OutputTokens != nil {
			t.Errorf("missing usage fabricated zeros: %+v", got)
		}
	})

	t.Run("wrong_run_negative_and_mixed_convention", func(t *testing.T) {
		a, _ := NewUsageAccumulator(42, UsageCumulative)
		if err := a.Add(usageSample(41, UsageCumulative, int64Pointer(1), nil)); err == nil {
			t.Errorf("wrong run usage was accepted")
		}
		if err := a.Add(usageSample(42, UsageDelta, int64Pointer(1), nil)); err == nil {
			t.Errorf("mixed usage convention was accepted")
		}
		if err := a.Add(usageSample(42, UsageCumulative, int64Pointer(-1), nil)); err == nil {
			t.Errorf("negative usage was accepted")
		}
	})

	t.Run("nullable_per_run_storage", func(t *testing.T) {
		s := newTestStore(t)
		task := intakeTestTask(t, s, "task-usage", false)
		run := dispatchTestRun(t, s, task, PhaseImplementation)
		usage := usageSample(run.ID, UsageCumulative, int64Pointer(50), nil).Value
		if _, err := s.SettleRun(context.Background(), SettlementRequest{
			RunID: run.ID, ExpectedTaskVersion: task.Version + 1, ExecutionOutcome: RunOK,
			Result: validTestResult(run, ResultDone), Usage: &usage,
		}); err != nil {
			t.Fatal(err)
		}
		got, err := s.GetUsage(context.Background(), run.ID)
		if err != nil || got.FreshInputTokens == nil || *got.FreshInputTokens != 50 || got.OutputTokens != nil {
			t.Errorf("stored usage = %+v, %v", got, err)
		}
	})
}
