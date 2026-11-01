package finite

import (
	"fmt"
	"strings"
)

// NewUsageAccumulator creates one run-bound adapter normalizer.
func NewUsageAccumulator(runID int64, convention UsageConvention) (*UsageAccumulator, error) {
	if runID <= 0 || (convention != UsageCumulative && convention != UsageDelta) {
		return nil, fmt.Errorf("finite: invalid usage accumulator binding")
	}
	return &UsageAccumulator{runID: runID, convention: convention}, nil
}

// Add folds one adapter sample into the normalized run total.
func (a *UsageAccumulator) Add(sample UsageSample) error {
	if a == nil || sample.RunID != a.runID || sample.Convention != a.convention {
		return fmt.Errorf("finite: usage run or convention mismatch")
	}
	if err := validateUsageValues(sample.Value); err != nil {
		return err
	}
	if strings.TrimSpace(sample.Value.Source) == "" || sample.Value.CollectedAt.IsZero() {
		return fmt.Errorf("finite: usage source and collection time are required")
	}
	fields := []struct {
		destination **int64
		value       *int64
	}{
		{&a.total.FreshInputTokens, sample.Value.FreshInputTokens},
		{&a.total.CacheReadTokens, sample.Value.CacheReadTokens},
		{&a.total.CacheWriteTokens, sample.Value.CacheWriteTokens},
		{&a.total.OutputTokens, sample.Value.OutputTokens},
		{&a.total.ReasoningTokens, sample.Value.ReasoningTokens},
		{&a.total.APICalls, sample.Value.APICalls},
		{&a.total.ToolCalls, sample.Value.ToolCalls},
	}
	for _, field := range fields {
		if field.value == nil {
			continue
		}
		value := *field.value
		if a.convention == UsageCumulative {
			if *field.destination != nil && value < **field.destination {
				return fmt.Errorf("finite: cumulative usage regressed")
			}
			*field.destination = &value
			continue
		}
		if *field.destination != nil {
			value += **field.destination
		}
		*field.destination = &value
	}
	a.total.Source = sample.Value.Source
	a.total.CollectedAt = sample.Value.CollectedAt.UTC()
	return nil
}

// Total returns the nullable normalized usage.
func (a *UsageAccumulator) Total() Usage {
	if a == nil {
		return Usage{}
	}
	return cloneUsage(a.total)
}

func cloneUsage(usage Usage) Usage {
	clone := usage
	clone.FreshInputTokens = cloneInt64(usage.FreshInputTokens)
	clone.CacheReadTokens = cloneInt64(usage.CacheReadTokens)
	clone.CacheWriteTokens = cloneInt64(usage.CacheWriteTokens)
	clone.OutputTokens = cloneInt64(usage.OutputTokens)
	clone.ReasoningTokens = cloneInt64(usage.ReasoningTokens)
	clone.APICalls = cloneInt64(usage.APICalls)
	clone.ToolCalls = cloneInt64(usage.ToolCalls)
	clone.CollectedAt = usage.CollectedAt.UTC()
	return clone
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func validateUsageValues(usage Usage) error {
	for _, value := range []*int64{usage.FreshInputTokens, usage.CacheReadTokens, usage.CacheWriteTokens, usage.OutputTokens, usage.ReasoningTokens, usage.APICalls, usage.ToolCalls} {
		if value != nil && *value < 0 {
			return fmt.Errorf("finite: usage values cannot be negative")
		}
	}
	return nil
}

func validateNormalizedUsage(usage Usage) error {
	if err := validateUsageValues(usage); err != nil {
		return err
	}
	if strings.TrimSpace(usage.Source) == "" || usage.CollectedAt.IsZero() {
		return fmt.Errorf("finite: usage source and collection time are required")
	}
	return nil
}
