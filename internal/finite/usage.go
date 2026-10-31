package finite

// NewUsageAccumulator creates one run-bound adapter normalizer.
func NewUsageAccumulator(runID int64, convention UsageConvention) (*UsageAccumulator, error) {
	return &UsageAccumulator{}, nil
}

// Add folds one adapter sample into the normalized run total.
func (a *UsageAccumulator) Add(sample UsageSample) error { return nil }

// Total returns the nullable normalized usage.
func (a *UsageAccumulator) Total() Usage { return Usage{} }
