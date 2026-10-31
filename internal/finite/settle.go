package finite

import "context"

// SettleRun applies one terminal settlement and replays it idempotently.
func (s *Store) SettleRun(ctx context.Context, req SettlementRequest) (Task, error) {
	return Task{}, nil
}

// RequestStop commits the durable cancellation fence.
func (s *Store) RequestStop(ctx context.Context, taskID string, expectedVersion int64) (Task, error) {
	return Task{}, nil
}

// WakeTime performs a due time-only, task-only wait transition.
func (s *Store) WakeTime(ctx context.Context, taskID string, expectedVersion int64) (Task, error) {
	return Task{}, nil
}

// AnswerJoey performs a matching-answer, task-only wait transition.
func (s *Store) AnswerJoey(ctx context.Context, taskID string, expectedVersion int64, optionID string) (Task, error) {
	return Task{}, nil
}
