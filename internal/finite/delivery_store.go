package finite

import (
	"context"
	"time"
)

// DeliveryAttempt atomically records the outcome of one completed transport call.
type DeliveryAttempt struct {
	DeliveryID        string
	ExpectedSendCount int
	AttemptedAt       time.Time
	RetryAt           time.Time
	Delivered         bool
	Err               string
}

// NextPendingOutbox returns the oldest due undelivered entry.
func (s *Store) NextPendingOutbox(ctx context.Context, now time.Time) (OutboxEntry, error) {
	return OutboxEntry{}, errDeliveryNotImplemented
}

// RecordDeliveryAttempt mutates send count, error, and delivery time together.
func (s *Store) RecordDeliveryAttempt(ctx context.Context, attempt DeliveryAttempt) (OutboxEntry, error) {
	return OutboxEntry{}, errDeliveryNotImplemented
}
