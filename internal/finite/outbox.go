package finite

import (
	"context"
	"net/http"
	"time"
)

// Delivery is the exact immutable byte sequence sent for one stable ID.
type Delivery struct {
	ID      string
	Payload []byte
}

// DeliveryTransport transmits one outbox entry without changing its bytes.
type DeliveryTransport interface {
	Deliver(ctx context.Context, delivery Delivery) error
}

// DeliveryTransportFunc adapts a function into a DeliveryTransport.
type DeliveryTransportFunc func(ctx context.Context, delivery Delivery) error

// Deliver calls f(ctx, delivery).
func (f DeliveryTransportFunc) Deliver(ctx context.Context, delivery Delivery) error {
	return f(ctx, delivery)
}

// HTTPDeliveryTransport sends exact payload bytes to a durable receiver.
type HTTPDeliveryTransport struct {
	Endpoint string
	Token    string
	Client   *http.Client
}

// Deliver transmits one delivery and waits for its acknowledgment.
func (t HTTPDeliveryTransport) Deliver(ctx context.Context, delivery Delivery) error {
	return errDeliveryNotImplemented
}

// OutboxProcessor delivers at most one due entry per pass.
type OutboxProcessor struct {
	Store         *Store
	Transport     DeliveryTransport
	RetryDelay    time.Duration
	Now           func() time.Time
	recordAttempt func(context.Context, DeliveryAttempt) (OutboxEntry, error)
}

// NewOutboxProcessor constructs a deterministic single-pass outbox processor.
func NewOutboxProcessor(store *Store, transport DeliveryTransport) *OutboxProcessor {
	processor := &OutboxProcessor{Store: store, Transport: transport}
	processor.recordAttempt = store.RecordDeliveryAttempt
	return processor
}

// ProcessOnce transmits and records at most one currently due outbox entry.
func (p *OutboxProcessor) ProcessOnce(ctx context.Context) (int, error) {
	return 0, errDeliveryNotImplemented
}

// DeliverOutbox runs one outbox pass with default retry policy.
func DeliverOutbox(ctx context.Context, store *Store, transport DeliveryTransport) (int, error) {
	return NewOutboxProcessor(store, transport).ProcessOnce(ctx)
}
