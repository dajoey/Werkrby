package finite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode"
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

// DeliveryHTTPError reports only a receiver status code, never response content.
type DeliveryHTTPError struct {
	StatusCode int
}

func (e *DeliveryHTTPError) Error() string { return fmt.Sprintf("finite: receiver returned HTTP %d", e.StatusCode) }

// HTTPDeliveryTransport sends exact payload bytes to a durable receiver.
type HTTPDeliveryTransport struct {
	Endpoint string
	Token    string
	Client   *http.Client
}

func validateDelivery(delivery Delivery) error {
	if delivery.ID == "" || len(delivery.ID) > 256 || strings.IndexFunc(delivery.ID, unicode.IsControl) >= 0 {
		return fmt.Errorf("finite: invalid delivery ID")
	}
	if len(delivery.Payload) == 0 || len(delivery.Payload) > maxDeliveryPayloadBytes || !json.Valid(delivery.Payload) {
		return fmt.Errorf("finite: delivery payload must be bounded valid JSON")
	}
	return nil
}

// Deliver transmits one delivery and waits for its acknowledgment.
func (t HTTPDeliveryTransport) Deliver(ctx context.Context, delivery Delivery) error {
	if err := validateDelivery(delivery); err != nil {
		return err
	}
	if err := validateBearerToken(t.Token); err != nil {
		return err
	}
	endpoint, err := validateLoopbackEndpoint(t.Endpoint)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String()+"/v1/deliveries", bytes.NewReader(delivery.Payload))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+t.Token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Werkrby-Delivery-ID", delivery.ID)
	client := t.Client
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	response, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("finite: delivery transport failed: %w", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return &DeliveryHTTPError{StatusCode: response.StatusCode}
	}
	return nil
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
	processor := &OutboxProcessor{Store: store, Transport: transport, RetryDelay: 5 * time.Second}
	if store != nil {
		processor.recordAttempt = store.RecordDeliveryAttempt
	}
	return processor
}

// ProcessOnce transmits and records at most one currently due outbox entry.
func (p *OutboxProcessor) ProcessOnce(ctx context.Context) (int, error) {
	if p == nil || p.Store == nil || p.Transport == nil || p.RetryDelay < 0 {
		return 0, fmt.Errorf("finite: outbox processor requires a store, transport, and nonnegative retry delay")
	}
	now := p.Now
	if now == nil {
		now = time.Now
	}
	attemptedAt := now().UTC()
	entry, err := p.Store.NextPendingOutbox(ctx, attemptedAt)
	if errors.Is(err, ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	delivery := Delivery{ID: entry.DeliveryID, Payload: []byte(entry.PayloadJSON)}
	deliveryErr := p.Transport.Deliver(ctx, delivery)
	attempt := DeliveryAttempt{
		DeliveryID: entry.DeliveryID, ExpectedSendCount: entry.SendCount,
		AttemptedAt: attemptedAt, RetryAt: attemptedAt.Add(p.RetryDelay), Delivered: deliveryErr == nil,
	}
	if deliveryErr != nil {
		attempt.Err = boundedDeliveryError(deliveryErr)
	}
	record := p.recordAttempt
	if record == nil {
		record = p.Store.RecordDeliveryAttempt
	}
	if _, err := record(ctx, attempt); err != nil {
		return 1, err
	}
	return 1, deliveryErr
}

func boundedDeliveryError(err error) string {
	var status *DeliveryHTTPError
	if errors.As(err, &status) {
		return fmt.Sprintf("receiver returned HTTP %d", status.StatusCode)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "delivery deadline exceeded"
	}
	return "delivery transport failed"
}

// DeliverOutbox runs one outbox pass with default retry policy.
func DeliverOutbox(ctx context.Context, store *Store, transport DeliveryTransport) (int, error) {
	return NewOutboxProcessor(store, transport).ProcessOnce(ctx)
}
