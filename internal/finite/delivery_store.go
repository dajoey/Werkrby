package finite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
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

const outboxSelect = `SELECT delivery_id,event_id,channel,kind,payload_json,created_at,available_at,send_count,delivered_at,last_error FROM outbox`

func scanOutboxEntry(scanner rowScanner) (OutboxEntry, error) {
	var entry OutboxEntry
	var created, available string
	var delivered, lastError sql.NullString
	if err := scanner.Scan(&entry.DeliveryID, &entry.EventID, &entry.Channel, &entry.Kind,
		&entry.PayloadJSON, &created, &available, &entry.SendCount, &delivered, &lastError); err != nil {
		return OutboxEntry{}, err
	}
	var err error
	if entry.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil {
		return OutboxEntry{}, err
	}
	if entry.AvailableAt, err = time.Parse(time.RFC3339Nano, available); err != nil {
		return OutboxEntry{}, err
	}
	if delivered.Valid {
		value, err := time.Parse(time.RFC3339Nano, delivered.String)
		if err != nil {
			return OutboxEntry{}, err
		}
		entry.DeliveredAt = &value
	}
	entry.LastError = lastError.String
	return entry, nil
}

// NextPendingOutbox returns the oldest due undelivered entry.
func (s *Store) NextPendingOutbox(ctx context.Context, now time.Time) (OutboxEntry, error) {
	entry, err := scanOutboxEntry(s.db.QueryRowContext(ctx, outboxSelect+`
		WHERE delivered_at IS NULL AND available_at<=? ORDER BY available_at,created_at,delivery_id LIMIT 1`, stamp(now)))
	if errors.Is(err, sql.ErrNoRows) {
		return OutboxEntry{}, fmt.Errorf("%w: no due outbox entry", ErrNotFound)
	}
	return entry, err
}

func getOutboxTx(ctx context.Context, tx txRunner, deliveryID string) (OutboxEntry, error) {
	entry, err := scanOutboxEntry(tx.QueryRowContext(ctx, outboxSelect+` WHERE delivery_id=?`, deliveryID))
	if errors.Is(err, sql.ErrNoRows) {
		return OutboxEntry{}, fmt.Errorf("%w: delivery %s", ErrNotFound, deliveryID)
	}
	return entry, err
}

// RecordDeliveryAttempt mutates send count, error, and delivery time together.
func (s *Store) RecordDeliveryAttempt(ctx context.Context, attempt DeliveryAttempt) (OutboxEntry, error) {
	if attempt.DeliveryID == "" || attempt.ExpectedSendCount < 0 || attempt.AttemptedAt.IsZero() {
		return OutboxEntry{}, fmt.Errorf("finite: invalid delivery attempt")
	}
	if !attempt.Delivered && (attempt.RetryAt.IsZero() || strings.TrimSpace(attempt.Err) == "") {
		return OutboxEntry{}, fmt.Errorf("finite: failed delivery attempt requires a retry time and error")
	}
	var recorded OutboxEntry
	err := s.withImmediateTx(ctx, func(tx txRunner) error {
		current, err := getOutboxTx(ctx, tx, attempt.DeliveryID)
		if err != nil {
			return err
		}
		if current.DeliveredAt != nil {
			recorded = current
			return nil
		}
		if current.SendCount != attempt.ExpectedSendCount {
			return fmt.Errorf("%w: delivery attempt expected send count %d, current %d", ErrVersionConflict, attempt.ExpectedSendCount, current.SendCount)
		}
		var result sql.Result
		if attempt.Delivered {
			result, err = tx.ExecContext(ctx, `UPDATE outbox SET send_count=send_count+1,delivered_at=?,last_error=NULL
				WHERE delivery_id=? AND send_count=? AND delivered_at IS NULL`, stamp(attempt.AttemptedAt), attempt.DeliveryID, attempt.ExpectedSendCount)
		} else {
			result, err = tx.ExecContext(ctx, `UPDATE outbox SET send_count=send_count+1,available_at=?,last_error=?
				WHERE delivery_id=? AND send_count=? AND delivered_at IS NULL`, stamp(attempt.RetryAt), cleanDeliveryError(attempt.Err), attempt.DeliveryID, attempt.ExpectedSendCount)
		}
		if err != nil {
			return err
		}
		if affected, _ := result.RowsAffected(); affected != 1 {
			return fmt.Errorf("%w: delivery attempt mutated concurrently", ErrVersionConflict)
		}
		recorded, err = getOutboxTx(ctx, tx, attempt.DeliveryID)
		return err
	})
	return recorded, err
}

func cleanDeliveryError(value string) string {
	value = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, strings.TrimSpace(value))
	if len(value) > 512 {
		value = value[:512]
	}
	return value
}
