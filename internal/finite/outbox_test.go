package finite

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func newOutboxTestEntry(t *testing.T, id string) (*Store, OutboxEntry) {
	t.Helper()
	store := newTestStore(t)
	task := intakeTestTask(t, store, id, false)
	run := dispatchTestRun(t, store, task, PhaseImplementation)
	settled, err := store.SettleRun(context.Background(), SettlementRequest{
		RunID: run.ID, ExpectedTaskVersion: task.Version + 1,
		ExecutionOutcome: RunOK, Result: validTestResult(run, ResultDone),
	})
	if err != nil {
		t.Fatalf("settle terminal fixture: %v", err)
	}
	if settled.State != StateDone {
		t.Fatalf("fixture state = %s", settled.State)
	}
	entries, err := store.ListOutbox(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("outbox entries = %d, want 1", len(entries))
	}
	return store, entries[0]
}

func TestFiniteOutboxDeliveryStableAndRetry(t *testing.T) {
	store, original := newOutboxTestEntry(t, "outbox-retry")
	var deliveries []Delivery
	transport := DeliveryTransportFunc(func(ctx context.Context, delivery Delivery) error {
		deliveries = append(deliveries, Delivery{ID: delivery.ID, Payload: bytes.Clone(delivery.Payload)})
		if len(deliveries) == 1 {
			return errors.New("synthetic receiver unavailable")
		}
		return nil
	})
	processor := NewOutboxProcessor(store, transport)
	processor.RetryDelay = 0
	processed, err := processor.ProcessOnce(context.Background())
	if processed != 1 || err == nil {
		t.Fatalf("first delivery = (%d, %v), want one failed transmission", processed, err)
	}
	failed, err := store.ListOutbox(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if failed[0].SendCount != 1 || failed[0].DeliveredAt != nil || strings.TrimSpace(failed[0].LastError) == "" {
		t.Fatalf("failed attempt was not atomically recorded: %+v", failed[0])
	}
	processed, err = processor.ProcessOnce(context.Background())
	if processed != 1 || err != nil {
		t.Fatalf("retry = (%d, %v), want one acknowledged transmission", processed, err)
	}
	if len(deliveries) != 2 {
		t.Fatalf("transmissions = %d, want 2", len(deliveries))
	}
	if deliveries[0].ID != original.DeliveryID || deliveries[1].ID != original.DeliveryID ||
		!bytes.Equal(deliveries[0].Payload, deliveries[1].Payload) ||
		!bytes.Equal(deliveries[0].Payload, []byte(original.PayloadJSON)) {
		t.Fatalf("delivery ID or immutable payload changed across retry")
	}
	delivered, err := store.ListOutbox(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if delivered[0].SendCount != 2 || delivered[0].DeliveredAt == nil || delivered[0].LastError != "" {
		t.Fatalf("acknowledged attempt state = %+v", delivered[0])
	}
	processed, err = processor.ProcessOnce(context.Background())
	if processed != 0 || err != nil || len(deliveries) != 2 {
		t.Fatalf("settled entry was redelivered: processed=%d err=%v transmissions=%d", processed, err, len(deliveries))
	}
	if got := countRows(t, store, `SELECT COUNT(*) FROM runs`); got != 1 {
		t.Fatalf("outbox processing redispatched a provider run: %d", got)
	}
	if got := countRows(t, store, `SELECT COUNT(*) FROM task_events`); got != 2 {
		t.Fatalf("outbox processing changed workflow events: %d", got)
	}
	if err := store.CheckInvariants(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestFiniteDeliveryAttemptMutationFencesStaleRetry(t *testing.T) {
	store, entry := newOutboxTestEntry(t, "outbox-fence")
	_, err := store.RecordDeliveryAttempt(context.Background(), DeliveryAttempt{
		DeliveryID: entry.DeliveryID, ExpectedSendCount: entry.SendCount + 1,
		AttemptedAt: time.Now(), RetryAt: time.Now(), Err: "stale",
	})
	if !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale delivery mutation error = %v, want ErrVersionConflict", err)
	}
	entries, listErr := store.ListOutbox(context.Background())
	if listErr != nil {
		t.Fatal(listErr)
	}
	if entries[0].SendCount != entry.SendCount || entries[0].PayloadJSON != entry.PayloadJSON || entries[0].DeliveredAt != nil {
		t.Fatalf("stale attempt mutated outbox state: %+v", entries[0])
	}
}
