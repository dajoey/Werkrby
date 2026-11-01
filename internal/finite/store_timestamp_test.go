package finite

import (
	"context"
	"testing"
	"time"
)

// Outbox due times are compared and ordered as SQLite text, so every stored
// timestamp has one fixed width: text order is then time order.

func TestFiniteStampIsFixedWidthAndParses(t *testing.T) {
	values := []time.Time{
		time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 10, 31, 12, 0, 0, 500_000_000, time.UTC),
		time.Date(2026, 10, 31, 12, 0, 0, 1, time.FixedZone("EST", -5*3600)),
	}
	for i, value := range values {
		text := stamp(value)
		if len(text) != len("2006-01-02T15:04:05.000000000Z") {
			t.Fatalf("stamp(%v) = %q is not fixed width", value, text)
		}
		parsed, err := time.Parse(time.RFC3339Nano, text)
		if err != nil || !parsed.Equal(value) {
			t.Fatalf("stamp(%v) = %q parses to %v, %v", value, text, parsed, err)
		}
		if i > 0 && stamp(values[i-1]) >= text && values[i-1].Before(value) {
			t.Fatalf("text order of %q and %q disagrees with time order", stamp(values[i-1]), text)
		}
	}
}

func TestFiniteOutboxDueTimeUsesTimeOrder(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	whole := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return whole }
	first := intakeTestTask(t, s, "outbox-whole-second", false)
	if _, err := s.FailTask(ctx, first.ID, first.Version, "max_runs"); err != nil {
		t.Fatal(err)
	}
	// An entry due on a whole second is due a fraction of a second later.
	due, err := s.NextPendingOutbox(ctx, whole.Add(200*time.Millisecond))
	if err != nil {
		t.Fatalf("whole-second entry not due 200ms later: %v", err)
	}
	if due.EventID == "" || !due.AvailableAt.Equal(whole) {
		t.Fatalf("due entry = %+v", due)
	}
	s.now = func() time.Time { return whole.Add(500 * time.Millisecond) }
	second := intakeTestTask(t, s, "outbox-half-second", false)
	if _, err := s.FailTask(ctx, second.ID, second.Version, "max_runs"); err != nil {
		t.Fatal(err)
	}
	// The earlier whole-second entry is still the oldest due entry.
	next, err := s.NextPendingOutbox(ctx, whole.Add(time.Second))
	if err != nil || next.DeliveryID != due.DeliveryID {
		t.Fatalf("oldest due entry = %+v, %v; want %s", next, err, due.DeliveryID)
	}
	entries, err := s.ListOutbox(ctx)
	if err != nil || len(entries) != 2 || entries[0].DeliveryID != due.DeliveryID {
		t.Fatalf("outbox list order = %+v, %v", entries, err)
	}
}
