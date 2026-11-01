package finite

import (
	"bytes"
	"errors"
	"testing"
)

func TestFiniteReceiverDeduplicationAndImmutableBytes(t *testing.T) {
	root := t.TempDir()
	receiver, err := NewFileReceiver(ReceiverConfig{Root: root, Token: "receiver-test-token-receiver-test-token"})
	if err != nil {
		t.Fatal(err)
	}
	id := "task:receiver:v2:default"
	original := []byte("{\n  \"state\": \"done\", \"version\": 2\n}\n")
	applied, err := receiver.AcceptDelivery(id, original)
	if err != nil || !applied {
		t.Fatalf("first receiver effect = (%t, %v), want applied", applied, err)
	}
	applied, err = receiver.AcceptDelivery(id, bytes.Clone(original))
	if err != nil || applied {
		t.Fatalf("identical duplicate = (%t, %v), want deduplicated", applied, err)
	}
	conflicting := []byte(`{"state":"done","version":2}`)
	if _, err := receiver.AcceptDelivery(id, conflicting); !errors.Is(err, ErrDeliveryConflict) {
		t.Fatalf("byte-conflicting duplicate error = %v, want ErrDeliveryConflict", err)
	}
	restarted, err := NewFileReceiver(ReceiverConfig{Root: root, Token: "receiver-test-token-receiver-test-token"})
	if err != nil {
		t.Fatal(err)
	}
	applied, err = restarted.AcceptDelivery(id, original)
	if err != nil || applied {
		t.Fatalf("post-restart duplicate = (%t, %v), want durable dedupe", applied, err)
	}
	effects, err := ReadReceiverDeliveries(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(effects) != 1 || effects[0].DeliveryID != id || !bytes.Equal(effects[0].Payload, original) {
		t.Fatalf("receiver effects = %+v, want one byte-identical effect", effects)
	}
}

func TestFiniteReceiverRejectsDifferentIDWithoutCouplingEffects(t *testing.T) {
	root := t.TempDir()
	receiver, err := NewFileReceiver(ReceiverConfig{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"state":"failed"}`)
	for _, id := range []string{"delivery-a", "delivery-b"} {
		applied, err := receiver.AcceptDelivery(id, payload)
		if err != nil || !applied {
			t.Fatalf("delivery %s = (%t, %v)", id, applied, err)
		}
	}
	effects, err := ReadReceiverDeliveries(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(effects) != 2 {
		t.Fatalf("independent delivery IDs produced %d effects, want 2", len(effects))
	}
}
