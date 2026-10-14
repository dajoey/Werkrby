package app

import (
	"sync"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/channel"
	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/theme"
	"github.com/agent0ai/spynel/internal/workspace"
)

func newPublishTestService(t *testing.T) *Service {
	t.Helper()
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	return New(cfg, newServiceHarness())
}

// runConcurrently runs fn from n goroutines with no consumer on the channels
// and fails if any of them is still blocked after the deadline.
func runConcurrently(t *testing.T, name string, n int, fn func(i int)) {
	t.Helper()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			fn(i)
		}(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s blocked forever", name)
	}
}

func TestSetPairingConcurrentCallersNeverBlockWithoutConsumer(t *testing.T) {
	service := newPublishTestService(t)
	for round := 0; round < 200; round++ {
		runConcurrently(t, "SetPairing", 8, func(int) {
			service.SetPairing(channel.PairingEvent{Name: "whatsapp", State: "starting"})
		})
	}
	select {
	case event := <-service.PairingEvents():
		if event.Name != "whatsapp" {
			t.Fatalf("pairing event = %#v", event)
		}
	default:
		t.Fatal("latest pairing event was not left for the consumer")
	}
}

func TestPublishThemeConcurrentCallersNeverBlockWithoutConsumer(t *testing.T) {
	service := newPublishTestService(t)
	for round := 0; round < 200; round++ {
		runConcurrently(t, "publishTheme", 8, func(int) {
			service.publishTheme(theme.Theme{Name: "x"})
		})
	}
	select {
	case got := <-service.ThemeChanges():
		if got.Name != "x" {
			t.Fatalf("theme = %#v", got)
		}
	default:
		t.Fatal("latest theme was not left for the consumer")
	}
}
