package finite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	receiverHelperEnv  = "WERKRBY_FINITE_RECEIVER_HELPER"
	receiverRootEnv    = "WERKRBY_FINITE_RECEIVER_ROOT"
	receiverTokenEnv   = "WERKRBY_FINITE_RECEIVER_TOKEN"
	receiverFaultEnv   = "WERKRBY_FINITE_RECEIVER_FAULT"
	receiverReadyEnv   = "WERKRBY_FINITE_RECEIVER_READY"
	receiverHelperTest = "^TestFiniteReceiverProcessHelper$"
)

type receiverReady struct {
	PID      int    `json:"pid"`
	Endpoint string `json:"endpoint"`
}

type receiverProcess struct {
	cmd      *exec.Cmd
	endpoint string
	pid      int
	finished bool
}

type recordingDeliveryTransport struct {
	base       *HTTPDeliveryTransport
	deliveries []Delivery
}

func (t *recordingDeliveryTransport) Deliver(ctx context.Context, delivery Delivery) error {
	t.deliveries = append(t.deliveries, Delivery{ID: delivery.ID, Payload: bytes.Clone(delivery.Payload)})
	return t.base.Deliver(ctx, delivery)
}

type outboxProcessFixture struct {
	store     *Store
	entry     OutboxEntry
	transport *recordingDeliveryTransport
	processor *OutboxProcessor
	root      string
	token     string
}

func TestFiniteReceiverProcessHelper(t *testing.T) {
	if os.Getenv(receiverHelperEnv) != "1" {
		return
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintln(os.Stderr, "receiver helper listen failed")
		os.Exit(2)
	}
	ready, err := json.Marshal(receiverReady{PID: os.Getpid(), Endpoint: "http://" + listener.Addr().String()})
	if err != nil {
		os.Exit(2)
	}
	if err := os.WriteFile(os.Getenv(receiverReadyEnv), ready, 0o600); err != nil {
		fmt.Fprintln(os.Stderr, "receiver helper readiness failed")
		os.Exit(2)
	}
	cfg := ReceiverConfig{
		Root: os.Getenv(receiverRootEnv), Token: os.Getenv(receiverTokenEnv),
		Fault: ReceiverFault(os.Getenv(receiverFaultEnv)),
	}
	if cfg.Fault == ReceiverFaultCrashAfterEffect {
		cfg.Exit = func() { os.Exit(70) }
	}
	if err := RunFileReceiver(context.Background(), listener, cfg); err != nil {
		fmt.Fprintln(os.Stderr, "receiver helper stopped with an error")
		os.Exit(2)
	}
	os.Exit(0)
}

func startReceiverProcess(t *testing.T, root, token string, fault ReceiverFault) *receiverProcess {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	readyPath := filepath.Join(t.TempDir(), "receiver-ready.json")
	command := exec.Command(executable, "-test.run="+receiverHelperTest)
	command.Env = append(os.Environ(),
		receiverHelperEnv+"=1", receiverRootEnv+"="+root, receiverTokenEnv+"="+token,
		receiverFaultEnv+"="+string(fault), receiverReadyEnv+"="+readyPath,
	)
	if err := command.Start(); err != nil {
		t.Fatalf("start receiver process: %v", err)
	}
	process := &receiverProcess{cmd: command, pid: command.Process.Pid}
	t.Cleanup(func() {
		if process.finished {
			return
		}
		if command.Process != nil {
			_ = command.Process.Kill()
		}
		_, _ = waitForCommand(command, 5*time.Second)
		process.finished = true
	})
	waitForFixture(t, func() bool { return fileExists(readyPath) }, "receiver process readiness")
	data, err := os.ReadFile(readyPath)
	if err != nil {
		t.Fatal(err)
	}
	var ready receiverReady
	if err := json.Unmarshal(data, &ready); err != nil {
		t.Fatal(err)
	}
	process.endpoint = ready.Endpoint
	process.pid = ready.PID
	t.Logf("receiver process started pid=%d fault=%s", process.pid, fault)
	return process
}

func newOutboxProcessFixture(t *testing.T, id string) *outboxProcessFixture {
	t.Helper()
	store, entry := newOutboxTestEntry(t, id)
	httpTransport := &http.Transport{DisableKeepAlives: true}
	base := &HTTPDeliveryTransport{
		Token:  "outbox-process-token-outbox-process-token",
		Client: &http.Client{Transport: httpTransport, Timeout: 2 * time.Second},
	}
	transport := &recordingDeliveryTransport{base: base}
	processor := NewOutboxProcessor(store, transport)
	processor.RetryDelay = 0
	t.Cleanup(httpTransport.CloseIdleConnections)
	return &outboxProcessFixture{
		store: store, entry: entry, transport: transport, processor: processor,
		root: t.TempDir(), token: base.Token,
	}
}

func (f *outboxProcessFixture) useEndpoint(endpoint string) {
	f.transport.base.Endpoint = endpoint
	f.transport.base.Token = f.token
}

func (f *outboxProcessFixture) outboxState(t *testing.T) OutboxEntry {
	t.Helper()
	entries, err := f.store.ListOutbox(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("outbox rows = %d, want 1", len(entries))
	}
	return entries[0]
}

func (f *outboxProcessFixture) requireOneEffect(t *testing.T) []ReceivedDelivery {
	t.Helper()
	effects, err := ReadReceiverDeliveries(f.root)
	if err != nil {
		t.Fatal(err)
	}
	if len(effects) != 1 {
		t.Fatalf("visible receiver effects = %d, want exactly 1", len(effects))
	}
	if effects[0].DeliveryID != f.entry.DeliveryID || !bytes.Equal(effects[0].Payload, []byte(f.entry.PayloadJSON)) {
		t.Fatalf("receiver effect does not preserve delivery identity and bytes")
	}
	return effects
}

func (f *outboxProcessFixture) requireDeliveredOnce(t *testing.T, scenario string, pid int) {
	t.Helper()
	state := f.outboxState(t)
	if state.DeliveredAt == nil || state.LastError != "" || state.SendCount < 1 {
		t.Fatalf("final outbox state = %+v", state)
	}
	if len(f.transport.deliveries) < 2 {
		t.Fatalf("transmissions = %d, want at least 2", len(f.transport.deliveries))
	}
	for _, delivery := range f.transport.deliveries {
		if delivery.ID != f.entry.DeliveryID || !bytes.Equal(delivery.Payload, []byte(f.entry.PayloadJSON)) {
			t.Fatalf("retry changed stable delivery ID or payload bytes")
		}
	}
	effects := f.requireOneEffect(t)
	if got := countRows(t, f.store, `SELECT COUNT(*) FROM runs`); got != 1 {
		t.Fatalf("provider run count changed during outbox recovery: %d", got)
	}
	if got := countRows(t, f.store, `SELECT COUNT(*) FROM task_events`); got != 2 {
		t.Fatalf("workflow event count changed during outbox recovery: %d", got)
	}
	if got := countRows(t, f.store, `SELECT COUNT(*) FROM outbox`); got != 1 {
		t.Fatalf("outbox row count changed during delivery: %d", got)
	}
	if err := f.store.CheckInvariants(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Logf("delivery evidence scenario=%s receiver_pid=%d delivery_id=%s transmissions=%d visible_effects=%d send_count=%d provider_runs=1", scenario, pid, f.entry.DeliveryID, len(f.transport.deliveries), len(effects), state.SendCount)
}

func TestFiniteOutboxACKLoss_D1_D2(t *testing.T) {
	t.Run("D1/receiver_unavailable", func(t *testing.T) {
		fixture := newOutboxProcessFixture(t, "outbox-d1")
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		unavailable := "http://" + listener.Addr().String()
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		fixture.useEndpoint(unavailable)
		processed, err := fixture.processor.ProcessOnce(context.Background())
		if processed != 1 || err == nil {
			t.Fatalf("unavailable receiver result = (%d, %v), want recorded failure", processed, err)
		}
		state := fixture.outboxState(t)
		if state.SendCount != 1 || state.DeliveredAt != nil || strings.TrimSpace(state.LastError) == "" {
			t.Fatalf("D1 pending outbox state = %+v", state)
		}
		receiver := startReceiverProcess(t, fixture.root, fixture.token, ReceiverFaultNone)
		fixture.useEndpoint(receiver.endpoint)
		processed, err = fixture.processor.ProcessOnce(context.Background())
		if processed != 1 || err != nil {
			t.Fatalf("D1 retry = (%d, %v)", processed, err)
		}
		fixture.requireDeliveredOnce(t, "D1/receiver_unavailable", receiver.pid)
	})

	t.Run("D2/ack_lost", func(t *testing.T) {
		fixture := newOutboxProcessFixture(t, "outbox-ack-lost")
		receiver := startReceiverProcess(t, fixture.root, fixture.token, ReceiverFaultACKLost)
		fixture.useEndpoint(receiver.endpoint)
		processed, err := fixture.processor.ProcessOnce(context.Background())
		if processed != 1 || err == nil {
			t.Fatalf("lost ACK result = (%d, %v), want ambiguous transport failure", processed, err)
		}
		state := fixture.outboxState(t)
		if state.SendCount != 1 || state.DeliveredAt != nil {
			t.Fatalf("lost ACK outbox state = %+v", state)
		}
		fixture.requireOneEffect(t)
		t.Logf("D2/ack_lost failpoint reached receiver_pid=%d", receiver.pid)
		processed, err = fixture.processor.ProcessOnce(context.Background())
		if processed != 1 || err != nil {
			t.Fatalf("lost ACK retry = (%d, %v)", processed, err)
		}
		fixture.requireDeliveredOnce(t, "D2/ack_lost", receiver.pid)
	})

	t.Run("D2/ack_received_mark_failed", func(t *testing.T) {
		fixture := newOutboxProcessFixture(t, "outbox-mark-failed")
		receiver := startReceiverProcess(t, fixture.root, fixture.token, ReceiverFaultNone)
		fixture.useEndpoint(receiver.endpoint)
		markFailure := errors.New("injected delivered-mark failure")
		record := fixture.processor.recordAttempt
		failMark := true
		fixture.processor.recordAttempt = func(ctx context.Context, attempt DeliveryAttempt) (OutboxEntry, error) {
			if failMark {
				failMark = false
				return OutboxEntry{}, markFailure
			}
			return record(ctx, attempt)
		}
		processed, err := fixture.processor.ProcessOnce(context.Background())
		if processed != 1 || !errors.Is(err, markFailure) {
			t.Fatalf("ACK/mark failure result = (%d, %v), want injected mark failure", processed, err)
		}
		state := fixture.outboxState(t)
		if state.SendCount != 0 || state.DeliveredAt != nil {
			t.Fatalf("failed mark partially changed outbox state = %+v", state)
		}
		fixture.requireOneEffect(t)
		t.Logf("D2/ack_received_mark_failed failpoint reached receiver_pid=%d", receiver.pid)
		processed, err = fixture.processor.ProcessOnce(context.Background())
		if processed != 1 || err != nil {
			t.Fatalf("mark-failure retry = (%d, %v)", processed, err)
		}
		fixture.requireDeliveredOnce(t, "D2/ack_received_mark_failed", receiver.pid)
	})

	t.Run("D2/receiver_crash_after_effect", func(t *testing.T) {
		fixture := newOutboxProcessFixture(t, "outbox-receiver-crash")
		receiver := startReceiverProcess(t, fixture.root, fixture.token, ReceiverFaultCrashAfterEffect)
		fixture.useEndpoint(receiver.endpoint)
		processed, err := fixture.processor.ProcessOnce(context.Background())
		if processed != 1 || err == nil {
			t.Fatalf("receiver crash result = (%d, %v), want lost ACK", processed, err)
		}
		fixture.requireOneEffect(t)
		waitErr, timeoutErr := waitForCommand(receiver.cmd, 5*time.Second)
		receiver.finished = true
		if timeoutErr != nil {
			t.Fatal(timeoutErr)
		}
		if waitErr == nil {
			t.Fatal("receiver returned success instead of crashing after its durable effect")
		}
		state := fixture.outboxState(t)
		if state.SendCount != 1 || state.DeliveredAt != nil {
			t.Fatalf("receiver-crash outbox state = %+v", state)
		}
		t.Logf("D2/receiver_crash_after_effect failpoint reached receiver_pid=%d", receiver.pid)
		restarted := startReceiverProcess(t, fixture.root, fixture.token, ReceiverFaultNone)
		fixture.useEndpoint(restarted.endpoint)
		processed, err = fixture.processor.ProcessOnce(context.Background())
		if processed != 1 || err != nil {
			t.Fatalf("post-crash retry = (%d, %v)", processed, err)
		}
		fixture.requireDeliveredOnce(t, "D2/receiver_crash_after_effect", restarted.pid)
	})
}
