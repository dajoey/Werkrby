package finite

import (
	"context"
	"errors"
	"net"
	"net/http"
)

// ReceiverFault identifies a test-only post-effect acknowledgment fault.
type ReceiverFault string

const (
	ReceiverFaultNone             ReceiverFault = ""
	ReceiverFaultACKLost          ReceiverFault = "ack_lost"
	ReceiverFaultCrashAfterEffect ReceiverFault = "receiver_crash_after_effect"
)

// ErrDeliveryConflict rejects reuse of a delivery ID with different bytes.
var ErrDeliveryConflict = errors.New("finite: delivery ID payload conflict")

// ReceiverConfig identifies an explicit disposable file-receiver root.
type ReceiverConfig struct {
	Root  string
	Token string
	Fault ReceiverFault
	Exit  func()
}

// FileReceiver durably deduplicates visible effects independently of SQLite.
type FileReceiver struct {
	cfg ReceiverConfig
}

// ReceivedDelivery is one durable receiver effect and its exact original bytes.
type ReceivedDelivery struct {
	DeliveryID string
	Payload    []byte
}

// NewFileReceiver constructs a test-only durable file receiver.
func NewFileReceiver(cfg ReceiverConfig) (*FileReceiver, error) {
	return &FileReceiver{cfg: cfg}, nil
}

// AcceptDelivery persists one new effect or deduplicates identical bytes.
func (r *FileReceiver) AcceptDelivery(deliveryID string, payload []byte) (bool, error) {
	return false, errDeliveryNotImplemented
}

// ServeHTTP accepts bounded authenticated receiver requests.
func (r *FileReceiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}

// RunFileReceiver serves on an already-bound loopback listener.
func RunFileReceiver(ctx context.Context, listener net.Listener, cfg ReceiverConfig) error {
	return errDeliveryNotImplemented
}

// ReadReceiverDeliveries reads durable effects for test verification.
func ReadReceiverDeliveries(root string) ([]ReceivedDelivery, error) {
	return nil, nil
}
