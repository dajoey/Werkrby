package finite

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"
)

var errDeliveryNotImplemented = errors.New("finite: delivery not implemented")

const maxControlRequestBytes = 16 * 1024

// ControlStatus is the bounded, non-secret task status exposed to owner clients.
type ControlStatus struct {
	ID              string     `json:"id"`
	State           State      `json:"state"`
	ReviewRequired  bool       `json:"review_required"`
	CancelRequested bool       `json:"cancel_requested"`
	Version         int64      `json:"version"`
	ResumeState     State      `json:"resume_state,omitempty"`
	WaitKind        WaitKind   `json:"wait_kind,omitempty"`
	WakeAt          *time.Time `json:"wake_at,omitempty"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

// ControlClient reaches the owner without opening the workflow database.
type ControlClient struct {
	Endpoint string
	Token    string
	Client   *http.Client
}

// NewControlToken returns a private owner-control bearer token.
func NewControlToken() (string, error) { return "", errDeliveryNotImplemented }

// NewControlHandler constructs the owner-lock-gated control handler.
func NewControlHandler(store *Store, owner *WorkspaceLock, token string) (http.Handler, error) {
	return nil, errDeliveryNotImplemented
}

// ServeControl serves the minimal API on an already-bound loopback listener.
func ServeControl(ctx context.Context, listener net.Listener, store *Store, owner *WorkspaceLock, token string) error {
	return errDeliveryNotImplemented
}

// Intake submits a bounded local brief path to the owner.
func (c ControlClient) Intake(ctx context.Context, sourcePath string) (ControlStatus, error) {
	return ControlStatus{}, errDeliveryNotImplemented
}

// Status reads one task's bounded status through the owner.
func (c ControlClient) Status(ctx context.Context, taskID string) (ControlStatus, error) {
	return ControlStatus{}, errDeliveryNotImplemented
}

// Stop requests a durable stop fence through the owner.
func (c ControlClient) Stop(ctx context.Context, taskID string, expectedVersion int64) (ControlStatus, error) {
	return ControlStatus{}, errDeliveryNotImplemented
}

// Answer completes a Joey wait with one offered option through the owner.
func (c ControlClient) Answer(ctx context.Context, taskID string, expectedVersion int64, optionID string) (ControlStatus, error) {
	return ControlStatus{}, errDeliveryNotImplemented
}
