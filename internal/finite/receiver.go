package finite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/agent0ai/spynel/internal/fsx"
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

type receiverRecord struct {
	DeliveryID    string `json:"delivery_id"`
	PayloadBase64 string `json:"payload_base64"`
}

func receiverDeliveryDir(root string) string { return filepath.Join(root, "deliveries") }

func receiverRecordPath(root, deliveryID string) string {
	hash := sha256.Sum256([]byte(deliveryID))
	return filepath.Join(receiverDeliveryDir(root), fmt.Sprintf("%x.json", hash))
}

// NewFileReceiver constructs a test-only durable file receiver.
func NewFileReceiver(cfg ReceiverConfig) (*FileReceiver, error) {
	if strings.TrimSpace(cfg.Root) == "" {
		return nil, fmt.Errorf("finite: receiver root is required")
	}
	root, err := filepath.Abs(cfg.Root)
	if err != nil {
		return nil, err
	}
	for _, part := range strings.Split(filepath.Clean(root), string(filepath.Separator)) {
		if strings.EqualFold(part, ".werkrby") {
			return nil, fmt.Errorf("finite: receiver root must be disposable, not legacy state")
		}
	}
	if cfg.Fault != ReceiverFaultNone && cfg.Fault != ReceiverFaultACKLost && cfg.Fault != ReceiverFaultCrashAfterEffect {
		return nil, fmt.Errorf("finite: unknown receiver fault %q", cfg.Fault)
	}
	cfg.Root = filepath.Clean(root)
	if err := os.MkdirAll(receiverDeliveryDir(cfg.Root), 0o700); err != nil {
		return nil, fmt.Errorf("finite: create receiver delivery directory: %w", err)
	}
	return &FileReceiver{cfg: cfg}, nil
}

func readReceiverRecord(path string) (ReceivedDelivery, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > 2*maxDeliveryPayloadBytes {
		return ReceivedDelivery{}, fmt.Errorf("finite: invalid receiver record")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ReceivedDelivery{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var record receiverRecord
	if err := decoder.Decode(&record); err != nil {
		return ReceivedDelivery{}, fmt.Errorf("finite: decode receiver record: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return ReceivedDelivery{}, fmt.Errorf("finite: receiver record must contain one JSON value")
	}
	payload, err := base64.StdEncoding.DecodeString(record.PayloadBase64)
	if err != nil || len(payload) == 0 || len(payload) > maxDeliveryPayloadBytes {
		return ReceivedDelivery{}, fmt.Errorf("finite: invalid receiver payload")
	}
	return ReceivedDelivery{DeliveryID: record.DeliveryID, Payload: payload}, nil
}

func duplicateReceiverEffect(path, deliveryID string, payload []byte) (bool, error) {
	existing, err := readReceiverRecord(path)
	if err != nil {
		return false, err
	}
	if existing.DeliveryID != deliveryID || !bytes.Equal(existing.Payload, payload) {
		return false, ErrDeliveryConflict
	}
	return false, nil
}

// AcceptDelivery persists one new effect or deduplicates identical bytes.
func (r *FileReceiver) AcceptDelivery(deliveryID string, payload []byte) (bool, error) {
	if r == nil {
		return false, fmt.Errorf("finite: nil receiver")
	}
	if err := validateDelivery(Delivery{ID: deliveryID, Payload: payload}); err != nil {
		return false, err
	}
	path := receiverRecordPath(r.cfg.Root, deliveryID)
	existing, err := readReceiverRecord(path)
	if err == nil {
		if existing.DeliveryID != deliveryID || !bytes.Equal(existing.Payload, payload) {
			return false, ErrDeliveryConflict
		}
		return false, nil
	}
	if !os.IsNotExist(err) && !errors.Is(err, os.ErrNotExist) {
		// A missing path is the only readable-error case that permits publication.
		if _, statErr := os.Lstat(path); statErr == nil || !os.IsNotExist(statErr) {
			return false, err
		}
	}
	record, err := json.Marshal(receiverRecord{DeliveryID: deliveryID, PayloadBase64: base64.StdEncoding.EncodeToString(payload)})
	if err != nil {
		return false, err
	}
	if err := fsx.AtomicCreateFile(path, record, 0o600); err != nil {
		if errors.Is(err, os.ErrExist) {
			return duplicateReceiverEffect(path, deliveryID, payload)
		}
		return false, fmt.Errorf("finite: persist receiver effect: %w", err)
	}
	return true, nil
}

// ServeHTTP accepts bounded authenticated receiver requests.
func (r *FileReceiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path != "/v1/deliveries" {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}
	if req.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	if r.cfg.Token == "" || !remoteIsLoopback(req) || !bearerAuthorized(req, r.cfg.Token) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	payload, err := io.ReadAll(http.MaxBytesReader(w, req.Body, maxDeliveryPayloadBytes))
	if err != nil {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "invalid delivery"})
		return
	}
	applied, err := r.AcceptDelivery(req.Header.Get("X-Werkrby-Delivery-ID"), payload)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, ErrDeliveryConflict) {
			status = http.StatusConflict
		}
		writeJSON(w, status, map[string]string{"error": "delivery rejected"})
		return
	}
	if applied {
		switch r.cfg.Fault {
		case ReceiverFaultACKLost:
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "acknowledgment unavailable"})
				return
			}
			connection, _, err := hijacker.Hijack()
			if err == nil {
				_ = connection.Close()
				return
			}
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "acknowledgment unavailable"})
			return
		case ReceiverFaultCrashAfterEffect:
			if r.cfg.Exit != nil {
				r.cfg.Exit()
			}
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "acknowledgment unavailable"})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// RunFileReceiver serves on an already-bound loopback listener.
func RunFileReceiver(ctx context.Context, listener net.Listener, cfg ReceiverConfig) error {
	if err := validateBearerToken(cfg.Token); err != nil {
		return err
	}
	receiver, err := NewFileReceiver(cfg)
	if err != nil {
		return err
	}
	return serveLoopbackHTTP(ctx, listener, receiver)
}

// ReadReceiverDeliveries reads durable effects for test verification.
func ReadReceiverDeliveries(root string) ([]ReceivedDelivery, error) {
	entries, err := os.ReadDir(receiverDeliveryDir(root))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	deliveries := make([]ReceivedDelivery, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		delivery, err := readReceiverRecord(filepath.Join(receiverDeliveryDir(root), entry.Name()))
		if err != nil {
			return nil, err
		}
		deliveries = append(deliveries, delivery)
	}
	return deliveries, nil
}
