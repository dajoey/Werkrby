package finite

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

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

// ControlError is a sanitized non-success response from the owner.
type ControlError struct {
	StatusCode int
	Message    string
}

func (e *ControlError) Error() string { return fmt.Sprintf("finite: owner returned %d: %s", e.StatusCode, e.Message) }

// ControlClient reaches the owner without opening the workflow database.
type ControlClient struct {
	Endpoint string
	Token    string
	Client   *http.Client
}

type controlHandler struct {
	store *Store
	owner *WorkspaceLock
	token string
}

// NewControlToken returns a private owner-control bearer token.
func NewControlToken() (string, error) {
	value := make([]byte, 32)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("finite: generate control token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}

// NewControlHandler constructs the owner-lock-gated control handler.
func NewControlHandler(store *Store, owner *WorkspaceLock, token string) (http.Handler, error) {
	if store == nil || owner == nil || owner.file == nil {
		return nil, fmt.Errorf("finite: control requires an open store under an acquired owner lock")
	}
	if err := validateBearerToken(token); err != nil {
		return nil, err
	}
	return &controlHandler{store: store, owner: owner, token: token}, nil
}

// ServeControl serves the minimal API on an already-bound loopback listener.
func ServeControl(ctx context.Context, listener net.Listener, store *Store, owner *WorkspaceLock, token string) error {
	handler, err := NewControlHandler(store, owner, token)
	if err != nil {
		return err
	}
	return serveLoopbackHTTP(ctx, listener, handler)
}

func controlStatus(task Task) ControlStatus {
	return ControlStatus{
		ID: task.ID, State: task.State, ReviewRequired: task.ReviewRequired,
		CancelRequested: task.CancelRequested, Version: task.Version, ResumeState: task.ResumeState,
		WaitKind: task.WaitKind, WakeAt: task.WakeAt, UpdatedAt: task.UpdatedAt,
	}
}

func (h *controlHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if !remoteIsLoopback(req) || !bearerAuthorized(req, h.token) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	if h.owner.file == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "owner unavailable"})
		return
	}
	switch req.URL.Path {
	case "/v1/intake":
		if req.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		h.intake(w, req)
	case "/v1/status":
		if req.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		h.status(w, req)
	case "/v1/stop":
		if req.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		h.stop(w, req)
	case "/v1/answer":
		if req.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		h.answer(w, req)
	default:
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
	}
}

func (h *controlHandler) intake(w http.ResponseWriter, req *http.Request) {
	var input struct {
		SourcePath string `json:"source_path"`
	}
	if err := decodeRequestJSON(w, req, &input, maxControlRequestBytes); err != nil {
		status := http.StatusBadRequest
		var maxBytes *http.MaxBytesError
		if errors.As(err, &maxBytes) {
			status = http.StatusRequestEntityTooLarge
		}
		writeJSON(w, status, map[string]string{"error": "invalid request"})
		return
	}
	if strings.TrimSpace(input.SourcePath) == "" || len(input.SourcePath) > 4096 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return
	}
	task, err := h.store.IntakeBrief(req.Context(), input.SourcePath)
	if err != nil {
		writeControlStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, controlStatus(task))
}

func (h *controlHandler) status(w http.ResponseWriter, req *http.Request) {
	taskID := req.URL.Query().Get("id")
	if !briefIDPattern.MatchString(taskID) || len(req.URL.Query()) != 1 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return
	}
	task, err := h.store.GetTask(req.Context(), taskID)
	if err != nil {
		writeControlStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, controlStatus(task))
}

func (h *controlHandler) stop(w http.ResponseWriter, req *http.Request) {
	var input struct {
		TaskID          string `json:"task_id"`
		ExpectedVersion int64  `json:"expected_version"`
	}
	if err := decodeRequestJSON(w, req, &input, maxControlRequestBytes); err != nil || !briefIDPattern.MatchString(input.TaskID) || input.ExpectedVersion < 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return
	}
	task, err := h.store.RequestStop(req.Context(), input.TaskID, input.ExpectedVersion)
	if err != nil {
		writeControlStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, controlStatus(task))
}

func (h *controlHandler) answer(w http.ResponseWriter, req *http.Request) {
	var input struct {
		TaskID          string `json:"task_id"`
		ExpectedVersion int64  `json:"expected_version"`
		OptionID        string `json:"option_id"`
	}
	if err := decodeRequestJSON(w, req, &input, maxControlRequestBytes); err != nil || !briefIDPattern.MatchString(input.TaskID) || input.ExpectedVersion < 0 || strings.TrimSpace(input.OptionID) == "" || len(input.OptionID) > 256 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return
	}
	task, err := h.store.AnswerJoey(req.Context(), input.TaskID, input.ExpectedVersion, input.OptionID)
	if err != nil {
		writeControlStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, controlStatus(task))
}

func writeControlStoreError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	message := "internal error"
	switch {
	case errors.Is(err, ErrNotFound):
		status, message = http.StatusNotFound, "not found"
	case errors.Is(err, ErrVersionConflict), errors.Is(err, ErrStateConflict), errors.Is(err, ErrSettlementConflict):
		status, message = http.StatusConflict, "conflict"
	}
	writeJSON(w, status, map[string]string{"error": message})
}

// Intake submits a bounded local brief path to the owner.
func (c ControlClient) Intake(ctx context.Context, sourcePath string) (ControlStatus, error) {
	if strings.TrimSpace(sourcePath) == "" || len(sourcePath) > 4096 {
		return ControlStatus{}, fmt.Errorf("finite: invalid intake source path")
	}
	return c.call(ctx, http.MethodPost, "/v1/intake", map[string]string{"source_path": sourcePath})
}

// Status reads one task's bounded status through the owner.
func (c ControlClient) Status(ctx context.Context, taskID string) (ControlStatus, error) {
	if !briefIDPattern.MatchString(taskID) {
		return ControlStatus{}, fmt.Errorf("finite: invalid task id")
	}
	return c.call(ctx, http.MethodGet, "/v1/status?id="+url.QueryEscape(taskID), nil)
}

// Stop requests a durable stop fence through the owner.
func (c ControlClient) Stop(ctx context.Context, taskID string, expectedVersion int64) (ControlStatus, error) {
	if !briefIDPattern.MatchString(taskID) || expectedVersion < 0 {
		return ControlStatus{}, fmt.Errorf("finite: invalid stop request")
	}
	return c.call(ctx, http.MethodPost, "/v1/stop", map[string]any{"task_id": taskID, "expected_version": expectedVersion})
}

// Answer completes a Joey wait with one offered option through the owner.
func (c ControlClient) Answer(ctx context.Context, taskID string, expectedVersion int64, optionID string) (ControlStatus, error) {
	if !briefIDPattern.MatchString(taskID) || expectedVersion < 0 || strings.TrimSpace(optionID) == "" || len(optionID) > 256 {
		return ControlStatus{}, fmt.Errorf("finite: invalid answer request")
	}
	return c.call(ctx, http.MethodPost, "/v1/answer", map[string]any{"task_id": taskID, "expected_version": expectedVersion, "option_id": optionID})
}

func (c ControlClient) call(ctx context.Context, method, path string, input any) (ControlStatus, error) {
	endpoint, err := validateLoopbackEndpoint(c.Endpoint)
	if err != nil {
		return ControlStatus{}, err
	}
	if err := validateBearerToken(c.Token); err != nil {
		return ControlStatus{}, err
	}
	var body io.Reader
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil || len(encoded) > maxControlRequestBytes {
			return ControlStatus{}, fmt.Errorf("finite: control request exceeds its bound")
		}
		body = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint.String()+path, body)
	if err != nil {
		return ControlStatus{}, err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := c.Client
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	response, err := client.Do(req)
	if err != nil {
		return ControlStatus{}, fmt.Errorf("finite: owner request failed: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var failure struct {
			Error string `json:"error"`
		}
		if err := decodeResponseJSON(response.Body, &failure, 4096); err != nil || failure.Error == "" {
			failure.Error = "request rejected"
		}
		return ControlStatus{}, &ControlError{StatusCode: response.StatusCode, Message: failure.Error}
	}
	var status ControlStatus
	if err := decodeResponseJSON(response.Body, &status, maxControlResponseBytes); err != nil {
		return ControlStatus{}, err
	}
	return status, nil
}
