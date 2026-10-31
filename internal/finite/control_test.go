package finite

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

type controlTestServer struct {
	store  *Store
	client ControlClient
	token  string
}

func newControlTestServer(t *testing.T) *controlTestServer {
	t.Helper()
	root := t.TempDir()
	owner, err := AcquireWorkspaceLock(root)
	if err != nil {
		t.Fatalf("acquire owner lock: %v", err)
	}
	store, err := OpenStore(Config{Root: root, WorkerDir: t.TempDir()})
	if err != nil {
		_ = owner.Close()
		t.Fatalf("open owner store: %v", err)
	}
	token := strings.Repeat("control-test-token-", 2)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = store.Close()
		_ = owner.Close()
		t.Fatalf("listen loopback: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- ServeControl(ctx, listener, store, owner, token) }()
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
		select {
		case <-serveErr:
		case <-time.After(2 * time.Second):
			t.Errorf("control server did not stop")
		}
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
		if err := owner.Close(); err != nil {
			t.Errorf("close owner lock: %v", err)
		}
	})
	return &controlTestServer{
		store: store,
		client: ControlClient{
			Endpoint: "http://" + listener.Addr().String(),
			Token:    token,
			Client:   &http.Client{Timeout: 2 * time.Second},
		},
		token: token,
	}
}

func TestFiniteControlAPI(t *testing.T) {
	t.Run("intake_status_stop", func(t *testing.T) {
		server := newControlTestServer(t)
		source := writeTestBrief(t, "control-intake", false)
		intaken, err := server.client.Intake(context.Background(), source)
		if err != nil {
			t.Fatalf("control intake: %v", err)
		}
		if intaken.ID != "control-intake" || intaken.State != StateTodo || intaken.Version != 0 {
			t.Fatalf("intake status = %+v", intaken)
		}
		status, err := server.client.Status(context.Background(), intaken.ID)
		if err != nil {
			t.Fatalf("control status: %v", err)
		}
		if status.State != StateTodo || status.ReviewRequired {
			t.Fatalf("unexpected bounded status: %+v", status)
		}
		response := controlRawRequest(t, server.client, http.MethodGet, "/v1/status?id="+url.QueryEscape(intaken.ID), nil, server.token)
		body, err := io.ReadAll(io.LimitReader(response.Body, 4096))
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("raw status code = %d, body = %s", response.StatusCode, body)
		}
		for _, secret := range []string{server.token, source, "Implement one finite objective", "BriefPath", "ProjectionPath", "JoeyAskJSON"} {
			if bytes.Contains(body, []byte(secret)) {
				t.Errorf("status response leaked %q: %s", secret, body)
			}
		}
		stopped, err := server.client.Stop(context.Background(), intaken.ID, intaken.Version)
		if err != nil {
			t.Fatalf("control stop: %v", err)
		}
		if stopped.State != StateCancelled || !stopped.CancelRequested || stopped.Version != 1 {
			t.Fatalf("stop status = %+v", stopped)
		}
	})

	t.Run("answer", func(t *testing.T) {
		server := newControlTestServer(t)
		task := intakeTestTask(t, server.store, "control-answer", false)
		run := dispatchTestRun(t, server.store, task, PhaseImplementation)
		ask, err := json.Marshal(JoeyAsk{Question: "Choose one", Options: []JoeyAskOption{{ID: "continue", Label: "Continue"}, {ID: "stop", Label: "Stop"}}})
		if err != nil {
			t.Fatal(err)
		}
		result := validTestResult(run, ResultWait)
		result.Wait = &Wait{Kind: WaitJoey, Reason: "A human choice is required", Ask: ask}
		waiting, err := server.store.SettleRun(context.Background(), SettlementRequest{
			RunID: run.ID, ExpectedTaskVersion: task.Version + 1, ExecutionOutcome: RunOK, Result: result,
		})
		if err != nil {
			t.Fatalf("settle Joey wait: %v", err)
		}
		answered, err := server.client.Answer(context.Background(), task.ID, waiting.Version, "continue")
		if err != nil {
			t.Fatalf("control answer: %v", err)
		}
		if answered.State != StateTodo || answered.WaitKind != "" || answered.Version != waiting.Version+1 {
			t.Fatalf("answer status = %+v", answered)
		}
	})

	t.Run("authentication_and_bounded_requests", func(t *testing.T) {
		server := newControlTestServer(t)
		response := controlRawRequest(t, server.client, http.MethodGet, "/v1/status?id=private-task", nil, "wrong-token")
		body, err := io.ReadAll(io.LimitReader(response.Body, 4096))
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusUnauthorized {
			t.Fatalf("unauthenticated status code = %d", response.StatusCode)
		}
		if bytes.Contains(body, []byte(server.token)) || bytes.Contains(body, []byte("private-task")) || bytes.Contains(body, []byte("SQLite")) {
			t.Fatalf("authentication response leaked details: %s", body)
		}
		oversized := bytes.Repeat([]byte("x"), maxControlRequestBytes+1)
		response = controlRawRequest(t, server.client, http.MethodPost, "/v1/intake", oversized, server.token)
		body, err = io.ReadAll(io.LimitReader(response.Body, 4096))
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusBadRequest && response.StatusCode != http.StatusRequestEntityTooLarge {
			t.Fatalf("oversized request status = %d, body = %s", response.StatusCode, body)
		}
	})

	t.Run("owner_and_loopback_required", func(t *testing.T) {
		if _, err := NewControlHandler(nil, nil, strings.Repeat("x", 32)); err == nil {
			t.Fatal("control handler accepted a missing owner lock")
		}
		client := ControlClient{Endpoint: "http://192.0.2.1:8080", Token: strings.Repeat("x", 32)}
		if _, err := client.Status(context.Background(), "task-id"); err == nil {
			t.Fatal("control client accepted a non-loopback endpoint")
		}
	})
}

func controlRawRequest(t *testing.T, client ControlClient, method, path string, body []byte, token string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, client.Endpoint+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	httpClient := client.Client
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 2 * time.Second}
	}
	response, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("control request %s %s: %v", method, path, err)
	}
	return response
}

func TestFiniteControlToken(t *testing.T) {
	first, err := NewControlToken()
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewControlToken()
	if err != nil {
		t.Fatal(err)
	}
	if len(first) < 32 || first == second {
		t.Fatalf("control tokens are not independent and sufficiently long")
	}
	if strings.ContainsAny(first, " \t\r\n") {
		t.Fatalf("control token contains whitespace")
	}
}

func ExampleControlClient_Status() {
	client := ControlClient{Endpoint: "http://127.0.0.1:8080", Token: "owner-provided-token"}
	_, _ = client.Status(context.Background(), "example-task")
	fmt.Println("status requested through the owner")
	// Output: status requested through the owner
}
