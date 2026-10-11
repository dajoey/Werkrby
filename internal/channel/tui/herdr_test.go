package tui

import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestHerdrReporterLifecycle(t *testing.T) {
	tempDir := t.TempDir()
	sockPath := filepath.Join(tempDir, "herdr-test.sock")

	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("failed to listen on mock socket: %v", err)
	}
	defer listener.Close()

	received := make(chan map[string]any, 10)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				scanner := bufio.NewScanner(c)
				for scanner.Scan() {
					line := scanner.Bytes()
					var msg map[string]any
					if err := json.Unmarshal(line, &msg); err == nil {
						received <- msg
					}
					// Respond with success JSON
					_, _ = c.Write([]byte(`{"result":{"type":"ok"}}` + "\n"))
				}
			}(conn)
		}
	}()

	t.Setenv("HERDR_ENV", "1")
	t.Setenv("HERDR_SOCKET_PATH", sockPath)
	t.Setenv("HERDR_PANE_ID", "wTest:p1")

	reporter := newHerdrReporter()
	if reporter == nil {
		t.Fatal("expected newHerdrReporter to be non-nil")
	}

	// 1. Report session
	reporter.ReportSession("conv-test-1", "")

	select {
	case msg := <-received:
		if msg["method"] != "pane.report_agent_session" {
			t.Fatalf("expected pane.report_agent_session, got %v", msg["method"])
		}
		params := msg["params"].(map[string]any)
		if params["pane_id"] != "wTest:p1" || params["agent"] != "spyjo" || params["agent_session_id"] != "conv-test-1" {
			t.Fatalf("unexpected params: %#v", params)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for report_agent_session")
	}

	select {
	case msg := <-received:
		if msg["method"] != "pane.report_metadata" {
			t.Fatalf("expected pane.report_metadata, got %v", msg["method"])
		}
		params := msg["params"].(map[string]any)
		if params["display_agent"] != "Werkrby" {
			t.Fatalf("unexpected display_agent: %#v", params)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for report_metadata")
	}

	// 2. Report state working
	reporter.ReportState("working", "conv-test-1", "Thinking...")
	select {
	case msg := <-received:
		if msg["method"] != "pane.report_agent" {
			t.Fatalf("expected pane.report_agent, got %v", msg["method"])
		}
		params := msg["params"].(map[string]any)
		if params["state"] != "working" || params["message"] != "Thinking..." {
			t.Fatalf("unexpected params: %#v", params)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for report_agent working")
	}

	// 3. Duplicate report state working should be deduplicated
	reporter.ReportState("working", "conv-test-1", "Still thinking...")
	select {
	case msg := <-received:
		t.Fatalf("unexpected duplicate message: %#v", msg)
	case <-time.After(100 * time.Millisecond):
		// Expected: deduplicated
	}

	// 4. Report state idle
	reporter.ReportState("idle", "conv-test-1", "Ready")
	select {
	case msg := <-received:
		if msg["method"] != "pane.report_agent" {
			t.Fatalf("expected pane.report_agent, got %v", msg["method"])
		}
		params := msg["params"].(map[string]any)
		if params["state"] != "idle" {
			t.Fatalf("unexpected params: %#v", params)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for report_agent idle")
	}

	// 5. Release
	reporter.Release()
	select {
	case msg := <-received:
		if msg["method"] != "pane.release_agent" {
			t.Fatalf("expected pane.release_agent, got %v", msg["method"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for release_agent")
	}
}

func TestHerdrReporterDisabledWithoutEnv(t *testing.T) {
	_ = os.Unsetenv("HERDR_ENV")
	_ = os.Unsetenv("HERDR_SOCKET_PATH")
	_ = os.Unsetenv("HERDR_PANE_ID")

	reporter := newHerdrReporter()
	if reporter != nil {
		t.Fatal("expected reporter to be nil when environment is not set")
	}
	// Nil reporter calls should not panic
	reporter.ReportSession("c1", "")
	reporter.ReportState("working", "c1", "")
	reporter.Heartbeat("c1")
	reporter.Release()
}

func TestModelHerdrSync(t *testing.T) {
	tempDir := t.TempDir()
	sockPath := filepath.Join(tempDir, "herdr-model-test.sock")

	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("failed to listen on mock socket: %v", err)
	}
	defer listener.Close()

	received := make(chan map[string]any, 10)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				scanner := bufio.NewScanner(c)
				for scanner.Scan() {
					line := scanner.Bytes()
					var msg map[string]any
					if err := json.Unmarshal(line, &msg); err == nil {
						received <- msg
					}
					_, _ = c.Write([]byte(`{"result":{"type":"ok"}}` + "\n"))
				}
			}(conn)
		}
	}()

	t.Setenv("HERDR_ENV", "1")
	t.Setenv("HERDR_SOCKET_PATH", sockPath)
	t.Setenv("HERDR_PANE_ID", "wTest:pModel")

	reporter := newHerdrReporter()
	if reporter == nil {
		t.Fatal("expected newHerdrReporter to be non-nil")
	}
	defer reporter.Release()

	m := notificationTestModel()
	m.conversation = "conv-model-1"
	m.herdr = reporter

	// 1. Initially sync idle
	m.syncHerdr()
	select {
	case msg := <-received:
		if msg["method"] != "pane.report_agent" {
			t.Fatalf("expected pane.report_agent, got %v", msg["method"])
		}
		params := msg["params"].(map[string]any)
		if params["state"] != "idle" || params["agent_session_id"] != "conv-model-1" {
			t.Fatalf("unexpected params: %#v", params)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for initial idle report")
	}

	// 2. Working state sync
	m.working = true
	m.syncHerdr()
	select {
	case msg := <-received:
		if msg["method"] != "pane.report_agent" {
			t.Fatalf("expected pane.report_agent, got %v", msg["method"])
		}
		params := msg["params"].(map[string]any)
		if params["state"] != "working" {
			t.Fatalf("unexpected params: %#v", params)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for working report")
	}

	// 3. Back to idle
	m.working = false
	m.syncHerdr()
	select {
	case msg := <-received:
		if msg["method"] != "pane.report_agent" {
			t.Fatalf("expected pane.report_agent, got %v", msg["method"])
		}
		params := msg["params"].(map[string]any)
		if params["state"] != "idle" {
			t.Fatalf("unexpected params: %#v", params)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for idle report")
	}
}
