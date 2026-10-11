package tui

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type herdrReporter struct {
	socketPath string
	paneID     string
	enabled    bool
	ch         chan []byte
	done       chan struct{}
	closeOnce  sync.Once

	mu        sync.Mutex
	seq       atomic.Uint64
	lastState string
	lastConv  string
}

func newHerdrReporter() *herdrReporter {
	if os.Getenv("HERDR_ENV") != "1" {
		return nil
	}
	socketPath := strings.TrimSpace(os.Getenv("HERDR_SOCKET_PATH"))
	paneID := strings.TrimSpace(os.Getenv("HERDR_PANE_ID"))
	if socketPath == "" || paneID == "" {
		return nil
	}
	r := &herdrReporter{
		socketPath: socketPath,
		paneID:     paneID,
		enabled:    true,
		ch:         make(chan []byte, 64),
		done:       make(chan struct{}),
	}
	go r.worker()
	return r
}

func (r *herdrReporter) worker() {
	for {
		select {
		case <-r.done:
			return
		case data, ok := <-r.ch:
			if !ok {
				return
			}
			r.sendRaw(data)
		}
	}
}

func (r *herdrReporter) sendRaw(data []byte) {
	conn, err := net.DialTimeout("unix", r.socketPath, 500*time.Millisecond)
	if err != nil {
		return
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(500 * time.Millisecond))
	_, _ = conn.Write(data)
	buf := make([]byte, 1024)
	_, _ = conn.Read(buf)
}

func (r *herdrReporter) queue(method string, params map[string]any) {
	if r == nil || !r.enabled {
		return
	}
	seq := r.seq.Add(1)
	params["pane_id"] = r.paneID
	params["source"] = "herdr:spyjo"
	params["seq"] = seq

	req := map[string]any{
		"id":     fmt.Sprintf("herdr:spyjo:%d", seq),
		"method": method,
		"params": params,
	}
	data, err := json.Marshal(req)
	if err != nil {
		return
	}
	data = append(data, '\n')
	select {
	case r.ch <- data:
	default:
	}
}

func (r *herdrReporter) ReportSession(conversationID, sessionPath string) {
	if r == nil || !r.enabled {
		return
	}
	conversationID = strings.TrimSpace(conversationID)
	r.mu.Lock()
	if r.lastConv == conversationID {
		r.mu.Unlock()
		return
	}
	r.lastConv = conversationID
	r.mu.Unlock()

	params := map[string]any{
		"agent":            "spyjo",
		"agent_session_id": conversationID,
	}
	if sessionPath != "" {
		params["agent_session_path"] = sessionPath
	}
	r.queue("pane.report_agent_session", params)
	r.queue("pane.report_metadata", map[string]any{
		"agent":         "spyjo",
		"display_agent": "Werkrby",
	})
}

func (r *herdrReporter) ReportState(state, conversationID, message string) {
	if r == nil || !r.enabled {
		return
	}
	state = strings.TrimSpace(state)
	conversationID = strings.TrimSpace(conversationID)
	r.mu.Lock()
	if r.lastState == state && r.lastConv == conversationID {
		r.mu.Unlock()
		return
	}
	r.lastState = state
	r.lastConv = conversationID
	r.mu.Unlock()

	params := map[string]any{
		"agent":            "spyjo",
		"state":            state,
		"agent_session_id": conversationID,
	}
	if message != "" {
		params["message"] = message
	}
	r.queue("pane.report_agent", params)
}

func (r *herdrReporter) Heartbeat(conversationID string) {
	if r == nil || !r.enabled {
		return
	}
	r.mu.Lock()
	currentState := r.lastState
	r.mu.Unlock()

	if currentState == "" {
		currentState = "idle"
	}
	params := map[string]any{
		"agent":            "spyjo",
		"state":            currentState,
		"agent_session_id": conversationID,
	}
	r.queue("pane.report_agent", params)
}

func (r *herdrReporter) Release() {
	if r == nil {
		return
	}
	r.closeOnce.Do(func() {
		params := map[string]any{
			"pane_id": r.paneID,
			"source":  "herdr:spyjo",
			"agent":   "spyjo",
		}
		seq := r.seq.Add(1)
		params["seq"] = seq
		req := map[string]any{
			"id":     fmt.Sprintf("herdr:spyjo:%d", seq),
			"method": "pane.release_agent",
			"params": params,
		}
		if data, err := json.Marshal(req); err == nil {
			data = append(data, '\n')
			r.sendRaw(data)
		}
		r.mu.Lock()
		r.enabled = false
		r.mu.Unlock()
		close(r.done)
	})
}
