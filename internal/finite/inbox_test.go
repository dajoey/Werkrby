package finite

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The run inbox is the engine's channel into one live provider turn: an
// append-only JSONL file in the run directory that the worker reads between
// steps (the prompt contract), never a second provider call for the attempt.
func TestFiniteRunInbox(t *testing.T) {
	t.Run("send_dedup_withdraw_read", func(t *testing.T) {
		server := newControlTestServer(t)
		task := intakeTestTask(t, server.store, "inbox-flow", false)
		run := dispatchTestRun(t, server.store, task, PhaseImplementation)

		reply, err := server.client.Message(context.Background(), task.ID, "message", "ctl-1", "steer left at the fork")
		if err != nil {
			t.Fatalf("send message: %v", err)
		}
		if !reply.OK || reply.RunID != run.ID || !reply.Queued || reply.MessageID != "ctl-1" {
			t.Fatalf("send reply = %+v", reply)
		}
		messages := readInboxTestLines(t, run.RunDir)
		if len(messages) != 1 || messages[0].ID != "ctl-1" || messages[0].Kind != "message" || messages[0].Text != "steer left at the fork" {
			t.Fatalf("inbox lines = %+v", messages)
		}

		// A retried send with the same id never delivers twice.
		duplicate, err := server.client.Message(context.Background(), task.ID, "message", "ctl-1", "steer left at the fork")
		if err != nil {
			t.Fatalf("duplicate send: %v", err)
		}
		if !duplicate.Duplicate || duplicate.Queued {
			t.Fatalf("duplicate reply = %+v", duplicate)
		}
		if len(readInboxTestLines(t, run.RunDir)) != 1 {
			t.Fatalf("duplicate send appended a second line")
		}

		if _, err := server.client.Message(context.Background(), task.ID, "message", "ctl-2", "also check the logs"); err != nil {
			t.Fatalf("second message: %v", err)
		}

		// Withdrawal of an unacknowledged message removes it before delivery.
		withdrawn, err := server.client.Withdraw(context.Background(), task.ID, "ctl-1")
		if err != nil {
			t.Fatalf("withdraw: %v", err)
		}
		if !withdrawn.OK || !withdrawn.Cancelled {
			t.Fatalf("withdraw reply = %+v", withdrawn)
		}
		for _, message := range readInboxTestLines(t, run.RunDir) {
			if message.ID == "ctl-1" {
				t.Fatalf("ctl-1 survived withdrawal: %+v", message)
			}
		}
		again, err := server.client.Withdraw(context.Background(), task.ID, "ctl-1")
		if err != nil {
			t.Fatalf("second withdraw: %v", err)
		}
		if again.Cancelled {
			t.Fatalf("withdrawing an unknown id reported cancelled: %+v", again)
		}

		// Read-back sees the surviving message only.
		read, err := server.client.Ping(context.Background(), task.ID)
		if err != nil {
			t.Fatalf("read inbox: %v", err)
		}
		if !read.OK || read.RunID != run.ID || len(read.Messages) != 1 || read.Messages[0].ID != "ctl-2" {
			t.Fatalf("read reply = %+v", read)
		}
		if len(read.AckedIDs) != 0 {
			t.Fatalf("no ack was written but acked = %+v", read.AckedIDs)
		}
	})

	t.Run("interrupt_upgrades_a_queued_message_in_place", func(t *testing.T) {
		server := newControlTestServer(t)
		task := intakeTestTask(t, server.store, "inbox-upgrade", false)
		run := dispatchTestRun(t, server.store, task, PhaseImplementation)
		if _, err := server.client.Message(context.Background(), task.ID, "message", "ctl-x", "hold on"); err != nil {
			t.Fatalf("send message: %v", err)
		}
		reply, err := server.client.Message(context.Background(), task.ID, "interrupt", "ctl-x", "hold on, drop everything else")
		if err != nil {
			t.Fatalf("upgrade to interrupt: %v", err)
		}
		if !reply.Upgraded || reply.Duplicate {
			t.Fatalf("upgrade reply = %+v", reply)
		}
		messages := readInboxTestLines(t, run.RunDir)
		if len(messages) != 1 || messages[0].Kind != "interrupt" || messages[0].ID != "ctl-x" {
			t.Fatalf("inbox after upgrade = %+v", messages)
		}
	})

	t.Run("acknowledged_message_cannot_be_withdrawn_or_upgraded", func(t *testing.T) {
		server := newControlTestServer(t)
		task := intakeTestTask(t, server.store, "inbox-ack", false)
		run := dispatchTestRun(t, server.store, task, PhaseImplementation)
		_ = run
		if _, err := server.client.Message(context.Background(), task.ID, "message", "ctl-a", "steer right"); err != nil {
			t.Fatalf("send message: %v", err)
		}
		ack := "ctl-a\nctl-other\n"
		if err := os.WriteFile(filepath.Join(run.RunDir, inboxAckFile), []byte(ack), 0o600); err != nil {
			t.Fatal(err)
		}
		withdrawn, err := server.client.Withdraw(context.Background(), task.ID, "ctl-a")
		if err != nil {
			t.Fatalf("withdraw acked: %v", err)
		}
		if withdrawn.Cancelled || !withdrawn.Delivered {
			t.Fatalf("acked withdraw reply = %+v", withdrawn)
		}
		upgraded, err := server.client.Message(context.Background(), task.ID, "interrupt", "ctl-a", "urgent now")
		if err != nil {
			t.Fatalf("upgrade acked: %v", err)
		}
		if upgraded.Upgraded || !upgraded.Duplicate || !upgraded.Delivered {
			t.Fatalf("acked upgrade reply = %+v", upgraded)
		}
		read, err := server.client.Ping(context.Background(), task.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(read.AckedIDs) != 2 || read.AckedIDs[0] != "ctl-a" {
			t.Fatalf("acked ids = %+v", read.AckedIDs)
		}
	})

	t.Run("refusals", func(t *testing.T) {
		server := newControlTestServer(t)
		task := intakeTestTask(t, server.store, "inbox-refuse", false)
		dispatchTestRun(t, server.store, task, PhaseImplementation)

		// Unknown task, unknown run target, and shape errors each refuse.
		if reply := controlRawRequest(t, server.client, http.MethodPost, "/v1/message",
			mustTestJSON(t, map[string]any{"task_id": "inbox-nope", "kind": "message", "id": "x", "text": "hi"}), server.token); reply.StatusCode != http.StatusNotFound {
			t.Fatalf("unknown task status = %d", reply.StatusCode)
		}
		idle := intakeTestTask(t, server.store, "inbox-idle", false)
		if reply := controlRawRequest(t, server.client, http.MethodPost, "/v1/message",
			mustTestJSON(t, map[string]any{"task_id": idle.ID, "kind": "message", "id": "x", "text": "hi"}), server.token); reply.StatusCode != http.StatusConflict {
			t.Fatalf("no open run status = %d", reply.StatusCode)
		}
		for name, body := range map[string]map[string]any{
			"empty text": {"task_id": task.ID, "kind": "message", "id": "x", "text": "  "},
			"bad kind":   {"task_id": task.ID, "kind": "shout", "id": "x", "text": "hi"},
			"bad id":     {"task_id": task.ID, "kind": "message", "id": "two\nlines", "text": "hi"},
		} {
			if reply := controlRawRequest(t, server.client, http.MethodPost, "/v1/message",
				mustTestJSON(t, body), server.token); reply.StatusCode != http.StatusBadRequest {
				t.Fatalf("%s status = %d", name, reply.StatusCode)
			}
		}
		if reply := controlRawRequest(t, server.client, http.MethodPost, "/v1/withdraw",
			mustTestJSON(t, map[string]any{"task_id": task.ID}), server.token); reply.StatusCode != http.StatusBadRequest {
			t.Fatalf("withdraw without id status = %d", reply.StatusCode)
		}
	})

	t.Run("delivery_never_opens_a_second_run", func(t *testing.T) {
		server := newControlTestServer(t)
		task := intakeTestTask(t, server.store, "inbox-onecall", false)
		dispatchTestRun(t, server.store, task, PhaseImplementation)
		before := countRows(t, server.store, `SELECT COUNT(*) FROM runs WHERE task_id=?`, task.ID)
		for i := 0; i < 3; i++ {
			if _, err := server.client.Message(context.Background(), task.ID, "message", "", "ping "+strings.Repeat("x", i+1)); err != nil {
				t.Fatalf("send %d: %v", i, err)
			}
		}
		if after := countRows(t, server.store, `SELECT COUNT(*) FROM runs WHERE task_id=?`, task.ID); after != before {
			t.Fatalf("runs changed from %d to %d", before, after)
		}
		messages := readInboxTestLines(t, dispatchTestRunRunDir(t, server.store, task))
		if len(messages) != 3 {
			t.Fatalf("expected three generated-id messages, got %+v", messages)
		}
	})

	t.Run("read_carries_last_activity", func(t *testing.T) {
		server := newControlTestServer(t)
		task := intakeTestTask(t, server.store, "inbox-activity", false)
		run := dispatchTestRun(t, server.store, task, PhaseImplementation)
		events := "{\"kind\":\"status\",\"text\":\"Finite: run working (30s)...\"}\n{\"kind\":\"status\",\"text\":\"Finite: run working (1m0s)...\"}\n"
		if err := os.WriteFile(filepath.Join(run.RunDir, "events.jsonl"), []byte(events), 0o600); err != nil {
			t.Fatal(err)
		}
		read, err := server.client.Ping(context.Background(), task.ID)
		if err != nil {
			t.Fatal(err)
		}
		if read.LastActivity != "Finite: run working (1m0s)..." {
			t.Fatalf("last activity = %q", read.LastActivity)
		}
	})

	t.Run("status_names_the_open_run", func(t *testing.T) {
		server := newControlTestServer(t)
		task := intakeTestTask(t, server.store, "inbox-status", false)
		run := dispatchTestRun(t, server.store, task, PhaseImplementation)
		status, err := server.client.Status(context.Background(), task.ID)
		if err != nil {
			t.Fatal(err)
		}
		if status.RunID != run.ID || status.RunPhase != PhaseImplementation {
			t.Fatalf("status run = %d/%s, want %d/%s", status.RunID, status.RunPhase, run.ID, PhaseImplementation)
		}
		idle := intakeTestTask(t, server.store, "inbox-status-idle", false)
		idleStatus, err := server.client.Status(context.Background(), idle.ID)
		if err != nil {
			t.Fatal(err)
		}
		if idleStatus.RunID != 0 {
			t.Fatalf("idle task names run %d", idleStatus.RunID)
		}
	})

	t.Run("prompt_teaches_the_inbox_contract", func(t *testing.T) {
		prompt, err := BuildPrompt(PromptRequest{TaskID: "t", RunID: 1, Phase: PhaseImplementation, Dynamic: "d"})
		if err != nil {
			t.Fatal(err)
		}
		for _, needle := range []string{"RUN INBOX", "inbox.jsonl", "inbox-ack.jsonl"} {
			if !strings.Contains(prompt.Text, needle) {
				t.Fatalf("prompt lacks %q", needle)
			}
		}
	})
}

func readInboxTestLines(t *testing.T, runDir string) []InboxMessage {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(runDir, inboxFile))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatal(err)
	}
	messages, err := parseInboxLines(data)
	if err != nil {
		t.Fatal(err)
	}
	return messages
}

func dispatchTestRunRunDir(t *testing.T, store *Store, task Task) string {
	t.Helper()
	run, err := store.OpenRunForTask(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	return run.RunDir
}

func mustTestJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
