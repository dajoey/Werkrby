package tui

import (
	"context"
	"errors"
	"testing"

	"github.com/agent0ai/spynel/internal/channel"
	"github.com/agent0ai/spynel/internal/channel/tui/textarea"
	"github.com/agent0ai/spynel/internal/core"
	"github.com/charmbracelet/bubbles/viewport"
)

func notificationTestModel() model {
	input := textarea.New()
	return model{ctx: context.Background(), input: input, viewport: viewport.New(80, 20), events: make(chan core.Event, 8)}
}

func TestNotificationSplitsStreamOnlyAtIdleBoundary(t *testing.T) {
	m := notificationTestModel()
	m.ackNotification = func(string, int) error { return nil }
	m.working = true
	m.streaming = "Hello "
	m.responseText = "Hello "
	m.deltaSequence = 3
	updated, _ := m.Update(taskNotificationEvent{notification: channel.Notification{ID: "n1", Text: "Task complete"}})
	m = updated.(model)
	updated, _ = m.Update(notificationPauseMsg{sequence: 3})
	m = updated.(model)
	if len(m.transcript) != 0 {
		t.Fatal("notification rendered before durable acknowledgement")
	}
	updated, _ = m.Update(notificationAckMsg{ids: []string{"n1"}, after: 6})
	m = updated.(model)
	if len(m.transcript) != 2 || m.transcript[0].text != "Hello " || m.transcript[1].text != "Task complete" || m.streaming != "" {
		t.Fatalf("split state = %#v, stream %q", m.transcript, m.streaming)
	}
	updated, _ = m.Update(uiEvent{event: core.Event{Kind: core.EventDelta, Text: "world"}})
	m = updated.(model)
	updated, _ = m.Update(uiEvent{event: core.Event{Kind: core.EventFinal, Text: "Hello world", Done: true}})
	m = updated.(model)
	if len(m.transcript) != 3 || m.transcript[2].text != "world" {
		t.Fatalf("continued response = %#v", m.transcript)
	}
}

func TestNotificationReconsidersLaterSafePause(t *testing.T) {
	m := notificationTestModel()
	m.ackNotification = func(string, int) error { return nil }
	m.working = true
	m.streaming = "hel"
	m.responseText = "hel"
	m.deltaSequence = 1
	updated, _ := m.Update(taskNotificationEvent{notification: channel.Notification{ID: "n1", Text: "Task complete"}})
	m = updated.(model)
	updated, _ = m.Update(notificationPauseMsg{sequence: 1})
	m = updated.(model)
	if m.notificationAckBusy {
		t.Fatal("unsafe boundary started acknowledgement")
	}
	updated, _ = m.Update(uiEvent{event: core.Event{Kind: core.EventDelta, Text: "lo "}})
	m = updated.(model)
	updated, _ = m.Update(notificationPauseMsg{sequence: 2})
	m = updated.(model)
	if !m.notificationAckBusy || m.notificationAckAfter != 6 {
		t.Fatalf("later safe pause was not reconsidered: busy=%v after=%d", m.notificationAckBusy, m.notificationAckAfter)
	}
	updated, _ = m.Update(notificationAckMsg{ids: []string{"n1"}, after: 6})
	m = updated.(model)
	if len(m.transcript) != 2 || m.transcript[0].text != "hello " || m.transcript[1].text != "Task complete" {
		t.Fatalf("later split state = %#v", m.transcript)
	}
}

func TestSafeNotificationBoundaryDoesNotSplitMarkdownTokens(t *testing.T) {
	for _, text := range []string{
		"https:", "https://example.com.", "[label].", "`code`.", "**bold**.",
		"[open label ", "`open code ", "**open bold ",
		"# ", "## Heading ", "> ", "> quote ", ">> nested ", "- ", "- item ", "+ ", "* ", "1. ", "12) item ", "    code ",
	} {
		if safeNotificationBoundary(text) {
			t.Errorf("boundary %q split a Markdown or URL token", text)
		}
	}
	for _, text := range []string{
		"hello ", "Done.", "Really?", "Great!", "[closed label](target) ", "`closed code` ", "**closed bold** ",
		"# Heading\n", "> quote\n", "- item\n", "1. item\n",
	} {
		if !safeNotificationBoundary(text) {
			t.Errorf("boundary %q was not accepted", text)
		}
	}
}

func TestNotificationWaitsForAcknowledgementAndDefersTerminal(t *testing.T) {
	m := notificationTestModel()
	m.ackNotification = func(string, int) error { return nil }
	m.working = true
	m.streaming = "Hello "
	m.responseText = "Hello "
	m.deltaSequence = 1
	m.pendingNotifications = []channel.Notification{{ID: "n1", Text: "Task complete"}}
	updated, _ := m.Update(notificationPauseMsg{sequence: 1})
	m = updated.(model)
	updated, _ = m.Update(uiEvent{event: core.Event{Kind: core.EventFinal, Text: "Hello world", Done: true}})
	m = updated.(model)
	if len(m.transcript) != 0 || len(m.deferredUIEvents) != 1 {
		t.Fatalf("terminal was not deferred: transcript=%#v deferred=%#v", m.transcript, m.deferredUIEvents)
	}
	updated, _ = m.Update(notificationAckMsg{ids: []string{"n1"}, after: 6, err: errors.New("temporary")})
	m = updated.(model)
	if len(m.transcript) != 0 || !m.notificationAckBusy {
		t.Fatal("failed acknowledgement changed visible ordering")
	}
	updated, _ = m.Update(notificationAckMsg{ids: []string{"n1"}, after: 6})
	m = updated.(model)
	if len(m.transcript) != 2 || m.transcript[0].text != "Hello " || m.transcript[1].text != "Task complete" || m.notificationAckBusy {
		t.Fatalf("acknowledged split = %#v busy=%v", m.transcript, m.notificationAckBusy)
	}
	if len(m.deferredUIEvents) != 0 {
		t.Fatalf("deferred event was not queued for replay: %#v", m.deferredUIEvents)
	}
	updated, _ = m.Update(uiEvent{event: core.Event{Kind: core.EventFinal, Text: "Hello world", Done: true}})
	m = updated.(model)
	if len(m.transcript) != 3 || m.transcript[2].text != "world" {
		t.Fatalf("replayed terminal order = %#v", m.transcript)
	}
}

func TestNotificationAcknowledgementPreservesDeltasReceivedInFlight(t *testing.T) {
	m := notificationTestModel()
	m.ackNotification = func(string, int) error { return nil }
	m.working = true
	m.streaming = "Hello "
	m.responseText = "Hello "
	m.deltaSequence = 1
	m.pendingNotifications = []channel.Notification{{ID: "n1", Text: "Task complete"}}
	updated, _ := m.Update(notificationPauseMsg{sequence: 1})
	m = updated.(model)
	updated, _ = m.Update(uiEvent{event: core.Event{Kind: core.EventDelta, Text: "world"}})
	m = updated.(model)
	updated, _ = m.Update(notificationAckMsg{ids: []string{"n1"}, after: 6})
	m = updated.(model)
	if len(m.transcript) != 2 || m.transcript[0].text != "Hello " || m.transcript[1].text != "Task complete" || m.streaming != "world" {
		t.Fatalf("in-flight delta split = %#v, stream %q", m.transcript, m.streaming)
	}
}

func TestLateNotificationAckAfterUserSendDoesNotDuplicateCommittedText(t *testing.T) {
	m := notificationTestModel()
	m.handler = channel.Handler(func(context.Context, core.Message, core.Emit) error { return nil })
	m.ackNotification = func(string, int) error { return nil }
	m.working = true
	m.streaming = "Hello "
	m.responseText = "Hello "
	m.deltaSequence = 1
	m.pendingNotifications = []channel.Notification{{ID: "n1", Text: "Task complete"}}
	updated, _ := m.Update(notificationPauseMsg{sequence: 1})
	m = updated.(model)
	updated, _ = m.Update(uiEvent{event: core.Event{Kind: core.EventDelta, Text: "world and more"}})
	m = updated.(model)
	// The user sends while the acknowledgement is still in flight: the whole
	// stream is committed before the ack for the earlier boundary lands.
	m.dispatchMessage("next", "next")
	updated, _ = m.Update(notificationAckMsg{ids: []string{"n1"}, after: 6})
	m = updated.(model)
	if m.streaming != "" {
		t.Fatalf("committed text came back as live stream: %q", m.streaming)
	}
	want := []string{"Hello world and more", "next", "Task complete"}
	if len(m.transcript) != len(want) {
		t.Fatalf("transcript = %#v", m.transcript)
	}
	for i, text := range want {
		if m.transcript[i].text != text {
			t.Fatalf("transcript[%d] = %q, want %q", i, m.transcript[i].text, text)
		}
	}
}

func TestStaleNotificationAckAfterConversationSwitchIsInert(t *testing.T) {
	m := notificationTestModel()
	m.ackNotification = func(string, int) error { return nil }
	m.working = true
	m.streaming = "Old stream "
	m.responseText = "Old stream "
	m.deltaSequence = 1
	m.pendingNotifications = []channel.Notification{{ID: "n1", Text: "Task complete"}}
	updated, _ := m.Update(notificationPauseMsg{sequence: 1})
	m = updated.(model)
	if !m.notificationAckBusy {
		t.Fatal("acknowledgement did not start")
	}
	// The conversation switches while the acknowledgement is in flight.
	m.openScreen(core.Screen{ID: "welcome", Conversation: "other"})
	updated, _ = m.Update(uiEvent{event: core.Event{Kind: core.EventDelta, Text: "Brand new reply"}})
	m = updated.(model)
	updated, _ = m.Update(notificationAckMsg{ids: []string{"n1"}, after: 11})
	m = updated.(model)
	if m.streaming != "Brand new reply" || m.responseCommit != 0 {
		t.Fatalf("stale ack split the new stream: streaming=%q commit=%d", m.streaming, m.responseCommit)
	}
	for _, entry := range m.transcript {
		if entry.text == "Brand new " || entry.text == "Brand new" {
			t.Fatalf("stale ack committed new text: %#v", m.transcript)
		}
	}
	// The pending notification is still flushable once the new stream ends.
	updated, _ = m.Update(uiEvent{event: core.Event{Kind: core.EventFinal, Text: "Brand new reply", Done: true}})
	m = updated.(model)
	if len(m.transcript) != 2 || m.transcript[0].text != "Brand new reply" || m.transcript[1].text != "Task complete" {
		t.Fatalf("pending notification lost after reset: %#v", m.transcript)
	}
}

func TestNotificationNoPauseFallbackAndMultipleQueue(t *testing.T) {
	m := notificationTestModel()
	m.working = true
	m.streaming = "hel"
	m.responseText = "hel"
	m.deltaSequence = 1
	for _, text := range []string{"one", "two"} {
		updated, _ := m.Update(taskNotificationEvent{notification: channel.Notification{ID: text, Text: text}})
		m = updated.(model)
	}
	updated, _ := m.Update(notificationPauseMsg{sequence: 1})
	m = updated.(model)
	if len(m.transcript) != 0 {
		t.Fatal("notification split an unsafe token")
	}
	updated, _ = m.Update(uiEvent{event: core.Event{Kind: core.EventFinal, Text: "hello", Done: true}})
	m = updated.(model)
	if len(m.transcript) != 3 || m.transcript[0].text != "hello" || m.transcript[1].text != "one" || m.transcript[2].text != "two" {
		t.Fatalf("fallback order = %#v", m.transcript)
	}
}

func TestNotificationFlushesAfterCancellation(t *testing.T) {
	m := notificationTestModel()
	m.working = true
	m.streaming = "partial"
	m.responseText = "partial"
	m.pendingNotifications = []channel.Notification{{ID: "n1", Text: "review failed"}}
	updated, _ := m.Update(uiEvent{event: core.Event{Kind: core.EventError, Text: "cancelled", Done: true}})
	m = updated.(model)
	if len(m.transcript) != 3 || m.transcript[0].text != "partial" || m.transcript[1].role != "error" || m.transcript[2].text != "review failed" {
		t.Fatalf("cancellation order = %#v", m.transcript)
	}
}
