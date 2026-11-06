package app

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/history"
	"github.com/agent0ai/spynel/internal/orchestrator"
	"github.com/agent0ai/spynel/internal/roster"
)

// quotaChatHarness fails dispatches with a provider quota wall and answers only on the next
// roster seat, so the chat fence must move the turn down the chain without ever delivering the
// raw runner error text.
type quotaChatHarness struct {
	*serviceHarness
	mu       sync.Mutex
	models   []string
	failMode string // "first-seat", "all-seats", "non-quota"
}

const quotaWallText = "runner turn failed: HTTP 429: Weekly/Monthly Limit Exhausted for plan zai; resets in 45h10m [rate_limit]"

func (h *quotaChatHarness) Send(_ context.Context, key, prompt string, emit core.Emit) (string, bool, error) {
	return h.SendWithModel(context.Background(), key, prompt, "", emit)
}

func (h *quotaChatHarness) SendWithModel(_ context.Context, key, prompt, model string, emit core.Emit) (string, bool, error) {
	h.mu.Lock()
	h.models = append(h.models, model)
	failMode := h.failMode
	h.mu.Unlock()
	switch {
	case failMode == "non-quota":
		return "", false, errors.New("runner turn failed: read tasks/todo/fix-issue.md 20260429: no such file")
	case failMode == "all-seats" || model != "@seat-b":
		return "", false, errors.New(quotaWallText)
	}
	emit(core.Event{Kind: core.EventFinal, Text: "Answered on the fallback seat.", Done: true})
	return "thread-b", false, nil
}

func (h *quotaChatHarness) SetModel(string) {}

func (h *quotaChatHarness) tried() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.models...)
}

// writeChatChainRoster writes a two-seat chat chain: seat-a falls back to seat-b.
func writeChatChainRoster(t *testing.T, cfg config.Config) {
	t.Helper()
	rosterYAML := "roles:\n  chat: seat-a\nstaff:\n  seat-a:\n    runner: hermes\n    fallback: seat-b\n  seat-b:\n    runner: hermes\n"
	if err := os.WriteFile(roster.Path(cfg.StatePath()), []byte(rosterYAML), 0o644); err != nil {
		t.Fatal(err)
	}
}

func collectEvents() (func(core.Event), func() []core.Event) {
	var mu sync.Mutex
	var delivered []core.Event
	return func(e core.Event) {
			mu.Lock()
			delivered = append(delivered, e)
			mu.Unlock()
		}, func() []core.Event {
			mu.Lock()
			defer mu.Unlock()
			return append([]core.Event(nil), delivered...)
		}
}

// A chat turn that runs into a provider quota wall is re-dispatched on the next seat of the
// chat roster chain in the same turn, and the raw runner error text never reaches the channel.
func TestChatQuotaWallMovesToNextSeatAndNeverDeliversRawText(t *testing.T) {
	service, _, cfg := newRecoveryTestService(t)
	writeChatChainRoster(t, cfg)
	target := &quotaChatHarness{serviceHarness: newServiceHarness()}
	service.Harness = target
	collect, delivered := collectEvents()
	message := core.Message{Channel: "telegram", Conversation: "4242", Sender: "joey", SourceMessageID: "telegram:4242:1", Text: "is the backup done?"}

	if err := service.dispatchHarnessPrompt(context.Background(), message, "chat prompt body", collect); err != nil {
		t.Fatalf("dispatchHarnessPrompt returned %v, want the chain retry to answer", err)
	}
	if tried := target.tried(); len(tried) != 2 || tried[0] != "" || tried[1] != "@seat-b" {
		t.Fatalf("models tried = %q, want the default dispatch then the next roster seat", tried)
	}
	for _, event := range delivered() {
		if strings.Contains(event.Text, "429") || strings.Contains(event.Text, "runner turn failed") || strings.Contains(event.Text, "rate_limit") {
			t.Fatalf("raw runner error text reached the channel: %+v", event)
		}
	}
	var finals []string
	for _, event := range delivered() {
		if event.Kind == core.EventFinal && event.Done {
			finals = append(finals, event.Text)
		}
	}
	if len(finals) != 1 || finals[0] != "Answered on the fallback seat." {
		t.Fatalf("EventFinal texts = %q, want only the fallback seat's answer", finals)
	}
}

// With every seat behind a quota wall the question is held with one plain sentence that names
// the reset time, a requeue is scheduled for the reset, and the stalled source is settled so
// the recovery scan never re-fires it while the quota is still out.
func TestChatQuotaWallWithNoSeatLeftHoldsTheQuestionForTheReset(t *testing.T) {
	service, _, cfg := newRecoveryTestService(t)
	writeChatChainRoster(t, cfg)
	target := &quotaChatHarness{serviceHarness: newServiceHarness(), failMode: "all-seats"}
	service.Harness = target
	collect, delivered := collectEvents()
	now := time.Now().UTC()
	if _, err := service.History.Append("cli", "hold-test", history.Entry{At: now, AcceptedAt: now, Role: "user", Content: "is the backup done?", SourceMessageID: "local:hold"}); err != nil {
		t.Fatal(err)
	}
	message := core.Message{Channel: "cli", Conversation: "hold-test", Sender: "joey", SourceMessageID: "local:hold", Text: "is the backup done?"}

	if err := service.dispatchHarnessPrompt(context.Background(), message, "chat prompt body", collect); err != nil {
		t.Fatalf("dispatchHarnessPrompt returned %v, want a held question, not an error return", err)
	}
	if tried := target.tried(); len(tried) != 2 {
		t.Fatalf("models tried = %q, want the default dispatch then the one fallback seat", tried)
	}
	var errorsText []string
	for _, event := range delivered() {
		text := event.Text
		if event.Kind == core.EventError && event.Done {
			errorsText = append(errorsText, text)
		}
		if strings.Contains(text, "429") || strings.Contains(text, "runner turn failed") || strings.Contains(text, "rate_limit") {
			t.Fatalf("raw runner error text reached the channel: %+v", event)
		}
	}
	if len(errorsText) != 1 || !strings.Contains(errorsText[0], "held") || !strings.Contains(errorsText[0], "answered after") {
		t.Fatalf("delivered error events = %q, want one hold sentence naming the reset", errorsText)
	}
	service.heldRequeueMu.Lock()
	scheduled := len(service.heldRequeues)
	service.heldRequeueMu.Unlock()
	if scheduled != 1 {
		t.Fatalf("scheduled held-question requeues = %d, want 1", scheduled)
	}
	if result := service.scanRecovery(context.Background(), "periodic"); result.Dispatched != 0 {
		t.Fatalf("the recovery scan re-fired a held question: %+v", result)
	}
}

// The scheduled requeue re-dispatches the held question through the ordinary recovery path.
func TestHeldQuestionRequeueRedispatchesThroughRecovery(t *testing.T) {
	service, target, _ := newRecoveryTestService(t)
	now := time.Now().UTC()
	if _, err := service.History.Append("cli", "hold-test", history.Entry{At: now, AcceptedAt: now, Role: "user", Content: "is the backup done?", SourceMessageID: "local:hold"}); err != nil {
		t.Fatal(err)
	}
	origin := orchestrator.Origin{Channel: "cli", Conversation: "hold-test"}
	service.redispatchHeldChatQuestion(origin, "local:hold", "is the backup done?")
	if prompts := target.prompts["chat:cli:hold-test"]; len(prompts) != 1 || !strings.Contains(prompts[0], "is the backup done?") {
		t.Fatalf("requeue prompts = %q, want one recovery turn for the held question", prompts)
	}
}

// Any other terminal chat failure delivers exactly one plain sentence: no stack, no provider
// text, and no chain move.
func TestChatGenericFailureDeliversOnePlainSentence(t *testing.T) {
	service, _, _ := newRecoveryTestService(t)
	target := &quotaChatHarness{serviceHarness: newServiceHarness(), failMode: "non-quota"}
	service.Harness = target
	collect, delivered := collectEvents()
	message := core.Message{Channel: "telegram", Conversation: "4242", Sender: "joey", SourceMessageID: "telegram:4242:2", Text: "summarise the log"}

	if err := service.dispatchHarnessPrompt(context.Background(), message, "chat prompt body", collect); err != nil {
		t.Fatalf("dispatchHarnessPrompt returned %v, want a fenced plain sentence", err)
	}
	if tried := target.tried(); len(tried) != 1 {
		t.Fatalf("models tried = %q, want no chain move for a non-quota failure", tried)
	}
	var errorsText []string
	for _, event := range delivered() {
		if event.Kind == core.EventError && event.Done {
			errorsText = append(errorsText, event.Text)
		}
		if strings.Contains(event.Text, "no such file") || strings.Contains(event.Text, "runner turn failed") {
			t.Fatalf("raw runner error text reached the channel: %+v", event)
		}
	}
	if len(errorsText) != 1 || !strings.Contains(errorsText[0], "chat turn failed") {
		t.Fatalf("delivered error events = %q, want one plain sentence", errorsText)
	}
}
