package harness

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/core"
)

func TestPiRPCStreamsSettlesAndResumes(t *testing.T) {
	command, root, logPath := portableHarnessFixture(t, "pi-lifecycle")
	sessionsPath := filepath.Join(root, "sessions.json")
	config := HarnessConfig{
		Command: command, Cwd: root, Model: "fixture/model-a", Effort: "high",
		Sandbox: "read-only", SessionsFile: sessionsPath,
	}
	pi, err := NewPi(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pi.Start(ctx); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var events []core.Event
	deltaSeen := make(chan struct{}, 1)
	done := make(chan core.Event, 1)
	threadID, steered, err := pi.Send(ctx, "chat", "test prompt", func(event core.Event) {
		mu.Lock()
		events = append(events, event)
		mu.Unlock()
		if event.Kind == core.EventDelta && strings.Contains(event.Text, "world") {
			select {
			case deltaSeen <- struct{}{}:
			default:
			}
		}
		if event.Done {
			done <- event
		}
	})
	if err != nil || steered || threadID != "pi-session" {
		t.Fatalf("Send() = %q, %t, %v", threadID, steered, err)
	}
	select {
	case <-deltaSeen:
	case <-ctx.Done():
		t.Fatal("timed out waiting for Pi delta")
	}
	select {
	case event := <-done:
		t.Fatalf("Pi treated agent_end as terminal before agent_settled: %#v", event)
	case <-time.After(30 * time.Millisecond):
	}
	var final core.Event
	select {
	case final = <-done:
	case <-ctx.Done():
		t.Fatal("timed out waiting for Pi agent_settled")
	}
	if final.Kind != core.EventFinal || final.Text != "hello world" || final.FinalText == nil || *final.FinalText != "hello world" || pi.IsActive("chat") {
		t.Fatalf("Pi final = %#v, active %t", final, pi.IsActive("chat"))
	}
	models, err := pi.Models(ctx)
	if err != nil || len(models) != 1 || models[0].ID != "fixture/model-a" || models[0].DefaultEffort != "high" || strings.Join(models[0].Efforts, ",") != "off,low,medium,high" {
		t.Fatalf("Pi models = %#v, %v", models, err)
	}
	if err := pi.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewPi(config)
	if err != nil || restarted.ThreadID("chat") != "pi-session" {
		t.Fatalf("persisted Pi session = %q, %v", restarted.ThreadID("chat"), err)
	}
	if err := restarted.Start(ctx); err != nil {
		t.Fatal(err)
	}
	restartedDone := make(chan struct{}, 1)
	if threadID, steered, err := restarted.Send(ctx, "chat", "continued", func(event core.Event) {
		if event.Done {
			restartedDone <- struct{}{}
		}
	}); err != nil || steered || threadID != "pi-session" {
		t.Fatalf("resumed Pi Send() = %q, %t, %v", threadID, steered, err)
	}
	select {
	case <-restartedDone:
	case <-ctx.Done():
		t.Fatal("timed out waiting for resumed Pi turn")
	}
	_ = restarted.Close()

	var rpcInvocations, resumed int
	for _, record := range readFixtureRecords(t, logPath) {
		if record.Kind != "invocation" || containsArgument(record.Args, "--version") {
			continue
		}
		rpcInvocations++
		arguments := strings.Join(record.Args, " ")
		if !strings.Contains(arguments, "--mode rpc") || record.Cwd != root || record.Executable != command {
			t.Fatalf("portable Pi invocation = %#v", record)
		}
		if containsArgument(record.Args, "--session") {
			resumed++
		}
	}
	if rpcInvocations != 4 || resumed != 1 {
		t.Fatalf("Pi RPC invocations = %d, resumed = %d", rpcInvocations, resumed)
	}
}

func TestPiModelsUseRPCThinkingLevelsPerModel(t *testing.T) {
	command, root, logPath := portableHarnessFixture(t, "pi-model-capabilities")
	pi, err := NewPi(HarnessConfig{Command: command, Cwd: root, SessionsFile: filepath.Join(root, "sessions.json")})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pi.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer pi.Close()

	models, err := pi.Models(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(models) != 3 {
		t.Fatalf("Pi model count = %d, want 3: %#v", len(models), models)
	}
	if got := strings.Join(models[0].Efforts, ","); got != "off,low,medium,high" || models[0].DefaultEffort != "high" {
		t.Fatalf("model-a efforts/default = %q/%q", got, models[0].DefaultEffort)
	}
	if len(models[1].Efforts) != 0 || models[1].DefaultEffort != "" {
		t.Fatalf("non-reasoning model properties = %#v", models[1])
	}
	if got := strings.Join(models[2].Efforts, ","); got != "off,medium,xhigh,max" || models[2].DefaultEffort != "" {
		t.Fatalf("model-max efforts/default = %q/%q", got, models[2].DefaultEffort)
	}
	if !models[0].Default || models[1].Default || models[2].Default {
		t.Fatalf("Pi current/default model mapping = %#v", models)
	}
	if err := ValidateInferenceSelection(models, InferenceSelection{Model: "fixture/model-a", Effort: "xhigh"}); err != nil {
		t.Fatalf("Pi rejected a manual effort absent from discovery: %v", err)
	}
	if err := ValidateInferenceSelection(models, InferenceSelection{Model: "fixture/model-max", Effort: "max"}); err != nil {
		t.Fatalf("Pi rejected an RPC-advertised effort: %v", err)
	}

	var sets, levelQueries, modelProbes int
	for _, record := range readFixtureRecords(t, logPath) {
		switch record.Method {
		case "set_model":
			sets++
		case "get_available_thinking_levels":
			levelQueries++
		}
		if record.Kind == "invocation" {
			for index, arg := range record.Args {
				if arg == "--model" && index+1 < len(record.Args) && strings.HasPrefix(record.Args[index+1], "fixture/model-") {
					modelProbes++
				}
			}
		}
	}
	if sets != 0 || levelQueries != 3 || modelProbes != 3 {
		t.Fatalf("Pi capability discovery = %d persistent set_model calls, %d level queries, %d runtime model probes; want 0, 3, 3", sets, levelQueries, modelProbes)
	}
}

func TestPiLegacyOmittedModelAndEffortUseCurrentModelCapabilities(t *testing.T) {
	command, root, logPath := portableHarnessFixture(t, "pi-off-default")
	pi, err := NewPi(HarnessConfig{Command: command, Cwd: root, Effort: "medium", LegacyEffort: true, SessionsFile: filepath.Join(root, "sessions.json")})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pi.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer pi.Close()
	if _, _, err := pi.Send(ctx, "legacy", "legacy defaults", nil); err != nil {
		t.Fatalf("Pi rejected omitted model with legacy medium effort: %v", err)
	}
	if _, _, err := pi.SendWithInference(ctx, "explicit", "manual value", InferenceSelection{Effort: "medium"}, nil); err != nil {
		t.Fatalf("Pi rejected a manual effort: %v", err)
	}
	var mediumInvocations int
	for _, record := range readFixtureRecords(t, logPath) {
		if record.Kind == "invocation" && containsArgument(record.Args, "--thinking") && containsArgument(record.Args, "medium") {
			mediumInvocations++
		}
	}
	if mediumInvocations != 2 {
		t.Fatalf("legacy and explicit Pi --thinking medium invocations = %d, want 2", mediumInvocations)
	}
}

func TestPiAuthoritativeMessageAddsOnlyMissingStreamSuffix(t *testing.T) {
	turn := &piTurn{}
	turn.appendText("session", "partial ")
	turn.finishMessage("session", "partial response")
	if got := turn.text.String(); got != "partial response" {
		t.Fatalf("reconciled Pi message = %q", got)
	}
	if turn.lastMessage != "partial response" {
		t.Fatalf("Pi final assistant item = %q", turn.lastMessage)
	}
}

func TestPiSteersActiveTurnAndReleasesPreviousEmitter(t *testing.T) {
	command, root, _ := portableHarnessFixture(t, "pi-steer")
	pi, err := NewPi(HarnessConfig{Command: command, Cwd: root, SessionsFile: filepath.Join(root, "sessions.json")})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pi.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer pi.Close()
	first := make(chan core.Event, 16)
	if _, steered, err := pi.Send(ctx, "chat", "first", func(event core.Event) { first <- event }); err != nil || steered {
		t.Fatalf("first Pi send = steered %t, %v", steered, err)
	}
	for {
		select {
		case event := <-first:
			if event.Kind == core.EventDelta && event.Text == "first" {
				goto firstSeen
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for first Pi delta")
		}
	}

firstSeen:
	second := make(chan core.Event, 16)
	if threadID, steered, err := pi.Send(ctx, "chat", "second", func(event core.Event) { second <- event }); err != nil || !steered || threadID != "pi-session" {
		t.Fatalf("steered Pi send = %q, %t, %v", threadID, steered, err)
	}
	var final core.Event
	for !final.Done {
		select {
		case event := <-second:
			if event.Done {
				final = event
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for steered Pi result")
		}
	}
	if final.Kind != core.EventFinal || final.Text != "first second" {
		t.Fatalf("steered Pi final = %#v", final)
	}
	released := false
	for len(first) > 0 {
		event := <-first
		released = released || event.Kind == core.EventStatus && event.Done
	}
	if !released {
		t.Fatal("previous Pi emitter was not released")
	}
}

func TestPiInterruptUsesRPCAbort(t *testing.T) {
	command, root, logPath := portableHarnessFixture(t, "pi-interrupt")
	pi, err := NewPi(HarnessConfig{Command: command, Cwd: root, SessionsFile: filepath.Join(root, "sessions.json")})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pi.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer pi.Close()
	events := make(chan core.Event, 16)
	if _, _, err := pi.Send(ctx, "chat", "work", func(event core.Event) { events <- event }); err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case event := <-events:
			if event.Kind == core.EventDelta {
				goto active
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for active Pi turn")
		}
	}

active:
	if stopped, err := pi.Interrupt(ctx, "chat"); err != nil || !stopped {
		t.Fatalf("Pi interrupt = %t, %v", stopped, err)
	}
	for {
		select {
		case event := <-events:
			if event.Done {
				if event.Kind != core.EventFinal {
					t.Fatalf("Pi interrupt terminal = %#v", event)
				}
				goto done
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for interrupted Pi turn")
		}
	}

done:
	foundAbort := false
	for _, record := range readFixtureRecords(t, logPath) {
		foundAbort = foundAbort || record.Method == "abort"
	}
	if !foundAbort {
		t.Fatal("Pi fixture did not receive abort")
	}
}

func TestPiLoadsOrdinaryUserResourcesWithoutSuppressionFlags(t *testing.T) {
	command, root, logPath := portableHarnessFixture(t, "pi-lifecycle")
	pi, err := NewPi(HarnessConfig{Command: command, Cwd: root, Sandbox: "read-only", SessionsFile: filepath.Join(root, "sessions.json")})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pi.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer pi.Close()
	done := make(chan struct{}, 1)
	if _, _, err := pi.Send(ctx, "chat", "resource check", func(event core.Event) {
		if event.Done {
			done <- struct{}{}
		}
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out waiting for Pi turn")
	}
	if _, err := pi.Models(ctx); err != nil {
		t.Fatal(err)
	}

	forbidden := []string{"--no-extensions", "--no-skills", "--no-prompt-templates", "--no-themes", "--approve", "-a", "--no-approve", "-na"}
	ordinary, ephemeral := 0, 0
	for _, record := range readFixtureRecords(t, logPath) {
		if record.Kind != "invocation" || containsArgument(record.Args, "--version") {
			continue
		}
		if len(record.Args) < 2 || record.Args[0] != "--mode" || record.Args[1] != "rpc" {
			t.Fatalf("Pi base arguments = %#v, want --mode rpc first", record.Args)
		}
		for _, flag := range forbidden {
			if containsArgument(record.Args, flag) {
				t.Fatalf("Pi invocation suppressed ordinary resources or approvals with %q: %#v", flag, record)
			}
		}
		if !containsArgument(record.Args, "read,grep,find,ls") {
			t.Fatalf("read-only Pi invocation omitted the strict tool allowlist: %#v", record)
		}
		if containsArgument(record.Args, "--no-session") {
			ephemeral++
		} else {
			ordinary++
		}
	}
	if ordinary != 1 || ephemeral != 2 {
		t.Fatalf("Pi ordinary/ephemeral invocations = %d/%d, want 1/2", ordinary, ephemeral)
	}
}

func TestPiExtensionUIDialogsCancelWithoutReceivingFireAndForgetResponses(t *testing.T) {
	command, root, logPath := portableHarnessFixture(t, "pi-extension-ui")
	pi, err := NewPi(HarnessConfig{Command: command, Cwd: root, SessionsFile: filepath.Join(root, "sessions.json")})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pi.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer pi.Close()
	events := make(chan core.Event, 32)
	done := make(chan core.Event, 1)
	if _, _, err := pi.Send(ctx, "chat", "work", func(event core.Event) {
		events <- event
		if event.Done {
			done <- event
		}
	}); err != nil {
		t.Fatal(err)
	}
	var final core.Event
	select {
	case final = <-done:
	case <-ctx.Done():
		t.Fatal("timed out waiting for Pi turn blocked on an extension dialog")
	}
	if final.Kind != core.EventFinal || final.Text != "hello world" || pi.IsActive("chat") {
		t.Fatalf("Pi extension-UI final = %#v, active %t", final, pi.IsActive("chat"))
	}
	cancelledEvent := false
drain:
	for {
		select {
		case event := <-events:
			cancelledEvent = cancelledEvent || event.Kind == core.EventStatus && strings.Contains(event.Text, "cancelled")
		default:
			break drain
		}
	}
	if !cancelledEvent {
		t.Fatal("Pi did not surface a status event for the cancelled extension dialog")
	}

	var responses []fixtureRecord
	for _, record := range readFixtureRecords(t, logPath) {
		if record.Kind == "request" && record.Method == "extension_ui_response" {
			responses = append(responses, record)
		}
	}
	if len(responses) != 1 {
		t.Fatalf("Pi extension UI responses = %d, want only the confirm cancellation: %#v", len(responses), responses)
	}
	var response struct {
		ID        string `json:"id"`
		Cancelled bool   `json:"cancelled"`
	}
	if err := json.Unmarshal(responses[0].Params, &response); err != nil {
		t.Fatal(err)
	}
	if response.ID != "ui-1" || !response.Cancelled {
		t.Fatalf("Pi extension UI response = %#v", response)
	}
}
