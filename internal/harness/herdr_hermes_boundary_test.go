package harness

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/roster"
)

func TestHerdrHeadlessHermesLargeSingleLineReplyIsSafelyBounded(t *testing.T) {
	f := newHermesBoundaryFixture(t, `prev=""
for a in "$@"; do
  if [ "$prev" = "--usage-file" ]; then
    printf '%s' '{"session_id":"local-abc123456789","completed":true,"failed":false}' > "$a"
  fi
  prev="$a"
done
head -c 900000 /dev/zero | tr '\0' 'x'
printf '\n'
head -c 1300000 /dev/zero | tr '\0' 'x'
`, "do the task")
	f.settle(t, "terminal", "completed")
	var log eventLog
	_, _, err := f.h.SendWithModel(f.context(), headlessKey, f.prompt, "@pilot", log.emit)
	if err != nil {
		t.Fatalf("large single-line reply: %v", err)
	}
	done := log.done()
	if len(done) != 1 || done[0].FinalText == nil {
		t.Fatalf("final events = %+v", done)
	}
	text := *done[0].FinalText
	if len(text) == 0 || len(text) > hermesResultCap || !strings.HasSuffix(text, strings.Repeat("x", 1024)) {
		t.Fatalf("bounded reply length = %d, want a nonempty capped x suffix", len(text))
	}
}

// The finite engine's headless client lost its gateway link mid-turn: it prints the disconnect
// copy and exits without a usage report while the gateway goes on and records the finished
// answer in the profile ledger. The turn must wait for that answer and deliver it, never fail
// with the raw disconnect copy (2026-11-06 finite run 803 failed "runner turn failed: Command
// helper: applied 4 secrets\nError: Gateway disconnected..." 39 seconds after the drop).
func TestHerdrHeadlessHermesDisconnectRecoversFinishedAnswer(t *testing.T) {
	f := newHermesBoundaryFixture(t, "echo 'Command helper: applied 4 secrets' >&2\necho 'Error: "+hermesDisconnectTurnCopy+
		" Resume the printed session ID to check whether it completed or was interrupted; do not resend the work.' >&2\nexit 1\n", "do the task")
	f.h.hermesDisconnectWait = 5 * time.Second
	// The gateway finishes the turn after the client gave up: its rows land later than any
	// settle window, well inside the disconnect wait.
	go func() {
		time.Sleep(500 * time.Millisecond)
		f.ledger.add(boundaryTestSession,
			hermesTestRow{role: "user", content: f.prompt},
			hermesTestRow{role: "assistant", content: "Recorded while the client was gone.", finishReason: "stop"},
		)
	}()
	var log eventLog
	_, _, err := f.h.SendWithModel(f.context(), headlessKey, f.prompt, "@pilot", log.emit)
	if err != nil {
		t.Fatalf("Send returned %v, want the gateway's recorded answer", err)
	}
	done := log.done()
	if len(done) != 1 || done[0].FinalText == nil || *done[0].FinalText != "Recorded while the client was gone." {
		t.Fatalf("final events = %+v, want the ledger's recorded answer", done)
	}
	if f.stopCalls.Load() == 0 {
		t.Fatal("the detached gateway turn was never stopped after the ledger recorded its outcome")
	}
}

// With no recorded outcome before the disconnect wait runs out the turn fails with one plain
// sentence: the raw disconnect copy is a Hermes interface string, never a run error.
func TestHerdrHeadlessHermesDisconnectExpiryFailsWithAPlainSentence(t *testing.T) {
	f := newHermesBoundaryFixture(t, "echo 'Error: "+hermesDisconnectTurnCopy+
		" Resume the printed session ID to check whether it completed or was interrupted; do not resend the work.' >&2\nexit 1\n", "do the task")
	f.h.hermesDisconnectWait = 300 * time.Millisecond
	var log eventLog
	_, _, err := f.h.SendWithModel(f.context(), headlessKey, f.prompt, "@pilot", log.emit)
	if err == nil {
		t.Fatal("Send succeeded with no recorded outcome, want a plain-sentence turn error")
	}
	if strings.Contains(err.Error(), "Gateway disconnected") {
		t.Fatalf("turn error %q carries the raw Hermes disconnect copy", err)
	}
	if !strings.Contains(err.Error(), "no answer was recorded") {
		t.Fatalf("turn error %q is not the plain no-outcome sentence", err)
	}
	for _, e := range log.done() {
		if e.FinalText != nil {
			t.Fatalf("a turn with no recorded outcome was delivered as a reply: %+v", e.FinalText)
		}
	}
	if f.stopCalls.Load() == 0 {
		t.Fatal("the detached gateway turn was never stopped after the wait expired")
	}
}

func TestHermesBoundaryRefreshesPersistedNilGeneration(t *testing.T) {
	f := newHermesBoundaryFixture(t, hermesTurnOK, "do the task")
	f.settle(t, "queued", "")
	boundary, err := newHermesRunBoundary(f.h, f.spec, headlessKey, f.prompt, roster.Staff{Runner: "hermes", Args: []string{"-p", boundaryTestProfile}})
	if err != nil {
		t.Fatal(err)
	}
	if err := boundary.refreshIdentity(); err != nil {
		t.Fatal(err)
	}
	if boundary.meta.Identity == nil || boundary.meta.Identity.Generation != nil {
		t.Fatalf("queued identity = %+v, want a persisted nil generation", boundary.meta.Identity)
	}
	f.settle(t, "started", "")
	if err := boundary.refreshIdentity(); err != nil {
		t.Fatal(err)
	}
	if boundary.meta.Identity.Generation == nil || *boundary.meta.Identity.Generation != 7 {
		t.Fatalf("started identity generation = %+v, want 7", boundary.meta.Identity.Generation)
	}
	data, err := os.ReadFile(filepath.Join(f.spec.RunDir, hermesBoundaryMetadataFile))
	if err != nil {
		t.Fatal(err)
	}
	var persisted hermesBoundaryMetadata
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Identity == nil || persisted.Identity.Generation == nil || *persisted.Identity.Generation != 7 {
		t.Fatalf("persisted generation = %+v, want 7", persisted.Identity)
	}
	observation, err := InspectHermesRunBoundary(context.Background(), f.spec)
	if err != nil || observation.Identity == nil || observation.Identity.Generation == nil || *observation.Identity.Generation != 7 {
		t.Fatalf("observed generation = %+v, %v", observation.Identity, err)
	}
}

// A 429 that names no parseable reset is still a wall: the boundary writes an
// execution.json with a provisional 15-minute probe wake, which the engine's
// ladder re-probes and never charges as a failure.
func TestHermesBoundaryWallWithoutResetIsStillWall(t *testing.T) {
	f := newHermesBoundaryFixture(t, `prev=""
for a in "$@"; do
  if [ "$prev" = "--usage-file" ]; then
    printf '%s' '{"session_id":"local-abc123456789","completed":false,"failed":true,"failure":"HTTP 429: rate limit exceeded","input_tokens":3,"api_calls":1}' > "$a"
  fi
  prev="$a"
done
exit 2
`, "do the task")
	before := time.Now().UTC()
	var log eventLog
	_, _, _ = f.h.SendWithModel(f.context(), headlessKey, f.prompt, "@pilot", log.emit)
	after := time.Now().UTC()
	data, err := os.ReadFile(filepath.Join(f.spec.RunDir, "execution.json"))
	if err != nil {
		t.Fatalf("probe wall execution record: %v", err)
	}
	var record struct {
		Outcome    string    `json:"outcome"`
		WallSource string    `json:"wall_source"`
		WallWakeAt time.Time `json:"wall_wake_at"`
	}
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatalf("decode probe wall record %s: %v", data, err)
	}
	if record.Outcome != "wall" || record.WallSource != "probe" {
		t.Fatalf("probe wall record = %s, want outcome wall with wall_source probe", data)
	}
	earliest := before.Add(15 * time.Minute).Add(-5 * time.Second)
	latest := after.Add(15 * time.Minute).Add(5 * time.Second)
	if record.WallWakeAt.Before(earliest) || record.WallWakeAt.After(latest) {
		t.Fatalf("probe wake = %s, want within 5s of the write time plus 15m", record.WallWakeAt)
	}
}
