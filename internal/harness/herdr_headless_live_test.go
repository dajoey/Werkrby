package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/roster"
)

const (
	boundaryTestProfile   = "wk-finite-test"
	boundaryTestSession   = "local-abc123456789"
	boundaryTestRequest   = "request-finite-1"
	boundaryTestAdmission = "admission-finite-1"
)

type hermesBoundaryFixture struct {
	h      *Herdr
	root   string
	spec   RunBoundarySpec
	ledger *hermesTestLedger
	prompt string
}

func newHermesBoundaryFixture(t *testing.T, fakeHermes, prompt string) *hermesBoundaryFixture {
	t.Helper()
	h, root := hermesHeadlessFixture(t, fakeHermes)
	rosterBody := "staff:\n  pilot:\n    runner: hermes\n    headless: true\n    args: [\"-p\", \"wk-finite-test\", \"--model\", \"zai/glm-5.3-flash\", \"--reasoning\", \"high\"]\n"
	if err := os.WriteFile(filepath.Join(root, ".werkrby", "roster.yaml"), []byte(rosterBody), 0o600); err != nil {
		t.Fatal(err)
	}
	home := filepath.Join(root, "disposable hermes home")
	if err := os.MkdirAll(home, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".finite-hermes-disposable"), []byte(boundaryTestProfile+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HERMES_HOME", home)
	runDir := filepath.Join(root, "finite run")
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "prompt.txt"), []byte(prompt), 0o600); err != nil {
		t.Fatal(err)
	}
	ledger := newHermesTestLedger(t, filepath.Join(home, "profiles", boundaryTestProfile, "state.db"))
	_, err := ledger.db.Exec(`ALTER TABLE sessions ADD COLUMN runtime_generation INTEGER NOT NULL DEFAULT 0;
CREATE TABLE session_admissions (
    admission_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    target_session_id TEXT NOT NULL,
    lineage_json TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_digest TEXT,
    intent TEXT NOT NULL DEFAULT 'queue',
    status TEXT NOT NULL,
    owner_epoch INTEGER NOT NULL,
    generation INTEGER,
    outcome TEXT,
    seq INTEGER NOT NULL UNIQUE
);
CREATE TABLE worker_executions (
    execution_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    kind TEXT,
    owner_epoch INTEGER NOT NULL,
    generation INTEGER NOT NULL,
    status TEXT NOT NULL,
    adoption_digest TEXT
);`)
	if err != nil {
		t.Fatal(err)
	}
	ledger.session(boundaryTestSession, "", "", time.Now())
	payload, _ := json.Marshal(map[string]any{"text": prompt, "api_turn_v1": map[string]any{}})
	lineage, _ := json.Marshal([]string{boundaryTestSession})
	generation := int64(7)
	_, err = ledger.db.Exec(`INSERT INTO session_admissions(
        admission_id,request_id,principal_id,target_session_id,lineage_json,payload_json,payload_digest,intent,status,owner_epoch,generation,seq
    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,1)`, boundaryTestAdmission, boundaryTestRequest, "api", boundaryTestSession,
		string(lineage), string(payload), "digest", "queue", "started", 11, generation)
	if err != nil {
		t.Fatal(err)
	}
	return &hermesBoundaryFixture{
		h: h, root: root, ledger: ledger, prompt: prompt,
		spec: RunBoundarySpec{RunID: 1, ProcessToken: strings.Repeat("a", 43), RunDir: runDir},
	}
}

func (f *hermesBoundaryFixture) settle(t *testing.T, status, outcome string) {
	t.Helper()
	var generation any
	if status != "queued" {
		generation = int64(7)
	}
	_, err := f.ledger.db.Exec(`UPDATE session_admissions SET status=?,outcome=?,generation=? WHERE admission_id=?`,
		status, outcome, generation, boundaryTestAdmission)
	if err != nil {
		t.Fatal(err)
	}
}

func (f *hermesBoundaryFixture) context() context.Context {
	return WithRunBoundary(context.Background(), f.spec)
}

func TestHerdrHeadlessHermesClientExitIsNotProviderTurnAbsence(t *testing.T) {
	f := newHermesBoundaryFixture(t, hermesTurnOK, "do the task")
	returned := make(chan error, 1)
	go func() {
		_, _, err := f.h.SendWithModel(f.context(), headlessKey, f.prompt, "@pilot", nil)
		returned <- err
	}()
	select {
	case err := <-returned:
		t.Fatalf("client exit was treated as provider-turn absence: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	observation, err := InspectHermesRunBoundary(context.Background(), f.spec)
	if err != nil || observation.Liveness != BoundaryLive || observation.Identity == nil || observation.Identity.AdmissionID != boundaryTestAdmission {
		f.settle(t, "terminal", "completed")
		t.Fatalf("live provider observation = %+v, %v; want the exact live admission", observation, err)
	}
	f.settle(t, "terminal", "completed")
	select {
	case err := <-returned:
		if err != nil {
			t.Fatalf("completed provider turn: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("client exit did not follow the exact gateway turn to terminal absence")
	}
}

func TestHerdrHeadlessHermesRecoveryIdentitySurvivesClientRestart(t *testing.T) {
	f := newHermesBoundaryFixture(t, `while [ ! -f "$(dirname "$0")/release-client" ]; do sleep 0.02; done
`+hermesTurnOK, "do the task")
	returned := make(chan error, 1)
	go func() {
		_, _, err := f.h.SendWithModel(f.context(), headlessKey, f.prompt, "@pilot", nil)
		returned <- err
	}()
	var first HermesBoundaryObservation
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		observation, err := InspectHermesRunBoundary(context.Background(), f.spec)
		if err == nil && observation.Liveness == BoundaryLive && observation.Identity != nil {
			first = observation
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if first.Identity == nil {
		_ = os.WriteFile(filepath.Join(f.root, "release-client"), nil, 0o600)
		f.settle(t, "terminal", "completed")
		t.Fatal("the live client never published its exact provider-turn identity")
	}
	// A fresh inspection has no adapter or client memory: run-private metadata
	// plus the disposable ledger must reconstruct the identical turn.
	restarted, err := InspectHermesRunBoundary(context.Background(), f.spec)
	if err != nil || restarted.Identity == nil || first.Identity == nil ||
		restarted.Identity.SessionID != first.Identity.SessionID ||
		restarted.Identity.RequestID != first.Identity.RequestID ||
		restarted.Identity.AdmissionID != first.Identity.AdmissionID ||
		restarted.Identity.OwnerEpoch != first.Identity.OwnerEpoch ||
		(restarted.Identity.Generation == nil) != (first.Identity.Generation == nil) ||
		(restarted.Identity.Generation != nil && first.Identity.Generation != nil && *restarted.Identity.Generation != *first.Identity.Generation) {
		t.Fatalf("restart identity = %+v, %v; first = %+v", restarted.Identity, err, first.Identity)
	}
	f.settle(t, "terminal", "completed")
	if err := os.WriteFile(filepath.Join(f.root, "release-client"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-returned:
		if err != nil {
			t.Fatalf("restarted boundary completion: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("restarted boundary did not release the completed client")
	}
}

func TestHerdrHeadlessHermesClientFailureStopsDetachedTurn(t *testing.T) {
	f := newHermesBoundaryFixture(t, `prev=""
for a in "$@"; do
  if [ "$prev" = "--usage-file" ]; then
    printf '%s' '{"session_id":"local-abc123456789","completed":false,"failed":true,"failure":"HTTP 429: Weekly Limit Exhausted. Your limit will reset at 2026-11-01 02:03:04","input_tokens":11,"output_tokens":7,"api_calls":1}' > "$a"
  fi
  prev="$a"
done
echo 'provider wall'
exit 2
`, "do the task")
	called := make(chan HermesTurnIdentity, 1)
	oldStop := stopHermesGatewayAdmission
	stopHermesGatewayAdmission = func(_ context.Context, _ string, identity HermesTurnIdentity) error {
		called <- identity
		f.settle(t, "terminal", "failed")
		return nil
	}
	t.Cleanup(func() { stopHermesGatewayAdmission = oldStop })
	_, _, err := f.h.SendWithModel(f.context(), headlessKey, f.prompt, "@pilot", nil)
	if err == nil || !strings.Contains(err.Error(), "HTTP 429: Weekly Limit Exhausted") {
		t.Fatalf("detached provider failure = %v, want the exact quota error", err)
	}
	select {
	case identity := <-called:
		if identity.AdmissionID != boundaryTestAdmission {
			t.Fatalf("stopped admission = %q", identity.AdmissionID)
		}
	default:
		t.Fatal("failed client left its detached gateway turn running")
	}
	execution, readErr := os.ReadFile(filepath.Join(f.spec.RunDir, "execution.json"))
	if readErr != nil || !strings.Contains(string(execution), `"outcome":"wall"`) || !strings.Contains(string(execution), `"wall_wake_at":"2026-11-01T02:03:04Z"`) {
		t.Fatalf("run-scoped wall artifact = %s, %v", execution, readErr)
	}
	usage, readErr := os.ReadFile(filepath.Join(f.spec.RunDir, "usage.json"))
	if readErr != nil || !strings.Contains(string(usage), `"fresh_input_tokens":11`) || !strings.Contains(string(usage), `"output_tokens":7`) {
		t.Fatalf("run-scoped usage artifact = %s, %v", usage, readErr)
	}
}

func TestHerdrHeadlessHermesUnknownAdmissionAndWorkerAreNotAbsence(t *testing.T) {
	f := newHermesBoundaryFixture(t, hermesTurnOK, "do the task")
	_, err := newHermesRunBoundary(f.h, f.spec, headlessKey, f.prompt, roster.Staff{Runner: "hermes", Args: []string{"-p", boundaryTestProfile}})
	if err != nil {
		t.Fatal(err)
	}
	f.settle(t, "unknown", "")
	observation, err := InspectHermesRunBoundary(context.Background(), f.spec)
	if err == nil || observation.Liveness != BoundaryUnknown {
		t.Fatalf("unknown admission = %+v, %v; must fail closed", observation, err)
	}
	f.settle(t, "terminal", "completed")
	_, err = f.ledger.db.Exec(`INSERT INTO worker_executions(execution_id,session_id,owner_epoch,generation,status) VALUES('worker-1',?,11,7,'running')`, boundaryTestSession)
	if err != nil {
		t.Fatal(err)
	}
	observation, err = InspectHermesRunBoundary(context.Background(), f.spec)
	if err != nil || observation.Liveness != BoundaryLive {
		t.Fatalf("terminal admission with a live worker = %+v, %v; want live", observation, err)
	}
	if _, err := f.ledger.db.Exec(`UPDATE worker_executions SET status='unknown'`); err != nil {
		t.Fatal(err)
	}
	observation, err = InspectHermesRunBoundary(context.Background(), f.spec)
	if err == nil || observation.Liveness != BoundaryUnknown {
		t.Fatalf("unknown worker = %+v, %v; must fail closed", observation, err)
	}
}

func TestHerdrHeadlessHermesRunBoundaryRefusesUnmarkedHomes(t *testing.T) {
	for _, defaultHome := range []bool{false, true} {
		t.Run(map[bool]string{false: "unmarked", true: "default"}[defaultHome], func(t *testing.T) {
			f := newHermesBoundaryFixture(t, hermesTurnOK, "do the task")
			if defaultHome {
				home, err := os.UserHomeDir()
				if err != nil {
					t.Fatal(err)
				}
				t.Setenv("HERMES_HOME", filepath.Join(home, ".hermes"))
			} else if err := os.Remove(filepath.Join(os.Getenv("HERMES_HOME"), finiteHermesMarker)); err != nil {
				t.Fatal(err)
			}
			_, _, err := f.h.SendWithModel(f.context(), headlessKey, f.prompt, "@pilot", nil)
			if err == nil || (!strings.Contains(err.Error(), "disposable") && !strings.Contains(err.Error(), "default ~/.hermes")) {
				t.Fatalf("unsafe home error = %v", err)
			}
			if _, statErr := os.Stat(filepath.Join(f.root, "argv")); !os.IsNotExist(statErr) {
				t.Fatal("unsafe home reached the provider client")
			}
		})
	}
}

func TestHerdrHeadlessHermesRunBoundaryHasNoPaneFallback(t *testing.T) {
	f := newHermesBoundaryFixture(t, hermesTurnOK, "do the task")
	_, _, err := f.h.SendWithModel(f.context(), headlessKey, f.prompt, "hermes", nil)
	if err == nil || !strings.Contains(err.Error(), "pane fallback is disabled") {
		t.Fatalf("bare runner error = %v, want headless-staff-only refusal", err)
	}
	if _, statErr := os.Stat(filepath.Join(f.root, "argv")); !os.IsNotExist(statErr) {
		t.Fatal("pane fallback reached the provider client")
	}
}

func TestHerdrHeadlessHermesLiveProviderBoundary(t *testing.T) {
	if os.Getenv("WERKRBY_FINITE_HERMES_LIVE") != "1" {
		t.Skip("real Hermes provider boundary requires explicit live opt-in")
	}
	home := os.Getenv("WERKRBY_FINITE_HERMES_HOME")
	if home == "" {
		t.Fatal("WERKRBY_FINITE_HERMES_HOME must name an explicitly marked disposable home")
	}
	if err := validateDisposableHermesHome(home, "wk-finite-test"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HERMES_HOME", home)
	command := os.Getenv("WERKRBY_FINITE_HERMES_COMMAND")
	if command == "" {
		t.Fatal("WERKRBY_FINITE_HERMES_COMMAND must name the real Hermes executable")
	}
	model := os.Getenv("WERKRBY_FINITE_HERMES_MODEL")
	if model == "" {
		model = "zai/glm-5.3-flash"
	}
	root := t.TempDir()
	if evidence := os.Getenv("WERKRBY_FINITE_HERMES_EVIDENCE"); evidence != "" {
		if !filepath.IsAbs(evidence) {
			t.Fatal("live evidence directory must be absolute")
		}
		if err := os.MkdirAll(evidence, 0o700); err != nil {
			t.Fatal(err)
		}
		root = filepath.Join(evidence, "provider-boundary")
		if err := os.Mkdir(root, 0o700); err != nil {
			t.Fatalf("refuse to reuse live evidence directory: %v", err)
		}
	}
	state := filepath.Join(root, ".werkrby")
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}
	rosterBody := fmt.Sprintf("staff:\n  pilot:\n    runner: hermes\n    headless: true\n    args: [\"-p\", \"wk-finite-test\", \"--model\", %q]\n", model)
	if err := os.WriteFile(filepath.Join(state, "roster.yaml"), []byte(rosterBody), 0o600); err != nil {
		t.Fatal(err)
	}
	runID := int64(900001)
	if text := os.Getenv("WERKRBY_FINITE_HERMES_RUN_ID"); text != "" {
		parsed, err := strconv.ParseInt(text, 10, 64)
		if err != nil || parsed <= 0 {
			t.Fatalf("invalid live run ID %q", text)
		}
		runID = parsed
	}
	prompt := fmt.Sprintf("FINITE TURN\nrun_id: %d\nReply with exactly FINITE_HERMES_OK and nothing else.", runID)
	runDir := filepath.Join(root, "run")
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "prompt.txt"), []byte(prompt), 0o600); err != nil {
		t.Fatal(err)
	}
	h, err := NewHerdr(HarnessConfig{Command: filepath.Join(root, "no-such-herdr"), Cwd: root, SessionsFile: filepath.Join(runDir, "sessions.json")})
	if err != nil {
		t.Fatal(err)
	}
	h.headlessHermesCommand = command
	spec := RunBoundarySpec{RunID: runID, ProcessToken: strings.Repeat("f", 43), RunDir: runDir}
	ctx, cancel := context.WithTimeout(WithRunBoundary(context.Background(), spec), 3*time.Minute)
	defer cancel()
	if err := h.Start(ctx); err != nil {
		t.Fatal(err)
	}
	var log eventLog
	threadID, _, sendErr := h.SendWithModel(ctx, headlessKey, prompt, "@pilot", log.emit)
	if sendErr != nil {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
		state, stopErr := StopHermesRunBoundary(stopCtx, spec)
		stopCancel()
		observation, inspectErr := InspectHermesRunBoundary(context.Background(), spec)
		t.Fatalf("real Hermes boundary is unadmitted: %v; stop=%s/%v; observation=%+v/%v", sendErr, state, stopErr, observation, inspectErr)
	}
	observation, err := InspectHermesRunBoundary(context.Background(), spec)
	if err != nil || observation.Liveness != BoundaryAbsent || observation.Outcome != "completed" || observation.Identity == nil || threadID != observation.Identity.SessionID {
		t.Fatalf("real terminal boundary = %+v, %v; thread=%q", observation, err, threadID)
	}
	done := log.done()
	if len(done) != 1 || done[0].FinalText == nil || strings.TrimSpace(*done[0].FinalText) != "FINITE_HERMES_OK" {
		t.Fatalf("real provider final events = %+v", done)
	}

	// Stop a real gateway turn, not just its client. The terminal tool keeps the
	// provider turn alive while the exact admission is discovered and cancelled.
	interruptPrompt := fmt.Sprintf("FINITE TURN\nrun_id: %d\nUse the terminal tool to run `sleep 60`, then reply STOPPED. Do not write files.", runID+1)
	interruptDir := filepath.Join(root, "run-interrupt")
	if err := os.MkdirAll(interruptDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(interruptDir, "prompt.txt"), []byte(interruptPrompt), 0o600); err != nil {
		t.Fatal(err)
	}
	interrupted, err := NewHerdr(HarnessConfig{Command: filepath.Join(root, "no-such-herdr"), Cwd: root, SessionsFile: filepath.Join(interruptDir, "sessions.json")})
	if err != nil {
		t.Fatal(err)
	}
	interrupted.headlessHermesCommand = command
	interruptSpec := RunBoundarySpec{RunID: runID + 1, ProcessToken: strings.Repeat("e", 43), RunDir: interruptDir}
	interruptCtx, interruptCancel := context.WithTimeout(WithRunBoundary(context.Background(), interruptSpec), 90*time.Second)
	defer interruptCancel()
	returned := make(chan error, 1)
	go func() {
		_, _, err := interrupted.SendWithModel(interruptCtx, headlessKey, interruptPrompt, "@pilot", nil)
		returned <- err
	}()
	deadline := time.Now().Add(30 * time.Second)
	for {
		observation, err := InspectHermesRunBoundary(context.Background(), interruptSpec)
		if err == nil && observation.Liveness == BoundaryLive && observation.Identity != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("real sleeping turn never published a live exact identity")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if ok, err := interrupted.Interrupt(context.Background(), headlessKey); !ok || err != nil {
		t.Fatalf("real gateway interrupt = %v, %v", ok, err)
	}
	var interruptSendErr error
	select {
	case err := <-returned:
		interruptSendErr = err
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("real interrupted send = %v, want cancellation after terminal absence", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("real gateway interruption did not return")
	}
	observation, err = InspectHermesRunBoundary(context.Background(), interruptSpec)
	if err != nil || observation.Liveness != BoundaryAbsent || observation.Identity == nil {
		t.Fatalf("real interrupted provider boundary = %+v, %v; send error = %v", observation, err, interruptSendErr)
	}
}
