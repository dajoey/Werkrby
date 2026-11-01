package finite

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent0ai/spynel/internal/harness"
)

func newEarlyRecoveryEngine(t *testing.T) *Engine {
	t.Helper()
	base := t.TempDir()
	root := filepath.Join(base, "state")
	worker := filepath.Join(base, "worker")
	if err := os.MkdirAll(filepath.Join(worker, ".werkrby"), 0o700); err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(Config{Root: root, WorkerDir: worker}, EngineOptions{
		Harness: harness.HarnessConfig{Name: "herdr", Cwd: worker, Model: "hermes"},
		Seats:   func(context.Context) ([]Seat, error) { return []Seat{{Name: "fake", Available: true}}, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	configureFiniteHermesOwner(engine, engine.opts.Harness)
	t.Cleanup(func() { _ = engine.Close() })
	return engine
}

func newEarlyRecoveryRun(t *testing.T, engine *Engine) Run {
	t.Helper()
	task := intakeTestTask(t, engine.store, "early-recovery", false)
	run, err := engine.store.Dispatch(context.Background(), DispatchRequest{
		TaskID: task.ID, ExpectedVersion: task.Version, Phase: PhaseImplementation,
		Seat: "fake", SessionKey: "finite:early-recovery", LaunchReason: "initial",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	token := strings.Repeat("x", 43)
	prompt := Prompt{Text: "early recovery prompt", BundleHash: "bundle", FullBundle: true}
	if err := engine.prepareRun(context.Background(), run, token, prompt); err != nil {
		t.Fatal(err)
	}
	run.ProcessToken = token
	request := supervisorRequest{SchemaVersion: 1, RunID: run.ID, RunDir: run.RunDir, ProcessToken: token, SessionKey: run.SessionKey, Harness: engine.opts.Harness}
	data, _ := json.Marshal(request)
	if err := os.WriteFile(filepath.Join(run.RunDir, "supervisor.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(run.RunDir, "prompt.txt"), []byte(prompt.Text), 0o600); err != nil {
		t.Fatal(err)
	}
	return run
}

func writeEarlyStartAndCompletion(t *testing.T, run Run, outcome RunOutcome) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(run.RunDir, "supervisor-started.json"), []byte(`{"started":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(supervisorCompletion{SchemaVersion: 1, RunID: run.ID, Outcome: outcome})
	if err := os.WriteFile(filepath.Join(run.RunDir, "completion.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func assertEarlySettled(t *testing.T, engine *Engine, run Run, outcome RunOutcome) {
	t.Helper()
	if err := engine.recoverRun(context.Background(), run); err != nil {
		t.Fatalf("recover run: %v", err)
	}
	settled, err := engine.store.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if settled.EndedAt == nil || settled.Outcome != outcome {
		t.Fatalf("settled run = %+v, want %s", settled, outcome)
	}
}

func TestEarlyRecoverySettlesInvalidStaffingWithoutAdmission(t *testing.T) {
	engine := newEarlyRecoveryEngine(t)
	run := newEarlyRecoveryRun(t, engine)
	writeEarlyStartAndCompletion(t, run, RunFail)
	assertEarlySettled(t, engine, run, RunFail)
}

func TestEarlyRecoverySettlesPreAdmissionCrash(t *testing.T) {
	engine := newEarlyRecoveryEngine(t)
	run := newEarlyRecoveryRun(t, engine)
	writeEarlyStartAndCompletion(t, run, RunFail)
	assertEarlySettled(t, engine, run, RunFail)
}

func TestEarlyRecoveryStopBeforeSpawnSettlesInterrupted(t *testing.T) {
	engine := newEarlyRecoveryEngine(t)
	run := newEarlyRecoveryRun(t, engine)
	task, err := engine.store.GetTask(context.Background(), run.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.store.RequestStop(context.Background(), task.ID, task.Version); err != nil {
		t.Fatal(err)
	}
	assertEarlySettled(t, engine, run, RunInterrupted)
}

func TestEarlyRecoveryMissingMetadataAfterAdmissionFailsClosed(t *testing.T) {
	engine := newEarlyRecoveryEngine(t)
	home := filepath.Join(t.TempDir(), "hermes-home")
	profile := "wk-finite-test"
	if err := os.MkdirAll(filepath.Join(home, "profiles", profile), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".finite-hermes-disposable"), []byte(profile+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HERMES_HOME", home)
	if err := os.WriteFile(filepath.Join(engine.cfg.WorkerDir, ".werkrby", "roster.yaml"), []byte("staff:\n  pilot:\n    runner: hermes\n    headless: true\n    args: [\"-p\", \"wk-finite-test\"]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	engine.opts.Harness.Model = "@pilot"
	run := newEarlyRecoveryRun(t, engine)
	writeEarlyStartAndCompletion(t, run, RunFail)
	ledger := filepath.Join(home, "profiles", profile, "state.db")
	db, err := sql.Open("sqlite3", "file:"+filepath.ToSlash(ledger)+"?_pragma=journal_mode(wal)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE session_admissions(admission_id TEXT PRIMARY KEY,request_id TEXT,principal_id TEXT,target_session_id TEXT,lineage_json TEXT,payload_json TEXT,payload_digest TEXT,intent TEXT,status TEXT,owner_epoch INTEGER,generation INTEGER,outcome TEXT,seq INTEGER)`); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]any{"text": "early recovery prompt"})
	if _, err := db.Exec(`INSERT INTO session_admissions(admission_id,request_id,principal_id,target_session_id,lineage_json,payload_json,status,owner_epoch,generation,seq) VALUES('a1','r1','api','local-early','["local-early"]',?,'terminal',1,1,1)`, string(payload)); err != nil {
		t.Fatal(err)
	}
	if err := engine.recoverRun(context.Background(), run); err == nil {
		t.Fatal("missing metadata after admission was treated as absence")
	}
	open, err := engine.store.GetRun(context.Background(), run.ID)
	if err != nil || open.EndedAt != nil {
		t.Fatalf("post-admission run = %+v, %v; must remain open", open, err)
	}
}

func TestEarlyRecoveryCorruptMetadataFailsClosed(t *testing.T) {
	engine := newEarlyRecoveryEngine(t)
	run := newEarlyRecoveryRun(t, engine)
	writeEarlyStartAndCompletion(t, run, RunFail)
	if err := os.WriteFile(filepath.Join(run.RunDir, "hermes-provider.json"), []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := engine.recoverRun(context.Background(), run); err == nil {
		t.Fatal("corrupt metadata was treated as absence")
	}
}

func TestLiveSecretBridgeRequiresEnvironment(t *testing.T) {
	t.Setenv("WERKRBY_FINITE_HERMES_SECRET_BRIDGE", "")
	_, err := liveSecretBridgePath()
	if err == nil || !strings.Contains(err.Error(), "WERKRBY_FINITE_HERMES_SECRET_BRIDGE") || !strings.Contains(err.Error(), "setup required") {
		t.Fatalf("missing bridge error = %v, want safe setup guidance", err)
	}
}
