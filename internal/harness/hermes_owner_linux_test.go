//go:build linux

package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/agent0ai/spynel/internal/roster"
)

// ownerTestStartTicks reads field 22 of /proc/<pid>/stat, the fingerprint
// Hermes records in gateway.pid.
func ownerTestStartTicks(t *testing.T, pid int) int64 {
	t.Helper()
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		t.Fatal(err)
	}
	fields := bytes.Fields(data[bytes.LastIndexByte(data, ')')+1:])
	ticks, err := strconv.ParseInt(string(fields[19]), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return ticks
}

// ownerTestReapedPID returns the PID of a process that has exited and been
// reaped, standing in for a gateway that died (for example in a reboot).
func ownerTestReapedPID(t *testing.T) int {
	t.Helper()
	cmd := exec.Command("true")
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	return cmd.Process.Pid
}

type ownerFixture struct {
	*hermesBoundaryFixture
	home        string
	gatewayHome string
}

// newOwnerFixture admits the fixture's turn, persists its identity, and gives
// the ledger a runtime epoch, as a live Hermes gateway does.
func newOwnerFixture(t *testing.T, status string, epoch int64) *ownerFixture {
	t.Helper()
	f := newHermesBoundaryFixture(t, hermesTurnOK, "do the task")
	f.settle(t, status, "")
	boundary, err := newHermesRunBoundary(f.h, f.spec, headlessKey, f.prompt, roster.Staff{Runner: "hermes", Args: []string{"-p", boundaryTestProfile}})
	if err != nil {
		t.Fatal(err)
	}
	if err := boundary.refreshIdentity(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ledger.db.Exec(`CREATE TABLE runtime_epoch (singleton INTEGER PRIMARY KEY, epoch INTEGER NOT NULL, instance_id TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ledger.db.Exec(`INSERT INTO runtime_epoch VALUES (1, ?, 'test-instance')`, epoch); err != nil {
		t.Fatal(err)
	}
	return &ownerFixture{hermesBoundaryFixture: f, home: boundary.meta.Home, gatewayHome: boundary.meta.GatewayHome}
}

func (f *ownerFixture) writeGatewayPID(t *testing.T, pid int, start int64) {
	t.Helper()
	writeOwnerTestRecord(t, f.home, f.home, pid, start)
}

func writeOwnerTestRecord(t *testing.T, dir, recordedHome string, pid int, start int64) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(map[string]any{"pid": pid, "kind": "hermes-gateway", "start_time": start, "hermes_home": recordedHome})
	if err := os.WriteFile(filepath.Join(dir, hermesGatewayPIDFile), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func (f *ownerFixture) addAdmission(t *testing.T, id, principal, session string, lineage []string, status string, epoch int64, generation any, seq int) {
	t.Helper()
	data, _ := json.Marshal(lineage)
	if _, err := f.ledger.db.Exec(`INSERT INTO session_admissions(
        admission_id,request_id,principal_id,target_session_id,lineage_json,payload_json,payload_digest,intent,status,owner_epoch,generation,seq
    ) VALUES(?,?,?,?,?,'{}','d','queue',?,?,?,?)`, id, "r-"+id, principal, session, string(data), status, epoch, generation, seq); err != nil {
		t.Fatal(err)
	}
}

func (f *ownerFixture) addWorker(t *testing.T, session string, epoch, generation int64, status string) {
	t.Helper()
	if _, err := f.ledger.db.Exec(`INSERT INTO worker_executions(execution_id,session_id,kind,owner_epoch,generation,status,adoption_digest)
        VALUES(?,?,'child',?,?,?,'d')`, "w-"+session+strconv.FormatInt(generation, 10), session, epoch, generation, status); err != nil {
		t.Fatal(err)
	}
}

func (f *ownerFixture) liveness(t *testing.T) BoundaryLiveness {
	t.Helper()
	observation, err := InspectHermesRunBoundary(context.Background(), f.spec)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	return observation.Liveness
}

func TestHermesStartedTurnOfDeadGatewayIsAbsent(t *testing.T) {
	f := newOwnerFixture(t, "started", 11)
	f.writeGatewayPID(t, ownerTestReapedPID(t), 43465815)
	if got := f.liveness(t); got != BoundaryAbsent {
		t.Fatalf("started turn whose gateway PID is dead = %v, want absent", got)
	}
}

func TestHermesStartedTurnOfReusedGatewayPIDIsAbsent(t *testing.T) {
	f := newOwnerFixture(t, "started", 11)
	// After a reboot the recorded PID can belong to an unrelated process with
	// a different start time.
	f.writeGatewayPID(t, os.Getpid(), ownerTestStartTicks(t, os.Getpid())+100000)
	if got := f.liveness(t); got != BoundaryAbsent {
		t.Fatalf("started turn whose gateway PID was reused = %v, want absent", got)
	}
}

func TestHermesStartedTurnOfLiveGatewayStaysLive(t *testing.T) {
	f := newOwnerFixture(t, "started", 11)
	f.writeGatewayPID(t, os.Getpid(), ownerTestStartTicks(t, os.Getpid()))
	if got := f.liveness(t); got != BoundaryLive {
		t.Fatalf("started turn of a live gateway = %v, want live", got)
	}
}

func TestHermesStartedTurnWithoutGatewayRecordStaysLive(t *testing.T) {
	f := newOwnerFixture(t, "started", 11)
	if got := f.liveness(t); got != BoundaryLive {
		t.Fatalf("started turn with no gateway.pid = %v, want live (fail closed)", got)
	}
}

func TestHermesUnknownTurnOfSupersededEpochIsAbsent(t *testing.T) {
	// A restarted gateway marks the old epoch's started turns unknown and
	// never replays them.
	f := newOwnerFixture(t, "unknown", 12)
	if got := f.liveness(t); got != BoundaryAbsent {
		t.Fatalf("unknown turn of superseded epoch = %v, want absent", got)
	}
}

func TestHermesDeadGatewayQueuedFollowUpOnSameSessionIsAbsent(t *testing.T) {
	f := newOwnerFixture(t, "started", 11)
	f.writeGatewayPID(t, ownerTestReapedPID(t), 43465815)
	// A background-process notice queued on the run's own session cannot
	// start: the next gateway marks the started turn unknown, which blocks
	// the session.
	f.addAdmission(t, "follow-up", "automation:agent:"+boundaryTestProfile+":local:dm:"+boundaryTestSession,
		boundaryTestSession, []string{boundaryTestSession}, "queued", 11, nil, 2)
	if got := f.liveness(t); got != BoundaryAbsent {
		t.Fatalf("dead gateway with a queued follow-up on the same session = %v, want absent", got)
	}
}

func TestHermesDeadGatewayQueuedTurnOnLinkedChildSessionStaysLive(t *testing.T) {
	f := newOwnerFixture(t, "started", 11)
	f.writeGatewayPID(t, ownerTestReapedPID(t), 43465815)
	// A child session nothing blocks may still be started by the next gateway.
	f.addAdmission(t, "child-turn", "api", "local-child", []string{boundaryTestSession, "local-child"}, "queued", 11, nil, 2)
	if got := f.liveness(t); got != BoundaryLive {
		t.Fatalf("queued turn on a linked child session = %v, want live", got)
	}
}

func TestHermesDeadGatewayRunningWorkerStaysLive(t *testing.T) {
	f := newOwnerFixture(t, "started", 11)
	f.writeGatewayPID(t, ownerTestReapedPID(t), 43465815)
	f.addWorker(t, boundaryTestSession, 11, 7, "running")
	if got := f.liveness(t); got != BoundaryLive {
		t.Fatalf("orphaned turn with a running worker = %v, want live", got)
	}
}

func TestHermesSkippedLinkedTurnRunningWorkerStaysLive(t *testing.T) {
	// The run's own turn finished at generation 7; a later turn on the same
	// session started at generation 8 under the gateway that died, and its
	// worker may still run.
	f := newOwnerFixture(t, "terminal", 11)
	f.settle(t, "terminal", "completed")
	f.writeGatewayPID(t, ownerTestReapedPID(t), 43465815)
	f.addAdmission(t, "later-turn", "api", boundaryTestSession, []string{boundaryTestSession}, "started", 11, int64(8), 2)
	if got := f.liveness(t); got != BoundaryAbsent {
		t.Fatalf("finished turn with an orphaned later turn and no worker = %v, want absent", got)
	}
	f.addWorker(t, boundaryTestSession, 11, 8, "running")
	if got := f.liveness(t); got != BoundaryLive {
		t.Fatalf("orphaned later turn with a running worker = %v, want live", got)
	}
}

func TestHermesSupersededEpochWithoutGatewayRecordIsAbsent(t *testing.T) {
	f := newOwnerFixture(t, "started", 11)
	if _, err := f.ledger.db.Exec(`UPDATE runtime_epoch SET epoch=12`); err != nil {
		t.Fatal(err)
	}
	if got := f.liveness(t); got != BoundaryAbsent {
		t.Fatalf("started turn of a superseded epoch = %v, want absent", got)
	}
}

func TestHermesGatewayRecordForAnotherHomeStaysLive(t *testing.T) {
	f := newOwnerFixture(t, "started", 11)
	writeOwnerTestRecord(t, f.home, filepath.Join(f.home, "elsewhere"), ownerTestReapedPID(t), 43465815)
	if got := f.liveness(t); got != BoundaryLive {
		t.Fatalf("dead record naming another home = %v, want live", got)
	}
}

func TestHermesLiveProfileGatewayKeepsTurnLive(t *testing.T) {
	// A stale dead root record must not hide a live gateway that serves only
	// the profile and writes its record in the profile folder.
	f := newOwnerFixture(t, "started", 11)
	if f.gatewayHome == "" || filepath.Clean(f.gatewayHome) == filepath.Clean(f.home) {
		t.Fatalf("fixture gateway home %q must be a profile folder distinct from %q", f.gatewayHome, f.home)
	}
	f.writeGatewayPID(t, ownerTestReapedPID(t), 43465815)
	writeOwnerTestRecord(t, f.gatewayHome, f.gatewayHome, os.Getpid(), ownerTestStartTicks(t, os.Getpid()))
	if got := f.liveness(t); got != BoundaryLive {
		t.Fatalf("live profile gateway with a dead root record = %v, want live", got)
	}
}

func TestHermesUnreadableGatewayRecordStaysLive(t *testing.T) {
	f := newOwnerFixture(t, "started", 11)
	if err := os.WriteFile(filepath.Join(f.home, hermesGatewayPIDFile), []byte("{half"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := f.liveness(t); got != BoundaryLive {
		t.Fatalf("half-written gateway record = %v, want live", got)
	}
}

func TestHermesQueuedTurnOfDeadGatewayStaysLive(t *testing.T) {
	// A queued turn may still be started by the next gateway.
	f := newOwnerFixture(t, "queued", 11)
	f.writeGatewayPID(t, ownerTestReapedPID(t), 43465815)
	if got := f.liveness(t); got != BoundaryLive {
		t.Fatalf("queued turn of a dead gateway = %v, want live", got)
	}
}
