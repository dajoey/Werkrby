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
	home string
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
	return &ownerFixture{hermesBoundaryFixture: f, home: boundary.meta.Home}
}

func (f *ownerFixture) writeGatewayPID(t *testing.T, pid int, start int64) {
	t.Helper()
	data, _ := json.Marshal(map[string]any{"pid": pid, "kind": "hermes-gateway", "start_time": start, "hermes_home": f.home})
	if err := os.WriteFile(filepath.Join(f.home, hermesGatewayPIDFile), data, 0o600); err != nil {
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
	// A background-process notice queued on the run's own session can never
	// start: the next gateway marks the started turn unknown, which blocks
	// the session.
	lineage, _ := json.Marshal([]string{boundaryTestSession})
	if _, err := f.ledger.db.Exec(`INSERT INTO session_admissions(
        admission_id,request_id,principal_id,target_session_id,lineage_json,payload_json,payload_digest,intent,status,owner_epoch,generation,seq
    ) VALUES('follow-up','r2','automation',?,?,'{}','d','queue','queued',11,NULL,2)`, boundaryTestSession, string(lineage)); err != nil {
		t.Fatal(err)
	}
	if got := f.liveness(t); got != BoundaryAbsent {
		t.Fatalf("dead gateway with a queued follow-up on the same session = %v, want absent", got)
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
