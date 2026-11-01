package finite

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const wantBriefChangedReason = "immutable brief changed after intake"

// A worker once appended progress to a committed brief; every dispatch after
// that was refused with an anonymous SHA-256 mismatch error and retried each
// scan, so the task sat in flight forever. The engine must instead fail the
// task once with a reason naming the brief and report an error that names the
// task.
func TestFiniteChangedBriefFailsTaskOnceAndNamesIt(t *testing.T) {
	engine := newEngineFixture(t, engineFixturePlan{})
	ctx := context.Background()
	task, err := engine.store.IntakeBrief(ctx, writeTestBrief(t, "brief-mismatch", false))
	if err != nil {
		t.Fatal(err)
	}
	assertBriefMode(t, task.BriefPath, 0o444)
	if err := os.Chmod(task.BriefPath, 0o600); err != nil {
		t.Fatal(err)
	}
	tampered := "---\nid: brief-mismatch\ntitle: Tampered after intake\nrisk: low\nreview_required: false\n---\n\nRewritten after intake.\n"
	if err := os.WriteFile(task.BriefPath, []byte(tampered), 0o600); err != nil {
		t.Fatal(err)
	}

	scanErr := engine.ScanOnce(ctx)
	if scanErr == nil {
		t.Fatal("dispatch of a changed brief returned no error")
	}
	if !strings.Contains(scanErr.Error(), task.ID) {
		t.Errorf("scan error does not name the task: %v", scanErr)
	}
	if !strings.Contains(scanErr.Error(), wantBriefChangedReason) {
		t.Errorf("scan error does not name the changed brief: %v", scanErr)
	}
	failed, err := engine.store.GetTask(ctx, task.ID)
	if err != nil || failed.State != StateFailed || failed.Version != task.Version+1 {
		t.Fatalf("task after changed brief = %+v (version %d), %v; want failed version %d", failed, failed.Version, err, task.Version+1)
	}
	var payload string
	if err := engine.store.db.QueryRow(`SELECT payload_json FROM task_events WHERE task_id=? AND kind='admission_failed'`, task.ID).Scan(&payload); err != nil {
		t.Fatalf("no admission failure event for the changed brief: %v", err)
	}
	if !strings.Contains(payload, wantBriefChangedReason) {
		t.Errorf("admission failure payload = %q, want reason %q", payload, wantBriefChangedReason)
	}
	if got := countRows(t, engine.store, `SELECT COUNT(*) FROM outbox`); got != 1 {
		t.Fatalf("terminal outbox rows = %d, want one", got)
	}
	if got := engineInvocationCount(t, engine); got != 0 {
		t.Fatalf("changed brief spawned %d provider runs", got)
	}
	assertEngineInvariants(t, engine.store, task.ID)

	if err := engine.ScanOnce(ctx); err != nil {
		t.Fatalf("later scan retried the changed brief: %v", err)
	}
	if got := countRows(t, engine.store, `SELECT COUNT(*) FROM task_events WHERE task_id=? AND kind='admission_failed'`, task.ID); got != 1 {
		t.Fatalf("admission failure recorded %d times, want exactly once", got)
	}
	if got := engineInvocationCount(t, engine); got != 0 {
		t.Fatalf("later scan spawned %d providers", got)
	}
}

// A deleted brief is the same defect: the task must fail once instead of
// erroring anonymously on every scan.
func TestFiniteDeletedBriefFailsTaskOnce(t *testing.T) {
	engine := newEngineFixture(t, engineFixturePlan{})
	ctx := context.Background()
	task, err := engine.store.IntakeBrief(ctx, writeTestBrief(t, "brief-deleted", false))
	if err != nil {
		t.Fatal(err)
	}
	assertBriefMode(t, task.BriefPath, 0o444)
	if err := os.Remove(task.BriefPath); err != nil {
		t.Fatal(err)
	}
	scanErr := engine.ScanOnce(ctx)
	if scanErr == nil || !strings.Contains(scanErr.Error(), task.ID) || !strings.Contains(scanErr.Error(), wantBriefChangedReason) {
		t.Fatalf("deleted brief scan error = %v, want it to name the task and the changed brief", scanErr)
	}
	failed, err := engine.store.GetTask(ctx, task.ID)
	if err != nil || failed.State != StateFailed {
		t.Fatalf("task after deleted brief = %+v, %v; want failed", failed, err)
	}
	if err := engine.ScanOnce(ctx); err != nil {
		t.Fatalf("later scan retried the deleted brief: %v", err)
	}
}

// The compiled serve must log the task id when a brief changes after intake;
// before the fix the log repeated an anonymous mismatch error while the task
// showed as in flight with nothing running.
func TestFiniteCompiledServeNamesTaskWhenBriefChanges(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "finite serve root")
	worker := filepath.Join(base, "worker target")
	if err := os.MkdirAll(worker, 0o700); err != nil {
		t.Fatal(err)
	}
	plan := engineFixturePlan{Runs: map[string]engineFixtureBehavior{}, InvocationsPath: filepath.Join(base, "provider invocations.jsonl")}
	planPath := filepath.Join(base, "provider plan.json")
	writeEngineJSONFile(t, planPath, plan)
	rosterPath := filepath.Join(base, "seats.yaml")
	writeBriefRoster := func(available bool) {
		t.Helper()
		value := "false"
		if available {
			value = "true"
		}
		if err := os.WriteFile(rosterPath, []byte("seats:\n  - name: fake-seat\n    available: "+value+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// The seat starts unavailable so the engine cannot dispatch before the
	// brief is tampered with; flipping the roster releases the next scan.
	writeBriefRoster(false)
	_, ownerOutput, ownerDone := startCLIHelperProcess(t, []string{
		"serve", "--root", root, "--worker", worker, "--harness", "finite-engine-fixture",
		"--roster", rosterPath, "--poll-interval", "5ms", "--stop-grace", "25ms",
	}, engineFixturePlanEnv+"="+planPath)
	waitForCLIProcessFile(t, ownerDone, filepath.Join(root, "control.json"), "compiled serve readiness")

	briefPath := filepath.Join(base, "changed brief.md")
	brief := "---\nid: compiled-mismatch\ntitle: Compiled mismatch\nrisk: low\nreview_required: false\n---\n\nEngine must fail this task when its brief changes after intake.\n"
	if err := os.WriteFile(briefPath, []byte(brief), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := runCLIHelperProcess(t, []string{"intake", "--root", root, "--brief", briefPath}, engineFixturePlanEnv+"="+planPath); err != nil {
		t.Fatalf("compiled intake: %v", err)
	}
	committed := filepath.Join(root, "briefs", "compiled-mismatch.md")
	assertBriefMode(t, committed, 0o444)
	if err := os.Chmod(committed, 0o600); err != nil {
		t.Fatal(err)
	}
	tampered := "---\nid: compiled-mismatch\ntitle: Tampered\nrisk: low\nreview_required: false\n---\n\nRewritten after intake.\n"
	if err := os.WriteFile(committed, []byte(tampered), 0o600); err != nil {
		t.Fatal(err)
	}
	writeBriefRoster(true)

	deadline := time.Now().Add(30 * time.Second)
	var status ControlStatus
	for time.Now().Before(deadline) {
		output, err := runCLIHelperProcess(t, []string{"status", "--root", root, "--task", "compiled-mismatch"}, engineFixturePlanEnv+"="+planPath)
		if err != nil {
			t.Fatalf("compiled status: %v; output=%s", err, output)
		}
		if err := json.Unmarshal(output, &status); err != nil {
			t.Fatalf("compiled status output = %q, %v", output, err)
		}
		if status.State == StateFailed {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if status.State != StateFailed {
		t.Fatalf("compiled workflow status = %+v, owner output=%s", status, ownerOutput.String())
	}
	// A settle window after the failure proves the named log line appears and
	// is not repeated by later scans.
	time.Sleep(500 * time.Millisecond)
	named := "werkrby-finite: finite: task compiled-mismatch: " + wantBriefChangedReason
	log := ownerOutput.String()
	if !strings.Contains(log, named) {
		t.Fatalf("serve log does not name the task on the changed brief; log tail=%s", tailForLog(log))
	}
	if got := strings.Count(log, named); got != 1 {
		t.Fatalf("task-named mismatch logged %d times, want exactly once; log tail=%s", got, tailForLog(log))
	}
}

func assertBriefMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != want {
		t.Fatalf("published brief %s mode = %o, want %o", path, perm, want)
	}
}

func tailForLog(log string) string {
	const tail = 2000
	if len(log) > tail {
		return log[len(log)-tail:]
	}
	return log
}
