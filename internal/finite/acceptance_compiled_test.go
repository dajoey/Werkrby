package finite

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The live path: a compiled serve binary on a temp root runs the declared
// acceptance check after the implementation run settles and records a pass
// verdict bound to the repo's commit — while the review prompt carries no
// implementer progress.
func TestFiniteAcceptanceShadowCompiledServe(t *testing.T) {
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
	if err := os.WriteFile(rosterPath, []byte("seats:\n  - name: fake-seat\n    available: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, ownerOutput, ownerDone := startCLIHelperProcess(t, []string{
		"serve", "--root", root, "--worker", worker, "--harness", "finite-engine-fixture",
		"--roster", rosterPath, "--model", "@fake-seat", "--poll-interval", "5ms", "--stop-grace", "25ms",
	}, engineFixturePlanEnv+"="+planPath)
	waitForCLIProcessFile(t, ownerDone, filepath.Join(root, "control.json"), "compiled serve readiness")

	repo := acceptanceTestRepo(t, base)
	head := acceptanceRepoHead(t, repo)

	briefPath := filepath.Join(base, "acceptance probe.md")
	brief := "---\nid: compiled-acceptance\ntitle: Compiled acceptance shadow\nrisk: low\nreview_required: true\n" +
		acceptanceFrontMatter("test -f marker", repo, 60, "") +
		"---\n\nProve the compiled serve runs the shadow acceptance check.\n"
	if err := os.WriteFile(briefPath, []byte(brief), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := runCLIHelperProcess(t, []string{"intake", "--root", root, "--brief", briefPath}, engineFixturePlanEnv+"="+planPath); err != nil {
		t.Fatalf("compiled intake: %v", err)
	}

	// Poll the temp engine.db read-only until the task is done and its
	// acceptance verdict is recorded.
	deadline := time.Now().Add(30 * time.Second)
	var state, payload string
	eventFound := false
	for time.Now().Before(deadline) {
		db, err := sql.Open("sqlite3", "file:"+filepath.ToSlash(filepath.Join(root, "engine.db"))+"?mode=ro&_pragma=busy_timeout(5000)")
		if err == nil {
			stateErr := db.QueryRow(`SELECT state FROM tasks WHERE id='compiled-acceptance'`).Scan(&state)
			payloadErr := db.QueryRow(`SELECT payload_json FROM task_events WHERE kind='acceptance'`).Scan(&payload)
			_ = db.Close()
			if stateErr == nil && state == string(StateDone) && payloadErr == nil && payload != "" {
				eventFound = true
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !eventFound {
		t.Fatalf("task did not reach done with an acceptance event in 30s; last state %q, owner output tail:\n%s",
			state, tailForLog(ownerOutput.String()))
	}
	var recorded acceptancePayloadView
	if err := json.Unmarshal([]byte(payload), &recorded); err != nil {
		t.Fatalf("acceptance payload = %s: %v", payload, err)
	}
	if recorded.Verdict != acceptanceVerdictPass {
		t.Fatalf("acceptance verdict = %+v, want pass", recorded)
	}
	if recorded.Commit != head {
		t.Fatalf("acceptance commit = %q, want the temp repo HEAD %q", recorded.Commit, head)
	}

	// The review run's prompt must not carry the fixture's implementation
	// progress; the implementation run's prompt must.
	db, err := sql.Open("sqlite3", "file:"+filepath.ToSlash(filepath.Join(root, "engine.db"))+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	reviewPrompt := readRunPrompt(t, db, root, "compiled-acceptance", "review")
	// The review prompt renders the implementation run's history line but
	// never the implementer's own progress text.
	if !strings.Contains(reviewPrompt, "- Run 1 (implementation): done.") {
		t.Errorf("review prompt lost the implementation run's history line:\n%s", tailForLog(reviewPrompt))
	}
	if strings.Contains(reviewPrompt, "fake provider completed the run") {
		t.Errorf("review prompt carries implementer progress:\n%s", tailForLog(reviewPrompt))
	}
}

func readRunPrompt(t *testing.T, db *sql.DB, root, taskID, phase string) string {
	t.Helper()
	var runID int64
	if err := db.QueryRow(`SELECT id FROM runs WHERE task_id=? AND phase=?`, taskID, phase).Scan(&runID); err != nil {
		t.Fatalf("read %s run of %s: %v", phase, taskID, err)
	}
	prompt, err := os.ReadFile(filepath.Join(root, "runs", strconv.FormatInt(runID, 10), "prompt.txt"))
	if err != nil {
		t.Fatalf("read %s prompt: %v", phase, err)
	}
	return string(prompt)
}
