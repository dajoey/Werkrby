package finite

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	acceptanceHookWait              = 10 * time.Second
	acceptanceStragglerWait         = 5 * time.Second
	acceptanceStragglerPollInterval = 100 * time.Millisecond
)

// acceptancePayloadView mirrors one recorded acceptance event's payload.
type acceptancePayloadView struct {
	SchemaVersion int    `json:"schema_version"`
	RunID         int64  `json:"run_id"`
	Verdict       string `json:"verdict"`
	Reason        string `json:"reason"`
	ExitCode      int    `json:"exit_code"`
	Commit        string `json:"commit"`
	DurationMS    int64  `json:"duration_ms"`
	OutputTail    string `json:"output_tail"`
}

// acceptanceGit runs one git command in the test repo and fails the test on
// any error.
func acceptanceGit(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return string(out)
}

// acceptanceTestRepo creates a temp git repo with one commit holding marker.
func acceptanceTestRepo(t *testing.T, base string) string {
	t.Helper()
	repo := filepath.Join(base, "repo")
	if err := os.MkdirAll(repo, 0o700); err != nil {
		t.Fatal(err)
	}
	acceptanceGit(t, "-C", repo, "init")
	if err := os.WriteFile(filepath.Join(repo, "marker"), []byte("marker\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	acceptanceGit(t, "-C", repo, "add", "marker")
	acceptanceGit(t, "-C", repo, "-c", "user.email=finite@test", "-c", "user.name=finite", "-c", "commit.gpgsign=false", "commit", "-m", "init")
	return repo
}

func acceptanceRepoHead(t *testing.T, repo string) string {
	t.Helper()
	return strings.TrimSpace(acceptanceGit(t, "-C", repo, "rev-parse", "HEAD"))
}

func acceptanceFrontMatter(command, repo string, timeout int, extra string) string {
	front := "acceptance_check:\n  command: " + strconv.Quote(command) + "\n  repo: " + strconv.Quote(repo)
	if timeout != 0 {
		front += "\n  timeout_seconds: " + strconv.Itoa(timeout)
	}
	if extra != "" {
		front += "\n" + extra
	}
	return front + "\n"
}

func acceptanceIntake(t *testing.T, s *Store, id string, front string) Task {
	t.Helper()
	// review_required: false keeps the scenario at one implementation run.
	path := filepath.Join(t.TempDir(), id+".md")
	data := "---\nid: " + id + "\ntitle: Test task " + id + "\nrisk: low\nreview_required: false\n" +
		front + "---\n\nRun one finite objective and end done.\n"
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatalf("write brief: %v", err)
	}
	task, err := s.IntakeBrief(context.Background(), path)
	if err != nil {
		t.Fatalf("intake brief %s: %v", id, err)
	}
	return task
}

func acceptanceWaitHook(t *testing.T, hooks <-chan int64, taskID string) int64 {
	t.Helper()
	select {
	case runID := <-hooks:
		return runID
	case <-time.After(acceptanceHookWait):
		t.Fatalf("acceptance hook not called for task %s", taskID)
		return 0
	}
}

func acceptanceWaitStragglerGone(t *testing.T, pidPath string) {
	t.Helper()
	deadline := time.Now().Add(acceptanceStragglerWait)
	for {
		check := exec.Command("/bin/sh", "-c", "kill -0 $(cat \""+pidPath+"\") 2>/dev/null")
		if err := check.Run(); err != nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("background child in %s survived the acceptance timeout", pidPath)
		}
		time.Sleep(acceptanceStragglerPollInterval)
	}
}

func acceptanceAssertNoAcceptanceText(t *testing.T, engine *Engine, task Task) {
	t.Helper()
	// Task ids legitimately contain "acceptance"; the forbidden text is the
	// verdict payload leaking into a prompt surface: the event id prefix and
	// the payload's own keys.
	for _, banned := range []string{"acceptance:", `"verdict"`, "verifier_failed"} {
		history, err := engine.store.TaskHistory(context.Background(), task.ID)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(history, banned) {
			t.Errorf("task history carries acceptance text %q:\n%s", banned, history)
		}
		projection, err := os.ReadFile(task.ProjectionPath)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(projection), banned) {
			t.Errorf("projection carries acceptance text %q:\n%s", banned, projection)
		}
	}
}

func TestParseBriefAcceptanceCheck(t *testing.T) {
	repo := filepath.Join(t.TempDir(), "repo")
	if err := os.MkdirAll(repo, 0o700); err != nil {
		t.Fatal(err)
	}
	briefBytes := func(front string) []byte {
		return []byte("---\nid: acceptance-parse\ntitle: Acceptance parse\nrisk: low\nreview_required: false\n" + front + "---\n\nBody.\n")
	}

	parsed, err := ParseBrief(briefBytes(acceptanceFrontMatter("test -f marker", repo, 60, "")))
	if err != nil {
		t.Fatalf("valid acceptance_check rejected: %v", err)
	}
	if parsed.AcceptanceCheck == nil {
		t.Fatal("acceptance_check not parsed into Brief.AcceptanceCheck")
	}
	if parsed.AcceptanceCheck.Command != "test -f marker" || parsed.AcceptanceCheck.Repo != repo || parsed.AcceptanceCheck.TimeoutSeconds != 60 {
		t.Fatalf("parsed check = %+v", parsed.AcceptanceCheck)
	}

	parsed, err = ParseBrief(briefBytes(acceptanceFrontMatter("test -f marker", repo, 0, "")))
	if err != nil {
		t.Fatalf("check without timeout_seconds rejected: %v", err)
	}
	if parsed.AcceptanceCheck == nil || parsed.AcceptanceCheck.TimeoutSeconds != acceptanceDefaultTimeoutSeconds {
		t.Fatalf("default timeout = %+v, want %d", parsed.AcceptanceCheck, acceptanceDefaultTimeoutSeconds)
	}

	for name, front := range map[string]string{
		"empty command":    acceptanceFrontMatter("", repo, 60, ""),
		"relative repo":    acceptanceFrontMatter("test -f marker", "relative/path", 60, ""),
		"zero timeout":     acceptanceFrontMatter("test -f marker", repo, 0, "\x00zero"),
		"oversize timeout": acceptanceFrontMatter("test -f marker", repo, acceptanceMaxTimeoutSeconds+1, ""),
		"unknown sub-key":  acceptanceFrontMatter("test -f marker", repo, 60, "  unknown_key: x"),
		"missing repo":     "acceptance_check:\n  command: \"test -f marker\"\n",
	} {
		frontBytes := briefBytes(front)
		if name == "zero timeout" {
			// timeout_seconds: 0 is a present zero value, distinct from an
			// absent key; build it directly.
			frontBytes = briefBytes("acceptance_check:\n  command: \"test -f marker\"\n  repo: " + strconv.Quote(repo) + "\n  timeout_seconds: 0\n")
		}
		if _, err := ParseBrief(frontBytes); err == nil {
			t.Errorf("%s: accepted, want rejected", name)
		}
	}

	parsed, err = ParseBrief(briefBytes(""))
	if err != nil {
		t.Fatalf("brief without the block rejected: %v", err)
	}
	if parsed.AcceptanceCheck != nil {
		t.Fatalf("brief without the block parsed with a check: %+v", parsed.AcceptanceCheck)
	}
}

// Shadow acceptance: every settled implementation done/review with a declared
// check gets exactly one verdict, and the verdict changes nothing about the
// task, its charges, its prompts, or the repo's worktrees.
func TestAcceptanceShadowVerdicts(t *testing.T) {
	base := t.TempDir()
	repo := acceptanceTestRepo(t, base)
	head := acceptanceRepoHead(t, repo)
	pidPath := filepath.Join(base, "timeout.pid")

	engine := newEngineFixture(t, engineFixturePlan{})
	hooks := make(chan int64, 16)
	engine.opts.hooks.afterAcceptance = func(runID int64, recorded bool) { hooks <- runID }
	ctx := context.Background()

	scenarios := []struct {
		id    string
		front string
		want  acceptancePayloadView
	}{
		{
			id:    "acceptance-shadow-pass",
			front: acceptanceFrontMatter("test -f marker", repo, 60, ""),
			want:  acceptancePayloadView{Verdict: acceptanceVerdictPass, Reason: "", ExitCode: 0, Commit: head},
		},
		{
			id:    "acceptance-shadow-fail",
			front: acceptanceFrontMatter("test -f marker-missing", repo, 60, ""),
			want:  acceptancePayloadView{Verdict: acceptanceVerdictFail, Reason: "", ExitCode: 1, Commit: head},
		},
		{
			id:    "acceptance-shadow-norepo",
			front: acceptanceFrontMatter("test -f marker", filepath.Join(base, "nope"), 60, ""),
			want:  acceptancePayloadView{Verdict: acceptanceVerdictVerifierFailed, Reason: "repo", ExitCode: -1, Commit: ""},
		},
		{
			id:    "acceptance-shadow-timeout",
			front: acceptanceFrontMatter("sleep 30 & echo $! > \""+pidPath+"\"; wait", repo, 1, ""),
			want:  acceptancePayloadView{Verdict: acceptanceVerdictVerifierFailed, Reason: "timeout", ExitCode: -1, Commit: head},
		},
	}

	control := acceptanceIntake(t, engine.store, "acceptance-shadow-control", "")
	scanEngineUntil(t, engine, control.ID, StateDone, 1)
	controlRuns, err := engine.store.ListRuns(ctx, control.ID)
	if err != nil {
		t.Fatal(err)
	}
	controlCharged := countRows(t, engine.store, `SELECT COUNT(*) FROM charged_attempts WHERE task_id=?`, control.ID)

	for _, sc := range scenarios {
		task := acceptanceIntake(t, engine.store, sc.id, sc.front)
		scanEngineUntil(t, engine, task.ID, StateDone, 1)
		runID := acceptanceWaitHook(t, hooks, task.ID)

		runs, err := engine.store.ListRuns(ctx, task.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(runs) != len(controlRuns) {
			t.Fatalf("task %s has %d runs, control has %d", task.ID, len(runs), len(controlRuns))
		}
		charged := countRows(t, engine.store, `SELECT COUNT(*) FROM charged_attempts WHERE task_id=?`, task.ID)
		if charged != controlCharged {
			t.Fatalf("task %s has %d charged attempts, control has %d", task.ID, charged, controlCharged)
		}
		acceptanceAssertNoAcceptanceText(t, engine, task)

		if runID != runs[len(runs)-1].ID {
			t.Fatalf("hook reported run %d, task's last run is %d", runID, runs[len(runs)-1].ID)
		}
		payloadRaw, exists, err := engine.store.AcceptanceEvent(ctx, runID)
		if err != nil || !exists {
			t.Fatalf("task %s: acceptance event missing: %v", task.ID, err)
		}
		var payload acceptancePayloadView
		if err := json.Unmarshal(payloadRaw, &payload); err != nil {
			t.Fatalf("task %s: acceptance payload = %s: %v", task.ID, payloadRaw, err)
		}
		sc.want.SchemaVersion = 1
		sc.want.RunID = runID
		if payload.Verdict != sc.want.Verdict || payload.Reason != sc.want.Reason || payload.ExitCode != sc.want.ExitCode {
			t.Fatalf("task %s verdict = %+v, want %+v", task.ID, payload, sc.want)
		}
		if payload.Commit != sc.want.Commit {
			t.Fatalf("task %s commit = %q, want %q", task.ID, payload.Commit, sc.want.Commit)
		}
		if payload.DurationMS < 0 {
			t.Fatalf("task %s duration_ms = %d", task.ID, payload.DurationMS)
		}
		if len(payload.Commit) != 0 && len(payload.Commit) != 40 {
			t.Fatalf("task %s commit is not 40 hex: %q", task.ID, payload.Commit)
		}

		worktrees := acceptanceGit(t, "-C", repo, "worktree", "list")
		if got := strings.Count(worktrees, repo); got != 1 {
			t.Fatalf("task %s left %d repo worktrees:\n%s", task.ID, got, worktrees)
		}
		dir := filepath.Join(engine.cfg.Root, "acceptance", strconv.FormatInt(runID, 10))
		if _, err := os.Lstat(dir); !os.IsNotExist(err) {
			t.Fatalf("task %s left %s behind: %v", task.ID, dir, err)
		}
		if sc.want.Reason == "timeout" {
			acceptanceWaitStragglerGone(t, pidPath)
		}
	}

	for _, run := range controlRuns {
		if _, exists, err := engine.store.AcceptanceEvent(ctx, run.ID); err != nil || exists {
			t.Fatalf("control task without a block got an acceptance event (run %d): %v", run.ID, err)
		}
	}
}

// One run gets at most one acceptance verdict: a settlement replay and a
// direct restart of the same check neither rerun the command nor re-record.
func TestAcceptanceShadowOncePerRun(t *testing.T) {
	base := t.TempDir()
	repo := acceptanceTestRepo(t, base)
	countPath := filepath.Join(base, "count")

	engine := newEngineFixture(t, engineFixturePlan{})
	hooks := make(chan int64, 4)
	engine.opts.hooks.afterAcceptance = func(runID int64, recorded bool) { hooks <- runID }
	ctx := context.Background()

	task := acceptanceIntake(t, engine.store, "acceptance-once",
		acceptanceFrontMatter("echo x >> \""+countPath+"\"", repo, 60, ""))
	run, err := engine.store.Dispatch(ctx, DispatchRequest{
		TaskID: task.ID, ExpectedVersion: task.Version, Phase: PhaseImplementation,
		Seat: "fake-seat", SessionKey: "finite:test", LaunchReason: "initial",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	request := SettlementRequest{
		RunID: run.ID, ExpectedTaskVersion: task.Version + 1, ExecutionOutcome: RunOK,
		Result: validTestResult(run, ResultDone),
	}
	if _, err := engine.settleRecovered(ctx, request); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.settleRecovered(ctx, request); err != nil {
		t.Fatalf("settlement replay: %v", err)
	}
	acceptanceWaitHook(t, hooks, task.ID)
	engine.startAcceptance(task.ID, run.ID, AcceptanceCheck{
		Command: "echo x >> \"" + countPath + "\"", Repo: repo, TimeoutSeconds: 60,
	})
	time.Sleep(2 * time.Second)

	select {
	case runID := <-hooks:
		t.Fatalf("acceptance hook called more than once (run %d)", runID)
	default:
	}
	if got := countRows(t, engine.store, `SELECT COUNT(*) FROM task_events WHERE kind='acceptance'`); got != 1 {
		t.Fatalf("acceptance events = %d, want 1", got)
	}
	counted, err := os.ReadFile(countPath)
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(strings.TrimSpace(string(counted)), "\n") + 1; lines != 1 {
		t.Fatalf("check command ran %d times, want once (count file: %q)", lines, counted)
	}
}

// The verdict record is the only write: no state, version, outbox row, or
// charge moves when a shadow verdict lands, and a replay is a no-op.
func TestRecordAcceptanceChangesNothingElse(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	task := intakeTestTask(t, s, "acceptance-record", true)
	run := dispatchTestRun(t, s, task, PhaseImplementation)
	if _, err := s.SettleRun(ctx, SettlementRequest{
		RunID: run.ID, ExpectedTaskVersion: task.Version + 1, ExecutionOutcome: RunOK,
		Result: validTestResult(run, ResultReview),
	}); err != nil {
		t.Fatal(err)
	}
	before, err := s.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if before.State != StateReview {
		t.Fatalf("task state = %s, want review", before.State)
	}
	outboxBefore := countRows(t, s, `SELECT COUNT(*) FROM outbox`)
	chargedBefore := countRows(t, s, `SELECT COUNT(*) FROM charged_attempts`)

	payload, err := json.Marshal(acceptancePayloadView{
		SchemaVersion: 1, RunID: run.ID, Verdict: acceptanceVerdictPass, ExitCode: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	inserted, err := s.RecordAcceptance(ctx, task.ID, run.ID, payload)
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("first RecordAcceptance = false, want true")
	}
	var from, to string
	if err := s.db.QueryRow(`SELECT from_state,to_state FROM task_events WHERE event_id=?`,
		acceptanceEventID(run.ID)).Scan(&from, &to); err != nil {
		t.Fatal(err)
	}
	if from != string(StateReview) || to != string(StateReview) {
		t.Fatalf("acceptance event states = %s -> %s, want review -> review", from, to)
	}
	after, err := s.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != before.State || after.Version != before.Version {
		t.Fatalf("task changed by the record: %+v -> %+v", before, after)
	}
	if got := countRows(t, s, `SELECT COUNT(*) FROM outbox`); got != outboxBefore {
		t.Fatalf("outbox rows = %d, want %d", got, outboxBefore)
	}
	if got := countRows(t, s, `SELECT COUNT(*) FROM charged_attempts`); got != chargedBefore {
		t.Fatalf("charged_attempts rows = %d, want %d", got, chargedBefore)
	}

	inserted, err = s.RecordAcceptance(ctx, task.ID, run.ID, payload)
	if err != nil {
		t.Fatal(err)
	}
	if inserted {
		t.Fatal("second RecordAcceptance = true, want false")
	}
	if got := countRows(t, s, `SELECT COUNT(*) FROM task_events WHERE event_id=?`, acceptanceEventID(run.ID)); got != 1 {
		t.Fatalf("acceptance events = %d, want 1", got)
	}
}

// A task without a declared check records nothing; a task after it still gets
// its verdict.
func TestAcceptanceShadowSkipsWithoutBlock(t *testing.T) {
	repo := acceptanceTestRepo(t, t.TempDir())
	engine := newEngineFixture(t, engineFixturePlan{})
	hooks := make(chan int64, 4)
	engine.opts.hooks.afterAcceptance = func(runID int64, recorded bool) { hooks <- runID }
	ctx := context.Background()

	plain := acceptanceIntake(t, engine.store, "acceptance-skip-plain", "")
	scanEngineUntil(t, engine, plain.ID, StateDone, 1)
	blocked := acceptanceIntake(t, engine.store, "acceptance-skip-blocked",
		acceptanceFrontMatter("test -f marker", repo, 60, ""))
	scanEngineUntil(t, engine, blocked.ID, StateDone, 1)
	acceptanceWaitHook(t, hooks, blocked.ID)

	plainRuns, err := engine.store.ListRuns(ctx, plain.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, run := range plainRuns {
		if _, exists, err := engine.store.AcceptanceEvent(ctx, run.ID); err != nil || exists {
			t.Fatalf("plain task run %d got an acceptance event: %v", run.ID, err)
		}
	}
	select {
	case runID := <-hooks:
		t.Fatalf("acceptance hook called more than once (extra run %d)", runID)
	default:
	}
}
