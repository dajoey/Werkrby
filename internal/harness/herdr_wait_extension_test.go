package harness

import (
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const herdrTimeoutOut = `{"error":{"code":"timeout","message":"timed out waiting for agent status"},"id":"cli:agent:prompt"}`

func agentGet(status string) []byte {
	return []byte(`{"result":{"agent":{"agent_status":"` + status + `"}}}`)
}

func agentGetWithPane(status, paneID string) []byte {
	return []byte(`{"result":{"agent":{"agent_status":"` + status + `","pane_id":"` + paneID + `"}}}`)
}

type scriptedHerdr struct {
	calls       []string
	get         []string // successive agent_status values; "" means the get fails
	getPaneID   string   // when non-empty, get responses carry this pane_id so the output probe runs
	wait        []error  // successive wait results
	processInfo []string // successive herdr pane process-info payloads; "" means the probe fails
	clock       *fakeHerdrClock
}

func (s *scriptedHerdr) run(_ context.Context, args ...string) ([]byte, error) {
	s.calls = append(s.calls, strings.Join(args[:2], " "))
	switch args[1] {
	case "get":
		status := s.get[0]
		s.get = s.get[1:]
		if status == "" {
			return nil, errors.New("agent not found")
		}
		if s.getPaneID != "" {
			return agentGetWithPane(status, s.getPaneID), nil
		}
		return agentGet(status), nil
	case "wait":
		err := s.wait[0]
		s.wait = s.wait[1:]
		if s.clock != nil {
			s.clock.advance(20 * time.Minute) // one 15-to-20-minute wait step passes
		}
		if err != nil {
			return []byte(herdrTimeoutOut), err
		}
		return []byte(`{"result":{}}`), nil
	case "process-info":
		payload := s.processInfo[0]
		s.processInfo = s.processInfo[1:]
		if payload == "" {
			return nil, errors.New("pane process-info failed")
		}
		return []byte(payload), nil
	}
	return nil, errors.New("unexpected")
}

// fakeHerdrClock stands in for herdrNow so wall-clock waits are testable.
type fakeHerdrClock struct{ now time.Time }

func newFakeHerdrClock(t *testing.T) *fakeHerdrClock {
	t.Helper()
	clock := &fakeHerdrClock{now: time.Date(2026, 10, 10, 1, 0, 0, 0, time.UTC)}
	original := herdrNow
	herdrNow = clock.Now
	t.Cleanup(func() { herdrNow = original })
	return clock
}

func (c *fakeHerdrClock) Now() time.Time { return c.now }

func (c *fakeHerdrClock) advance(d time.Duration) { c.now = c.now.Add(d) }

// herdrProcessInfoPayload builds a herdr pane process-info response in the
// exact shape of real `herdr pane process-info` output (field names copied
// from a live capture); the treeReadBytes stub decides what the pids read.
func herdrProcessInfoPayload() string {
	return `{"id":"cli:pane:process_info","result":{"process_info":{"foreground_process_group_id":818020,"foreground_processes":[{"argv":["pi"],"cmdline":"pi","cwd":"/home/dajoey/aibs/spyjo","name":"pi","pid":818020}],"pane_id":"wQ:p359","shell_pid":817989},"type":"pane_process_info"}}`
}

// stubTreeReadBytes replaces the /proc reader with a scripted byte counter:
// successive values, holding the last once the script runs out.
func stubTreeReadBytes(t *testing.T, values []int64) {
	t.Helper()
	original := treeReadBytes
	i := 0
	treeReadBytes = func(int) int64 {
		if i >= len(values) {
			return values[len(values)-1]
		}
		v := values[i]
		i++
		return v
	}
	t.Cleanup(func() { treeReadBytes = original })
}

// Real `herdr pane process-info --pane wQ:p359` output captured 2026-10-10.
const herdrRealProcessInfo = `{"id":"cli:pane:process_info","result":{"process_info":{"foreground_process_group_id":818020,"foreground_processes":[{"argv":["pi"],"cmdline":"pi","cwd":"/home/dajoey/aibs/spyjo","name":"pi","pid":818020}],"pane_id":"wQ:p359","shell_pid":817989},"type":"pane_process_info"}}`

// A worker that keeps producing pane output inside one long tool call must
// never be retired by the wall-clock ceiling: the deadline re-arms whenever
// the pane's foreground processes read more bytes. This is the regression
// test for the 2026-10-10 burn where two 2-hour ceilings and two recovery
// attempts were charged to one healthy turn that sat inside bash poll loops.
func TestExtendHerdrWaitPaneProgressKeepsTurnAlive(t *testing.T) {
	clock := newFakeHerdrClock(t)
	stubTreeReadBytes(t, []int64{100, 200, 300, 400, 500, 600, 700})
	fake := &scriptedHerdr{
		get:         []string{"working", "working", "working", "working", "working", "working", "working", "done"},
		getPaneID:   "wQ:p359",
		wait:        []error{errors.New("timeout"), errors.New("timeout"), errors.New("timeout"), errors.New("timeout"), errors.New("timeout"), errors.New("timeout"), errors.New("timeout")},
		processInfo: []string{herdrProcessInfoPayload(), herdrProcessInfoPayload(), herdrProcessInfoPayload(), herdrProcessInfoPayload(), herdrProcessInfoPayload(), herdrProcessInfoPayload(), herdrProcessInfoPayload()},
		clock:       clock,
	}
	out, err := extendHerdrWait(context.Background(), "sj-t-a1-x", []byte(herdrTimeoutOut), errors.New("exit status 1"), 2*time.Hour, fake.run,
		func(string) {})
	if err != nil {
		t.Fatalf("turn with advancing pane output was retired at the ceiling: %v", err)
	}
	if !strings.Contains(string(out), "done") {
		t.Fatalf("expected the done get response, got %s", out)
	}
	// Seven re-armed wait steps (140 simulated minutes against a 105-minute
	// wall that pre-fix retired the turn at step six), then the done get.
	want := strings.TrimSuffix(strings.Repeat("agent get,pane process-info,agent wait,", 7), ",") + ",agent get"
	if got := strings.Join(fake.calls, ","); got != want {
		t.Fatalf("calls = %v, want %v", got, want)
	}
}

// A pane herdr reports working but that reads no more bytes still hits the
// ceiling: the bound moves from wall-clock-since-turn-start to
// no-pane-output-progress.
func TestExtendHerdrWaitNoPaneProgressStillBounded(t *testing.T) {
	clock := newFakeHerdrClock(t)
	stubTreeReadBytes(t, []int64{100, 100, 100, 100, 100, 100, 100})
	fake := &scriptedHerdr{
		get:         []string{"working", "working", "working", "working", "working", "working", "working"},
		getPaneID:   "wQ:p359",
		wait:        []error{errors.New("timeout"), errors.New("timeout"), errors.New("timeout"), errors.New("timeout"), errors.New("timeout"), errors.New("timeout"), errors.New("timeout")},
		processInfo: []string{herdrProcessInfoPayload(), herdrProcessInfoPayload(), herdrProcessInfoPayload(), herdrProcessInfoPayload(), herdrProcessInfoPayload(), herdrProcessInfoPayload(), herdrProcessInfoPayload()},
		clock:       clock,
	}
	_, err := extendHerdrWait(context.Background(), "sj-t-a1-x", []byte(herdrTimeoutOut), errors.New("exit status 1"), 2*time.Hour, fake.run,
		func(string) {})
	if err == nil {
		t.Fatal("a working pane with no output progress must still be retired at the ceiling")
	}
}

// A failing pane-output probe (older herdr, missing command) keeps today's
// bounded behavior instead of extending forever.
func TestExtendHerdrWaitProbeFailureKeepsBound(t *testing.T) {
	clock := newFakeHerdrClock(t)
	fake := &scriptedHerdr{
		get:         []string{"working", "working", "working", "working", "working", "working", "working"},
		wait:        []error{errors.New("timeout"), errors.New("timeout"), errors.New("timeout"), errors.New("timeout"), errors.New("timeout"), errors.New("timeout"), errors.New("timeout")},
		processInfo: []string{"", "", "", "", "", "", ""},
		clock:       clock,
	}
	_, err := extendHerdrWait(context.Background(), "sj-t-a1-x", []byte(herdrTimeoutOut), errors.New("exit status 1"), 2*time.Hour, fake.run,
		func(string) {})
	if err == nil {
		t.Fatal("a failing pane-output probe must keep the ceiling bound")
	}
}

// The pane process-info parser is tested against the real command's bytes.
func TestPaneOutputBytesParsesRealProcessInfo(t *testing.T) {
	var resp herdrPaneProcessInfoResponse
	if err := json.Unmarshal([]byte(herdrRealProcessInfo), &resp); err != nil {
		t.Fatalf("real process-info output does not parse: %v", err)
	}
	procs := resp.Result.ProcessInfo.ForegroundProcesses
	if len(procs) != 1 || procs[0].PID != 818020 {
		t.Fatalf("parsed foreground processes = %+v, want one pid 818020", procs)
	}
	stubTreeReadBytes(t, []int64{42})
	fake := &scriptedHerdr{processInfo: []string{herdrRealProcessInfo}}
	got, err := paneOutputBytes(context.Background(), fake.run, "wQ:p359")
	if err != nil {
		t.Fatalf("paneOutputBytes failed on real payload: %v", err)
	}
	if got != 42 {
		t.Fatalf("paneOutputBytes = %d, want 42 (sum over parsed foreground pids)", got)
	}
}

// treeReadBytes reads a real process's /proc io and tolerates missing pids.
func TestTreeReadBytes(t *testing.T) {
	cmd := exec.Command("sh", "-c", "exec sleep 30")
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot spawn a process here: %v", err)
	}
	defer func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() }()
	if got := treeReadBytes(cmd.Process.Pid); got <= 0 {
		t.Fatalf("treeReadBytes(%d) = %d, want > 0 for a running process", cmd.Process.Pid, got)
	}
	if got := treeReadBytes(-1); got != 0 {
		t.Fatalf("treeReadBytes(-1) = %d, want 0", got)
	}
}

func TestExtendHerdrWait(t *testing.T) {
	original := errors.New("exit status 1")
	for _, test := range []struct {
		name      string
		get       []string
		wait      []error
		ceiling   time.Duration
		wantErr   bool
		wantCalls []string
	}{
		{name: "still working then finishes", get: []string{"working"}, wait: []error{nil}, ceiling: time.Hour,
			wantCalls: []string{"agent get", "agent wait"}},
		{name: "second wait times out then agent is done", get: []string{"working", "done"}, wait: []error{original}, ceiling: time.Hour,
			wantCalls: []string{"agent get", "agent wait", "agent get"}},
		{name: "finished right at the timeout", get: []string{"idle"}, ceiling: time.Hour,
			wantCalls: []string{"agent get"}},
		{name: "blocked keeps the original failure", get: []string{"blocked"}, ceiling: time.Hour, wantErr: true,
			wantCalls: []string{"agent get"}},
		{name: "vanished pane keeps the original failure", get: []string{""}, ceiling: time.Hour, wantErr: true,
			wantCalls: []string{"agent get"}},
		{name: "ceiling reached", get: []string{"working"}, ceiling: 15 * time.Minute, wantErr: true,
			wantCalls: []string{"agent get"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := &scriptedHerdr{get: test.get, wait: test.wait}
			var notes []string
			_, err := extendHerdrWait(context.Background(), "sj-t-a1-x", []byte(herdrTimeoutOut), original, test.ceiling, fake.run,
				func(text string) { notes = append(notes, text) })
			if (err != nil) != test.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, test.wantErr)
			}
			if strings.Join(fake.calls, ",") != strings.Join(test.wantCalls, ",") {
				t.Fatalf("calls = %v, want %v", fake.calls, test.wantCalls)
			}
			if strings.Contains(strings.Join(test.wantCalls, ","), "agent wait") && len(notes) == 0 {
				t.Fatal("extension was not surfaced as a status event")
			}
		})
	}
	if !isHerdrWaitTimeout(herdrTimeoutOut) || isHerdrWaitTimeout(`{"error":{"code":"agent_not_running"}}`) {
		t.Fatal("timeout detection is wrong")
	}
}
