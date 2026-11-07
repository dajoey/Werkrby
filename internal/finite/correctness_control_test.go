package finite

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var _ ControlWorkflow = (*Engine)(nil)

type correctnessWorkflow struct {
	store   *Store
	stops   atomic.Int64
	answers atomic.Int64
}

func (w *correctnessWorkflow) RequestStop(ctx context.Context, taskID string, version int64) (Task, error) {
	w.stops.Add(1)
	return w.store.RequestStop(ctx, taskID, version)
}

func (w *correctnessWorkflow) AnswerJoey(ctx context.Context, taskID string, version int64, optionID, note string) (Task, error) {
	w.answers.Add(1)
	return w.store.AnswerJoey(ctx, taskID, version, optionID, note)
}

type correctnessControlServer struct {
	store    *Store
	client   ControlClient
	workflow *correctnessWorkflow
}

func newCorrectnessControlServer(t *testing.T) *correctnessControlServer {
	t.Helper()
	root := t.TempDir()
	owner, err := AcquireWorkspaceLock(root)
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(Config{Root: root, WorkerDir: t.TempDir()})
	if err != nil {
		_ = owner.Close()
		t.Fatal(err)
	}
	workflow := &correctnessWorkflow{store: store}
	token := strings.Repeat("correctness-control-token-", 2)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = store.Close()
		_ = owner.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- ServeControlWithWorkflow(ctx, listener, store, owner, token, workflow) }()
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
		select {
		case <-serveErr:
		case <-time.After(2 * time.Second):
			t.Errorf("workflow control server did not stop")
		}
		_ = store.Close()
		_ = owner.Close()
	})
	return &correctnessControlServer{
		store: store, workflow: workflow,
		client: ControlClient{Endpoint: "http://" + listener.Addr().String(), Token: token, Client: &http.Client{Timeout: 2 * time.Second}},
	}
}

func TestFiniteCorrectnessControlUsesWorkflow(t *testing.T) {
	t.Run("stop", func(t *testing.T) {
		server := newCorrectnessControlServer(t)
		task := intakeTestTask(t, server.store, "correctness-control-stop", false)
		stopped, err := server.client.Stop(context.Background(), task.ID, task.Version)
		if err != nil {
			t.Fatal(err)
		}
		if stopped.State != StateCancelled || !stopped.CancelRequested {
			t.Fatalf("workflow stop status = %+v", stopped)
		}
		if got := server.workflow.stops.Load(); got != 1 {
			t.Fatalf("workflow stop calls = %d, want one", got)
		}
		if got := server.workflow.answers.Load(); got != 0 {
			t.Fatalf("stop unexpectedly called answer workflow %d times", got)
		}
	})

	t.Run("answer", func(t *testing.T) {
		server := newCorrectnessControlServer(t)
		task := intakeTestTask(t, server.store, "correctness-control-answer", false)
		run := dispatchTestRun(t, server.store, task, PhaseImplementation)
		result := validTestResult(run, ResultWait)
		result.Wait = &Wait{Kind: WaitJoey, Reason: "workflow answer", Ask: json.RawMessage(`{"question":"Continue?","options":[{"id":"yes","label":"Yes"}]}`)}
		waiting, err := server.store.SettleRun(context.Background(), SettlementRequest{
			RunID: run.ID, ExpectedTaskVersion: task.Version + 1, ExecutionOutcome: RunOK, Result: result,
		})
		if err != nil {
			t.Fatal(err)
		}
		answered, err := server.client.Answer(context.Background(), task.ID, waiting.Version, "yes", "")
		if err != nil {
			t.Fatal(err)
		}
		if answered.State != StateTodo {
			t.Fatalf("workflow answer status = %+v", answered)
		}
		if got := server.workflow.answers.Load(); got != 1 {
			t.Fatalf("workflow answer calls = %d, want one", got)
		}
		if got := server.workflow.stops.Load(); got != 0 {
			t.Fatalf("answer unexpectedly called stop workflow %d times", got)
		}
	})
}
