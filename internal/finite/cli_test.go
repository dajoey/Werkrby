package finite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/harness"
)

func TestFiniteCLICommandValidation(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "missing command", want: "command required"},
		{name: "unknown command", args: []string{"unknown"}, want: `unknown command "unknown"`},
		{name: "serve missing root and worker", args: []string{"serve"}, want: "--root and --worker are required"},
		{name: "serve missing worker", args: []string{"serve", "--root", "state"}, want: "--root and --worker are required"},
		{name: "serve unknown flag", args: []string{"serve", "--unknown"}, want: "flag provided but not defined"},
		{name: "serve conflicting seat sources", args: []string{"serve", "--root", "state", "--worker", "work", "--seats", "one", "--roster", "seats.yaml"}, want: "cannot be used together"},
		{name: "serve invalid poll interval", args: []string{"serve", "--root", "state", "--worker", "work", "--poll-interval", "0s"}, want: "--poll-interval must be positive"},
		{name: "intake missing brief", args: []string{"intake", "--root", "state"}, want: "--brief is required"},
		{name: "status missing task", args: []string{"status", "--root", "state"}, want: "--task is required"},
		{name: "status rejects positional arguments", args: []string{"status", "--root", "state", "--task", "one", "extra"}, want: "no positional arguments"},
		{name: "stop missing expected version", args: []string{"stop", "--root", "state", "--task", "one"}, want: "--expected-version is required"},
		{name: "answer missing option", args: []string{"answer", "--root", "state", "--task", "one", "--expected-version", "2"}, want: "--option is required"},
		{name: "receiver missing root", args: []string{"test-receiver"}, want: "--root is required"},
		{name: "receiver unknown fault", args: []string{"test-receiver", "--root", "receiver", "--fault", "unknown"}, want: "unknown receiver fault"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := executeWithDeps(context.Background(), test.args, "test", defaultCLIDependencies())
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Execute(%q) error = %v, want substring %q", test.args, err, test.want)
			}
		})
	}
}

func TestFiniteCLIServeDelegation(t *testing.T) {
	var captured serveOptions
	called := false
	deps := defaultCLIDependencies()
	deps.serve = func(ctx context.Context, options serveOptions) error {
		called = true
		captured = options
		if _, ok := ctx.Deadline(); ok {
			t.Error("serve must retain its long-running context without a client timeout")
		}
		return nil
	}
	args := []string{
		"serve", "--root", "state root", "--worker", "worker target",
		"--seats", "alpha,beta", "--harness", "finite-engine-fixture", "--model", "fake-model",
		"--control-listen", "127.0.0.1:12345", "--receiver-config", "receiver.json",
		"--domains", "code,testing", "--poll-interval", "25ms", "--stop-grace", "2s",
	}
	if err := executeWithDeps(context.Background(), args, "test", deps); err != nil {
		t.Fatalf("serve delegation: %v", err)
	}
	if !called {
		t.Fatal("serve command did not invoke its injected runtime")
	}
	want := serveOptions{
		Root: "state root", Worker: "worker target", Seats: []string{"alpha", "beta"},
		Harness:     harness.HarnessConfig{Name: "finite-engine-fixture", Model: "fake-model"},
		ControlAddr: "127.0.0.1:12345", ReceiverConfig: "receiver.json", Domains: []string{"code", "testing"},
		PollInterval: 25 * time.Millisecond, StopGrace: 2 * time.Second,
	}
	if !reflect.DeepEqual(captured, want) {
		t.Fatalf("serve options = %#v, want %#v", captured, want)
	}
}

type fakeCLIControlClient struct {
	calls  []string
	status ControlStatus
	err    error
}

func (c *fakeCLIControlClient) record(ctx context.Context, call string) (ControlStatus, error) {
	if _, ok := ctx.Deadline(); !ok {
		return ControlStatus{}, fmt.Errorf("control call %s has no bounded deadline", call)
	}
	c.calls = append(c.calls, call)
	return c.status, c.err
}

func (c *fakeCLIControlClient) Intake(ctx context.Context, path string) (ControlStatus, error) {
	return c.record(ctx, "intake:"+path)
}
func (c *fakeCLIControlClient) Status(ctx context.Context, id string) (ControlStatus, error) {
	return c.record(ctx, "status:"+id)
}
func (c *fakeCLIControlClient) Stop(ctx context.Context, id string, version int64) (ControlStatus, error) {
	return c.record(ctx, fmt.Sprintf("stop:%s:%d", id, version))
}
func (c *fakeCLIControlClient) Answer(ctx context.Context, id string, version int64, option string) (ControlStatus, error) {
	return c.record(ctx, fmt.Sprintf("answer:%s:%d:%s", id, version, option))
}

func TestFiniteCLIControlClientBehavior(t *testing.T) {
	tests := []struct {
		name string
		args []string
		call string
	}{
		{name: "intake", args: []string{"intake", "--root", "state", "--brief", "brief.md"}, call: "intake:brief.md"},
		{name: "status", args: []string{"status", "--root", "state", "--task", "task-one"}, call: "status:task-one"},
		{name: "stop", args: []string{"stop", "--root", "state", "--task", "task-one", "--expected-version", "7"}, call: "stop:task-one:7"},
		{name: "answer", args: []string{"answer", "--root", "state", "--task", "task-one", "--expected-version", "8", "--option", "approve"}, call: "answer:task-one:8:approve"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeCLIControlClient{status: ControlStatus{ID: "task-one", State: StateWaiting, Version: 8}}
			var output bytes.Buffer
			deps := defaultCLIDependencies()
			deps.stdout = &output
			deps.controlClient = func(root string) (cliControlClient, error) {
				if root != "state" {
					t.Fatalf("control root = %q, want state", root)
				}
				return client, nil
			}
			if err := executeWithDeps(context.Background(), test.args, "test", deps); err != nil {
				t.Fatalf("control command: %v", err)
			}
			if !reflect.DeepEqual(client.calls, []string{test.call}) {
				t.Fatalf("control calls = %q, want %q", client.calls, test.call)
			}
			var status ControlStatus
			if err := json.Unmarshal(output.Bytes(), &status); err != nil {
				t.Fatalf("control output is not bounded status JSON: %v; output=%q", err, output.String())
			}
			if status.ID != "task-one" || status.State != StateWaiting || status.Version != 8 {
				t.Fatalf("control output = %+v", status)
			}
		})
	}
}

func TestFiniteCLISuperviseDelegation(t *testing.T) {
	wantErr := errors.New("supervise sentinel")
	deps := defaultCLIDependencies()
	called := false
	deps.runSupervisor = func(ctx context.Context, args []string, factory harness.Factory) error {
		called = true
		if !reflect.DeepEqual(args, []string{"--run-id", "42", "--run-dir", "run dir", "--process-token", "token"}) {
			t.Fatalf("supervisor args = %q", args)
		}
		if factory == nil {
			t.Fatal("supervisor did not receive the NewRunner factory")
		}
		return wantErr
	}
	err := executeWithDeps(context.Background(), []string{"supervise", "--run-id", "42", "--run-dir", "run dir", "--process-token", "token"}, "test", deps)
	if !errors.Is(err, wantErr) || !called {
		t.Fatalf("supervise error = %v, called=%t", err, called)
	}
}

func TestFiniteCLIServePublishesAuthenticatedControlAndStops(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "state")
	worker := filepath.Join(base, "worker")
	if err := os.MkdirAll(worker, 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deps := defaultCLIDependencies()
	done := make(chan error, 1)
	go func() {
		done <- executeWithDeps(ctx, []string{"serve", "--root", root, "--worker", worker, "--harness", "finite-engine-fixture", "--poll-interval", "10ms"}, "test", deps)
	}()
	controlPath := filepath.Join(root, "control.json")
	waitForCLIProcessFile(t, done, controlPath, "serve control readiness")
	rendezvous := readCLIRendezvousForTest(t, controlPath)
	client := ControlClient{Endpoint: rendezvous.Endpoint, Token: rendezvous.Token, Client: &httpClientForTest}
	requestCtx, requestCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer requestCancel()
	_, err := client.Status(requestCtx, "missing-task")
	var controlErr *ControlError
	if !errors.As(err, &controlErr) || controlErr.StatusCode != 404 {
		t.Fatalf("authenticated owner status error = %v, want bounded 404", err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve shutdown: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("serve did not stop after context cancellation")
	}
	if fileExists(controlPath) {
		t.Fatal("serve left its private control rendezvous after shutdown")
	}
}

func TestFiniteCLIReceiverDurableDelivery(t *testing.T) {
	root := filepath.Join(t.TempDir(), "receiver state")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deps := defaultCLIDependencies()
	var output bytes.Buffer
	deps.stdout = &output
	done := make(chan error, 1)
	go func() {
		done <- executeWithDeps(ctx, []string{"test-receiver", "--root", root}, "test", deps)
	}()
	rendezvousPath := filepath.Join(root, "receiver.json")
	waitForCLIProcessFile(t, done, rendezvousPath, "receiver readiness")
	rendezvous := readCLIRendezvousForTest(t, rendezvousPath)
	if strings.Contains(output.String(), rendezvous.Token) {
		t.Fatal("receiver command exposed its bearer token")
	}
	transport := HTTPDeliveryTransport{Endpoint: rendezvous.Endpoint, Token: rendezvous.Token, Client: &http.Client{Timeout: 15 * time.Second}}
	delivery := Delivery{ID: "cli-receiver-one", Payload: []byte(`{"task_id":"cli-receiver","state":"done"}`)}
	requestCtx, requestCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer requestCancel()
	if err := transport.Deliver(requestCtx, delivery); err != nil {
		t.Fatalf("deliver to CLI receiver: %v", err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("receiver shutdown: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("receiver did not stop after context cancellation")
	}
	deliveries, err := ReadReceiverDeliveries(root)
	if err != nil || len(deliveries) != 1 || deliveries[0].DeliveryID != delivery.ID || !bytes.Equal(deliveries[0].Payload, delivery.Payload) {
		t.Fatalf("durable receiver effects = %+v, %v", deliveries, err)
	}
}

const cliHelperConfigEnv = "WERKRBY_FINITE_CLI_HELPER_CONFIG"

type cliHelperConfig struct {
	Args []string `json:"args"`
}

func TestFiniteCLIProcessHelper(t *testing.T) {
	path := os.Getenv(cliHelperConfigEnv)
	if path == "" {
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "read CLI helper config:", err)
		os.Exit(2)
	}
	var config cliHelperConfig
	if err := json.Unmarshal(data, &config); err != nil {
		fmt.Fprintln(os.Stderr, "decode CLI helper config:", err)
		os.Exit(2)
	}
	if err := Execute(config.Args, "test"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

func TestFiniteCLICompiledFakeProviderWorkflow(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "finite state 世界")
	worker := filepath.Join(base, "worker target λ")
	receiverRoot := filepath.Join(base, "receiver state 世界")
	if err := os.MkdirAll(worker, 0o700); err != nil {
		t.Fatal(err)
	}
	plan := engineFixturePlan{Runs: map[string]engineFixtureBehavior{
		"1": {Result: ResultDone},
		"2": {Result: ResultAccepted},
	}, InvocationsPath: filepath.Join(base, "provider invocations.jsonl")}
	planPath := filepath.Join(base, "provider plan.json")
	writeEngineJSONFile(t, planPath, plan)
	briefPath := filepath.Join(base, "compiled workflow.md")
	brief := "---\nid: compiled-cli\ntitle: Compiled CLI workflow\nrisk: low\nreview_required: true\n---\n\nExercise the compiled CLI with the existing fake provider.\n"
	if err := os.WriteFile(briefPath, []byte(brief), 0o600); err != nil {
		t.Fatal(err)
	}
	rosterPath := filepath.Join(base, "finite seats.yaml")
	roster := "seats:\n  - name: fake-seat\n    available: true\n"
	if err := os.WriteFile(rosterPath, []byte(roster), 0o600); err != nil {
		t.Fatal(err)
	}

	_, receiverOutput, receiverDone := startCLIHelperProcess(t, []string{"test-receiver", "--root", receiverRoot})
	receiverPath := filepath.Join(receiverRoot, "receiver.json")
	waitForCLIProcessFile(t, receiverDone, receiverPath, "compiled receiver readiness")
	_, ownerOutput, ownerDone := startCLIHelperProcess(t, []string{
		"serve", "--root", root, "--worker", worker, "--harness", "finite-engine-fixture",
		"--roster", rosterPath, "--receiver-config", receiverPath,
		"--poll-interval", "5ms", "--stop-grace", "25ms",
	}, engineFixturePlanEnv+"="+planPath)
	controlPath := filepath.Join(root, "control.json")
	waitForCLIProcessFile(t, ownerDone, controlPath, "compiled serve readiness")

	var clientOutput strings.Builder
	output, err := runCLIHelperProcess(t, []string{"intake", "--root", root, "--brief", briefPath}, engineFixturePlanEnv+"="+planPath)
	clientOutput.Write(output)
	if err != nil {
		t.Fatalf("compiled intake: %v; output=%s", err, output)
	}
	deadline := time.Now().Add(60 * time.Second)
	var status ControlStatus
	for time.Now().Before(deadline) {
		output, err = runCLIHelperProcess(t, []string{"status", "--root", root, "--task", "compiled-cli"}, engineFixturePlanEnv+"="+planPath)
		clientOutput.Write(output)
		if err != nil {
			t.Fatalf("compiled status: %v; output=%s", err, output)
		}
		if err := json.Unmarshal(output, &status); err != nil {
			t.Fatalf("compiled status output = %q, %v", output, err)
		}
		if status.State == StateDone {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if status.State != StateDone || status.Version != 4 {
		t.Fatalf("compiled workflow status = %+v, owner output=%s", status, ownerOutput.String())
	}
	data, err := os.ReadFile(plan.InvocationsPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("compiled fake provider invocations = %d, want implementation and review", len(lines))
	}
	var deliveries []ReceivedDelivery
	waitForFixture(t, func() bool {
		deliveries, err = ReadReceiverDeliveries(receiverRoot)
		return err == nil && len(deliveries) == 1
	}, "compiled workflow outbox delivery")
	if deliveries[0].DeliveryID != "task:compiled-cli:v4:default" || !bytes.Contains(deliveries[0].Payload, []byte(`"state":"done"`)) {
		t.Fatalf("compiled workflow delivery = %+v", deliveries[0])
	}
	control := readCLIRendezvousForTest(t, controlPath)
	receiver := readCLIRendezvousForTest(t, receiverPath)
	allOutput := ownerOutput.String() + receiverOutput.String() + clientOutput.String()
	if strings.Contains(allOutput, control.Token) || strings.Contains(allOutput, receiver.Token) {
		t.Fatal("compiled CLI exposed a control or receiver bearer token")
	}
}

var httpClientForTest = http.Client{Timeout: 15 * time.Second}

type cliRendezvousForTest struct {
	SchemaVersion int    `json:"schema_version"`
	Endpoint      string `json:"endpoint"`
	Token         string `json:"token"`
}

func readCLIRendezvousForTest(t *testing.T, path string) cliRendezvousForTest {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var rendezvous cliRendezvousForTest
	if err := json.Unmarshal(data, &rendezvous); err != nil {
		t.Fatal(err)
	}
	if rendezvous.SchemaVersion != 1 || rendezvous.Endpoint == "" || rendezvous.Token == "" {
		t.Fatalf("invalid CLI rendezvous: %+v", rendezvous)
	}
	return rendezvous
}

func waitForCLIProcessFile(t *testing.T, done <-chan error, path, description string) {
	t.Helper()
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	for {
		if fileExists(path) {
			return
		}
		select {
		case err := <-done:
			t.Fatalf("%s: command exited before readiness: %v", description, err)
		case <-timer.C:
			t.Fatalf("timed out waiting for %s", description)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

type cliLockedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *cliLockedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(data)
}

func (b *cliLockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func startCLIHelperProcess(t *testing.T, args []string, extraEnv ...string) (*exec.Cmd, *cliLockedBuffer, <-chan error) {
	t.Helper()
	configPath := filepath.Join(t.TempDir(), "cli helper.json")
	writeEngineJSONFile(t, configPath, cliHelperConfig{Args: args})
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run=^TestFiniteCLIProcessHelper$")
	command.Env = append(os.Environ(), cliHelperConfigEnv+"="+configPath)
	command.Env = append(command.Env, extraEnv...)
	output := &cliLockedBuffer{}
	command.Stdout = output
	command.Stderr = output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	t.Cleanup(func() {
		if command.Process != nil {
			_ = command.Process.Kill()
		}
		select {
		case <-done:
		case <-time.After(15 * time.Second):
		}
	})
	return command, output, done
}

func runCLIHelperProcess(t *testing.T, args []string, extraEnv ...string) ([]byte, error) {
	t.Helper()
	configPath := filepath.Join(t.TempDir(), "cli command.json")
	writeEngineJSONFile(t, configPath, cliHelperConfig{Args: args})
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, "-test.run=^TestFiniteCLIProcessHelper$")
	command.Env = append(os.Environ(), cliHelperConfigEnv+"="+configPath)
	command.Env = append(command.Env, extraEnv...)
	return command.CombinedOutput()
}

func TestFiniteCLIControlClientRendezvousValidation(t *testing.T) {
	root := t.TempDir()
	token := strings.Repeat("s", 43)
	path := filepath.Join(root, "control.json")
	if err := os.WriteFile(path, []byte(`{"schema_version":1,"endpoint":"http://192.0.2.1:1234","token":"`+token+`"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := newCLIControlClient(root)
	if err == nil || strings.Contains(err.Error(), token) || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("invalid rendezvous error = %v", err)
	}
}

func TestFiniteCLIServeRejectsNonLoopbackReceiver(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "state")
	worker := filepath.Join(base, "worker")
	if err := os.MkdirAll(worker, 0o700); err != nil {
		t.Fatal(err)
	}
	receiverPath := filepath.Join(base, "receiver.json")
	writeEngineJSONFile(t, receiverPath, cliRendezvousForTest{SchemaVersion: 1, Endpoint: "http://192.0.2.1:1234", Token: strings.Repeat("r", 43)})
	err := runFiniteServe(context.Background(), serveOptions{Root: root, Worker: worker, Seats: []string{"default"}, Harness: harness.HarnessConfig{Name: "finite-engine-fixture"}, ReceiverConfig: receiverPath})
	if err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("serve receiver validation error = %v", err)
	}
}

func TestFiniteCLIListenerContract(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := validateLoopbackListener(listener); err != nil {
		t.Fatal(err)
	}
}

func TestFiniteServeControlUsesEngineWorkflow(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "state")
	worker := filepath.Join(base, "worker")
	if err := os.MkdirAll(worker, 0o700); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("control workflow sentinel")
	deps := defaultServeRuntimeDependencies()
	called := make(chan ControlWorkflow, 1)
	deps.serveControl = func(_ context.Context, _ net.Listener, _ *Store, _ *WorkspaceLock, _ string, workflow ControlWorkflow) error {
		called <- workflow
		return sentinel
	}
	err := runFiniteServeWithDeps(context.Background(), serveOptions{
		Root: root, Worker: worker, Seats: []string{"default"}, Harness: harness.HarnessConfig{Name: "finite-engine-fixture"},
	}, deps)
	if !errors.Is(err, sentinel) {
		t.Fatalf("serve error = %v, want control sentinel", err)
	}
	workflow := <-called
	engine, ok := workflow.(*Engine)
	if !ok || engine == nil {
		t.Fatalf("control workflow = %T, want the owner Engine serialized through scanMu", workflow)
	}
}
