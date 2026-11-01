package finite

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/fsx"
	"github.com/agent0ai/spynel/internal/harness"
)

const (
	supervisorRequestVersion = 1
	maxSupervisorPrompt      = 1024 * 1024
	maxSupervisorRequest     = 1024 * 1024
	maxSupervisorDiagnostic  = 64 * 1024
)

// SupervisorSpec completely describes one isolated provider turn.
type SupervisorSpec struct {
	RunID        int64
	RunDir       string
	ProcessToken string
	Executable   string
	SessionKey   string
	Prompt       string
	Harness      harness.HarnessConfig
}

// SupervisorHandle is the owner's local handle to a re-executed supervisor.
type SupervisorHandle struct {
	Identity ProcessIdentity
	done     chan struct{}
	waitErr  error
}

type supervisorRequest struct {
	SchemaVersion  int                   `json:"schema_version"`
	RunID          int64                 `json:"run_id"`
	RunDir         string                `json:"run_dir"`
	ProcessToken   string                `json:"process_token"`
	SessionKey     string                `json:"session_key"`
	Harness        harness.HarnessConfig `json:"harness"`
	HarnessEnvKeys []string              `json:"harness_env_keys,omitempty"`
}

type supervisorStartRecord struct {
	SchemaVersion int             `json:"schema_version"`
	Identity      ProcessIdentity `json:"identity"`
	StartedAt     time.Time       `json:"started_at"`
}

type supervisorCompletion struct {
	SchemaVersion int        `json:"schema_version"`
	RunID         int64      `json:"run_id"`
	Outcome       RunOutcome `json:"outcome"`
	Error         string     `json:"error,omitempty"`
	CompletedAt   time.Time  `json:"completed_at"`
}

// StartSupervisor writes run-private protocol artifacts and re-executes the
// experimental binary. Unsupported platforms refuse before creating artifacts
// or a child. The child's lifetime is intentionally independent of ctx after a
// successful spawn; recovery and StopRun own it thereafter.
func StartSupervisor(ctx context.Context, spec SupervisorSpec) (*SupervisorHandle, error) {
	if !processBoundarySupported {
		return nil, ErrUnsupportedPlatform
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateSupervisorSpec(&spec); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(spec.RunDir, "artifacts"), 0o700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(spec.RunDir, "sessions"), 0o700); err != nil {
		return nil, err
	}
	request := supervisorRequest{
		SchemaVersion: supervisorRequestVersion,
		RunID:         spec.RunID,
		RunDir:        spec.RunDir,
		ProcessToken:  spec.ProcessToken,
		SessionKey:    spec.SessionKey,
		Harness:       spec.Harness,
	}
	request.Harness.Env = nil
	request.Harness.Stderr = nil
	request.Harness.SessionsFile = ""
	seenKeys := map[string]struct{}{}
	for _, variable := range spec.Harness.Env {
		equals := strings.IndexByte(variable, '=')
		if equals <= 0 {
			return nil, fmt.Errorf("harness environment entry must be KEY=value")
		}
		key := variable[:equals]
		if _, exists := seenKeys[key]; !exists {
			request.HarnessEnvKeys = append(request.HarnessEnvKeys, key)
			seenKeys[key] = struct{}{}
		}
	}
	requestData, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	if err := fsx.AtomicCreateFile(filepath.Join(spec.RunDir, "supervisor.json"), requestData, 0o600); err != nil {
		return nil, fmt.Errorf("publish supervisor request: %w", err)
	}
	if err := fsx.AtomicCreateFile(filepath.Join(spec.RunDir, "prompt.txt"), []byte(spec.Prompt), 0o600); err != nil {
		return nil, fmt.Errorf("publish supervisor prompt: %w", err)
	}
	logFile, err := os.OpenFile(filepath.Join(spec.RunDir, "supervisor.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	executable := spec.Executable
	if executable == "" {
		executable, err = os.Executable()
		if err != nil {
			_ = logFile.Close()
			return nil, err
		}
	}
	command := exec.Command(executable, "supervise", "--run-id", strconv.FormatInt(spec.RunID, 10), "--run-dir", spec.RunDir, "--process-token", spec.ProcessToken)
	command.Stdout = logFile
	command.Stderr = logFile
	command.Env = mergeEnvironment(os.Environ(), spec.Harness.Env, runEnvironment(spec.RunID, spec.ProcessToken, spec.RunDir))
	launch, err := newSupervisorLaunch(command, spec.ProcessToken)
	if err != nil {
		_ = logFile.Close()
		return nil, err
	}
	launchOpen := true
	defer func() {
		if launchOpen {
			_ = launch.Close()
		}
	}()
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		return nil, err
	}
	if err := launch.Started(command); err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		_ = logFile.Close()
		return nil, err
	}
	identity, err := IdentifyProcess(command.Process.Pid, spec.RunID, spec.ProcessToken)
	if err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		_ = logFile.Close()
		return nil, fmt.Errorf("identify started supervisor: %w", err)
	}
	handle := &SupervisorHandle{Identity: identity, done: make(chan struct{})}
	// Transfer the platform launch handle to the waiter. In particular, the
	// Windows owner keeps its named job handle alive until the child has exited;
	// the child's own containment handle covers owner-crash recovery.
	launchOpen = false
	go func() {
		// Wrapper exit is deliberately not provider absence: report completion
		// to the waiter as soon as the child exits, then drain the platform
		// launch handle. The Windows owner keeps its named job handle open
		// until the job reports no members, so a provider that outlived a
		// crashed supervisor stays contained and observable while recovery
		// works. Close errors are best effort and no longer join waitErr.
		handle.waitErr = command.Wait()
		close(handle.done)
		_ = launch.Close()
		_ = logFile.Close()
	}()
	return handle, nil
}

// Wait waits only for wrapper exit; provider-boundary absence requires
// InspectRun or AdoptedRun.Wait.
func (h *SupervisorHandle) Wait() error {
	<-h.done
	return h.waitErr
}

// RunSupervisor is the child-side file-only protocol. It never opens SQLite.
func RunSupervisor(ctx context.Context, args []string, factory harness.Factory) (runErr error) {
	binding, err := parseSupervisorArgs(args)
	if err != nil {
		return err
	}
	containment, err := openSupervisorContainment(binding.token)
	if err != nil {
		return err
	}
	defer containment.Close()
	request, err := readSupervisorRequest(filepath.Join(binding.runDir, "supervisor.json"))
	if err != nil {
		return err
	}
	if request.SchemaVersion != supervisorRequestVersion || request.RunID != binding.runID || request.ProcessToken != binding.token || filepath.Clean(request.RunDir) != filepath.Clean(binding.runDir) {
		return fmt.Errorf("supervisor request does not match immutable run binding")
	}
	prompt, err := readSupervisorBoundedFile(filepath.Join(binding.runDir, "prompt.txt"), maxSupervisorPrompt)
	if err != nil {
		return err
	}
	identity, err := IdentifyProcess(os.Getpid(), binding.runID, binding.token)
	if err != nil {
		return err
	}
	startData, _ := json.Marshal(supervisorStartRecord{SchemaVersion: supervisorRequestVersion, Identity: identity, StartedAt: time.Now().UTC()})
	if err := fsx.AtomicCreateFile(filepath.Join(binding.runDir, "supervisor-started.json"), startData, 0o600); err != nil {
		return fmt.Errorf("supervisor start fence: %w", err)
	}
	eventsFile, err := os.OpenFile(filepath.Join(binding.runDir, "events.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer eventsFile.Close()
	stderrFile, err := os.OpenFile(filepath.Join(binding.runDir, "provider-stderr.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer stderrFile.Close()

	runnerConfig := request.Harness
	runnerConfig.Env = nil
	for _, key := range request.HarnessEnvKeys {
		if value, exists := os.LookupEnv(key); exists {
			runnerConfig.Env = append(runnerConfig.Env, key+"="+value)
		}
	}
	runnerConfig.Env = mergeEnvironment(runnerConfig.Env, runEnvironment(binding.runID, binding.token, binding.runDir))
	runnerConfig.Stderr = &boundedSupervisorWriter{writer: stderrFile, remaining: maxSupervisorDiagnostic}
	sessionDigest := sha256.Sum256([]byte(request.SessionKey))
	runnerConfig.SessionsFile = filepath.Join(binding.runDir, "sessions", hex.EncodeToString(sessionDigest[:])+".json")
	if runnerConfig.Cwd == "" {
		runnerConfig.Cwd = binding.runDir
	}
	provider, err := factory(runnerConfig)
	if err != nil {
		return writeSupervisorFailure(binding.runDir, binding.runID, err)
	}

	var eventMu sync.Mutex
	var emitErr error
	encoder := json.NewEncoder(eventsFile)
	emit := func(event core.Event) {
		eventMu.Lock()
		defer eventMu.Unlock()
		if emitErr == nil {
			emitErr = encoder.Encode(event)
		}
	}
	threadID := ""
	steered := false
	if err := provider.Start(ctx); err != nil {
		runErr = err
	} else {
		threadID, steered, runErr = provider.Send(ctx, request.SessionKey, string(prompt), emit)
	}
	closeErr := provider.Close()
	runErr = errors.Join(runErr, closeErr, emitErr)
	providerData, _ := json.Marshal(map[string]any{
		"schema_version": supervisorRequestVersion,
		"run_id":         binding.runID,
		"thread_id":      threadID,
		"steered":        steered,
	})
	if err := fsx.AtomicWriteFile(filepath.Join(binding.runDir, "provider.json"), providerData, 0o600); err != nil {
		runErr = errors.Join(runErr, err)
	}
	outcome := RunOK
	switch {
	case errors.Is(runErr, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded):
		outcome = RunTimeout
	case errors.Is(runErr, context.Canceled) || errors.Is(ctx.Err(), context.Canceled):
		outcome = RunInterrupted
	case runErr != nil:
		outcome = RunFail
	}
	completionData, _ := json.Marshal(supervisorCompletion{
		SchemaVersion: supervisorRequestVersion,
		RunID:         binding.runID,
		Outcome:       outcome,
		Error:         boundedSupervisorError(runErr),
		CompletedAt:   time.Now().UTC(),
	})
	if err := fsx.AtomicWriteFile(filepath.Join(binding.runDir, "completion.json"), completionData, 0o600); err != nil {
		runErr = errors.Join(runErr, err)
	}
	if err := eventsFile.Sync(); err != nil {
		runErr = errors.Join(runErr, err)
	}
	return runErr
}

func validateSupervisorSpec(spec *SupervisorSpec) error {
	if err := validateRunBinding(spec.RunID, spec.ProcessToken); err != nil {
		return err
	}
	if spec.RunDir == "" || !filepath.IsAbs(spec.RunDir) {
		return fmt.Errorf("supervisor run directory must be absolute")
	}
	spec.RunDir = filepath.Clean(spec.RunDir)
	if spec.SessionKey == "" {
		return fmt.Errorf("supervisor session key is empty")
	}
	if len(spec.Prompt) == 0 || len(spec.Prompt) > maxSupervisorPrompt || !utf8.ValidString(spec.Prompt) {
		return fmt.Errorf("supervisor prompt must be valid UTF-8 between 1 and %d bytes", maxSupervisorPrompt)
	}
	if spec.Harness.Name == "" {
		return fmt.Errorf("supervisor harness name is empty")
	}
	return nil
}

func readSupervisorRequest(path string) (supervisorRequest, error) {
	var request supervisorRequest
	file, err := os.Open(path)
	if err != nil {
		return request, err
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, maxSupervisorRequest+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return request, err
	}
	if err := ensureJSONEnd(decoder); err != nil {
		return request, err
	}
	return request, nil
}

func ensureJSONEnd(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("supervisor request has trailing data")
	}
	return nil
}

func readSupervisorBoundedFile(path string, limit int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", path)
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("%s exceeds %d bytes", path, limit)
	}
	return data, nil
}

func writeSupervisorFailure(runDir string, runID int64, cause error) error {
	data, _ := json.Marshal(supervisorCompletion{SchemaVersion: supervisorRequestVersion, RunID: runID, Outcome: RunFail, Error: boundedSupervisorError(cause), CompletedAt: time.Now().UTC()})
	return errors.Join(cause, fsx.AtomicWriteFile(filepath.Join(runDir, "completion.json"), data, 0o600))
}

func boundedSupervisorError(err error) string {
	if err == nil {
		return ""
	}
	text := err.Error()
	if len(text) <= 4096 {
		return text
	}
	return string([]rune(text)[:min(1024, len([]rune(text)))])
}

func mergeEnvironment(groups ...[]string) []string {
	values := map[string]string{}
	order := []string{}
	for _, group := range groups {
		for _, variable := range group {
			equals := strings.IndexByte(variable, '=')
			if equals <= 0 {
				continue
			}
			key := variable[:equals]
			if _, exists := values[key]; !exists {
				order = append(order, key)
			}
			values[key] = variable[equals+1:]
		}
	}
	merged := make([]string, 0, len(order))
	for _, key := range order {
		merged = append(merged, key+"="+values[key])
	}
	return merged
}

type boundedSupervisorWriter struct {
	writer    io.Writer
	remaining int
}

func (w *boundedSupervisorWriter) Write(data []byte) (int, error) {
	original := len(data)
	if w.remaining <= 0 {
		return original, nil
	}
	if len(data) > w.remaining {
		data = data[:w.remaining]
	}
	written, err := w.writer.Write(data)
	w.remaining -= written
	return original, err
}
