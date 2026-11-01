package orchestrator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/workspace"
)

// moveThenBlockHarness plays a worker that settles its task (moves it out of working/) and then
// sits on a prompt nobody clears: the send returns sendErr only after release is closed, so a scan
// can reconcile the move while the send still runs.
type moveThenBlockHarness struct {
	cfg         config.Config
	toStatus    string
	sendErr     error
	workerCalls atomic.Int32
	notifyCalls atomic.Int32
	moved       chan struct{}
	release     chan struct{}
}

func (*moveThenBlockHarness) Start(context.Context) error                     { return nil }
func (*moveThenBlockHarness) Close() error                                    { return nil }
func (*moveThenBlockHarness) Interrupt(context.Context, string) (bool, error) { return false, nil }
func (*moveThenBlockHarness) ResetSession(string) error                       { return nil }
func (*moveThenBlockHarness) ThreadID(string) string                          { return "thread-x" }
func (*moveThenBlockHarness) IsActive(string) bool                            { return false }

func (h *moveThenBlockHarness) Send(_ context.Context, key, _ string, emit core.Emit) (string, bool, error) {
	if strings.HasPrefix(key, "orchestrator:notification:") {
		h.notifyCalls.Add(1)
		emit(core.Event{Kind: core.EventFinal, Text: "skipped", ThreadID: "thread-n", Done: true})
		return "thread-n", false, nil
	}
	if h.workerCalls.Add(1) > 1 {
		emit(core.Event{Kind: core.EventFinal, Text: "done", ThreadID: "thread-x", Done: true})
		return "thread-x", false, nil
	}
	working := h.cfg.StatePath("tasks", "working")
	entries, _ := os.ReadDir(working)
	for _, e := range entries {
		if e.Name() == "AGENTS.md" {
			continue
		}
		dst := h.cfg.StatePath("tasks", h.toStatus, e.Name())
		if err := moveDocumentWithProgress(filepath.Join(working, e.Name()), dst, h.toStatus, time.Now().UTC(), "worker settled the task"); err != nil {
			panic(err)
		}
	}
	close(h.moved)
	<-h.release
	return "", false, h.sendErr
}

// A worker that settles its task and then blocks on a prompt: reconcile runs the terminal
// transition (task.completed hooks, the notification agent) and removes the lease while the send
// still runs. The error that send returns afterwards must not write the lease back, or the next
// scan runs the same transition a second time. Both the hermes_blocked path and the ordinary
// error path.
func TestErrorAfterReconcileRemovedTheLeaseDoesNotRecreateIt(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("hook fixture is a shell script")
	}
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "hermes_blocked", err: hermesBlockedErr()},
		{name: "ordinary agent_blocked", err: errors.New(`herdr agent wk-t-a1 blocked: {"error":{"code":"agent_blocked"}}`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if err := workspace.Init(root, false); err != nil {
				t.Fatal(err)
			}
			cfg, err := config.Load(config.PathForRoot(root))
			if err != nil {
				t.Fatal(err)
			}
			cfg.Orchestrator.TaskNotifications = config.TaskNotificationsDecide
			extension := filepath.Join(cfg.Resolve(cfg.Extensions.Directory), "counter")
			if err := os.MkdirAll(extension, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(extension, extensions.ManifestName), []byte("name: counter\nhooks:\n  task.completed: [\"./hook.sh\"]\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(extension, "hook.sh"), []byte("#!/bin/sh\nprintf x >> completed.count\nprintf '%s\\n' '{}'\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			path, err := Create(cfg, "tasks", "move then block fixture", "")
			if err != nil {
				t.Fatal(err)
			}
			doc, err := ReadDocument(path)
			if err != nil {
				t.Fatal(err)
			}
			doc.FrontMatter["notify"] = map[string]any{"enabled": true, "origin": "tui/local", "on": []any{"cancelled"}}
			if err := WriteDocument(path, doc); err != nil {
				t.Fatal(err)
			}
			h := &moveThenBlockHarness{cfg: cfg, toStatus: "cancelled", sendErr: tc.err, moved: make(chan struct{}), release: make(chan struct{})}
			manager := New(cfg, h, extensions.Runner{Directory: cfg.Resolve(cfg.Extensions.Directory), Timeout: 5 * time.Second})
			var logMu sync.Mutex
			var logs []string
			manager.Log = func(s string) { logMu.Lock(); logs = append(logs, s); logMu.Unlock() }
			ctx := context.Background()

			if err := manager.ScanOnce(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case <-h.moved:
			case <-time.After(10 * time.Second):
				t.Fatal("worker never ran")
			}
			// A scan during the blocked wait reconciles the worker's move and removes the lease.
			if err := manager.ScanOnce(ctx); err != nil {
				t.Fatal(err)
			}
			if leases, _ := manager.loadLeases(); len(leases) != 0 {
				t.Fatalf("leases after the reconcile = %#v, want none", leases)
			}
			deadline := time.Now().Add(10 * time.Second)
			for h.notifyCalls.Load() < 1 && time.Now().Before(deadline) {
				time.Sleep(20 * time.Millisecond)
			}

			close(h.release)
			manager.Wait()
			if leases, _ := manager.loadLeases(); len(leases) != 0 {
				t.Fatalf("the send error re-created the lease reconcile removed: %#v", leases)
			}
			if err := manager.ScanOnce(ctx); err != nil {
				t.Fatal(err)
			}
			manager.Wait()
			count, _ := os.ReadFile(filepath.Join(extension, "completed.count"))
			if string(count) != "x" || h.notifyCalls.Load() != 1 || h.workerCalls.Load() != 1 {
				logMu.Lock()
				t.Logf("logs: %q", logs)
				logMu.Unlock()
				t.Fatalf("hooks = %q, notification turns = %d, worker turns = %d; want the transition run once and no further worker turn", count, h.notifyCalls.Load(), h.workerCalls.Load())
			}
		})
	}
}

// recordError on a lease whose file reconcile removed after the worker moved the document is
// handled without writing anything: no lease file, no failed/ move, no dispatch_error line.
func TestRecordErrorOnARemovedLeaseWritesNothing(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "hermes_blocked", err: hermesBlockedErr()},
		{name: "ordinary", err: errors.New("herdr prompt failed: exit status 1")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager, lease, doc, failed := blockedTaskFixture(t, 0)
			done := filepath.Join(filepath.Dir(filepath.Dir(doc)), "done", filepath.Base(doc))
			if err := moveDocumentWithProgress(doc, done, "done", time.Now().UTC(), "worker finished"); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(manager.leasePath(lease.ID)); err != nil {
				t.Fatal(err)
			}
			var kinds []string
			manager.LogError = func(source, kind, message string) { kinds = append(kinds, kind) }

			manager.recordError(context.Background(), lease, tc.err)

			if _, err := os.Stat(manager.leasePath(lease.ID)); !os.IsNotExist(err) {
				saved, _ := manager.loadLease(lease.ID)
				t.Fatalf("recordError re-created the removed lease: %+v", saved)
			}
			if fileExists(failed) || !fileExists(done) {
				t.Fatalf("recordError moved the settled document")
			}
			if len(kinds) != 0 {
				t.Fatalf("logged error kinds = %q, want none for an error on a settled lease", kinds)
			}
		})
	}
}
