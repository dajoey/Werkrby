//go:build linux

package finite

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/harness"
)

const finiteScanCostEnv = "WERKRBY_FINITE_SCAN_COST"

func processCPUTime(t *testing.T) time.Duration {
	t.Helper()
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		t.Fatal(err)
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}

// TestFiniteScanCostReport measures owner-side process-boundary cost on the
// current host: one complete scan, then one steady open run with the owner
// that dispatched it and with an owner that adopted it after restart. It is a
// measurement, gated by WERKRBY_FINITE_SCAN_COST=1; the asserted budget is
// 5% of one core for the adopted steady state.
func TestFiniteScanCostReport(t *testing.T) {
	if os.Getenv(finiteScanCostEnv) == "" {
		t.Skip("set " + finiteScanCostEnv + "=1 to measure process-boundary scan cost")
	}
	entries, _ := os.ReadDir("/proc")
	token, err := NewProcessToken()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	const scans = 200
	cpuBefore, wallBefore := processCPUTime(t), time.Now()
	for index := 0; index < scans; index++ {
		if _, err := InspectRun(ctx, 55555, token); err != nil {
			t.Fatalf("scan: %v", err)
		}
	}
	cpu, wall := processCPUTime(t)-cpuBefore, time.Since(wallBefore)
	t.Logf("one InspectRun scan: cpu %v wall %v (%d /proc entries)", cpu/scans, wall/scans, len(entries))

	base := t.TempDir()
	root := filepath.Join(base, "finite state")
	worker := filepath.Join(base, "worker")
	if err := os.MkdirAll(worker, 0o700); err != nil {
		t.Fatal(err)
	}
	plan := engineFixturePlan{Runs: map[string]engineFixtureBehavior{"1": {Result: ResultDone, Block: true}}, InvocationsPath: filepath.Join(base, "invocations.jsonl")}
	planPath := filepath.Join(base, "plan.json")
	writeEngineJSONFile(t, planPath, plan)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	open := func() *Engine {
		engine, err := NewEngine(Config{Root: root, WorkerDir: worker}, EngineOptions{
			Harness: harness.HarnessConfig{Name: "finite-engine-fixture", Cwd: worker, Env: []string{engineFixturePlanEnv + "=" + planPath}},
			Seats: func(context.Context) ([]Seat, error) {
				return []Seat{{Name: "fake-seat", Available: true}}, nil
			},
			Executable:   executable,
			PollInterval: 250 * time.Millisecond,
			StopGrace:    25 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		return engine
	}
	first := open()
	task := intakeTestTask(t, first.store, "scan-cost", false)
	if err := first.ScanOnce(ctx); err != nil {
		t.Fatal(err)
	}
	waitEngineInvocationCount(t, first, 1)
	run, err := first.store.GetRun(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = StopRun(stopCtx, run.ID, run.ProcessToken, 0)
	})
	measure := func(name string, engine *Engine) float64 {
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = engine.Run(runCtx)
		}()
		time.Sleep(time.Second)
		cpuBefore, wallBefore := processCPUTime(t), time.Now()
		time.Sleep(5 * time.Second)
		cpu, wall := processCPUTime(t)-cpuBefore, time.Since(wallBefore)
		cancel()
		<-done
		share := 100 * cpu.Seconds() / wall.Seconds()
		t.Logf("%s: owner cpu %v over %v = %.2f%% of one core", name, cpu.Round(time.Millisecond), wall.Round(time.Millisecond), share)
		return share
	}
	measure("steady open run, dispatching owner", first)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second := open()
	defer second.Close()
	adopted := measure("steady open run, adopting owner", second)
	current, _ := second.store.GetTask(ctx, task.ID)
	if current.State != StateWorking {
		t.Fatalf("measured task left working: %+v", current)
	}
	if adopted >= 5 {
		t.Errorf("adopted steady open run costs %.2f%% of one core, budget 5%%", adopted)
	}
}

func BenchmarkFiniteInspectRun(b *testing.B) {
	token, err := NewProcessToken()
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	for b.Loop() {
		if _, err := InspectRun(ctx, 55555, token); err != nil {
			b.Fatal(err)
		}
	}
}
