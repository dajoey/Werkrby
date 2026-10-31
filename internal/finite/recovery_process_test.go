package finite

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestFinitePIDReuse_L2(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	spec := newFiniteFixtureSpec(t, "block")
	handle, err := StartSupervisor(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	if handle.Identity.PID <= 0 || handle.Identity.Start == "" {
		t.Fatalf("supervisor lacks real OS process identity: %#v", handle.Identity)
	}
	observation, err := InspectRun(ctx, spec.RunID, spec.ProcessToken)
	if err != nil || observation.Supervisor == nil || !observation.Supervisor.Equal(handle.Identity) {
		t.Fatalf("exact supervisor discovery = %#v, %v; handle %#v", observation, err, handle.Identity)
	}
	if _, err := AdoptRun(ctx, handle.Identity); err != nil {
		t.Fatalf("exact identity was not adoptable: %v", err)
	}

	wrongStart := handle.Identity
	wrongStart.Start += "-stale"
	if state, err := StopSupervisor(ctx, wrongStart, 0); !errors.Is(err, ErrProcessIdentityMismatch) || state != LivenessUnknown {
		t.Fatalf("stale start stop = %s, %v; want unknown identity mismatch", state, err)
	}
	observation, err = InspectRun(ctx, spec.RunID, spec.ProcessToken)
	if err != nil || observation.Supervisor == nil || !observation.Supervisor.Equal(handle.Identity) {
		t.Fatalf("stale start token signaled the supervisor: %#v, %v", observation, err)
	}

	unrelated, _ := startFiniteSleeper(t)
	unrelatedIdentity, err := IdentifyProcess(unrelated.Process.Pid, 999, spec.ProcessToken)
	if err != nil {
		t.Fatal(err)
	}
	// This is the reused-PID condition with real processes: durable metadata
	// names the old run generation but its PID now belongs to an unrelated live
	// process. No signal may reach that process.
	reused := handle.Identity
	reused.PID = unrelated.Process.Pid
	if _, err := AdoptRun(ctx, reused); !errors.Is(err, ErrProcessIdentityMismatch) {
		t.Fatalf("reused PID adoption error = %v, want identity mismatch", err)
	}
	if state, err := StopSupervisor(ctx, reused, 0); !errors.Is(err, ErrProcessIdentityMismatch) || state != LivenessUnknown {
		t.Fatalf("reused PID stop = %s, %v; want unknown identity mismatch", state, err)
	}
	current, err := IdentifyProcess(unrelated.Process.Pid, 999, spec.ProcessToken)
	if err != nil || !current.Equal(unrelatedIdentity) {
		t.Fatalf("unrelated reused-PID process was signaled: before=%#v after=%#v err=%v", unrelatedIdentity, current, err)
	}

	state, err := StopRun(ctx, spec.RunID, spec.ProcessToken, 25*time.Millisecond)
	if err != nil || state != LivenessAbsent {
		t.Fatalf("exact run stop = %s, %v", state, err)
	}
	_ = handle.Wait()
	observation, err = InspectRun(ctx, spec.RunID, spec.ProcessToken)
	if err != nil || observation.Liveness != LivenessAbsent {
		t.Fatalf("post-stop observation = %#v, %v", observation, err)
	}
	current, err = IdentifyProcess(unrelated.Process.Pid, 999, spec.ProcessToken)
	if err != nil || !current.Equal(unrelatedIdentity) {
		t.Fatalf("run termination reached unrelated process: %#v, %v", current, err)
	}
}
