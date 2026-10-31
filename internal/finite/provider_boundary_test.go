package finite

import (
	"context"
	"testing"
	"time"
)

func TestFiniteDetachedProvider_L3(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	spec := newFiniteFixtureSpec(t, "orphan")
	handle, err := StartSupervisor(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Wait(); err != nil {
		t.Fatalf("orphaning supervisor exit: %v", err)
	}
	// Wrapper exit is deliberately not provider-turn absence. The real orphan
	// retains the run markers and waits for a test-controlled release.
	observation, err := InspectRun(ctx, spec.RunID, spec.ProcessToken)
	if err != nil || observation.Liveness != LivenessLive || observation.Supervisor != nil || len(observation.Processes) == 0 {
		t.Fatalf("detached provider boundary = %#v, %v; want provider-only live observation", observation, err)
	}
	if !fileExists(spec.Harness.Env[2][len(fixtureStartedEnv)+1:]) {
		t.Fatal("detached fake provider did not execute")
	}
	state, err := StopRun(ctx, spec.RunID, spec.ProcessToken, 25*time.Millisecond)
	if err != nil || state != LivenessAbsent {
		t.Fatalf("detached run stop = %s, %v", state, err)
	}
	observation, err = InspectRun(ctx, spec.RunID, spec.ProcessToken)
	if err != nil || observation.Liveness != LivenessAbsent {
		t.Fatalf("detached provider survived confirmed stop: %#v, %v", observation, err)
	}
}
