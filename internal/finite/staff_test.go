package finite

import (
	"testing"

	"github.com/agent0ai/spynel/internal/harness"
)

// A brief may name the seat for each phase; the run uses that seat's staff
// entry, and a brief without one keeps the engine's configured default.
func TestBriefStaffSelectsTheHarnessSeatPerPhase(t *testing.T) {
	brief, err := ParseBrief([]byte("---\nid: t1\ntitle: x\nrisk: high\nreview_required: true\nstaff: finite-ops\nreview_staff: finite-thinker\n---\nbody\n"))
	if err != nil {
		t.Fatal(err)
	}
	base := harness.HarnessConfig{Name: "herdr", Model: "@finite"}
	if got := harnessForPhase(base, brief, PhaseImplementation); got.Model != "@finite-ops" || got.Name != "herdr" {
		t.Fatalf("implementation harness = %+v", got)
	}
	if got := harnessForPhase(base, brief, PhaseReview); got.Model != "@finite-thinker" {
		t.Fatalf("review harness = %+v", got)
	}
	plain, err := ParseBrief([]byte("---\nid: t2\ntitle: x\nrisk: low\nreview_required: false\n---\nbody\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got := harnessForPhase(base, plain, PhaseReview); got.Model != "@finite" {
		t.Fatalf("default harness = %+v", got)
	}
	if base.Model != "@finite" {
		t.Fatal("the engine's base harness was modified")
	}
	for _, bad := range []string{"staff: ../wk-code", "staff: Finite", "review_staff: \"a b\""} {
		if _, err := ParseBrief([]byte("---\nid: t3\ntitle: x\nrisk: low\nreview_required: false\n" + bad + "\n---\nbody\n")); err == nil {
			t.Errorf("brief with %q was accepted", bad)
		}
	}
}
