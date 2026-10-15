package orchestrator

import (
	"strings"
	"testing"

	"github.com/agent0ai/spynel/internal/roster"
)

// Werkrby routes a task by work type. A filer (goal planner, worker, chat agent) may stamp a seat the
// routing table or a role names; it may not pin a model or fallback rung on its own. A person pins
// with pinned_by, and tasks the Helm pump files for Joey carry helm_thread_id.

func pinTestRoster() *roster.Roster {
	return &roster.Roster{
		Staff: map[string]roster.Staff{
			"implementer": {}, "ops": {}, "architect": {}, "second": {}, "gemini": {}, "principal": {},
		},
		Roles:   map[string]string{"task_implementation": "implementer"},
		Routing: []roster.RouteEntry{{Domain: "fleet ops", Staff: "ops"}, {Domain: "root cause", Staff: "architect"}},
	}
}

func TestStaffPinsFromFilersStandOnlyForRoutableSeats(t *testing.T) {
	cases := []struct {
		name  string
		front map[string]any
		want  string
		logs  bool
	}{
		{"routing-table seat", map[string]any{"staff": "ops"}, "ops", false},
		{"role seat", map[string]any{"staff": "implementer"}, "implementer", false},
		{"no pin", map[string]any{}, "", false},
		{"model seat from a filer", map[string]any{"staff": "second"}, "", true},
		{"fallback rung from a filer", map[string]any{"staff": "gemini"}, "", true},
		{"principal from a filer", map[string]any{"staff": "principal"}, "", true},
		{"model seat a person pinned", map[string]any{"staff": "second", "pinned_by": "joey"}, "second", false},
		{"model seat on a Helm task", map[string]any{"staff": "second", "helm_thread_id": "joey-20261015-x"}, "second", false},
		{"blank attribution is none", map[string]any{"staff": "second", "pinned_by": "  "}, "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var logged []string
			got := honoredStaffPin(pinTestRoster(), tc.front, "tasks-x.md", func(line string) { logged = append(logged, line) })
			if got != tc.want {
				t.Fatalf("pin = %q, want %q", got, tc.want)
			}
			if tc.logs != (len(logged) == 1 && strings.Contains(logged[0], "ignored staff pin")) {
				t.Fatalf("log lines = %v, want a single ignored-pin line: %v", logged, tc.logs)
			}
		})
	}
}

// The decision reaches the seat the task runs on: an ignored pin routes the task by the role default.
func TestAnIgnoredStaffPinRoutesByTheRoleDefault(t *testing.T) {
	staffing := pinTestRoster()
	seat, ok := rosterSeat(staffing, t.TempDir(), phaseTaskImplementation, map[string]any{"staff": "second", "attempt": 0}, "tasks-x.md", nil)
	if !ok || seat != "implementer" {
		t.Fatalf("seat = %q (%v), want the role default implementer", seat, ok)
	}
	seat, ok = rosterSeat(staffing, t.TempDir(), phaseTaskImplementation, map[string]any{"staff": "second", "pinned_by": "joey", "attempt": 0}, "tasks-x.md", nil)
	if !ok || seat != "second" {
		t.Fatalf("seat = %q (%v), want the pinned second", seat, ok)
	}
}
