package orchestrator

import (
	"path/filepath"
	"time"
)

type workflowRoute struct {
	Name           string
	Source         string
	Working        string
	Prompt         string
	RecoveryPrompt string
	ReviewPrompt   string
	StaleAfter     time.Duration
	AllowedNext    []string
}

// workflowRoutes defines the two fixed workspace workflows. It is never configuration.
func workflowRoutes() []workflowRoute {
	return []workflowRoute{
		{Name: "tasks", Source: ".werkrby/tasks/todo", Working: ".werkrby/tasks/working", Prompt: ".werkrby/prompts/task.md", RecoveryPrompt: ".werkrby/prompts/recovery.md", ReviewPrompt: ".werkrby/prompts/review.md", StaleAfter: 30 * time.Minute, AllowedNext: []string{"todo", "working", "review", "reviewing", "waiting", "done", "failed", "cancelled"}},
		{Name: "goals", Source: ".werkrby/goals/proposed", Working: ".werkrby/goals/planning", Prompt: ".werkrby/prompts/goal.md", RecoveryPrompt: ".werkrby/prompts/recovery.md", ReviewPrompt: ".werkrby/prompts/goal-review.md", StaleAfter: 2 * time.Hour, AllowedNext: []string{"proposed", "planning", "active", "review", "reviewing", "waiting", "done", "abandoned"}},
	}
}

// A lease parked in awaiting_transition is waiting only for the agent-authored
// durable file move to become observable, which reconcileTransitions sees on the
// next scan. route.StaleAfter is sized for a turn that is still running, so it is
// far too slow for a turn that already ended without moving anything: the fleet's
// external runner watchdog fails such a task forward after 10 minutes of stale
// heartbeat, which costs the task an attempt it never got a fair run at. Recover
// it in minutes instead, and bound the quick path -- a lease whose recovery turns
// have ended empty this many times is a broken runner rather than an interrupted
// one, and belongs to the ordinary stale path and its escalation.
const (
	awaitingTransitionStaleAfter      = 2 * time.Minute
	quickAwaitingTransitionRecoveries = 1
	// restartInterruptGrace is how long after a manager starts that a worker
	// SIGTERM/SIGKILL is still treated as the restart that created this
	// process, not as a dispatch failure of the task. Observed 2026-09-19:
	// three service restarts inside six minutes killed every in-flight
	// worker; the dying process cancels its context (covered separately),
	// and the new process can still see the same storm for about a minute
	// after it comes up.
	restartInterruptGrace = 2 * time.Minute
)

// workflowRoutesGoalsFirst is the claim-loop order: the goals route before the
// tasks route. A scan stops at the first full house (canAdmitClaim), and the
// tasks route is long; scanned first it kept every slot busy and a goal in
// proposed/ or review/ never got to plan or close (observed 2026-10-03, goal
// back in proposed/ behind 30+ queued tasks). Goals are few, so claiming them
// first costs standalone work at most one slot per goal phase.
func workflowRoutesGoalsFirst() []workflowRoute {
	routes := workflowRoutes()
	ordered := make([]workflowRoute, 0, len(routes))
	for _, route := range routes {
		if route.Name == "goals" {
			ordered = append(ordered, route)
		}
	}
	for _, route := range routes {
		if route.Name != "goals" {
			ordered = append(ordered, route)
		}
	}
	return ordered
}

func routeByName(name string) (workflowRoute, bool) {
	for _, route := range workflowRoutes() {
		if route.Name == name {
			return route, true
		}
	}
	return workflowRoute{}, false
}

// WorkflowDirectories lists only canonical live task and goal status folders.
func (m *Manager) WorkflowDirectories() []string {
	var paths []string
	for _, route := range workflowRoutes() {
		for _, status := range route.AllowedNext {
			paths = append(paths, filepath.Join(filepath.Dir(m.Config.Resolve(route.Source)), status))
		}
	}
	return paths
}
