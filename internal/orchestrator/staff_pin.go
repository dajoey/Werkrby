package orchestrator

import (
	"strconv"
	"strings"

	"github.com/agent0ai/spynel/internal/roster"
)

// A filer (a goal planner, a worker, the chat agent) routes a task by work type: it may stamp a
// `staff:` the routing table or a role names. It may not pin a model or a fallback rung (second,
// gemini, sonnet-credits, principal and so on); those are reached through the roster's chains and
// escalation, or by a person asking for them. honoredStaffPin returns the document's pin when it
// may stand and "" when the roster must route the task as if it had none.
//
// A pin stands when any of these holds:
//   - the seat is routable: the routing table or a role names it;
//   - `pinned_by` is set, which is how a person (or Claude Code on their word) pins a seat;
//   - the task came from a Helm thread (`helm_thread_id`), which the Helm pump files for Joey.
func honoredStaffPin(staffing *roster.Roster, frontMatter map[string]any, fileName string, logf func(string)) string {
	pinned := textField(frontMatter, "staff")
	if pinned == "" || staffing.Routable(pinned) {
		return pinned
	}
	if textField(frontMatter, "pinned_by") != "" || textField(frontMatter, "helm_thread_id") != "" {
		return pinned
	}
	if logf != nil {
		logf("roster: ignored staff pin " + strconv.Quote(pinned) + " in " + fileName +
			": it names a seat the routing table and roles do not, and nobody set `pinned_by`; routed by the roster instead")
	}
	return ""
}

func textField(frontMatter map[string]any, key string) string {
	value, _ := frontMatter[key].(string)
	return strings.TrimSpace(value)
}
