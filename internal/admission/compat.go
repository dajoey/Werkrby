package admission

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/agent0ai/spynel/internal/roster"
)

// DefaultSnapshotPath is where the ai-usage collector writes its plan readings.
func DefaultSnapshotPath() string {
	home := os.Getenv("HOME")
	if home == "" {
		home = "/home/dajoey"
	}
	return filepath.Join(home, "ops", "state", "ai-usage", "snapshot.json")
}

// providerPlans maps the provider prefix of a pinned "--model provider/model" to the usage plan it
// spends, as the quota gate's PLAN_BY_PROVIDER does. This is the compat copy of a table that Q1 turns
// into policy data; nothing else in the Go tree should grow another one.
var providerPlans = map[string]string{
	"zai-coding-plan":       "zai",
	"zai":                   "zai",
	"kimi-for-coding":       "kimi_coding",
	"kimi":                  "kimi_coding",
	"kimi-code":             "kimi_coding",
	"antigravity":           "antigravity",
	"claude":                "claude",
	"zai-balance":           "zai_balance",
	"anthropic-api-credits": "anthropic_api_credits",
}

// PlanOf is the usage plan a roster seat spends, or "" when the seat is not on a metered plan (an
// unmetered seat is never a fallback target for a full plan). It follows the quota gate's
// runner_model: agy spends antigravity; a claude seat launched with a claude-api-credits settings
// file spends the API credit, any other claude seat the Claude plan; a pinned provider/model names
// its plan; an unpinned pi seat spends the Z.ai plan; anything else is unmetered.
func PlanOf(s roster.Staff) string {
	switch s.Runner {
	case "agy":
		return "antigravity"
	case "claude":
		if strings.Contains(filepath.Base(argValue(s.Args, "--settings")), "claude-api-credits") {
			return "anthropic_api_credits"
		}
		return "claude"
	}
	if pinned := argValue(s.Args, "--model"); strings.Contains(pinned, "/") {
		return providerPlans[pinned[:strings.IndexByte(pinned, '/')]]
	}
	if s.Runner == "pi" {
		return "zai"
	}
	return ""
}

func argValue(args []string, flag string) string {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == flag {
			return args[i+1]
		}
	}
	return ""
}

func claudePlan(plan string) bool {
	return plan == "claude" || plan == "anthropic_api_credits"
}
