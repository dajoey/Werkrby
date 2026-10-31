package finite

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

const universalPromptCore = `FINITE CORE
You execute one finite provider turn. SQLite owns workflow state. Do not edit front matter, move task files, choose task status, or request attempt credits. Write only inside your assigned run directory or approved task workspace. Return one result.json before exit.`

const typedResultContract = `TYPED RESULT
Write at most 256 KiB of JSON with schema_version=1 and the exact task_id, run_id, and phase from FINITE TURN. Unknown fields are invalid. Implementation outcomes: done, review, todo, wait, failed. Review outcomes: accepted, rejected, wait, failed. Include bounded progress and evidence in summary; put large evidence under artifacts/. A wait includes a typed time or joey wait object.`

var promptPacks = map[string]string{
	"code":          "PACK: code\nMake the smallest maintainable code change and run focused failing-first verification.",
	"docs":          "PACK: docs\nVerify claims against the source and keep documentation concise and current.",
	"infrastructure": "PACK: infrastructure\nPrefer reversible changes, explicit fences, and evidence of live behavior.",
	"testing":       "PACK: testing\nExercise real boundaries with deterministic temporary state and report exact evidence.",
}

// BuildPrompt renders a full static bundle or a dynamic-only resume delta.
func BuildPrompt(req PromptRequest) (Prompt, error) {
	if req.TaskID == "" || req.RunID <= 0 || (req.Phase != PhaseImplementation && req.Phase != PhaseReview) {
		return Prompt{}, fmt.Errorf("finite: invalid prompt binding")
	}
	role := "IMPLEMENTATION\nComplete the single finite objective and return the implementation result contract."
	if req.Phase == PhaseReview {
		role = "REVIEW\nIndependently inspect and verify the implementation; do not inherit its claims without evidence."
	}
	domains := append([]string(nil), req.Domains...)
	sort.Strings(domains)
	var sections []string
	seen := ""
	for _, domain := range domains {
		if domain == seen {
			continue
		}
		seen = domain
		pack, ok := promptPacks[domain]
		if !ok {
			return Prompt{}, fmt.Errorf("finite: unknown prompt domain pack %q", domain)
		}
		sections = append(sections, pack)
	}
	static := strings.Join(append([]string{universalPromptCore, role, typedResultContract}, sections...), "\n\n")
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(static)))
	dynamic := "FINITE TURN\ntask_id: " + req.TaskID + "\nrun_id: " + strconv.FormatInt(req.RunID, 10) + "\nphase: " + string(req.Phase) + "\n\n" + req.Dynamic
	full := req.PreviousBundleHash == "" || req.PreviousBundleHash != hash || req.ResetContext || req.UncertainContext
	text := dynamic
	if full {
		text = static + "\n\n" + dynamic
	}
	return Prompt{Text: text, BundleHash: hash, FullBundle: full}, nil
}
