package finite

import (
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const universalPromptCore = `FINITE CORE
You execute one finite provider turn. SQLite owns workflow state. Do not edit front matter, move task files, choose task status, or request attempt credits. Write only inside your assigned run directory or approved task workspace. Write the typed result to result_path before exit.`

const typedResultContract = `TYPED RESULT
Before you exit, write exactly one JSON object (at most 256 KiB, UTF-8) to the absolute result_path named in FINITE TURN. Copy task_id, run_id, and phase exactly from FINITE TURN. Unknown fields are invalid. Implementation outcomes: done, review, todo, wait, failed. Review outcomes: accepted, rejected, wait, failed. progress is required: what you did, in plain words. Put large evidence under the run_dir artifacts/ folder. Example:
{"schema_version": 1, "task_id": "<task_id>", "run_id": <run_id>, "phase": "<phase>", "outcome": "done", "progress": "What changed and how it was verified.", "summary": {"outcome": "one line", "evidence": "commands run and their results", "uncertainty": "what is not proven"}}
A wait adds "outcome": "wait" and a wait object: {"kind": "time", "wake_at": "<RFC 3339 UTC>", "reason": "..."} or {"kind": "joey", "reason": "...", "ask": {"question": "...", "options": [{"id": "approve", "label": "Approve"}, {"id": "deny", "label": "Deny"}]}}. A review that rejects adds "findings": ["..."].`

const runInboxContract = `RUN INBOX
The engine may deliver operator messages into inbox.jsonl inside your run directory while you work: check that file at the start of the turn and between each of your steps (absent or empty means nothing is waiting). Each line is one message {"id","kind","text"}: kind "message" is guidance to apply at the next step; kind "interrupt" is urgent and preempts your current plan. When you act on a message, quote its text (at least its first line) in the progress you report and in the result, and answer a question it asks inside this turn or by ending the turn as a Joey wait. Once you have handled a message (acted on it or explicitly declined it), append its id on its own line to inbox-ack.jsonl in the run directory. Never write inbox.jsonl yourself; the engine owns it.`

var promptPacks = map[string]string{
	"code":           "PACK: code\nMake the smallest maintainable code change and run focused failing-first verification.",
	"docs":           "PACK: docs\nVerify claims against the source and keep documentation concise and current.",
	"infrastructure": "PACK: infrastructure\nPrefer reversible changes, explicit fences, and evidence of live behavior.",
	"testing":        "PACK: testing\nExercise real boundaries with deterministic temporary state and report exact evidence.",
}

// BuildPrompt renders a full static bundle or a dynamic-only resume delta.
func BuildPrompt(req PromptRequest) (Prompt, error) {
	if req.TaskID == "" || req.RunID <= 0 || (req.Phase != PhaseImplementation && req.Phase != PhaseReview) {
		return Prompt{}, fmt.Errorf("finite: invalid prompt binding")
	}
	if req.RunDir != "" && !filepath.IsAbs(req.RunDir) {
		return Prompt{}, fmt.Errorf("finite: prompt run directory must be absolute")
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
	static := strings.Join(append([]string{universalPromptCore, role, typedResultContract, runInboxContract}, sections...), "\n\n")
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(static)))
	dynamic := "FINITE TURN\ntask_id: " + req.TaskID + "\nrun_id: " + strconv.FormatInt(req.RunID, 10) + "\nphase: " + string(req.Phase) + "\n"
	if req.RunDir != "" {
		dynamic += "run_dir: " + req.RunDir + "\nresult_path: " + filepath.Join(req.RunDir, "result.json") + "\n"
	}
	dynamic += "\n" + req.Dynamic
	full := req.PreviousBundleHash == "" || req.PreviousBundleHash != hash || req.ResetContext || req.UncertainContext
	text := dynamic
	if full {
		text = static + "\n\n" + dynamic
	}
	return Prompt{Text: text, BundleHash: hash, FullBundle: full}, nil
}
