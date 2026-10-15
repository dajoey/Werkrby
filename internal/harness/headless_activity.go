package harness

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// headlessActivity writes the Agent Wall's activity log for a headless run whose CLI has no
// run-activity mod of its own (kimi): one line per tool call and result, in the format the Claude Code
// run-activity mod writes (~/ops/claude-mods/run-activity), to <dir>/<pid>.log, which the wall feed
// (~/ops/agent-wall/agent-wall-feed.py) tails. The wall is a convenience: every write error is ignored.
type headlessActivity struct {
	mu      sync.Mutex
	path    string
	lines   []string
	footer  string
	started time.Time
	calls   map[string]headlessActivityCall
}

type headlessActivityCall struct {
	tool string
	at   time.Time
}

const (
	headlessActivityMaxLines = 200
	headlessActivityWidth    = 110
)

// headlessActivityNow is the activity log's clock; tests replace it.
var headlessActivityNow = time.Now

// headlessActivityDir is where the wall reads activity logs: AGENT_ACTIVITY_DIR, else
// ~/ops/state/agent-activity when that directory exists. Empty means no log is written.
func headlessActivityDir() string {
	if dir := strings.TrimSpace(os.Getenv("AGENT_ACTIVITY_DIR")); dir != "" {
		return dir
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	dir := filepath.Join(home, "ops", "state", "agent-activity")
	if info, err := os.Stat(dir); err == nil && info.IsDir() {
		return dir
	}
	return ""
}

func newHeadlessActivity(dir string, pid int, cwd, prompt, model string) *headlessActivity {
	if dir == "" || pid <= 0 {
		return nil
	}
	a := &headlessActivity{path: filepath.Join(dir, fmt.Sprintf("%d.log", pid)), started: headlessActivityNow(), calls: map[string]headlessActivityCall{}}
	if model != "" {
		a.footer = "⏵ " + model
	}
	a.emit(fmt.Sprintf("%s  ● run started  %s", activityHMS(a.started), cwd))
	a.emit(fmt.Sprintf("%s  ◆ task%s", activityHMS(a.started), activityTail(activityClip(activityRedact(activityFirstLine(prompt)), headlessActivityWidth))))
	return a
}

func (a *headlessActivity) emit(line string) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.lines = append(a.lines, line)
	if len(a.lines) > headlessActivityMaxLines {
		a.lines = a.lines[len(a.lines)-headlessActivityMaxLines:]
	}
	body := strings.Join(a.lines, "\n") + "\n"
	if a.footer != "" {
		body += a.footer + "\n"
	}
	tmp := a.path + ".tmp"
	if os.WriteFile(tmp, []byte(body), 0o600) == nil {
		_ = os.Rename(tmp, a.path)
	}
}

// call records a tool call (`▶ Tool  summary`) and remembers when it started.
func (a *headlessActivity) call(id, tool, arguments string) {
	if a == nil {
		return
	}
	now := headlessActivityNow()
	a.mu.Lock()
	if _, seen := a.calls[id]; seen { // a repeated ACTIVE update for the same step
		a.mu.Unlock()
		return
	}
	a.calls[id] = headlessActivityCall{tool: tool, at: now}
	a.mu.Unlock()
	a.emit(fmt.Sprintf("%s  ▶ %s%s", activityHMS(now), tool, activityTail(activitySummary(tool, arguments))))
}

// result records the end of a tool call (`✓ Tool  0.4s`).
func (a *headlessActivity) result(id string) {
	if a == nil {
		return
	}
	now := headlessActivityNow()
	a.mu.Lock()
	call, ok := a.calls[id]
	delete(a.calls, id)
	a.mu.Unlock()
	if !ok {
		return
	}
	a.emit(fmt.Sprintf("%s  ✓ %s  %s", activityHMS(now), call.tool, activitySpan(now.Sub(call.at))))
}

// finish closes the log: `◼ turn done` and `● run finished` on success; on failure a red
// `✗ run failed  <reason>` line, so a quota wall reads on the wall at a glance.
func (a *headlessActivity) finish(failure string) {
	if a == nil {
		return
	}
	now := headlessActivityNow()
	if failure != "" {
		reason := strings.TrimPrefix(activityErrorLine(failure), "error: failed to run prompt: ")
		a.emit(fmt.Sprintf("%s  ✗ run failed  %s", activityHMS(now), activityClip(activityRedact(reason), headlessActivityWidth)))
	} else {
		a.emit(fmt.Sprintf("%s  ◼ turn done  %s", activityHMS(now), activitySpan(now.Sub(a.started))))
	}
	a.emit(fmt.Sprintf("%s  ● run finished", activityHMS(now)))
}

var activitySecretPatterns = []struct {
	re *regexp.Regexp
	to string
}{
	{regexp.MustCompile(`(?i)(Bearer\s+)\S+`), "${1}[redacted]"},
	{regexp.MustCompile(`(?i)\b((?:api[_-]?key|token|secret|password|passwd|authorization)\s*[=:]\s*)\S+`), "${1}[redacted]"},
	{regexp.MustCompile(`\b(?:sk|pk|ghp|gho|glpat|xox[abp])[-_][A-Za-z0-9_-]{12,}`), "[redacted]"},
	{regexp.MustCompile(`\bAKIA[0-9A-Z]{16}\b`), "[redacted]"},
	{regexp.MustCompile(`(?i)\b[0-9a-f]{32,}\b`), "[redacted]"},
}

// activityLongToken is the mod's "long mixed-case token with a digit" rule (Go regexp has no
// lookahead, so the three conditions are checked by hand).
var activityLongToken = regexp.MustCompile(`[A-Za-z0-9+_-]{40,}`)

func activityRedact(text string) string {
	for _, p := range activitySecretPatterns {
		text = p.re.ReplaceAllString(text, p.to)
	}
	return activityLongToken.ReplaceAllStringFunc(text, func(tok string) string {
		if strings.ContainsAny(tok, "ABCDEFGHIJKLMNOPQRSTUVWXYZ") && strings.ContainsAny(tok, "abcdefghijklmnopqrstuvwxyz") && strings.ContainsAny(tok, "0123456789") {
			return "[redacted]"
		}
		return tok
	})
}

func activityFirstLine(text string) string {
	for _, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) != "" {
			return strings.TrimSpace(line)
		}
	}
	return ""
}

// activityErrorLine is the line that says why a run failed: the last line starting with "error:"
// (Kimi follows it with a "See log: ..." line), else the last non-empty line.
func activityErrorLine(text string) string {
	lines := strings.Split(strings.TrimSpace(text), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if line := strings.TrimSpace(lines[i]); strings.HasPrefix(strings.ToLower(line), "error:") {
			return line
		}
	}
	return activityLastLine(text)
}

func activityLastLine(text string) string {
	lines := strings.Split(strings.TrimSpace(text), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if strings.TrimSpace(lines[i]) != "" {
			return strings.TrimSpace(lines[i])
		}
	}
	return ""
}

func activityClip(text string, width int) string {
	r := []rune(text)
	if len(r) <= width {
		return text
	}
	return string(r[:width-1]) + "…"
}

func activityClipLeft(text string, width int) string {
	r := []rune(text)
	if len(r) <= width {
		return text
	}
	return "…" + string(r[len(r)-width+1:])
}

func activityTail(text string) string {
	if text == "" {
		return ""
	}
	return "  " + text
}

func activityHMS(t time.Time) string { return t.Format("15:04:05") }

// activitySpan formats like the mod: "0.4s", "12s", "2m05s".
func activitySpan(d time.Duration) string {
	seconds := d.Seconds()
	if seconds < 0 {
		seconds = 0
	}
	switch {
	case seconds < 10:
		return fmt.Sprintf("%.1fs", seconds)
	case seconds < 60:
		return fmt.Sprintf("%.0fs", seconds)
	}
	whole := int(seconds + 0.5)
	return fmt.Sprintf("%dm%02ds", whole/60, whole%60)
}

// activitySummary says what a tool call is about, never what it carries (no file contents or edits):
// the command's first line, a path, a pattern, a host or a query.
func activitySummary(tool, arguments string) string {
	var raw map[string]any
	if json.Unmarshal([]byte(arguments), &raw) != nil {
		return ""
	}
	// Keys are matched case-insensitively: Kimi and Claude use command/path, agy CommandLine/TargetFile.
	args := make(map[string]string, len(raw))
	for k, v := range raw {
		if text, ok := v.(string); ok {
			args[strings.ToLower(k)] = text
		}
	}
	str := func(keys ...string) string {
		for _, key := range keys {
			if v := activityFirstLine(args[key]); v != "" {
				return v
			}
		}
		return ""
	}
	if c := str("command", "commandline", "cmd"); c != "" {
		return activityClip(activityRedact(c), headlessActivityWidth)
	}
	paths := []string{"path", "file_path", "filepath", "targetfile", "absolutepath", "notebook_path", "searchpath", "directorypath", "searchdirectory"}
	if p := str("pattern", "includes"); p != "" {
		if where := str(paths...); where != "" {
			p += "  in " + where
		}
		return activityClip(activityRedact(p), headlessActivityWidth)
	}
	if p := str(paths...); p != "" {
		return activityClipLeft(p, headlessActivityWidth)
	}
	if u := str("url"); u != "" {
		if parsed, err := url.Parse(u); err == nil {
			return parsed.Host
		}
		return ""
	}
	if q := str("query", "description"); q != "" {
		return activityClip(activityRedact(q), headlessActivityWidth)
	}
	return ""
}

// headlessTaskID is the task or goal id in a document session key
// (orchestrator:<tasks|goals>:<phase>:<id>:<attempt>), else "" (chat, notification and heartbeat keys
// name no document).
func headlessTaskID(key string) string {
	parts := strings.Split(strings.TrimSpace(key), ":")
	if len(parts) >= 5 && parts[0] == "orchestrator" && (parts[1] == "tasks" || parts[1] == "goals") {
		return parts[3]
	}
	return ""
}
