package orchestrator

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	phaseTaskImplementation = "task_implementation"
	phaseTaskReview         = "task_review"
	phaseGoalPlanning       = "goal_planning"
	phaseGoalReview         = "goal_review"
)

var taskSettledStatuses = map[string]bool{"done": true, "failed": true, "cancelled": true}

func statusPath(base, status, name string) string {
	return filepath.Join(base, status, name)
}

func documentID(document Document) string {
	id, _ := document.FrontMatter["id"].(string)
	return strings.TrimSpace(id)
}

func stringField(document Document, name string) string {
	value, _ := document.FrontMatter[name].(string)
	return strings.TrimSpace(value)
}

func phaseAttemptField(phase string) string {
	switch phase {
	case phaseTaskReview, phaseGoalReview:
		return "review_attempt"
	case phaseGoalPlanning:
		return "planning_attempt"
	default:
		return "attempt"
	}
}

func phaseClaimedStatus(phase string) string {
	switch phase {
	case phaseTaskImplementation:
		return "working"
	case phaseTaskReview, phaseGoalReview:
		return "reviewing"
	case phaseGoalPlanning:
		return "planning"
	default:
		return "working"
	}
}

func phaseSessionKey(routeName, id, phase string, attempt int) string {
	if attempt < 1 {
		attempt = 1
	}
	return fmt.Sprintf("orchestrator:%s:%s:%s:%d", routeName, phase, id, attempt)
}

func validSuccessCriteria(document Document) error {
	raw, ok := document.FrontMatter["success_criteria"]
	if !ok {
		return errors.New("success_criteria is required")
	}
	criteria, ok := raw.([]any)
	if !ok {
		// Documents created in-process can retain the concrete map slice until
		// they are serialized and read again.
		if typed, typedOK := raw.([]map[string]any); typedOK {
			criteria = make([]any, len(typed))
			for i := range typed {
				criteria[i] = typed[i]
			}
		} else {
			return errors.New("success_criteria must be a list")
		}
	}
	if len(criteria) == 0 {
		return errors.New("success_criteria cannot be empty")
	}
	seen := map[string]bool{}
	for i, item := range criteria {
		criterion, ok := item.(map[string]any)
		if !ok {
			return fmt.Errorf("success_criteria[%d] must be a mapping", i)
		}
		id, _ := criterion["id"].(string)
		condition, _ := criterion["condition"].(string)
		evidence, _ := criterion["evidence_required"].(string)
		id = strings.TrimSpace(id)
		if id == "" || strings.TrimSpace(condition) == "" || strings.TrimSpace(evidence) == "" {
			return fmt.Errorf("success_criteria[%d] requires id, condition, and evidence_required", i)
		}
		if seen[id] {
			return fmt.Errorf("duplicate success criterion %q", id)
		}
		seen[id] = true
	}
	return nil
}

func goalReviewProvesDone(document Document) error {
	if err := validSuccessCriteria(document); err != nil {
		return err
	}
	raw, ok := document.FrontMatter["last_review"].(map[string]any)
	if !ok {
		return errors.New("last_review is required")
	}
	verdict, _ := raw["verdict"].(string)
	satisfied, _ := raw["criteria_satisfied"].(bool)
	if strings.TrimSpace(verdict) != "done" || !satisfied {
		return errors.New("last_review must prove a done verdict and all criteria satisfied")
	}
	if numberValue(raw["round"]) != numberValue(document.FrontMatter["round"]) {
		return errors.New("last_review.round must match the current goal round")
	}
	if reviewedAt, _ := raw["reviewed_at"].(string); strings.TrimSpace(reviewedAt) == "" {
		return errors.New("last_review.reviewed_at is required")
	} else if _, err := time.Parse(time.RFC3339, reviewedAt); err != nil {
		return errors.New("last_review.reviewed_at must be RFC 3339")
	}
	return nil
}

// hasJoeyAsk reports whether the document carries a non-empty joey_ask: it is
// parked on Joey's answer, which no clock can supply.
func hasJoeyAsk(document Document) bool {
	switch typed := document.FrontMatter["joey_ask"].(type) {
	case nil:
		return false
	case string:
		return strings.TrimSpace(typed) != ""
	case map[string]any:
		return len(typed) > 0
	case map[any]any:
		return len(typed) > 0
	default:
		return true
	}
}

// scheduledWake reports whether a waiting document's date has come. A document
// parked on a joey_ask is never due by date: a wake_at or not_before left over
// from an earlier clock park must not return Joey's question to the queue.
func scheduledWake(document Document, now time.Time) (bool, error) {
	if hasJoeyAsk(document) {
		return false, nil
	}
	for _, field := range []string{"wake_at", "not_before", "next_dispatch_at"} {
		value, exists := document.FrontMatter[field]
		if !exists {
			continue
		}
		var when time.Time
		switch typed := value.(type) {
		case string:
			parsed, err := time.Parse(time.RFC3339, typed)
			if err != nil {
				return false, fmt.Errorf("%s must be RFC 3339", field)
			}
			when = parsed
		case time.Time:
			when = typed
		default:
			return false, fmt.Errorf("%s must be an RFC 3339 timestamp", field)
		}
		return !when.After(now), nil
	}
	return false, nil
}

func checkpointDue(document Document, now time.Time) (bool, error) {
	value, exists := document.FrontMatter["next_review_at"]
	if !exists {
		return false, nil
	}
	var when time.Time
	switch typed := value.(type) {
	case string:
		parsed, err := time.Parse(time.RFC3339, typed)
		if err != nil {
			return false, errors.New("next_review_at must be RFC 3339")
		}
		when = parsed
	case time.Time:
		when = typed
	default:
		return false, errors.New("next_review_at must be an RFC 3339 timestamp")
	}
	return !when.After(now), nil
}

type linkedTask struct {
	ID     string
	Path   string
	Status string
}

// unreadableRoundTaskError reports a task document that cannot be parsed but names the goal
// whose round is being read. It matches errUnreadableDocument so callers skip that one goal for
// the scan instead of treating the round as smaller than declared.
type unreadableRoundTaskError struct {
	Path string
	Err  error
}

func (e *unreadableRoundTaskError) Error() string {
	return "round task " + e.Path + " is unreadable: " + e.Err.Error()
}

func (e *unreadableRoundTaskError) Unwrap() error { return e.Err }

func (e *unreadableRoundTaskError) Is(target error) bool { return target == errUnreadableDocument }

// unreadableNamesGoal reports whether the raw text of an unparseable document mentions goalID.
func unreadableNamesGoal(path, goalID string) bool {
	raw, err := os.ReadFile(path)
	return err == nil && goalID != "" && strings.Contains(string(raw), goalID)
}

func linkedRoundTasks(tasksBase, goalID string, round int) ([]linkedTask, error) {
	statuses := []string{"todo", "working", "review", "reviewing", "waiting", "done", "failed", "cancelled"}
	var found []linkedTask
	for _, status := range statuses {
		directory := filepath.Join(tasksBase, status)
		entries, err := os.ReadDir(directory)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".md") || entry.Name() == "AGENTS.md" {
				continue
			}
			path := filepath.Join(directory, entry.Name())
			document, err := ReadDocument(path)
			if err != nil {
				// An unreadable task that names this goal may be one of its round tasks, so the
				// round cannot be counted. Anything else is not this goal's business.
				if unreadableNamesGoal(path, goalID) {
					return nil, &unreadableRoundTaskError{Path: path, Err: err}
				}
				continue
			}
			if stringField(document, "goal_id") == goalID && numberValue(document.FrontMatter["goal_round"]) == round {
				found = append(found, linkedTask{ID: documentID(document), Path: path, Status: status})
			}
		}
	}
	return found, nil
}

func moveDocument(path, target, status string, now time.Time) error {
	return moveDocumentWithProgress(path, target, status, now, "")
}

func moveDocumentWithProgress(path, target, status string, now time.Time, note string) error {
	return moveDocumentWithUpdate(path, target, status, now, note, nil)
}

// moveDocumentWithUpdate applies update to the front matter in the same
// locked write that records the new status, before the rename.
func moveDocumentWithUpdate(path, target, status string, now time.Time, note string, update func(map[string]any)) error {
	return moveDocumentLocked(path, target, status, now, func(document *Document) {
		if update != nil {
			update(document.FrontMatter)
		}
	}, func(document *Document) {
		if note != "" {
			appendProgress(document, now, note)
		}
	})
}

// moveDocumentWithEdit applies edit to the whole document (front matter and body) in the same
// locked write that records the new status, before the rename. edit runs after status and
// updated_at are set, so it may read them.
func moveDocumentWithEdit(path, target, status string, now time.Time, edit func(*Document)) error {
	return moveDocumentLocked(path, target, status, now, nil, edit)
}

// moveDocumentLocked is one locked write and rename: before runs ahead of the status stamp, after
// runs once status and updated_at are set.
func moveDocumentLocked(path, target, status string, now time.Time, before, after func(*Document)) error {
	lock, err := lockProviderTurn(path)
	if err != nil {
		return err
	}
	defer unlockProviderTurn(lock)
	document, err := ReadDocument(path)
	if err != nil {
		return err
	}
	if before != nil {
		before(&document)
	}
	document.FrontMatter["status"] = status
	document.FrontMatter["updated_at"] = now.UTC().Format(time.RFC3339)
	if after != nil {
		after(&document)
	}
	if err := WriteDocument(path, document); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		return err
	}
	return os.Rename(path, target)
}

func updateDocumentProgress(path string, now time.Time, note string) error {
	return updateDocumentProgressWithUpdate(path, now, note, nil)
}

// updateDocumentProgressWithUpdate applies update to the front matter in the
// same locked write that appends the progress note, without moving the file.
func updateDocumentProgressWithUpdate(path string, now time.Time, note string, update func(map[string]any)) error {
	lock, err := lockProviderTurn(path)
	if err != nil {
		return err
	}
	defer unlockProviderTurn(lock)
	document, err := ReadDocument(path)
	if err != nil {
		return err
	}
	if update != nil {
		update(document.FrontMatter)
	}
	document.FrontMatter["updated_at"] = now.UTC().Format(time.RFC3339)
	appendProgress(&document, now, note)
	return WriteDocument(path, document)
}

func appendProgress(document *Document, now time.Time, note string) {
	note = strings.Join(strings.Fields(note), " ")
	if runes := []rune(note); len(runes) > 1000 {
		note = string(runes[:999]) + "…"
	}
	insertProgressEntry(document, "- "+now.UTC().Format(time.RFC3339)+" — "+note)
}

// appendProgressBlock appends a multi-line Markdown entry (a worker's own progress text) as one
// list item: the first line carries the timestamp and later lines are indented under it.
func appendProgressBlock(document *Document, now time.Time, text string) {
	text = strings.TrimSpace(strings.ReplaceAll(text, "\r\n", "\n"))
	if runes := []rune(text); len(runes) > maxOutcomeProgressRunes {
		text = string(runes[:maxOutcomeProgressRunes-1]) + "…"
	}
	lines := strings.Split(text, "\n")
	for index, line := range lines {
		line = strings.TrimRight(line, " \t")
		switch {
		case index == 0:
			lines[index] = "- " + now.UTC().Format(time.RFC3339) + " — " + line
		case line == "":
			lines[index] = ""
		case strings.HasPrefix(strings.TrimSpace(line), "#"):
			// A heading inside the entry would end the Progress section for later appends.
			lines[index] = "  \\" + strings.TrimSpace(line)
		default:
			lines[index] = "  " + line
		}
	}
	insertProgressEntry(document, strings.Join(lines, "\n"))
}

// insertProgressEntry puts entry at the end of the ## Progress section (creating it if needed),
// unless the body already holds the same entry.
func insertProgressEntry(document *Document, entry string) {
	body := strings.ReplaceAll(document.Body, "\r\n", "\n")
	if strings.Contains("\n"+body+"\n", "\n"+entry+"\n") {
		return
	}
	lines := strings.Split(body, "\n")
	progress := -1
	end := len(lines)
	for index, line := range lines {
		trimmed := strings.TrimSpace(line)
		if progress < 0 {
			if strings.EqualFold(trimmed, "## Progress") {
				progress = index
			}
			continue
		}
		if strings.HasPrefix(trimmed, "## ") {
			end = index
			break
		}
	}
	if progress < 0 {
		document.Body = strings.TrimRight(body, "\n") + "\n\n## Progress\n\n" + entry + "\n"
		return
	}
	prefix := strings.TrimRight(strings.Join(lines[:end], "\n"), "\n")
	suffix := strings.TrimLeft(strings.Join(lines[end:], "\n"), "\n")
	document.Body = prefix + "\n\n" + entry + "\n"
	if suffix != "" {
		document.Body += "\n" + suffix
		if !strings.HasSuffix(document.Body, "\n") {
			document.Body += "\n"
		}
	}
}
