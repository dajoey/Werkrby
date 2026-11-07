package finite

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	historyProgressRunes = 2000
	historyFindingRunes  = 500
	historyNoteRunes     = 4000
	historyMaxBytes      = 24 * 1024
)

func clipRunes(text string, limit int) string {
	text = strings.TrimSpace(text)
	runes := []rune(text)
	if len(runes) <= limit {
		return text
	}
	return string(runes[:limit]) + "…"
}

// TaskHistory renders what earlier runs of a task concluded and how Joey
// answered, oldest first, from committed task events. It is prompt context
// only and never drives workflow state. This is the implementation-phase
// view: every earlier run carries its own progress.
func (s *Store) TaskHistory(ctx context.Context, taskID string) (string, error) {
	return s.TaskHistoryForPhase(ctx, taskID, PhaseImplementation)
}

// TaskHistoryForPhase renders the task history as one phase's prompt reads
// it. A review run sees findings and Joey's answers but never the progress
// text an implementer wrote about its own work; an implementation run sees
// everything.
func (s *Store) TaskHistoryForPhase(ctx context.Context, taskID string, phase Phase) (string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT kind, run_id, payload_json FROM task_events
		WHERE task_id=? AND kind IN ('settled','answered') ORDER BY rowid`, taskID)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var kind, payload string
		var runID *int64
		if err := rows.Scan(&kind, &runID, &payload); err != nil {
			return "", err
		}
		var event struct {
			Phase            string   `json:"phase"`
			ExecutionOutcome string   `json:"execution_outcome"`
			ResultOutcome    string   `json:"result_outcome"`
			Error            string   `json:"error"`
			Progress         string   `json:"progress"`
			Findings         []string `json:"findings"`
			Question         string   `json:"question"`
			Label            string   `json:"label"`
			OptionID         string   `json:"option_id"`
			Note             string   `json:"note"`
		}
		if err := json.Unmarshal([]byte(payload), &event); err != nil {
			continue
		}
		switch kind {
		case "answered":
			line := fmt.Sprintf("- Joey answered %q: %s (%s)", event.Question, event.Label, event.OptionID)
			if note := strings.TrimSpace(event.Note); note != "" {
				// The note is Joey's instruction and wins over the option
				// label when they conflict; a later run that acts on the
				// label alone repeats the ignored-note failure.
				line += " AND attached a note — the NOTE overrides the option label when they conflict, follow the note:\n  Note: " + clipRunes(note, historyNoteRunes)
			}
			lines = append(lines, line)
		case "settled":
			if runID == nil {
				continue
			}
			outcome := event.ResultOutcome
			if outcome == "" {
				outcome = "no result (" + event.ExecutionOutcome + ")"
			}
			line := fmt.Sprintf("- Run %d (%s): %s.", *runID, event.Phase, outcome)
			if event.Phase == "" {
				line = fmt.Sprintf("- Run %d: %s.", *runID, outcome)
			}
			// A review prompt never sees what an implementer wrote about its own
			// work: a settled implementation run renders without its progress,
			// with its error text when there is one. Findings stay.
			implemented := event.Phase == "" || event.Phase == string(PhaseImplementation)
			if event.Progress != "" && !(phase == PhaseReview && implemented) {
				line += " " + event.Progress
			} else if event.Error != "" {
				line += " " + clipRunes(event.Error, historyFindingRunes)
			}
			for _, finding := range event.Findings {
				line += "\n  - Finding: " + finding
			}
			lines = append(lines, line)
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	text := strings.Join(lines, "\n")
	for len(text) > historyMaxBytes && len(lines) > 1 {
		lines = lines[1:]
		text = "- (earlier runs omitted)\n" + strings.Join(lines, "\n")
	}
	return text, nil
}
