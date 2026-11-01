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
// only and never drives workflow state.
func (s *Store) TaskHistory(ctx context.Context, taskID string) (string, error) {
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
		}
		if err := json.Unmarshal([]byte(payload), &event); err != nil {
			continue
		}
		switch kind {
		case "answered":
			line := fmt.Sprintf("- Joey answered %q: %s (%s)", event.Question, event.Label, event.OptionID)
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
			if event.Progress != "" {
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
