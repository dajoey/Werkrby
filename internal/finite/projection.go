package finite

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/agent0ai/spynel/internal/fsx"
)

var atomicWriteFile = fsx.AtomicWriteFile

// RenderProjection renders deterministic Markdown from authoritative records.
func RenderProjection(task Task, runs []Run) string {
	quote := func(value string) string {
		encoded, _ := json.Marshal(value)
		return string(encoded)
	}
	var out strings.Builder
	out.WriteString("---\n")
	fmt.Fprintf(&out, "id: %s\n", task.ID)
	fmt.Fprintf(&out, "title: %s\n", quote(task.Title))
	fmt.Fprintf(&out, "state: %s\n", task.State)
	fmt.Fprintf(&out, "risk: %s\n", task.Risk)
	fmt.Fprintf(&out, "review_required: %t\n", task.ReviewRequired)
	fmt.Fprintf(&out, "cancel_requested: %t\n", task.CancelRequested)
	fmt.Fprintf(&out, "version: %d\n", task.Version)
	fmt.Fprintf(&out, "created_at: %s\n", quote(stamp(task.CreatedAt)))
	fmt.Fprintf(&out, "updated_at: %s\n", quote(stamp(task.UpdatedAt)))
	if task.State == StateWaiting {
		fmt.Fprintf(&out, "resume_state: %s\n", task.ResumeState)
		fmt.Fprintf(&out, "wait_kind: %s\n", task.WaitKind)
		if task.WakeAt != nil {
			fmt.Fprintf(&out, "wake_at: %s\n", quote(stamp(*task.WakeAt)))
		}
		fmt.Fprintf(&out, "waiting_for: %s\n", quote(task.WaitingFor))
	}
	out.WriteString("---\n\n<!-- Generated from engine.db; read-only projection. -->\n\n")
	fmt.Fprintf(&out, "# %s\n\n", task.Title)
	out.WriteString("## Runs\n\n")
	if len(runs) == 0 {
		out.WriteString("No provider runs.\n")
		return out.String()
	}
	out.WriteString("| Run | Phase | Execution | Created | Ended |\n| ---: | --- | --- | --- | --- |\n")
	ordered := append([]Run(nil), runs...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })
	for _, run := range ordered {
		outcome := run.Outcome
		if outcome == "" {
			outcome = "open"
		}
		ended := ""
		if run.EndedAt != nil {
			ended = stamp(*run.EndedAt)
		}
		fmt.Fprintf(&out, "| %d | %s | %s | %s | %s |\n", run.ID, run.Phase, outcome, stamp(run.CreatedAt), ended)
	}
	return out.String()
}

// ProjectTask atomically republishes the disposable Markdown projection.
func (s *Store) ProjectTask(ctx context.Context, taskID string) error {
	task, err := s.GetTask(ctx, taskID)
	if err != nil {
		return err
	}
	runs, err := s.ListRuns(ctx, taskID)
	if err != nil {
		return err
	}
	return writeProjectionWithFaults(task.ProjectionPath, []byte(RenderProjection(task, runs)), 0o600)
}
