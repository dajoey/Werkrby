package cli

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"sort"

	"github.com/agent0ai/spynel/internal/admission"
	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/roster"
)

const admissionUsage = "usage: werkrby admission explain [--seat NAME] [--kind KIND] [--claude-only] [--json]"

var admissionKinds = map[string]admission.Kind{
	"task_implementation": admission.KindTaskImplementation,
	"task_review":         admission.KindTaskReview,
	"goal_planning":       admission.KindGoalPlanning,
	"goal_review":         admission.KindGoalReview,
	"chat":                admission.KindChat,
	"notification":        admission.KindNotification,
	"heartbeat":           admission.KindHeartbeat,
	"continuation":        admission.KindContinuation,
}

// runAdmissionCommand answers "why is this seat (not) running": the verdict admission.Decide gives for
// a seat right now, from the same files the old deciders read. It writes nothing.
func runAdmissionCommand(args []string, out io.Writer) error {
	if len(args) == 0 || args[0] != "explain" {
		return errors.New(admissionUsage)
	}
	flags := flag.NewFlagSet("admission explain", flag.ContinueOnError)
	seat := flags.String("seat", "", "explain one roster seat (default: every seat)")
	kindName := flags.String("kind", "task_implementation", "kind of claim")
	claudeOnly := flags.Bool("claude-only", false, "treat the claim as claude_only")
	asJSON := flags.Bool("json", false, "emit JSON keyed by seat")
	stateDir := flags.String("state", "", "state directory (default: the workspace's)")
	snapshot := flags.String("snapshot", "", "usage snapshot (default: the ai-usage collector's)")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New(admissionUsage)
	}
	kind, ok := admissionKinds[*kindName]
	if !ok {
		return fmt.Errorf("unknown kind %q", *kindName)
	}
	if *stateDir == "" {
		cfg, err := config.Load("")
		if err != nil {
			return err
		}
		*stateDir = cfg.StatePath()
	}
	if *snapshot == "" {
		*snapshot = admission.DefaultSnapshotPath()
	}
	r, err := roster.Load(*stateDir)
	if err != nil {
		return err
	}
	if r == nil {
		return fmt.Errorf("no roster at %s: admission decides for roster seats", roster.Path(*stateDir))
	}
	seats := []string{*seat}
	if *seat == "" {
		seats = seats[:0]
		for name := range r.Staff {
			seats = append(seats, name)
		}
		sort.Strings(seats)
	} else if !r.Has(*seat) {
		return fmt.Errorf("seat %q is not in the roster", *seat)
	}
	var constraints []string
	if *claudeOnly {
		constraints = append(constraints, admission.ConstraintClaudeOnly)
	}
	decider := admission.New(*stateDir, *snapshot)
	decisions := map[string]admission.Decision{}
	for _, name := range seats {
		decisions[name] = decider.Decide(admission.Request{Kind: kind, Asked: name, Constraints: constraints})
	}
	if *asJSON {
		data, err := json.MarshalIndent(decisions, "", "  ")
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(out, string(data))
		return err
	}
	for _, name := range seats {
		if _, err := fmt.Fprint(out, decisions[name].Explain(name)); err != nil {
			return err
		}
	}
	return nil
}
