package finite

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/agent0ai/spynel/internal/harness"
)

// The real finite supervisor opts in only for the explicitly gated live test.
// Ordinary production composition has no run-boundary context and stays inert.
func init() {
	if len(os.Args) <= 1 || os.Args[1] != "supervise" || os.Getenv("WERKRBY_FINITE_HERMES_LIVE") != "1" {
		return
	}
	runID, err := strconv.ParseInt(os.Getenv(finiteRunIDEnv), 10, 64)
	if err != nil {
		os.Exit(2)
	}
	spec := harness.RunBoundarySpec{RunID: runID, ProcessToken: os.Getenv(finiteRunTokenEnv), RunDir: os.Getenv(finiteRunDirEnv)}
	ctx := harness.WithRunBoundary(context.Background(), spec)
	if err := RunSupervisor(ctx, os.Args[1:], NewRunner); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}
