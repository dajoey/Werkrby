package finite

import (
	"context"

	"github.com/agent0ai/spynel/internal/harness"
)

// SupervisorSpec completely describes one isolated provider turn.
type SupervisorSpec struct {
	RunID        int64
	RunDir       string
	ProcessToken string
	Executable   string
	SessionKey   string
	Prompt       string
	Harness      harness.HarnessConfig
}

// SupervisorHandle is the owner's local handle to a re-executed supervisor.
type SupervisorHandle struct {
	Identity ProcessIdentity
	done     chan struct{}
	waitErr  error
}

// StartSupervisor writes run-private protocol artifacts and re-executes the
// experimental binary. Unsupported platforms refuse before creating a child.
func StartSupervisor(ctx context.Context, spec SupervisorSpec) (*SupervisorHandle, error) {
	done := make(chan struct{})
	close(done)
	return &SupervisorHandle{Identity: ProcessIdentity{RunID: spec.RunID, Token: spec.ProcessToken}, done: done}, nil
}

// Wait waits only for wrapper exit; provider-boundary absence requires
// InspectRun or AdoptedRun.Wait.
func (h *SupervisorHandle) Wait() error {
	<-h.done
	return h.waitErr
}

// RunSupervisor is the child-side file-only protocol. It never opens SQLite.
func RunSupervisor(ctx context.Context, args []string, factory harness.Factory) error {
	return nil
}
