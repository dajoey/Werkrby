package orchestrator

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/agent0ai/spynel/internal/config"
)

// externalExecutorField is the front-matter key a separate engine sets on a task document it
// executes; finiteExecutor is the only value the legacy orchestrator recognises. The bridge that
// hands todo/ documents to werkrby-finite writes it under the document's provider-turn lock before
// moving the document, and owns every later move (working, waiting, done, failed).
const (
	externalExecutorField = "executor"
	finiteExecutor        = "finite"
)

// errExternallyExecuted is returned by the locked document helpers (claim, move, progress, provider
// reservation, resume credit) when the document they hold the lock for is executed by another
// engine. Nothing was written or moved.
var errExternallyExecuted = errors.New("document is executed by werkrby-finite (executor: finite)")

// errClaimSourceGone is returned by a claim whose source left its queue folder before the claim
// held the provider-turn lock (the bridge took it, or another writer moved it). Nothing was claimed.
var errClaimSourceGone = errors.New("claim source left its queue folder before the claim lock")

// externallyExecuted reports whether another engine owns this document's execution: front matter
// `executor` is exactly the string "finite". The legacy orchestrator never claims, leases,
// dispatches, recovers, wakes, notifies about, archives, or writes such a document in any status
// folder. Goals still read its folder and completion_summary as round evidence.
func externallyExecuted(document Document) bool {
	value, ok := document.FrontMatter[externalExecutorField].(string)
	return ok && value == finiteExecutor
}

// externallyExecutedAt reads path and applies externallyExecuted. A missing or unreadable document
// is not external, so it keeps today's behaviour.
func externallyExecutedAt(path string) bool {
	if path == "" {
		return false
	}
	document, err := ReadDocument(path)
	return err == nil && externallyExecuted(document)
}

// refuseExternallyExecuted is the check every locked document helper applies to the document it
// read under the provider-turn lock.
func refuseExternallyExecuted(path string, document Document) error {
	if externallyExecuted(document) {
		return fmt.Errorf("%s: %w", filepath.Base(path), errExternallyExecuted)
	}
	return nil
}

// claimAbandoned reports a claim that found, under its lock, that the document is no longer the
// orchestrator's to claim. The caller drops the claim's lease and moves on: no attempt, no error
// path, no recovery.
func claimAbandoned(err error) bool {
	return errors.Is(err, errExternallyExecuted) || errors.Is(err, errClaimSourceGone)
}

// releaseExternallyExecutedLease drops a legacy lease whose document (the first readable of paths)
// another engine now executes, and reports whether it did. The document itself is not written,
// moved, hooked, or notified about. A lease on a missing or unreadable document is kept.
func (m *Manager) releaseExternallyExecutedLease(lease Lease, paths ...string) bool {
	for _, path := range paths {
		if externallyExecutedAt(path) {
			m.dropExternallyExecutedLease(lease, path)
			return true
		}
	}
	return false
}

func (m *Manager) dropExternallyExecutedLease(lease Lease, path string) {
	_ = os.Remove(m.leasePath(lease.ID))
	m.finishRuntimeJob(lease.ID)
	m.log(fmt.Sprintf("released lease %s without acting on %s: it is executed by werkrby-finite (executor: finite)", lease.ID, filepath.Base(path)))
}

// handedToExternalExecutor reports whether a queued task belongs to the external engine's bridge:
// the configured switch file exists and the old runner has never started the task (no
// first_assigned_at or implementation_session; attempt and review_attempt zero, as producers write
// a new task). Such a task stays in todo/ for the bridge to take. With the file absent, or for work
// already started, the orchestrator behaves as before.
func handedToExternalExecutor(cfg config.Config, route workflowRoute, phase string, document Document) bool {
	flag := strings.TrimSpace(cfg.Orchestrator.ExternalExecutorFlag)
	if flag == "" || route.Name != "tasks" || phase != phaseTaskImplementation {
		return false
	}
	if _, err := os.Stat(flag); err != nil {
		return false
	}
	for _, key := range []string{"first_assigned_at", "implementation_session"} {
		if value, ok := document.FrontMatter[key]; ok && value != nil && strings.TrimSpace(fmt.Sprint(value)) != "" {
			return false
		}
	}
	for _, key := range []string{"attempt", "review_attempt"} {
		if numberValue(document.FrontMatter[key]) > 0 {
			return false
		}
	}
	return true
}
