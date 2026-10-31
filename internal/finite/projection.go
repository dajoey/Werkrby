package finite

import (
	"context"

	"github.com/agent0ai/spynel/internal/fsx"
)

var atomicWriteFile = fsx.AtomicWriteFile

// RenderProjection renders deterministic Markdown from authoritative records.
func RenderProjection(task Task, runs []Run) string { return "" }

// ProjectTask atomically republishes the disposable Markdown projection.
func (s *Store) ProjectTask(ctx context.Context, taskID string) error { return nil }
