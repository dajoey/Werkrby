package finite

import (
	"fmt"
	"path/filepath"
	"strings"
)

// Validate checks the disposable state-root boundary.
func (c Config) Validate() error {
	if strings.TrimSpace(c.Root) == "" || strings.TrimSpace(c.WorkerDir) == "" {
		return fmt.Errorf("finite: root and worker directory are required")
	}
	root, err := filepath.Abs(c.Root)
	if err != nil {
		return fmt.Errorf("finite: resolve state root: %w", err)
	}
	worker, err := filepath.Abs(c.WorkerDir)
	if err != nil {
		return fmt.Errorf("finite: resolve worker directory: %w", err)
	}
	root = filepath.Clean(root)
	worker = filepath.Clean(worker)
	if pathWithin(root, worker) || pathWithin(worker, root) {
		return fmt.Errorf("finite: state root and worker directory must be disjoint")
	}
	for _, part := range strings.Split(filepath.Clean(root), string(filepath.Separator)) {
		if strings.EqualFold(part, ".werkrby") {
			return fmt.Errorf("finite: legacy .werkrby state is not a disposable root")
		}
	}
	return nil
}

func pathWithin(path, parent string) bool {
	rel, err := filepath.Rel(parent, path)
	return err == nil && (rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))))
}

func (c Config) DatabasePath() string  { return filepath.Join(c.Root, "engine.db") }
func (c Config) BriefDir() string      { return filepath.Join(c.Root, "briefs") }
func (c Config) ProjectionDir() string { return filepath.Join(c.Root, "tasks") }
func (c Config) RunsDir() string       { return filepath.Join(c.Root, "runs") }
