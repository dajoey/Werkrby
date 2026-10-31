package finite

import "path/filepath"

// Validate checks the disposable state-root boundary.
func (c Config) Validate() error { return nil }

func (c Config) DatabasePath() string  { return filepath.Join(c.Root, "engine.db") }
func (c Config) BriefDir() string      { return filepath.Join(c.Root, "briefs") }
func (c Config) ProjectionDir() string { return filepath.Join(c.Root, "tasks") }
func (c Config) RunsDir() string       { return filepath.Join(c.Root, "runs") }
