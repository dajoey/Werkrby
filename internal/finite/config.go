package finite

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// legacyStateDirNames are the ordinary Werkrby state trees a finite root must
// never be, sit inside, or contain.
var legacyStateDirNames = []string{".werkrby", ".spynel"}

// Validate checks the disposable state-root boundary after symlink
// resolution: the root is disjoint from the worker directory and is not, is
// not inside, and does not directly contain a legacy .werkrby/.spynel tree.
func (c Config) Validate() error {
	if strings.TrimSpace(c.Root) == "" || strings.TrimSpace(c.WorkerDir) == "" {
		return fmt.Errorf("finite: root and worker directory are required")
	}
	root, resolvedRoot, err := resolveStatePath(c.Root)
	if err != nil {
		return fmt.Errorf("finite: resolve state root: %w", err)
	}
	worker, resolvedWorker, err := resolveStatePath(c.WorkerDir)
	if err != nil {
		return fmt.Errorf("finite: resolve worker directory: %w", err)
	}
	for _, pair := range [][2]string{{root, worker}, {resolvedRoot, resolvedWorker}} {
		if pathWithin(pair[0], pair[1]) || pathWithin(pair[1], pair[0]) {
			return fmt.Errorf("finite: state root and worker directory must be disjoint")
		}
	}
	return checkLegacyStateRoot(root, resolvedRoot)
}

// resolveStatePath returns path made absolute and clean, and the same path
// with every symlink in its longest existing prefix resolved. The remainder
// does not exist yet, so it holds no symlinks. A dangling symlink is refused
// because its eventual target cannot be checked.
func resolveStatePath(path string) (string, string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", "", err
	}
	absolute = filepath.Clean(absolute)
	prefix, rest := absolute, ""
	for {
		resolved, err := filepath.EvalSymlinks(prefix)
		if err == nil {
			return absolute, filepath.Join(resolved, rest), nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return "", "", err
		}
		if info, lstatErr := os.Lstat(prefix); lstatErr == nil && info.Mode()&fs.ModeSymlink != 0 {
			return "", "", fmt.Errorf("%s is a dangling symlink", prefix)
		}
		parent := filepath.Dir(prefix)
		if parent == prefix {
			return "", "", err
		}
		rest = filepath.Join(filepath.Base(prefix), rest)
		prefix = parent
	}
}

func isLegacyStateDirName(name string) bool {
	for _, legacy := range legacyStateDirNames {
		if strings.EqualFold(name, legacy) {
			return true
		}
	}
	return false
}

// checkLegacyStateRoot refuses a root whose lexical or resolved path has a
// legacy state component, or whose resolved directory directly contains a
// legacy state directory. A root that is a file contains nothing.
func checkLegacyStateRoot(root, resolved string) error {
	for _, path := range []string{root, resolved} {
		for _, part := range strings.Split(path, string(filepath.Separator)) {
			if isLegacyStateDirName(part) {
				return fmt.Errorf("finite: legacy %s state is not a disposable root", part)
			}
		}
	}
	for _, legacy := range legacyStateDirNames {
		info, err := os.Stat(filepath.Join(resolved, legacy))
		if err == nil && info.IsDir() {
			return fmt.Errorf("finite: state root %s contains legacy %s state", root, legacy)
		}
		if err != nil && !errors.Is(err, fs.ErrNotExist) && !errors.Is(err, syscall.ENOTDIR) {
			return fmt.Errorf("finite: inspect state root for legacy %s state: %w", legacy, err)
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
