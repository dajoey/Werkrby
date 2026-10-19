package workspace

import (
	"embed"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/fsx"
	"github.com/agent0ai/spynel/internal/harness"
	"github.com/agent0ai/spynel/internal/theme"
)

//go:embed templates/*.md templates/*.yaml
var templates embed.FS

var detectCodingHarness = harness.Detect

type fileSpec struct {
	Path       string
	Template   string
	Executable bool
}

var files = []fileSpec{
	{Path: config.FileName, Template: "templates/config.yaml"},
	{Path: ".werkrby/AGENTS.md", Template: "templates/workspace-AGENTS.md"},
	{Path: ".werkrby/tasks/AGENTS.md", Template: "templates/tasks-AGENTS.md"},
	{Path: ".werkrby/goals/AGENTS.md", Template: "templates/goals-AGENTS.md"},
	{Path: ".werkrby/prompts/chat.md", Template: "templates/chat.md"},
	{Path: ".werkrby/prompts/create-task.md", Template: "templates/create-task.md"},
	{Path: ".werkrby/prompts/create-goal.md", Template: "templates/create-goal.md"},
	{Path: ".werkrby/prompts/task.md", Template: "templates/task.md"},
	{Path: ".werkrby/prompts/goal.md", Template: "templates/goal.md"},
	{Path: ".werkrby/prompts/goal-review.md", Template: "templates/goal-review.md"},
	{Path: ".werkrby/prompts/recovery.md", Template: "templates/recovery.md"},
	{Path: ".werkrby/prompts/review.md", Template: "templates/review.md"},
	{Path: ".werkrby/prompts/heartbeat.md", Template: "templates/heartbeat.md"},
	{Path: ".werkrby/prompts/notification.md", Template: "templates/notification.md"},
	{Path: ".werkrby/instructions/agent-chat.md", Template: "templates/agent-chat.md"},
	{Path: ".werkrby/instructions/agent-developer.md", Template: "templates/agent-developer.md"},
	{Path: ".werkrby/instructions/agent-reviewer.md", Template: "templates/agent-reviewer.md"},
	{Path: ".werkrby/instructions/agent-notification.md", Template: "templates/agent-notification.md"},
	{Path: ".werkrby/instructions/agent-heartbeat.md", Template: "templates/agent-heartbeat.md"},
	{Path: ".werkrby/extensions/README.md", Template: "templates/extensions.md"},
}

var directories = []string{
	".werkrby/history", ".werkrby/jobs", ".werkrby/attachments", ".werkrby/runtime/leases", ".werkrby/extensions", ".werkrby/themes", ".werkrby/instructions",
	".werkrby/tasks/todo", ".werkrby/tasks/working", ".werkrby/tasks/review", ".werkrby/tasks/reviewing", ".werkrby/tasks/waiting", ".werkrby/tasks/done", ".werkrby/tasks/failed", ".werkrby/tasks/cancelled", ".werkrby/tasks/archive",
	".werkrby/goals/proposed", ".werkrby/goals/planning", ".werkrby/goals/active", ".werkrby/goals/review", ".werkrby/goals/reviewing", ".werkrby/goals/waiting", ".werkrby/goals/done", ".werkrby/goals/abandoned",
}

func ensureRealDirectory(path, description string) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		if err := os.Mkdir(path, 0o700); err == nil {
			return nil
		} else if !os.IsExist(err) {
			return err
		}
		info, err = os.Lstat(path)
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%s must not be a symbolic link", description)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s must be a directory", description)
	}
	return nil
}

func ensureInstructionBoundary(root string) error {
	stateRoot := config.StateDir(root)
	stateName := filepath.Base(stateRoot)
	if err := ensureRealDirectory(stateRoot, stateName+" state path"); err != nil {
		return err
	}
	return ensureRealDirectory(filepath.Join(stateRoot, "instructions"), stateName+"/instructions path")
}

func Init(root string, force bool) error {
	abs, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return err
	}
	if err := ensureRealDirectory(filepath.Join(abs, ".werkrby"), ".werkrby state path"); err != nil {
		return err
	}
	configPath := config.PathForRoot(abs)
	if !force {
		if _, err := os.Stat(configPath); err == nil {
			return errors.New(".werkrby/config.yaml already exists (use --force to restore missing templates)")
		}
	}
	if err := ensureInstructionBoundary(abs); err != nil {
		return err
	}
	for _, dir := range directories {
		if dir == ".werkrby/instructions" {
			continue
		}
		if err := os.MkdirAll(filepath.Join(abs, filepath.FromSlash(dir)), 0o700); err != nil {
			return err
		}
	}
	if err := theme.InstallBuiltins(filepath.Join(abs, ".werkrby", "themes")); err != nil {
		return err
	}
	createdConfig := false
	for _, spec := range files {
		target := filepath.Join(abs, filepath.FromSlash(spec.Path))
		if force && spec.Path == config.FileName {
			if _, err := os.Stat(target); err == nil {
				continue
			}
		}
		if _, err := os.Stat(target); err == nil {
			continue
		}
		data, err := templates.ReadFile(spec.Template)
		if err != nil {
			return err
		}
		data = []byte(strings.ReplaceAll(string(data), "{{PROJECT_ROOT}}", filepath.ToSlash(abs)))
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			return err
		}
		mode := os.FileMode(0o600)
		if spec.Executable {
			mode = 0o700
		}
		if err := fsx.AtomicCreateFile(target, data, mode); err != nil {
			if os.IsExist(err) {
				continue
			}
			return err
		}
		if spec.Path == config.FileName {
			createdConfig = true
		}
	}
	if createdConfig {
		cfg, err := config.Load(configPath)
		if err != nil {
			return err
		}
		if detected, _, ok := detectCodingHarness(nil); ok {
			cfg.Harness.Name = detected.Name
		}
		if err := config.Save(cfg); err != nil {
			return err
		}
	}
	return nil
}

// Upgrade restores missing runtime directories and embedded support files. It
// preserves current configuration and user-owned prompts, instructions,
// themes, and extensions. Unused configuration keys disappear on the next save.
func Upgrade(root string) error {
	abs, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return err
	}
	if err := ensureInstructionBoundary(abs); err != nil {
		return err
	}
	stateRoot := config.StateDir(abs)
	for _, dir := range directories {
		rel := strings.TrimPrefix(dir, ".werkrby/")
		if rel == "instructions" {
			continue
		}
		if err := os.MkdirAll(filepath.Join(stateRoot, filepath.FromSlash(rel)), 0o700); err != nil {
			return err
		}
	}
	for _, spec := range files {
		if spec.Path == config.FileName {
			continue
		}
		rel := strings.TrimPrefix(spec.Path, ".werkrby/")
		target := filepath.Join(stateRoot, filepath.FromSlash(rel))
		if _, err := os.Stat(target); err == nil {
			continue
		} else if !os.IsNotExist(err) {
			return err
		}
		data, err := templates.ReadFile(spec.Template)
		if err != nil {
			return err
		}
		data = []byte(strings.ReplaceAll(string(data), "{{PROJECT_ROOT}}", filepath.ToSlash(abs)))
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			return err
		}
		mode := os.FileMode(0o600)
		if spec.Executable {
			mode = 0o700
		}
		if err := fsx.AtomicCreateFile(target, data, mode); err != nil {
			if os.IsExist(err) {
				continue
			}
			return err
		}
	}
	return nil
}

func Template(name string) ([]byte, error) {
	data, err := templates.ReadFile("templates/" + name)
	if err != nil {
		return nil, fmt.Errorf("embedded template %s: %w", name, err)
	}
	return data, nil
}
