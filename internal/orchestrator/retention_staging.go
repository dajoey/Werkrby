package orchestrator

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"path"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// The retention registry (tasks-20261029-retention-as-a-check-01) gives every folder a
// fleet process writes — the NAS share and Werkrby scratch — an owner and a retention
// rule. A task that stages bulk data declares those paths in a `staging:` front-matter
// list (path + optional delete_when); the completion gate below refuses `done` while a
// declared path still exists and no registry entry covers it. This replaces the prose
// rule "clean up your staging" with a check (class: prose-rules).
//
// The registry lives at $HOME/ops/retention-registry.yaml (WERKRBY_RETENTION_REGISTRY
// overrides it). A missing or unreadable registry fails closed with the exact reason:
// a task that declared staging cannot silently complete with the data unowned.

type retentionRegistryEntry struct {
	Path  string `yaml:"path"`
	Owner string `yaml:"owner"`
	Rule  string `yaml:"rule"`
}

type retentionRegistryFile struct {
	Entries      []retentionRegistryEntry `yaml:"entries"`
	StagingRoots []string                 `yaml:"staging_roots"`
}

func retentionRegistryPath() string {
	if override := strings.TrimSpace(os.Getenv("WERKRBY_RETENTION_REGISTRY")); override != "" {
		return override
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, "ops", "retention-registry.yaml")
}

func loadRetentionRegistry() (*retentionRegistryFile, error) {
	registryPath := retentionRegistryPath()
	if registryPath == "" {
		return nil, errors.New("no home directory is known")
	}
	data, err := os.ReadFile(registryPath)
	if err != nil {
		return nil, err
	}
	var registry retentionRegistryFile
	if err := yaml.Unmarshal(data, &registry); err != nil {
		return nil, err
	}
	if len(registry.Entries) == 0 {
		return nil, fmt.Errorf("%s carries no entries", registryPath)
	}
	return &registry, nil
}

func retentionRegistryCovers(entries []retentionRegistryEntry, target string) bool {
	cleanTarget := filepath.Clean(target)
	for _, entry := range entries {
		if strings.TrimSpace(entry.Path) == "" {
			continue
		}
		cleanEntry := filepath.Clean(entry.Path)
		if cleanEntry == cleanTarget {
			return true
		}
		if strings.HasPrefix(cleanTarget, cleanEntry+string(os.PathSeparator)) {
			return true
		}
		if ok, err := path.Match(filepath.ToSlash(cleanEntry), filepath.ToSlash(cleanTarget)); err == nil && ok {
			return true
		}
	}
	return false
}

type stagingEntry struct {
	Path       string
	DeleteWhen string
}

// parseStagingFrontMatter reads the optional `staging:` list: one mapping per staged
// path, `path` required, `delete_when` optional. Malformed values fail closed with the
// exact rule, the same contract as the completion-summary diagnostics.
func parseStagingFrontMatter(document Document) ([]stagingEntry, error) {
	raw, ok := document.FrontMatter["staging"]
	if !ok || raw == nil {
		return nil, nil
	}
	list, ok := raw.([]any)
	if !ok {
		return nil, errors.New("staging must be a YAML list of mappings, each with a path and an optional delete_when")
	}
	entries := make([]stagingEntry, 0, len(list))
	for i, item := range list {
		mapping, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("staging[%d] must be a mapping with a path and an optional delete_when", i)
		}
		stagingPath, _ := mapping["path"].(string)
		if strings.TrimSpace(stagingPath) == "" {
			return nil, fmt.Errorf("staging[%d] is missing its path", i)
		}
		deleteWhen, _ := mapping["delete_when"].(string)
		entries = append(entries, stagingEntry{Path: stagingPath, DeleteWhen: deleteWhen})
	}
	return entries, nil
}

// validateStagingCleanup is the completion gate: a task whose `staging:` list names a
// path that still exists cannot reach done unless a retention-registry entry covers
// that path (an owner and a rule keep it from outliving its purpose silently).
func validateStagingCleanup(document Document) error {
	entries, err := parseStagingFrontMatter(document)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	registry, registryErr := loadRetentionRegistry()
	if registryErr != nil {
		return fmt.Errorf("the retention registry is unreadable (%s), so the staging paths cannot be verified against it", registryErr)
	}
	for _, entry := range entries {
		_, statErr := os.Stat(entry.Path)
		if statErr != nil {
			if os.IsNotExist(statErr) {
				continue // staging is gone: the gate is satisfied
			}
			return fmt.Errorf("staging path %s cannot be checked: %v", entry.Path, statErr)
		}
		if retentionRegistryCovers(registry.Entries, entry.Path) {
			continue
		}
		detail := ""
		if strings.TrimSpace(entry.DeleteWhen) != "" {
			detail = fmt.Sprintf(" (promised delete_when %s)", entry.DeleteWhen)
		}
		return fmt.Errorf("staging %s still exists and no retention-registry entry covers it%s: delete it, or register it in the registry with an owner and a rule, before completing", entry.Path, detail)
	}
	return nil
}

// stagingPathPattern matches absolute paths under root that a brief names in prose.
func stagingPathPattern(root string) *regexp.Regexp {
	return regexp.MustCompile(regexp.QuoteMeta(root) + `/[^\s"'` + "`" + `\)\]},]+`)
}

// stagingDefaultsFromBody names the staged paths a brief already declares: every
// absolute path under a registry staging root. Task creation adds them as the default
// `staging:` list so the completion gate holds the task to what its own brief named.
// An unreadable registry means no default (creation is a convenience; the gate is the
// enforcement), and the scan is capped so a brief full of paths stays bounded.
func stagingDefaultsFromBody(body string) []map[string]any {
	if strings.TrimSpace(body) == "" {
		return nil
	}
	registry, err := loadRetentionRegistry()
	if err != nil {
		return nil
	}
	var defaults []map[string]any
	seen := map[string]bool{}
	for _, root := range registry.StagingRoots {
		root = strings.TrimRight(strings.TrimSpace(root), "/")
		if root == "" {
			continue
		}
		matches := stagingPathPattern(root).FindAllString(body, 16)
		for _, match := range matches {
			stagingPath := strings.TrimRight(match, ".,;:)")
			if seen[stagingPath] {
				continue
			}
			seen[stagingPath] = true
			defaults = append(defaults, map[string]any{"path": stagingPath})
			if len(defaults) >= 5 {
				return defaults
			}
		}
	}
	return defaults
}
