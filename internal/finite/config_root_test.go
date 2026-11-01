package finite

import (
	"os"
	"path/filepath"
	"testing"
)

func mkdirAllForTest(t *testing.T, paths ...string) {
	t.Helper()
	for _, path := range paths {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
}

func symlinkForTest(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks are unavailable: %v", err)
	}
}

// The disposable state root is checked after symlink resolution: it must not
// be, sit inside, or contain a legacy .werkrby/.spynel tree, and it must be
// disjoint from the worker directory.
func TestFiniteStateRootBoundaryResolvesSymlinks(t *testing.T) {
	base := t.TempDir()
	workspace := filepath.Join(base, "werkrby")
	legacy := filepath.Join(workspace, ".werkrby")
	spynelWorkspace := filepath.Join(base, "spynel-ws")
	spynel := filepath.Join(spynelWorkspace, ".spynel")
	worker := filepath.Join(base, "worker")
	existingRoot := filepath.Join(base, "existing state")
	mkdirAllForTest(t, legacy, spynel, worker, existingRoot)
	symlinkForTest(t, legacy, filepath.Join(base, "innocent"))
	symlinkForTest(t, spynelWorkspace, filepath.Join(base, "innocent-spynel-ws"))
	symlinkForTest(t, worker, filepath.Join(base, "alias"))
	symlinkForTest(t, filepath.Join(existingRoot, "inner"), filepath.Join(base, "worker-into-root"))
	mkdirAllForTest(t, filepath.Join(existingRoot, "inner"))

	rejectedRoots := map[string]string{
		"symlink_to_legacy_werkrby":       filepath.Join(base, "innocent"),
		"inside_legacy_via_symlink":       filepath.Join(base, "innocent", "state"),
		"ancestor_of_legacy_werkrby":      workspace,
		"legacy_spynel_component":         filepath.Join(base, "x", ".spynel", "state"),
		"inside_spynel_via_symlink":       filepath.Join(base, "innocent-spynel-ws", ".spynel"),
		"ancestor_of_legacy_spynel":       spynelWorkspace,
		"ancestor_of_spynel_via_symlink":  filepath.Join(base, "innocent-spynel-ws"),
		"legacy_werkrby_case_insensitive": filepath.Join(base, "y", ".WERKRBY"),
	}
	for name, root := range rejectedRoots {
		t.Run("cli_"+name, func(t *testing.T) {
			if path, err := cleanCLIDisposableRoot(root); err == nil {
				t.Fatalf("legacy-shaped root %q was accepted as %q", root, path)
			}
		})
		t.Run("config_"+name, func(t *testing.T) {
			if err := (Config{Root: root, WorkerDir: worker}).Validate(); err == nil {
				t.Fatalf("legacy-shaped root %q was accepted", root)
			}
		})
	}

	rejectedConfigs := map[string]Config{
		"root_inside_worker_via_symlink": {Root: filepath.Join(base, "alias", "state"), WorkerDir: worker},
		"root_equals_worker_via_symlink": {Root: filepath.Join(base, "alias"), WorkerDir: worker},
		"worker_inside_root_via_symlink": {Root: existingRoot, WorkerDir: filepath.Join(base, "worker-into-root")},
		"root_contains_worker":           {Root: base, WorkerDir: worker},
	}
	for name, cfg := range rejectedConfigs {
		t.Run("config_"+name, func(t *testing.T) {
			if err := cfg.Validate(); err == nil {
				t.Fatalf("overlapping config was accepted: %+v", cfg)
			}
		})
	}

	accepted := map[string]Config{
		"fresh_sibling":    {Root: filepath.Join(base, "fresh state", "nested"), WorkerDir: worker},
		"existing_sibling": {Root: existingRoot, WorkerDir: worker},
	}
	for name, cfg := range accepted {
		t.Run("accepted_"+name, func(t *testing.T) {
			if err := cfg.Validate(); err != nil {
				t.Fatalf("disposable config was rejected: %+v: %v", cfg, err)
			}
			if _, err := cleanCLIDisposableRoot(cfg.Root); err != nil {
				t.Fatalf("disposable CLI root was rejected: %v", err)
			}
		})
	}
}
