package orchestrator

import (
	"os"
	"path/filepath"
	"strings"
	"time"
)

// providerTurnLockStaleAge bounds how old an empty provider-turn lock must be before the sweep may
// remove it. A lock backs a claim for seconds; anything a day old belongs to a finished claim.
const providerTurnLockStaleAge = 24 * time.Hour

// SweepStaleProviderTurnLocks removes 0-byte provider-turn lock dotfiles older than
// providerTurnLockStaleAge from dir and returns how many it removed. providerTurnLockPath creates
// these files with O_CREATE and unlockProviderTurn only unlocks, never unlinks, so every document
// ever claimed left one behind forever (2,200+ empty dotfiles in one workspace by 2026-10-24). A
// lock backing an in-flight claim is younger than the stale age, so the sweep cannot take a lock a
// live claim still uses; a holder that does outlast the age keeps its flocked inode and is still
// excluded against, and the sweep runs once at process start before this instance dispatches.
func SweepStaleProviderTurnLocks(dir string) int {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	removed := 0
	cutoff := time.Now().Add(-providerTurnLockStaleAge)
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, ".") || !strings.HasSuffix(name, providerTurnLockSuffix) {
			continue
		}
		path := filepath.Join(dir, name)
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() || info.Size() != 0 || info.ModTime().After(cutoff) {
			continue
		}
		if os.Remove(path) == nil {
			removed++
		}
	}
	return removed
}
