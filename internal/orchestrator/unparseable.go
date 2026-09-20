package orchestrator

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"github.com/agent0ai/spynel/internal/fsx"
)

// unparseableRepairPrefix marks the repair tasks this file creates. A repair
// task that is itself unparseable must never produce another repair task.
const unparseableRepairPrefix = "tasks-unparseable-"

// unparseableRepairID builds a stable task id for one corrupt durable file so
// repeated scan cycles file exactly one repair task for it.
func unparseableRepairID(path string) string {
	base := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	var builder strings.Builder
	for _, symbol := range strings.ToLower(base) {
		if unicode.IsLetter(symbol) || unicode.IsDigit(symbol) || symbol == '-' {
			builder.WriteRune(symbol)
		} else {
			builder.WriteByte('-')
		}
	}
	slug := strings.Trim(builder.String(), "-")
	for strings.Contains(slug, "--") {
		slug = strings.ReplaceAll(slug, "--", "-")
	}
	if slug == "" {
		slug = "document"
	}
	if len(slug) > 80 {
		slug = strings.Trim(slug[:80], "-")
	}
	return unparseableRepairPrefix + slug
}

// reportUnparseableDocument turns a silent YAML skip into work somebody sees.
// Until 2026-09-19 an unreadable task or goal was written to the runtime log and
// nothing else: the document kept its status on disk, the board drew it healthy,
// and a goal sat inert for 1h53m. Mechanism, deliberately reusing what exists
// rather than adding a reminder queue: one deduplicated repair task in the
// ordinary todo queue (visible on the board, dispatched like any other work) plus
// one structured error record. Its terminal transition runs the ordinary
// notification agent, so a repair that cannot be made reaches Joey by the
// existing path. The corrupt document itself is never rewritten here.
func (m *Manager) reportUnparseableDocument(path string, parseErr error) {
	if parseErr == nil || path == "" {
		return
	}
	message := "unparseable durable document " + path + ": " + parseErr.Error()
	if strings.HasPrefix(filepath.Base(path), unparseableRepairPrefix) {
		// Never escalate our own repair task into a second repair task.
		m.log(message)
		return
	}

	filed, err := m.fileUnparseableRepairTask(path, parseErr)
	switch {
	case err != nil:
		m.logUnparseable("unparseable_document", message+" (repair task not filed: "+err.Error()+")")
	case filed:
		m.logUnparseable("unparseable_document", message)
	default:
		// Repair already filed; keep later scans out of the error record.
		m.log(message)
	}
}

func (m *Manager) logUnparseable(event, message string) {
	if m.LogError != nil {
		m.LogError("orchestrator", event, message)
		return
	}
	m.log(message)
}

// fileUnparseableRepairTask creates the repair task, reporting whether this call
// is the one that created it. It is a no-op when a repair task for the same
// document already exists in any status folder.
func (m *Manager) fileUnparseableRepairTask(path string, parseErr error) (bool, error) {
	id := unparseableRepairID(path)
	for _, status := range []string{"working", "waiting", "review", "reviewing", "done", "failed", "cancelled"} {
		if _, err := os.Stat(m.Config.StatePath("tasks", status, id+".md")); err == nil {
			return false, nil
		}
	}
	todoDir := m.Config.StatePath("tasks", "todo")
	if err := os.MkdirAll(todoDir, 0o700); err != nil {
		return false, err
	}
	taskPath := filepath.Join(todoDir, id+".md")
	if _, err := os.Stat(taskPath); err == nil {
		return false, nil
	}

	document := unparseableRepairDocument(id, path, m.workspaceRelative(path), parseErr, time.Now().UTC())
	if err := fsx.AtomicCreateFile(taskPath, []byte(document), 0o600); err != nil {
		if _, statErr := os.Stat(taskPath); statErr == nil {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// workspaceRelative keeps the repair task pointing at a workspace path rather
// than a host path, so the document stays portable and safe to show.
func (m *Manager) workspaceRelative(path string) string {
	if relative, err := filepath.Rel(m.Config.Root, path); err == nil && !strings.HasPrefix(relative, "..") {
		return filepath.ToSlash(relative)
	}
	return filepath.Base(path)
}

func unparseableRepairDocument(id, path, relative string, parseErr error, now time.Time) string {
	kind := "task"
	if strings.Contains(filepath.ToSlash(path), "/goals/") || strings.HasPrefix(filepath.Base(path), "goals-") {
		kind = "goal"
	}
	stamp := now.Format(time.RFC3339)
	name := filepath.Base(path)
	detail := strings.Join(strings.Fields(parseErr.Error()), " ")
	if runes := []rune(detail); len(runes) > 200 {
		detail = string(runes[:199]) + "…"
	}

	return fmt.Sprintf(`---
id: %s
title: "Repair unparseable front matter: %s"
status: todo
created_at: "%s"
updated_at: "%s"
review_required: false
notify:
    enabled: true
    "on":
        - done
        - failed
        - cancelled
---

# Repair unparseable front matter: %s

## Objective

Make `+"`%s`"+` parse again so Spynel stops skipping it, changing nothing about the
work it describes.

## Context

Spynel could not read this %s document's YAML front matter, so every scan skips it
and the work behind it does not advance. The document is at `+"`%s`"+` in this
workspace. Spynel filed this repair itself and did not touch the corrupt file.
Parse error:

    %s

## Scope

- Repair only the YAML front matter of that document. Keep every existing key, its
  value, the body and the whole `+"`## Progress`"+` log exactly as they are.
- Do not change `+"`status`"+`, `+"`attempt`"+`, `+"`provider_iterations`"+` or any other
  code-managed field, and do not move that document between status folders.
- If the front matter cannot be repaired without guessing at lost content, stop and
  fail this task with the evidence rather than inventing values.

## Acceptance criteria

- The document's front matter loads as a YAML mapping, and its required fields
  (`+"`id`"+`, `+"`title`"+`, `+"`status`"+`, `+"`created_at`"+`, `+"`updated_at`"+`,
  `+"`review_required`"+`) are present and unchanged in meaning.
- The diff touches front matter only; the body and progress log are byte-identical.

## Progress

- %s - Filed by Spynel: %s would not parse (%s), so it was being skipped in silence.
`, id, name, stamp, stamp, name, name, kind, relative, detail, stamp, name, detail)
}
