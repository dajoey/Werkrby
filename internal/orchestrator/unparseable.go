package orchestrator

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/fsx"
)

// errUnreadableDocument marks a per-document parse/read failure. scanOnce must
// report that document and continue; only errors that are not about one durable
// file (lease dir, route dirs, config) may abort the scan.
var errUnreadableDocument = errors.New("unreadable durable document")

// skipUnreadableDocument reports the broken durable file and returns the
// sentinel that lets the scan continue with every other document.
func (m *Manager) skipUnreadableDocument(path string, err error) error {
	if err == nil {
		return nil
	}
	m.reportUnparseableDocument(path, err)
	return errUnreadableDocument
}

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

// unparseableGracePeriod: a document must stay unparseable this long before a
// repair task files (2026-10-19 token-cut audit: about half of the sampled
// repair tasks were no-ops - the document parsed again by the time a worker
// opened it). A document that parses on re-read is never escalated and loses
// its sighting.
const unparseableGracePeriod = 30 * time.Minute

// unparseableSightings records, per document path, when it was first seen
// unparseable. Durable across restarts in runtime/unparseable-sightings.json.
type unparseableSightings map[string]string // path -> first-seen RFC3339

var unparseableSightingsMu sync.Mutex

func (m *Manager) unparseableClock() time.Time {
	if m.unparseableNow != nil {
		return m.unparseableNow()
	}
	return time.Now()
}

func (m *Manager) unparseableSightingsPath() string {
	return m.Config.StatePath("runtime", "unparseable-sightings.json")
}

func (m *Manager) loadUnparseableSightings() unparseableSightings {
	data, err := os.ReadFile(m.unparseableSightingsPath())
	if err != nil {
		return unparseableSightings{}
	}
	sightings := unparseableSightings{}
	if json.Unmarshal(data, &sightings) != nil {
		return unparseableSightings{}
	}
	return sightings
}

func (m *Manager) saveUnparseableSightings(sightings unparseableSightings) {
	path := m.unparseableSightingsPath()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		m.log("cannot record unparseable sightings: " + err.Error())
		return
	}
	data, err := json.Marshal(sightings)
	if err != nil {
		return
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		m.log("cannot record unparseable sightings: " + err.Error())
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		m.log("cannot record unparseable sightings: " + err.Error())
	}
}

// unparseableSightedAt returns the recorded first sighting, creating one
// (within the grace) when the path is new.
func (m *Manager) unparseableSightedAt(path string) (time.Time, bool) {
	unparseableSightingsMu.Lock()
	defer unparseableSightingsMu.Unlock()
	sightings := m.loadUnparseableSightings()
	now := m.unparseableClock()
	if seen, ok := sightings[path]; ok {
		first, err := time.Parse(time.RFC3339, seen)
		if err == nil {
			return first, true
		}
	}
	sightings[path] = now.UTC().Format(time.RFC3339)
	m.saveUnparseableSightings(sightings)
	return now, false
}

func (m *Manager) forgetUnparseableDocument(path string) {
	unparseableSightingsMu.Lock()
	defer unparseableSightingsMu.Unlock()
	sightings := m.loadUnparseableSightings()
	if _, ok := sightings[path]; !ok {
		return
	}
	delete(sightings, path)
	m.saveUnparseableSightings(sightings)
}

// pruneUnparseableSightings drops sightings whose document now parses (it was
// repaired) or has vanished: a healed document never reaches the report path
// again, so its sighting would otherwise outlive the breakage forever.
func (m *Manager) pruneUnparseableSightings() {
	unparseableSightingsMu.Lock()
	defer unparseableSightingsMu.Unlock()
	sightings := m.loadUnparseableSightings()
	changed := false
	for path := range sightings {
		if _, err := ReadDocument(path); err == nil || errors.Is(err, fs.ErrNotExist) {
			delete(sightings, path)
			changed = true
		}
	}
	if changed {
		m.saveUnparseableSightings(sightings)
	}
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
	if errors.Is(parseErr, fs.ErrNotExist) {
		m.log("skipping vanished document " + path + ": " + parseErr.Error())
		return
	}
	message := "unparseable durable document " + path + ": " + parseErr.Error()
	if strings.HasPrefix(filepath.Base(path), unparseableRepairPrefix) {
		// Never escalate our own repair task into a second repair task.
		m.log(message)
		return
	}

	// Two-stage grace. A document that parses on re-read right now was a
	// transient (a partial or racing write): no task, sighting forgotten. A
	// document inside its first grace period gets one more scan cycle before
	// its breakage counts as persistent; only then does a repair task file.
	m.pruneUnparseableSightings()
	if _, err := ReadDocument(path); err == nil {
		m.forgetUnparseableDocument(path)
		m.log("unparseable document parses again on re-read; no repair task: " + path)
		return
	}
	if first, seen := m.unparseableSightedAt(path); !seen || m.unparseableClock().Sub(first) < unparseableGracePeriod {
		m.log(message + " (within the " + unparseableGracePeriod.String() + " repair grace; no task yet)")
		return
	}

	filed, err := m.fileUnparseableRepairTask(path, parseErr)
	switch {
	case err != nil:
		m.logUnparseable("unparseable_document", message+" (repair task not filed: "+err.Error()+")")
	case filed:
		m.forgetUnparseableDocument(path)
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

	document := unparseableRepairDocument(id, path, m.workspaceRelative(path), parseErr, time.Now().UTC(), m.repairNotifyOrigin())
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

// repairNotifyOrigin picks where a repair task's terminal notification goes. The
// notify block is only usable with an origin, so a repair filed without one could
// never reach anybody. With exactly one authorized numeric Telegram user that
// user's private chat is the origin; otherwise the local CLI conversation, which
// the CLI already uses as its default.
func (m *Manager) repairNotifyOrigin() string {
	telegram := m.Config.Channels.Telegram
	if telegram.Enabled && len(telegram.AllowedUsers) == 1 {
		id := config.NormalizeTelegramUser(telegram.AllowedUsers[0])
		if id != "" && strings.Trim(id, "0123456789") == "" {
			return "telegram/TG-" + id
		}
	}
	return "cli/local"
}

func unparseableRepairDocument(id, path, relative string, parseErr error, now time.Time, origin string) string {
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
    origin: %s
    "on":
        - done
        - failed
        - cancelled
---

# Repair unparseable front matter: %s

## Objective

Make `+"`%s`"+` parse again so Werkrby stops skipping it, changing nothing about the
work it describes.

## Context

Werkrby could not read this %s document's YAML front matter, so every scan skips it
and the work behind it does not advance. The document is at `+"`%s`"+` in this
workspace. Werkrby filed this repair itself and did not touch the corrupt file.
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

- %s - Filed by Werkrby: %s would not parse (%s), so it was being skipped in silence.
`, id, name, stamp, stamp, origin, name, name, kind, relative, detail, stamp, name, detail)
}
