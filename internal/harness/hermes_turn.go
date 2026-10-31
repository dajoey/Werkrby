package harness

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	_ "github.com/ncruces/go-sqlite3/driver"
)

// A Hermes worker's turn runs inside the multiplexed root Hermes gateway, not in the pane herdr
// launched: herdr reports no agent_session for it and the pane writes no transcript. The turn's
// rows land in the launch profile's SessionDB instead (~/.hermes/profiles/<name>/state.db, or
// ~/.hermes/state.db for the root profile). readHermesTurn judges the turn from those rows the
// way extractTranscriptMessage judges a pi or Claude Code turn from its transcript; the screen
// scrape is the fallback when the ledger cannot be read.

const (
	// hermesFailedTurnKind is messages.display_kind of the boundary row Hermes appends when a turn
	// ended without an answer (agent/turn_failure_copy.py FAILED_TURN_DISPLAY_KIND).
	hermesFailedTurnKind = "failed_turn"
	// The boundary row's copy, for rows written before the closers typed them.
	hermesFailedTurnNotice  = "Your request was not processed. Send it again if you still want me to carry it out."
	hermesPartialTurnNotice = "This turn did not complete. Some actions may already have run; verify their effects before resending."
	// hermesCompactionPrefix opens the summary row context compression writes; it is never an answer.
	hermesCompactionPrefix = "[CONTEXT COMPACTION"

	// HermesUnresolvedTurn is the detail of the verdict judgeHermesTurn emits when a delivered
	// turn wrote rows but ended with neither an answer row nor a failed-turn boundary row inside
	// the settle and grace windows: the turn ran, the harness could not classify how it ended.
	// The orchestrator keys its do-not-strike rule (recordHermesUnresolvedTurn) on this text.
	HermesUnresolvedTurn = "hermes turn ended without a final reply"

	defaultHermesLedgerSettle = 5 * time.Second
	// defaultHermesFailureGrace bounds the re-read for a turn ending that lands after the settle
	// window: under the 2026-10-30 provider storm the gateway persisted the failed_turn row
	// 10.4s after the turn's last row, past the settle, and the judge's generic verdict then hid
	// a quota wall from the orchestrator's classifier. Only turns that already stalled pay it.
	defaultHermesFailureGrace = 10 * time.Second
	// HermesPromptNotInLedger is the turn error when herdr reported the turn finished, the
	// profile ledger is readable, and it never recorded this prompt: the screen can then only
	// hold earlier turns, so nothing on it is this turn's reply.
	HermesPromptNotInLedger = "hermes never recorded this prompt in its ledger"
	// defaultHermesDeliveryWait bounds the wait for a prompt's user row once herdr has called the
	// turn finished without it: on 2026-10-31 a 56 KB chat prompt's row landed 4 s after herdr
	// reported the warm chat pane idle, and its answer 2 min 16 s later.
	defaultHermesDeliveryWait = 60 * time.Second
	// defaultHermesInflightBudget bounds the wait for the ending of a turn whose prompt landed
	// (or whose first assistant row landed) after herdr had already called it finished.
	defaultHermesInflightBudget = 10 * time.Minute
	hermesLedgerPoll            = 250 * time.Millisecond
	hermesCompressionHops       = 8
)

var (
	// hermesProfileID mirrors hermes_constants.PROFILE_ID_RE; anything else is not a profile
	// directory and must never become a path component.
	hermesProfileID = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)
	// A stop gate (verify-on-stop, the pre_verify hook, the kanban guard) handed this answer back
	// for a rewrite: it is the rejected draft, never the reply (agent/turn_stop_gates.py).
	hermesGateContinueReasons = map[string]bool{
		"verify_hook_continue":     true,
		"verification_required":    true,
		"kanban_terminal_required": true,
	}
	// hermesErrorPanelTop is the top border of the corner-less panel (Rich box HORIZONTALS) the
	// CLI draws for a reply it did not stream, which on a streaming worker is a failed turn's
	// error copy. A streamed reply's box opens with a corner (╭) and never matches.
	hermesErrorPanelTop = regexp.MustCompile(`(?m)^[ \t]*─{1,3}[ \t]*[⚕☤][ \t]*Hermes\b[^\n]*$`)
	hermesPanelBottom   = regexp.MustCompile(`(?m)^[ \t]*─{20,}[ \t]*$`)
)

// hermesLedgerTurn is what the profile ledger says about one turn.
type hermesLedgerTurn struct {
	// Delivered: a user row carrying this prompt was written at or after the turn began.
	Delivered bool
	// SessionID is the session holding that row, or its compression continuation.
	SessionID string
	// Final is the turn's last accepted answer: an assistant row with text, no tool calls, and
	// no stop gate sending it back.
	Final string
	// Failure is the error Hermes recorded when the turn ended on its failed-turn boundary row.
	Failure string
	// SawAssistant: the turn wrote at least one assistant row (a tool step, a draft, an answer).
	SawAssistant bool
}

func (t hermesLedgerTurn) ended() bool { return t.Final != "" || t.Failure != "" }

// hermesHomeDir is the Hermes home the gateway and the pane client resolve: HERMES_HOME when set,
// else ~/.hermes. override (tests) wins.
func hermesHomeDir(override string) (string, error) {
	if override != "" {
		return override, nil
	}
	if env := strings.TrimSpace(os.Getenv("HERMES_HOME")); env != "" {
		if env == "~" || strings.HasPrefix(env, "~/") {
			home, err := os.UserHomeDir()
			if err != nil {
				return "", err
			}
			env = filepath.Join(home, strings.TrimPrefix(env, "~"))
		}
		return filepath.Clean(env), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".hermes"), nil
}

// hermesLaunchProfile reads the profile from a hermes launch command line the way
// hermes_cli/main.py does: -p NAME, --profile NAME or --profile=NAME, casefolded.
func hermesLaunchProfile(args []string) (name string, explicit bool, err error) {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			break
		}
		var raw string
		switch {
		case arg == "-p" || arg == "--profile":
			if i+1 >= len(args) {
				return "", true, fmt.Errorf("hermes launch args end at %s with no profile", arg)
			}
			raw = args[i+1]
		case strings.HasPrefix(arg, "--profile="):
			raw = strings.TrimPrefix(arg, "--profile=")
		default:
			continue
		}
		name = strings.ToLower(strings.TrimSpace(raw))
		if !hermesProfileID.MatchString(name) {
			return "", true, fmt.Errorf("hermes profile %q is not a profile id", raw)
		}
		return name, true, nil
	}
	return "", false, nil
}

// hermesLedgerPath is the state.db a turn launched with args is recorded in: the named profile's
// under <root>/profiles/<name>/, the root's for none or "default". Without -p, Hermes uses the
// sticky <root>/active_profile, or the profile HERMES_HOME itself names.
func hermesLedgerPath(home string, args []string) (string, error) {
	root := home
	if filepath.Base(filepath.Dir(home)) == "profiles" {
		root = filepath.Dir(filepath.Dir(home))
	}
	name, explicit, err := hermesLaunchProfile(args)
	if err != nil {
		return "", err
	}
	if !explicit {
		if home != root {
			return filepath.Join(home, "state.db"), nil
		}
		if data, err := os.ReadFile(filepath.Join(root, "active_profile")); err == nil {
			name = strings.ToLower(strings.TrimSpace(string(data)))
		}
	}
	if name == "" || name == "default" {
		return filepath.Join(root, "state.db"), nil
	}
	if !hermesProfileID.MatchString(name) {
		return "", fmt.Errorf("hermes profile %q is not a profile id", name)
	}
	return filepath.Join(root, "profiles", name, "state.db"), nil
}

// openHermesLedger opens a Hermes state.db read-only. mode=ro still reads the -wal file the
// gateway appends to; never immutable=1, which skips the WAL and serves a stale view.
func openHermesLedger(path string) (*sql.DB, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	escaped := strings.NewReplacer("%", "%25", "?", "%3f", "#", "%23").Replace(filepath.ToSlash(path))
	db, err := sql.Open("sqlite3", "file:"+escaped+"?mode=ro&_pragma=busy_timeout(2000)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

func hermesLedgerColumns(ctx context.Context, db *sql.DB, table string) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx, "SELECT name FROM pragma_table_info(?)", table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		cols[name] = true
	}
	return cols, rows.Err()
}

// hermesLedgerText folds whitespace so a prompt matches its stored row however the terminal
// paste and Hermes's trim reshaped line ends, tabs and runs of spaces.
func hermesLedgerText(s string) string { return strings.Join(strings.Fields(s), " ") }

func hermesHasToolCalls(raw string) bool {
	raw = strings.TrimSpace(raw)
	return raw != "" && raw != "[]" && raw != "null"
}

// hermesFailureDetail is the provider error a failed-turn row carries in display_metadata
// ({"error": ..., "error_surface": {"code": ...}}), so the orchestrator's quota and transient
// classifiers see the same words a pi turn error carries.
func hermesFailureDetail(content, metadata string) string {
	var meta struct {
		Error   string `json:"error"`
		Surface struct {
			Code string `json:"code"`
		} `json:"error_surface"`
	}
	detail := strings.TrimSpace(content)
	if json.Unmarshal([]byte(metadata), &meta) == nil {
		if e := strings.TrimSpace(meta.Error); e != "" {
			detail = e
		}
		if code := strings.TrimSpace(meta.Surface.Code); code != "" && !strings.Contains(detail, code) {
			detail += " [" + code + "]"
		}
	}
	if detail == "" {
		detail = "hermes recorded a failed turn"
	}
	return detail
}

// readHermesTurn reads the turn started at since for prompt from the ledger at path.
//
// The turn's user row is the newest user row written at or after since whose text contains the
// whole prompt. The whole prompt, not a head: every Werkrby task prompt (and every chat prompt)
// opens and closes with the same shared text, so a head would match a concurrent worker's turn
// on the same profile. The newest match is taken because context compression copies the row,
// with its timestamp, into a child session that carries the turn on; a parent that ended by
// compression with no copy is followed to its child. Rows older than since are never read.
func readHermesTurn(ctx context.Context, path, prompt string, since time.Time) (hermesLedgerTurn, error) {
	var turn hermesLedgerTurn
	needle := hermesLedgerText(prompt)
	if needle == "" {
		return turn, errors.New("hermes ledger: empty prompt")
	}
	db, err := openHermesLedger(path)
	if err != nil {
		return turn, err
	}
	defer db.Close()

	cols, err := hermesLedgerColumns(ctx, db, "messages")
	if err != nil {
		return turn, err
	}
	for _, c := range []string{"id", "session_id", "role", "content", "tool_calls", "timestamp"} {
		if !cols[c] {
			return turn, fmt.Errorf("hermes ledger %s: messages has no %s column", path, c)
		}
	}
	sinceEpoch := float64(since.UnixNano()) / 1e9

	// CROSS JOIN fixes sessions as the outer loop, so each probe is an index range on
	// idx_messages_session (session_id, timestamp) instead of a scan of every message's content.
	rows, err := db.QueryContext(ctx, `SELECT m.id, m.session_id, m.content FROM sessions s CROSS JOIN messages m ON m.session_id = s.id
		WHERE m.role = 'user' AND m.timestamp >= ? ORDER BY m.id`, sinceEpoch)
	if err != nil {
		return turn, err
	}
	var rowID int64
	var session string
	for rows.Next() {
		var id int64
		var sid string
		var content sql.NullString
		if err := rows.Scan(&id, &sid, &content); err != nil {
			rows.Close()
			return turn, err
		}
		if content.Valid && strings.Contains(hermesLedgerText(content.String), needle) {
			rowID, session = id, sid
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return turn, err
	}
	rows.Close()
	if session == "" {
		return turn, nil
	}
	turn.Delivered = true

	if scols, err := hermesLedgerColumns(ctx, db, "sessions"); err == nil && scols["end_reason"] && scols["parent_session_id"] {
		for hop := 0; hop < hermesCompressionHops; hop++ {
			var endReason sql.NullString
			if err := db.QueryRowContext(ctx, `SELECT end_reason FROM sessions WHERE id = ?`, session).Scan(&endReason); err != nil || endReason.String != "compression" {
				break
			}
			var child string
			if err := db.QueryRowContext(ctx, `SELECT id FROM sessions WHERE parent_session_id = ? ORDER BY started_at DESC LIMIT 1`, session).Scan(&child); err != nil {
				break
			}
			session, rowID = child, 0
		}
	}
	turn.SessionID = session

	optional := func(c string) string {
		if cols[c] {
			return c
		}
		return "NULL"
	}
	query := fmt.Sprintf(`SELECT role, content, tool_calls, %s, %s, %s, %s FROM messages
		WHERE session_id = ? AND id > ? AND timestamp >= ? ORDER BY id`,
		optional("finish_reason"), optional("display_kind"), optional("display_metadata"), optional("_compressed_summary"))
	rows, err = db.QueryContext(ctx, query, session, rowID, sinceEpoch)
	if err != nil {
		return turn, err
	}
	defer rows.Close()
	for rows.Next() {
		var role string
		var content, toolCalls, finishReason, displayKind, displayMetadata sql.NullString
		var summary sql.NullInt64
		if err := rows.Scan(&role, &content, &toolCalls, &finishReason, &displayKind, &displayMetadata, &summary); err != nil {
			return turn, err
		}
		if role != "assistant" {
			continue
		}
		text := strings.TrimSpace(content.String)
		kind := strings.TrimSpace(displayKind.String)
		switch {
		case summary.Int64 != 0 || strings.HasPrefix(text, hermesCompactionPrefix):
			continue
		case kind == hermesFailedTurnKind || (kind == "" && (text == hermesFailedTurnNotice || text == hermesPartialTurnNotice)):
			turn.SawAssistant = true
			turn.Final, turn.Failure = "", hermesFailureDetail(text, displayMetadata.String)
		case kind != "":
			// Display-only rows (hidden, notifications) are not the model's reply.
			continue
		case hermesHasToolCalls(toolCalls.String), hermesGateContinueReasons[strings.TrimSpace(finishReason.String)]:
			// A tool step's interim text, or a draft a stop gate sent back: the turn went on.
			turn.SawAssistant = true
			turn.Final, turn.Failure = "", ""
		default:
			turn.SawAssistant = true
			turn.Final, turn.Failure = text, ""
		}
	}
	return turn, rows.Err()
}

// pollHermesTurn re-reads the turn at path until it ends, is not worth waiting for (an error,
// or never delivered), or budget runs out; budget <= 0 reads once.
func pollHermesTurn(ctx context.Context, path, prompt string, since time.Time, budget time.Duration) (hermesLedgerTurn, error) {
	poll := hermesLedgerPoll
	if budget > 0 && budget < poll {
		poll = budget
	}
	deadline := time.Now().Add(budget)
	for {
		turn, err := readHermesTurn(ctx, path, prompt, since)
		if err != nil || !turn.Delivered || turn.ended() || budget <= 0 || !time.Now().Before(deadline) {
			return turn, err
		}
		select {
		case <-ctx.Done():
			return turn, ctx.Err()
		case <-time.After(poll):
		}
	}
}

func (h *Herdr) hermesLedgerFile(launchArgs []string) (string, error) {
	home, err := hermesHomeDir(h.hermesHome)
	if err != nil {
		return "", err
	}
	return hermesLedgerPath(home, launchArgs)
}

// readHermesLedger resolves the launch profile's ledger and reads the turn. A delivered turn
// that has not yet written its answer or failure row is re-read for a short settle window: the
// gateway persists the rows before the pane returns to its composer, so this only covers a
// slow flush.
func (h *Herdr) readHermesLedger(ctx context.Context, launchArgs []string, prompt string, since time.Time) (hermesLedgerTurn, error) {
	path, err := h.hermesLedgerFile(launchArgs)
	if err != nil {
		return hermesLedgerTurn{}, err
	}
	settle := h.hermesLedgerSettle
	if settle == 0 {
		settle = defaultHermesLedgerSettle
	}
	return pollHermesTurn(ctx, path, prompt, since, settle)
}

// readHermesLateEnding re-reads a delivered turn for its ending after the settle window closed:
// the gateway can persist the failed_turn boundary row (or a late answer) long after the pane
// went quiet — 10.4s under the 2026-10-30 provider storm — and the generic verdict an expired
// settle emits hides the row's real error from the orchestrator's quota classifier. Bounded;
// the caller falls back to the generic verdict when it expires with no ending.
func (h *Herdr) readHermesLateEnding(ctx context.Context, launchArgs []string, prompt string, since time.Time) (hermesLedgerTurn, error) {
	path, err := h.hermesLedgerFile(launchArgs)
	if err != nil {
		return hermesLedgerTurn{}, err
	}
	grace := h.hermesFailureGrace
	if grace == 0 {
		grace = defaultHermesFailureGrace
	}
	return pollHermesTurn(ctx, path, prompt, since, grace)
}

// readHermesInflightTurn waits on the ledger for a turn herdr called finished before the turn
// had started: first for the prompt's user row (bounded by the delivery wait), then for the
// turn's ending (bounded by the in-flight budget). Herdr's idle is a screen reading; a turn
// that has not written a single assistant row has not run yet, whatever the pane shows.
func (h *Herdr) readHermesInflightTurn(ctx context.Context, launchArgs []string, prompt string, since time.Time) (hermesLedgerTurn, error) {
	path, err := h.hermesLedgerFile(launchArgs)
	if err != nil {
		return hermesLedgerTurn{}, err
	}
	deliveryWait := h.hermesDeliveryWait
	if deliveryWait == 0 {
		deliveryWait = defaultHermesDeliveryWait
	}
	budget := h.hermesInflightBudget
	if budget == 0 {
		budget = defaultHermesInflightBudget
	}
	poll := hermesLedgerPoll
	deliverBy := time.Now().Add(deliveryWait)
	var endBy time.Time
	for {
		turn, err := readHermesTurn(ctx, path, prompt, since)
		if err != nil || turn.ended() {
			return turn, err
		}
		now := time.Now()
		if turn.Delivered && endBy.IsZero() {
			endBy = now.Add(budget)
		}
		if (!turn.Delivered && !now.Before(deliverBy)) || (turn.Delivered && !now.Before(endBy)) {
			return turn, nil
		}
		select {
		case <-ctx.Done():
			return turn, ctx.Err()
		case <-time.After(poll):
		}
	}
}

// hermesNewestTurnTail is the screen text the newest turn drew after its last reply box: after
// the last 40-wide turn rule when a rule follows the last box (the newest turn drew no box), else
// after the last box, else after the last rule.
func hermesNewestTurnTail(screen string) string {
	start := 0
	if boxes := hermesBoxPattern.FindAllStringIndex(screen, -1); len(boxes) > 0 {
		start = boxes[len(boxes)-1][1]
	}
	if rules := hermesTurnRule.FindAllStringIndex(screen[start:], -1); len(rules) > 0 {
		start += rules[len(rules)-1][1]
	}
	return screen[start:]
}

// hermesScreenTurnError reports the newest turn ending in Hermes's corner-less panel after its
// last reply box: on a streaming worker that is the failed turn's error copy, never a reply.
func hermesScreenTurnError(screen string) (string, bool) {
	tail := hermesNewestTurnTail(screen)
	loc := hermesErrorPanelTop.FindStringIndex(tail)
	if loc == nil {
		return "", false
	}
	body := tail[loc[1]:]
	if end := hermesPanelBottom.FindStringIndex(body); end != nil {
		body = body[:end[0]]
	}
	var lines []string
	for _, line := range strings.Split(body, "\n") {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			lines = append(lines, trimmed)
		}
	}
	if len(lines) == 0 {
		return "hermes drew an error panel with no text", true
	}
	return strings.Join(lines, " "), true
}

// hermesScreenShowsReply: Hermes drew a reply box or a panel, so a prompt reached it.
func hermesScreenShowsReply(screen string) bool {
	return hermesBoxPattern.MatchString(screen) || hermesErrorPanelTop.MatchString(screen)
}

// hermesDropPromptPreview removes the "● <prompt>" preview Hermes prints between a submitted
// message's turn rules; it is the prompt, not the reply.
func hermesDropPromptPreview(text string) string {
	lines := strings.Split(text, "\n")
	kept := lines[:0]
	for _, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "● ") {
			continue
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, "\n")
}

// hermesVerdict is what Werkrby learned about a finished Hermes turn.
type hermesVerdict struct {
	// delivered: the ledger holds this turn's user row, or the screen shows a Hermes reply.
	delivered bool
	// final is the ledger's answer; "" leaves the reply to the screen scrape.
	final string
	// ledger: the profile ledger was readable and judged this turn. Its verdict is final; the
	// screen never supplies this turn's reply.
	ledger bool
	// err: the turn failed. It is returned as the turn error and never delivered as a reply.
	err error
}

func hermesTurnFailed(detail string) error { return fmt.Errorf("runner turn failed: %s", detail) }

func (h *Herdr) readWorkerScreen(target string) string {
	out, _ := exec.Command(h.config.Command, "agent", "read", target, "--source", "recent-unwrapped", "--lines", "250").Output()
	return string(out)
}

// judgeHermesTurn decides a Hermes turn that herdr reports finished. The ledger decides when it
// holds the turn: its answer is the reply (a gated draft is superseded by its rewrite), and a
// failed-turn row, or a turn that wrote rows but no answer, is a turn error. Otherwise the screen
// decides: a newest turn ending in the error panel is a turn error, and a reply box or panel on
// screen is evidence the prompt landed.
func (h *Herdr) judgeHermesTurn(ctx context.Context, target string, launchArgs []string, prompt string, since time.Time) hermesVerdict {
	turn, err := h.readHermesLedger(ctx, launchArgs, prompt, since)
	if err == nil && !turn.ended() && !turn.SawAssistant {
		// Herdr called the turn finished, but the ledger holds no row of it past the prompt (or
		// not even the prompt): the turn has not run yet. Wait on the ledger, never the screen.
		turn, err = h.readHermesInflightTurn(ctx, launchArgs, prompt, since)
		if ctx.Err() != nil {
			return hermesVerdict{err: ctx.Err()}
		}
		if err == nil && !turn.Delivered {
			// Never delivered: the caller's new-pane re-send may still run; nothing on the
			// screen is this turn's reply.
			return hermesVerdict{ledger: true}
		}
	}
	if err == nil && turn.Delivered {
		v := hermesVerdict{delivered: true, ledger: true}
		switch {
		case turn.Final != "":
			v.final = turn.Final
		case turn.Failure != "":
			v.err = hermesTurnFailed(turn.Failure)
		default:
			if detail, ok := hermesScreenTurnError(h.readWorkerScreen(target)); ok {
				v.err = hermesTurnFailed(detail)
			} else if turn.SawAssistant {
				// The ending row can land past the settle window (a provider storm
				// persisted one 10.4s after the turn's last row): one bounded re-read
				// before the generic verdict. A landed failure carries the real error
				// (quota stays quota); a landed answer is the reply.
				if late, err := h.readHermesLateEnding(ctx, launchArgs, prompt, since); err == nil {
					switch {
					case late.Final != "":
						v.final = late.Final
					case late.Failure != "":
						v.err = hermesTurnFailed(late.Failure)
					}
				}
				if v.final == "" && v.err == nil {
					v.err = hermesTurnFailed(HermesUnresolvedTurn)
				}
			} else {
				// Delivered, but not one assistant row inside the in-flight budget.
				v.err = hermesTurnFailed(HermesUnresolvedTurn)
			}
		}
		return v
	}
	screen := h.readWorkerScreen(target)
	v := hermesVerdict{delivered: hermesScreenShowsReply(screen)}
	if detail, ok := hermesScreenTurnError(screen); ok {
		v.delivered = true
		v.err = hermesTurnFailed(detail)
	}
	return v
}
