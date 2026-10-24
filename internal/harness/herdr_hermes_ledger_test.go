package harness

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/ncruces/go-sqlite3/driver"

	"github.com/agent0ai/spynel/internal/core"
)

// hermesTestLedgerSchema is the subset of Hermes's SessionDB (state.db) the ledger reader
// touches, in the live column order: timestamp comes after content, as in
// ~/.hermes/profiles/wk-code/state.db.
const hermesTestLedgerSchema = `
CREATE TABLE sessions (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL DEFAULT 'local',
    parent_session_id TEXT,
    started_at REAL NOT NULL,
    ended_at REAL,
    end_reason TEXT,
    cwd TEXT,
    profile_name TEXT
);
CREATE TABLE messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL REFERENCES sessions(id),
    role TEXT NOT NULL,
    content TEXT,
    tool_call_id TEXT,
    tool_calls TEXT,
    tool_name TEXT,
    effect_disposition TEXT,
    timestamp REAL NOT NULL,
    token_count INTEGER,
    finish_reason TEXT,
    reasoning TEXT,
    _compressed_summary INTEGER NOT NULL DEFAULT 0,
    active INTEGER NOT NULL DEFAULT 1,
    compacted INTEGER NOT NULL DEFAULT 0,
    display_kind TEXT,
    display_metadata TEXT
);
CREATE INDEX idx_messages_session ON messages(session_id, timestamp);
CREATE INDEX idx_messages_session_id ON messages(session_id, id);
`

// hermesTestLedger is a Hermes state.db written the way the gateway writes it: WAL mode, with
// the writer connection held open and automatic checkpoints off, so every row the reader sees
// comes out of the -wal file.
type hermesTestLedger struct {
	t    *testing.T
	path string
	db   *sql.DB
}

func newHermesTestLedger(t *testing.T, path string) *hermesTestLedger {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", "file:"+filepath.ToSlash(path)+"?_pragma=journal_mode(wal)&_pragma=wal_autocheckpoint(0)&_pragma=busy_timeout(2000)")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(hermesTestLedgerSchema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &hermesTestLedger{t: t, path: path, db: db}
}

type hermesTestRow struct {
	role            string
	content         string
	toolCalls       string
	finishReason    string
	displayKind     string
	displayMetadata string
	summary         bool
	at              time.Time // zero: now
}

func hermesNullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func hermesEpoch(at time.Time) float64 { return float64(at.UnixNano()) / 1e9 }

func (l *hermesTestLedger) session(id, parent, endReason string, started time.Time) {
	l.t.Helper()
	var ended any
	if endReason != "" {
		ended = hermesEpoch(time.Now())
	}
	if _, err := l.db.Exec(`INSERT INTO sessions (id, parent_session_id, started_at, ended_at, end_reason, cwd) VALUES (?, ?, ?, ?, ?, ?)`,
		id, hermesNullable(parent), hermesEpoch(started), ended, hermesNullable(endReason), "/home/dajoey/aibs/werkrby"); err != nil {
		l.t.Fatal(err)
	}
}

func (l *hermesTestLedger) add(session string, rows ...hermesTestRow) {
	l.t.Helper()
	for _, r := range rows {
		at := r.at
		if at.IsZero() {
			at = time.Now()
		}
		summary := 0
		if r.summary {
			summary = 1
		}
		if _, err := l.db.Exec(`INSERT INTO messages (session_id, role, content, tool_calls, timestamp, finish_reason, _compressed_summary, display_kind, display_metadata) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			session, r.role, r.content, hermesNullable(r.toolCalls), hermesEpoch(at), hermesNullable(r.finishReason), summary, hermesNullable(r.displayKind), hermesNullable(r.displayMetadata)); err != nil {
			l.t.Fatal(err)
		}
	}
}

// hermesFakeHerdr writes a herdr stand-in for a Hermes worker: `agent get` reports a hermes
// agent with no agent_session (herdr never learns one for a gateway-run Hermes turn), `agent
// read` serves dir/screen.txt, `agent list` serves dir/agents.json, `agent start` appends its
// arguments as one line to dir/starts.txt, and `agent prompt` records
// the prompt as dir/prompts/<n> and returns only after the simulator acks it in dir/acks/<n>, so
// the simulator's ledger rows are written while the turn is in flight, as Hermes writes them.
func hermesFakeHerdr(t *testing.T, dir string) string {
	t.Helper()
	for _, sub := range []string{"prompts", "acks"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	script := filepath.Join(dir, "fake-herdr")
	content := fmt.Sprintf(`#!/usr/bin/env bash
dir=%q
cmd="$1"
sub="$2"
shift 2
case "$cmd" in
workspace)
	echo '{"result":{"workspaces":[{"workspace_id":"w1","label":"[sj-worker] test"}]}}'
	;;
agent)
	case "$sub" in
	list)
		if [ -f "$dir/agents.json" ]; then cat "$dir/agents.json"; else echo '{"result":{"agents":[]}}'; fi
		;;
	get)
		echo '{"result":{"agent":{"agent":"hermes","agent_status":"idle","agent_session":{"value":""}}}}'
		;;
	start)
		printf '%%s\n' "$*" >> "$dir/starts.txt"
		echo '{"result":{}}'
		;;
	prompt)
		n=$(( $(ls "$dir/prompts" | wc -l) + 1 ))
		printf '%%s' "$2" > "$dir/prompts/.tmp-$n" && mv "$dir/prompts/.tmp-$n" "$dir/prompts/$n"
		for i in $(seq 1 1000); do [ -f "$dir/acks/$n" ] && break; sleep 0.01; done
		echo '{"result":{}}'
		;;
	read)
		cat "$dir/screen.txt" 2>/dev/null
		;;
	*)
		echo '{"result":{}}'
		;;
	esac
	;;
tab)
	case "$sub" in
	create)
		echo '{"result":{"tab":{"tab_id":"tab-123"},"root_pane":{"pane_id":"w1:p456"}}}'
		;;
	get)
		echo '{"result":{"tab":{"focused":false}}}'
		;;
	close)
		echo "tab-closed" >> "$dir/events.txt"
		echo '{"result":{}}'
		;;
	esac
	;;
pane)
	echo '{"result":{}}'
	;;
esac
`, dir)
	if err := os.WriteFile(script, []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
	return script
}

// startHermesSim plays Hermes for the fake herdr: for each prompt herdr types, onPrompt writes
// that turn's ledger rows, then the prompt is acked so `agent prompt --wait` returns.
func startHermesSim(t *testing.T, dir string, onPrompt func(n int, prompt string)) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	t.Cleanup(func() { cancel(); <-done })
	go func() {
		defer close(done)
		for n := 1; ; n++ {
			path := filepath.Join(dir, "prompts", fmt.Sprint(n))
			var data []byte
			for {
				var err error
				if data, err = os.ReadFile(path); err == nil {
					break
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Millisecond):
				}
			}
			if onPrompt != nil {
				onPrompt(n, string(data))
			}
			_ = os.WriteFile(filepath.Join(dir, "acks", fmt.Sprint(n)), nil, 0o644)
		}
	}()
}

func hermesPromptsSent(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, "prompts"))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), ".") {
			n++
		}
	}
	return n
}

func writeHermesScreen(t *testing.T, dir, screen string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "screen.txt"), []byte(screen), 0o644); err != nil {
		t.Fatal(err)
	}
}

// writeWarmHermesWorker registers an existing idle hermes worker for key, so the turn reuses
// it instead of spawning a pane (a warm chat pane).
func writeWarmHermesWorker(t *testing.T, dir, key string) {
	t.Helper()
	name, _ := workerNameAndLabel(key, "hermes")
	agents := fmt.Sprintf(`{"result":{"agents":[{"name":%q,"agent":"hermes","agent_status":"idle","pane_id":"w1:p9","tab_id":"tab-9","workspace_id":"w1","agent_session":{"value":""}}]}}`, name)
	if err := os.WriteFile(filepath.Join(dir, "agents.json"), []byte(agents), 0o644); err != nil {
		t.Fatal(err)
	}
}

type hermesTurnResult struct {
	err    error
	events []core.Event
}

func (r hermesTurnResult) finals() []string {
	var out []string
	for _, ev := range r.events {
		if ev.Kind == core.EventFinal {
			out = append(out, ev.Text)
		}
	}
	return out
}

func (r hermesTurnResult) mentions(text string) bool {
	for _, ev := range r.events {
		if strings.Contains(ev.Text, text) {
			return true
		}
	}
	return r.err != nil && strings.Contains(r.err.Error(), text)
}

func runHermesTurn(t *testing.T, dir, script, key, model, prompt string) hermesTurnResult {
	t.Helper()
	return runHermesTurnConfigured(t, dir, script, "hermes", key, model, prompt)
}

// runHermesTurnConfigured is runHermesTurn with the harness's configured model (harness.model)
// set to configModel instead of "hermes".
func runHermesTurnConfigured(t *testing.T, dir, script, configModel, key, model, prompt string) hermesTurnResult {
	t.Helper()
	h, err := NewHerdr(HarnessConfig{
		Name:         "herdr",
		Command:      script,
		Cwd:          dir,
		Model:        configModel,
		SessionsFile: filepath.Join(dir, "sessions.json"),
	})
	if err != nil {
		t.Fatal(err)
	}
	h.SetIdleStability(3, 10*time.Millisecond)
	h.SetComposerWaitTimeout(20 * time.Millisecond)
	h.hermesLedgerSettle = 600 * time.Millisecond
	var mu sync.Mutex
	var events []core.Event
	emit := func(e core.Event) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if model == "" {
		_, _, err = h.Send(ctx, key, prompt, emit)
	} else {
		_, _, err = h.SendWithModel(ctx, key, prompt, model, emit)
	}
	// Let the deferred tab cleanup of a successful orchestrator turn finish before TempDir removal.
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	return hermesTurnResult{err: err, events: append([]core.Event(nil), events...)}
}

// hermesTestTaskPrompt mimics a Werkrby task prompt: a long preamble every task shares, the
// claimed task path, then the shared persistent instructions.
func hermesTestTaskPrompt(task string) string {
	return "You are running from the Werkrby orchestrator system.\n\nWhen Werkrby-specific behavior is missing or may be stale, query the docs.\n\n" +
		"Open the claimed task file at `/home/dajoey/aibs/werkrby/.werkrby/tasks/working/" + task + ".md`.\n\n" +
		"<workspace_owner_persistent_instructions>\n- Never pin a version.\n</workspace_owner_persistent_instructions>\n"
}

// A Hermes worker on a new pane: herdr reports no agent_session, so before the ledger reader
// the guard re-sent the whole prompt and the turn ended prompt_not_delivered (three identical
// user rows in wk-code/state.db for one e2e task). The ledger's user row proves delivery.
func TestHerdrHermesNewPaneLedgerProvesDeliverySendsPromptOnce(t *testing.T) {
	dir := t.TempDir()
	hermesHome := filepath.Join(dir, "hermes-home")
	t.Setenv("HERMES_HOME", hermesHome)
	ledger := newHermesTestLedger(t, hermesWorkerLedger(hermesHome))
	ledger.session("local-a", "", "", time.Now())
	script := hermesFakeHerdr(t, dir)
	writeHermesScreen(t, dir, hermesTestTurn("You are running from the Werkrby orchestrator system.", hermesTestBox("Task moved to review.\nEvidence in the task file."))+hermesTestChrome)
	startHermesSim(t, dir, func(n int, prompt string) {
		ledger.add("local-a",
			hermesTestRow{role: "user", content: prompt},
			hermesTestRow{role: "assistant", content: "Reading the task.", toolCalls: `[{"id":"c1","function":{"name":"read_file"}}]`, finishReason: "tool_calls"},
			hermesTestRow{role: "tool", content: `{"content": "..."}`},
			hermesTestRow{role: "assistant", content: "Task moved to review.\nEvidence in the task file.", finishReason: "stop"},
		)
	})

	got := runHermesTurn(t, dir, script, "orchestrator:tasks:task_implementation:tasks-20261024-hermes-e2e-check-01:1", "", hermesTestTaskPrompt("tasks-20261024-hermes-e2e-check-01"))

	if sent := hermesPromptsSent(t, dir); sent != 1 {
		t.Fatalf("prompt sent %d times, want exactly 1 (the ledger already holds this turn's user row)", sent)
	}
	if got.err != nil {
		t.Fatalf("Send returned %v, want success", got.err)
	}
	if got.mentions("prompt_not_delivered") {
		t.Fatalf("turn reported prompt_not_delivered although the ledger recorded the prompt: %+v", got.events)
	}
	if finals := got.finals(); len(finals) != 1 || finals[0] != "Task moved to review.\nEvidence in the task file." {
		t.Fatalf("EventFinal texts = %q, want the ledger's final answer", finals)
	}
}

// The first chat message on a new Hermes pane got Hermes's answer twice and then an error.
func TestHerdrHermesNewChatPaneFirstMessageGetsTheReply(t *testing.T) {
	dir := t.TempDir()
	hermesHome := filepath.Join(dir, "hermes-home")
	t.Setenv("HERMES_HOME", hermesHome)
	ledger := newHermesTestLedger(t, hermesWorkerLedger(hermesHome))
	ledger.session("local-chat", "", "", time.Now())
	script := hermesFakeHerdr(t, dir)
	writeHermesScreen(t, dir, hermesTestTurn("Werkrby chat", hermesTestBox("The dentist card wakes on January 14."))+hermesTestChrome)
	startHermesSim(t, dir, func(n int, prompt string) {
		ledger.add("local-chat",
			hermesTestRow{role: "user", content: prompt},
			hermesTestRow{role: "assistant", content: "The dentist card wakes on January 14.", finishReason: "stop"},
		)
	})

	got := runHermesTurn(t, dir, script, "chat:telegram:4242", "", "Werkrby chat for telegram/4242.\n\nJoey: when is the dentist?")

	if sent := hermesPromptsSent(t, dir); sent != 1 {
		t.Fatalf("chat prompt sent %d times, want 1", sent)
	}
	if got.err != nil {
		t.Fatalf("Send returned %v, want the reply", got.err)
	}
	if finals := got.finals(); len(finals) != 1 || finals[0] != "The dentist card wakes on January 14." {
		t.Fatalf("EventFinal texts = %q, want the reply", finals)
	}
}

// The chat gate blocked a draft (task id plus a Routed: line) and the model rewrote it. Hermes
// streams both into ONE reply box, so the screen scrape delivered draft and rewrite glued
// together. The ledger keeps them as two rows: the draft (finish_reason verify_hook_continue)
// and the rewrite. The rewrite is the reply.
func TestHerdrHermesChatGateRewriteIsTheReplyNotTheScreenBox(t *testing.T) {
	dir := t.TempDir()
	hermesHome := filepath.Join(dir, "hermes-home")
	t.Setenv("HERMES_HOME", hermesHome)
	ledger := newHermesTestLedger(t, hermesWorkerLedger(hermesHome))
	ledger.session("local-chat", "", "", time.Now().Add(-time.Hour))
	// An earlier turn of the same warm conversation.
	ledger.add("local-chat",
		hermesTestRow{role: "user", content: "earlier question", at: time.Now().Add(-time.Hour)},
		hermesTestRow{role: "assistant", content: "Earlier answer.", finishReason: "stop", at: time.Now().Add(-time.Hour)},
	)
	key := "chat:telegram:4242"
	writeWarmHermesWorker(t, dir, key)
	script := hermesFakeHerdr(t, dir)
	// What Hermes actually draws for a gated turn: one box, the rewrite glued to the draft.
	writeHermesScreen(t, dir, hermesTestTurn("Earlier question", hermesTestBox("Earlier answer."))+
		hermesTestTurn("Fix the dashboard", hermesTestBox("Queued it as tasks-20261024-fix-the-thing-01.\n\nRouted: fix -> WerkrbyDone: the dashboard loads again."))+
		hermesTestChrome)
	startHermesSim(t, dir, func(n int, prompt string) {
		ledger.add("local-chat",
			hermesTestRow{role: "user", content: prompt},
			hermesTestRow{role: "assistant", content: "Queued it as tasks-20261024-fix-the-thing-01.\n\nRouted: fix -> Werkrby", finishReason: "verify_hook_continue"},
			hermesTestRow{role: "assistant", content: "Done: the dashboard loads again.", finishReason: "stop"},
		)
	})

	got := runHermesTurn(t, dir, script, key, "", "Werkrby chat for telegram/4242.\n\nJoey: fix the dashboard")

	if got.err != nil {
		t.Fatalf("Send returned %v, want the rewrite", got.err)
	}
	finals := got.finals()
	if len(finals) != 1 || finals[0] != "Done: the dashboard loads again." {
		t.Fatalf("EventFinal texts = %q, want only the gate's rewrite", finals)
	}
	if sent := hermesPromptsSent(t, dir); sent != 1 {
		t.Fatalf("prompt sent %d times, want 1", sent)
	}
}

// Hermes ran out of retries on a Z.ai 429. The ledger closes the turn with a failed_turn
// boundary row and the screen shows the corner-less error panel. That is a failed turn, never
// a reply: the error must reach the orchestrator so its quota park / in-place retry runs.
func TestHerdrHermesFailedTurnIsAnErrorNotAReply(t *testing.T) {
	errorPanel := " ─ ☤ Hermes " + strings.Repeat("─", 104) + " \n\n Rate limited by zai; wait a minute or /model to switch.\n Details: HTTP 429: Rate limit reached for requests\n\n " +
		strings.Repeat("─", 116) + " \n"
	for _, tc := range []struct {
		name string
		key  string
		warm bool
	}{
		{name: "task worker on a new pane", key: "orchestrator:tasks:task_implementation:tasks-20261024-quota-01:1"},
		{name: "warm chat pane", key: "chat:telegram:4242", warm: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			hermesHome := filepath.Join(dir, "hermes-home")
			t.Setenv("HERMES_HOME", hermesHome)
			ledger := newHermesTestLedger(t, hermesWorkerLedger(hermesHome))
			ledger.session("local-q", "", "", time.Now().Add(-time.Hour))
			screen := hermesTestTurn("Do the work.", errorPanel) + hermesTestChrome
			if tc.warm {
				ledger.add("local-q",
					hermesTestRow{role: "user", content: "earlier question", at: time.Now().Add(-time.Hour)},
					hermesTestRow{role: "assistant", content: "Older reply: three tasks are open.", finishReason: "stop", at: time.Now().Add(-time.Hour)},
				)
				writeWarmHermesWorker(t, dir, tc.key)
				screen = hermesTestTurn("earlier question", hermesTestBox("Older reply: three tasks are open.")) + screen
			}
			script := hermesFakeHerdr(t, dir)
			writeHermesScreen(t, dir, screen)
			startHermesSim(t, dir, func(n int, prompt string) {
				ledger.add("local-q",
					hermesTestRow{role: "user", content: prompt},
					hermesTestRow{role: "assistant", content: "Your request was not processed. Send it again if you still want me to carry it out.", displayKind: "failed_turn",
						displayMetadata: `{"error": "HTTP 429: Rate limit reached for requests", "error_surface": {"layer": "provider", "code": "rate_limit", "retryable": true}}`},
				)
			})

			got := runHermesTurn(t, dir, script, tc.key, "", "Do the work.\n\n"+tc.key)

			if finals := got.finals(); len(finals) != 0 {
				t.Fatalf("a failed Hermes turn was delivered as a reply: %q", finals)
			}
			if got.err == nil {
				t.Fatalf("Send succeeded on a failed Hermes turn, want a turn error")
			}
			for _, want := range []string{"runner turn failed", "HTTP 429", "rate_limit"} {
				if !strings.Contains(got.err.Error(), want) {
					t.Fatalf("turn error %q does not carry %q (the orchestrator classifies quota walls from it)", got.err, want)
				}
			}
			if sent := hermesPromptsSent(t, dir); sent != 1 {
				t.Fatalf("prompt sent %d times, want 1", sent)
			}
		})
	}
}

// With no readable ledger (no state.db for the profile) the reply still comes from the screen,
// and a Hermes reply box on a new pane is proof enough that the prompt landed.
func TestHerdrHermesUnreadableLedgerFallsBackToScreen(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HERMES_HOME", filepath.Join(dir, "no-such-hermes-home"))
	script := hermesFakeHerdr(t, dir)
	writeHermesScreen(t, dir, hermesTestTurn("You are running from the Werkrby orchestrator system.", hermesTestBox("Screen reply: task moved to review."))+hermesTestChrome)
	startHermesSim(t, dir, nil)

	got := runHermesTurn(t, dir, script, "orchestrator:tasks:task_implementation:tasks-20261024-screen-01:1", "", hermesTestTaskPrompt("tasks-20261024-screen-01"))

	if sent := hermesPromptsSent(t, dir); sent != 1 {
		t.Fatalf("prompt sent %d times, want 1 (a Hermes reply box is on screen)", sent)
	}
	if got.err != nil {
		t.Fatalf("Send returned %v, want the screen reply", got.err)
	}
	if finals := got.finals(); len(finals) != 1 || finals[0] != "Screen reply: task moved to review." {
		t.Fatalf("EventFinal texts = %q, want the screen reply", finals)
	}
}

// With no readable ledger, a newest turn that drew no reply box but an error panel is a failed
// turn on the screen path too.
func TestHerdrHermesUnreadableLedgerScreenErrorPanelIsAnError(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HERMES_HOME", filepath.Join(dir, "no-such-hermes-home"))
	key := "chat:telegram:4242"
	writeWarmHermesWorker(t, dir, key)
	script := hermesFakeHerdr(t, dir)
	errorPanel := " ─ ☤ Hermes " + strings.Repeat("─", 104) + " \n\n API call failed after 3 retries: HTTP 503 overloaded\n\n " +
		strings.Repeat("─", 116) + " \n"
	writeHermesScreen(t, dir, hermesTestTurn("What is on my plate today?", hermesTestBox("Older reply: three tasks are open."))+
		hermesTestTurn("And the dentist?", errorPanel)+hermesTestChrome)
	startHermesSim(t, dir, nil)

	got := runHermesTurn(t, dir, script, key, "", "Werkrby chat.\n\nJoey: and the dentist?")

	if finals := got.finals(); len(finals) != 0 {
		t.Fatalf("an error panel was delivered as the reply: %q", finals)
	}
	if got.err == nil || !strings.Contains(got.err.Error(), "runner turn failed") || !strings.Contains(got.err.Error(), "HTTP 503 overloaded") {
		t.Fatalf("Send error = %v, want a runner turn failure carrying the panel text", got.err)
	}
}

// A roster seat launched with `-p wk-chat` is judged from ~/.hermes/profiles/wk-chat/state.db,
// never from the root ledger.
func TestHerdrHermesRosterProfileSelectsItsLedger(t *testing.T) {
	dir := t.TempDir()
	hermesHome := filepath.Join(dir, "hermes-home")
	t.Setenv("HERMES_HOME", hermesHome)
	if err := os.MkdirAll(filepath.Join(dir, ".werkrby"), 0o755); err != nil {
		t.Fatal(err)
	}
	roster := "staff:\n  chatseat:\n    runner: hermes\n    args: [\"-p\", \"wk-chat\", \"--model\", \"zai/glm-5.3\", \"--reasoning\", \"high\"]\n"
	if err := os.WriteFile(filepath.Join(dir, ".werkrby", "roster.yaml"), []byte(roster), 0o644); err != nil {
		t.Fatal(err)
	}
	root := newHermesTestLedger(t, filepath.Join(hermesHome, "state.db"))
	root.session("local-root", "", "", time.Now())
	profile := newHermesTestLedger(t, filepath.Join(hermesHome, "profiles", "wk-chat", "state.db"))
	profile.session("local-chat", "", "", time.Now())
	script := hermesFakeHerdr(t, dir)
	writeHermesScreen(t, dir, hermesTestTurn("Werkrby chat", hermesTestBox("From the wk-chat profile."))+hermesTestChrome)
	startHermesSim(t, dir, func(n int, prompt string) {
		// A decoy: the same prompt answered in the root ledger.
		root.add("local-root",
			hermesTestRow{role: "user", content: prompt},
			hermesTestRow{role: "assistant", content: "From the root profile.", finishReason: "stop"},
		)
		profile.add("local-chat",
			hermesTestRow{role: "user", content: prompt},
			hermesTestRow{role: "assistant", content: "From the wk-chat profile.", finishReason: "stop"},
		)
	})

	got := runHermesTurn(t, dir, script, "chat:telegram:77", "@chatseat", "Werkrby chat for telegram/77.\n\nJoey: hi")

	if got.err != nil {
		t.Fatalf("Send returned %v", got.err)
	}
	if finals := got.finals(); len(finals) != 1 || finals[0] != "From the wk-chat profile." {
		t.Fatalf("EventFinal texts = %q, want the wk-chat ledger's answer", finals)
	}
	if sent := hermesPromptsSent(t, dir); sent != 1 {
		t.Fatalf("prompt sent %d times, want 1", sent)
	}
	// A seat that names its profile launches with exactly its own args: no default profile added.
	starts := hermesStarts(t, dir)
	if len(starts) != 1 {
		t.Fatalf("agent start ran %d times, want 1: %q", len(starts), starts)
	}
	if kind, args := starts[0].kind, strings.Join(starts[0].args, " "); kind != "hermes" || args != "-p wk-chat --model zai/glm-5.3 --reasoning high" {
		t.Fatalf("agent start --kind %q -- %q, want --kind hermes -- %q", kind, args, "-p wk-chat --model zai/glm-5.3 --reasoning high")
	}
}

// The gateway can flush the answer row a moment after the pane returns to its composer; the
// settle window waits for it instead of judging the turn answerless.
func TestHerdrHermesAnswerRowLandingAfterIdleIsStillTheReply(t *testing.T) {
	dir := t.TempDir()
	hermesHome := filepath.Join(dir, "hermes-home")
	t.Setenv("HERMES_HOME", hermesHome)
	ledger := newHermesTestLedger(t, hermesWorkerLedger(hermesHome))
	ledger.session("local-a", "", "", time.Now())
	script := hermesFakeHerdr(t, dir)
	// The screen still shows the turn's interim box: only the ledger's late row is the answer.
	writeHermesScreen(t, dir, hermesTestTurn("You are running from the Werkrby orchestrator system.", hermesTestBox("Working on it."))+hermesTestChrome)
	var late sync.WaitGroup
	t.Cleanup(late.Wait)
	startHermesSim(t, dir, func(n int, prompt string) {
		ledger.add("local-a", hermesTestRow{role: "user", content: prompt})
		late.Add(1)
		go func() {
			defer late.Done()
			time.Sleep(150 * time.Millisecond)
			ledger.add("local-a", hermesTestRow{role: "assistant", content: "Late answer.", finishReason: "stop"})
		}()
	})

	got := runHermesTurn(t, dir, script, "orchestrator:tasks:task_implementation:tasks-20261024-late-01:1", "", hermesTestTaskPrompt("tasks-20261024-late-01"))

	if got.err != nil {
		t.Fatalf("Send returned %v, want the late answer", got.err)
	}
	if finals := got.finals(); len(finals) != 1 || finals[0] != "Late answer." {
		t.Fatalf("EventFinal texts = %q, want the late answer", finals)
	}
}

// A turn that stopped on a tool step and went idle without an answer row or a failed-turn row is
// a failed turn: the interim box on screen ("Let me read the task first.") is not the reply.
func TestHerdrHermesTurnWithoutAnswerRowIsAnError(t *testing.T) {
	dir := t.TempDir()
	hermesHome := filepath.Join(dir, "hermes-home")
	t.Setenv("HERMES_HOME", hermesHome)
	ledger := newHermesTestLedger(t, hermesWorkerLedger(hermesHome))
	ledger.session("local-a", "", "", time.Now())
	script := hermesFakeHerdr(t, dir)
	writeHermesScreen(t, dir, hermesTestTurn("You are running from the Werkrby orchestrator system.", hermesTestBox("Let me read the task first."), "  ┊ 📖 read_file  0.1s\n")+hermesTestChrome)
	startHermesSim(t, dir, func(n int, prompt string) {
		ledger.add("local-a",
			hermesTestRow{role: "user", content: prompt},
			hermesTestRow{role: "assistant", content: "Let me read the task first.", toolCalls: `[{"id":"c1"}]`, finishReason: "tool_calls"},
			hermesTestRow{role: "tool", content: "{}"},
		)
	})

	got := runHermesTurn(t, dir, script, "orchestrator:tasks:task_implementation:tasks-20261024-tooltail-01:1", "", hermesTestTaskPrompt("tasks-20261024-tooltail-01"))

	if finals := got.finals(); len(finals) != 0 {
		t.Fatalf("an answerless turn delivered %q as its reply", finals)
	}
	if got.err == nil || !strings.Contains(got.err.Error(), "runner turn failed") {
		t.Fatalf("Send error = %v, want a runner turn failure", got.err)
	}
	if sent := hermesPromptsSent(t, dir); sent != 1 {
		t.Fatalf("prompt sent %d times, want 1", sent)
	}
}
