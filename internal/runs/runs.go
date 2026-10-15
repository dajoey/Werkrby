// Package runs is the run record Stage B of the structural program derives attempts from: one row
// per provider turn that happened (pid, start, end, exit, outcome), and an append-only intents inbox
// other writers can fill while the owner is down. In this stage it is a shadow: the orchestrator
// writes it beside the task files, which stay the source of truth, and logs derived attempts next
// to the ones the files charge.
package runs

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/ncruces/go-sqlite3/driver"
)

// Outcome is how a run ended. Running means it has not.
type Outcome string

const (
	Running Outcome = "open"
	// OK is a turn the provider finished. Stage B refines it into ok, parked and format-rejected
	// where the transition is reconciled.
	OK      Outcome = "ok"
	Fail    Outcome = "fail"
	Timeout Outcome = "timeout"
	// Lost is a run its owner never saw end (a restart or a crash took the owner).
	Lost Outcome = "lost"
	// Interrupted is a run a service restart or an operator stop cut short.
	Interrupted Outcome = "interrupted"
	// Wall is a run a quota or rate wall refused.
	Wall Outcome = "wall"
)

// Counts reports whether a run with this outcome is an attempt: a model run that happened and
// finished by itself. Open, lost, interrupted and wall runs are not.
func (o Outcome) Counts() bool { return o == OK || o == Fail || o == Timeout }

func (o Outcome) valid() bool {
	switch o {
	case OK, Fail, Timeout, Lost, Interrupted, Wall:
		return true
	}
	return false
}

// Run is one provider turn. Charged is the attempt number the task file recorded when the run
// started, kept beside the derived count so the two can be compared.
type Run struct {
	ID         int64
	Doc        string
	Phase      string
	Seat       string
	SessionKey string
	Charged    int
	PID        int
	Start      time.Time
	End        time.Time
	Exit       int
	Outcome    Outcome
}

// Intent is a request another writer filed for the owner to apply.
type Intent struct {
	ID      int64
	Kind    string
	Doc     string
	Payload string
	At      time.Time
	Applied time.Time
}

// Store is the SQLite file. It is safe for concurrent use.
type Store struct{ db *sql.DB }

const schema = `
CREATE TABLE IF NOT EXISTS runs (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  doc         TEXT NOT NULL,
  phase       TEXT NOT NULL,
  seat        TEXT NOT NULL DEFAULT '',
  session_key TEXT NOT NULL DEFAULT '',
  charged     INTEGER NOT NULL DEFAULT 0,
  pid         INTEGER NOT NULL DEFAULT 0,
  start_at    TEXT NOT NULL,
  end_at      TEXT,
  exit_code   INTEGER NOT NULL DEFAULT 0,
  outcome     TEXT NOT NULL DEFAULT 'open'
);
CREATE INDEX IF NOT EXISTS runs_doc_phase ON runs (doc, phase);
CREATE TABLE IF NOT EXISTS intents (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  kind       TEXT NOT NULL,
  doc        TEXT NOT NULL,
  payload    TEXT NOT NULL DEFAULT '',
  at         TEXT NOT NULL,
  applied_at TEXT
);`

// Open opens or creates the store at path.
func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	dsn := "file:" + filepath.ToSlash(path) + "?_pragma=busy_timeout(10000)&_pragma=journal_mode(wal)"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	// One connection serialises this process's writers; a second process waits on busy_timeout.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("runs store %s: %w", path, err)
	}
	return &Store{db: db}, nil
}

// Close closes the store.
func (s *Store) Close() error { return s.db.Close() }

func stamp(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func parseStamp(value sql.NullString) time.Time {
	if !value.Valid {
		return time.Time{}
	}
	t, _ := time.Parse(time.RFC3339Nano, value.String)
	return t
}

// Begin records a run that has started and returns its id.
func (s *Store) Begin(r Run) (int64, error) {
	if r.Start.IsZero() {
		r.Start = time.Now()
	}
	result, err := s.db.Exec(`INSERT INTO runs (doc, phase, seat, session_key, charged, pid, start_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		r.Doc, r.Phase, r.Seat, r.SessionKey, r.Charged, r.PID, stamp(r.Start))
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// End records how a run ended. It never overwrites an outcome: a run ends once.
func (s *Store) End(id int64, outcome Outcome, exit int, end time.Time) error {
	if !outcome.valid() {
		return fmt.Errorf("runs: %q is not an ending outcome", outcome)
	}
	result, err := s.db.Exec(`UPDATE runs SET outcome = ?, exit_code = ?, end_at = ? WHERE id = ? AND outcome = 'open'`,
		string(outcome), exit, stamp(end), id)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n == 0 {
		return fmt.Errorf("runs: run %d is unknown or already ended", id)
	}
	return nil
}

// LostOpen closes every run still open as lost. A new owner calls it once at start, when none of
// its own runs exist yet. It returns how many it closed.
func (s *Store) LostOpen(now time.Time) (int, error) {
	result, err := s.db.Exec(`UPDATE runs SET outcome = 'lost', end_at = ? WHERE outcome = 'open'`, stamp(now))
	if err != nil {
		return 0, err
	}
	n, err := result.RowsAffected()
	return int(n), err
}

// Attempts is the attempt count a document's phase has used, derived from its runs.
func (s *Store) Attempts(doc, phase string) (int, error) {
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM runs WHERE doc = ? AND phase = ? AND outcome IN ('ok', 'fail', 'timeout')`, doc, phase).Scan(&n)
	return n, err
}

// Runs lists a document's runs in start order.
func (s *Store) Runs(doc string) ([]Run, error) {
	rows, err := s.db.Query(`SELECT id, doc, phase, seat, session_key, charged, pid, start_at, end_at, exit_code, outcome FROM runs WHERE doc = ? ORDER BY id`, doc)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Run
	for rows.Next() {
		var r Run
		var start string
		var end sql.NullString
		var outcome string
		if err := rows.Scan(&r.ID, &r.Doc, &r.Phase, &r.Seat, &r.SessionKey, &r.Charged, &r.PID, &start, &end, &r.Exit, &outcome); err != nil {
			return nil, err
		}
		r.Start, _ = time.Parse(time.RFC3339Nano, start)
		r.End = parseStamp(end)
		r.Outcome = Outcome(outcome)
		out = append(out, r)
	}
	return out, rows.Err()
}

// AppendIntent files an intent and returns its id.
func (s *Store) AppendIntent(i Intent) (int64, error) {
	if strings.TrimSpace(i.Kind) == "" || strings.TrimSpace(i.Doc) == "" {
		return 0, errors.New("runs: an intent needs a kind and a document")
	}
	if i.At.IsZero() {
		i.At = time.Now()
	}
	result, err := s.db.Exec(`INSERT INTO intents (kind, doc, payload, at) VALUES (?, ?, ?, ?)`, i.Kind, i.Doc, i.Payload, stamp(i.At))
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// PendingIntents lists the intents not yet applied, oldest first.
func (s *Store) PendingIntents() ([]Intent, error) {
	rows, err := s.db.Query(`SELECT id, kind, doc, payload, at FROM intents WHERE applied_at IS NULL ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Intent
	for rows.Next() {
		var i Intent
		var at string
		if err := rows.Scan(&i.ID, &i.Kind, &i.Doc, &i.Payload, &at); err != nil {
			return nil, err
		}
		i.At, _ = time.Parse(time.RFC3339Nano, at)
		out = append(out, i)
	}
	return out, rows.Err()
}

// ApplyIntent marks an intent applied. It fails for one already applied or unknown.
func (s *Store) ApplyIntent(id int64, at time.Time) error {
	result, err := s.db.Exec(`UPDATE intents SET applied_at = ? WHERE id = ? AND applied_at IS NULL`, stamp(at), id)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n == 0 {
		return fmt.Errorf("runs: intent %d is unknown or already applied", id)
	}
	return nil
}
