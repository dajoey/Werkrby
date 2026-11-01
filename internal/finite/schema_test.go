package finite

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestFiniteSchemaInvariants(t *testing.T) {
	s := newTestStore(t)
	if s.db == nil {
		t.Errorf("current SQLite schema was not initialized")
		return
	}
	var journal string
	if err := s.db.QueryRow(`PRAGMA journal_mode`).Scan(&journal); err != nil || strings.ToLower(journal) != "wal" {
		t.Errorf("journal mode = %q, %v", journal, err)
	}
	var synchronous, foreignKeys int
	_ = s.db.QueryRow(`PRAGMA synchronous`).Scan(&synchronous)
	_ = s.db.QueryRow(`PRAGMA foreign_keys`).Scan(&foreignKeys)
	if synchronous != 2 || foreignKeys != 1 {
		t.Errorf("synchronous=%d foreign_keys=%d", synchronous, foreignKeys)
	}
	var userVersion int
	if err := s.db.QueryRow(`PRAGMA user_version`).Scan(&userVersion); err != nil || userVersion != schemaVersion {
		t.Errorf("schema user_version=%d, %v", userVersion, err)
	}
	for _, table := range []string{"tasks", "runs", "run_results", "run_usage", "task_events", "outbox"} {
		var definition string
		if err := s.db.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&definition); err != nil {
			t.Errorf("missing table %s: %v", table, err)
			continue
		}
		if !strings.Contains(strings.ToUpper(definition), "STRICT") {
			t.Errorf("%s is not STRICT", table)
		}
	}
	rows, err := s.db.Query(`SELECT name FROM pragma_table_info('tasks')`)
	if err != nil {
		t.Fatal(err)
	}
	columns := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		columns[name] = true
	}
	_ = rows.Close()
	for _, forbidden := range []string{"attempt", "review_attempt", "charged", "resume_credit", "format_credit", "outcome_miss"} {
		if columns[forbidden] {
			t.Errorf("legacy accounting column %q exists", forbidden)
		}
	}

	task := intakeTestTask(t, s, "task-schema", true)
	insertOpen := `INSERT INTO runs(task_id,phase,task_version,seat,session_key,launch_reason,run_dir,created_at) VALUES(?,?,?,?,?,?,?,?)`
	res, err := s.db.Exec(insertOpen, task.ID, PhaseImplementation, task.Version, "seat", "finite:schema", "initial", filepathJoinForSchema(s, 1), s.now().Format(timeFormatForSchema))
	if err != nil {
		t.Fatalf("insert open run: %v", err)
	}
	runID, _ := res.LastInsertId()
	if _, err := s.db.Exec(insertOpen, task.ID, PhaseReview, task.Version, "seat", "finite:schema", "review", filepathJoinForSchema(s, 2), s.now().Format(timeFormatForSchema)); err == nil {
		t.Errorf("second open run was allowed")
	}
	if _, err := s.db.Exec(`UPDATE tasks SET state='waiting' WHERE id=?`, task.ID); err == nil {
		t.Errorf("malformed waiting shape was allowed")
	}
	if _, err := s.db.Exec(`UPDATE runs SET outcome='ok' WHERE id=?`, runID); err == nil {
		t.Errorf("outcome without ended_at was allowed")
	}
	if _, err := s.db.Exec(`UPDATE runs SET outcome='ok',ended_at=? WHERE id=?`, s.now().Format(timeFormatForSchema), runID); err != nil {
		t.Fatalf("end run: %v", err)
	}
	if _, err := s.db.Exec(`INSERT INTO run_usage(run_id,source,collected_at) VALUES(?,?,?)`, runID, "adapter", s.now().Format(timeFormatForSchema)); err != nil {
		t.Fatalf("insert NULL usage: %v", err)
	}
	var attempts int
	if err := s.db.QueryRow(`SELECT attempts FROM charged_attempts WHERE task_id=? AND phase='implementation'`, task.ID).Scan(&attempts); err != nil || attempts != 1 {
		t.Errorf("charged attempts=%d, %v", attempts, err)
	}
	var fresh any
	if err := s.db.QueryRow(`SELECT fresh_input_tokens FROM run_usage WHERE run_id=?`, runID).Scan(&fresh); err != nil || fresh != nil {
		t.Errorf("missing usage became %#v, %v", fresh, err)
	}
	event := `INSERT INTO task_events(event_id,task_id,run_id,kind,to_state,task_version,payload_json,created_at) VALUES(?,?,?,?,?,?,?,?)`
	if _, err := s.db.Exec(event, "settle-1", task.ID, runID, "settled", "todo", task.Version+1, `{}`, s.now().Format(timeFormatForSchema)); err != nil {
		t.Fatalf("insert settlement event: %v", err)
	}
	if _, err := s.db.Exec(event, "settle-2", task.ID, runID, "settled", "todo", task.Version+1, `{}`, s.now().Format(timeFormatForSchema)); err == nil {
		t.Errorf("second settlement event was allowed")
	}
	if _, err := s.db.Exec(`INSERT INTO run_results(run_id,task_id,phase,outcome,result_json) VALUES(?,?,?,?,?)`, runID, task.ID, "review", "accepted", `{}`); err == nil {
		t.Errorf("composite result FK accepted a phase mismatch")
	}
	if err := s.CheckInvariants(context.Background()); err != nil {
		t.Errorf("invariant checker rejected valid ended run: %v", err)
	}
}

const timeFormatForSchema = "2006-01-02T15:04:05.999999999Z07:00"

func filepathJoinForSchema(s *Store, id int64) string {
	return fmt.Sprintf("%s%c%d", s.cfg.RunsDir(), separatorForSchema, id)
}

const separatorForSchema = '/'
