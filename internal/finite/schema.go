package finite

const schemaVersion = 1

const currentSchema = `
CREATE TABLE tasks (
  id                TEXT NOT NULL PRIMARY KEY,
  title             TEXT NOT NULL,
  state             TEXT NOT NULL CHECK (state IN (
                      'todo','working','review','reviewing',
                      'waiting','done','failed','cancelled'
                    )),
  risk              TEXT NOT NULL CHECK (risk IN ('low','medium','high')),
  review_required   INTEGER NOT NULL CHECK (review_required IN (0,1)),
  cancel_requested  INTEGER NOT NULL DEFAULT 0 CHECK (cancel_requested IN (0,1)),
  max_runs          INTEGER NOT NULL DEFAULT 12 CHECK (max_runs > 0),
  max_failures      INTEGER NOT NULL DEFAULT 3 CHECK (max_failures > 0),
  max_review_cycles INTEGER NOT NULL DEFAULT 3 CHECK (max_review_cycles > 0),
  brief_path        TEXT NOT NULL,
  brief_sha256      TEXT NOT NULL CHECK (length(brief_sha256) = 64),
  projection_path   TEXT NOT NULL,
  resume_state      TEXT CHECK (resume_state IN ('todo','review')),
  wait_kind         TEXT CHECK (wait_kind IN ('time','joey')),
  wake_at           TEXT,
  waiting_for       TEXT,
  joey_ask_json     TEXT,
  version           INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0),
  created_at        TEXT NOT NULL,
  updated_at        TEXT NOT NULL,
  CHECK (
    (
      state = 'waiting'
      AND resume_state IS NOT NULL
      AND wait_kind IS NOT NULL
      AND waiting_for IS NOT NULL
      AND trim(waiting_for) <> ''
      AND (
        (wait_kind = 'time'
          AND wake_at IS NOT NULL
          AND joey_ask_json IS NULL)
        OR
        (wait_kind = 'joey'
          AND wake_at IS NULL
          AND joey_ask_json IS NOT NULL
          AND json_valid(joey_ask_json))
      )
    )
    OR
    (
      state <> 'waiting'
      AND resume_state IS NULL
      AND wait_kind IS NULL
      AND wake_at IS NULL
      AND waiting_for IS NULL
      AND joey_ask_json IS NULL
    )
  )
) STRICT;

CREATE TABLE runs (
  id                INTEGER PRIMARY KEY,
  task_id           TEXT NOT NULL
                    REFERENCES tasks(id) ON DELETE RESTRICT,
  phase             TEXT NOT NULL
                    CHECK (phase IN ('implementation','review')),
  task_version      INTEGER NOT NULL CHECK (task_version >= 0),
  seat              TEXT NOT NULL,
  session_key       TEXT NOT NULL,
  launch_reason     TEXT NOT NULL CHECK (launch_reason IN (
                      'initial','retry','wake','review','operator'
                    )),
  prompt_hash       TEXT,
  run_dir           TEXT NOT NULL UNIQUE,
  supervisor_pid    INTEGER,
  process_token     TEXT,
  created_at        TEXT NOT NULL,
  started_at        TEXT,
  ended_at          TEXT,
  exit_code         INTEGER,
  outcome           TEXT CHECK (
                      outcome IS NULL OR outcome IN
                      ('ok','fail','timeout','lost','interrupted','wall')
                    ),
  error             TEXT,
  CHECK (
    (outcome IS NULL AND ended_at IS NULL)
    OR
    (outcome IS NOT NULL AND ended_at IS NOT NULL)
  ),
  UNIQUE (id, task_id),
  UNIQUE (id, task_id, phase)
) STRICT;

CREATE UNIQUE INDEX one_open_run_per_task
  ON runs(task_id) WHERE ended_at IS NULL;
CREATE INDEX runs_task_phase
  ON runs(task_id, phase, id);

CREATE TABLE run_results (
  run_id            INTEGER NOT NULL PRIMARY KEY,
  task_id           TEXT NOT NULL,
  phase             TEXT NOT NULL,
  outcome           TEXT NOT NULL,
  result_json       TEXT NOT NULL CHECK (json_valid(result_json)),
  FOREIGN KEY (run_id, task_id, phase)
    REFERENCES runs(id, task_id, phase) ON DELETE RESTRICT,
  CHECK (
    (phase = 'implementation' AND outcome IN
      ('done','review','todo','wait','failed'))
    OR
    (phase = 'review' AND outcome IN
      ('accepted','rejected','wait','failed'))
  )
) STRICT;

CREATE TABLE run_usage (
  run_id                INTEGER NOT NULL PRIMARY KEY
                        REFERENCES runs(id) ON DELETE RESTRICT,
  fresh_input_tokens    INTEGER CHECK (fresh_input_tokens >= 0),
  cache_read_tokens     INTEGER CHECK (cache_read_tokens >= 0),
  cache_write_tokens    INTEGER CHECK (cache_write_tokens >= 0),
  output_tokens         INTEGER CHECK (output_tokens >= 0),
  reasoning_tokens      INTEGER CHECK (reasoning_tokens >= 0),
  api_calls             INTEGER CHECK (api_calls >= 0),
  tool_calls            INTEGER CHECK (tool_calls >= 0),
  source                TEXT NOT NULL,
  collected_at          TEXT NOT NULL
) STRICT;

CREATE TABLE task_events (
  event_id          TEXT NOT NULL PRIMARY KEY,
  task_id           TEXT NOT NULL
                    REFERENCES tasks(id) ON DELETE RESTRICT,
  run_id            INTEGER,
  kind              TEXT NOT NULL,
  from_state        TEXT,
  to_state          TEXT NOT NULL,
  task_version      INTEGER NOT NULL CHECK (task_version >= 0),
  payload_json      TEXT NOT NULL CHECK (json_valid(payload_json)),
  created_at        TEXT NOT NULL,
  FOREIGN KEY (run_id, task_id)
    REFERENCES runs(id, task_id) ON DELETE RESTRICT
) STRICT;

CREATE UNIQUE INDEX one_settlement_event_per_run
  ON task_events(run_id) WHERE kind = 'settled';

CREATE TABLE outbox (
  delivery_id       TEXT NOT NULL PRIMARY KEY,
  event_id          TEXT NOT NULL
                    REFERENCES task_events(event_id) ON DELETE RESTRICT,
  channel           TEXT NOT NULL,
  kind              TEXT NOT NULL,
  payload_json      TEXT NOT NULL CHECK (json_valid(payload_json)),
  created_at        TEXT NOT NULL,
  available_at      TEXT NOT NULL,
  send_count        INTEGER NOT NULL DEFAULT 0 CHECK (send_count >= 0),
  delivered_at      TEXT,
  last_error        TEXT,
  UNIQUE (event_id, channel, kind)
) STRICT;

CREATE INDEX outbox_pending
  ON outbox(available_at, created_at)
  WHERE delivered_at IS NULL;

CREATE VIEW charged_attempts AS
SELECT task_id, phase, COUNT(*) AS attempts
FROM runs
WHERE outcome IN ('ok','fail','timeout')
GROUP BY task_id, phase;
`
