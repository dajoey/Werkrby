"""Helm triage store: threads, events, categories, suppressions.

Pure store logic and validation. No HTTP, no model call, no Planka. Everything
here is deterministic and testable against a throwaway SQLite file, which is
the point: when the list is wrong you need to know whether to look at the
triage pass or at the store, and only one of those two can be tested offline.

The rule the whole design serves: a card must never lie about its state.

Five invariants enforce it, each written after a specific failure:

  1. PUT is an upsert. It removes only what the caller explicitly names in
     `tombstones`. A pass that crashes halfway must not delete answered work.
  2. A thread that reached `done` is never dragged back by a PUT. The triage
     pass re-derives from the same cards every run and has no idea what was
     already dealt with.
  3. An answered question is never re-armed underneath the user. A later PUT
     may refresh the wording; it may not rewrite an ask that was already
     tapped.
  4. A lit thread carries BOTH a report and a question. `needs-you` with no
     ask renders as a bare Close button over finished work, and a bot result
     with no prose note renders as a single status word.
  5. Acking an `ack` ask settles it on the tap. An ack exists for exactly
     "reading it is genuinely the whole job", so there is nothing left for an
     executor to do with one; before this it dispatched anyway, and since a
     bot may never mark a thread done, the executor could only hand the same
     ack straight back. That looped forever until the person tapped the
     unrelated Close button, which every "Got it" tap looked like it should
     have already done.

Validation is hand-written rather than declared as pydantic models on purpose.
Every rejection has to be readable by the model that produced the payload and
specific enough for it to fix itself on the retry, which a terse type error is
not.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import sqlite3
import time
import uuid

DB_PATH = os.environ.get("HELM_DB") or os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "triage.db")

URGENCY_ORDER = {"now": 0, "this-week": 1, "whenever": 2}
VALID_URGENCY = set(URGENCY_ORDER)
VALID_STATE = {"needs-you", "working", "waiting-external", "done"}
VALID_ASK = {"none", "ack", "choice", "text"}

MIN_RESULT_NOTE = 80
MAX_NOTE = 8000
MAX_SNOOZE_S = 400 * 86400
MAX_LINKS = 12
MAX_LINK_FIELD = 800
MAX_TITLE = 300

# A card written by hand, from the app, rather than derived by a producer.
# The prefix is on the thread_id so the two can never be confused: a producer
# owns the ids it mints and may tombstone them, and nothing it sends will ever
# name one of these.
HAND_ID_PREFIX = "joey-"
# The reply value a hand-written card carries when it is handed straight to an
# executor. Deliberately not "__redirect__": nothing was redirected, no options
# were ever offered, and the run is told so plainly instead of being handed a
# rejection with nothing to reject.
HAND_TASK_VALUE = "__task__"
# What is meant to happen next, which is the only thing a hand-written card
# has to decide. It sets state and ask together, because those two are what
# the store's own invariants are about, and letting a form post them
# separately is how you get a lit card with no question.
VALID_INTENT = {"mine", "claude", "waiting"}

# Durable-queue behaviour. A dispatched run that never reports is re-offered
# after RESULT_TIMEOUT_S; retries back off exponentially and then give up
# LOUDLY, because a silent infinite retry and a working queue look identical.
MAX_DISPATCH_ATTEMPTS = 5
RESULT_TIMEOUT_S = 30 * 60
DISPATCH_BACKOFF_S = 60
DISPATCH_BACKOFF_MAX_S = 15 * 60

# --- The park contract (contract: P1 §1; LESSONS 4, 5, 6, 10) ----------------
#
# `waiting-external` used to be a hole in the state machine rather than a state.
# Neither queue can see a parked thread, so it was invisible to every agent
# while rendering to the owner as "waiting on someone else" with no button; on
# one live board twelve of fourteen visible cards sat in it simultaneously.
# Bots reached it honestly: forbidden to close a thread and (rightly) blocked
# from a lit card with no ask, parking was the only accepted state left, and
# they wrote notes promising "his final close tap" that the state never offers.
#
# So a park is now a claim with an owner and an expiry. `blocked_on` says WHO
# or WHAT is being waited on; `recheck_at` says when the system looks again, on
# its own clock; the optional `gate_cmd` is a command a sweeper can run to
# answer "has it cleared yet" without a human or a model. Waiting on nobody is
# not waiting, it is dropping, and waiting on the owner himself is a needs-you
# ask wearing a park's clothes.
MIN_BLOCKED_ON = 8
DEFAULT_RECHECK_S = 6 * 3600
MAX_RECHECK_S = 30 * 86400
MAX_GATE_CMD = 500
# A park aimed at the person the list is for. Matched only when that is the
# WHOLE value: "Joey's manager" and "the user group admin" are real third
# parties and must not be swept up by this.
_WAITS_ON_OWNER = re.compile(r"^\W*(joey|the user|user|the owner|owner|you|me|myself)\W*$",
                             re.I)

_LOCAL_TZ = dt.datetime.now().astimezone().tzinfo


class StoreError(Exception):
    """A rejection the caller is expected to read and correct itself from.

    Carries an HTTP status so api.py can map it without knowing why.
    """

    def __init__(self, status: int, detail: str, extra: dict | None = None):
        super().__init__(detail)
        self.status = status
        self.detail = detail
        self.extra = extra or {}


# --- Connection --------------------------------------------------------------
DDL = """
CREATE TABLE IF NOT EXISTS threads (
  thread_id   TEXT PRIMARY KEY,
  title       TEXT NOT NULL DEFAULT '',
  summary     TEXT NOT NULL DEFAULT '',
  category    TEXT NOT NULL DEFAULT '',
  urgency     TEXT NOT NULL DEFAULT 'whenever',
  state       TEXT NOT NULL DEFAULT 'needs-you',
  ask_kind    TEXT NOT NULL DEFAULT 'none',
  ask_prompt  TEXT NOT NULL DEFAULT '',
  ask_options TEXT NOT NULL DEFAULT '[]',
  responder   TEXT,
  sources     TEXT NOT NULL DEFAULT '[]',
  first_seen  REAL NOT NULL DEFAULT 0,
  last_change REAL NOT NULL DEFAULT 0,
  suppressed  INTEGER NOT NULL DEFAULT 0,
  tombstoned  INTEGER NOT NULL DEFAULT 0,
  dispatched_at     REAL NOT NULL DEFAULT 0,
  dispatch_attempts INTEGER NOT NULL DEFAULT 0,
  last_dispatch_try REAL NOT NULL DEFAULT 0,
  last_note   TEXT NOT NULL DEFAULT '',
  wake_at     REAL NOT NULL DEFAULT 0,
  wake_note   TEXT NOT NULL DEFAULT '',
  wake_ask    TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS events (
  event_id  TEXT PRIMARY KEY,
  thread_id TEXT NOT NULL,
  ts        REAL NOT NULL,
  source    TEXT NOT NULL DEFAULT '',
  source_id TEXT NOT NULL DEFAULT '',
  kind      TEXT NOT NULL DEFAULT '',
  body      TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_events_thread ON events(thread_id, ts);

CREATE TABLE IF NOT EXISTS categories (
  name       TEXT PRIMARY KEY,
  first_seen REAL NOT NULL DEFAULT 0,
  last_seen  REAL NOT NULL DEFAULT 0
);

-- Metadata only. The bytes live wherever the API put them; a store that also
-- writes files can fail for reasons its tests cannot reach (contract: P1 §6).
CREATE TABLE IF NOT EXISTS attachments (
  att_id    TEXT PRIMARY KEY,
  thread_id TEXT NOT NULL,
  ts        REAL NOT NULL DEFAULT 0,
  filename  TEXT NOT NULL DEFAULT '',
  mime      TEXT NOT NULL DEFAULT '',
  size      INTEGER NOT NULL DEFAULT 0,
  sha256    TEXT NOT NULL DEFAULT '',
  url       TEXT NOT NULL DEFAULT '',
  uploader  TEXT NOT NULL DEFAULT 'joey',
  caption   TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_att_thread ON attachments(thread_id, ts);
CREATE UNIQUE INDEX IF NOT EXISTS idx_att_dupe ON attachments(thread_id, sha256);

CREATE TABLE IF NOT EXISTS suppressions (
  id          TEXT PRIMARY KEY,
  producer    TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  sample      TEXT NOT NULL DEFAULT '',
  note        TEXT NOT NULL DEFAULT '',
  created     REAL NOT NULL DEFAULT 0,
  active      INTEGER NOT NULL DEFAULT 1,
  hits        INTEGER NOT NULL DEFAULT 0,
  last_hit    REAL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_supp_rule ON suppressions(producer, fingerprint);

-- The lesson ledger (contract: P3c §3). In the main DDL rather than a
-- migration: CREATE TABLE IF NOT EXISTS means an existing DB adopts it on its
-- next open with no step, and no ported test sees a schema change. A lesson is
-- a correction Joey adopted: durable, scoped, rendered into executor prompts
-- until retired, and never silently mutated -- every change lands in changelog
-- as a report, not a status word (LESSON 11).
CREATE TABLE IF NOT EXISTS lessons (
  id             TEXT PRIMARY KEY,
  created_ts     REAL NOT NULL,
  source_thread  TEXT NOT NULL,
  rule_text      TEXT NOT NULL,
  scope          TEXT NOT NULL DEFAULT 'global',
  status         TEXT NOT NULL DEFAULT 'active',      -- active | retired
  superseded_by  TEXT NOT NULL DEFAULT '',
  gate_status    TEXT NOT NULL DEFAULT '',            -- '' | pending | implemented
  changelog      TEXT NOT NULL DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS idx_lessons_scope ON lessons(scope, status);

CREATE TABLE IF NOT EXISTS labels (
  name                   TEXT PRIMARY KEY,
  description            TEXT NOT NULL DEFAULT '',
  prompt_template        TEXT NOT NULL,
  webhook_url            TEXT NOT NULL DEFAULT '',
  executor_runtime       TEXT NOT NULL DEFAULT 'hermes',
  convert_to_needs_human INTEGER NOT NULL DEFAULT 0,
  enabled                INTEGER NOT NULL DEFAULT 1,
  created_at             REAL NOT NULL,
  updated_at             REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
  key        TEXT PRIMARY KEY,
  value      TEXT NOT NULL,
  updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS operational_errors (
  error_id   TEXT PRIMARY KEY,
  ts         REAL NOT NULL,
  trace_id   TEXT NOT NULL,
  component  TEXT NOT NULL,
  event      TEXT NOT NULL,
  details    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_operational_errors_ts ON operational_errors(ts);
"""

_MIGRATIONS = (
    ("dispatched_at", "REAL NOT NULL DEFAULT 0"),
    ("dispatch_attempts", "INTEGER NOT NULL DEFAULT 0"),
    ("last_dispatch_try", "REAL NOT NULL DEFAULT 0"),
    ("last_note", "TEXT NOT NULL DEFAULT ''"),
    ("wake_at", "REAL NOT NULL DEFAULT 0"),
    ("wake_note", "TEXT NOT NULL DEFAULT ''"),
    ("wake_ask", "TEXT NOT NULL DEFAULT ''"),
    # Orthogonal to state, not a replacement for it. `waiting-external` was
    # doing two jobs at once: a real wait on someone who might answer any day,
    # and a dropped or dependency-blocked thread with nothing actually moving.
    # Both rendered identically ("waiting on someone else. nothing is needed
    # from you"), so a backlog of the second kind read as a backlog of the
    # first, and there was no way to tell them apart without opening every
    # card and reading its history. `parked` names the second kind explicitly
    # instead of leaving it to look like the first.
    ("parked", "INTEGER NOT NULL DEFAULT 0"),
    ("parked_reason", "TEXT NOT NULL DEFAULT ''"),
    # A thread describes a decision; `links` says where the actual thing lives
    # so a card can be opened instead of hunted for. Set by a producer (PUT,
    # same treatment as title/summary: a resync always reflects what it sent)
    # or by an executor's result (see post_result, which preserves the
    # existing value when the key is simply absent, unlike `ask`). Either way
    # it is frozen by _answer_is_in_flight the same as ask_kind/ask_options,
    # so a routine producer resync landing right after a bot-result cannot
    # silently erase the link that result just reported.
    ("links", "TEXT NOT NULL DEFAULT '[]'"),
    # The park contract (see the constants block above). Additive, so a store
    # written before these existed opens unchanged; parks already in it carry
    # recheck_at=0 and are adopted by adopt_legacy_parks() rather than being
    # left inert, because a gate is not a migration.
    ("blocked_on", "TEXT NOT NULL DEFAULT ''"),
    ("recheck_at", "REAL NOT NULL DEFAULT 0"),
    ("recheck_count", "INTEGER NOT NULL DEFAULT 0"),
    ("gate_cmd", "TEXT NOT NULL DEFAULT ''"),
    ("parked_since", "REAL NOT NULL DEFAULT 0"),
    # The self-contained gate's one escape hatch (contract: P1 §3). A lettered
    # list is legible on a phone exactly when the list is ON the phone, so the
    # card carries its own copy. Additive and optional: every ask written
    # before it existed reads back as ''.
    ("ask_context", "TEXT NOT NULL DEFAULT ''"),
    # The bot-close rules' two assertions (contract: P1 §4). They live on the
    # row, not only in the result body, because "the answer was applied and
    # nothing is owed" is a claim about the ROUND, and a run that applies the
    # answer in one result and closes in the next must not have to re-assert
    # what it already said. They are cleared the moment either could stop being
    # true: a new reply from him, or a new question from a run.
    ("answer_applied", "INTEGER NOT NULL DEFAULT 0"),
    ("nothing_owed", "INTEGER NOT NULL DEFAULT 0"),
    ("label", "TEXT NOT NULL DEFAULT ''"),
)


def init(con: sqlite3.Connection) -> None:
    con.executescript(DDL)
    have = {r["name"] for r in con.execute("PRAGMA table_info(threads)")}
    for col, decl in _MIGRATIONS:
        if col not in have:
            con.execute("ALTER TABLE threads ADD COLUMN " + col + " " + decl)
    con.execute("CREATE INDEX IF NOT EXISTS idx_threads_label ON threads(label)")
    cnt = con.execute("SELECT COUNT(*) FROM labels").fetchone()[0]
    if cnt == 0:
        now = time.time()
        con.execute(
            "INSERT OR IGNORE INTO labels (name, description, prompt_template, webhook_url, executor_runtime, convert_to_needs_human, enabled, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("triage", "Default intake triage for new tasks",
             "Triage this intake request, perform any immediate analysis needed, and produce an actionable result or structured ask.",
             "", "hermes", 0, 1, now, now)
        )
        con.execute(
            "INSERT OR IGNORE INTO labels (name, description, prompt_template, webhook_url, executor_runtime, convert_to_needs_human, enabled, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("needs-human", "Converts raw issue into structured decision options for Joey",
             "Review the input and structure it as a decision for Joey. The deliverable MUST be a self-contained needs-you card with kind choice or ack, clear options, and no assumed context.",
             "", "hermes", 1, 1, now, now)
        )
    scnt = con.execute("SELECT COUNT(*) FROM settings").fetchone()[0]
    if scnt == 0:
        now = time.time()
        for k, v in [
            ("default_intake_label", "triage"),
            ("llm_conversion_enabled", "1"),
            ("needs_human_prompt", "Convert raw intake tasks into clear, actionable needs-you choices for Joey with multiple-choice options and no context assumed."),
        ]:
            con.execute("INSERT OR IGNORE INTO settings (key, value, updated_at) VALUES (?, ?, ?)", (k, v, now))
    con.commit()


def db(path: str | None = None) -> sqlite3.Connection:
    p = path or DB_PATH
    parent = os.path.dirname(p)
    if parent:
        os.makedirs(parent, exist_ok=True)
    con = sqlite3.connect(p, timeout=15)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA foreign_keys=ON")
    init(con)
    return con


def _jloads(raw, default):
    try:
        v = json.loads(raw)
    except (TypeError, ValueError):
        return default
    return v if v is not None else default


# --- Events: append-only, content-addressed ----------------------------------
def event_id(thread_id, ts, source, kind, body) -> str:
    raw = "|".join([str(thread_id), str(ts), str(source), str(kind), str(body)])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def add_event(con, thread_id, ts, source, source_id, kind, body) -> None:
    """INSERT OR IGNORE on a content hash, so re-running a pass is idempotent
    rather than additive. `ts` is part of the hash, so the same observation
    re-sent with a new timestamp still appends: the producer sends stable
    timestamps for stable observations."""
    con.execute(
        "INSERT OR IGNORE INTO events (event_id, thread_id, ts, source, source_id, kind, body)"
        " VALUES (?,?,?,?,?,?,?)",
        (event_id(thread_id, ts, source, kind, body), str(thread_id), float(ts),
         str(source or ""), str(source_id or ""), str(kind or ""), str(body or "")))


# --- Validation --------------------------------------------------------------
def slug(s: str, maxlen: int = 48) -> str:
    out = "".join(c if c.isalnum() else "-" for c in (s or "").strip().lower())
    while "--" in out:
        out = out.replace("--", "-")
    return out.strip("-")[:maxlen].strip("-")


def normalize_options(raw):
    """Coerce ask.options into [{label,value,effect}]. Returns (options, errors).

    A language model will hand us an array of bare strings sooner or later.
    Forgive the shape we can repair; refuse the shape we cannot. A silent
    accept here produces a dead screen, which is strictly worse than a loud
    400 the producer can read and correct itself from.
    """
    options, errors = [], []
    if raw is None:
        return options, errors
    if not isinstance(raw, list):
        return options, ["ask.options is " + type(raw).__name__ + ", expected a list"]
    for i, o in enumerate(raw):
        if isinstance(o, str):
            label = o.strip()
            opt = {"label": label, "value": slug(label), "effect": "", "terminal": False}
        elif isinstance(o, dict):
            label = str(o.get("label") or "").strip()
            val = o.get("value")
            val = str(val).strip() if val not in (None, "") else slug(label)
            # `terminal` means picking this IS the whole action: settle it and
            # stop asking. It must not wake an executor, because there is
            # nothing for one to do and it would come back reporting that it
            # could not do what the option promised.
            #
            # `resolved` means picking this option is an assertion that the
            # underlying work is actually finished, not just that Helm should
            # stop asking about it. It is a separate fact from `terminal` on
            # purpose: "leave it, I'll write it myself" and "it's his to
            # finish" are terminal too, but the card is not done, and moving
            # it to Done on a guess is exactly the kind of lie about state
            # this store exists to refuse. Whoever authors the option sets
            # this explicitly; write-back never infers it from the label or
            # effect text.
            opt = {"label": label, "value": val, "effect": str(o.get("effect") or ""),
                   "terminal": bool(o.get("terminal")), "resolved": bool(o.get("resolved"))}
        else:
            errors.append("ask.options[%d] is %s, expected string or object"
                          % (i, type(o).__name__))
            continue
        if not opt["label"]:
            errors.append("ask.options[%d] has an empty label" % i)
            continue
        if not opt["value"]:
            errors.append("ask.options[%d] label %r yields an empty value" % (i, opt["label"]))
            continue
        options.append(opt)
    return options, errors


def normalize_links(raw):
    """Coerce links into [{label,url}]. Returns (links, errors).

    A link is how a thread points at the actual page, card, report or file it
    is about, so it can be opened instead of hunted for by hand. `url` is the
    only required part and is not restricted to http(s): a Windows path or a
    drive share is just as valid a destination, and the UI decides how to
    render it (a real anchor for a web address, visible plain text for a
    path) rather than this rejecting one shape or the other. `label` defaults
    to the url itself when omitted, the same fallback normalize_options uses
    for `value`.
    """
    links, errors = [], []
    if raw is None:
        return links, errors
    if not isinstance(raw, list):
        return links, ["links is " + type(raw).__name__ + ", expected a list"]
    if len(raw) > MAX_LINKS:
        return links, ["links has %d entries, max %d" % (len(raw), MAX_LINKS)]
    for i, item in enumerate(raw):
        if isinstance(item, str):
            url, label = item.strip(), ""
        elif isinstance(item, dict):
            url = str(item.get("url") or "").strip()
            label = str(item.get("label") or "").strip()
        else:
            errors.append("links[%d] is %s, expected string or object"
                          % (i, type(item).__name__))
            continue
        if not url:
            errors.append("links[%d] has no url" % i)
            continue
        if len(url) > MAX_LINK_FIELD:
            errors.append("links[%d].url is %d chars, max %d" % (i, len(url), MAX_LINK_FIELD))
            continue
        links.append({"label": (label or url)[:MAX_LINK_FIELD], "url": url})
    return links, errors


def park_fields(body, now=None):
    """The park contract as pure data. Returns ((blocked_on, recheck_at, gate_cmd),
    errors).

    Returns errors rather than raising so the producer's door can reject one bad
    thread in a payload and still apply the rest (put_threads' partial success),
    while the bot's door raises the first error as a 400. Every message names the
    field and what to send instead, because the reader is usually a model that has
    to correct its own payload without a second round trip.
    """
    now = time.time() if now is None else now
    errors = []

    blocked_on = str(body.get("blocked_on") or "").strip()
    if len(blocked_on) < MIN_BLOCKED_ON:
        errors.append(
            "state 'waiting-external' requires 'blocked_on' (at least %d characters, got "
            "%d): name WHO or WHAT this waits on -- a person, a vendor case, an agent, a "
            "scheduled run, an external date. If the answer is the owner, this is NOT "
            "waiting-external: use 'needs-you' with an ask ('ack' if reading it is the "
            "whole job). If the answer is nobody, the work is not blocked and should "
            "continue." % (MIN_BLOCKED_ON, len(blocked_on)))
    elif _WAITS_ON_OWNER.match(blocked_on):
        errors.append(
            "'blocked_on' is %r, which says this waits on the person the list is FOR. "
            "'waiting-external' renders as \"waiting on someone else\" with no button, "
            "so a park aimed at him waits on him invisibly, forever. Use state "
            "'needs-you' with ask.kind 'ack' (he reads it and closes) or 'choice' (he "
            "decides)." % blocked_on)

    raw = body.get("recheck_at")
    recheck_at = 0.0
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        # Default rather than reject: a run that named a real blocker did the
        # honest thing, and a missing date should not cost it the whole result.
        recheck_at = now + DEFAULT_RECHECK_S
    elif isinstance(raw, bool):
        errors.append("'recheck_at' is a boolean; send epoch seconds or an ISO-8601 date "
                      "(e.g. '2026-10-01' or '2026-10-01T09:00:00'), or omit it to accept "
                      "the %dh default" % (DEFAULT_RECHECK_S // 3600))
    else:
        parsed = _parse_wake_at(raw)
        if parsed is None:
            errors.append("'recheck_at' is %r, which is neither epoch seconds nor an "
                          "ISO-8601 date. Send '2026-10-01' or '2026-10-01T09:00:00' or a "
                          "number, or omit it to accept the %dh default"
                          % (raw, DEFAULT_RECHECK_S // 3600))
        elif parsed > now + MAX_RECHECK_S:
            errors.append("'recheck_at' is %.1f days out and the ceiling is %d: that is "
                          "abandonment wearing a park's clothes. Pick a real date inside "
                          "%d days, or hand it back as 'needs-you' and let him decide"
                          % ((parsed - now) / 86400.0, MAX_RECHECK_S // 86400,
                             MAX_RECHECK_S // 86400))
        else:
            # A date already in the past would hide the thread from its own
            # sweeper's next pass being meaningful; it is honoured as "check
            # again soon" rather than rejected, since nothing about it is a lie.
            recheck_at = parsed if parsed > now else now + DEFAULT_RECHECK_S

    gate_cmd = ""
    raw_gate = body.get("gate")
    if raw_gate is None:
        raw_gate = body.get("gate_cmd")
    if raw_gate is not None:
        if not isinstance(raw_gate, str):
            errors.append("'gate' is %s, expected a single shell command as a string that "
                          "a sweeper can run to test whether the blocker still holds "
                          "(exit 0 = cleared)" % type(raw_gate).__name__)
        else:
            cmd = raw_gate.strip()
            if cmd[:5].upper() == "GATE:":
                cmd = cmd[5:].strip()
            if not cmd:
                errors.append("'gate' was sent empty. Omit the key entirely if there is no "
                              "command that can answer 'has it cleared yet'; an empty one "
                              "reads as a check that always passes")
            elif "\n" in cmd or "\r" in cmd or "\x00" in cmd:
                errors.append("'gate' must be a single line: a sweeper runs it as one "
                              "command and a multi-line script has nowhere to be reviewed")
            elif len(cmd) > MAX_GATE_CMD:
                errors.append("'gate' is %d chars, max %d. Put the logic in a script and "
                              "name the script here" % (len(cmd), MAX_GATE_CMD))
            else:
                gate_cmd = cmd

    return (blocked_on, recheck_at, gate_cmd), errors


def require_park(body, now=None):
    """park_fields, raising the first error as the 400 a caller gets."""
    park, errors = park_fields(body, now)
    if errors:
        raise StoreError(400, "; ".join(errors))
    return park


# --- The self-contained gate (contract: P1 §3; LESSON 17) --------------------
#
# An option is written by something that is looking at the email, the ticket,
# the previous card or its own last message, and then rendered on a phone where
# none of that exists. "Go with the first one" is a complete thought at the
# moment it is authored and an unanswerable riddle ninety seconds later.
#
# The test is the screenshot: the prompt and its buttons, alone, with nothing
# above or below them. Deterministic patterns only -- this must never need a
# model to decide whether a button is legible (LESSON 33).
_EXTERNAL_REF = (
    (re.compile(r"\bas (discussed|described|mentioned|noted|agreed|stated|above|below|"
                r"before|previously|per)\b", re.I),
     "refers to a conversation that is not on the card"),
    # "check"/"read" are deliberately NOT in this verb list: "I go and read the
    # logs" is an ACTION the option performs, not a reference the reader has to
    # chase. Only the pointing verbs count.
    (re.compile(r"\b(see|per|refer to) (the )?(above|below|attached|attachment|"
                r"e-?mail|thread|ticket|card|doc|document|report|link|message|chat|"
                r"screenshot|log|transcript)s?\b", re.I),
     "sends the reader somewhere else to find out what the option means"),
    (re.compile(r"\b(the|my|his|her|their|that) (first|second|third|last|previous|earlier|"
                r"other|former|latter|original) (one|option|choice|suggestion|plan|idea|"
                r"approach|version|answer|reply)\b", re.I),
     "counts or points at something that is not visible here"),
    (re.compile(r"\b(that) (one|thing)\b", re.I),
     "points at a thing the card never names"),
    # "this one" is deliberately absent: on a card, "this" is the card, which is
    # the one piece of context the reader always has. Only a label that is
    # NOTHING but a pointer ("this one" alone) is refused, by _DEICTIC_ONLY.
    (re.compile(r"\b(we|you|i|they|he|she) (agreed|discussed|decided|said|talked about|"
                r"settled on|went over)\b", re.I),
     "refers to a conversation that is not on the card"),
    (re.compile(r"\bsame as (before|last time|above|the previous|the last)\b", re.I),
     "assumes the reader remembers what happened last time"),
    (re.compile(r"\blike last time\b", re.I),
     "assumes the reader remembers what happened last time"),
    (re.compile(r"\bdo (what|as) (he|she|they|it|the note|the e-?mail|the doc|the ticket|"
                r"the card) (said|says|asked|wanted)\b", re.I),
     "defers to text that is not on the card"),
)
# The whole label is a pointer, with nothing pointed at.
_DEICTIC_ONLY = {"it", "that", "this", "them", "those", "these", "same", "the same",
                 "other", "the other", "above", "below", "that one", "this one",
                 "the one", "both", "either", "neither"}
# The whole label is a position in a list the reader cannot see, spelled out in
# words ("the second", "first"). No letter or number to look up, so no amount of
# card context can rescue it: it counts through a list that was never rendered.
_INDEX_ONLY = re.compile(
    r"^(?:the )?(?:first|second|third|fourth|fifth|last|latter|former)(?: one)?$", re.I)
# The whole label is one or more list TOKENS -- "A", "#2", "option 3", "choice b",
# "C+D", "A and B", "options C & D". These are the labels the brief names: a
# producer that has a lettered list in front of it writes the letters, and the
# phone gets a row of buttons reading "C+D" with no C and no D anywhere on it.
# They are recoverable, and only these are: if the SAME card carries the list
# (ask.context), the letters are legible and the ask is allowed to stand.
_TOKEN_LIST_ONLY = re.compile(
    r"^(?:options?|choices?)?\s*"
    r"#?\s*([0-9]{1,2}|[a-z])"
    r"((?:\s*(?:\+|&|/|,|\band\b|\bor\b|\bplus\b)\s*#?\s*(?:[0-9]{1,2}|[a-z]))*)\s*$", re.I)
_TOKEN_SPLIT = re.compile(r"\s*(?:\+|&|/|,|\band\b|\bor\b|\bplus\b)\s*", re.I)
# A letter or number named INSIDE a longer sentence: "Take option 2",
# "Go with C+D but tell them first".
_OPTION_REF = re.compile(r"\b(?:options?|choices?)\s*#?\s*([0-9]{1,2}|[a-z])\b", re.I)
# How a list defines its own entries, in the shapes a run actually writes them:
# "A) renew", "A. renew", "A - renew", "(A) renew", "A: renew", "A = renew".
# A definition needs a body: "A)" alone on a line defines nothing.
_CONTEXT_DEF = re.compile(
    r"(?:^|[\n;|])\s*\(?\s*([0-9]{1,2}|[a-z])\s*[\)\.\:\-–=]\s*(\S[^\n;|]{2,})",
    re.I)
MAX_ASK_CONTEXT = 2000


def context_tokens(context):
    """The list entries this card DEFINES on itself, lowercased.

    `ask.context` is the one legitimate way to keep a lettered list: put the
    list on the card. Then "C+D" is readable in the screenshot, because C and D
    are in the screenshot. A letter mentioned but never defined does not count
    -- that is the same riddle with extra words in front of it.
    """
    text = str(context or "")
    return {m.group(1).lower() for m in _CONTEXT_DEF.finditer(text)}


def _tokens_of(label):
    """The list tokens a label is made ENTIRELY of, or None if it is real text."""
    m = _TOKEN_LIST_ONLY.match(label or "")
    if not m:
        return None
    raw = (m.group(1) + (m.group(2) or "")).strip()
    return [t.lower() for t in _TOKEN_SPLIT.split(raw) if t.strip()]


def self_contained_errors(options, prompt="", context=""):
    """Every reason this choice ask would not survive being screenshotted alone.

    Returned as a list so the producer's door can attach them to one rejected
    thread while the rest of a payload lands, and so a producer can call this
    itself before sending anything.

    `context` is the card's own copy of whatever list the options refer to. It
    is the ONLY thing that licenses a lettered label, and only for the letters
    it actually defines, because it is the only thing that is still there when
    the card is looked at alone.
    """
    errors = []
    if not str(prompt or "").strip():
        errors.append("ask.kind is 'choice' but ask.prompt is empty: the prompt is what the "
                      "buttons are answering. A row of options with no question on the same "
                      "card cannot be answered by someone who is only looking at the card")
    context = str(context or "")
    if len(context) > MAX_ASK_CONTEXT:
        errors.append("ask.context is %d chars, max %d: it renders on the card, so it has to "
                      "be readable there" % (len(context), MAX_ASK_CONTEXT))
    defined = context_tokens(context)
    # A context that itself points offstage ("A) as discussed with the vendor")
    # has moved the riddle rather than solved it.
    for rx, why in _EXTERNAL_REF:
        m = rx.search(context)
        if m:
            errors.append("ask.context %s (%r). The context is what makes the letters "
                          "readable on the card; it cannot itself refer to something that is "
                          "not on the card" % (why, m.group(0)))
            break

    seen = {}
    for i, o in enumerate(options or []):
        label = str((o or {}).get("label") or "").strip()
        effect = str((o or {}).get("effect") or "").strip()
        key = label.lower()
        if key in seen:
            errors.append("ask.options[%d] label %r repeats ask.options[%d]: two buttons that "
                          "read identically cannot be told apart. Say what is different about "
                          "each one" % (i, label, seen[key]))
        else:
            seen[key] = i
        if key in _DEICTIC_ONLY:
            errors.append("ask.options[%d] label %r is a pointer word with nothing on the card "
                          "to point at. Write what picking it DOES, e.g. 'Renew the cert "
                          "today'" % (i, label))
            continue
        if _INDEX_ONLY.match(label):
            errors.append("ask.options[%d] label %r is a position in a list the reader cannot "
                          "see. Label it with the action, not its number: an option has to be "
                          "readable in a screenshot of this card alone" % (i, label))
            continue
        # Labels that are nothing but list tokens, including combinations.
        toks = _tokens_of(label)
        if toks is not None:
            missing = [t for t in toks if t not in defined]
            if missing:
                errors.append(
                    "ask.options[%d] label %r names %s from a list that exists only outside "
                    "this card. Either write what the option DOES (\"Renew the cert and "
                    "rotate the key\"), or put the list itself in ask.context (\"A) renew "
                    "... B) rotate ...\") so the letters are readable in a screenshot of "
                    "this card alone"
                    % (i, label, ", ".join(repr(t.upper()) for t in missing)))
                continue
            # Licensed by the card's own list: nothing further to check on a
            # label that is only those tokens.
            continue
        # A letter or number named inside a longer sentence.
        m = _OPTION_REF.search(label) or _OPTION_REF.search(effect)
        if m and m.group(1).lower() not in defined:
            where = "label" if _OPTION_REF.search(label) else "effect"
            errors.append(
                "ask.options[%d] %s %r names an option by number or letter (%r) instead of by "
                "what it does, and no list on this card defines it. Restate the thing itself, "
                "or put the list in ask.context so %r is readable here"
                % (i, where, label if where == "label" else effect, m.group(0),
                   m.group(1).upper()))
            continue
        for rx, why in _EXTERNAL_REF:
            m = rx.search(label)
            where = "label"
            if not m:
                m = rx.search(effect)
                where = "effect"
            if m:
                errors.append("ask.options[%d] %s %r %s (%r). Options must survive being "
                              "screenshotted alone: restate the thing itself instead of "
                              "referring to it"
                              % (i, where, label if where == "label" else effect, why,
                                 m.group(0)))
                break
    return errors


# --- The needs-you four-test (contract: P3b §1; LESSONS 3, 14, 15, 33) --------
#
# The fleet's filing rule, as code: an ask pages Joey ONLY if ALL FOUR hold --
# only-he-can, still-true-now, he-doesnt-know, agent-cannot-fix. Any test
# failing means the work is NOT his: the ladder continues or the thread settles
# elsewhere. This is the pitfall report's fix made structural (71.5% of the
# retired board's cards were system-generated); a rule a prompt has to remember
# is a rule the next prompt forgets (LESSON 2: topology beats prompt changes).
#
# Every test is deterministic -- markers, timestamps, token fingerprints,
# budgets. No embeddings, no model: freshness/dedup/urgency are FACTS about the
# data, and facts get code (LESSON 33).
REQUIRES_MARKERS = ("decision", "money", "credential", "presence", "contact")

# T4's ladder context, at the doors that have one. None = the caller is not the
# ladder (a producer PUT): T4 is embodied there by the ported invariants rather
# than re-derived, so a producer is never refused for "retry budget" it does
# not have.
MAX_RETRY_ATTEMPTS_CONTEXT = 5

# How old a closed duplicate may be and still count as "he has seen this
# concern recently". Same horizon as the park contract's expiry: a month.
DUPE_WINDOW_S = 30 * 86400

# The refusal detail per test: names the test AND the fix, because the reader
# is a producer that has to correct its own payload (P1 §6).
_NJ_FIX = {
    "T1-only-he-can": (
        "send ask.requires naming which only-he-can capability the next step "
        "needs, one of %s. If the next step is none of those, the work is not "
        "his: keep climbing the ladder (retry, park with a blocker, or settle)"
        % ", ".join(REQUIRES_MARKERS)),
    "T2-still-true-now": (
        "he has replied since this ask was generated, so the ask speaks about a "
        "round he already answered. Do not re-ask: act on his reply (it re-queues "
        "the work) or report on it"),
    "T3-he-doesnt-know": (
        "he already has an ask open (or recently settled) covering this same "
        "concern, so a second card adds noise, not information. Route the work "
        "through the existing thread's stable id"),
    "T4-agent-cannot-fix": (
        "the ladder is not exhausted: retry budget or a self-fix path remains "
        "(other executor, re-check, wait for a dependency). Only genuinely "
        "agent-unfixable work pages him"),
}


def needs_joey(thread, context=None):
    """The four-test. Returns (needs_you: bool, failed_test: str).

    Pure decision over supplied data; SQL only through the optional `con` in
    the context, and only to complete a dedupe scan the caller did not. Tests
    run in order T1..T4 and the first failure is returned -- a producer fixing
    its payload wants the one reason, not four.
    """
    context = context or {}
    t = thread if isinstance(thread, dict) else {}
    ask = t.get("ask") or {}
    if not isinstance(ask, dict):
        ask = {}
    tid = str(t.get("thread_id") or "")
    ask_ts = float(context.get("ask_ts") or 0)
    now_ = float(context.get("now") or time.time())

    # T1 only-he-can: the next step requires his decision, his money, his
    # account/credential, his physical presence, or a person only he can
    # contact. Evidence is EXPLICIT markers the producer states; absent
    # markers mean T1 fails -- a producer that cannot name the capability is
    # a producer that has not established it (LESSON 15's evidence rule).
    requires = ask.get("requires")
    if isinstance(requires, str):
        requires = [requires]
    markers = {str(m).strip().lower() for m in (requires or []) if str(m).strip()}
    markers &= set(REQUIRES_MARKERS)   # unknown markers die at validation (§1.3)
    if not markers:
        return False, "T1-only-he-can"

    # T2 still-true-now: no newer event contradicts the ask. His reply, his
    # note, or his settle on THIS thread after the ask's generation means the
    # ask describes a round he already acted on.
    owner_ts = float(context.get("owner_event_ts") or 0)
    if ask_ts and owner_ts > ask_ts:
        return False, "T2-still-true-now"

    # T3 he-doesnt-know: no open/recently-closed duplicate ask, no
    # answered-decision covering it, no ops-log hit. Dedupe by stable id and
    # deterministic title fingerprint -- the same normalizer the mute surface
    # uses (numbers/units/stopwords stripped), proven against re-derived
    # concerns whose titles differ only by measurement.
    dupes = context.get("duplicates")
    if dupes is None and context.get("con") is not None:
        con = context["con"]
        fp = fingerprint(str(t.get("title") or ""))
        dupes = []
        for r in con.execute(
                "SELECT thread_id, title, state, ask_kind, ask_prompt, last_change"
                " FROM threads WHERE tombstoned=0 AND suppressed=0").fetchall():
            if r["thread_id"] != tid and fingerprint(r["title"]) == fp:
                dupes.append(dict(r))
    for d in dupes or []:
        state = d.get("state")
        if state == "needs-you" and (d.get("ask_kind") or "none") != "none":
            return False, "T3-he-doesnt-know"   # open ask OR answered decision
        if state == "done" and (now_ - float(d.get("last_change") or 0)) <= DUPE_WINDOW_S:
            return False, "T3-he-doesnt-know"   # recently settled
    if context.get("ops_log_hits"):
        return False, "T3-he-doesnt-know"

    # T4 agent-cannot-fix: no retry budget left AND no self-fix path applies.
    # `ladder` None = the caller is not the ladder; the test passes for it
    # (the ported invariants embody T4 at that door, contract §1.2).
    ladder = context.get("ladder")
    if ladder is not None:
        budget_left = int(ladder.get("retry_budget_left") or 0)
        self_fix = ladder.get("self_fix_paths") or []
        if budget_left > 0 or self_fix:
            return False, "T4-agent-cannot-fix"

    return True, ""


def assert_needs_joey(thread, context=None):
    """The four-test as a rejection. Raises StoreError(400) naming the failed
    test and the fix; returns quietly when the ask may page him."""
    ok, failed = needs_joey(thread, context)
    if ok:
        return
    raise StoreError(400, "needs-you refused: %s: %s" % (failed, _NJ_FIX[failed]),
                     {"failed_test": failed})


def _nj_ask_of(thread_payload):
    """The four-test's view of an ask payload: requires markers included."""
    ask = thread_payload.get("ask") if isinstance(thread_payload.get("ask"), dict) else {}
    return {"kind": ask.get("kind"), "prompt": ask.get("prompt"),
            "options": ask.get("options") or [], "requires": ask.get("requires")}


def needs_joey_gate_errors(con, c, existing, now=None):
    """The four-test at the producer's door. Returns [] (may light) or [detail].

    Called by put_threads only for a NEW ask reaching the board (contract
    §1.2): a fresh lit thread, or re-lighting one whose stored ask is 'none'.
    A thread already carrying an ask under the same stable id is a licensed
    resync -- P1 invariant 3 still protects whatever he has answered -- so it
    is not re-interrogated.
    """
    now = time.time() if now is None else now
    tid = c["thread_id"]
    owner = con.execute(
        "SELECT MAX(ts) FROM events WHERE thread_id=? AND (kind='joey-reply'"
        " OR kind='joey-note' OR (kind='state-change' AND source='joey'))",
        (tid,)).fetchone()[0] or 0
    fp = fingerprint(c["title"])
    dups = []
    for r in con.execute(
            "SELECT thread_id, title, state, ask_kind, ask_prompt, last_change"
            " FROM threads WHERE tombstoned=0 AND suppressed=0 AND thread_id != ?",
            (tid,)).fetchall():
        if fingerprint(r["title"]) == fp:
            dups.append(dict(r))
    thread = {"thread_id": tid, "title": c["title"], "ask": _nj_ask_of({"ask": {
        "kind": c["ask_kind"], "prompt": c["ask_prompt"],
        "options": c["ask_options"], "requires": c.get("requires")}})}
    ctx = {"existing": dict(existing) if existing is not None else None,
           "owner_event_ts": float(owner), "ask_ts": now, "duplicates": dups}
    ok, failed = needs_joey(thread, ctx)
    if ok:
        return []
    return ["needs-you refused: %s: %s" % (failed, _NJ_FIX[failed])]


# --- The bare-ask rules (contract: P3b §4; LESSONS 15, 11, 13, 3) -------------
#
# The retired board's worst card was lit, optionless, and asked nothing: "what
# do you want". Multiple choice or nothing (LESSON 15); a note that is a
# statement is not an ask (LESSON 11); reading it is a tap, not a decision
# (LESSON 13); Helm asks only what needs the owner (LESSON 3).
BANNED_ASK_PHRASE = "what do you want"

# A prompt that is NOTHING but a demand for direction, whole-prompt match.
# Deliberately small (contract §open-questions 8): under-matching leaves one
# odd text ask standing; over-matching eats a real question, which is the
# failure P1 §2 exists for. Same direction as the '?' detector.
_PURE_DIRECTION = re.compile(
    r"^(?:what|how)\s+(?:do|should|would|shall)\s+(?:you|we|i)\s+"
    r"(?:want|do|proceed|handle|act)(?:\s+(?:about|with|now|next|this))?\??$"
    r"|^what\s+(?:now|next)\??$"
    r"|^your\s+call\??$"
    r"|^(?:awaiting|await)\s+(?:your\s+)?(?:decision|instructions|input)\.?$"
    r"|^tell\s+me\s+what\s+to\s+do\??$"
    r"|^(?:how|what)\s+next\??$", re.I)


def bare_ask_errors(kind, prompt, options, context=""):
    """Every reason this ask is the optionless card, at any door."""
    errors = []
    hay = " | ".join([str(prompt or ""), str(context or "")] +
                     [str((o or {}).get("label") or "") + " " +
                      str((o or {}).get("effect") or "") for o in (options or [])])
    if BANNED_ASK_PHRASE in hay.lower():
        errors.append(
            "this ask lights the exact card the optionless-ask rule exists to stop: "
            "the phrase %r is banned in every stored ask (prompt, option, context, "
            "at either door). Multiple choice or nothing: give each option as an "
            "action he can read alone, or send an ack if reading it is the whole job"
            % BANNED_ASK_PHRASE)
    return errors


def pure_direction_demand(prompt):
    """True when the whole prompt is a demand for direction with no substance.

    The shape the rule is for: a run at the end of its rope typing 'how should
    we proceed?' -- which looks like a question and is actually the thinking
    being offloaded. A REAL question ('Which certificate did you mean, the
    relay or the site one?') does not match and stands.
    """
    return bool(_PURE_DIRECTION.match(str(prompt or "").strip()))


def rewrite_to_ack_summary(outcome, note):
    """The deterministic rewrite of a pure direction-demand into an honest
    card: what stopped, where, and that any reply reopens it. Never a
    question, never 'what do you want'."""
    clip = " ".join(str(note or "").split())[:200]
    return ("Work stopped at %s: %s — next options unknown. Reply anything to "
            "reopen." % (str(outcome or "an impasse")[:40], clip))


# --- The bot-close rules (contract: P1 §4; LESSONS 13, 18, 12, 10) -----------
#
# Only Joey closes. A thread closed by a bot is work that disappeared off the
# list without anyone agreeing it was finished, and the whole point of the list
# is that it tells the truth about what is outstanding. There are exactly two
# mechanical exceptions, and both are cases where HE already did the closing and
# the store is only catching up with him:
#
#   ack-tapped        -- the ask was "read this" and he tapped it.
#   answered-decision -- he answered a choice/text ask, the run applied that
#                        answer, and nothing is left owed to him.
#
# Everything else is a 400. A redirect on an ack card is not an ack (LESSON 13):
# it is him sending the run off to do something else, which is the opposite of
# "we are finished here".
#
# Bodies that are a tap, not a decision. "close" and "ack" are buttons; an empty
# reply carries a note only; "__redirect__" is him asking for something ELSE,
# which is the loudest possible sign that something is still owed.
NON_DECISION_REPLIES = {"ack", "close", "note-only", "__redirect__", ""}


def _generation_start(con, thread_id) -> float:
    """When this thread's CURRENT life began.

    A thread id is stable across passes, so a concern that was retired and
    later refiled under the same id would otherwise inherit the previous
    life's reply and close itself on an answer to a question nobody asked
    this time round.
    """
    row = con.execute(
        "SELECT MAX(ts) FROM events WHERE thread_id=? AND kind='state-change'"
        "   AND (body LIKE 'retired%' OR body LIKE 'auto-retired%'"
        "        OR body LIKE 'reopened by you%')", (thread_id,)).fetchone()
    return float(row[0] or 0)


def newest_ask_ts(con, thread_id, since=0.0) -> float:
    """When the live question was last asked, from the event log.

    `ask_prompt` is a mutable column: the next result overwrites it, and then
    "did he reply AFTER the ask" is unanswerable one generation back. The
    bot-ask event is the durable record of the question having been put.
    """
    row = con.execute(
        "SELECT MAX(ts) FROM events WHERE thread_id=? AND kind='bot-ask' AND ts > ?",
        (thread_id, since)).fetchone()
    return float(row[0] or 0)


def bot_close_reason(con, row, answer_applied=None, nothing_owed=None,
                     no_action=None):
    """(reason, refusal) -- which mechanical exception permits this close.

    Returns (None, detail) when none of them does, with a detail that names the
    two ways through and the one the caller is closest to.
    """
    tid = row["thread_id"]
    if row["state"] == "done":
        # Already closed, by him or by a tap that settled. A late result from a
        # run that was still in flight is not a bot closing anything, and
        # refusing it would make a finished run look failed.
        return "already-closed", ""
    gen = _generation_start(con, tid)
    replies = con.execute(
        "SELECT ts, body FROM events WHERE thread_id=? AND kind='joey-reply' AND ts > ?"
        " ORDER BY ts ASC", (tid, gen)).fetchall()
    kind = row["ask_kind"]

    # Contract: P7 §2 — the terminal no-action verdict. The run asserts, as a
    # literal boolean recorded as data (never read out of prose, LESSON 12),
    # that his instruction required no thread action beyond recording it. The
    # instruction itself is the evidence: a reply from him newer than the
    # current ask — INCLUDING a note-only reply, which is binding (LESSON 15);
    # the observed wedge (2026-09-13: one thread claimed 18 times, another
    # burned 6 rounds) was exactly a note-shaped
    # instruction the executor rightly read as "record, don't act" and had no
    # honest way to settle. The answer gate and the note floor have ALREADY run
    # by the time this is called, and are not exempted.
    if no_action:
        ask_ts = newest_ask_ts(con, tid, gen)
        if any(r["ts"] > ask_ts for r in replies):
            return "no-action-settle", ""
        return None, (
            "'no_action' settles a thread on the strength of HIS reply (a "
            "note-only reply is binding), but this thread has no reply from him "
            "newer than the question on it — there is nothing his words "
            "instructed that 'no action' could settle. Report with state "
            "'needs-you' and an ask instead")

    if kind == "ack":
        if any((r["body"] or "").strip().lower() == "ack" for r in replies):
            return "ack-tapped", ""
        return None, (
            "a bot may not set 'done'; only Joey closes a thread. This one carries an 'ack' "
            "ask that he has not tapped"
            + (" (his last reply was %r, which is him sending you somewhere else, not "
               "agreeing this is finished)" % (replies[-1]["body"] or "")[:40]
               if replies else "")
            + ". Report with state 'needs-you' and keep the ack: the close is his tap")

    if kind in ("choice", "text"):
        ask_ts = newest_ask_ts(con, tid, gen)
        decided = [r for r in replies
                   if r["ts"] > ask_ts
                   and (r["body"] or "").strip().lower() not in NON_DECISION_REPLIES]
        if not decided:
            return None, (
                "a bot may not set 'done'; only Joey closes a thread. The mechanical "
                "exception for an answered decision does not apply: there is no reply from "
                "him newer than the question currently on this card (a bare 'ack'/'close' "
                "tap or a note with no choice in it is not a decision). Report with state "
                "'needs-you' and leave the question up")
        if not (answer_applied and nothing_owed):
            return None, (
                "a bot may not set 'done' on an answered decision unless it states, "
                "explicitly, that it finished the job: send answer_applied: true AND "
                "nothing_owed: true in this result (got answer_applied=%r, "
                "nothing_owed=%r). Completion is never read out of the note's prose -- if "
                "either is not true, this is 'needs-you' with an ask or "
                "'waiting-external' with a blocker"
                % (answer_applied, nothing_owed))
        return "answered-decision", ""

    return None, (
        "a bot may not set 'done'; only Joey closes a thread. This one has no ask on it for "
        "him to have answered or acknowledged (state %r, ask %r), so there is nothing that "
        "could have closed it but you. If the work really is finished, put the ack ask IN "
        "this post -- state 'needs-you' with ask.kind 'ack' -- and his tap closes it"
        % (row["state"], kind))


def _bool_flag(body, key):
    """An assertion, or None when the caller did not make one.

    Strings are refused on purpose: "true", "yes" and "done" are prose, and
    reading completion out of prose is the failure this exists to stop
    (LESSON 12).
    """
    if key not in body or body[key] is None:
        return None
    v = body[key]
    if isinstance(v, bool):
        return v
    raise StoreError(400, "%r is %r; it must be a literal true or false, not a string or a "
                          "number. It is an assertion you are making about the work, and it "
                          "is recorded as one" % (key, v))


def _ask_event_body(kind, prompt, options, context=""):
    """The question, written into history the way a reader reads it."""
    lines = ["ASKED YOU (" + kind + "): " + (prompt or "").strip()]
    if context.strip():
        lines.append(context.strip())
    for o in options or []:
        label = str((o or {}).get("label") or "").strip()
        effect = str((o or {}).get("effect") or "").strip()
        lines.append("- " + label + ((" -- " + effect) if effect else ""))
    return "\n".join(lines)


# --- The answer gate (contract: P1 §2; LESSON 16) ----------------------------
#
# The binding note rides with an answer and is read by whoever executes it. A
# QUESTION inside that note had nowhere to be answered: the run did the work,
# reported what it did, and the question was never mentioned again -- so the card
# looked dealt with while the one thing actually asked for was dropped.
#
# Detection is a regex, not a model (LESSON 33): the sentence ends in '?', that
# is the whole test. Over-triggering costs a run one sentence; under-triggering
# loses the question.
MIN_ANSWER = 12
_QUESTION = re.compile(r"[^.!?\n]*\?")


def questions_in(text):
    """Every question-shaped fragment in a note, verbatim, for quoting back."""
    return [q.strip() for q in _QUESTION.findall(str(text or "")) if q.strip()]


def normalize_answers(raw):
    """Coerce `answers` into the block rendered above the report.

    Accepts a string (the answer, as prose), a {question, answer} object, or a
    list of either. Returns (text, errors). The pair form is preferred when more
    than one thing was asked, because an unlabelled wall of prose answering three
    questions is indistinguishable from one answering none of them.
    """
    errors = []
    if raw is None:
        return "", errors

    def one(item, where):
        if isinstance(item, str):
            return item.strip()
        if isinstance(item, dict):
            q = str(item.get("question") or "").strip()
            a = str(item.get("answer") or "").strip()
            if not a:
                errors.append("answers%s has no 'answer': send {question, answer} with the "
                              "actual reply in 'answer'" % where)
                return ""
            return ("Q: " + q + "\nA: " + a) if q else a
        errors.append("answers%s is %s, expected a string or {question, answer}"
                      % (where, type(item).__name__))
        return ""

    if isinstance(raw, list):
        if not raw:
            errors.append("'answers' is an empty list, which answers nothing. Send the "
                          "reply as a string, or [{question, answer}, ...]")
            return "", errors
        parts = [p for p in (one(it, "[%d]" % i) for i, it in enumerate(raw)) if p]
        return "\n\n".join(parts), errors
    if isinstance(raw, (str, dict)):
        return one(raw, ""), errors
    errors.append("'answers' is %s, expected a string or a list of {question, answer}"
                  % type(raw).__name__)
    return "", errors


def unanswered_questions(con, thread_id):
    """Questions in notes the owner has written since the last report landed.

    "Since the last report" is what makes a question consumable: once a result
    lands, the notes it was answering are behind it, and a later run is not asked
    to re-answer them. Notes with no question mark contribute nothing.
    """
    last_report = con.execute(
        "SELECT MAX(ts) FROM events WHERE thread_id=? AND kind='bot-result'",
        (thread_id,)).fetchone()[0] or 0
    rows = con.execute(
        "SELECT body FROM events WHERE thread_id=? AND kind='joey-note' AND ts > ?"
        " ORDER BY ts ASC", (thread_id, last_report)).fetchall()
    out = []
    for r in rows:
        out.extend(questions_in(r["body"]))
    return out


def validate_thread(t):
    """Pure validation and normalization. Returns (clean, errors).

    Anything outside a valid set is an error rather than a silent coercion:
    a wrong urgency quietly rewritten to 'whenever' is a lie the store then
    serves forever.

    thread_id is REQUIRED. The reference implementation fell back to a fresh
    uuid4() when it was missing, which means a producer that forgets it emits
    a complete set of duplicates on every single pass.
    """
    errors = []
    if not isinstance(t, dict):
        return None, ["thread is " + type(t).__name__ + ", expected object"]

    tid = str(t.get("thread_id") or "").strip()
    if not tid:
        errors.append("thread_id is required and must be stable across passes for the same "
                      "underlying concern; without it every pass creates duplicates")

    urgency = str(t.get("urgency") or "whenever")
    if urgency not in VALID_URGENCY:
        errors.append("urgency %r not one of %s" % (urgency, sorted(VALID_URGENCY)))

    state = str(t.get("state") or "needs-you")
    if state not in VALID_STATE:
        errors.append("state %r not one of %s" % (state, sorted(VALID_STATE)))

    ask = t.get("ask")
    if ask is None:
        ask = {}
    if not isinstance(ask, dict):
        errors.append("ask is " + type(ask).__name__ + ", expected object")
        ask = {}

    kind = str(ask.get("kind") or "none")
    if kind not in VALID_ASK:
        errors.append("ask.kind %r not one of %s" % (kind, sorted(VALID_ASK)))

    # The four-test's T1 evidence (contract: P3b §1.3). Producers state which
    # only-he-can capability the next step needs, as data. Unknown markers are
    # a loud 400 naming the set, never a silent T1 failure on a typo'd value.
    requires = ask.get("requires")
    if isinstance(requires, str):
        requires = [requires]
    if requires is not None and not isinstance(requires, list):
        errors.append("'ask.requires' is %s, expected a marker or a list of markers "
                      "from %s (contract: P3b §1.3: state which only-he-can "
                      "capability the next step needs)"
                      % (type(requires).__name__, list(REQUIRES_MARKERS)))
        requires = None
    if requires is not None:
        bad = [m for m in requires
               if not isinstance(m, str) or m.strip().lower() not in REQUIRES_MARKERS]
        if bad:
            errors.append("'ask.requires' has unknown marker(s) %r; valid: %s. A "
                          "marker names the only-he-can capability the NEXT step "
                          "needs -- decision, money, credential, presence, or "
                          "contact. No markers at all is legal but the four-test "
                          "will refuse the ask (T1)"
                          % (bad[:3], list(REQUIRES_MARKERS)))

    options, opt_errors = normalize_options(ask.get("options"))
    errors.extend(opt_errors)
    if kind == "choice" and not options:
        errors.append("ask.kind is 'choice' but no usable options survived normalization")
    ask_context = str(ask.get("context") or "")
    if kind == "choice" and options:
        # Contract: P1 §3. The producer's door: a pass that authors options while
        # looking at the source material is exactly the writer that produces
        # "as discussed" buttons.
        errors.extend(self_contained_errors(options, ask.get("prompt"), ask_context))

    # The bare-ask rules, producer's door (contract: P3b §4): R1 banned phrase
    # (mirrored from the bot's door so neither door can store it), R2 a text
    # statement is not an ask, R3 a pure direction-demand is refused with the
    # fix named -- a producer can correct its payload.
    errors.extend(bare_ask_errors(kind, ask.get("prompt"), options, ask_context))
    if state == "needs-you":
        if kind == "text":
            prompt_s = str(ask.get("prompt") or "").strip()
            if not prompt_s:
                errors.append("ask.kind is 'text' but ask.prompt is empty")
            elif "?" not in prompt_s:
                errors.append(
                    "state 'needs-you' with a text ask whose prompt has no question "
                    "mark: a statement is not an ask. Use ask.kind 'ack' if reading "
                    "it is the whole job, or 'choice' with options that say what "
                    "picking them does (contract: P3b §4 R2)")
            elif pure_direction_demand(prompt_s):
                errors.append(
                    "the text ask %r is a demand for direction, not a question: it "
                    "offloads the thinking instead of asking anything. Either ask "
                    "the real question (name the options inline), or report with "
                    "ask.kind 'ack' and let the store write the summary itself "
                    "(contract: P3b §4 R3)" % prompt_s[:80])

    links, link_errors = normalize_links(t.get("links"))
    errors.extend(link_errors)

    # Invariant 4a, applied at the producer's door as well as at the bot's.
    if state == "needs-you" and kind == "none":
        errors.append("state is 'needs-you' but ask.kind is 'none': a lit thread with no "
                      "question renders as a bare Close button, which forces the work to be "
                      "discarded in order to clear it. Use 'ack' if reading it is genuinely "
                      "the whole job, or set state to 'waiting-external'")

    # The park contract, at the producer's door (contract: P1 §1). A producer
    # resyncs the same concern every pass, so if it parks a thread it must keep
    # re-stating who it waits on; a pass that stops naming the blocker is how a
    # park quietly becomes a drop.
    park = ("", 0.0, "")
    if state == "waiting-external":
        park, park_errors = park_fields(t)
        errors.extend(park_errors)

    label = str(t.get("label") or "").strip().lower()
    if label and not LABEL_NAME_RE.match(label):
        errors.append("label must match ^[a-z0-9][a-z0-9_-]{1,31}$")

    if errors:
        return None, errors

    return {
        "label": label,
        "blocked_on": park[0],
        "recheck_at": park[1],
        "gate_cmd": park[2],
        "thread_id": tid,
        "title": str(t.get("title") or ""),
        "summary": str(t.get("summary") or ""),
        "category": str(t.get("category") or ""),
        "urgency": urgency,
        "state": state,
        "ask_kind": kind,
        "ask_prompt": str(ask.get("prompt") or ""),
        "ask_options": options,
        "ask_context": ask_context,
        # The four-test's T1 evidence, kept as DOOR evidence for the gate that
        # fires in put_threads; never persisted on the row (contract: P3b
        # §1.3 / §open-questions 5).
        "requires": [m for m in (requires or [])
                     if isinstance(m, str) and m.strip().lower() in REQUIRES_MARKERS],
        "responder": t.get("responder"),
        "sources": t.get("sources") or [],
        "links": links,
        "first_seen": t.get("first_seen"),
        "events": t.get("events") or [],
    }, []


# --- Mute fingerprints -------------------------------------------------------
# Deliberately NOT the title. Titles embed live measurements ("E: at 11.4%
# free" one day, "at 7.8%" the next), so title-based dedupe is already
# defeated in practice. Strip every number and unit before hashing.
_PRODUCER = re.compile(r"^\s*\[([^\]]{1,64})\]\s*")
_NUM_UNIT = re.compile(
    r"\b\d[\d,._]*\s*(?:%|gib|gb|mib|mb|kib|kb|tib|tb|ms|s|m|h|d|days?|hours?|mins?)\b", re.I)
_BARE_NUM = re.compile(r"\b\d[\d,._:/-]*\b")
_PUNCT = re.compile(r"[^a-z0-9\s]+")
_STOP = {"the", "a", "an", "is", "are", "was", "were", "be", "been", "being", "to", "of",
         "and", "or", "in", "on", "at", "for", "with", "by", "from", "as", "it", "its",
         "this", "that", "these", "those", "has", "have", "had", "not", "no", "but"}


def normalize_subject(title: str) -> str:
    t = _PRODUCER.sub("", title or "").lower()
    t = _NUM_UNIT.sub(" ", t)
    t = _BARE_NUM.sub(" ", t)
    t = _PUNCT.sub(" ", t)
    return " ".join(w for w in t.split() if w and w not in _STOP)


def fingerprint(title: str) -> str:
    return hashlib.sha256(normalize_subject(title).encode("utf-8")).hexdigest()[:20]


def producer_of(title: str, sources) -> str:
    m = _PRODUCER.match(title or "")
    if m:
        return m.group(1).strip().lower()
    if isinstance(sources, list) and sources:
        first = sources[0]
        if isinstance(first, dict):
            return str(first.get("source") or "unknown").lower()
    return "unknown"


# --- Reads -------------------------------------------------------------------
def _row_to_thread(con, r, with_events=True):
    out = {
        "thread_id": r["thread_id"],
        "title": r["title"],
        "summary": r["summary"],
        "category": r["category"] or "Uncategorized",
        "urgency": r["urgency"],
        "state": r["state"],
        "ask": {
            "kind": r["ask_kind"],
            "prompt": r["ask_prompt"],
            "options": _jloads(r["ask_options"], []),
            # Rendered with the options, because it is the thing that makes
            # lettered options readable (contract: P1 §3).
            "context": r["ask_context"],
        },
        "responder": r["responder"],
        "sources": _jloads(r["sources"], []),
        "links": _jloads(r["links"], []),
        "first_seen": r["first_seen"],
        "last_change": r["last_change"],
        "last_note": r["last_note"],
        "wake_at": r["wake_at"],
        "parked": bool(r["parked"]),
        "parked_reason": r["parked_reason"],
        # The two assertions a run has made about this round (contract: P1 §4).
        # Rendered because they are claims someone made, and a claim that is
        # only checked and never shown is a claim nobody can dispute.
        "answer_applied": bool(r["answer_applied"]),
        "nothing_owed": bool(r["nothing_owed"]),
        # The park's machine-checkable exit, rendered with the thread so a
        # reader can tell a real wait from a dropped one without opening
        # anything (contract: P1 §1).
        "blocked_on": r["blocked_on"],
        "recheck_at": r["recheck_at"],
        "recheck_count": r["recheck_count"],
        "gate_cmd": r["gate_cmd"],
        "label": r["label"] if "label" in r.keys() else "",
    }
    if with_events:
        out["events"] = [
            {"ts": e["ts"], "source": e["source"], "source_id": e["source_id"],
             "kind": e["kind"], "body": e["body"]}
            for e in con.execute(
                "SELECT ts, source, source_id, kind, body FROM events"
                " WHERE thread_id=? ORDER BY ts ASC", (r["thread_id"],))
        ]
    return out


def categories_of(threads):
    """Computed at read time from the live set, never read from the table.
    A category is a grouping of what is actually there right now."""
    buckets = {}
    for t in threads:
        buckets.setdefault(t.get("category") or "Uncategorized", []).append(t)
    out = []
    for name, items in buckets.items():
        items = sorted(items, key=lambda x: (URGENCY_ORDER.get(x.get("urgency"), 9),
                                             -(x.get("last_change") or 0)))
        out.append({"name": name, "count": len(items),
                    "max_urgency": items[0].get("urgency") or "whenever",
                    "top_title": items[0].get("title") or ""})
    return sorted(out, key=lambda c: (URGENCY_ORDER.get(c["max_urgency"], 9), -c["count"]))


def wake_due(con) -> int:
    """Hand back every thread whose snooze has expired.

    Runs inside the read, so opening the app is what wakes a deferred thread.
    No cron on another host that can silently stop.
    """
    now = time.time()
    rows = con.execute(
        "SELECT thread_id, wake_note, wake_ask FROM threads"
        " WHERE wake_at > 0 AND wake_at <= ? AND tombstoned=0 AND suppressed=0"
        " AND state != 'done'", (now,)).fetchall()
    for r in rows:
        ask = _jloads(r["wake_ask"], {}) or {}
        kind = str(ask.get("kind") or "ack")
        if kind not in VALID_ASK or kind == "none":
            kind = "ack"  # never wake into a bare Close button
        options, _ = normalize_options(ask.get("options"))
        note = (r["wake_note"] or "").strip() or (
            "Bringing this back, as you asked. Nothing has changed on it since you deferred "
            "it; this is the reminder itself.")
        add_event(con, r["thread_id"], now, "helm-wake", "", "bot-result", note)
        con.execute(
            "UPDATE threads SET state='needs-you', ask_kind=?, ask_prompt=?, ask_options=?,"
            " ask_context=?, wake_at=0, wake_note='', wake_ask='', last_change=?"
            " WHERE thread_id=?",
            (kind, str(ask.get("prompt") or ""), json.dumps(options),
             str(ask.get("context") or ""), now, r["thread_id"]))
    if rows:
        con.commit()
    return len(rows)


def due_rechecks(con):
    """Parked threads whose recheck date has passed. Reads only, writes nothing.

    Separate from the transition on purpose: the sweeper may have to RUN a gate
    command between finding a thread and deciding what to do with it, and the
    store does not run commands (contract: P1 §6). This hands out the command as
    data; apply_recheck() takes the verdict back.

    Snoozed rows (wake_at > 0) are excluded: they already have a promised return
    date, honoured by wake_due(), and one exit per thread is the whole point.
    """
    now = time.time()
    rows = con.execute(
        "SELECT thread_id, title, blocked_on, gate_cmd, recheck_at, recheck_count,"
        " parked_since FROM threads"
        " WHERE state='waiting-external' AND recheck_at > 0 AND recheck_at <= ?"
        "   AND wake_at = 0 AND tombstoned=0 AND suppressed=0"
        " ORDER BY recheck_at ASC", (now,)).fetchall()
    return [dict(r) for r in rows]


# What a 30-day-old park becomes. It is a real ask, with real options, because
# the alternative -- re-parking it for another month -- is how a thread nobody
# is chasing outlives the reason it was opened. Each option says what picking it
# does, and each survives being read on its own (contract: P1 §3).
def _expiry_ask(blocked_on, days):
    return {
        "kind": "choice",
        "prompt": ("This has been parked %d days waiting on %s and it still has not "
                   "cleared. What do you want to do with it?" % (days, blocked_on)),
        "options": [
            {"label": "Keep waiting and chase it again",
             "value": "keep-waiting",
             "effect": "Hands this to a run that chases the blocker again and re-parks "
                       "it with a fresh recheck date.",
             "terminal": False, "resolved": False},
            {"label": "Stop tracking it and file it away",
             "value": "stop-tracking",
             "effect": "Settles the thread here. Nothing is written anywhere else and no "
                       "work is claimed as finished.",
             "terminal": True, "resolved": False},
            {"label": "It already happened, close it out",
             "value": "already-happened",
             "effect": "Settles the thread and records the underlying work as finished.",
             "terminal": True, "resolved": True},
        ],
    }


def apply_recheck(con, thread_id, gate_passed=None, detail=""):
    """The recheck transition, given the sweeper's verdict on the gate.

    gate_passed:
      True / None -- no gate, or the blocker cleared: the thread returns to the
                     work queue ('working') with a clean dispatch slate, so the
                     next pass offers it rather than waiting out a stale timer.
      False       -- the blocker demonstrably still holds: re-armed for another
                     look, and counted. Unless the park has now run 30 days, in
                     which case it expires into a real ask instead of quietly
                     renewing itself forever.

    Every branch writes an event, because a thread that keeps rechecking without
    resolving is a real signal and belongs in the history rather than in a
    counter nobody reads.
    """
    row = _require_thread(con, thread_id)
    if row["state"] != "waiting-external" or (row["wake_at"] or 0) > 0:
        raise StoreError(400, "only a parked thread can be rechecked: this one is %r%s. A "
                              "snoozed thread comes back through its own wake date instead"
                              % (row["state"], " and snoozed" if (row["wake_at"] or 0) > 0
                                 else ""))
    now = time.time()
    n = int(row["recheck_count"] or 0) + 1
    blocked_on = row["blocked_on"] or "(unspecified)"
    detail = str(detail or "").strip()

    if gate_passed is False:
        age_days = int((now - (row["parked_since"] or now)) // 86400)
        if (now - (row["parked_since"] or now)) >= MAX_RECHECK_S:
            ask = _expiry_ask(blocked_on, age_days)
            body = ("This park has run %d days waiting on %s and %d recheck(s) have not "
                    "cleared it%s. A park is not allowed to outlive its own deadline, so "
                    "it comes back to you as a decision instead of renewing itself again."
                    % (age_days, blocked_on, n, (": " + detail) if detail else ""))
            add_event(con, thread_id, now, "helm-park-expiry", "", "bot-result", body)
            con.execute(
                "UPDATE threads SET state='needs-you', ask_kind=?, ask_prompt=?,"
                " ask_options=?, blocked_on='', recheck_at=0, gate_cmd='', parked_since=0,"
                " recheck_count=?, last_change=? WHERE thread_id=?",
                (ask["kind"], ask["prompt"], json.dumps(ask["options"]), n, now, thread_id))
            con.commit()
            return {"thread_id": thread_id, "outcome": "expired", "recheck_count": n,
                    "blocked_on": blocked_on, "parked_days": age_days}
        next_at = now + DEFAULT_RECHECK_S
        add_event(con, thread_id, now, "helm-recheck", "", "state-change",
                  "recheck #%d: still blocked on %s%s. Next look %s."
                  % (n, blocked_on, (" -- " + detail) if detail else "",
                     dt.datetime.fromtimestamp(next_at, _LOCAL_TZ).strftime("%Y-%m-%d %H:%M")))
        con.execute("UPDATE threads SET recheck_at=?, recheck_count=?, last_change=?"
                    " WHERE thread_id=?", (next_at, n, now, thread_id))
        con.commit()
        return {"thread_id": thread_id, "outcome": "still-blocked", "recheck_count": n,
                "blocked_on": blocked_on, "recheck_at": next_at}

    add_event(con, thread_id, now, "helm-recheck", "", "state-change",
              "recheck #%d due -- was parked waiting on %s%s. Returning it to the work "
              "queue to verify whether that blocker still holds."
              % (n, blocked_on, (" -- " + detail) if detail else ""))
    # dispatched_at=0 so the next pass offers it immediately instead of measuring
    # a re-arm window against a timestamp from before the park.
    con.execute(
        "UPDATE threads SET state='working', recheck_at=0, recheck_count=?, gate_cmd=gate_cmd,"
        " dispatched_at=0, dispatch_attempts=0, last_dispatch_try=0, last_change=?"
        " WHERE thread_id=?", (n, now, thread_id))
    con.commit()
    return {"thread_id": thread_id, "outcome": "requeued", "recheck_count": n,
            "blocked_on": blocked_on}


def sweep_rechecks(con, run_gate=None):
    """Every due recheck, in one pass. Returns what happened to each.

    `run_gate` is injected: `(thread_id, cmd) -> (passed: bool, detail: str)`.
    That is how the shell stays outside the store (contract: P1 §6) and how both
    branches stay testable without a subprocess. With no runner, or a park with
    no gate command, the date alone is the exit and the thread returns to the
    work queue -- the conservative direction, because it costs one cheap run
    whereas never rechecking costs the whole promise.

    A runner that raises is treated as "still blocked" with the exception in the
    detail, never as a pass: a broken checker must not buy silence (LESSON 24).
    """
    out = []
    for d in due_rechecks(con):
        passed, detail = None, ""
        if d["gate_cmd"] and run_gate is not None:
            try:
                passed, detail = run_gate(d["thread_id"], d["gate_cmd"])
            except Exception as exc:  # noqa: BLE001 - a broken gate fails closed
                passed, detail = False, ("gate runner raised %s: %s"
                                         % (type(exc).__name__, exc))
        out.append(apply_recheck(con, d["thread_id"], passed, detail))
    return out


def adopt_legacy_parks(con):
    """Give an exit to parks written before the park contract existed.

    A gate is not a migration: rows parked under the old rules carry
    recheck_at=0 and would sit inert forever while every new park is policed.
    They are staggered a few minutes apart rather than dumped into one pass,
    which would spawn a dozen concurrent runs the first time the sweeper ticks.
    """
    rows = con.execute(
        "SELECT thread_id FROM threads WHERE state='waiting-external' AND recheck_at=0"
        "   AND wake_at=0 AND tombstoned=0 AND suppressed=0"
        " ORDER BY last_change ASC").fetchall()
    if not rows:
        return 0
    now = time.time()
    base = now + 120
    for i, r in enumerate(rows):
        con.execute(
            "UPDATE threads SET recheck_at=?, parked_since=CASE WHEN parked_since>0"
            " THEN parked_since ELSE ? END,"
            " blocked_on=CASE WHEN blocked_on='' THEN ? ELSE blocked_on END"
            " WHERE thread_id=?",
            (base + i * 180, now, "(adopted: parked before the recheck contract existed)",
             r["thread_id"]))
    con.commit()
    return len(rows)


def list_threads(con, include_done=False, include_snoozed=False):
    """Suppressed and tombstoned threads are never returned; no flag exposes
    them. Snoozed threads are omitted entirely rather than dimmed: a deferred
    thing should not be visible until it needs you."""
    woke = wake_due(con)
    sql = "SELECT * FROM threads WHERE tombstoned=0 AND suppressed=0"
    if not include_done:
        sql += " AND state != 'done'"
    if not include_snoozed:
        sql += " AND wake_at = 0"
    rows = con.execute(sql).fetchall()
    threads = [_row_to_thread(con, r) for r in rows]
    threads.sort(key=lambda t: (URGENCY_ORDER.get(t["urgency"], 9), -(t["last_change"] or 0)))
    return {
        "generated_at": time.time(),
        "woke": woke,
        "categories": categories_of(threads),
        "threads": threads,
    }


def activity(con, limit=60, since=None):
    """Recent events across every thread, newest first.

    Joined to the thread title, because an event body on its own does not say
    what it was about. Includes suppressed and tombstoned threads: this is the
    record of what happened, and hiding a muted thread's history here would
    make a mute look like nothing occurred.
    """
    sql = ("SELECT e.ts, e.kind, e.source, e.body, e.thread_id,"
           "       t.title, t.state, t.urgency, t.category"
           "  FROM events e JOIN threads t ON t.thread_id = e.thread_id")
    args = []
    if since:
        sql += " WHERE e.ts > ?"
        args.append(float(since))
    sql += " ORDER BY e.ts DESC LIMIT ?"
    args.append(int(limit))
    return [dict(r) for r in con.execute(sql, args)]


# Origin accounting. The one metric the homelab Helm could not compute while
# its board was eating itself: of the threads created in a window, how many
# were created by the system's own execution of other threads? A thread has
# no creator column; the source of its FIRST event is the creator, and the
# only source an executor run can write under is "bot" (post_result). Every
# other source is a human at a keyboard (joey, chat, claude, claude-code) or
# a deterministic producer (helm, email-sweep, scheduled-task).
CREATED_WINDOW_S = 7 * 86400
RUN_SOURCES = {"bot"}


def origins(con, since=None):
    """Where the threads created since `since` came from, and how many runs
    happened in the same window, so the replacement rate (new threads created
    by runs, per run) is one division and not a correlation."""
    since = float(since if since is not None else time.time() - CREATED_WINDOW_S)
    rows = con.execute(
        "SELECT t.thread_id,"
        " (SELECT e.source FROM events e WHERE e.thread_id=t.thread_id"
        "   ORDER BY e.ts ASC, e.event_id ASC LIMIT 1) AS src"
        " FROM threads t WHERE t.first_seen > ?", (since,)).fetchall()
    by_source = {}
    by_run = 0
    for r in rows:
        src = r["src"] or "(no events)"
        by_source[src] = by_source.get(src, 0) + 1
        if src in RUN_SOURCES:
            by_run += 1
    runs = con.execute(
        "SELECT COUNT(*) n FROM events WHERE kind='bot-result' AND source IN (%s) AND ts > ?"
        % ",".join("?" * len(RUN_SOURCES)), (*sorted(RUN_SOURCES), since)).fetchone()["n"]
    return {
        "since": since,
        "created": len(rows),
        "by_source": dict(sorted(by_source.items(), key=lambda kv: -kv[1])),
        "by_run": by_run,
        # Advisory definition: new items created by runs, per run. Below 1.0
        # decays, above 1.0 explodes; 0.5 is where to start worrying.
        "runs": runs,
        "replacement_rate": (by_run / runs) if runs else 0.0,
    }


def health(con):
    """Counts and queue state. Every number carries its denominator."""
    rows = con.execute(
        "SELECT state, COUNT(*) n FROM threads"
        " WHERE tombstoned=0 AND suppressed=0 GROUP BY state").fetchall()
    by_state = {r["state"]: r["n"] for r in rows}
    total = sum(by_state.values())
    snoozed = con.execute(
        "SELECT COUNT(*) n FROM threads WHERE tombstoned=0 AND suppressed=0"
        " AND wake_at > 0").fetchone()["n"]
    parked = con.execute(
        "SELECT COUNT(*) n FROM threads WHERE tombstoned=0 AND suppressed=0"
        " AND parked=1").fetchone()["n"]
    next_wake = con.execute(
        "SELECT MIN(wake_at) w FROM threads WHERE wake_at > 0 AND tombstoned=0"
        " AND suppressed=0").fetchone()["w"]
    q = pending_dispatch(con)
    return {
        "threads_total": total,
        "by_state": by_state,
        "needs_you": by_state.get("needs-you", 0),
        "snoozed": snoozed,
        "parked": parked,
        # Live, not done, not deliberately set aside. The number that answers
        # "how much is actually moving", as opposed to threads_total, which
        # answers "how much is on the board at all" and counts done and
        # parked right alongside the rest.
        "in_flight": total - by_state.get("done", 0) - parked,
        "next_wake": next_wake,
        # Parks whose recheck date has already passed. A number that only ever
        # grows means the sweeper is not running, which is the failure mode a
        # park contract without an actor always degrades into.
        "rechecks_due": len(due_rechecks(con)),
        "created": origins(con),
        "retired": con.execute(
            "SELECT COUNT(*) n FROM threads WHERE tombstoned=1 OR suppressed=1"
        ).fetchone()["n"],
        "queue": {"pending": q["count"], "gave_up": q["gave_up"],
                  "stale": len(stale_dispatches(con)),
                  # The oldest unactioned answer. Queue AGE is the signal that
                  # matters: attempts stay at 0 when a poller dies before its
                  # first call, so a broken pump looks healthier than a busy one.
                  "oldest_wait_s": (time.time() - q["pending"][0]["reply_ts"])
                                   if q["pending"] else 0},
    }


def list_suppressions(con):
    return [dict(r) for r in con.execute(
        "SELECT * FROM suppressions WHERE active=1 ORDER BY created DESC")]


def _reply_value(con, thread_id, ts):
    r = con.execute("SELECT body FROM events WHERE thread_id=? AND kind='joey-reply'"
                    " AND ts=? LIMIT 1", (thread_id, ts)).fetchone()
    return r["body"] if r else ""


def pending_dispatch(con, max_attempts=MAX_DISPATCH_ATTEMPTS):
    """Answers given that nothing has acted on yet.

    Pending means the newest joey-reply event is newer than dispatched_at,
    which keys idempotency to the REPLY rather than to the thread: a second
    answer on the same thread is new work, the same answer never is.

    A dispatched run that never produced a result is re-offered after
    RESULT_TIMEOUT_S, behind an exponential backoff, and gives up loudly after
    max_attempts rather than retrying forever in silence.
    """
    now = time.time()
    rows = con.execute(
        "SELECT t.*, (SELECT MAX(ts) FROM events e WHERE e.thread_id=t.thread_id"
        "   AND e.kind='joey-reply') AS reply_ts"
        # A settled thread never dispatches, however it got settled: a terminal
        # option, an explicit close, or a mute. Closing it is the instruction.
        " FROM threads t WHERE t.tombstoned=0 AND t.suppressed=0"
        "   AND t.state != 'done'").fetchall()
    out, gave_up = [], 0
    for r in rows:
        rts = r["reply_ts"]
        if not rts or rts <= (r["dispatched_at"] or 0):
            continue
        if (r["dispatch_attempts"] or 0) >= max_attempts:
            gave_up += 1
            continue
        # Backoff applies to RETRIES only. A new answer is new work, not a
        # retry, so it must not be held behind the timer left by the previous
        # answer's successful dispatch. attempts == 0 means nothing has failed.
        attempts = r["dispatch_attempts"] or 0
        if attempts > 0:
            backoff = min(DISPATCH_BACKOFF_S * (2 ** (attempts - 1)),
                          DISPATCH_BACKOFF_MAX_S)
            if (r["last_dispatch_try"] or 0) + backoff > now:
                continue  # still inside the backoff window from the failed attempt
        out.append({
            "thread_id": r["thread_id"], "title": r["title"], "summary": r["summary"],
            "category": r["category"], "urgency": r["urgency"],
            "ask_prompt": r["ask_prompt"],
            "ask_options": _jloads(r["ask_options"], []),
            "ask_context": r["ask_context"],
            "responder": r["responder"], "sources": _jloads(r["sources"], []),
            "reply_ts": rts, "reply_value": _reply_value(con, r["thread_id"], rts),
            "reply_note": r["last_note"], "state": r["state"],
            # The files ride with the answer, because they ARE part of it
            # (contract: P1 §5). An executor that cannot see them would report
            # back asking for the thing it was already sent.
            "attachments": attachments_for(con, r["thread_id"]),
            "attempts": r["dispatch_attempts"] or 0,
            "history": [dict(e) for e in con.execute(
                "SELECT ts, source, kind, body FROM events WHERE thread_id=?"
                " ORDER BY ts ASC", (r["thread_id"],))],
        })
    out.sort(key=lambda x: x["reply_ts"])
    return {"count": len(out), "gave_up": gave_up, "max_attempts": max_attempts,
            "pending": out}


def stale_dispatches(con):
    """Runs that were dispatched and never reported back.

    The test is "nothing reported SINCE the dispatch", not "the dispatch is
    old". Age alone is wrong, and confidently so: a thread that ran, reported
    properly, and was then answered again is back in 'working' with a fresh
    reply while `dispatched_at` still points at the previous round. Measuring
    age flagged four such threads as dead runs when all four had filed real
    reports, which is the view lying about state, the one thing it exists not
    to do.
    """
    now = time.time()
    rows = con.execute(
        "SELECT thread_id, title, dispatched_at, dispatch_attempts FROM threads t"
        " WHERE tombstoned=0 AND suppressed=0 AND state='working' AND dispatched_at > 0"
        "   AND dispatched_at < ?"
        "   AND NOT EXISTS (SELECT 1 FROM events e WHERE e.thread_id = t.thread_id"
        "                     AND e.kind = 'bot-result' AND e.ts > t.dispatched_at)",
        (now - RESULT_TIMEOUT_S,)).fetchall()
    return [dict(r) for r in rows]


def mark_dispatched(con, thread_id, reply_ts=None, ok=True, detail="", mode="reply"):
    """The poller reporting what happened when it woke an executor.

    The asymmetry between modes is deliberate. A reply-mode success resets the
    attempt counter, because the answer genuinely reached someone. Any failure
    burns an attempt, so a thread that keeps consuming runs without ever
    reporting marches toward the cap and gives up loudly instead of spinning.
    """
    _require_thread(con, thread_id)
    now = time.time()
    if mode == "start":
        # Claim the attempt BEFORE the executor is spawned. If the poller dies
        # mid-run, this is the only record that anything was tried: without it
        # the answer looks untouched, re-dispatches on the very next tick, and
        # can do so forever while an executor that already did the work is
        # still running. Claiming first turns that into one bounded retry.
        con.execute("UPDATE threads SET dispatch_attempts=dispatch_attempts+1,"
                    " last_dispatch_try=? WHERE thread_id=?", (now, thread_id))
        con.commit()
        row = con.execute("SELECT dispatch_attempts FROM threads WHERE thread_id=?",
                          (thread_id,)).fetchone()
        return {"accepted": True, "thread_id": thread_id, "claimed": True,
                "attempts": row["dispatch_attempts"]}
    if ok and mode == "reply":
        if reply_ts is None:
            raise StoreError(400, "'reply_ts' is required on a successful reply dispatch, "
                                  "because idempotency keys to the reply and not to the thread")
        # Monotonic (MAX), never backward: if the executor's own report
        # already reached post_result() before the poller got here (see
        # post_result's matching comment), dispatched_at may already be
        # ahead of this specific reply_ts, and this call must not undo that.
        con.execute("UPDATE threads SET dispatched_at=MAX(COALESCE(dispatched_at, 0), ?),"
                    " dispatch_attempts=0, last_dispatch_try=? WHERE thread_id=?",
                    (float(reply_ts), now, thread_id))
    elif ok:
        con.execute("UPDATE threads SET dispatched_at=?, dispatch_attempts=dispatch_attempts+1,"
                    " last_dispatch_try=? WHERE thread_id=?", (now, now, thread_id))
    else:
        # No increment here. "start" mode already claimed this attempt before
        # the executor was ever spawned (see the mode == "start" branch
        # above), so a failed "reply" report is the SAME attempt reporting
        # in, not a second one. Incrementing again here double-charged every
        # failed run against the 5-attempt budget (one on start, one on
        # failure), which is what let three real timeouts on
        # 2026-09-01 burn all 5 attempts and silently wedge
        # t3-audit-job-20260901 in 'working' for two days.
        con.execute("UPDATE threads SET last_dispatch_try=? WHERE thread_id=?",
                    (now, thread_id))
        add_event(con, thread_id, now, "helm", "", "state-change",
                  "dispatch FAILED: " + (str(detail) or "no detail given"))
    con.commit()
    row = con.execute("SELECT dispatch_attempts FROM threads WHERE thread_id=?",
                      (thread_id,)).fetchone()
    return {"accepted": True, "thread_id": thread_id, "ok": bool(ok),
            "attempts": row["dispatch_attempts"]}


def restore_gave_up(con, max_attempts=MAX_DISPATCH_ATTEMPTS):
    """Undo a silent give-up: put a capped-out thread back in front of Joey.

    pending_dispatch() stops OFFERING a thread once dispatch_attempts crosses
    max_attempts, on purpose, so a broken run cannot spin forever (see its own
    docstring). What it never did was undo anything: a thread that hit the cap
    right after being answered was left wearing whatever state the reply had
    set, almost always 'working', with its ORIGINAL ask still sitting
    unchanged on the row (reply() never clears it) but not shown, because
    'working' is not 'needs-you'. On screen that is indistinguishable from
    real work in progress, forever. It is exactly how
    t3-audit-job-20260901 wedged for two days: three real 900s
    timeouts on 2026-09-01 burned the whole budget (helped by a since-fixed
    bug in mark_dispatched() that charged each failure twice), pending_
    dispatch() correctly stopped offering it back out, and nothing ever
    offered it back to a human either.

    This does, every time it is called, the same repair that was done for t3
    by hand: consume the reply that gave up (so a tick landing mid-restore
    cannot see attempts==0 with that reply still "unconsumed" and dispatch it
    again before this finishes), then restore state='needs-you' with the SAME
    ask that was there the whole time, plus a note saying why it is back.

    Guards against one race a naive version of this would not: if a brand new
    reply has landed since the LAST real dispatch attempt
    (reply_ts > last_dispatch_try), nothing that failed ever saw it, so it is
    left alone for pending_dispatch() to offer normally rather than being
    swallowed here as if it were the stale one.
    """
    now = time.time()
    rows = con.execute(
        "SELECT t.*, (SELECT MAX(ts) FROM events e WHERE e.thread_id=t.thread_id"
        "   AND e.kind='joey-reply') AS reply_ts"
        " FROM threads t WHERE t.tombstoned=0 AND t.suppressed=0"
        "   AND t.state != 'done'").fetchall()
    restored = []
    for r in rows:
        rts = r["reply_ts"]
        if not rts or rts <= (r["dispatched_at"] or 0):
            continue  # no unconsumed reply: nothing to have given up on
        if (r["dispatch_attempts"] or 0) < max_attempts:
            continue  # still has retries left; pending_dispatch() owns this one
        if rts > (r["last_dispatch_try"] or 0):
            continue  # a fresh reply arrived after the last real attempt; not stale
        tid = r["thread_id"]
        attempts = r["dispatch_attempts"] or 0
        # Same first step as the by-hand t3 fix: consume the reply that gave
        # up, exactly as a successful dispatch would, before touching state.
        mark_dispatched(con, tid, reply_ts=rts, ok=True, mode="reply")
        if r["ask_kind"] not in VALID_ASK or r["ask_kind"] == "none":
            # Never restore into a bare Close button (invariant 4). Only
            # reachable if the ask was cleared by something other than the
            # normal reply cycle (e.g. a snooze) while a dispatch was in
            # flight; the ordinary case needs no repair here at all, because
            # reply() never clears the ask and it is still sitting on the row.
            con.execute("UPDATE threads SET ask_kind='ack',"
                        " ask_prompt='Pick up where this left off.' WHERE thread_id=?", (tid,))
        note = (
            "The dispatcher tried %d time(s) to act on your answer and never got a "
            "usable result back, so it gave up rather than retry forever. Restoring "
            "this to needs-you with your original question unchanged; nothing it "
            "promised has been done yet. Pick an option to try again."
            % attempts)
        add_event(con, tid, now, "helm-gave-up", "", "bot-result", note)
        con.execute("UPDATE threads SET state='needs-you', last_note=?, last_change=?"
                    " WHERE thread_id=?", (note, now, tid))
        restored.append({"thread_id": tid, "title": r["title"], "attempts": attempts})
    if restored:
        con.commit()
    return restored


# --- Writes ------------------------------------------------------------------
def _answer_is_in_flight(con, existing) -> bool:
    """Invariant 3. Three cases, each with a real reason.

    A finished item that reappeared carrying its ORIGINAL prompt and buttons
    is on screen indistinguishable from never having been touched, which is
    exactly the lie this whole thing exists to stop telling.
    """
    if existing["state"] == "working":
        return True  # something is acting on the answer right now
    try:
        if (existing["wake_at"] or 0) > time.time():
            return True  # deliberately hidden until a promised date
    except (IndexError, KeyError):
        pass  # pre-migration row
    if existing["ask_kind"] != "none":
        return False
    kinds = {r["kind"] for r in con.execute(
        "SELECT DISTINCT kind FROM events WHERE thread_id=?", (existing["thread_id"],))}
    # ask cleared, a reply given, a report back: the work is done and only
    # Close remains.
    return "joey-reply" in kinds and "bot-result" in kinds


def _touch_category(con, name, now):
    if not name:
        return
    con.execute("INSERT INTO categories (name, first_seen, last_seen) VALUES (?,?,?)"
                " ON CONFLICT(name) DO UPDATE SET last_seen=excluded.last_seen",
                (name, now, now))


def _matching_suppression(con, title, sources):
    prod = producer_of(title, sources)
    fp = fingerprint(title)
    return con.execute(
        "SELECT * FROM suppressions WHERE producer=? AND fingerprint=? AND active=1",
        (prod, fp)).fetchone()


def put_threads(con, payload):
    """The producer's write. Upsert by thread_id; retire only what is named
    in `tombstones`.

    Partial success on purpose: every thread is validated before anything is
    written, a bad thread costs only itself, and the response names each
    rejection and why. One malformed item must never throw away a whole pass,
    and the producer can read the error and correct itself, whereas a human
    would just get a screen that fails to load.
    """
    now = time.time()
    raw_threads = payload.get("threads") or []
    tombstones = payload.get("tombstones") or []
    if not isinstance(raw_threads, list):
        raise StoreError(400, "threads must be a list")
    if not isinstance(tombstones, list):
        raise StoreError(400, "tombstones must be a list of thread_id strings")

    clean, rejected = [], []
    for i, t in enumerate(raw_threads):
        c, errs = validate_thread(t)
        if errs:
            rejected.append({
                "index": i,
                "thread_id": (t or {}).get("thread_id") if isinstance(t, dict) else None,
                "title": (t or {}).get("title") if isinstance(t, dict) else None,
                "errors": errs,
            })
        else:
            clean.append((i, c))

    created = updated = retired = suppressed_on_arrival = 0
    preserved_done = preserved_answered = 0

    for idx, c in clean:
        tid = c["thread_id"]
        existing = con.execute("SELECT * FROM threads WHERE thread_id=?", (tid,)).fetchone()

        if existing is None:
            rule = _matching_suppression(con, c["title"], c["sources"])
            if rule is not None:
                con.execute(
                    "INSERT INTO threads (thread_id, title, summary, category, urgency, state,"
                    " ask_kind, ask_prompt, ask_options, responder, sources, first_seen,"
                    " last_change, suppressed) VALUES (?,?,?,?,?,'done','none','','[]',?,?,?,?,1)",
                    (tid, c["title"], c["summary"], c["category"], c["urgency"],
                     c["responder"], json.dumps(c["sources"]), c["first_seen"] or now, now))
                con.execute("UPDATE suppressions SET hits=hits+1, last_hit=? WHERE id=?",
                            (now, rule["id"]))
                add_event(con, tid, now, "helm", "", "state-change",
                          "auto-retired on arrival by mute rule: " + (rule["note"] or "(no note)"))
                suppressed_on_arrival += 1
                continue

            # The needs-you four-test, producer's door (contract: P3b §1.2).
            # Fires only for a NEW ask reaching the board: a fresh lit thread.
            # A resync of a thread that already carries an ask under this
            # stable id is licensed (below); preservation branches never
            # reach a gate at all.
            if (c["state"] == "needs-you"
                    and c["ask_kind"] in ("choice", "text")):
                nj_errors = needs_joey_gate_errors(con, c, None, now)
                if nj_errors:
                    rejected.append({
                        "index": idx,
                        "thread_id": tid,
                        "title": c["title"],
                        "errors": nj_errors,
                    })
                    continue

            con.execute(
                "INSERT INTO threads (thread_id, title, summary, category, urgency, state,"
                " ask_kind, ask_prompt, ask_options, ask_context, responder, sources, links,"
                " first_seen, last_change, blocked_on, recheck_at, gate_cmd, parked_since, label)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (tid, c["title"], c["summary"], c["category"], c["urgency"], c["state"],
                 c["ask_kind"], c["ask_prompt"], json.dumps(c["ask_options"]),
                 c["ask_context"], c["responder"],
                 json.dumps(c["sources"]), json.dumps(c["links"]), c["first_seen"] or now, now,
                 c["blocked_on"], c["recheck_at"], c["gate_cmd"],
                 now if c["state"] == "waiting-external" else 0.0,
                 c.get("label", "")))
            created += 1
        else:
            new_state = c["state"]
            ask_kind, ask_prompt = c["ask_kind"], c["ask_prompt"]
            ask_options = json.dumps(c["ask_options"])
            ask_context = c["ask_context"]
            links = json.dumps(c["links"])

            if existing["state"] == "done":
                new_state = "done"  # Invariant 2
                preserved_done += 1
            elif _answer_is_in_flight(con, existing):
                new_state = existing["state"]  # Invariant 3
                ask_kind = existing["ask_kind"]
                ask_prompt = existing["ask_prompt"]
                ask_options = existing["ask_options"]
                ask_context = existing["ask_context"]
                # A bot-result's links must not be clobbered by the next
                # routine resync any more than its ask_options are; see the
                # migrations comment on this column.
                links = existing["links"]
                preserved_answered += 1
            elif (c["state"] == "needs-you" and c["ask_kind"] in ("choice", "text")
                  and (existing["ask_kind"] or "none") == "none"):
                # The four-test, producer's door (contract: P3b §1.2): this
                # branch is reached only when no earlier invariant applies, so
                # the thread is live and no answer is in flight. Re-lighting a
                # thread whose stored ask is 'none' is a NEW ask reaching the
                # board -- the un-licensed case. Licensed resyncs (stored ask
                # != 'none') fall through to the plain UPDATE below.
                nj_errors = needs_joey_gate_errors(con, c, existing, now)
                if nj_errors:
                    rejected.append({
                        "index": idx,
                        "thread_id": tid,
                        "title": c["title"],
                        "errors": nj_errors,
                    })
                    continue

            # Park fields follow the state that is actually being STORED, not
            # the one the producer sent: if an invariant held the thread where
            # it was, its existing claim (or lack of one) stands, and a thread
            # that is no longer parked must not keep wearing a blocker it is no
            # longer waiting on. parked_since is preserved across resyncs so a
            # producer cannot reset the 30-day expiry clock by resending.
            if new_state == "waiting-external":
                if c["state"] == "waiting-external":
                    blocked_on, recheck_at = c["blocked_on"], c["recheck_at"]
                    gate_cmd = c["gate_cmd"]
                else:
                    blocked_on = existing["blocked_on"]
                    recheck_at = existing["recheck_at"]
                    gate_cmd = existing["gate_cmd"]
                parked_since = existing["parked_since"] or now
            else:
                blocked_on, recheck_at, gate_cmd, parked_since = "", 0.0, "", 0.0

            new_label = c.get("label")
            if not new_label and "label" in existing.keys():
                new_label = existing["label"] or ""
            con.execute(
                "UPDATE threads SET title=?, summary=?, category=?, urgency=?, state=?,"
                " ask_kind=?, ask_prompt=?, ask_options=?, ask_context=?, responder=?,"
                " sources=?, links=?,"
                " blocked_on=?, recheck_at=?, gate_cmd=?, parked_since=?,"
                " label=?, last_change=? WHERE thread_id=?",
                (c["title"], c["summary"], c["category"], c["urgency"], new_state,
                 ask_kind, ask_prompt, ask_options, ask_context, c["responder"],
                 json.dumps(c["sources"]), links, blocked_on, recheck_at, gate_cmd,
                 parked_since, new_label, now, tid))
            updated += 1

        _touch_category(con, c["category"], now)
        for ev in c["events"]:
            if not isinstance(ev, dict):
                continue
            add_event(con, tid, ev.get("ts") or now, ev.get("source"), ev.get("source_id"),
                      ev.get("kind") or "update", ev.get("body") or "")

    for tid in tombstones:
        tid = str(tid).strip()
        if not tid:
            continue
        row = con.execute("SELECT thread_id FROM threads WHERE thread_id=?", (tid,)).fetchone()
        if row is None:
            continue
        con.execute("UPDATE threads SET tombstoned=1, state='done', last_change=?"
                    " WHERE thread_id=?", (now, tid))
        add_event(con, tid, now, "helm", "", "state-change", "retired: no longer true")
        retired += 1

    con.commit()

    result = {
        "ok": not rejected,
        # Threads actually stored. Pre-P3b every validated thread was always
        # written, so this equals len(clean) exactly for every ported caller;
        # the four-test's gate rejections (P3b §1.2) now reject inside the
        # write loop, and inflating 'accepted' with them would make a payload
        # of all-rejected read as accepted at the API door.
        "accepted": created + updated + suppressed_on_arrival,
        "rejected_count": len(rejected),
        "created": created,
        "updated": updated,
        "retired": retired,
        "suppressed_on_arrival": suppressed_on_arrival,
        "preserved_done": preserved_done,
        "preserved_answered": preserved_answered,
    }
    if rejected:
        result["rejected"] = rejected
        result["detail"] = ("these threads were NOT stored; every other thread in the payload "
                            "was applied. Fix these and re-PUT just them.")
    return result


def _mint_hand_id(con, title, now):
    """A readable, date-stamped, unique id for a card written by hand.

    Minted here rather than supplied, which is the exact opposite of the rule
    for a producer's thread_id (validate_thread requires one, because a
    producer resyncs the same concern every pass and a missing id means a
    fresh set of duplicates every time). Nothing ever resends a hand-written
    card, so there is nothing for a stable id to key to, and asking the person
    typing it to invent one would be a field with no purpose. The date is in
    the id so writing the same title again next week is a new card rather than
    a collision, and the numeric suffix covers the same title twice in one day.
    """
    day = dt.datetime.fromtimestamp(now).strftime("%Y%m%d")
    base = HAND_ID_PREFIX + day + "-" + (slug(title, 40) or "note")
    tid, n = base, 2
    while con.execute("SELECT 1 FROM threads WHERE thread_id=?", (tid,)).fetchone():
        tid = "%s-%d" % (base, n)
        n += 1
    return tid


def create_thread(con, body):
    """One card, written by hand from the app. Not a producer's PUT.

    A PUT is a resync: it re-derives the whole picture every pass, keyed by
    ids the producer owns, and its invariants exist to stop it from
    overwriting decisions it knows nothing about. A hand-written card shares
    none of that. It arrives once, from the person the list is for, and the
    only thing it has to decide is what happens next. So it gets its own
    entry point instead of a flag on put_threads, and three deliberate
    differences from one:

      * The id is minted here (see _mint_hand_id).
      * Mute rules are NOT consulted. put_threads auto-retires an arriving
        thread that matches one, because a producer re-derives the same
        nagging item forever and the rule is how you make it stop. A card
        someone just typed is not that, and silently filing it as done
        would look exactly like the app dropping it.
      * `intent` sets state and ask together, so the shape the store's
        invariants care about cannot be posted half-formed. A form is not
        going to author a well-made ask, and invariant 4 would (rightly)
        reject a lit card without one.

    intent:
      mine     -- yours to deal with. Lit, with one option: Done.
      claude   -- yours, but hand it straight to a background run. Same card,
                  plus the instruction filed as an answer, which is what the
                  dispatcher watches for. `instruction` is required: without
                  it a run has nothing to act on and can only hand it back.
      waiting  -- on someone else. Not lit, so it carries no question; it
                  sits in the list until you park it, close it, or it moves.
    """
    if not isinstance(body, dict):
        raise StoreError(400, "body must be a JSON object")

    title = str(body.get("title") or "").strip()
    if not title:
        raise StoreError(400, "'title' is required: it is the line you will read on the list")
    if len(title) > MAX_TITLE:
        raise StoreError(400, "title is %d chars, max %d; put the detail in 'summary'"
                              % (len(title), MAX_TITLE))

    summary = str(body.get("summary") or "").strip()
    category = str(body.get("category") or "").strip()
    urgency = str(body.get("urgency") or "whenever").strip()
    if urgency not in VALID_URGENCY:
        raise StoreError(400, "urgency %r not one of %s" % (urgency, sorted(VALID_URGENCY)))

    intent = str(body.get("intent") or "mine").strip()
    if intent not in VALID_INTENT:
        raise StoreError(400, "intent %r not one of %s" % (intent, sorted(VALID_INTENT)))

    instruction = str(body.get("instruction") or "").strip()
    if intent == "claude" and not instruction:
        raise StoreError(400, "intent 'claude' needs an 'instruction': it is the whole brief "
                              "for the run, and a run with nothing to act on can only hand "
                              "the card straight back to you")
    if len(instruction) > MAX_NOTE:
        raise StoreError(400, "instruction is %d chars, max %d" % (len(instruction), MAX_NOTE))

    links, link_errors = normalize_links(body.get("links"))
    if link_errors:
        raise StoreError(400, "; ".join(link_errors))

    park = ("", 0.0, "")
    if intent == "waiting":
        # A wait someone typed by hand is still a wait, and the same contract
        # applies (P1 §1): the form asks who it waits on, because a card that
        # waits on nobody is a card that was dropped and looks like one that is
        # moving. Loud at creation rather than discovered weeks later.
        park = require_park(body)
        state, ask_kind, ask_prompt, options = "waiting-external", "none", "", []
    else:
        state, ask_kind = "needs-you", "choice"
        ask_prompt = "Is this finished?"
        options = [{"label": "Done", "value": "done",
                    "effect": "Settles it here and marks the work finished. "
                              "Nothing is written anywhere else.",
                    "terminal": True, "resolved": True}]

    label = str(body.get("label") or "").strip().lower()
    if label:
        lbl = get_label(con, label)
        if lbl is None:
            raise StoreError(400, f"label {label!r} does not exist in labels table")
        if not lbl["enabled"]:
            raise StoreError(400, f"label {label!r} is disabled")
    else:
        settings = get_settings(con)
        def_lbl = settings.get("default_intake_label", "").strip().lower()
        if def_lbl and get_label(con, def_lbl):
            label = def_lbl

    now = time.time()
    tid = _mint_hand_id(con, title, now)
    con.execute(
        "INSERT INTO threads (thread_id, title, summary, category, urgency, state,"
        " ask_kind, ask_prompt, ask_options, responder, sources, links, first_seen,"
        " last_change, blocked_on, recheck_at, gate_cmd, parked_since, label)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        # responder stays NULL: it records who last reported on a thread (an
        # executor stamps its own name there), and nothing has yet.
        (tid, title, summary, category, urgency, state, ask_kind, ask_prompt,
         json.dumps(options), None, json.dumps([]), json.dumps(links), now, now,
         park[0], park[1], park[2], now if state == "waiting-external" else 0.0, label))
    _touch_category(con, category, now)
    add_event(con, tid, now, "joey", "", "state-change",
              "you wrote this card yourself"
              + (", filed as waiting on someone else" if intent == "waiting" else ""))
    con.commit()

    handed_off = False
    if intent == "claude":
        # The same path any answer takes, on purpose: the dispatcher picks up
        # a reply newer than the last dispatch and knows nothing about where
        # the card came from, so there is no second queue to keep working.
        reply(con, tid, HAND_TASK_VALUE, note=instruction)
        handed_off = True

    row = con.execute("SELECT * FROM threads WHERE thread_id=?", (tid,)).fetchone()
    return {"created": True, "thread_id": tid, "state": row["state"],
            "intent": intent, "handed_off": handed_off, "label": row["label"]}


def _require_thread(con, thread_id):
    row = con.execute("SELECT * FROM threads WHERE thread_id=?", (thread_id,)).fetchone()
    if row is None:
        raise StoreError(404, "no such thread: " + str(thread_id))
    return row


# --- Attachments are instructions (contract: P1 §5; LESSON 19, supporting 15)-
#
# Sending a file IS the instruction. "Here's the invoice" plus a tap on Done is
# not a person closing a thread, it is a person handing over the thing the work
# needs and expecting it to be used; settling on that tap throws the file away
# in the same motion that received it, silently, and nobody finds out until the
# work comes back wrong. Same treatment as a binding note (LESSON 15).
#
# The store keeps metadata only. Bytes are the API's business (contract: P1 §6).
MAX_ATTACHMENT_BYTES = 25 * 1024 * 1024
MAX_FILENAME = 255
_SHA256_HEX = re.compile(r"^[0-9a-fA-F]{64}$")


def _human_size(n):
    n = float(n or 0)
    for unit in ("B", "KB", "MB"):
        if n < 1024 or unit == "MB":
            return ("%.0f %s" % (n, unit)) if unit == "B" else ("%.1f %s" % (n, unit))
        n /= 1024.0


def normalize_attachment(meta):
    """Coerce one attachment's metadata. Returns (clean, errors).

    Hand-written rather than schema-driven for the reason the rest of this file
    is: the producer has to be able to read the rejection and fix its own
    payload without a second round trip.
    """
    errors = []
    if not isinstance(meta, dict):
        return None, ["attachment is " + type(meta).__name__ + ", expected an object with "
                      "filename, mime, size and sha256"]
    filename = str(meta.get("filename") or meta.get("name") or "").strip()
    if not filename:
        errors.append("attachment has no 'filename': history names the file, because "
                      "'1 attachment' tells the reader nothing about what arrived")
    if len(filename) > MAX_FILENAME:
        errors.append("attachment filename is %d chars, max %d" % (len(filename),
                                                                   MAX_FILENAME))
    if "\x00" in filename or "/" in filename or "\\" in filename:
        errors.append("attachment filename %r contains a path separator or a NUL: send the "
                      "bare file name; where the bytes live is the 'url'" % filename)
    mime = str(meta.get("mime") or meta.get("content_type") or "").strip()
    if not mime:
        errors.append("attachment %r has no 'mime': the reader decides whether they can "
                      "open it before they tap anything" % filename)
    size = meta.get("size", meta.get("bytes"))
    try:
        size = int(size)
    except (TypeError, ValueError):
        size = -1
    if size <= 0:
        errors.append("attachment %r has size %r: a zero-byte file is a failed upload "
                      "wearing an instruction's clothes" % (filename, meta.get("size")))
    elif size > MAX_ATTACHMENT_BYTES:
        errors.append("attachment %r is %s, over the %s limit"
                      % (filename, _human_size(size), _human_size(MAX_ATTACHMENT_BYTES)))
    sha = str(meta.get("sha256") or "").strip().lower()
    if not _SHA256_HEX.match(sha or ""):
        errors.append("attachment %r has sha256 %r, which is not 64 hex characters. It is "
                      "what tells a re-sent file from a second one" % (filename, sha))
    if errors:
        return None, errors
    return {
        "att_id": str(meta.get("att_id") or uuid.uuid4().hex),
        "ts": float(meta.get("ts") or time.time()),
        "filename": filename,
        "mime": mime,
        "size": size,
        "sha256": sha,
        "url": str(meta.get("url") or ""),
        "uploader": str(meta.get("uploader") or "joey"),
        "caption": str(meta.get("caption") or ""),
    }, []


def add_attachment(con, thread_id, meta):
    """Record a file that arrived on this thread, and say so in history.

    The event body names the file, its size and where it is, and tells the
    reader to OPEN it before answering. "Attachment added" would be the
    status-word failure (LESSON 11): history is event bodies, and a body that
    describes nothing is a line nobody can act on.
    """
    _require_thread(con, thread_id)
    clean, errors = normalize_attachment(meta)
    if errors:
        raise StoreError(400, "; ".join(errors))
    dupe = con.execute("SELECT * FROM attachments WHERE thread_id=? AND sha256=?",
                       (thread_id, clean["sha256"])).fetchone()
    if dupe is not None:
        # The same bytes twice is a retry, not a second instruction.
        return dict(dupe, duplicate=True)
    con.execute(
        "INSERT INTO attachments (att_id, thread_id, ts, filename, mime, size, sha256, url,"
        " uploader, caption) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (clean["att_id"], thread_id, clean["ts"], clean["filename"], clean["mime"],
         clean["size"], clean["sha256"], clean["url"], clean["uploader"], clean["caption"]))
    body = ("FILE ATTACHED -- OPEN IT BEFORE YOU ANSWER: %s (%s, %s)%s%s"
            % (clean["filename"], clean["mime"], _human_size(clean["size"]),
               ("\n" + clean["caption"]) if clean["caption"] else "",
               ("\n" + clean["url"]) if clean["url"] else ""))
    # Only a file Joey actually sent is a joey-attachment. An agent attaching its
    # own diagram to the card it is asking about must not be readable as Joey
    # answering -- an agent attaching its own visual to a card
    # where a worker visual was delivered back as his reply and closed the ask.
    kind = "joey-attachment" if clean["uploader"] == "joey" else "agent-attachment"
    add_event(con, thread_id, clean["ts"], clean["uploader"], clean["att_id"],
              kind, body)
    con.commit()
    return dict(clean, thread_id=thread_id, duplicate=False)


def attachments_for(con, thread_id):
    return [dict(r) for r in con.execute(
        "SELECT * FROM attachments WHERE thread_id=? ORDER BY ts ASC", (thread_id,))]


def unconsumed_attachments(con, thread_id):
    """Files that arrived in the round nothing has acted on yet.

    "Unconsumed" is measured against the newest bot-result, the same test the
    answer gate uses on a note: a file is consumed when something has reported
    back after it, not when it has merely been looked at.
    """
    last_report = con.execute(
        "SELECT MAX(ts) FROM events WHERE thread_id=? AND kind='bot-result'",
        (thread_id,)).fetchone()[0] or 0
    return [dict(r) for r in con.execute(
        "SELECT * FROM attachments WHERE thread_id=? AND ts > ? ORDER BY ts ASC",
        (thread_id, last_report))]


def _attachment_hold(pending):
    """The one sentence every close-tap gives back when files are waiting."""
    names = ", ".join("%s (%s)" % (a["filename"], _human_size(a["size"])) for a in pending)
    return ("this reply carries %d file(s) nothing has acted on yet -- %s. A file sent with "
            "an answer is an instruction, so it goes to whoever does the work instead of "
            "being closed away with the card" % (len(pending), names))


def reply(con, thread_id, value, note="", attachments=None):
    """An answer, with an optional note that is binding on whoever executes it.

    The note is not a comment field. It rides with the answer and wins over
    the option text on material contradiction. So does a file: `attachments`
    may be sent inline with the reply, and a reply carrying one never settles
    (contract: P1 §5).
    """
    row = _require_thread(con, thread_id)
    if value is None or (isinstance(value, str) and not value.strip()):
        raise StoreError(400, "'value' is required: which option was chosen")
    if not isinstance(value, str):
        value = json.dumps(value)
    note = str(note or "")
    if len(note) > MAX_NOTE:
        raise StoreError(400, "note is %d chars, max %d" % (len(note), MAX_NOTE))

    now = time.time()
    # Inline files, recorded BEFORE the settle predicates below read them: a
    # file sent with the tap is part of the tap (contract: P1 §5).
    added = [add_attachment(con, thread_id, m) for m in (attachments or [])]
    add_event(con, thread_id, now, "joey", "", "joey-reply", value)
    if note.strip():
        add_event(con, thread_id, now + 0.001, "joey", "", "joey-note", note)
    # Contract: P1 §4. A fresh answer resets the two assertions: whatever an
    # earlier result claimed it had applied, it cannot have applied THIS.
    con.execute("UPDATE threads SET answer_applied=0, nothing_owed=0 WHERE thread_id=?",
                (thread_id,))

    # Contract: P1 §5, LESSON 19. Every close-tap below asks this first. A file
    # nothing has acted on yet turns a settling tap into working: the answer
    # goes out carrying the files, instead of the files going in the bin with
    # the card. Sending a file and settling in the same act is the one
    # combination that silently throws the file away.
    pending_files = unconsumed_attachments(con, thread_id)

    # An ack ask exists for exactly one reason: "reading it is genuinely the
    # whole job" (see validate_thread's rejection text and build_prompt's own
    # instructions to the executor). There is nothing an executor could do
    # with a plain "Got it" that the ack didn't already cover, so settle it
    # here, the same as a terminal choice option, rather than waking one that
    # can only report back that nothing needed doing and hand back the same
    # ack question. A bot may never mark a thread done, and that is exactly
    # why this had no exit before: the executor was correctly forbidden from
    # closing it, so an ack-only card could ONLY loop until this tap did it
    # instead. Keyed to the literal value "ack", not to ask_kind alone, so a
    # redirect (value "__redirect__", sent with a note instead) still reaches
    # an executor to interpret.
    if row["ask_kind"] == "ack" and value == "ack" and not pending_files:
        add_event(con, thread_id, now + 0.002, "joey", "", "state-change",
                  "settled by acknowledging: the ask was only ever \"read this\"")
        con.execute("UPDATE threads SET state='done', ask_kind='none', ask_prompt='',"
                    " ask_options='[]', ask_context='', wake_at=0, last_note=?, last_change=?"
                    " WHERE thread_id=?", (note, now, thread_id))
        con.commit()
        # An ack is never an assertion that the work is finished, only that
        # reading it was the whole job, so it is never `resolved`.
        return {"accepted": True, "thread_id": thread_id, "state": "done",
                "terminal": True, "resolved": False, "note_recorded": bool(note.strip())}

    # A terminal option is the whole action. Settle it here rather than
    # queueing an executor that has nothing to do and would come back saying
    # it could not deliver what the option promised.
    chosen = next((o for o in _jloads(row["ask_options"], [])
                   if o.get("value") == value), None)
    if chosen and chosen.get("terminal") and not pending_files:
        add_event(con, thread_id, now + 0.002, "joey", "", "state-change",
                  "settled by choosing " + repr(chosen.get("label") or value))
        con.execute("UPDATE threads SET state='done', ask_kind='none', ask_prompt='',"
                    " ask_options='[]', ask_context='', wake_at=0, last_note=?, last_change=?"
                    " WHERE thread_id=?", (note, now, thread_id))
        con.commit()
        return {"accepted": True, "thread_id": thread_id, "state": "done",
                "terminal": True, "resolved": bool(chosen.get("resolved")),
                "note_recorded": bool(note.strip())}

    # wake_at is cleared here for the same reason the two settling branches
    # above clear it: he has just engaged with this thread, so the deferral he
    # (or a run) put on it is spent. Leaving it set hid a thread that was
    # actively being worked -- list_threads filters on wake_at, not on state --
    # and then let wake_due() slam it back to 'needs-you' on the old date,
    # overwriting the live work with the stale question it was deferred with.
    con.execute("UPDATE threads SET state='working', wake_at=0, wake_note='', wake_ask='',"
                " last_note=?, last_change=? WHERE thread_id=?", (note, now, thread_id))
    con.commit()
    out = {"accepted": True, "thread_id": thread_id, "state": "working",
           "terminal": False, "note_recorded": bool(note.strip())}
    if pending_files:
        would_have_settled = ((row["ask_kind"] == "ack" and value == "ack")
                              or bool(chosen and chosen.get("terminal")))
        out["attachments"] = [a["filename"] for a in pending_files]
        if would_have_settled:
            # Said out loud in the return AND in history: a tap that did not do
            # what the button said must never look like it did.
            reason = _attachment_hold(pending_files)
            out["settlement_suppressed"] = True
            out["reason"] = reason
            add_event(con, thread_id, now + 0.002, "joey", "", "state-change",
                      "not settled on this tap: " + reason)
            con.commit()
    return out


def close_thread(con, thread_id, reason="", actor="joey", force=False):
    """The user closes it. Always succeeds: a tap that worked must not look
    like it failed because a downstream write-back did.

    `actor` defaults to "joey", so the UI's Close button and every ported
    caller are unchanged. Anything else is refused (contract: P1 §4): this is
    the human close path, and a bot that believes a thread is finished has
    exactly two mechanical exceptions, both of which run through post_result
    where they can be checked against what he actually tapped.

    `force` is the one way past an unconsumed attachment (contract: P1 §5).
    The tap still works when it is meant; it just cannot happen by accident in
    the same breath as sending a file.
    """
    _require_thread(con, thread_id)
    pending_files = unconsumed_attachments(con, thread_id)
    if pending_files and not force:
        raise StoreError(400, "not closed: " + _attachment_hold(pending_files)
                              + ". Answer the card so the files reach a run, or close it "
                                "with force=true if you really do want them dropped")
    if str(actor or "joey").strip().lower() != "joey":
        raise StoreError(400, "actor %r may not close a thread: only Joey closes. A run that "
                              "believes a thread is finished reports it with post_result -- "
                              "either on an ack he has tapped, or on a decision he answered "
                              "with answer_applied: true and nothing_owed: true -- or it "
                              "hands the close back to him as an ack ask in the same post"
                              % actor)
    now = time.time()
    body = "closed by you" + ((": " + reason) if reason else "")
    add_event(con, thread_id, now, "joey", "", "state-change", body)
    con.execute("UPDATE threads SET state='done', ask_kind='none', ask_prompt='',"
                " ask_options='[]', ask_context='', wake_at=0, last_change=? WHERE thread_id=?",
                (now, thread_id))
    con.commit()
    return {"accepted": True, "thread_id": thread_id, "state": "done"}


def reopen_thread(con, thread_id, note=""):
    """Reverse a settle. The one place that is allowed to defy invariant 2.

    Invariant 2 stops a PRODUCER pass from dragging a done thread back to
    life without knowing what was already decided there. This is not that:
    it is you, directly, saying the settle itself was wrong (a mis-tap, most
    often). A terminal reply and an explicit close both clear ask_kind /
    ask_prompt / ask_options the moment they fire, so by the time a thread
    can be reopened its original options are already gone. This does not
    pretend to reconstruct them; it asks again, as a plain text question,
    which is the honest version of "I don't know what the options were
    anymore" rather than a guess dressed up as a memory.
    """
    row = _require_thread(con, thread_id)
    if row["state"] != "done":
        raise StoreError(400, "only a 'done' thread can be reopened; this one is %r"
                              % row["state"])
    now = time.time()
    body = "reopened by you" + ((": " + note) if note else "")
    add_event(con, thread_id, now, "joey", "", "state-change", body)
    con.execute(
        "UPDATE threads SET state='needs-you', ask_kind='text',"
        " ask_prompt='Reopened. The original options are gone; say what you want done.',"
        " ask_options='[]', ask_context='', wake_at=0, answer_applied=0, nothing_owed=0,"
        " last_change=? WHERE thread_id=?",
        (now, thread_id))
    con.commit()
    return {"accepted": True, "thread_id": thread_id, "state": "needs-you"}


def mute(con, thread_id, note="", force=False):
    """Retire this thread AND stop asking about this kind of thing.

    This is the endpoint that actually drains a backlog rather than
    reshuffling it, so it is reversible and auditable by construction: the
    rule is listed with a hit count, never a silent black hole.

    It also settles the thread, which makes it a close tap, which makes it
    subject to the attachment rule (contract: P1 §5): muting a card he has just
    sent a file to would drop the file and the rule that produced it in one go.
    """
    row = _require_thread(con, thread_id)
    pending_files = unconsumed_attachments(con, thread_id)
    if pending_files and not force:
        raise StoreError(400, "not muted: " + _attachment_hold(pending_files)
                              + ". Mute it with force=true if the files are genuinely no "
                                "longer wanted")
    now = time.time()
    sources = _jloads(row["sources"], [])
    prod = producer_of(row["title"], sources)
    fp = fingerprint(row["title"])
    con.execute(
        "INSERT INTO suppressions (id, producer, fingerprint, sample, note, created, active)"
        " VALUES (?,?,?,?,?,?,1) ON CONFLICT(producer, fingerprint) DO UPDATE SET"
        " active=1, note=excluded.note",
        (uuid.uuid4().hex, prod, fp, row["title"], str(note or ""), now))
    con.execute("UPDATE threads SET state='done', suppressed=1, ask_kind='none',"
                " ask_prompt='', ask_options='[]', ask_context='', wake_at=0,"
                " last_change=? WHERE thread_id=?",
                (now, thread_id))
    add_event(con, thread_id, now, "joey", "", "state-change",
              "muted: " + (str(note) or "stop asking about this kind of thing"))
    con.commit()
    return {"accepted": True, "thread_id": thread_id, "producer": prod,
            "fingerprint": fp, "subject": normalize_subject(row["title"])}


def _parse_wake_at(raw):
    """Epoch seconds or ISO-8601. A naive date is read in host local time,
    because that is the clock 'in a week' means to a person."""
    if raw is None or raw == "":
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    s = str(raw).strip()
    try:
        return float(s)
    except ValueError:
        pass
    try:
        d = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=_LOCAL_TZ)
    return d.timestamp()


def snooze(con, thread_id, wake_at_raw=None, note="", hours=None, wake_at=None):
    """Defer a thread to a date, from the UI, in one action.

    Deferral is a button rather than a convention somebody has to remember,
    because the half-done version of it (park the thread and separately
    remember to bring it back) turns a deferral into a silent drop. The live
    ask is stashed so it comes back carrying the same question, and cleared
    so a hidden thread is not also lit underneath.
    """
    row = _require_thread(con, thread_id)
    if row["state"] == "done":
        raise StoreError(400, "thread is closed; reopening is a re-file, not a snooze")

    now = time.time()
    label = None
    if hours is not None:
        try:
            hours_f = float(hours)
        except (TypeError, ValueError):
            raise StoreError(400, "'hours' must be a number (the UI sends 1, 24, 168 or 720)")
        if hours_f <= 0:
            raise StoreError(400, "'hours' must be positive")
        wake_at_val = now + hours_f * 3600.0
        if wake_at_val - now > MAX_SNOOZE_S:
            raise StoreError(400, f"'hours' is more than {MAX_SNOOZE_S // 3600}h out - a snooze is a reminder, not an archive")
        whole_hours = int(hours_f) if hours_f == int(hours_f) else None
        label = {1: "1h", 24: "1d", 168: "1w", 720: "1mo"}.get(whole_hours) or f"{hours_f:g}h"
    elif wake_at_raw is not None or wake_at is not None:
        target = wake_at_raw if wake_at_raw is not None else wake_at
        wake_at_val = _parse_wake_at(target) or 0.0
        if wake_at_val <= 0:
            raise StoreError(400, "'wake_at' is required, as epoch seconds or ISO-8601")
        if wake_at_val <= now:
            raise StoreError(400, "'wake_at' is in the past, which would hide this forever")
        if wake_at_val - now > MAX_SNOOZE_S:
            raise StoreError(400, "'wake_at' is more than 400 days out: that is abandonment "
                                  "wearing a deferral's clothes")
    else:
        raise StoreError(400, "'hours' is required and must be a number (the UI sends 1, 24, 168 or 720)")

    # Preserve the ask the card will return WITH.
    ask_kind = row["ask_kind"]
    if ask_kind not in VALID_ASK or ask_kind == "none":
        ask_kind = "ack"
    ask_prompt = row["ask_prompt"] or f"Bringing this back: {row['title']}"
    options, _ = normalize_options(_jloads(row["ask_options"], []) or [])
    if ask_kind == "choice" and not options:
        ask_kind = "ack"
    kept = {
        "kind": ask_kind,
        "prompt": ask_prompt,
        "options": options,
        "context": row["ask_context"]
    }

    clean_note = str(note or "").strip()
    if label:
        wake_note = clean_note or f"Snoozed {label} - back {dt.datetime.fromtimestamp(wake_at_val, _LOCAL_TZ).strftime('%b %d %H:%M')}"
        event_body = f"snoozed by Joey for {label}" + (f" - {clean_note}" if clean_note else "")
    else:
        wake_note = clean_note or f"Snoozed - back {dt.datetime.fromtimestamp(wake_at_val, _LOCAL_TZ).strftime('%b %d %H:%M')}"
        event_body = f"snoozed until {dt.datetime.fromtimestamp(wake_at_val, _LOCAL_TZ).strftime('%Y-%m-%d %H:%M')}" + (f": {clean_note}" if clean_note else "")

    add_event(con, thread_id, now, "joey", "", "state-change", event_body)
    con.execute(
        "UPDATE threads SET state='waiting-external', ask_kind='none', ask_prompt='',"
        " ask_options='[]', ask_context='', wake_at=?, wake_note=?, wake_ask=?, last_change=?"
        " WHERE thread_id=?",
        (wake_at_val, wake_note, json.dumps(kept), now, thread_id))
    con.commit()
    return {
        "accepted": True,
        "thread_id": thread_id,
        "state": "snoozed",
        "wake_at": wake_at_val,
        "hidden_until": wake_at_val,
        "wakes": dt.datetime.fromtimestamp(wake_at_val, _LOCAL_TZ).isoformat()
    }


def set_parked(con, thread_id, parked, reason="", source="joey"):
    """Label a waiting-external thread as deliberately set aside, or bring it
    back into the plain in-flight list.

    Deliberately kept out of put_threads(): a producer resync only ever
    resends title/summary/category/etc, and those already overwrite
    unconditionally on every PUT (see put_threads' UPDATE), so if `parked`
    lived there too, an ordinary resync that simply forgot to repeat it would
    silently un-park something a human, or a reviewed sweep, had deliberately
    set aside. Routing it through its own endpoint means the only way this
    flag ever changes is a direct, reasoned call, never a side effect of
    something else refreshing.

    Restricted to `waiting-external` on the way IN, not on the way out: a
    thread has to actually be a live, unanswered wait before it can be
    called parked, but unparking a thread that has since moved on (answered,
    settled, reopened) should never itself be an error, since the fix here is
    just to stop it from misleadingly registering as a park.

    A reason is required to park something, and not to un-park it, on
    purpose: parking without saying why is exactly the failure this exists to
    prevent, a pile of things nobody can tell apart from a real wait. Bringing
    something back into view is never the half that needs explaining.
    """
    row = _require_thread(con, thread_id)
    parked = bool(parked)
    reason = str(reason or "").strip()
    if parked:
        if row["state"] != "waiting-external":
            raise StoreError(400, "only a 'waiting-external' thread can be parked (this one is "
                                  "%r); answer it, or use snooze for a hidden recheck instead"
                                  % row["state"])
        if not reason:
            raise StoreError(400, "'reason' is required to park a thread: say what it is "
                                  "actually waiting on and why nothing is pushing it, so a "
                                  "later look can tell a real wait from a dropped one")
        if len(reason) < MIN_RESULT_NOTE:
            raise StoreError(400, "'reason' is %d chars, minimum %d. A one-line reason is as "
                                  "unreviewable as no reason at all" % (len(reason), MIN_RESULT_NOTE))
    if len(reason) > MAX_NOTE:
        raise StoreError(400, "'reason' is %d chars, max %d" % (len(reason), MAX_NOTE))

    now = time.time()
    add_event(con, thread_id, now, source, "", "park" if parked else "unpark",
              reason or "brought back into the plain in-flight list")
    con.execute("UPDATE threads SET parked=?, parked_reason=? WHERE thread_id=?",
                (1 if parked else 0, reason, thread_id))
    con.commit()
    return {"accepted": True, "thread_id": thread_id, "parked": parked, "reason": reason}


def append_note(con, thread_id, note, actor="", dedupe_key=""):
    """Additive note append that provably never flips thread state.

    Contract (Helm intake rebuild slice A, 2026-09-15):
    MAY touch: exactly one new `events` row (kind 'note') plus the
      `threads.last_note` display mirror (already surfaced read-only by
      GET /threads and pending-dispatch reply_note).
    NEVER touches: state, ask_kind/prompt/options/context, blocked_on,
      recheck_at, gate_cmd, parked_since, parked, parked_reason, wake_*,
      dispatched_at, dispatch_attempts, last_dispatch_try, last_change,
      tombstoned, suppressed, responder, urgency, category, title, summary,
      sources, links, label, or any other thread column; never deletes or
      rewrites an existing event. Dispatch safety: pending_dispatch keys
      only on joey-reply events newer than dispatched_at and
      stale_dispatches keys on working + dispatched_at + absence of
      bot-result, so a kind='note' event cannot re-arm dispatch on any
      state.
    Idempotency: dedupe_key rides in events.source_id. A repeat carrying
      the same (thread_id, kind='note', source_id) returns deduped=True
      with no new row; distinct appends always append, so concurrent
      writers neither duplicate nor drop entries.
    """
    row = _require_thread(con, thread_id)
    if not isinstance(note, str):
        raise StoreError(400, "'note' is required and must be a string")
    note = note.strip()
    if not note:
        raise StoreError(400, "'note' must not be empty")
    if len(note) > MAX_NOTE:
        raise StoreError(400, "'note' is %d chars, max %d" % (len(note), MAX_NOTE))
    actor = str(actor or "").strip() or "bot"
    dedupe_key = str(dedupe_key or "")
    now = time.time()
    if dedupe_key:
        dup = con.execute(
            "SELECT ts FROM events WHERE thread_id=? AND kind='note' AND source_id=?",
            (thread_id, dedupe_key)).fetchone()
        if dup is not None:
            count = con.execute(
                "SELECT COUNT(*) FROM events WHERE thread_id=?", (thread_id,)).fetchone()[0]
            return {"accepted": True, "thread_id": thread_id, "state": row["state"],
                    "event_ts": dup["ts"], "deduped": True, "history_count": count}
    add_event(con, thread_id, now, actor, dedupe_key, "note", note)
    con.execute("UPDATE threads SET last_note=? WHERE thread_id=?", (note, thread_id))
    con.commit()
    count = con.execute(
        "SELECT COUNT(*) FROM events WHERE thread_id=?", (thread_id,)).fetchone()[0]
    return {"accepted": True, "thread_id": thread_id, "state": row["state"],
            "event_ts": now, "deduped": False, "history_count": count}


def record_operational_error(con, trace_id, component, event, details):
    """Persist an unhandled error or severe failure to operational_errors table."""
    import uuid
    err_id = "err-%s" % uuid.uuid4().hex[:12]
    now = time.time()
    con.execute(
        "INSERT INTO operational_errors (error_id, ts, trace_id, component, event, details) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (err_id, now, str(trace_id or ""), str(component or "gateway"), str(event or "error"), str(details or ""))
    )
    con.commit()
    return err_id


def post_result(con, thread_id, body):
    """A report back from whatever acted on an answer.

    Every rule below exists because its absence shipped a card that lied.
    """
    row = _require_thread(con, thread_id)
    now = time.time()

    outcome = str(body.get("outcome") or "").strip()
    if not outcome:
        raise StoreError(400, "'outcome' is required: a short status word")

    state = str(body.get("state") or "needs-you")
    if state == "snoozed":
        stored_state = "waiting-external"
    elif state in ("needs-you", "waiting-external", "done"):
        stored_state = state
    else:
        raise StoreError(400, "state %r not one of ['needs-you','waiting-external',"
                              "'snoozed','done']" % state)


    note = body.get("note")
    if note is None:
        note = body.get("outcome_note") or body.get("report")
    if not isinstance(note, str):
        raise StoreError(400, "'note' is required and must be a string: it is the ONLY thing "
                              "rendered in the thread's history. 'outcome' alone renders as "
                              "one word and says nothing. Write it to the reader: what you "
                              "did, what you found, and what they need in order to decide")
    note = note.strip()
    if len(note) < MIN_RESULT_NOTE:
        raise StoreError(400, "'note' is %d chars, minimum %d. Received: %r. If work was "
                              "drafted, the full draft goes in the note verbatim; it has "
                              "nowhere else to go, and a note that merely says a draft exists "
                              "is a failed run" % (len(note), MIN_RESULT_NOTE, note[:120]))
    if len(note) > MAX_NOTE:
        raise StoreError(400, "'note' is %d chars, max %d" % (len(note), MAX_NOTE))
    if outcome.startswith("{") and "note" in outcome:
        raise StoreError(400, "'outcome' looks like a stringified dict with the real report "
                              "trapped inside it. Send a short status word in 'outcome' and "
                              "the report in 'note'")

    # The answer gate (contract: P1 §2, LESSON 16). The note is binding, so a
    # question inside it is an instruction like any other part of it. No state
    # excuses skipping it: parking or closing a thread does not answer what was
    # asked, it just stops the card showing that nobody did.
    asked = unanswered_questions(con, thread_id)
    answers_text, answer_errors = normalize_answers(body.get("answers"))
    if answer_errors:
        raise StoreError(400, "; ".join(answer_errors))
    if asked and len(answers_text) < MIN_ANSWER:
        raise StoreError(
            400,
            "the note you are reporting on asks %d question(s) and this result carries no "
            "usable 'answers' field (%d chars, minimum %d). Answer them in 'answers' -- it "
            "is rendered at the TOP of the history entry, above your report, because it is "
            "the part that was actually asked for. Questions asked: %s"
            % (len(asked), len(answers_text), MIN_ANSWER,
               " | ".join(repr(q) for q in asked[:5])),
            {"questions": asked})
    if len(answers_text) > MAX_NOTE:
        raise StoreError(400, "'answers' is %d chars, max %d" % (len(answers_text), MAX_NOTE))

    # Optional, and deliberately NOT reset to empty when simply absent, unlike
    # `ask` below: a run that has nothing new to report should not erase a
    # link a previous run or the producer already attached. Passing an
    # explicit empty list DOES clear it; the two are told apart by whether the
    # key was sent at all, not by its value.
    links_provided = "links" in body
    links, link_errors = normalize_links(body.get("links"))
    if link_errors:
        raise StoreError(400, "; ".join(link_errors))

    ask = body.get("ask")
    ask_kind, ask_prompt, ask_options, ask_context = "none", "", [], ""
    rewritten_ask = False
    if ask is not None:
        if not isinstance(ask, dict):
            raise StoreError(400, "'ask' is " + type(ask).__name__ + ", expected an object")
        ask_kind = str(ask.get("kind") or "none")
        if ask_kind not in VALID_ASK:
            raise StoreError(400, "ask.kind %r not one of %s" % (ask_kind, sorted(VALID_ASK)))
        ask_prompt = str(ask.get("prompt") or "")
        ask_context = str(ask.get("context") or "")
        ask_options, opt_errors = normalize_options(ask.get("options"))
        if opt_errors:
            raise StoreError(400, "; ".join(opt_errors))
        if ask_kind == "choice" and not ask_options:
            raise StoreError(400, "ask.kind is 'choice' but no usable options survived "
                                  "normalization")
        if ask_kind == "choice":
            # Contract: P1 §3, at the bot's door. A run has just been reading a
            # log, a ticket and its own scratch notes, which is precisely the
            # state of mind that writes "go with the first one".
            sc_errors = self_contained_errors(ask_options, ask_prompt, ask_context)
            if sc_errors:
                raise StoreError(400, "; ".join(sc_errors))
        # The bare-ask rules, bot's door (contract: P3b §4). R1 first: the
        # banned phrase is a hard 400 at this door too. R2: a text ask that is
        # a statement is refused. R3: a pure direction-demand is REWRITTEN to
        # the ack-style summary -- the run is at the end of its rope and
        # cannot correct its payload; the store writes the honest card.
        ba_errors = bare_ask_errors(ask_kind, ask_prompt, ask_options, ask_context)
        if ba_errors:
            raise StoreError(400, "; ".join(ba_errors))
        if stored_state == "needs-you" and ask_kind == "text":
            prompt_s = ask_prompt.strip()
            if not prompt_s:
                raise StoreError(400, "ask.kind 'text' with an empty prompt: either ask "
                                      "the real question or use kind 'ack' (contract: "
                                      "P3b §4 R2)")
            if "?" not in prompt_s:
                raise StoreError(
                    400,
                    "the text ask %r has no question mark: a statement is not an ask. "
                    "Use kind 'ack' with that statement as the prompt (reading it is "
                    "the whole job), or ask the real question (contract: P3b §4 R2)"
                    % prompt_s[:80])
            if pure_direction_demand(prompt_s):
                ask_kind = "ack"
                ask_prompt = rewrite_to_ack_summary(outcome, note)
                ask_options, ask_context, rewritten_ask = [], "", True

    # The bot-close rules (contract: P1 §4, LESSONS 13, 18, 12). Placed after the
    # ask is parsed because one legitimate outcome is to hand the close back to
    # him as a tap, which needs the ack ask that came in this same post.
    answer_applied = _bool_flag(body, "answer_applied")
    nothing_owed = _bool_flag(body, "nothing_owed")
    # Contract: P7 §2 — the terminal no-action verdict. A literal boolean
    # assertion (strings/numbers 400 via _bool_flag, same as the other two),
    # valid ONLY as a settle: a no-action verdict that does not close the
    # thread is ambiguous — either the run judged nothing was owed (done) or it
    # didn't (one of the other states, without the flag).
    no_action = _bool_flag(body, "no_action")
    if no_action and state != "done":
        raise StoreError(400, "'no_action' is the terminal settle verdict and is "
                              "only valid with state 'done' (got state %r). If the run "
                              "did act, drop the flag; if it deliberately did not, this "
                              "result settles the thread" % state)
    eff_applied = bool(row["answer_applied"]) if answer_applied is None else answer_applied
    eff_owed = bool(row["nothing_owed"]) if nothing_owed is None else nothing_owed
    close_requested = False
    closed_by = ""
    if state == "done":
        if ask_kind in ("choice", "text"):
            raise StoreError(400, "state is 'done' but this result asks a new %r question. A "
                                  "card that is asking something is 'needs-you', not done: "
                                  "closing it throws the question away before he sees it"
                                  % ask_kind)
        closed_by, refusal = bot_close_reason(con, row, eff_applied, eff_owed,
                                              no_action=no_action)
        if closed_by is None:
            if ask_kind == "ack":
                # The ack ask is IN this post, which is the honest version of a
                # bot close on a thread he has never been asked about (the
                # ever-parked case): the run says it is finished, and the card
                # lights up with one tap that ends it. Nothing closes without
                # him, and nothing is left in limbo either.
                state = stored_state = "needs-you"
                close_requested = True
            else:
                raise StoreError(400, refusal)
        else:
            # A settled card is never also lit: the mechanical close clears the
            # question it is closing on.
            ask_kind, ask_prompt, ask_options, ask_context = "none", "", [], ""

    # Invariant 4a.
    if stored_state == "needs-you" and state != "snoozed" and ask_kind == "none":
        raise StoreError(400, "state is 'needs-you' but no 'ask' was given. A lit thread with "
                              "no question renders as a bare Close button, which forces your "
                              "work to be discarded in order to clear it. Include an ask "
                              "(use kind 'ack' if reading it is genuinely the whole job), or "
                              "use 'waiting-external' or 'snoozed'")

    # The park contract at the bot's door (contract: P1 §1). A snooze is exempt:
    # wake_at plus the in-store wake sweeper IS its machine-checkable exit
    # (LESSON 6), and demanding a second one would be ceremony.
    blocked_on, recheck_at, gate_cmd = "", 0.0, ""
    if state == "waiting-external":
        blocked_on, recheck_at, gate_cmd = require_park(body, now)

    wake_at = 0.0
    wake_note = wake_ask = ""
    if state == "snoozed":
        wake_at = _parse_wake_at(body.get("wake_at")) or 0.0
        if wake_at <= 0:
            raise StoreError(400, "state 'snoozed' requires 'wake_at' as epoch seconds or "
                                  "ISO-8601. A deferral is a promise, so it needs a date")
        if wake_at <= now:
            raise StoreError(400, "'wake_at' is in the past, which would hide this forever")
        if wake_at - now > MAX_SNOOZE_S:
            raise StoreError(400, "'wake_at' is more than 400 days out: that is abandonment "
                                  "wearing a deferral's clothes")
        wake_note = str(body.get("wake_note") or note)
        wa = body.get("wake_ask")
        if wa is None and ask_kind != "none":
            wa = {"kind": ask_kind, "prompt": ask_prompt, "options": ask_options,
                  "context": ask_context}
        wake_ask = json.dumps(wa or {})
        # A hidden thread must not also be lit underneath.
        ask_kind, ask_prompt, ask_options, ask_context = "none", "", [], ""

    # Invariant 5: a live deferral survives a bot result (LESSON: the snooze
    # that would not stick). Every other door already refuses to overwrite a
    # decision he made -- put_threads preserves 'done' (Invariant 2) and an
    # answer in flight (Invariant 3) -- but this one wrote wake_at=0
    # unconditionally, so ANY result landing on a snoozed thread un-hid it.
    # A producer that re-asks whenever its card is not lit (which a snooze
    # makes true by definition) therefore brought the card back within its own
    # poll interval, every interval: a snoozed thread was observed re-lighting
    # within about a minute of each snooze, twice in a row.
    #
    # The report itself is never dropped -- the event is written below exactly
    # as it would have been, so the history is whole -- but the thread stays
    # hidden until the date he picked, and the question rides along in
    # wake_ask so what comes back is the NEWEST question, not a stale one.
    #
    # 'done' is deliberately not preserved against: settling a thread needs no
    # wake, and wake_due() skips done rows anyway. An explicit state
    # 'snoozed' falls through too: re-deferring is a caller saying so on
    # purpose, which is the one thing that may move the date.
    deferred_until = float(row["wake_at"] or 0)
    snooze_kept = False
    if deferred_until > now and state in ("needs-you", "waiting-external"):
        stored_state = row["state"]
        wake_at = deferred_until
        wake_note = note
        if ask_kind != "none":
            wake_ask = json.dumps({"kind": ask_kind, "prompt": ask_prompt,
                                   "options": ask_options, "context": ask_context})
            # A new question means something is owed again, exactly as it does
            # on the lit path below; the deferral changes when he is asked, not
            # whether the answer is still outstanding.
            eff_applied = eff_owed = False
        else:
            wake_ask = row["wake_ask"] or ""
        # Hidden and lit are mutually exclusive, and a deferred thread wears
        # the park claim it already had rather than one this result invented.
        ask_kind, ask_prompt, ask_options, ask_context = "none", "", [], ""
        blocked_on = row["blocked_on"] or ""
        recheck_at = float(row["recheck_at"] or 0)
        gate_cmd = row["gate_cmd"] or ""
        snooze_kept = True

    short = outcome.replace("\n", " ")[:40]
    event_body = ("[" + short + "] " + note) if short and short not in note[:60] else note
    if answers_text:
        # Rendered FIRST, above the report and above the outcome prefix. History
        # is event bodies, and the answer to what he asked is what he opens the
        # card to read; below a paragraph of run narration it may as well be
        # missing (LESSONS 16, 11).
        event_body = ("ANSWERS TO YOUR QUESTION(S):\n" + answers_text + "\n\n" + event_body)
    add_event(con, thread_id, now, "bot", str(body.get("responder") or ""),
              "bot-result", event_body)
    if ask_kind != "none":
        # Contract: P1 §4. ask_prompt is a mutable column -- the next result
        # overwrites it -- so without this event "did he reply after the ask"
        # is unanswerable one generation back, and a stale reply could close a
        # question it never saw. Additive: no reader switches on an exhaustive
        # set of kinds.
        # Same timestamp as the result that carried it, deliberately: a reply is
        # "newer than the ask" only if it arrived after the result went out, and
        # a synthetic offset here would make a fast tap look older than the
        # question it answered.
        add_event(con, thread_id, now, "bot", str(body.get("responder") or ""),
                  "bot-ask", _ask_event_body(ask_kind, ask_prompt, ask_options, ask_context))
    if close_requested:
        add_event(con, thread_id, now + 0.002, "bot", str(body.get("responder") or ""),
                  "state-change",
                  "the run reports this is finished and asked to close it. A bot may not "
                  "close a thread, so it is here with one tap on it: acknowledging ends it")

    # A landed result means the run worked, so the retry budget resets. It
    # must also advance dispatched_at past every reply that exists as of
    # right now, not just the one this run was originally spawned for.
    # mark_dispatched()'s ok=False branch deliberately never touches
    # dispatched_at (a genuine failure has to stay retryable), so when the
    # poller gives up at the 900s mark and the run finishes anyway in the
    # background, this call is the ONLY thing left that ever will. Skipping
    # it left a late-but-successful report indistinguishable from an
    # unconsumed reply, and pending_dispatch() re-offered it on the very
    # next tick using whatever ask happened to be live by then, sometimes a
    # stale, already-settled reply to a completely different question (an
    # 'ack' that had already closed the thread, resurrected and redispatched
    # against a newer ask once an older in-flight run's late report
    # overwrote it back to 'needs-you'). Monotonic (MAX), never backward: a
    # concurrent mark_dispatched() call may already be ahead of this one.
    newest_reply = con.execute(
        "SELECT MAX(ts) FROM events WHERE thread_id=? AND kind='joey-reply'",
        (thread_id,)).fetchone()[0] or 0
    # NULL, not '[]', when the key was simply absent (see the note above the
    # normalize_links call): COALESCE only falls back on a real SQL NULL, so
    # an explicit empty list still clears it while an omitted key leaves
    # whatever link was already there untouched.
    links_param = json.dumps(links) if links_provided else None
    # A thread that is no longer parked must not keep wearing a blocker it is
    # not waiting on any more: every non-park state clears the claim, so the
    # park fields on a row are always about the park it is in RIGHT NOW.
    # parked_since is kept while a park continues, so re-parking the same thread
    # every few hours cannot reset its 30-day expiry clock.
    # Flip rules for the two assertions. A NEW question means something is owed
    # again by definition, whatever the result claimed one line earlier; a
    # handed-back close is not an applied answer either.
    if ask_kind in ("choice", "text") or close_requested:
        eff_applied = eff_owed = False

    parked_since = 0.0
    if snooze_kept:
        # Whatever clock the row was already on keeps running: a result landing
        # under a deferral must not start one, nor reset one.
        parked_since = row["parked_since"] or 0
    elif state == "waiting-external":
        parked_since = (row["parked_since"] or 0) if row["state"] == "waiting-external" else 0
        parked_since = parked_since or now
    con.execute(
        "UPDATE threads SET state=?, ask_kind=?, ask_prompt=?, ask_options=?, ask_context=?,"
        " wake_at=?,"
        " wake_note=?, wake_ask=?, responder=COALESCE(?, responder),"
        " links=COALESCE(?, links), dispatch_attempts=0,"
        " blocked_on=?, recheck_at=?, gate_cmd=?, parked_since=?,"
        " answer_applied=?, nothing_owed=?,"
        " dispatched_at=MAX(COALESCE(dispatched_at, 0), ?), last_change=? WHERE thread_id=?",
        (stored_state, ask_kind, ask_prompt, json.dumps(ask_options), ask_context,
         wake_at, wake_note,
         wake_ask, body.get("responder") or None, links_param,
         blocked_on, recheck_at, gate_cmd, parked_since,
         1 if eff_applied else 0, 1 if eff_owed else 0, newest_reply, now, thread_id))
    con.commit()
    out = {"accepted": True, "thread_id": thread_id, "state": stored_state,
           "note_len": len(note), "hidden_until": wake_at or None,
           "answered": bool(answers_text)}
    if snooze_kept:
        # Said out loud: the result landed and is in the history, but it did
        # NOT light the card. A caller that keeps re-asking because the card
        # is not lit can read this and stop.
        out["snooze_kept"] = True
        out["state_requested"] = state
    if rewritten_ask:
        # Contract: P3b §4 R3: the demand-for-direction the run sent never
        # became a card; this ack-style summary did. Said out loud so no
        # caller mistakes the stored ask for the one it sent.
        out["rewritten_ask"] = True
    if closed_by:
        out["closed_by"] = closed_by
    if close_requested:
        # Never reported as 'done': the caller asked to close and did not.
        out["close_requested"] = True
        out["closes_on"] = "his ack tap"
    if recheck_at:
        # Echoed back so the caller can see the exit it just committed to,
        # including the one it got by default rather than by asking.
        out["blocked_on"] = blocked_on
        out["recheck_at"] = recheck_at
        out["rechecks"] = dt.datetime.fromtimestamp(recheck_at, _LOCAL_TZ).isoformat()
        if gate_cmd:
            out["gate_cmd"] = gate_cmd
    return out

# --- The lesson ledger (contract: P3c §3; LESSONS 10, 11, 18, 39, 41) ---------
#
# Joey's corrections used to evaporate: they landed in a reply note, steered
# that one run, and were gone. The ledger is where an adopted correction
# becomes durable state -- one row, one scope, a changelog that reports every
# mutation, and a render read the dispatcher prepends to executor prompts.
#
# The adoption gate is the whole design (contract §2/§3): only HIS tap makes a
# lesson. The classifier (learning/classify.py) proposes; nothing adopts.

LESSON_SOURCE = "learning"
LESSON_MIN_RULE = MIN_ANSWER        # 12: the one-word-floor reading (P1 §2)
LESSON_MAX_RULE = 2000
VALID_LESSON_SCOPE_KEYS = ("global",)
MAX_RENDER_LESSONS = 20             # LESSON 34: the fixed head is the cost
GATE_MARKER = "GATE:"               # contract §5: a promise to change code

LESSON_ADOPT_VALUES = ("adopt", "adopt-with-edit")


def _lesson_scope_ok(scope: str) -> bool:
    if scope == "global":
        return True
    # A lane/skill key is a slug: letters, digits, dash, 1..48 chars.
    return bool(re.fullmatch(r"[a-z0-9][a-z0-9-]{0,47}", scope or ""))


def lesson_thread_id(rule_candidate: str) -> str:
    """The stable id for a candidate: same words, same thread (contract §2).

    Triaged once: a re-derived candidate re-opens the SAME lesson thread,
    which the four-test's T3 and invariant 3 both already protect."""
    fp = fingerprint(rule_candidate or "")
    return "lesson-" + hashlib.sha256(fp.encode("utf-8")).hexdigest()[:12]


def _lesson_options():
    """The four options the work order names, each self-contained (P1 §3):
    every label says what picking it DOES, so the card survives a screenshot."""
    return [
        {"label": "Adopt it as a standing rule",
         "value": "adopt",
         "effect": "Stores this as an active lesson; it renders into every "
                   "executor prompt for its scope from now on.",
         "terminal": False, "resolved": False},
        {"label": "Adopt it with my wording edits",
         "value": "adopt-with-edit",
         "effect": "Stores this as an active lesson using the exact wording "
                   "you send in a note with this tap; the note is binding.",
         "terminal": False, "resolved": False},
        {"label": "Not the lesson - the real one differs",
         "value": "not-the-lesson",
         "effect": "Closes this candidate and adopts nothing; send the real "
                   "lesson in a note and it gets filed as a new candidate.",
         "terminal": True, "resolved": False},
        {"label": "No lesson - that was a one-off",
         "value": "one-off",
         "effect": "Closes this candidate and adopts nothing; the correction "
                   "applied to that thread only.",
         "terminal": True, "resolved": False},
    ]


def open_lesson_thread(con, source_thread_id, event_ids, quote, rule_candidate,
                       scope="global", signals=(), now=None):
    """Open the lesson thread for a classified correction (contract §2).

    Files through put_threads -- never a raw INSERT -- so every producer-door
    gate fires on the store's own filing exactly as it does for everyone else:
    the P3b four-test (T1 passes on requires:['decision'], which is true here:
    adopting is his judgment call), the P1 §3 self-contained gate, the P3b §4
    bare-ask rules. The evidence rides the card itself: ask.context carries
    the candidate + his words verbatim + the event ids + the matched signals.
    """
    now = time.time() if now is None else now
    rule_candidate = str(rule_candidate or "").strip()
    quote = str(quote or "").strip()
    if len(rule_candidate) < LESSON_MIN_RULE:
        raise StoreError(400, "rule_candidate is %d chars, minimum %d: a "
                              "one-word rule is not a lesson (send his words "
                              "or clip them to the sentence that corrects)"
                              % (len(rule_candidate), LESSON_MIN_RULE))
    if not quote:
        raise StoreError(400, "quote is required: the thread must carry his "
                              "words verbatim, so the adoption decision is "
                              "made against the evidence, not a summary")
    if not isinstance(event_ids, (list, tuple)) or not event_ids:
        raise StoreError(400, "event_ids is required (a non-empty list): the "
                              "lesson's evidence must be traceable to the "
                              "events it came from")
    scope = str(scope or "global").strip()
    if not _lesson_scope_ok(scope):
        raise StoreError(400, "scope %r is not 'global' or a slug key "
                              "(lowercase letters/digits/dashes, 1-48 chars)"
                              % scope)

    evidence = ("RULE CANDIDATE:\n%s\n\nHIS WORDS (verbatim):\n%s\n\n"
                "SOURCE: thread %s, events %s\nSIGNALS: %s"
                % (rule_candidate, quote[:1200], source_thread_id,
                   ", ".join(str(e) for e in event_ids[:8]) or "(none)",
                   ", ".join(signals) or "(none)"))
    if len(evidence) > MAX_ASK_CONTEXT:
        # Clip his words, never the candidate: the decision is about the rule.
        overhead = len(evidence) - MAX_ASK_CONTEXT
        quote = quote[:max(LESSON_MIN_RULE, len(quote) - overhead - 40)]
        evidence = ("RULE CANDIDATE:\n%s\n\nHIS WORDS (verbatim):\n%s\n\n"
                    "SOURCE: thread %s, events %s\nSIGNALS: %s"
                    % (rule_candidate, quote, source_thread_id,
                       ", ".join(str(e) for e in event_ids[:8]) or "(none)",
                       ", ".join(signals) or "(none)"))

    tid = lesson_thread_id(rule_candidate)
    payload = {
        "thread_id": tid,
        "title": "Lesson candidate: " + rule_candidate[:80],
        "summary": ("A correction from you was classified as a lesson "
                    "candidate. Decide once whether it becomes a standing "
                    "rule for executors."),
        "category": "Learning",
        "urgency": "this-week",
        "state": "needs-you",
        "responder": "dispatcher-learning",
        "sources": [{"source": LESSON_SOURCE, "id": tid, "note": ""}],
        "events": [{
            "ts": now, "source": LESSON_SOURCE, "source_id": "",
            "kind": "lesson-candidate",
            "body": json.dumps({
                "candidate": rule_candidate, "quote": quote,
                "source_thread": str(source_thread_id or ""),
                "event_ids": [str(e) for e in event_ids],
                "signals": list(signals), "scope": scope,
            }, ensure_ascii=False),
        }],
        "ask": {"kind": "choice",
                "prompt": ("Should this correction become a standing rule for "
                           "future executor runs?"),
                "options": _lesson_options(),
                "requires": ["decision"]},
        "ask_context": evidence,
    }
    # put_threads normalizes ask context from ask["context"].
    payload["ask"]["context"] = payload.pop("ask_context")

    out = put_threads(con, {"threads": [payload]})
    rej = [r for r in out.get("rejected", []) if r.get("thread_id") == tid]
    if rej:
        raise StoreError(400, "lesson thread refused by the producer's door "
                              "(the same gates every producer faces): "
                              + "; ".join(rej[0].get("errors") or []))
    return dict(out, lesson_thread_id=tid, candidate=rule_candidate)


def _lesson_thread_row(con, thread_id):
    row = con.execute("SELECT * FROM threads WHERE thread_id=?",
                      (thread_id,)).fetchone()
    if row is None:
        raise StoreError(404, "no lesson thread %r: open it first with "
                              "open_lesson_thread" % thread_id)
    srcs = _jloads(row["sources"], [])
    if not any((s or {}).get("source") == LESSON_SOURCE for s in srcs):
        raise StoreError(400, "thread %r is not a lesson thread (its sources "
                              "carry no %r entry); create_lesson_from_thread "
                              "adopts from lesson threads only"
                              % (thread_id, LESSON_SOURCE))
    return row


def _newest_lesson_candidate_event(con, thread_id):
    row = con.execute(
        "SELECT ts, body FROM events WHERE thread_id=? AND kind='lesson-candidate'"
        " ORDER BY ts DESC LIMIT 1", (thread_id,)).fetchone()
    if row is None:
        raise StoreError(400, "lesson thread %r carries no lesson-candidate "
                              "event to adopt from" % thread_id)
    try:
        d = json.loads(row["body"])
    except ValueError:
        raise StoreError(400, "lesson thread %r's candidate event is not "
                              "readable JSON; it cannot be adopted" % thread_id)
    return float(row["ts"]), d


def _lesson_changelog(con, lesson_id) -> list:
    r = con.execute("SELECT changelog FROM lessons WHERE id=?",
                    (lesson_id,)).fetchone()
    return _jloads(r["changelog"], []) if r else []


def _lesson_append_changelog(con, lesson_id, entry: dict) -> None:
    entries = _lesson_changelog(con, lesson_id)
    entries.append(dict(entry, ts=time.time()))
    con.execute("UPDATE lessons SET changelog=? WHERE id=?",
                (json.dumps(entries, ensure_ascii=False), lesson_id))


def _lesson_decision_tap(con, row):
    """The adoption evidence: his decision reply, this generation, one of the
    two adopt values. Returns (reply_ts, value) or raises the 400 that IS the
    never-auto-adopts rule as code (contract §3)."""
    tid = row["thread_id"]
    gen = _generation_start(con, tid)
    ask_ts = newest_ask_ts(con, tid, gen)
    replies = con.execute(
        "SELECT ts, body FROM events WHERE thread_id=? AND kind='joey-reply'"
        " AND ts > ? ORDER BY ts ASC", (tid, gen)).fetchall()
    taps = [r for r in replies
            if r["ts"] > ask_ts
            and (r["body"] or "").strip().lower() in LESSON_ADOPT_VALUES]
    if not taps:
        raise StoreError(400, "no adoption tap on lesson thread %r: adopting "
                              "requires his tap (adopt or adopt-with-edit "
                              "newer than the ask). The classifier proposes; "
                              "only he decides -- nothing may adopt for him"
                              % tid)
    return float(taps[-1]["ts"]), (taps[-1]["body"] or "").strip().lower()


def _newest_joey_note_after(con, thread_id, ts):
    r = con.execute(
        "SELECT body FROM events WHERE thread_id=? AND kind='joey-note'"
        " AND ts > ? ORDER BY ts DESC LIMIT 1", (thread_id, ts)).fetchone()
    return (r["body"] or "") if r else ""


def _insert_lesson(con, lesson_id, now, source_thread, rule_text, scope,
                   changelog_entry) -> dict:
    con.execute(
        "INSERT INTO lessons (id, created_ts, source_thread, rule_text, scope,"
        " status, superseded_by, gate_status, changelog)"
        " VALUES (?,?,?,?,?,'active','','',?)",
        (lesson_id, now, source_thread, rule_text, scope,
         json.dumps([changelog_entry], ensure_ascii=False)))
    return {"id": lesson_id, "created_ts": now, "source_thread": source_thread,
            "rule_text": rule_text, "scope": scope, "status": "active",
            "superseded_by": "", "gate_status": "", "changelog": [changelog_entry]}


def create_lesson_from_thread(con, thread_id, actor="joey"):
    """The adoption gate (contract §3). HIS tap decides; this only files it.

    Exactly the answered-decision shape (P1 §4): his reply (the tap) is the
    decision, this post reports it applied and nothing owed, and the thread
    settles on that -- the one mechanical close where the closing was already
    his (LESSON 18). No tap -> 400: that refusal is the never-auto-adopts rule.
    """
    if str(actor or "joey").strip().lower() != "joey":
        raise StoreError(400, "actor %r may not adopt a lesson: adoption is "
                              "his tap on the lesson thread, never a "
                              "programmatic act (contract: P3c §3)" % actor)
    row = _lesson_thread_row(con, thread_id)
    if row["state"] == "done" and (row["ask_kind"] or "none") == "none":
        # Idempotent re-post after a settle: return the existing lesson rather
        # than a confusing 400 (a retry is not a second adoption attempt).
        existing = con.execute(
            "SELECT * FROM lessons WHERE source_thread=? ORDER BY created_ts"
            " DESC LIMIT 1", (thread_id,)).fetchone()
        if existing is not None:
            return _lesson_row_out(existing, already=True)
    tap_ts, tap_value = _lesson_decision_tap(con, row)
    cand_ts, cand = _newest_lesson_candidate_event(con, thread_id)
    candidate = str(cand.get("candidate") or "").strip()
    scope = str(cand.get("scope") or "global").strip() or "global"
    if len(candidate) < LESSON_MIN_RULE:
        raise StoreError(400, "the stored candidate is under the %d-char "
                              "floor; re-open the lesson thread with the "
                              "full words" % LESSON_MIN_RULE)

    rule_text = candidate
    edited = ""
    if tap_value == "adopt-with-edit":
        # The binding note (LESSON 15): his edited wording IS the rule.
        edited = _newest_joey_note_after(con, thread_id, tap_ts - 0.002).strip()
        if len(edited) < LESSON_MIN_RULE:
            raise StoreError(400, "adopt-with-edit needs his edited wording "
                                  "in a note sent with or after the tap (%d "
                                  "chars found, minimum %d): without it there "
                                  "is nothing to adopt but the original "
                                  "candidate, which is what 'adopt' is for"
                                  % (len(edited), LESSON_MIN_RULE))
        rule_text = edited
    if len(rule_text) > LESSON_MAX_RULE:
        rule_text = rule_text[:LESSON_MAX_RULE]

    now = time.time()
    tid = row["thread_id"]
    existing = con.execute(
        "SELECT * FROM lessons WHERE source_thread=? ORDER BY created_ts DESC"
        " LIMIT 1", (tid,)).fetchone()
    if existing is not None:
        if fingerprint(existing["rule_text"]) == fingerprint(rule_text):
            # Same words: the same lesson. Read it back, settle if needed.
            lesson = _lesson_row_out(existing, already=True)
        else:
            # His re-edit after a reopen: the tap is the authority, and one
            # live lesson per thread -- supersede the old row (contract §3).
            lesson = supersede_lesson(con, existing["id"], rule_text,
                                      scope=scope, actor=actor,
                                      _from_create=True)
    else:
        lesson_id = "lsn-" + uuid.uuid4().hex[:12]
        entry = {"event": "adopted", "actor": "joey", "tap": tap_value,
                 "tap_ts": tap_ts, "signals": list(cand.get("signals") or []),
                 "source_thread": tid,
                 "quote": str(cand.get("quote") or "")[:1200],
                 "edited": bool(edited)}
        if _is_gate_rule(rule_text):
            entry["gate"] = "pending"
        lesson = _insert_lesson(con, lesson_id, now, tid, rule_text, scope, entry)
    if _is_gate_rule(lesson["rule_text"]):
        # Adopting a GATE: lesson is a promise to change code (contract §5).
        row_gate = con.execute(
            "SELECT gate_status FROM lessons WHERE id=?", (lesson["id"],)).fetchone()
        if row_gate and row_gate["gate_status"] == "":
            con.execute("UPDATE lessons SET gate_status='pending' WHERE id=?",
                        (lesson["id"],))
            lesson["gate_status"] = "pending"

    # Settle the thread through post_result: answered-decision (P1 §4) -- his
    # tap is the decision; this post applies it and owes nothing more.
    outcome = ("lesson adopted (his edited wording)" if edited
               else "lesson adopted")
    note = ("%s: %r [scope %s, lesson %s]. The rule now renders into executor "
            "prompts for its scope until retired." % (outcome, rule_text[:200],
                                                      lesson["scope"], lesson["id"]))
    res = post_result(con, tid, {
        "outcome": outcome, "state": "done",
        "note": note,
        "responder": "dispatcher-learning",
        "answer_applied": True, "nothing_owed": True,
        "links": [{"label": "lesson " + lesson["id"],
                   "url": "lesson://" + lesson["id"]}],
    })
    con.commit()
    out = dict(lesson)
    out["settled"] = res.get("closed_by") == "answered-decision"
    out["thread_state"] = res.get("state")
    return out


def _is_gate_rule(rule_text: str) -> bool:
    return str(rule_text or "").strip().lower().startswith(GATE_MARKER.lower())


def _lesson_row_out(r, already=False) -> dict:
    d = {"id": r["id"], "created_ts": r["created_ts"],
         "source_thread": r["source_thread"], "rule_text": r["rule_text"],
         "scope": r["scope"], "status": r["status"],
         "superseded_by": r["superseded_by"] or "",
         "gate_status": r["gate_status"] or "",
         "changelog": _jloads(r["changelog"], [])}
    if already:
        d["already"] = True
    return d


def lessons_list(con, scope=None, status=None, render_for=None):
    """The ledger read (contract §3). render_for is the dispatcher's view:
    active lessons for ('global', key) only, capped at MAX_RENDER_LESSONS."""
    if render_for is not None:
        key = str(render_for or "").strip()
        rows = con.execute(
            "SELECT * FROM lessons WHERE status='active' AND scope IN (?,?)"
            " ORDER BY created_ts, id", ("global", key)).fetchall()
        return [_lesson_row_out(r) for r in rows[:MAX_RENDER_LESSONS]]
    rows = con.execute(
        "SELECT * FROM lessons WHERE (?1 IS NULL OR scope=?1)"
        " AND (?2 IS NULL OR status=?2) ORDER BY created_ts, id",
        (scope, status)).fetchall()
    return [_lesson_row_out(r) for r in rows]


def lesson_get(con, lesson_id):
    r = con.execute("SELECT * FROM lessons WHERE id=?", (lesson_id,)).fetchone()
    if r is None:
        raise StoreError(404, "no lesson %r" % lesson_id)
    return _lesson_row_out(r)


def lessons_for_prompt(con, scope):
    """The render read (contract §4): active only, global or the exact key,
    (created_ts, id) order, capped -- the fixed head stays fixed (LESSON 34)."""
    return lessons_list(con, render_for=scope)


def retire_lesson(con, lesson_id, reason, actor="joey"):
    """active -> retired (contract §3). A reason is required and the change is
    reported in the changelog; retiring a retired lesson is a LOUD 400, not a
    quiet no-op -- a no-op here would make 'retired' unauditable."""
    r = con.execute("SELECT * FROM lessons WHERE id=?", (lesson_id,)).fetchone()
    if r is None:
        raise StoreError(404, "no lesson %r to retire" % lesson_id)
    if r["status"] != "active":
        raise StoreError(400, "lesson %r is already %s (retired %s): retiring "
                              "again would be a no-op wearing an audit trail"
                              % (lesson_id, r["status"],
                                 "by " + (r["superseded_by"] or "him")
                                 if r["superseded_by"] else ""))
    reason = str(reason or "").strip()
    if len(reason) < LESSON_MIN_RULE:
        raise StoreError(400, "a retirement reason is required (min %d chars): "
                              "a retired lesson never renders again, so the "
                              "changelog must say why it stopped being true"
                              % LESSON_MIN_RULE)
    con.execute("UPDATE lessons SET status='retired' WHERE id=?", (lesson_id,))
    _lesson_append_changelog(con, lesson_id,
                             {"event": "retired", "actor": actor,
                              "reason": reason[:500]})
    con.commit()
    return lesson_get(con, lesson_id)


def supersede_lesson(con, lesson_id, rule_text, scope=None, actor="joey",
                     _from_create=False):
    """Replace a lesson ATOMICALLY (contract §3): validate the new rule and
    scope BEFORE touching the old row, then one commit creates the new active
    lesson and flips the old to retired with superseded_by set. A failed
    supersede leaves the old row untouched and nothing new -- probed."""
    old = con.execute("SELECT * FROM lessons WHERE id=?",
                      (lesson_id,)).fetchone()
    if old is None:
        raise StoreError(404, "no lesson %r to supersede" % lesson_id)
    if old["status"] != "active":
        raise StoreError(400, "lesson %r is %s, not active: only an active "
                              "lesson can be superseded" % (lesson_id, old["status"]))
    rule_text = str(rule_text or "").strip()
    if len(rule_text) < LESSON_MIN_RULE:
        raise StoreError(400, "the replacement rule is %d chars, minimum %d: "
                              "superseding with a non-rule would leave the "
                              "fleet rendering nothing where it used to "
                              "render guidance" % (len(rule_text), LESSON_MIN_RULE))
    new_scope = str(scope if scope is not None else old["scope"]).strip()
    if not _lesson_scope_ok(new_scope):
        raise StoreError(400, "scope %r is not 'global' or a slug key" % new_scope)
    now = time.time()
    new_id = "lsn-" + uuid.uuid4().hex[:12]
    entry = {"event": "supersedes", "actor": actor, "supersedes": old["id"],
             "old_rule": old["rule_text"][:200], "source_thread": old["source_thread"]}
    if _is_gate_rule(rule_text):
        entry["gate"] = "pending"
    # One commit for the whole swap: the new row lands and the old flips in
    # the same transaction (sqlite3 holds the implicit txn until commit), so
    # a crash mid-supersede rolls back BOTH halves -- never two live lessons.
    _insert_lesson(con, new_id, now, old["source_thread"], rule_text, new_scope, entry)
    con.execute("UPDATE lessons SET status='retired', superseded_by=? WHERE id=?",
                (new_id, lesson_id))
    _lesson_append_changelog(con, lesson_id,
                             {"event": "superseded-by", "actor": actor,
                              "by": new_id, "new_rule": rule_text[:200]})
    con.commit()
    return lesson_get(con, new_id)


def mark_gate_implemented(con, lesson_id, commit_ref, actor="orchestrator"):
    """Record that a GATE lesson landed as code (contract §5). The ledger
    records the implementation commit; the listing stops showing it."""
    r = con.execute("SELECT * FROM lessons WHERE id=?", (lesson_id,)).fetchone()
    if r is None:
        raise StoreError(404, "no lesson %r" % lesson_id)
    if not _is_gate_rule(r["rule_text"]):
        raise StoreError(400, "lesson %r is not a GATE: lesson (rule does not "
                              "start with %s); there is nothing to implement"
                              % (lesson_id, GATE_MARKER))
    if r["gate_status"] == "implemented":
        raise StoreError(400, "lesson %r's gate is already implemented (%s)"
                              % (lesson_id, "see changelog"))
    con.execute("UPDATE lessons SET gate_status='implemented' WHERE id=?",
                (lesson_id,))
    _lesson_append_changelog(con, lesson_id,
                             {"event": "gate-implemented", "actor": actor,
                              "commit": str(commit_ref or "")[:80]})
    con.commit()
    return lesson_get(con, lesson_id)


def lessons_pending_gates(con):
    """The honest gap, visible (contract §5; LESSON 23): every ACTIVE lesson
    adopted as a GATE that has not landed as code yet. A retired pending gate
    leaves the listing -- a withdrawn lesson is a withdrawn promise."""
    rows = con.execute(
        "SELECT * FROM lessons WHERE status='active' AND gate_status='pending'"
        " ORDER BY created_ts, id").fetchall()
    return [_lesson_row_out(r) for r in rows]


# --- Claims (executor-agnostic dispatch surface, P2) --------------------------

_CLAIMS_DDL = """
CREATE TABLE IF NOT EXISTS claims (
  claim_id    TEXT PRIMARY KEY,
  thread_id   TEXT NOT NULL,
  executor_id TEXT NOT NULL,
  runtime     TEXT NOT NULL,
  caps        TEXT NOT NULL DEFAULT '{}',
  claimed_at  REAL NOT NULL,
  deadline    REAL NOT NULL,
  released_at REAL,
  result_at   REAL,
  reported    TEXT
);
CREATE INDEX IF NOT EXISTS claims_thread ON claims(thread_id);
"""


def init_claims(con: sqlite3.Connection) -> None:
    con.executescript(_CLAIMS_DDL)
    con.commit()

# --- Labels & Intake Routing (Contract P6) -----------------------------------

LABEL_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{1,31}$")
RESERVED_LABELS = {"__proto__", "admin", "api", "settings", "null", "undefined"}
VALID_LABEL_RUNTIMES = {"hermes", "claude", "webhook", "none"}
MIN_PROMPT_TEMPLATE = 20
MAX_PROMPT_TEMPLATE = 10000


def validate_label(data: dict) -> tuple[dict | None, list[str]]:
    errors = []
    if not isinstance(data, dict):
        return None, ["label data must be a JSON object"]
    name = str(data.get("name") or "").strip()
    if not name:
        errors.append("name is required")
    elif not LABEL_NAME_RE.match(name):
        errors.append("name must match ^[a-z0-9][a-z0-9_-]{1,31}$ (lowercase alphanumeric with dashes/underscores, 2-32 chars)")
    elif name in RESERVED_LABELS:
        errors.append(f"name {name!r} is reserved")

    prompt = str(data.get("prompt_template") or "").strip()
    if not prompt:
        errors.append("prompt_template is required (minimum 20 characters)")
    elif len(prompt) < MIN_PROMPT_TEMPLATE:
        errors.append(f"prompt_template is {len(prompt)} chars, minimum {MIN_PROMPT_TEMPLATE}")
    elif len(prompt) > MAX_PROMPT_TEMPLATE:
        errors.append(f"prompt_template is {len(prompt)} chars, maximum {MAX_PROMPT_TEMPLATE}")

    runtime = str(data.get("executor_runtime") or "hermes").strip().lower()
    if runtime not in VALID_LABEL_RUNTIMES:
        errors.append(f"executor_runtime {runtime!r} not one of {sorted(VALID_LABEL_RUNTIMES)}")

    webhook = str(data.get("webhook_url") or "").strip()
    if webhook and not (webhook.startswith("http://") or webhook.startswith("https://")):
        errors.append("webhook_url must be empty or a valid http/https URL")

    desc = str(data.get("description") or "").strip()
    convert_human = 1 if data.get("convert_to_needs_human") in (1, True, "1", "true") else 0
    enabled = 0 if data.get("enabled") in (0, False, "0", "false") else 1

    clean = {
        "name": name,
        "description": desc,
        "prompt_template": prompt,
        "webhook_url": webhook,
        "executor_runtime": runtime,
        "convert_to_needs_human": convert_human,
        "enabled": enabled,
    }
    if errors:
        return None, errors
    return clean, []


def list_labels(con: sqlite3.Connection, include_disabled: bool = False) -> list[dict]:
    sql = "SELECT * FROM labels"
    if not include_disabled:
        sql += " WHERE enabled=1"
    sql += " ORDER BY name ASC"
    rows = con.execute(sql).fetchall()
    return [{
        "name": r["name"],
        "description": r["description"],
        "prompt_template": r["prompt_template"],
        "webhook_url": r["webhook_url"],
        "executor_runtime": r["executor_runtime"],
        "convert_to_needs_human": bool(r["convert_to_needs_human"]),
        "enabled": bool(r["enabled"]),
        "created_at": r["created_at"],
        "updated_at": r["updated_at"],
    } for r in rows]


def get_label(con: sqlite3.Connection, name: str) -> dict | None:
    if not name:
        return None
    r = con.execute("SELECT * FROM labels WHERE name=?", (name.strip().lower(),)).fetchone()
    if r is None:
        return None
    return {
        "name": r["name"],
        "description": r["description"],
        "prompt_template": r["prompt_template"],
        "webhook_url": r["webhook_url"],
        "executor_runtime": r["executor_runtime"],
        "convert_to_needs_human": bool(r["convert_to_needs_human"]),
        "enabled": bool(r["enabled"]),
        "created_at": r["created_at"],
        "updated_at": r["updated_at"],
    }


def put_label(con: sqlite3.Connection, data: dict) -> dict:
    clean, errors = validate_label(data)
    if errors:
        raise StoreError(400, "; ".join(errors))
    now = time.time()
    existing = con.execute("SELECT created_at FROM labels WHERE name=?", (clean["name"],)).fetchone()
    created_at = existing["created_at"] if existing else now
    con.execute(
        "INSERT INTO labels (name, description, prompt_template, webhook_url, executor_runtime, "
        "convert_to_needs_human, enabled, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(name) DO UPDATE SET "
        "description=excluded.description, "
        "prompt_template=excluded.prompt_template, "
        "webhook_url=excluded.webhook_url, "
        "executor_runtime=excluded.executor_runtime, "
        "convert_to_needs_human=excluded.convert_to_needs_human, "
        "enabled=excluded.enabled, "
        "updated_at=excluded.updated_at",
        (clean["name"], clean["description"], clean["prompt_template"], clean["webhook_url"],
         clean["executor_runtime"], clean["convert_to_needs_human"], clean["enabled"],
         created_at, now)
    )
    con.commit()
    return get_label(con, clean["name"])


def delete_label(con: sqlite3.Connection, name: str) -> dict:
    name = name.strip().lower()
    lbl = get_label(con, name)
    if lbl is None:
        raise StoreError(404, f"no such label: {name}")
    ref = con.execute(
        "SELECT COUNT(*) FROM threads WHERE label=? AND state != 'done' AND tombstoned=0",
        (name,)
    ).fetchone()[0]
    now = time.time()
    if ref > 0:
        con.execute("UPDATE labels SET enabled=0, updated_at=? WHERE name=?", (now, name))
        con.commit()
        return {"name": name, "deleted": False, "disabled": True, "active_threads": ref}
    con.execute("DELETE FROM labels WHERE name=?", (name,))
    con.commit()
    return {"name": name, "deleted": True, "disabled": False}


def get_settings(con: sqlite3.Connection) -> dict[str, str]:
    rows = con.execute("SELECT key, value FROM settings").fetchall()
    out = {r["key"]: r["value"] for r in rows}
    defaults = {
        "default_intake_label": "triage",
        "llm_conversion_enabled": "1",
        "needs_human_prompt": "Convert raw intake tasks into clear, actionable needs-you choices for Joey with multiple-choice options and no context assumed.",
    }
    for k, v in defaults.items():
        if k not in out:
            out[k] = v
    return out


def put_settings(con: sqlite3.Connection, updates: dict) -> dict[str, str]:
    if not isinstance(updates, dict):
        raise StoreError(400, "settings must be an object")
    now = time.time()
    for k, v in updates.items():
        if not isinstance(k, str) or not k.strip():
            continue
        v_str = str(v)
        con.execute(
            "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            (k.strip(), v_str, now)
        )
    con.commit()
    return get_settings(con)


def set_thread_label(con: sqlite3.Connection, thread_id: str, label: str) -> dict:
    row = _require_thread(con, thread_id)
    lbl_str = label.strip().lower()
    if lbl_str:
        lbl = get_label(con, lbl_str)
        if lbl is None:
            raise StoreError(400, f"label {lbl_str!r} does not exist in labels table")
        if not lbl["enabled"]:
            raise StoreError(400, f"label {lbl_str!r} is disabled")
    now = time.time()
    con.execute("UPDATE threads SET label=?, last_change=? WHERE thread_id=?", (lbl_str, now, thread_id))
    add_event(con, thread_id, now, "system", "", "state-change",
              f"label set to {lbl_str!r}" if lbl_str else "label cleared")
    con.commit()
    return {"thread_id": thread_id, "label": lbl_str, "accepted": True}

