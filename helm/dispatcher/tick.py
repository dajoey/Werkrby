"""dispatcher/tick.py — the claim-before-spawn pump (contract: docs/contracts/P3.md).

One thread per tick. Ordering, in this exact order, probed by tests:

  1. heartbeat file touched EVERY run, including no-ops (LESSON 21)
  2. run-dir retention sweep (30d)
  3. gave-up restore (the visible restore-to-ack path)
  4. spending brake — distinct words: usage-over vs usage-blind, fail closed
  5. auth pre-check (a sign-in that cannot succeed must not burn attempts)
  6. round-cap check (4; only run-produced bot-result events count)
  7. POST /claims — BEFORE any spawn (LESSON 26: crash re-offers the answer)
  8. POST /dispatched mode=start — attempt claimed before the executor exists
  9. spawn adapter under the OS jail, caps held by the runner
 10. adapter reader maps artifacts onto one Result (executor-agnostic)
 11. POST the result through the claim EXACTLY ONCE (harness posts; the
     executor has no network). A 409 re-report means already-posted: success.
 12. on run failure: mark_dispatched ok=False — no extra attempt charged
     (start already claimed it), backoff applies, 5 attempts → gave_up,
     visible, restore path on the next tick.

Single instance: flock held on a module-global fd for the LIFE OF THE
PROCESS (the Python form of LESSONS "subshell flock drops on exit" — a
lock taken in a scope that exits is not a lock). A second tick while one
holds it logs one line and exits 0.

NO model inside the pump. The executor CLIs appear only as adapter data.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import sys
import time
import subprocess
import tempfile
import urllib.error
import urllib.request

if __package__ in (None, ""):  # direct script run: python3 dispatcher/tick.py
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from dispatcher import adapters, brake as brake_mod, caps as caps_mod
    from dispatcher import jail as jail_mod, retention
    from dispatcher.common import clip, env_int, log, write_json_atomic
else:
    from . import adapters, brake as brake_mod, caps as caps_mod
    from . import jail as jail_mod, retention
    from .common import clip, env_int, log, write_json_atomic

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATE_DIR = os.environ.get("HELM_DISPATCHER_STATE",
                           os.path.join(ROOT, "data", "dispatcher"))
RUNS_DIR = os.environ.get("HELM_RUNS_DIR", os.path.join(ROOT, "data", "runs"))
# --selftest parses RECORDED run dirs — captured artifacts, never a live run —
# so a checkout without them can point this at a tree it builds and deletes.
# Unset, it is the deployment's own `runs/` beside the subsystem, as before.
SELFTEST_RUNS_DIR = os.environ.get("HELM_SELFTEST_RUNS_DIR",
                                   os.path.join(ROOT, "runs"))
HEARTBEAT = os.path.join(STATE_DIR, "heartbeat.json")
LOCK_PATH = os.path.join(STATE_DIR, "tick.lock")

API_BASE = os.environ.get("HELM_API", "http://127.0.0.1:3038").rstrip("/")
APP_TOKEN = os.environ.get("HELM_APP_TOKEN", "")

MAX_ROUNDS = env_int("HELM_MAX_ROUNDS", 4)
ROUND_CAP_OVERRIDE = "one more round"
MAX_PER_TICK = 1
RESULT_POST_RETRIES = 2
NON_RUN_SOURCES = {"helm-wake", "helm-gave-up"}
EXECUTOR_ID = os.environ.get("HELM_EXECUTOR_ID", "helm-dispatcher-p3")
RUNTIME = os.environ.get("HELM_RUNTIME", "claude")   # claude | hermes

AUTH_MARKERS = ("failed to authenticate", "oauth session expired", "invalid api key",
                "please run /login", "not logged in", "authentication_error")

# Module-global: holds the flock fd for the process lifetime. NEVER closed,
# NEVER opened inside a function scope that returns (the subshell-flock bug).
_LOCK_FD = None


def acquire_lock(path: str = LOCK_PATH):
    """Take the single-instance flock on the MAIN process fd. Returns fd or
    None if another tick holds it. The caller keeps the returned fd alive;
    tick.main() stores it in _LOCK_FD for exactly that."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        return None
    os.write(fd, b"%d\n" % os.getpid())
    return fd


# ------------------------------------------------------------------ gateway

class Gateway:
    """Minimal authed client. The ONLY network the dispatcher itself holds:
    the loopback/LAN gateway API. Executors get no network at all."""

    def __init__(self, base=API_BASE, token=APP_TOKEN, timeout=15.0):
        self.base = base
        self.token = token
        self.timeout = timeout

    def call(self, method: str, path: str, payload=None) -> tuple[int, dict]:
        data = json.dumps(payload).encode() if payload is not None else None
        req = urllib.request.Request(
            self.base + path, data=data, method=method,
            headers={"Content-Type": "application/json",
                     "Authorization": "Bearer " + self.token})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                body = r.read().decode("utf-8", "replace")
                return r.status, (json.loads(body) if body.strip() else {})
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace")
            try:
                parsed = json.loads(body)
            except ValueError:
                parsed = {"detail": body[:500]}
            return e.code, parsed

    def get(self, path):
        return self.call("GET", path)

    def post(self, path, payload):
        return self.call("POST", path, payload)


# ------------------------------------------------------------------ rounds

# A round-cap hand-back is posted BY the dispatcher as a bot-result with this
# responder (the event's source_id). It is a note to Joey, not a run: counting
# it meant a capped thread's own hand-back permanently read as round 5 and
# could never be answered again. Same class as the work system's wake-events-
# counted-as-rounds bug (2026-09-09).
NON_RUN_RESPONDERS = {"dispatcher-roundcap", "dispatcher-skeptic"}


def bot_rounds(history) -> int:
    """Run-produced rounds only: bot-result events whose source is not a
    dispatcher note (a gave-up, wake, or round-cap hand-back note is not a
    round — counting them once made a snooze-and-wake cycle hit the cap with
    zero runs)."""
    return sum(1 for e in (history or [])
               if (e or {}).get("kind") == "bot-result"
               and (e or {}).get("source") not in NON_RUN_SOURCES
               and (e or {}).get("source_id") not in NON_RUN_RESPONDERS)


def is_auth_failure(stdout: str, stderr: str) -> bool:
    blob = ((stdout or "") + " " + (stderr or "")).lower()
    return any(m in blob for m in AUTH_MARKERS)


# ------------------------------------------------------------------ prompt

PROMPT_TEMPLATE = """You are executing one settled decision on a personal ops board.
Work ONLY inside the current directory. You have NO network access.

{switches}
{lessons_block}
THREAD: {title}
CATEGORY: {category} | URGENCY: {urgency}
THE DECISION THAT WAS MADE (your job is to carry it out):
  choice: {reply_value}
  note: {reply_note}{prior_rounds}

{history_block}
DELIVERABLE: do the work the decision describes, then write a file named
result.json in the current directory with EXACTLY this shape:
  {{"outcome": "<status word>", "state": "needs-you",
    "note": "<what you did, what you found, what he needs to decide — MINIMUM 80 chars,
              this is the only thing rendered in the thread's history>",
    "ask": {{"kind": "ack", "prompt": "<one tap that settles it, self-contained>"}}}}
If you cannot finish, still write result.json with outcome "partial" and an
honest note. A capped or partial run keeps its work; do not start over.

NO-ACTION VERDICT: if the decision genuinely requires NO action — his note IS
the whole instruction and recording it is the entire job — write result.json as
the terminal settle instead:
  {{"outcome": "note-only", "state": "done", "no_action": true,
    "note": "<what you judged and WHY nothing was done — MINIMUM 80 chars>"}}
This settles the thread; it is never retried. Use it ONLY when acting would be
wrong. The store refuses it when his reply asked a question you did not answer
('answers' is still required) or when there is no reply to settle on.
Write result.json as your LAST action. Do not write anywhere else."""

# The numbered lessons block (contract: P3c §4). Standing rules HE adopted,
# rendered as data into every executor prompt for the scope. Top of the brief:
# lessons constrain HOW the work is done, so they come before the work.
LESSONS_HEADER = "STANDING RULES (lessons he adopted; they bind this run):"


def render_lessons(rules) -> str:
    """Active lessons as a numbered block (contract: P3c §4). Pure formatter:
    deterministic order comes from the store read; each rule clipped to 300
    chars; empty in -> empty out (a scope with no lessons renders nothing)."""
    lines = []
    for i, r in enumerate(rules or [], 1):
        text = " ".join(str((r or {}).get("rule_text") or "").split())[:300]
        if not text:
            continue
        lines.append("%d. %s" % (i, text))
    if not lines:
        return ""
    return LESSONS_HEADER + "\n" + "\n".join(lines) + "\n\n"


def fetch_lessons(gw, scope_key: str) -> list:
    """The render read over the API (contract §4). Fails OPEN with a loud log
    line: a dead lesson read must not stop the board's work from running
    (contract §open-questions 3); the miss is logged, never silent. gw None
    (an offline caller) renders nothing rather than crashing."""
    if gw is None:
        return []
    try:
        code, body = gw.get("/api/triage/lessons?render_for=" + scope_key)
        if code == 200 and isinstance(body.get("lessons"), list):
            return body["lessons"]
        log("  LESSONS-MISS render read for scope %r -> %d %s"
            % (scope_key, code, clip(str(body.get("detail") or ""), 120)))
    except Exception as e:
        log("  LESSONS-MISS render read for scope %r failed: %s" % (scope_key, e))
    return []


class _SafeDict(dict):
    def __missing__(self, key):
        return "{" + key + "}"


def fetch_label(gw, label_name: str) -> dict | None:
    if gw is None or not label_name:
        return None
    try:
        code, body = gw.get("/api/triage/labels?include_disabled=0")
        if code == 200 and isinstance(body.get("labels"), list):
            for l in body["labels"]:
                if l.get("name") == str(label_name).strip().lower():
                    return l
    except Exception as e:
        log("  LABEL-MISS render read for label %r failed: %s" % (label_name, e))
    return None


def _send_webhook(url: str, payload: dict) -> None:
    if not url or not (url.startswith("http://") or url.startswith("https://")):
        return
    import urllib.request
    try:
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json", "User-Agent": "Helm-Dispatcher/1.0"},
        )
        with urllib.request.urlopen(req, timeout=5) as _:
            pass
    except Exception as e:
        log("  WEBHOOK-FAIL delivery to %s failed: %s" % (url, e))


def build_prompt(item: dict, run_dir: str, rounds: int, lessons=None, label_data=None) -> str:
    prior = ""
    if rounds:
        prior = ("\n  (round %d of %d on this thread — earlier rounds already ran; "
                 "do not redo them)" % (rounds + 1, MAX_ROUNDS))
    hist_lines = []
    for e in (item.get("history") or [])[-8:]:
        body = clip((e or {}).get("body"), 160).replace("\n", " ")
        hist_lines.append("- [%s] %s %s" % ((e or {}).get("kind"),
                                            (e or {}).get("source") or "", body))
    history_block = ("RECENT THREAD HISTORY (newest last):\n" + "\n".join(hist_lines)
                     + "\n" if hist_lines else "")
    # Scope key from the thread's category, slugged deterministically: the
    # store's render read is exact-key + global (contract P3c §3).
    scope_key = _scope_key_of(item)

    # Contract P6 §3: Label-based prompt template
    lbl = label_data
    if lbl is None and item.get("label"):
        lbl = fetch_label(_GW_FOR_RENDER, item["label"])

    template = PROMPT_TEMPLATE
    if lbl and lbl.get("prompt_template"):
        tmpl = lbl["prompt_template"].strip()
        if "DELIVERABLE:" not in tmpl:
            tmpl += ("\n\nDELIVERABLE: do the work the decision describes, then write a file named\n"
                     "result.json in the current directory with EXACTLY this shape:\n"
                     "  {{\"outcome\": \"<status word>\", \"state\": \"needs-you\",\n"
                     "    \"note\": \"<what you did, what you found, what he needs to decide — MINIMUM 80 chars>\",\n"
                     "    \"ask\": {{\"kind\": \"ack\", \"prompt\": \"<one tap that settles it, self-contained>\"}}}}"
                     "\nWrite result.json as your LAST action. Do not write anywhere else.")
        if lbl.get("convert_to_needs_human"):
            tmpl += (
                "\n\nCONVERT TO NEEDS-HUMAN: Review the input and structure it as a decision for Joey.\n"
                "The deliverable MUST be a self-contained needs-you card with kind 'choice' or 'ack',\n"
                "clear options, and no assumed context."
            )
        template = tmpl

    ctx = {
        "switches": adapters.render_switches(RUNTIME),
        "lessons_block": render_lessons(lessons if lessons is not None
                                     else fetch_lessons(_GW_FOR_RENDER, scope_key)),
        "title": clip(item.get("title"), 200),
        "category": item.get("category") or "-",
        "urgency": item.get("urgency") or "-",
        "reply_value": clip(item.get("reply_value"), 200),
        "reply_note": clip(item.get("reply_note") or "(none)", 400),
        "prior_rounds": prior,
        "history_block": history_block,
    }
    if "{history_block}" not in template and history_block:
        template = template + "\n\n" + history_block
    if "{lessons_block}" not in template and ctx["lessons_block"]:
        template = ctx["lessons_block"] + "\n" + template
    if "{switches}" not in template:
        template = ctx["switches"] + "\n\n" + template

    prompt = template.format_map(_SafeDict(ctx))
    if _item_has_questions(item):
        prompt += (
            "\n\nNOTE ASKS A QUESTION:\n"
            "The owner's decision note contains one or more questions ('?').\n"
            "Your result.json MUST carry a top-level 'answers' field (min 12 chars) answering what was asked:\n"
            "  \"answers\": \"<your answer to the question(s)>\""
        )
    return prompt[:8000]


def _scope_key_of(item: dict) -> str:
    """The render scope for a pending item: its category, slugged (contract
    §4). 'LinuxOps' -> 'linuxops'; empty category -> 'global' only matches."""
    cat = str(item.get("category") or "").strip().lower()
    return "".join(c if c.isalnum() or c == "-" else "-" for c in cat).strip("-")[:48]


def _is_lesson_item(item: dict) -> bool:
    """A pending item that is a lesson thread (contract: P3c §4): its sources
    carry source='learning'. pending_dispatch passes sources through, so the
    check is pure data, no extra read."""
    return any((s or {}).get("source") == "learning"
               for s in (item.get("sources") or []))


def _apply_lesson_tap(gw: Gateway, item: dict) -> dict:
    """Apply his adoption tap deterministically (contract: P3c §4).

    A lesson thread reaching the dispatch queue means he tapped adopt or
    adopt-with-edit (terminal taps settle in the store and never queue). An
    executor cannot do this work -- it has no network and cannot write the
    ledger -- so the tick does it itself, through the store's adoption gate,
    which refuses anything without his tap. A gate refusal (e.g. an
    adopt-with-edit with no note) is reposted honestly as a needs-you result
    re-presenting the same four options with the refusal detail -- never a
    bare card, never a gave-up loop (contract P3c §4).
    """
    tid = item["thread_id"]
    log("  LESSON %s applying his tap through the adoption gate" % tid)
    code, body = gw.post("/api/triage/lessons/from-thread/%s" % tid, {})
    if code == 202:
        _manifest_add(tid, os.path.join(RUNS_DIR, "lesson"), {
            "n": None, "runtime": "none", "executor_id": "dispatcher-learning",
            "claim_id": "", "rc": 0, "posted": True, "switched": False,
            "lesson": body.get("id"), "tap_applied": True})
        log("  LESSON %s adopted as %s (scope %s)"
            % (tid, body.get("id"), body.get("scope")))
        return {"thread_id": tid, "ok": True, "posted": True, "lesson": body}
    # The store gate refused (no tap / no note / etc.): the honest repost.
    detail = clip(str(body.get("detail") or ""), 300)
    log("  LESSON %s adoption refused by the store gate: %s" % (tid, detail))
    _manifest_add(tid, os.path.join(RUNS_DIR, "lesson"), {
        "n": None, "runtime": "none", "executor_id": "dispatcher-learning",
        "claim_id": "", "rc": 0, "posted": False, "switched": False,
        "refused": detail})
    code2, _ = gw.post("/api/triage/threads/%s/result" % tid, {
        "outcome": "lesson-adoption-refused",
        "state": "needs-you",
        "note": ("His adoption tap could not be applied: %s. The four options "
                 "are re-presented; send the missing wording as a note with "
                 "the adopt-with-edit tap." % detail),
        "responder": "dispatcher-learning",
        "ask": {"kind": "choice",
                "prompt": ("The adoption tap was refused by the store gate. "
                           "Pick how to proceed with this lesson candidate."),
                "options": skeptic_free_lesson_options(),
                "requires": ["decision"]},
    })
    return {"thread_id": tid, "ok": False, "posted": code2 in (202, 409),
            "refused": detail}


def skeptic_free_lesson_options():
    """The four lesson options, locally (contract P3c §4). skeptic.escalation_ask
    derives DIFFERENT options for round-cap escalations; these are the lesson
    thread's own, identical to the store's _lesson_options (kept in one shape
    by the offline suite, which asserts the labels match)."""
    return [
        {"label": "Adopt it as a standing rule", "value": "adopt",
         "effect": "Stores this as an active lesson; it renders into every "
                   "executor prompt for its scope from now on.",
         "terminal": False, "resolved": False},
        {"label": "Adopt it with my wording edits", "value": "adopt-with-edit",
         "effect": "Stores this as an active lesson using the exact wording "
                   "you send in a note with this tap; the note is binding.",
         "terminal": False, "resolved": False},
        {"label": "Not the lesson - the real one differs",
         "value": "not-the-lesson",
         "effect": "Closes this candidate and adopts nothing; send the real "
                   "lesson in a note and it gets filed as a new candidate.",
         "terminal": True, "resolved": False},
        {"label": "No lesson - that was a one-off", "value": "one-off",
         "effect": "Closes this candidate and adopts nothing; the correction "
                   "applied to that thread only.",
         "terminal": True, "resolved": False},
    ]


# Set by main()/run_one so build_prompt's default lesson fetch has a client
# without threading one through every call site (tests inject None to force
# the fetch path or pass lessons explicitly).
_GW_FOR_RENDER = None


# ------------------------------------------------------------------ the tick

def touch_heartbeat(state="idle", **fields) -> None:
    os.makedirs(os.path.dirname(HEARTBEAT), exist_ok=True)
    blob = dict({"ts": time.time(), "state": state, "pid": os.getpid()}, **fields)
    write_json_atomic(HEARTBEAT, blob)
    # Publish to the gateway too, so the cross-host watchdog can
    # read heartbeat age without sharing the dispatcher host's failure domain
    # (contract P3 §6). Never fatal: a heartbeat publish failure must not
    # kill the tick that carries real work.
    try:
        Gateway().post("/api/triage/dispatcher-heartbeat", blob)
    except Exception:
        pass


def round_cap_hit(item: dict) -> tuple[bool, int]:
    rounds = bot_rounds(item.get("history"))
    if rounds < MAX_ROUNDS:
        return False, rounds
    if ROUND_CAP_OVERRIDE in (item.get("reply_note") or "").lower():
        return False, rounds
    return True, rounds


def hand_back_round_cap(gw: Gateway, item: dict, rounds: int) -> dict:
    """At the cap the artifact goes to a second-lane review (contract: P3b
    §3), never a bare hand-back. The reviewer is a real executor spawn at
    RUNTIME on the OTHER runtime (the stub plays it in tests); it claims
    before spawning (LESSON 26) and runs jailed like any run.

    Returns {"outcome": "ready"|"needs-joey"|"needs-work-bonus"|"fallback"|
    "duplicate", ...} so the caller and the manifest can record what the
    ladder did at the cap.
    """
    try:
        from . import skeptic
    except ImportError:   # direct script run
        from dispatcher import skeptic as skeptic
    tid = item["thread_id"]
    review_runtime = "hermes" if RUNTIME == "claude" else "claude"
    review_dir = os.path.join(RUNS_DIR, "%s-review" % tid[:48])
    os.makedirs(review_dir, exist_ok=True)

    # Four-test the escalation BEFORE spawning anything that pages (§3.2).
    # The ready/fallback paths post no ask that pages, so only the
    # needs-joey branch needs the check; but the duplicate check is cheap and
    # is exactly what stops a second card for the same concern.
    code, chk = gw.post("/api/triage/needs-joey-check", {
        "thread": {"thread_id": tid, "title": item.get("title"),
                   "state": "needs-you",
                   "ask": skeptic.escalation_ask(item.get("title") or "")},
        "context": {"ladder": {"retry_budget_left": 0, "self_fix_paths": []},
                    "ask_ts": item.get("reply_ts")}})
    dupe_of = None
    if code == 200 and not chk.get("needs_you") and chk.get("failed_test") == \
            "T3-he-doesnt-know":
        # A same-concern ask is already open: page nothing new. The scan in
        # the service named no thread, so find it locally for the pointer.
        dupes = None
        try:
            code2, th = gw.get("/api/triage/threads?include_done=1")
            fp_title = item.get("title") or ""
            # deterministic fingerprints come from the store; reuse it
            from helm import store as _store
            fp = _store.fingerprint(fp_title)
            dupes = [t["thread_id"] for t in th.get("threads", [])
                     if t["thread_id"] != tid
                     and _store.fingerprint(t.get("title") or "") == fp]
        except Exception:
            dupes = None
        if dupes:
            dupe_of = dupes[0]
    if dupe_of:
        body = skeptic.dupe_ack_result(dupe_of)
        gw.post("/api/triage/threads/%s/result" % tid, body)
        log("  ROUND-CAP %s review skipped: same concern already asked on %s"
            % (tid, dupe_of))
        return {"outcome": "duplicate", "thread_id": tid, "dupe_of": dupe_of}

    # claim BEFORE spawn (LESSON 26), under the reviewer identity
    cap_set = caps_mod.default_caps(review_runtime)
    code, body = gw.post("/api/triage/threads/%s/claims" % tid, {
        "executor_id": skeptic.REVIEWER_EXECUTOR_ID, "runtime": review_runtime,
        "caps": cap_set.enforced_by_map(review_runtime)})
    if code == 409:
        log("  ROUND-CAP %s already claimed; review deferred to a later tick"
            % tid)
        return {"outcome": "deferred", "thread_id": tid}
    if code != 201:
        # Never a bare card, never silence (§3.3): post the honest ack.
        gw.post("/api/triage/threads/%s/result" % tid,
                skeptic.fallback_result("claim POST %d" % code))
        return {"outcome": "fallback", "thread_id": tid, "claim_code": code}
    claim_id = body["claim_id"]

    def release(why):
        gw.post("/api/triage/threads/%s/claims/%s/release" % (tid, claim_id),
                {"note": why})

    prompt = skeptic.render_review_prompt(item, rounds,
                                          adapters.render_switches(review_runtime))
    with open(os.path.join(review_dir, "prompt.txt"), "w", encoding="utf-8") as fh:
        fh.write(prompt)
    spec = (adapters.spec_claude(review_dir, prompt, cap_set)
            if review_runtime == "claude"
            else adapters.spec_hermes(review_dir, prompt, cap_set))
    spec.runtime_out = "stdout.json" if review_runtime == "claude" else "stdout.txt"
    review_record = {"runtime": review_runtime, "claim_id": claim_id,
                     "rounds": rounds, "verdict": None}
    try:
        jail = jail_mod.Jail(review_dir, mode=None)
        proc = _spawn_wrapped(jail, spec, review_dir, cap_set, prompt)
        out, err = proc.communicate(timeout=cap_set.timeout_s + 60)
        rc = proc.returncode
    except PermissionError as e:
        gw.post("/api/triage/threads/%s/result" % tid,
                skeptic.fallback_result("jail refused the reviewer: %s" % e))
        release("jail refused reviewer spawn")
        return {"outcome": "fallback", "thread_id": tid, "jail_refused": True}
    except subprocess.TimeoutExpired:
        rc, out, err = -1, "", "review harness timeout"

    verdict, note, raw = skeptic.parse_verdict(review_dir)
    review_record.update({"rc": rc, "verdict": verdict, "note_len": len(note)})
    write_json_atomic(os.path.join(review_dir, "review_recorded.json"),
                      review_record)
    if verdict is None:
        release("review produced no usable verdict")
        gw.post("/api/triage/threads/%s/result" % tid,
                skeptic.fallback_result("review verdict unusable (rc=%d)" % rc))
        return {"outcome": "fallback", "thread_id": tid, "review": review_record}

    if verdict == "ready":
        code, _ = gw.post("/api/triage/threads/%s/claims/%s/result"
                          % (tid, claim_id), skeptic.ready_result(note))
        posted = code in (202, 409)
        log("  ROUND-CAP %s review verdict ready (POST %d); his tap settles"
            % (tid, code))
        return {"outcome": "ready", "thread_id": tid, "posted": posted,
                "review": review_record}

    if verdict == "needs-joey":
        # The escalation itself was four-test checked above (T3 branch); T2
        # can still have flipped between the check and here in pathological
        # interleavings, but the post_result gates below are the backstop.
        code, rbody = gw.post("/api/triage/threads/%s/claims/%s/result"
                              % (tid, claim_id),
                              skeptic.escalation_result(item, note))
        if code == 400:
            # refused by a store gate: fall back rather than lose the cap
            release("escalation refused: %s" % clip(str(rbody.get("detail")), 160))
            gw.post("/api/triage/threads/%s/result" % tid,
                    skeptic.fallback_result("escalation refused by a store gate"))
            return {"outcome": "fallback", "thread_id": tid,
                    "refused": clip(str(rbody.get("detail")), 200)}
        posted = code in (202, 409)
        log("  ROUND-CAP %s review verdict needs-joey; ask lit WITH options"
            % tid)
        return {"outcome": "needs-joey", "thread_id": tid, "posted": posted,
                "review": review_record}

    # needs-work: ONE bonus round, once ever (§3.3). The once-ever marker is
    # the bonus result's event prefix; if it is already in the thread's
    # history this verdict escalates instead of re-granting (the ladder is
    # exhausted, T4 holds).
    already = any("round-cap bonus (once per thread)" in (e.get("body") or "")
                  for e in (item.get("history") or []))
    if already:
        code, rbody = gw.post("/api/triage/threads/%s/claims/%s/result"
                              % (tid, claim_id),
                              skeptic.escalation_result(item, note))
        if code == 400:
            release("escalation refused: %s" % clip(str(rbody.get("detail")), 160))
            gw.post("/api/triage/threads/%s/result" % tid,
                    skeptic.fallback_result("escalation refused by a store gate"))
            return {"outcome": "fallback", "thread_id": tid,
                    "refused": clip(str(rbody.get("detail")), 200)}
        log("  ROUND-CAP %s second needs-work ESCALATES (bonus already granted)"
            % tid)
        return {"outcome": "needs-joey", "thread_id": tid,
                "posted": code in (202, 409), "second_needs_work": True,
                "review": review_record}
    release("review verdict needs-work; bonus round follows")
    bonus = _run_bonus_round(gw, item, rounds, note)
    return {"outcome": "needs-work-bonus", "thread_id": tid,
            "review": review_record, "bonus": bonus}


def _run_bonus_round(gw: Gateway, item: dict, rounds: int, review_note: str) -> dict:
    """The once-ever bonus round past the cap (contract: P3b §3.3). A normal
    claimed, capped, jailed run whose result prefix IS the once-ever marker;
    the next review that sees the marker escalates instead of re-granting."""
    try:
        from . import skeptic
    except ImportError:   # direct script run
        from dispatcher import skeptic as skeptic
    tid = item["thread_id"]
    run_dir = os.path.join(RUNS_DIR, "%s-bonus-%d" % (tid[:48], int(time.time())))
    cap_set = caps_mod.default_caps(RUNTIME)
    code, body = gw.post("/api/triage/threads/%s/claims" % tid, {
        "executor_id": EXECUTOR_ID + "-bonus", "runtime": RUNTIME,
        "caps": cap_set.enforced_by_map(RUNTIME)})
    if code != 201:
        gw.post("/api/triage/threads/%s/result" % tid,
                skeptic.fallback_result("bonus claim POST %d" % code))
        return {"outcome": "fallback", "claim_code": code}
    claim_id = body["claim_id"]
    gw.post("/api/triage/threads/%s/dispatched" % tid, {"mode": "start"})
    os.makedirs(run_dir, exist_ok=True)
    prompt = ("ROUND-CAP BONUS (once per thread): the second-lane review read "
              "every round and said one more bounded round could finish this.\n"
              "REVIEWER FINDINGS (carry these):\n"
              + (review_note or "").strip()[:2000] + "\n\n"
              + build_prompt(item, run_dir, rounds))
    with open(os.path.join(run_dir, "prompt.txt"), "w", encoding="utf-8") as fh:
        fh.write(prompt)
    spec = (adapters.spec_claude(run_dir, prompt, cap_set)
            if RUNTIME == "claude" else adapters.spec_hermes(run_dir, prompt, cap_set))
    spec.runtime_out = "stdout.json" if RUNTIME == "claude" else "stdout.txt"
    jail = jail_mod.Jail(run_dir)
    rec = {"runtime": RUNTIME, "claim_id": claim_id, "bonus": True}
    try:
        proc = _spawn_wrapped(jail, spec, run_dir, cap_set, prompt)
        out, err = proc.communicate(timeout=cap_set.timeout_s + 60)
        rc = proc.returncode
    except PermissionError as e:
        gw.post("/api/triage/threads/%s/claims/%s/release" % (tid, claim_id),
                {"note": "jail refused bonus spawn"})
        gw.post("/api/triage/threads/%s/result" % tid,
                skeptic.fallback_result("jail refused the bonus round: %s" % e))
        return {"outcome": "fallback", "jail_refused": True}
    except subprocess.TimeoutExpired:
        rc, out, err = -1, "", "bonus harness timeout"
    result = adapters.from_runtime(RUNTIME, run_dir, rc, rc == 124,
                                   0.0, cap_set, EXECUTOR_ID,
                                   os.path.basename(run_dir))
    body_out, mode = _executor_result_body(result, item)
    if mode != "invalid":
        payload = dict(body_out)
        payload["responder"] = EXECUTOR_ID
        payload["note"] = ("round-cap bonus (once per thread): "
                           + str(payload.get("note") or ""))[:8000]
        code, rbody = gw.post("/api/triage/threads/%s/claims/%s/result"
                              % (tid, claim_id), payload)
        rec.update({"rc": rc, "posted": code in (202, 409), "mode": mode})
        write_json_atomic(os.path.join(run_dir, "result_recorded.json"), rec)
        return {"outcome": "posted" if code in (202, 409) else "failed",
                "post_code": code}
    gw.post("/api/triage/threads/%s/claims/%s/release" % (tid, claim_id),
            {"note": "bonus round invalid"})
    gw.post("/api/triage/threads/%s/dispatched" % tid,
            {"reply_ts": item["reply_ts"], "ok": False, "mode": "reply",
             "detail": "bonus round produced no usable result"})
    rec.update({"rc": rc, "posted": False, "mode": mode})
    write_json_atomic(os.path.join(run_dir, "result_recorded.json"), rec)
    return {"outcome": "failed", "rc": rc}


def _spawn_wrapped(jail: "jail_mod.Jail", spec: "adapters.CommandSpec",
                   run_dir: str, cap_set, prompt: str):
    """Wrap the adapter argv with timeout + file redirections INSIDE the jail.

    The wall-clock cap must be INSIDE the jail: an outer timeout(1) kills the
    `systemd-run` client but NOT the detached transient service, so the
    executor would outlive its own cap. With timeout inside, rc 124 has one
    meaning on every mode (service, fallback), matching the spike.

    The service's stdout does not come back through systemd-run --wait (it
    goes to the journal), so the executor writes its artifacts to run_dir
    files itself via the wrapper's redirections; ReadWritePaths=run_dir is
    what makes them possible.
    """
    prompt_path = os.path.join(run_dir, "prompt.txt")
    stdin_redir = "< \"%s\"" % prompt_path if spec.stdin_text else "< /dev/null"
    wrapper = ('exec timeout --kill-after=10s %ds "$@" %s > "%s" 2> "%s"'
               % (int(cap_set.timeout_s), stdin_redir,
                  os.path.join(run_dir, spec.runtime_out),
                  os.path.join(run_dir, "stderr.txt")))
    argv = ["sh", "-c", wrapper, "_"] + list(spec.argv)
    env = dict(spec.env)
    env.update({"HELM_RUN_DIR": run_dir})
    return jail.spawn(argv, env=env, timeout_s=cap_set.timeout_s + 60)


def _item_has_questions(item: dict) -> bool:
    if not isinstance(item, dict):
        return False
    if item.get("reply_note") and "?" in str(item["reply_note"]):
        return True
    for e in item.get("history") or []:
        if isinstance(e, dict) and e.get("kind") == "joey-note" and "?" in str(e.get("body") or ""):
            return True
    return False


def _executor_result_body(result: "adapters.Result", item: dict) -> tuple[dict, str]:
    """Shape what gets POSTed from the executor's result.json + the Result.

    Returns (body, mode) where mode is 'executor' (result.json was usable),
    'dispatcher-fallback' (final text posted by the harness), or 'invalid'.
    """
    ans = adapters.answer_of(result)
    if isinstance(ans, dict) and ans.get("note") and len(str(ans["note"]).strip()) >= 80:
        body: dict = dict(ans)
        body.setdefault("outcome", "ok")
        body.setdefault("state", "needs-you")
        # P7 §2: the terminal no-action verdict rides through ONLY as a literal
        # boolean. A malformed flag (string "true", 1, ...) is stripped with a
        # loud log line and the result degrades to the normal needs-you+ack
        # hand-back — a bad flag must never wedge the thread in a 400 loop
        # (LESSON 24: fail toward the path that still has a human exit).
        na = body.get("no_action")
        if na is not None and not isinstance(na, bool):
            log("  NOTE-ONLY-FLAG malformed no_action=%r stripped; result "
                "degrades to the normal hand-back" % (na,))
            del body["no_action"]
            if str(body.get("state")) == "done":
                # A stripped settle verdict with state done would 400 the
                # bot-close rules and loop the thread forever; degrade to the
                # ack hand-back, which always has a human exit.
                body["state"] = "needs-you"
                if "ask" not in body:
                    body["ask"] = {"kind": "ack", "prompt": (
                        "The run judged no action was owed, but its settle "
                        "verdict was malformed so it could not close this "
                        "itself. Acknowledging settles the thread.")}
        # Answer-gate shaping (contract P5 §5, LESSON 16): if the note asked a question
        # and the executor omitted 'answers', shape it from the note report so the gate passes.
        if _item_has_questions(item) and len(str(body.get("answers") or "").strip()) < 12:
            note_str = str(body.get("note") or "").strip()
            if len(note_str) >= 12:
                body["answers"] = note_str
        return body, "executor"
    text = (result.final_text or "").strip()
    if len(text) >= 80 and (result.ok or result.capped):
        fb = {
            "outcome": "partial-capped" if result.capped else "reported-by-dispatcher",
            "state": "needs-you",
            "note": text[:8000],
            "ask": {"kind": "ack", "prompt": (
                "The run hit a cap before filing its own report; this is where it "
                "got to. Acknowledging settles this thread.") if result.capped else (
                "The run finished without filing its own report; this is its final "
                "message. Acknowledging settles this thread.")},
        }
        if _item_has_questions(item) and len(str(fb.get("answers") or "").strip()) < 12:
            fb["answers"] = text[:1000]
        return fb, "dispatcher-fallback"
    return {}, "invalid"


def _other_runtime(runtime: str) -> str:
    """The one-more-round switch target (contract: P3b §2). The adapter
    registry knows both; the pump stays executor-agnostic."""
    return "hermes" if runtime == "claude" else "claude"


def _manifest_path(tid: str) -> str:
    """The manifest is THREAD-scoped (contract: P3b §2.3): every attempt on a
    thread appends one entry — runtime, executor, claim, rc, posted, switched
    — so executor switches are visible on disk across the run dirs a
    re-answered thread legitimately produces (a fresh reply means a fresh
    run dir; the attempts are one story)."""
    return os.path.join(RUNS_DIR, "manifests", "%s.json" % tid[:80])


def _manifest_add(tid: str, run_dir: str, entry: dict) -> None:
    path = _manifest_path(tid)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        with open(path, encoding="utf-8") as fh:
            entries = json.load(fh)
        if not isinstance(entries, list):
            entries = []
    except (OSError, ValueError):
        entries = []
    entry = dict(entry, run_dir=os.path.basename(run_dir), ts=time.time())
    entries.append(entry)
    write_json_atomic(path, entries)


def run_one(gw: Gateway, item: dict, jail_mode: str | None = None,
            dry: bool = False, cap_override: dict | None = None) -> dict:
    """Claim → start → spawn → read → post exactly once. One thread.

    P3b §2: on the LAST regular attempt's failure, ONE more pass runs on the
    OTHER executor before gave_up is declared. Every attempt lands in the run
    dir's manifest.json.

    P3c §4: a pending item that is a lesson thread is NEVER spawned -- an
    executor has no network and cannot write the ledger. The tick applies his
    tap deterministically through the store's adoption gate.
    """
    global _GW_FOR_RENDER
    _GW_FOR_RENDER = gw
    tid = item["thread_id"]
    capped_rounds, rounds = round_cap_hit(item)
    run_dir = os.path.join(RUNS_DIR, "%s-%d" % (tid[:48], int(item["reply_ts"])))

    if dry:
        log("  DRY would %s %s (round %d/%d, run_dir %s)"
            % ("REVIEW (round cap)" if capped_rounds else "claim+spawn",
               tid, rounds + 1, MAX_ROUNDS, run_dir))
        return {"thread_id": tid, "dry": True}

    if capped_rounds:
        review = hand_back_round_cap(gw, item, rounds)
        return {"thread_id": tid, "round_cap": True, "review": review}

    # P3c §4: his adoption tap applied deterministically, never spawned.
    if _is_lesson_item(item):
        return _apply_lesson_tap(gw, item)

    out = _run_attempt(gw, item, run_dir, jail_mode, cap_override,
                       runtime=RUNTIME, switched=False)
    # P3b §2 one-more-round: a failed LAST attempt gets one pass on the OTHER
    # executor before gave_up. Auth failure switches too (a dead sign-in on
    # one runtime is what the other is for); jail refusal does not (the jail
    # is per-host: failing closed once is enough, contract §2.2).
    if (out.get("ok") is False and not out.get("posted")
            and out.get("gave_up_pending") and not out.get("jail_refused")):
        target = _other_runtime(RUNTIME)
        # P7 §1 (live-gate defect A, 2026-09-13): a switch to claude whose
        # auth cannot work burns a claim and ~a minute to die with "the CLI
        # could not authenticate" — 66 fails of exactly this shape in one day.
        # Check BEFORE claiming: unavailable -> loud, no claim, no spawn, and
        # the thread falls through to the normal gave_up -> restore path
        # (already loud, LESSON 9) instead of discovering it inside the jail.
        if target == "claude":
            ok_auth, why = claude_auth_available()
            if not ok_auth:
                log("  AUTH-SWITCH %s claude fallback cannot authenticate "
                    "(%s); no claim burned, no spawn" % (tid, why))
                _manifest_add(tid, run_dir, {
                    "n": None, "runtime": "claude", "executor_id":
                    EXECUTOR_ID + "-switch", "claim_id": "", "rc": None,
                    "posted": False, "switched": True,
                    "auth_switch_refused": clip(why, 200)})
                out["switch_refused"] = "claude-auth-unavailable"
                return out
        log("  SWITCH %s last attempt failed; ONE more round on %s"
            % (tid, target))
        switch_dir = os.path.join(run_dir, "switch")
        out2 = _run_attempt(gw, item, switch_dir, jail_mode, None,
                            runtime=target, switched=True)
        out2["switched"] = True
        return out2
    return out


def _run_attempt(gw: Gateway, item: dict, run_dir: str,
                 jail_mode: str | None, cap_override: dict | None,
                 runtime: str = RUNTIME, switched: bool = False) -> dict:
    """One claimed, jailed executor attempt (runtime as data, P3b §2)."""
    tid = item["thread_id"]
    # ---- claim BEFORE spawn (LESSON 26). 409 = someone else's live claim.
    cap_set = caps_mod.from_caps_payload(cap_override) if cap_override \
        else caps_mod.default_caps(runtime)
    executor_id = EXECUTOR_ID + ("-switch" if switched else "")
    code, body = gw.post("/api/triage/threads/%s/claims" % tid, {
        "executor_id": executor_id, "runtime": runtime,
        "caps": cap_set.enforced_by_map(runtime)})
    if code == 409:
        log("  SKIP %s already claimed by a live claim" % tid)
        return {"thread_id": tid, "skipped": "live-claim"}
    if code != 201:
        log("  CLAIM-FAIL %s POST /claims -> %d %s" % (tid, code, clip(body.get("detail"), 200)))
        return {"thread_id": tid, "claim_fail": code}
    claim_id = body["claim_id"]
    log("  CLAIM %s -> %s (BEFORE spawn)" % (tid, claim_id))
    lbl = fetch_label(gw, item.get("label")) if item.get("label") else None
    if lbl and lbl.get("webhook_url"):
        _send_webhook(lbl["webhook_url"], {"event": "claim", "thread_id": tid, "claim_id": claim_id, "item": item})

    def release(why):
        """Release the claim on any exit that posts no result, so the retry on
        a later tick claims anew instead of 409-ing against its own dead
        claim for the remaining lease. Idempotent server-side."""
        gw.post("/api/triage/threads/%s/claims/%s/release" % (tid, claim_id),
                {"note": why})

    # ---- attempt claimed before the executor exists (store: mode=start)
    code, sbody = gw.post("/api/triage/threads/%s/dispatched" % tid,
                          {"mode": "start"})
    attempts = sbody.get("attempts", "?")

    # ---- spawn under the jail
    _, rounds = round_cap_hit(item)   # the round this attempt reports as
    os.makedirs(run_dir, exist_ok=True)
    prompt = build_prompt(item, run_dir, rounds, label_data=lbl)
    with open(os.path.join(run_dir, "prompt.txt"), "w", encoding="utf-8") as fh:
        fh.write(prompt)
    spec = (adapters.spec_claude(run_dir, prompt, cap_set)
            if runtime == "claude" else adapters.spec_hermes(run_dir, prompt, cap_set))
    spec.runtime_out = "stdout.json" if runtime == "claude" else "stdout.txt"
    jail = jail_mod.Jail(run_dir, mode=jail_mode)
    t0 = time.time()
    try:
        proc = _spawn_wrapped(jail, spec, run_dir, cap_set, prompt)
        out, err = proc.communicate(timeout=cap_set.timeout_s + 60)
        rc = proc.returncode
    except PermissionError as e:
        log("  JAIL-REFUSED %s: %s" % (tid, e))
        release("jail degraded; spawn refused (fail closed)")
        gw.post("/api/triage/threads/%s/dispatched" % tid,
                {"reply_ts": item["reply_ts"], "ok": False, "mode": "reply",
                 "detail": "jail degraded; spawn refused (fail closed)"})
        return {"thread_id": tid, "jail_refused": True}
    except subprocess.TimeoutExpired:
        rc, out, err = -1, "", "outer harness timeout"
    duration = time.time() - t0
    timed_out = rc == 124

    result = adapters.from_runtime(runtime, run_dir, rc, timed_out, round(duration, 2),
                                   cap_set, executor_id, os.path.basename(run_dir))

    # auth failure is not a retryable failure: report once, leave queued
    auth_failed = (not result.ok
                   and is_auth_failure(result.final_text, result.stderr_tail))
    if auth_failed:
        log("  AUTH %s the CLI could not authenticate; answer stays queued, "
            "attempt %s not retried this tick" % (tid, attempts))
        release("auth failure; not retryable")
        gw.post("/api/triage/threads/%s/dispatched" % tid,
                {"reply_ts": item["reply_ts"], "ok": False, "mode": "reply",
                 "detail": "auth failure (not retryable; fix sign-in)"})
        _manifest_add(tid, run_dir, {"n": int(attempts) if str(attempts).isdigit() else None,
                                "runtime": runtime, "executor_id": executor_id,
                                "claim_id": claim_id, "rc": rc, "posted": False,
                                "switched": switched, "auth_failure": True})
        out = {"thread_id": tid, "auth_failure": True}
        # Auth failure is not a retryable failure (LESSON 9) but it IS a
        # switchable one (P3b §2.2): the other runtime's sign-in is exactly
        # what the switch is for. gave_up_pending signals run_one to switch.
        # P7: ok must be present and False — without it the run_one switch
        # guard (`out.get("ok") is False`) read None and the documented
        # auth-failure switch NEVER fired (found while wiring defect A).
        out["ok"] = False
        out["posted"] = False
        out["gave_up_pending"] = True
        return out

    # ---- post the result through the claim EXACTLY ONCE
    posted, post_code, mode = False, 0, ""
    body_out, mode = _executor_result_body(result, item)
    if mode != "invalid":
        payload = dict(body_out)
        payload.update({
            "responder": EXECUTOR_ID,
            "model": result.model, "models_billed": result.models_billed,
            "cost_usd": result.cost_usd, "cost_basis": result.cost_basis,
            "duration_s": result.duration_s, "num_turns": result.num_turns,
            "capped": result.capped, "timed_out": result.timed_out,
            "permission_denials": result.permission_denials,
            "run_id": result.run_id, "run_dir": result.run_dir,
        })
        for attempt in range(1 + RESULT_POST_RETRIES):
            post_code, rbody = gw.post(
                "/api/triage/threads/%s/claims/%s/result" % (tid, claim_id), payload)
            if post_code in (202,):
                posted = True
                break
            if post_code == 409:
                # already reported for this claim: exactly-once holds
                posted, post_code = True, 202
                break
            log("  POST-RETRY %s attempt %d -> %d %s"
                % (tid, attempt + 1, post_code, clip(str(rbody.get("detail")), 160)))
            time.sleep(1)
    else:
        post_code = 0

    write_json_atomic(os.path.join(run_dir, "result_recorded.json"), {
        "posted": posted, "post_code": post_code, "mode": mode,
        "ok": result.ok, "rc": result.rc, "timed_out": result.timed_out,
        "capped": result.capped, "cost_usd": result.cost_usd,
        "num_turns": result.num_turns, "duration_s": result.duration_s,
        "claim_id": claim_id, "attempts": attempts,
    })

    if posted:
        log("  OK %s rc=%d %.1fs turns=%s cost=%s capped=%s posted=%s attempts=%s"
            % (tid, rc, duration, result.num_turns, result.cost_usd,
               result.capped, mode, attempts))
        if mode == "executor" and body_out.get("no_action") is True:
            # P7 §2: a deliberate note-only settle is NOT a failure and never
            # reads as one in the log (LESSON 23: distinct words).
            log("  SETTLED-NOACTION %s the executor judged no action was owed; "
                "the thread settled done, no reclaim" % tid)
        if lbl and lbl.get("webhook_url"):
            _send_webhook(lbl["webhook_url"], {"event": "result", "thread_id": tid, "claim_id": claim_id, "result": payload})
        _manifest_add(tid, run_dir, {"n": int(attempts) if str(attempts).isdigit() else None,
                                "runtime": runtime, "executor_id": executor_id,
                                "claim_id": claim_id, "rc": rc, "posted": True,
                                "switched": switched})
        return {"thread_id": tid, "ok": True, "posted": True, "mode": mode}

    # failed run: no extra attempt charged (start claimed it); backoff applies
    detail = "rc=%d timed_out=%s posted=%d mode=%s %s" % (
        rc, timed_out, posted, mode, clip(result.stderr_tail or result.final_text, 240))
    release("failed run: " + detail[:120])
    gw.post("/api/triage/threads/%s/dispatched" % tid,
            {"reply_ts": item["reply_ts"], "ok": False, "mode": "reply",
             "detail": detail})
    log("  FAIL %s %s" % (tid, detail))
    out = {"thread_id": tid, "ok": False, "posted": False, "rc": rc}
    if not auth_failed:
        _manifest_add(tid, run_dir, {"n": int(attempts) if str(attempts).isdigit() else None,
                                "runtime": runtime, "executor_id": executor_id,
                                "claim_id": claim_id, "rc": rc, "posted": False,
                                "switched": switched, "detail": clip(detail, 200)})
        # P3b §2: gave_up_pending = this failure exhausted the regular budget
        # (mode=start already claimed this attempt, so attempts >= max).
        from helm.store import MAX_DISPATCH_ATTEMPTS as _MAX_ATT
        out["gave_up_pending"] = (int(attempts) >= _MAX_ATT
                                  if str(attempts).isdigit() else False)
    return out


# ------------------------------------------------------------------ main

def claude_auth_available() -> tuple[bool, str]:
    """Can a claude runtime spawn authenticate? Both paths checked offline,
    no model call (contract: P7 §1):

      1. a credential env var present in the tick's own env — that exact value
         rides the jail EnvironmentFile into the spawn (jail.PASSTHROUGH_ENV;
         the names are the ones the installed claude binary actually reads:
         ANTHROPIC_AUTH_TOKEN -> Bearer, ANTHROPIC_API_KEY -> API key);
      2. the CLI's own sign-in (`auth status` loggedIn) — covers OAuth material
         under $HOME, which ProtectHome=read-only lets the jail READ.

    Bearer-via-env is the durable jail-compatible path: the host's OAuth file
    has been dead since 2026-09-06 and a refresh could never be written back
    under the read-only home anyway.
    """
    for var in jail_mod.ANTHROPIC_AUTH_VARS:
        if os.environ.get(var):
            return True, "%s is set (rides the jail EnvironmentFile)" % var
    binary = os.environ.get("HELM_CLAUDE_BIN", "claude").split()[0]
    if not os.path.exists(binary) and "/" not in binary:
        return False, ("no anthropic credential env var (tried %s) and no "
                       "local claude binary to ask"
                       % ",".join(jail_mod.ANTHROPIC_AUTH_VARS))
    try:
        p = subprocess.run([binary, "auth", "status"], capture_output=True,
                           text=True, timeout=60,
                           stdin=subprocess.DEVNULL,   # see the note below
                           cwd=tempfile.gettempdir())
    except (OSError, subprocess.TimeoutExpired) as e:
        return False, "no credential env var and auth status unrunnable (%s)" % e
    try:
        d = json.loads(p.stdout)
    except ValueError:
        return False, "no credential env var and auth status not JSON"
    if d.get("loggedIn"):
        return True, "signed in via %s" % (d.get("authMethod") or "?")
    return False, ("no credential env var and the CLI is NOT signed in "
                   "(authMethod=%s)" % (d.get("authMethod") or "none"))


def auth_precheck() -> tuple[bool, str]:
    """Ask the CLI whether it can sign in, BEFORE claiming/spawning anything.
    Free (no model). hermes has no equivalent: always proceed."""
    if RUNTIME != "claude":
        return True, ""
    binary = os.environ.get("HELM_CLAUDE_BIN", "claude").split()[0]
    if not os.path.exists(binary) and "/" not in binary:
        return True, ""   # not obviously local; don't block on a guess
    import subprocess
    try:
        p = subprocess.run([binary, "auth", "status"], capture_output=True,
                           text=True, timeout=60,
                           stdin=subprocess.DEVNULL,   # never the caller's: a
                                                       # runtime that peeks at
                                                       # stdin blocks the whole
                                                       # 60s under any launcher
                                                       # that holds it open, and
                                                       # a stalled precheck reads
                                                       # as a hung tick
                           cwd=tempfile.gettempdir())  # a precheck must never
                                                       # write into the repo
    except (OSError, subprocess.TimeoutExpired):
        return True, "could not ask; proceeding"
    try:
        d = json.loads(p.stdout)
    except ValueError:
        return True, "auth status not JSON; proceeding"
    if d.get("loggedIn"):
        return True, "signed in via %s" % (d.get("authMethod") or "?")
    return False, ("claude CLI is NOT signed in (authMethod=%s); nothing was "
                   "spawned and the answer stays queued" % (d.get("authMethod") or "none"))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="tick")
    ap.add_argument("--dry", action="store_true",
                    help="write NOTHING: no heartbeat, no claims, no posts")
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--only", dest="only")
    args = ap.parse_args(argv)

    if args.selftest:
        return selftest()

    global _LOCK_FD
    _LOCK_FD = acquire_lock()
    if _LOCK_FD is None:
        log("another dispatcher holds the lock, exiting")
        return 0

    if args.dry:
        # --dry writes NOTHING (probe-verified in tests): reads only.
        gw = Gateway()
        code, q = gw.get("/api/triage/pending-dispatch")
        if code != 200:
            log("DRY helm unreachable (%d)" % code)
            return 1
        pend = q.get("pending", [])
        log("DRY pending=%d gave_up=%d" % (len(pend), q.get("gave_up", 0)))
        for item in pend[:MAX_PER_TICK]:
            run_one(gw, item, dry=True)
        return 0

    # 1. heartbeat, then everything else
    touch_heartbeat(state="starting")

    # 2. retention
    retention.sweep_and_log(RUNS_DIR)

    gw = Gateway()
    code, q = gw.get("/api/triage/pending-dispatch")
    if code != 200:
        touch_heartbeat(state="api-down", detail=str(code))
        log("helm unreachable (%d)" % code)
        return 1
    pend, gave_up = q.get("pending", []), q.get("gave_up", 0)
    if args.only:
        pend = [p for p in pend if p["thread_id"] == args.only]

    # 3. restore gave-up threads to Joey (the visible restore-to-ack path)
    if gave_up:
        code, rbody = gw.post("/api/triage/gave-up/restore", {})
        restored = (rbody if isinstance(rbody, list)
                    else (rbody.get("restored", []) if isinstance(rbody, dict) else [])) if code == 202 else []
        log("pending=%d gave_up=%d restored=%d" % (len(pend), gave_up, len(restored)))
        for r in restored:
            log("  RESTORED %s after %d attempt(s)" % (r["thread_id"], r["attempts"]))
        code, q = gw.get("/api/triage/pending-dispatch")
        pend = q.get("pending", [])
    else:
        log("pending=%d" % len(pend))

    if not pend:
        touch_heartbeat(state="idle", pending=0)
        return 0

    # 4. spending brake (distinct words, fail closed)
    b = brake_mod.Brake().check()
    if b["hold"]:
        touch_heartbeat(state="blocked", reason=b["reason"], detail=b["description"])
        tag = "USAGE-BLIND" if b["reason"] == brake_mod.REASON_BLIND else "USAGE-OVER"
        log("  %s %s; %d answer(s) stay queued" % (tag, b["description"], len(pend)))
        return 0

    # 5. auth pre-check — a dead sign-in must not burn five attempts
    if pend:
        ok, why = auth_precheck()
        if not ok:
            touch_heartbeat(state="blocked", reason="auth", detail=why)
            log("  AUTH %s" % why)
            return 0

    # 6-11. one thread per tick
    item = pend[0]
    out = run_one(gw, item)
    touch_heartbeat(state="idle", pending=max(0, len(pend) - 1), last=out)
    return 0 if out.get("ok") or out.get("posted") or out.get("round_cap") else 1


# ------------------------------------------------------------------ selftest

def selftest() -> int:
    """--selftest: brake reasons, adapter parse of a recorded run, jail
    refusal. Offline; no gateway, no executor, no billing."""
    import http.server
    import threading

    fails = []

    class FakeVM(http.server.BaseHTTPRequestHandler):
        mode = "clear"

        def do_GET(self):  # noqa: N802
            import urllib.parse
            q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query).get("query", [""])[0]
            if FakeVM.mode == "down":
                self.send_error(503)
                return
            now = time.time()
            if "max(timestamp" in q:
                body = {"status": "success", "data": {"resultType": "vector", "result": [
                    {"metric": {}, "value": [now, str(now if FakeVM.mode != "stale"
                                                                else now - 7200)]}]}}
            else:
                burn = 999 if FakeVM.mode != "over" else 10 ** 12
                body = {"status": "success", "data": {"resultType": "vector", "result": [
                    {"metric": {}, "value": [now, str(burn)]}]}}
            blob = json.dumps(body).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(blob)))
            self.end_headers()
            self.wfile.write(blob)

        def log_message(self, format, *args):  # noqa: A002
            pass

    srv = http.server.HTTPServer(("127.0.0.1", 0), FakeVM)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    url = "http://127.0.0.1:%d" % srv.server_address[1]

    def brake_check(mode):
        FakeVM.mode = mode
        return brake_mod.Brake(vm_url=url, hold_tokens=1000,
                               stale_s=1800).check()

    c = brake_check("clear")
    if c["reason"] is not None:
        fails.append("brake clear: %s" % c)
    o = brake_check("over")
    if o["reason"] != "usage-over":
        fails.append("brake over: %s" % o)
    bl = brake_check("stale")
    if bl["reason"] != "usage-blind":
        fails.append("brake blind(stale): %s" % bl)
    FakeVM.mode = "down"
    db_ = brake_mod.Brake(vm_url=url, exporter_log="/nonexistent/x.log").check()
    if db_["reason"] != "usage-blind":
        fails.append("brake blind(down): %s" % db_)
    if o["reason"] == bl["reason"]:
        fails.append("brake words not distinct")
    srv.shutdown()
    print("selftest brake: %s" % ("ok" if not fails else fails))

    # 2. adapter parse of recorded runs (real captured artifacts, zero calls)
    cap = caps_mod.CapSet(max_budget_usd=1.50)
    r1 = adapters.from_claude(os.path.join(SELFTEST_RUNS_DIR, "claude-1"),
                              0, False, 7.1, cap, "selftest", "claude-1")
    if not (r1.ok and r1.num_turns == 3 and r1.cost_usd
            and r1.cost_basis == "reported" and "claude-sonnet" in (r1.model or "")):
        fails.append("claude parse: %s" % r1.to_payload())
    esc = adapters.from_claude(os.path.join(SELFTEST_RUNS_DIR, "claude-violation"),
                               0, False, 10.0, cap, "selftest", "esc")
    if esc.ok and not esc.permission_denials:
        pass  # the recorded escape: rc 0, no denials — invisibility is the finding
    r2 = adapters.from_hermes(os.path.join(SELFTEST_RUNS_DIR, "hermes-4-in-flag"),
                             0, False, 13.9, cap, "selftest", "h4")
    if r2.cost_usd is not None or r2.cost_basis != "unknown/none":
        fails.append("hermes cost must stay unpriced-null, got %s/%s"
                     % (r2.cost_usd, r2.cost_basis))
    if r2.num_turns != 2:
        fails.append("hermes api_calls read: %s" % r2.num_turns)
    print("selftest adapters: %s" % ("ok" if not fails else fails))

    # 3. jail: escape probe must be REFUSED in service mode; degraded must be
    #    loud and refuse untrusted spawns
    j = jail_mod.Jail(os.path.join(RUNS_DIR, "selftest-jail"))
    if j.mode == "service":
        res = j.selfcheck()
        if not res["ok"]:
            fails.append("jail escape NOT refused: %s" % res)
        print("selftest jail(service): %s" % ("ok" if res["ok"] else res))
    else:
        try:
            j.spawn(["true"], env={})
            fails.append("degraded jail spawned without opt-in")
        except PermissionError:
            print("selftest jail(degraded): refused untrusted spawn (loud)")

    if fails:
        print("SELFTEST FAILURES: %d" % len(fails))
        for f in fails:
            print("  - %s" % f)
        return 1
    print("selftest ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
