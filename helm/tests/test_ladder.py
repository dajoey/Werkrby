"""P3b ladder suite (contract: docs/contracts/P3b.md).

Real app + real store + real jail + loopback gateway + stub executors for
BOTH runtimes. NO real model call anywhere: the stub plays the executor and
the reviewer. Covers the work order's probe list:

  - the four-test: one probe per failing test (T1..T4), plus positive
    controls (licensed resync, ack exemption) so the gate is proven not to
    over-fire
  - retry/switch: the LAST attempt before gave_up runs the OTHER executor
    (manifest evidence); both-fail still reaches gave_up + restore
  - round-cap skeptic: verdict ready settles toward his tap; verdict
    needs-joey lights needs-you WITH options; needs-work grants the
    once-ever bonus (and a second needs-work escalates); invalid reviews
    fall back to the honest ack
  - bare asks: the banned phrase, the statement-not-ask, the R3 rewrite
  - the banned-phrase sweep over every stored ask in the probe DB
"""
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

TMP = tempfile.mkdtemp(prefix="helm-ladder-")
os.environ["HELM_APP_TOKEN"] = "test-token"
os.environ["HELM_DB"] = os.path.join(TMP, "t.db")
os.environ["HELM_ATTACH_DIR"] = os.path.join(TMP, "att")
os.environ["HELM_DISPATCHER_STATE"] = os.path.join(TMP, "state")
# Run dirs under $HOME (PrivateTmp makes /tmp invisible inside the jail —
# measured in the P3 suite; same reason applies here).
_HOME_TMP = os.path.join(os.path.expanduser("~"), ".helm-p3b-test-runs")
shutil.rmtree(_HOME_TMP, ignore_errors=True)
os.makedirs(_HOME_TMP, exist_ok=True)
os.environ["HELM_RUNS_DIR"] = os.path.join(_HOME_TMP, "runs")
os.environ["HELM_API"] = "http://127.0.0.1:3039"
os.environ["HELM_RUNTIME"] = "claude"
os.environ["HELM_JAIL_EXEC_UID"] = str(os.getuid())
os.environ["HELM_JAIL_EXEC_GID"] = str(os.getgid())
os.environ["HELM_STUB_CAP_BUDGET"] = "1.50"
os.environ["HELM_VM_URL"] = "http://127.0.0.1:1"   # replaced by the fake VM

STUB = os.path.join(HERE, "stub_executor.py")
os.environ["HELM_CLAUDE_BIN"] = STUB
os.environ["HELM_HERMES_BIN"] = STUB   # the review + switch ride "hermes"

from dispatcher import tick                                             # noqa: E402
from helm import store                                                  # noqa: E402

PASSED = FAILED = 0


def check(name, cond, detail=""):
    global PASSED, FAILED
    if cond:
        PASSED += 1
    else:
        FAILED += 1
        print("  FAIL %s %s" % (name, str(detail)[:400]))


# ---------------------------------------------------------------- fake VM
import http.server


class FakeVM(http.server.BaseHTTPRequestHandler):
    mode = "clear"
    burn = 999

    def do_GET(self):
        import urllib.parse
        q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query).get("query", [""])[0]
        now = time.time()
        val = now if "max(timestamp" in q else FakeVM.burn
        body = {"status": "success", "data": {"resultType": "vector", "result": [
            {"metric": {}, "value": [now, str(val)]}]}}
        blob = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(blob)))
        self.end_headers()
        self.wfile.write(blob)

    def log_message(self, format, *args):
        pass


_vm_srv = http.server.HTTPServer(("127.0.0.1", 0), FakeVM)
threading.Thread(target=_vm_srv.serve_forever, daemon=True).start()
os.environ["HELM_VM_URL"] = "http://127.0.0.1:%d" % _vm_srv.server_address[1]


# ---------------------------------------------------------------- gateway
import uvicorn                                                          # noqa: E402
import app.service as svc                                               # noqa: E402

_config = uvicorn.Config(svc.app, host="127.0.0.1", port=3039, log_level="error")
_server = uvicorn.Server(_config)
threading.Thread(target=_server.run, daemon=True).start()
for _ in range(100):
    try:
        req = urllib.request.Request("http://127.0.0.1:3039/api/meta")
        with urllib.request.urlopen(req, timeout=2):
            break
    except OSError:
        time.sleep(0.1)


def api(method, path, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request("http://127.0.0.1:3039" + path, data=data,
                                 method=method,
                                 headers={"Content-Type": "application/json",
                                          "Authorization": "Bearer test-token"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode("utf-8", "replace")
            return r.status, (json.loads(body) if body.strip() else {})
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode("utf-8", "replace") or "{}")


def put_thread(tid, title, ask_kind="choice", prompt="Pick one", options=None,
               requires=None):
    t = {"thread_id": tid, "title": title, "summary": "s " * 10,
         "category": "Test", "urgency": "this-week", "state": "needs-you",
         "sources": [{"source": "test", "id": tid, "note": ""}]}
    if ask_kind:
        ask = {"kind": ask_kind, "prompt": prompt}
        # P3b §1.5: helper-created choice/text asks carry the T1 marker by
        # default (explicit requires=[] keeps the markerless probe honest).
        ask["requires"] = ["decision"] if requires is None else requires
        if ask_kind == "choice":
            ask["options"] = options or [
                {"label": "Do the thing fully", "value": "do", "effect": "It gets done"},
                {"label": "Skip it for now", "value": "no", "effect": "Nothing happens"}]
        t["ask"] = ask
    return api("PUT", "/api/triage/threads", {"threads": [t]})


def thread_state(tid):
    code, th = api("GET", "/api/triage/threads?include_done=1")
    return next((t for t in th["threads"] if t["thread_id"] == tid), None)


def run_tick(stub_mode, extra=None, args=()):
    env = dict(os.environ)
    env["HELM_STUB_MODE"] = stub_mode
    env.update(extra or {})
    cmd = [sys.executable, os.path.join(ROOT, "dispatcher", "tick.py"), *args]
    return subprocess.run(cmd, env=env, capture_output=True, text=True,
                          cwd=ROOT, timeout=300)


def bot_rounds(tid):
    t = thread_state(tid)
    return tick.bot_rounds(t["events"]) if t else 0


LONG = "x" * 120

# ============================================================ 1. four-test
print("section 1: the needs-you four-test (contract P3b §1)")

# T1: no markers -> refused at the producer's door, named. The door's
# contract is per-thread rejection in rejected[] (P1 partial success); a
# single-thread payload of one rejected thread reads 400 at the API.
def rejected_of(res):
    return " | ".join(" ".join(r["errors"]) for r in res.get("rejected", []))


# T1: no markers -> refused at the producer's door, named. Both shapes:
# requires: [] (stated, empty) and no requires key at all.
code, res = put_thread("t4-one", "Relay certificate renewal", requires=[])
code2, res2 = api("PUT", "/api/triage/threads", {"threads": [{
    "thread_id": "t4-one-b", "title": "Relay certificate renewal unstated",
    "summary": "s " * 10, "category": "Test", "urgency": "this-week",
    "state": "needs-you",
    "sources": [{"source": "test", "id": "t4-one-b", "note": ""}],
    "ask": {"kind": "choice", "prompt": "Pick one", "options": [
        {"label": "Do the thing fully", "value": "do", "effect": "It gets done"},
        {"label": "Skip it for now", "value": "no", "effect": "Nothing happens"}]}}]})
errs = rejected_of(res) + rejected_of(res2)
check("T1 probe: choice ask with no markers is refused at the door",
      code == 400 and code2 == 400 and "T1-only-he-can" in errs,
      (code, code2, errs[:200]))
check("T1 probe: the refusal names the fix", "requires" in errs, errs[:200])
check("    and the thread never landed",
      thread_state("t4-one") is None and thread_state("t4-one-b") is None)

# T1 positive: markers land
code, res = put_thread("t4-ok", "Relay certificate renewal decision",
                       requires=["decision"])
check("T1 control: markers land", code == 200 and res.get("created") == 1,
      (code, res))

# T1 unknown marker -> validation 400 naming the set
code, res = put_thread("t4-bad", "Unknown marker probe", requires=["whim"])
check("T1 probe: unknown marker is a loud 400 naming the set",
      code == 400 and "decision" in rejected_of(res), (code, res))

# T1 exemption: ack asks carry no markers and land
code, res = put_thread("t4-ack", "An ack card for information only",
                       ask_kind="ack", prompt="Read this and close it.")
check("T1 control: producer ack asks are exempt (land without markers)",
      code == 200, (code, res))

# T2: an owner event newer than the ask's generation invalidates it
ok, failed = store.needs_joey(
    {"thread_id": "x", "title": "t", "ask": {"kind": "choice",
                                             "requires": ["decision"]}},
    {"ask_ts": 100.0, "owner_event_ts": 200.0, "duplicates": []})
check("T2 probe: newer owner event fails still-true-now",
      ok is False and failed == "T2-still-true-now", (ok, failed))
ok, failed = store.needs_joey(
    {"thread_id": "x", "title": "t", "ask": {"kind": "choice",
                                             "requires": ["decision"]}},
    {"ask_ts": 200.0, "owner_event_ts": 100.0, "duplicates": []})
check("T2 control: ask newer than his last event passes T2", ok is True)

# T3: duplicate open ask / recently-settled / ops-log hit
ok, failed = store.needs_joey(
    {"thread_id": "new", "title": "Disk on host-a at 11 percent free",
     "ask": {"kind": "choice", "requires": ["decision"]}},
    {"ask_ts": 100.0, "owner_event_ts": 0, "duplicates": [
        {"thread_id": "old", "state": "needs-you", "ask_kind": "choice",
         "last_change": 50}]})
check("T3 probe: open duplicate ask fails he-doesnt-know",
      ok is False and failed == "T3-he-doesnt-know", (ok, failed))
ok, failed = store.needs_joey(
    {"thread_id": "new", "title": "Disk on host-a at 11 percent free",
     "ask": {"kind": "choice", "requires": ["decision"]}},
    {"ask_ts": 100.0, "owner_event_ts": 0, "duplicates": [
        {"thread_id": "old", "state": "needs-you", "ask_kind": "choice",
         "title": "Disk on host-a at 7 percent free", "last_change": 50}]})
check("T3 probe: fingerprint dedupe catches the retitled duplicate (numbers stripped)",
      ok is False and failed == "T3-he-doesnt-know", (ok, failed))
ok, failed = store.needs_joey(
    {"thread_id": "new", "title": "t", "ask": {"kind": "choice",
                                               "requires": ["decision"]}},
    {"ask_ts": 100.0, "owner_event_ts": 0, "duplicates": [],
     "ops_log_hits": ["Ops Log 2026-09-10: known"]})
check("T3 probe: an ops-log hit fails he-doesnt-know",
      ok is False and failed == "T3-he-doesnt-know", (ok, failed))
ok, failed = store.needs_joey(
    {"thread_id": "new", "title": "t", "ask": {"kind": "choice",
                                               "requires": ["decision"]}},
    {"ask_ts": 100.0, "owner_event_ts": 0, "duplicates": [
        {"thread_id": "old", "state": "working", "ask_kind": "none",
         "last_change": 50}]})
check("T3 control: a working non-ask thread is not a duplicate",
      ok is True, (ok, failed))

# T3 at the producer's door: a second card for the same concern
code, res = put_thread("t4-dupe", "Relay certificate renewal decision",
                       requires=["decision"])
check("T3 door probe: duplicate title refused at the producer's door",
      code == 400 and "T3-he-doesnt-know" in rejected_of(res), (code, res))
check("    and the refusal names the existing concern's route",
      "stable id" in rejected_of(res), rejected_of(res)[:200])

# T4: ladder context with budget left / self-fix paths
ok, failed = store.needs_joey(
    {"thread_id": "x", "title": "t", "ask": {"kind": "choice",
                                             "requires": ["decision"]}},
    {"ask_ts": 100.0, "owner_event_ts": 0, "duplicates": [],
     "ladder": {"retry_budget_left": 1, "self_fix_paths": []}})
check("T4 probe: retry budget left fails agent-cannot-fix",
      ok is False and failed == "T4-agent-cannot-fix", (ok, failed))
ok, failed = store.needs_joey(
    {"thread_id": "x", "title": "t", "ask": {"kind": "choice",
                                             "requires": ["decision"]}},
    {"ask_ts": 100.0, "owner_event_ts": 0, "duplicates": [],
     "ladder": {"retry_budget_left": 0, "self_fix_paths": ["other-executor"]}})
check("T4 probe: a self-fix path fails agent-cannot-fix",
      ok is False and failed == "T4-agent-cannot-fix", (ok, failed))
ok, failed = store.needs_joey(
    {"thread_id": "x", "title": "t", "ask": {"kind": "choice",
                                             "requires": ["decision"]}},
    {"ask_ts": 100.0, "owner_event_ts": 0, "duplicates": [],
     "ladder": {"retry_budget_left": 0, "self_fix_paths": []}})
check("T4 control: exhausted ladder with no self-fix passes", ok is True)

# licensed resync: the SAME thread id re-PUT with its ask lands (invariant 3
# still protects any answer); a NEW ask on an ask-less live thread is gated
code, res = put_thread("t4-ok", "Relay certificate renewal decision",
                       requires=["decision"])
check("licensed resync: same id + same concern lands, no re-interrogation",
      code == 200 and res.get("updated") == 1, (code, res))

# the self-check route (§1.4): the fix as code, callable by any producer
code, res = api("POST", "/api/triage/needs-joey-check", {"thread": {
    "thread_id": "t4-route", "title": "A route check concern",
    "state": "needs-you",
    "ask": {"kind": "choice", "prompt": "Which way?",
            "requires": ["decision"],
            "options": [{"label": "Do the thing", "value": "do",
                         "effect": "it happens"}]}}})
check("self-check route: pass is data, not a page",
      code == 200 and res["needs_you"] is True, (code, res))
code, res = api("POST", "/api/triage/needs-joey-check", {"thread": {
    "thread_id": "t4-route-nope", "title": "Another route check concern",
    "state": "needs-you",
    "ask": {"kind": "choice", "prompt": "Which way?",
            "options": [{"label": "Do the thing", "value": "do",
                         "effect": "it happens"}]}}})
check("self-check route: T1 failure comes back with the failed test",
      code == 200 and res["needs_you"] is False
      and res["failed_test"] == "T1-only-he-can", (code, res))

# ============================================================ 2. one-more-round
print("section 2: retries/backoff/one-more-round (contract P3b §2)")

put_thread("t-switch-1", "Switch on the last attempt")
api("POST", "/api/triage/threads/t-switch-1/reply", {"value": "do"})


def fast_forward_backoff(tid):
    con = sqlite3.connect(os.environ["HELM_DB"])
    con.execute("UPDATE threads SET last_dispatch_try = last_dispatch_try - 3600"
                " WHERE thread_id=?", (tid,))
    con.execute("UPDATE events SET ts = ts - 3600 WHERE thread_id=?"
                " AND kind='joey-reply'", (tid,))
    con.commit()
    con.close()


# four failures on the primary runtime, backoff time-travelled
for i in range(4):
    p = run_tick("fail")
    fast_forward_backoff("t-switch-1")

code, q = api("GET", "/api/triage/pending-dispatch")
me = next((x for x in q["pending"] if x["thread_id"] == "t-switch-1"), None)
check("four failures recorded (attempts=4, still offered)",
      me is not None and me["attempts"] == 4, (me and me["attempts"]))

# 5th attempt fails on claude -> SWITCH -> hermes stub SUCCEEDS.
# HELM_STUB_MODE fail (claude, stdin) + HELM_STUB_MODE_HERMES ok (-z, argv).
p = run_tick("fail", extra={"HELM_STUB_MODE_HERMES": "ok"})
check("the switching tick ran", p.returncode in (0, 1),
      p.stdout[-500:] + p.stderr[-300:])
mpath = os.path.join(os.environ["HELM_RUNS_DIR"], "manifests",
                     "t-switch-1.json")
entries = json.load(open(mpath)) if os.path.exists(mpath) else []
runtimes = [e.get("runtime") for e in entries]
check("thread manifest records every attempt (>=5)",
      len(entries) >= 5, entries)
check("manifest shows the executor switch (claude then hermes)",
      "claude" in runtimes and "hermes" in runtimes
      and entries[-1].get("switched") is True
      and entries[-1].get("posted") is True, entries)
t = thread_state("t-switch-1")
check("switched attempt landed the result (no gave_up)",
      t is not None and t["state"] == "needs-you"
      and bot_rounds("t-switch-1") >= 1,
      (t and t["state"], p.stdout[-600:]))

# both runtimes fail -> gave_up is still loud, restore still works
put_thread("t-gaveup-1", "Both runtimes fail loudly")
api("POST", "/api/triage/threads/t-gaveup-1/reply", {"value": "do"})
for i in range(5):
    run_tick("fail", extra={"HELM_STUB_MODE_HERMES": "fail"})
    fast_forward_backoff("t-gaveup-1")
code, q = api("GET", "/api/triage/pending-dispatch")
gave = q.get("gave_up", 0)
pend_ids = [x["thread_id"] for x in q.get("pending", [])]
check("both-fail: gave_up visible and thread not offered",
      gave >= 1 and "t-gaveup-1" not in pend_ids, (gave, pend_ids))
gmp = os.path.join(os.environ["HELM_RUNS_DIR"], "manifests", "t-gaveup-1.json")
gm = json.load(open(gmp)) if os.path.exists(gmp) else []
check("both-fail: manifest shows claude AND hermes attempts before gave_up",
      len(gm) >= 6 and {"claude", "hermes"} <= {e.get("runtime") for e in gm},
      gm)
# restore path still works
run_tick("ok")
t = thread_state("t-gaveup-1")
check("both-fail: restore-to-ack still fires after the switch",
      t and t["state"] == "needs-you", t and t["state"])

# ============================================================ 3. round-cap review
print("section 3: round-cap -> skeptic-lite review (contract P3b §3)")


def drive_to_cap(tid, title):
    put_thread(tid, title)
    for i in range(4):
        api("POST", "/api/triage/threads/%s/reply" % tid, {"value": "do"})
        run_tick("ok", args=("--only", tid))
    api("POST", "/api/triage/threads/%s/reply" % tid, {"value": "do"})
    return bot_rounds(tid)


# 3a. verdict ready: the reviewed result posts; ONLY his tap settles
n = drive_to_cap("t-rev-ready", "Review verdict ready thread")
check("4 rounds ran before the review", n == 4, n)
p = run_tick("review-ready", args=("--only", "t-rev-ready"))
t = thread_state("t-rev-ready")
check("verdict ready: posted as needs-you ack (his tap settles, no bot close)",
      t["state"] == "needs-you" and t["ask"]["kind"] == "ack", t and t["state"])
evs = [e for e in t["events"] if e["kind"] == "bot-result"]
check("verdict ready: the review result is on the thread",
      any("skeptic" in (e.get("body") or "").lower() for e in evs), len(evs))
check("verdict ready: review did NOT count as a round",
      bot_rounds("t-rev-ready") == 4, bot_rounds("t-rev-ready"))
rev = os.path.join(os.environ["HELM_RUNS_DIR"], "t-rev-ready-review")
rec = json.load(open(os.path.join(rev, "review_recorded.json")))
check("verdict ready: recorded on the other runtime, claimed first",
      rec["verdict"] == "ready" and rec["runtime"] == "hermes"
      and rec["claim_id"].startswith("c-"), rec)

# 3b. verdict needs-joey: needs-you WITH options + the T1 marker
n = drive_to_cap("t-rev-nj", "Review verdict needs-joey thread")
p = run_tick("review-needs-joey", args=("--only", "t-rev-nj"))
t = thread_state("t-rev-nj")
check("verdict needs-joey: lit as a CHOICE with >=2 options",
      t["state"] == "needs-you" and t["ask"]["kind"] == "choice"
      and len(t["ask"]["options"]) >= 2,
      (t and t["state"], t and t["ask"]["kind"]))
labels = " | ".join(o["label"] for o in t["ask"]["options"])
check("verdict needs-joey: options are actions, self-contained",
      "Run one more executor round on it" in labels
      and "what do you want" not in labels.lower(), labels)
check("verdict needs-joey: no banned phrase anywhere on the ask",
      store.BANNED_ASK_PHRASE not in (t["ask"]["prompt"] + labels).lower())

# 3c. verdict needs-work: ONE bonus round, once ever
n = drive_to_cap("t-rev-nw", "Review verdict needs-work thread")
p = run_tick("review-needs-work", args=("--only", "t-rev-nw"))
t = thread_state("t-rev-nw")
check("verdict needs-work: bonus round ran (5th run round)",
      bot_rounds("t-rev-nw") == 5, bot_rounds("t-rev-nw"))
bodies = [e.get("body") or "" for e in t["events"] if e["kind"] == "bot-result"]
check("verdict needs-work: the once-ever marker is in the event log",
      any("round-cap bonus (once per thread)" in b.lower() for b in bodies),
      bodies[-1][:140])
# a SECOND needs-work at the cap escalates (once, ever). His fresh reply
# re-queues the thread; the cap fires again; the marker in history turns the
# second needs-work into an escalation.
api("POST", "/api/triage/threads/t-rev-nw/reply", {"value": "do"})
p = run_tick("review-needs-work", args=("--only", "t-rev-nw"))
n_before = bot_rounds("t-rev-nw")
t = thread_state("t-rev-nw")
check("second needs-work escalates (no second bonus)",
      n_before == 5 and t["state"] == "needs-you"
      and t["ask"]["kind"] in ("choice", "ack"),
      (n_before, t and t["state"], t and t["ask"]["kind"], p.stdout[-300:]))
check("second needs-work did not spawn a third bonus round",
      bot_rounds("t-rev-nw") == 5, bot_rounds("t-rev-nw"))

# 3d. invalid review: fallback ack, never bare, never silence
for mode, why in (("review-empty", "no artifact"),
                  ("review-garbage", "unparseable"),
                  ("review-invalid", "unknown verdict")):
    tid = "t-rev-bad-%s" % mode.split("-")[1]
    drive_to_cap(tid, "Review broken %s thread" % mode)
    p = run_tick(mode, args=("--only", tid))
    t = thread_state(tid)
    check("broken review (%s): honest ack fallback, not a bare card" % why,
          t["state"] == "needs-you" and t["ask"]["kind"] == "ack"
          and "review could not" in t["ask"]["prompt"],
          (t and t["state"], t and t["ask"]["prompt"][:60]))

# ============================================================ 4. bare asks
print("section 4: bare cards never light 'what do you want' (contract P3b §4)")

put_thread("t-bare-1", "Bare ask target", ask_kind="ack",
           prompt="Read this and close it.")
api("POST", "/api/triage/threads/t-bare-1/reply", {"value": "ack"})

# R1 at the bot's door
code, res = api("POST", "/api/triage/threads/t-bare-1/result", {
    "outcome": "asked", "state": "needs-you", "note": LONG,
    "ask": {"kind": "ack", "prompt": "So, what do you want to do about this?"}})
check("R1 bot door: the banned phrase is a 400",
      code == 400 and "banned" in json.dumps(res).lower(), (code, res))
# R1 at the producer's door
code, res = put_thread("t-bare-2", "Producer banned phrase",
                       prompt="What do you want done here?")
check("R1 producer door: the banned phrase is a 400",
      code == 400 and "banned" in json.dumps(res).lower(), (code, res))
# R1 in an option effect
code, res = put_thread("t-bare-3", "Banned phrase in effect", options=[
    {"label": "Do the thing", "value": "do", "effect": "does what do you want"},
    {"label": "Leave it", "value": "no", "effect": "nothing"}])
check("R1 option effect: the banned phrase is a 400", code == 400, (code, res))

# R2 at the bot's door: a text ask with no '?' is a statement
code, res = api("POST", "/api/triage/threads/t-bare-1/result", {
    "outcome": "asked", "state": "needs-you", "note": LONG,
    "ask": {"kind": "text", "prompt": "The vendor replied with a new quote today."}})
check("R2 bot door: a text statement is refused with the ack fix",
      code == 400 and "ack" in json.dumps(res), (code, res))
# R2 at the producer's door
code, res = put_thread("t-bare-4", "Producer statement ask", ask_kind="text",
                       prompt="The migration finished overnight.")
check("R2 producer door: a text statement is refused", code == 400, (code, res))

# R3: the pure direction-demand is REWRITTEN at the bot's door
code, res = api("POST", "/api/triage/threads/t-bare-1/result", {
    "outcome": "blocked-external", "state": "needs-you", "note": LONG,
    "ask": {"kind": "text", "prompt": "How should we proceed?"}})
t = thread_state("t-bare-1")
check("R3 bot door: the demand is rewritten to the ack summary (202)",
      code == 202 and res.get("rewritten_ask") is True
      and t["ask"]["kind"] == "ack"
      and t["ask"]["prompt"].startswith("Work stopped at"),
      (code, res, t and t["ask"]["prompt"][:60]))
check("R3: the rewritten card says reply-anything reopens",
      "Reply anything to reopen" in t["ask"]["prompt"], t["ask"]["prompt"])
check("R3: the rewritten card carries no banned phrase",
      store.BANNED_ASK_PHRASE not in t["ask"]["prompt"].lower())
# R3 at the producer's door: refused with the fix (producers can correct)
code, res = put_thread("t-bare-5", "Producer direction demand", ask_kind="text",
                       prompt="What now?")
check("R3 producer door: the demand is refused with the fix",
      code == 400 and "demand for direction" in json.dumps(res), (code, res))
# a REAL question stands as text
code, res = api("POST", "/api/triage/threads/t-bare-1/result", {
    "outcome": "asked", "state": "needs-you", "note": LONG,
    "ask": {"kind": "text",
            "prompt": "Which certificate did you mean, the relay or the site one?"}})
check("R3 control: a real question stands as a text ask", code == 202,
      (code, res))

# the banned-phrase sweep over EVERY stored ask in this DB
swept = 0
code, th = api("GET", "/api/triage/threads?include_done=1&include_snoozed=1")
for t in th["threads"]:
    blob = (t["ask"]["prompt"] + " " + t["ask"].get("context", "") + " "
            + " ".join(o.get("label", "") + " " + o.get("effect", "")
                       for o in t["ask"]["options"])).lower()
    if store.BANNED_ASK_PHRASE in blob:
        swept += 1
check("banned-phrase sweep: no stored ask contains it", swept == 0, swept)

# ============================================================ wrap
print("%d passed, %d failed" % (PASSED, FAILED))
sys.exit(1 if FAILED else 0)
