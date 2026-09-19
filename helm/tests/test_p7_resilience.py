"""P7 offline suite (contract: docs/contracts/P7.md) — dispatcher resilience.

Defect A (claude fallback auth in the jail) and Defect B (note-only settle),
proven OFFLINE: real app + real store + real jail + loopback gateway + the stub
executor. NO real claude/hermes invocation, NO billing, NO secrets, NO network
beyond loopback. Live evidence behind both fixes: the dispatcher log,
2026-09-13 (66 fails, 18 claims on one note-only thread).

Gates covered here:
  store:   no-action verdict settles ONLY with a literal-true flag, a reply
           newer than the ask, the note floor, and the answer gate intact;
           every refusal shape 400s; the old bot-close rules are unchanged.
  tick:    note-only verdict posts, settles done, leaves the pending queue,
           and is never re-claimed; a malformed flag degrades to the ack
           hand-back instead of a 400 loop.
  switch:  HELM_PROVIDER_ENV_VARS can no longer drop the anthropic credential
           names; a switch-to-claude with bearer material carries it into the
           jail EnvironmentFile; a switch with NO usable auth is refused
           BEFORE claim (no claim burned, loud AUTH-SWITCH).
  regress: genuine failure still backs off to a loud gave_up, and restore
           still hands the thread back to Joey.
"""
import json
import os
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

TMP = tempfile.mkdtemp(prefix="helm-p7-")
os.environ["HELM_APP_TOKEN"] = "test-token"
os.environ["HELM_DB"] = os.path.join(TMP, "t.db")
os.environ["HELM_ATTACH_DIR"] = os.path.join(TMP, "att")
os.environ["HELM_DISPATCHER_STATE"] = os.path.join(TMP, "state")
_HOME_TMP = os.path.join(os.path.expanduser("~"), ".helm-p7-test-runs")
import shutil as _sh
_sh.rmtree(_HOME_TMP, ignore_errors=True)
os.makedirs(_HOME_TMP, exist_ok=True)
os.environ["HELM_RUNS_DIR"] = os.path.join(_HOME_TMP, "runs")
os.environ["HELM_API"] = "http://127.0.0.1:3041"
os.environ["HELM_RUNTIME"] = "claude"
os.environ["HELM_JAIL_EXEC_UID"] = str(os.getuid())
os.environ["HELM_JAIL_EXEC_GID"] = str(os.getgid())

STUB = os.path.join(HERE, "stub_executor.py")
os.environ["HELM_CLAUDE_BIN"] = STUB
os.environ["HELM_HERMES_BIN"] = STUB

# Hermetic: the host env must not leak real credential names or stub knobs
# into any tick this suite spawns.
SCRUB = ("ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_API_KEY", "ANTHROPIC_TOKEN",
         "CLAUDE_CODE_OAUTH_TOKEN", "HELM_STUB_AUTH_STATUS",
         "HELM_STUB_MODE_HERMES", "HELM_PROVIDER_ENV_VARS")
for _k in SCRUB:
    os.environ.pop(_k, None)

from helm import store  # noqa: E402
from dispatcher import jail as jail_mod, tick  # noqa: E402

PASSED = FAILED = 0


def check(name, cond, detail: object = ""):
    global PASSED, FAILED
    if cond:
        PASSED += 1
    else:
        FAILED += 1
        print("  FAIL %s %s" % (name, str(detail)[:300]))


# ---------------------------------------------------------------- fake VM

import http.server


class FakeVM(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        import urllib.parse
        q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query).get("query", [""])[0]
        now = time.time()
        if "max(timestamp" in q:
            val = now
        else:
            val = 999  # way under any hold threshold: brake clear
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


def api(method, path, payload=None, auth=True):
    data = json.dumps(payload).encode() if payload is not None else None
    headers = {"Content-Type": "application/json"}
    if auth:
        headers["Authorization"] = "Bearer test-token"
    req = urllib.request.Request("http://127.0.0.1:3041" + path, data=data,
                                 method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode("utf-8", "replace")
            return r.status, (json.loads(body) if body.strip() else {})
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode("utf-8", "replace") or "{}")


def put_thread(tid, title="A thread"):
    return api("PUT", "/api/triage/threads", {"threads": [{
        "thread_id": tid, "title": title, "summary": "s " * 10,
        "category": "Test", "urgency": "this-week", "state": "needs-you",
        "ask": {"kind": "choice", "prompt": "Pick one", "requires": ["decision"],
                "options": [
            {"label": "Do the thing fully", "value": "do", "effect": "It gets done"},
            {"label": "Skip it for now", "value": "no", "effect": "Nothing happens"}]},
        "sources": [{"source": "test", "id": tid, "note": ""}],
    }]})


import uvicorn                                                                    # noqa: E402
import app.service as svc                                                         # noqa: E402

_config = uvicorn.Config(svc.app, host="127.0.0.1", port=3041, log_level="error")
_server = uvicorn.Server(_config)
threading.Thread(target=_server.run, daemon=True).start()
for _ in range(100):
    try:
        api("GET", "/api/meta", auth=False)
        break
    except OSError:
        time.sleep(0.1)


def run_tick(stub_mode, extra=None, args=()):
    env = dict(os.environ)
    for k in SCRUB:           # re-scrub: extra is the ONLY way they come back
        env.pop(k, None)
    env["HELM_STUB_MODE"] = stub_mode
    env.update(extra or {})
    cmd = [sys.executable, os.path.join(ROOT, "dispatcher", "tick.py"), *args]
    return subprocess.run(cmd, env=env, capture_output=True, text=True, cwd=ROOT,
                          timeout=300)


def thread_state(tid):
    code, th = api("GET", "/api/triage/threads?include_done=1")
    return next((t for t in th["threads"] if t["thread_id"] == tid), None)


def claims_of(tid):
    code, cl = api("GET", "/api/triage/claims?state=all")
    return [c for c in cl.get("claims", []) if c["thread_id"] == tid]


def db_attempts(tid):
    con = sqlite3.connect(os.environ["HELM_DB"])
    row = con.execute("SELECT dispatch_attempts FROM threads WHERE thread_id=?",
                      (tid,)).fetchone()
    con.close()
    return row[0] if row else None


def fast_forward_backoff(tid):
    con = sqlite3.connect(os.environ["HELM_DB"])
    con.execute("UPDATE threads SET last_dispatch_try = last_dispatch_try - 3600"
                " WHERE thread_id=?", (tid,))
    con.execute("UPDATE events SET ts = ts - 3600 WHERE thread_id=?"
                " AND kind='joey-reply'", (tid,))
    con.commit()
    con.close()


NOTE80 = ("The decision was note-only: no thread action was owed, his point is "
          "recorded verbatim here, and nothing was dispatched or changed. "
          "Settling deliberately.")

# ==================================================== A. store gate (P7 §2)
print("section A: the terminal no-action verdict at the store gate")

_con = store.db(os.path.join(TMP, "store-gate.db"))


def _mk(tid, ask=None):
    t = {"thread_id": tid, "title": "concern %s" % tid, "summary": "s " * 10,
         "category": "Test", "urgency": "this-week", "state": "needs-you",
         "ask": ask or {"kind": "choice", "prompt": "Which way on this?",
                        "requires": ["decision"],
                        "options": [
             {"label": "Do the thing fully", "value": "do",
              "effect": "It gets done"},
             {"label": "Skip it for now", "value": "no",
              "effect": "Nothing happens"}]},
         "sources": [{"source": "test", "id": tid, "note": ""}]}
    store.put_threads(_con, {"threads": [t]})


def _refused(tid, body):
    try:
        store.post_result(_con, tid, body)
        return None
    except store.StoreError as e:
        return e.status


# A1. the verdict settles: note-only reply (binding note, NON_DECISION_REPLIES)
_mk("A1")
store.reply(_con, "A1", "note-only", note="just record that I wrote it myself")
out = store.post_result(_con, "A1", {"outcome": "note-only", "state": "done",
                                     "no_action": True, "note": NOTE80})
check("A1 no-action verdict settles done",
      out.get("accepted") and out.get("state") == "done", out)
check("A1 closed_by reports the mechanism",
      out.get("closed_by") == "no-action-settle", out)

# A2. no reply newer than the ask -> refused
_mk("A2")
st = _refused("A2", {"outcome": "note-only", "state": "done",
                     "no_action": True, "note": NOTE80})
check("A2 no-action with nothing of his to settle on -> 400", st == 400, st)

# A3. the flag is an assertion, not prose -> string refused
store.reply(_con, "A2", "note-only", note="record this, no action needed")
st = _refused("A2", {"outcome": "note-only", "state": "done",
                     "no_action": "true", "note": NOTE80})
check("A3 string no_action -> 400 (literal bool only)", st == 400, st)

# A4. the verdict that does not settle is ambiguous -> refused
st = _refused("A2", {"outcome": "note-only", "state": "needs-you",
                     "no_action": True, "note": NOTE80,
                     "ask": {"kind": "ack", "prompt": "Read and settle."}})
check("A4 no_action with state != done -> 400", st == 400, st)

# A5/A6. the answer gate is NOT exempted by the verdict
_mk("A5")
store.reply(_con, "A5", "note-only",
            note="record this please; also: which port did you pick?")
st = _refused("A5", {"outcome": "note-only", "state": "done",
                     "no_action": True, "note": NOTE80})
check("A5 unanswered question defeats the verdict -> 400", st == 400, st)
out = store.post_result(_con, "A5", {
    "outcome": "note-only", "state": "done", "no_action": True,
    "note": NOTE80, "answers": "Port 3038, the one the container publishes."})
check("A6 verdict + answers settles done",
      out.get("accepted") and out.get("state") == "done", out)

# A7. preserved: the SAME shape without the flag still hits the old refusal
_mk("A7")
store.reply(_con, "A7", "note-only", note="just record it, nothing to do")
st = _refused("A7", {"outcome": "note-only", "state": "done", "note": NOTE80})
check("A7 done without the verdict still 400s (bot-close unchanged)",
      st == 400, st)

# A8. preserved: the answered-decision close still works
_mk("A8")
store.reply(_con, "A8", "do")
out = store.post_result(_con, "A8", {"outcome": "done", "state": "done",
                                     "note": NOTE80, "answer_applied": True,
                                     "nothing_owed": True})
check("A8 answered-decision close still works",
      out.get("accepted") and out.get("closed_by") == "answered-decision", out)
_con.close()

# ============================================== B. note-only E2E (P7 §2/§3)
print("section B: note-only settle through the whole pump")

put_thread("t-p7-note", "Note-only settle")
api("POST", "/api/triage/threads/t-p7-note/reply",
    {"value": "note-only", "note": "he wrote the card himself; record it and "
                                   "route such cards to triage, no action"})
p = run_tick("note-only", args=("--only", "t-p7-note"))
check("B1 note-only tick ran", p.returncode == 0,
      p.stdout[-600:] + p.stderr[-300:])
t = thread_state("t-p7-note")
check("B1 thread settled done",
      t is not None and t["state"] == "done", t and t["state"])
check("B1 exactly one bot-result landed",
      t and [e["kind"] for e in t["events"]].count("bot-result") == 1,
      t and [e["kind"] for e in t["events"]])
check("B1 SETTLED-NOACTION is its own log word (not OK, not FAIL)",
      "SETTLED-NOACTION" in p.stdout, p.stdout[-400:])
check("B1 retry counter reset, not burned", db_attempts("t-p7-note") == 0,
      db_attempts("t-p7-note"))
rd = [d for d in os.listdir(os.environ["HELM_RUNS_DIR"])
      if d.startswith("t-p7-note")]
rec = json.load(open(os.path.join(os.environ["HELM_RUNS_DIR"], rd[0],
                                  "result_recorded.json")))
check("B1 result posted exactly once", rec["posted"] and rec["post_code"] == 202,
      rec)

# the settle is terminal: a later tick never re-offers or re-claims it
p2 = run_tick("ok", args=("--only", "t-p7-note"))
check("B1 second tick idles (nothing pending for a settled thread)",
      p2.returncode == 0 and "pending=0" in p2.stdout, p2.stdout[-300:])
check("B1 never re-claimed (still exactly one claim)",
      len(claims_of("t-p7-note")) == 1, claims_of("t-p7-note"))

# B2. a malformed flag degrades to the ack hand-back, never a 400 loop
put_thread("t-p7-note2", "Malformed note-only flag")
api("POST", "/api/triage/threads/t-p7-note2/reply",
    {"value": "note-only", "note": "record only, nothing to do here"})
p = run_tick("note-only-malformed", args=("--only", "t-p7-note2"))
t = thread_state("t-p7-note2")
check("B2 malformed flag stripped, loudly", "NOTE-ONLY-FLAG" in p.stdout,
      p.stdout[-500:])
check("B2 degraded to needs-you + ack (human exit), not done, not failed",
      t is not None and t["state"] == "needs-you"
      and t["ask"]["kind"] == "ack",
      t and (t["state"], t.get("ask")))
check("B2 the degraded post landed (no 400 loop)",
      t and [e["kind"] for e in t["events"]].count("bot-result") == 1,
      t and [e["kind"] for e in t["events"]])

# ============================================== C. fallback auth (P7 §1/§3)
print("section C: the claude fallback and what its spawn env carries")

# C1. HELM_PROVIDER_ENV_VARS is additive: the production zai-only shape can no
#     longer drop the anthropic credential names (the live defect-A root cause)
def _provider_vars(env_extra):
    env = dict(os.environ)
    env.pop("HELM_PROVIDER_ENV_VARS", None)
    env.update(env_extra)
    p = subprocess.run(
        [sys.executable, "-c",
         "from dispatcher import jail; print(','.join(jail.PROVIDER_ENV_VARS))"],
        capture_output=True, text=True, cwd=ROOT, env=env)
    return p.stdout.strip().split(",") if p.returncode == 0 else []


prod_shape = _provider_vars(
    {"HELM_PROVIDER_ENV_VARS": "ZAI_API_KEY,Z_AI_API_KEY,GLM_API_KEY"})
check("C1 prod-shaped override still carries the anthropic names",
      "ANTHROPIC_AUTH_TOKEN" in prod_shape and "ANTHROPIC_API_KEY" in prod_shape,
      prod_shape)
check("C1 override's own vars survive too",
      "ZAI_API_KEY" in prod_shape and "GLM_API_KEY" in prod_shape, prod_shape)
default_shape = _provider_vars({})
check("C1 default carries anthropic + zai names",
      "ANTHROPIC_AUTH_TOKEN" in default_shape and "ZAI_API_KEY" in default_shape,
      default_shape)

# C2. claude_auth_available(): env bearer path, CLI signed-in path, refusal
os.environ["ANTHROPIC_AUTH_TOKEN"] = "probe-not-a-real-token"
ok, why = tick.claude_auth_available()
check("C2 bearer env var present -> available without asking the CLI",
      ok and "ANTHROPIC_AUTH_TOKEN" in why, (ok, why))
del os.environ["ANTHROPIC_AUTH_TOKEN"]
ok, why = tick.claude_auth_available()
check("C2 signed-in CLI -> available", ok, (ok, why))
os.environ["HELM_STUB_AUTH_STATUS"] = "logged-out"
ok, why = tick.claude_auth_available()
check("C2 no env var + signed-out CLI -> unavailable", not ok, (ok, why))
del os.environ["HELM_STUB_AUTH_STATUS"]

# C3. E2E: hermes primary auth-fails; the switch to claude SPAWNS, and the
#     jail EnvironmentFile carries the bearer token the runtime reads.
put_thread("t-p7-switch", "Switch to claude with bearer auth")
api("POST", "/api/triage/threads/t-p7-switch/reply", {"value": "do"})
p = run_tick("ok", extra={"HELM_RUNTIME": "hermes",
                          "HELM_STUB_MODE_HERMES": "auth-fail",
                          "ANTHROPIC_AUTH_TOKEN": "probe-not-a-real-token-p7"},
             args=("--only", "t-p7-switch"))
check("C3 the switch fired", "SWITCH" in p.stdout, p.stdout[-600:])
check("C3 not refused", "AUTH-SWITCH" not in p.stdout, p.stdout[-300:])
sw = [d for d in os.listdir(os.environ["HELM_RUNS_DIR"])
      if d.startswith("t-p7-switch")]
env_file = os.path.join(os.environ["HELM_RUNS_DIR"], sw[0], "switch", ".env") \
    if sw else ""
check("C3 switch run dir exists", bool(sw) and os.path.isdir(
    os.path.join(os.environ["HELM_RUNS_DIR"], sw[0], "switch")), sw)
env_text = open(env_file).read() if os.path.exists(env_file) else ""
check("C3 jail EnvironmentFile carries ANTHROPIC_AUTH_TOKEN (the var the "
      "installed CLI reads -> Bearer)",
      "ANTHROPIC_AUTH_TOKEN=probe-not-a-real-token-p7" in env_text, env_text[:200])
check("C3 env file is 0600 (secret carrier)",
      os.path.exists(env_file)
      and (os.stat(env_file).st_mode & 0o777) == 0o600,
      oct(os.stat(env_file).st_mode & 0o777) if os.path.exists(env_file) else "")
mpath = os.path.join(os.environ["HELM_RUNS_DIR"], "manifests",
                     "t-p7-switch.json")
entries = json.load(open(mpath)) if os.path.exists(mpath) else []
check("C3 manifest: hermes auth-fail then claude switch posted",
      len(entries) >= 2 and entries[-1].get("switched") is True
      and entries[-1].get("posted") is True
      and entries[-1].get("runtime") == "claude",
      entries)
t = thread_state("t-p7-switch")
check("C3 switched run landed the result",
      t is not None and [e["kind"] for e in t["events"]].count("bot-result") == 1,
      t and [e["kind"] for e in t["events"]])

# C4. the refusal probe (input SHOULD fail and does): no anthropic material
#     anywhere + the CLI signed out -> the switch is refused BEFORE claim.
put_thread("t-p7-noauth", "Switch refused without auth material")
api("POST", "/api/triage/threads/t-p7-noauth/reply", {"value": "do"})
p = run_tick("ok", extra={"HELM_RUNTIME": "hermes",
                          "HELM_STUB_MODE_HERMES": "auth-fail",
                          "HELM_STUB_AUTH_STATUS": "logged-out"},
             args=("--only", "t-p7-noauth"))
check("C4 AUTH-SWITCH refusal is loud in the log", "AUTH-SWITCH" in p.stdout,
      p.stdout[-600:])
check("C4 the switch never claimed (exactly one claim: the failed primary)",
      len(claims_of("t-p7-noauth")) == 1, claims_of("t-p7-noauth"))
sw = [d for d in os.listdir(os.environ["HELM_RUNS_DIR"])
      if d.startswith("t-p7-noauth") and os.path.isdir(
          os.path.join(os.environ["HELM_RUNS_DIR"], d, "switch"))]
check("C4 no switch run dir (nothing spawned)", sw == [], sw)
check("C4 one attempt charged, not two", db_attempts("t-p7-noauth") == 1,
      db_attempts("t-p7-noauth"))
mpath = os.path.join(os.environ["HELM_RUNS_DIR"], "manifests",
                     "t-p7-noauth.json")
entries = json.load(open(mpath)) if os.path.exists(mpath) else []
check("C4 manifest records the refusal",
      any(e.get("auth_switch_refused") for e in entries), entries)
t = thread_state("t-p7-noauth")
check("C4 thread stays queued (working), no result landed",
      t is not None and t["state"] == "working"
      and [e["kind"] for e in t["events"]].count("bot-result") == 0,
      t and t["state"])

# ============================================== D. regression (P7 §2)
print("section D: genuine failure still backs off, gave_up loud, restore works")

put_thread("t-p7-fail", "Genuine failure still retries")
api("POST", "/api/triage/threads/t-p7-fail/reply", {"value": "do"})
for i in range(5):
    run_tick("fail", args=("--only", "t-p7-fail"))
    fast_forward_backoff("t-p7-fail")
code, q = api("GET", "/api/triage/pending-dispatch")
pend_ids = [x["thread_id"] for x in q.get("pending", [])]
check("D1 gave_up visible and the thread no longer offered",
      q.get("gave_up", 0) >= 1 and "t-p7-fail" not in pend_ids,
      (q.get("gave_up"), pend_ids))
run_tick("ok")   # restore fires on any tick
t = thread_state("t-p7-fail")
check("D1 restore hands it back to Joey with an ask",
      t is not None and t["state"] == "needs-you"
      and t["ask"]["kind"] in ("choice", "ack", "text"),
      t and (t["state"], t.get("ask")))

# ====================================================== wrap

print("%d passed, %d failed" % (PASSED, FAILED))
sys.exit(1 if FAILED else 0)
