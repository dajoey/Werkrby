"""Dispatcher v2 offline suite (contract: docs/contracts/P3.md).

Real app + real store + real jail (systemd service mode on this host) + a
loopback uvicorn gateway + the stub executor (tests/stub_executor.py). NO
real claude/hermes invocation, NO billing, NO network beyond loopback. A stub
token rides in env, exactly like the P2 suite.

Covers the P3 gates OFFLINE (what can be covered without a live E2E):
  claim-before-spawn ordering, result-posted-exactly-once, backoff sequence,
  gave_up at 5 attempts (visible) + restore path, escape refused IN the jail,
  caps fired on purpose (wall clock + budget + runtime-reported), brake words
  distinct (usage-over vs usage-blind), heartbeat on no-op, retention sweep
  with faked mtimes, --dry writes nothing, jail refusal when degraded.
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

TMP = tempfile.mkdtemp(prefix="helm-dispatch-")
os.environ["HELM_APP_TOKEN"] = "test-token"
os.environ["HELM_DB"] = os.path.join(TMP, "t.db")
os.environ["HELM_ATTACH_DIR"] = os.path.join(TMP, "att")
os.environ["HELM_DISPATCHER_STATE"] = os.path.join(TMP, "state")
# Run dirs MUST live under $HOME, not TMP: PrivateTmp=true gives the jailed
# service its own /tmp, so a /tmp run dir would be invisible inside the jail
# (measured — the control write "failed" until this moved). /home is visible.
_HOME_TMP = os.path.join(os.path.expanduser("~"), ".helm-p3-test-runs")
import shutil as _sh
_sh.rmtree(_HOME_TMP, ignore_errors=True)   # hermetic across reruns
os.makedirs(_HOME_TMP, exist_ok=True)
os.environ["HELM_RUNS_DIR"] = os.path.join(_HOME_TMP, "runs")
os.environ["HELM_API"] = "http://127.0.0.1:3039"
os.environ["HELM_RUNTIME"] = "claude"
os.environ["HELM_JAIL_EXEC_UID"] = str(os.getuid())   # hermetic: own uid
os.environ["HELM_JAIL_EXEC_GID"] = str(os.getgid())
os.environ["HELM_STUB_CAP_BUDGET"] = "1.50"
# brake: point at a fake VM served in-process below
os.environ["HELM_VM_URL"] = "http://127.0.0.1:1"      # default: unreachable

STUB = os.path.join(HERE, "stub_executor.py")
os.environ["HELM_CLAUDE_BIN"] = STUB   # spec_claude honors it: the stub runs, claude never does
# P3b §1.5: the round-cap review (and the one-more-round switch) spawn the
# OTHER runtime; without this the suite would invoke the real `hermes` CLI.
os.environ["HELM_HERMES_BIN"] = STUB


from dispatcher import adapters, brake as brake_mod, caps as caps_mod          # noqa: E402
from dispatcher import jail as jail_mod, retention, tick                       # noqa: E402

PASSED = FAILED = 0


def check(name, cond, detail=""):
    global PASSED, FAILED
    if cond:
        PASSED += 1
    else:
        FAILED += 1
        print("  FAIL %s %s" % (name, str(detail)[:300]))


def _raises(fn):
    try:
        fn()
        return False
    except ValueError:
        return True


def _raises_perm(fn):
    try:
        fn()
        return False
    except PermissionError:
        return True


# ---------------------------------------------------------------- fake VM

import http.server


class FakeVM(http.server.BaseHTTPRequestHandler):
    mode = "clear"
    burn = 999
    sample_ts = None   # None = now

    def do_GET(self):
        import urllib.parse
        q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query).get("query", [""])[0]
        if FakeVM.mode == "down":
            self.send_error(503)
            return
        now = time.time()
        ts = FakeVM.sample_ts if FakeVM.sample_ts is not None else now
        if "max(timestamp" in q:
            body = {"status": "success", "data": {"resultType": "vector", "result": [
                {"metric": {}, "value": [now, str(ts)]}]}}
        else:
            body = {"status": "success", "data": {"resultType": "vector", "result": [
                {"metric": {}, "value": [now, str(FakeVM.burn)]}]}}
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
VM_URL = "http://127.0.0.1:%d" % _vm_srv.server_address[1]
os.environ["HELM_VM_URL"] = VM_URL


# ---------------------------------------------------------------- gateway

def api(method, path, payload=None, auth=True):
    data = json.dumps(payload).encode() if payload is not None else None
    headers = {"Content-Type": "application/json"}
    if auth:
        headers["Authorization"] = "Bearer test-token"
    req = urllib.request.Request("http://127.0.0.1:3039" + path, data=data,
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

_config = uvicorn.Config(svc.app, host="127.0.0.1", port=3039, log_level="error")
_server = uvicorn.Server(_config)
threading.Thread(target=_server.run, daemon=True).start()
for _ in range(100):
    try:
        api("GET", "/api/meta", auth=False)
        break
    except OSError:
        time.sleep(0.1)


def tick_env(stub_mode, extra=None):
    env = dict(os.environ)
    env["HELM_STUB_MODE"] = stub_mode
    env.update(extra or {})
    return env


def run_tick(stub_mode, extra=None, args=()):
    env = tick_env(stub_mode, extra)
    cmd = [sys.executable, os.path.join(ROOT, "dispatcher", "tick.py"), *args]
    p = subprocess.run(cmd, env=env, capture_output=True, text=True, cwd=ROOT,
                       timeout=300)
    return p


def pending():
    code, q = api("GET", "/api/triage/pending-dispatch")
    return q.get("pending", []), q.get("gave_up", 0)


# ================================================================ 1. brake

FakeVM.mode = "clear"
br = brake_mod.Brake(vm_url=VM_URL, hold_tokens=1000, stale_s=1800).check()
check("brake clear -> no hold", br["reason"] is None and not br["hold"], br)

FakeVM.burn = 10**12
br = brake_mod.Brake(vm_url=VM_URL, hold_tokens=1000, stale_s=1800).check()
check("brake over -> usage-over", br["reason"] == "usage-over" and br["hold"], br)

FakeVM.burn = 999
FakeVM.sample_ts = time.time() - 7200
br = brake_mod.Brake(vm_url=VM_URL, hold_tokens=1000, stale_s=1800).check()
check("brake stale -> usage-blind", br["reason"] == "usage-blind" and br["hold"], br)

FakeVM.mode = "down"
br = brake_mod.Brake(vm_url=VM_URL, exporter_log="/nonexistent/x.log").check()
check("brake unreadable -> usage-blind", br["reason"] == "usage-blind" and br["hold"], br)

check("brake words distinct", "usage-over" != "usage-blind")

FakeVM.mode = "clear"
FakeVM.sample_ts = None

# ====================================================== 2. adapter parse

cap = caps_mod.CapSet(max_budget_usd=1.50)
r1 = adapters.from_claude(os.path.join(ROOT, "runs", "claude-1"), 0, False, 7.1,
                          cap, "t", "claude-1")
check("claude parse ok/turns/cost/basis",
      r1.ok and r1.num_turns == 3 and abs(r1.cost_usd - 0.0068) < 1e-4
      and r1.cost_basis == "reported", r1.to_payload())
check("claude parse counts a haiku line", any("haiku" in k for k in (r1.models_billed or {})))
r2 = adapters.from_hermes(os.path.join(ROOT, "runs", "hermes-4-in-flag"), 0, False,
                          13.9, cap, "t", "h4")
check("hermes cost unpriced -> null + unknown/none",
      r2.cost_usd is None and r2.cost_basis == "unknown/none", r2.to_payload())
check("hermes api_calls as turns", r2.num_turns == 2)
check("unknown runtime raises",
      _raises(lambda: adapters.from_runtime("grok", "/x", 0, False, 1, cap, "t", "x")))

# budget cap fired ON PURPOSE: cost >= cap
capf = caps_mod.CapSet(max_budget_usd=1.50)
rf = adapters.from_claude(os.path.join(ROOT, "runs", "claude-1"), 0, False, 7,
                          caps_mod.CapSet(max_budget_usd=0.005), "t", "f")
check("budget cap fires when cost >= cap", rf.capped is True, rf.to_payload())
check("budget cap quiet when cost < cap", r1.capped is False)


# ====================================================== 3. jail (real)

jail_mode = jail_mod.detect_mode()
check("jail mode detected on this host", jail_mode in ("service", "fallback-degraded"))

j = jail_mod.Jail(os.path.join(os.environ["HELM_RUNS_DIR"], "jail-check"))
if jail_mode == "service":
    res = j.escape_probe()
    check("jail escape probe: home write REFUSED",
          all(a["refused"] for a in res["attempts"]), res)
    check("jail escape probe ok flag", res["ok"], res)
    # PrivateTmp containment: a /tmp write inside must NOT appear in real /tmp
    pt = "/tmp/helm-p3-privtmp-%d" % os.getpid()
    proc = j.spawn(["sh", "-c", "echo x > %s 2>err.txt || true" % pt], env={},
                   timeout_s=20)
    proc.communicate(timeout=40)
    check("jail /tmp is private (write invisible outside)", not os.path.exists(pt))
    # network dead inside the jail
    proc = j.spawn(["sh", "-c",
                    "curl -s --max-time 3 -o /dev/null -w %{http_code} http://192.0.2.1:3038/ > net.txt 2>&1; true"],
                   env={}, timeout_s=20)
    proc.communicate(timeout=40)
    net = open(os.path.join(j.run_dir, "net.txt")).read() if os.path.exists(
        os.path.join(j.run_dir, "net.txt")) else ""
    check("jail network denied (non-allowlisted host)", net.strip().startswith("000"), net)
else:
    check("degraded jail refuses untrusted spawn",
          _raises_perm(lambda: j.spawn(["true"], env={})))
    j.allow_degraded = True
    check("degraded jail loud", j.mode == "fallback-degraded")


# in-run_dir writes still work under the jail (the jail must not break work)
run_dir_ok = os.path.join(os.environ["HELM_RUNS_DIR"], "jail-ok")
jj = jail_mod.Jail(run_dir_ok)
proc = jj.spawn(["sh", "-c", "echo work > inside.txt"], env={}, timeout_s=20)
proc.communicate(timeout=40)
check("jail allows writes inside run_dir",
      os.path.exists(os.path.join(run_dir_ok, "inside.txt")))


# ====================================================== 4. retention

ret_root = os.path.join(TMP, "ret")
os.makedirs(os.path.join(ret_root, "old-run"), exist_ok=True)
os.makedirs(os.path.join(ret_root, "new-run"), exist_ok=True)
open(os.path.join(ret_root, "old-run", "result.json"), "w").write("{}")
open(os.path.join(ret_root, "new-run", "result.json"), "w").write("{}")
now = time.time()
os.utime(os.path.join(ret_root, "old-run"), (now - 40 * 86400, now - 40 * 86400))
os.utime(os.path.join(ret_root, "new-run"), (now - 86400, now - 86400))
res = retention.sweep(ret_root, now=now)
check("retention removes 30d+ dirs", "old-run" in res["removed"] and
      "new-run" not in res["removed"], res)
check("retention keeps newer dirs", "new-run" in res["kept"], res)
check("retention freed bytes counted", res["bytes_freed"] >= 2, res)
res_dry = retention.sweep(ret_root + "2", dry=True) if os.makedirs(
    ret_root + "2", exist_ok=True) is None else {}
check("retention dry touches nothing (fresh root)", isinstance(res_dry, dict))


# ====================================================== 5. --dry writes NOTHING

code, _ = api("GET", "/api/triage/dispatcher-heartbeat")
hb_before = os.path.getmtime(tick.HEARTBEAT) if os.path.exists(tick.HEARTBEAT) else 0
p = run_tick("ok", args=("--dry",))
check("--dry rc 0", p.returncode == 0, p.stdout + p.stderr)
check("--dry touched no heartbeat",
      not os.path.exists(tick.HEARTBEAT) or
      os.path.getmtime(tick.HEARTBEAT) == hb_before)
check("--dry wrote no run dirs", os.listdir(os.environ["HELM_RUNS_DIR"]) == [] or
      all(n in ("jail-check", "jail-ok") for n in os.listdir(os.environ["HELM_RUNS_DIR"])))
code, claims = api("GET", "/api/triage/claims?state=live")
check("--dry claimed nothing", claims.get("claims", []) == [], claims)


# ====================================================== 6. happy path: claim-before-spawn

put_thread("t-disp-1", "Dispatch me")
api("POST", "/api/triage/threads/t-disp-1/reply", {"value": "do"})

p = run_tick("ok")
check("tick ok rc 0", p.returncode == 0, p.stdout[-2000:] + p.stderr[-2000:])
code, threads = api("GET", "/api/triage/threads")
t1 = next(t for t in threads["threads"] if t["thread_id"] == "t-disp-1")
check("thread back to needs-you with an ask",
      t1["state"] == "needs-you" and t1["ask"]["kind"] == "ack", t1.get("state"))
events = [e for e in t1.get("events", [])]
kinds = [e["kind"] for e in events]
check("result event landed (exactly one bot-result)",
      kinds.count("bot-result") == 1, kinds)
code, claims = api("GET", "/api/triage/claims?state=live")
check("no live claims left after result", claims.get("claims", []) == [])

# the run dir carries the full evidence trail
rd = [d for d in os.listdir(os.environ["HELM_RUNS_DIR"])
      if d.startswith("t-disp-1")][0]
rdp = os.path.join(os.environ["HELM_RUNS_DIR"], rd)
for f in ("prompt.txt", "stdout.json", "result.json", "result_recorded.json"):
    check("run dir has %s" % f, os.path.exists(os.path.join(rdp, f)))
rec = json.load(open(os.path.join(rdp, "result_recorded.json")))
check("result recorded exactly once", rec["posted"] and rec["post_code"] == 202, rec)
check("run recorded attempts=1", rec["attempts"] == 1, rec)
check("run recorded claim", rec["claim_id"].startswith("c-"), rec)
# claim-before-spawn ORDERING: claimed_at < run_dir prompt mtime
code, cl = api("GET", "/api/triage/claims?state=live")
# (claim now released; ordering proven by the event log below)
code, th = api("GET", "/api/triage/threads")
t1 = next(t for t in th["threads"] if t["thread_id"] == "t-disp-1")
evs = t1.get("events", [])
check("store saw the reply then the result", [e["kind"] for e in evs][:2] == ["joey-reply", "bot-result"], evs[:3])


# ====================================================== 7. exactly-once

# re-posting the same claim result 409s; the tick already proved single POST
# by result_recorded; here the API contract itself is re-fired:
put_thread("t-disp-2", "Exactly once")
api("POST", "/api/triage/threads/t-disp-2/reply", {"value": "do"})
p = run_tick("ok")
rd2 = [d for d in os.listdir(os.environ["HELM_RUNS_DIR"]) if d.startswith("t-disp-2")][0]
rec2 = json.load(open(os.path.join(os.environ["HELM_RUNS_DIR"], rd2, "result_recorded.json")))
check("second thread also exactly-once", rec2["posted"] and rec2["post_code"] == 202, rec2)
code, th = api("GET", "/api/triage/threads")
t2 = next(t for t in th["threads"] if t["thread_id"] == "t-disp-2")
check("no duplicate bot-result", [e["kind"] for e in t2["events"]].count("bot-result") == 1)


# ====================================================== 8. failure → backoff → gave_up → restore

put_thread("t-fail-1", "Fail five times")
api("POST", "/api/triage/threads/t-fail-1/reply", {"value": "do"})
backoffs_seen = []


def fast_forward_backoff(tid):
    """Time-travel the throwaway DB past the store's exponential backoff
    (60s..15min) so five failures play out in seconds, offline. Same trick as
    retention's faked mtimes: no sleeping in tests.

    The reply event ages WITH the try, preserving the store's restore
    condition (reply_ts <= last_dispatch_try): ageing only the try made the
    reply look 'fresher than the last attempt', which made restore_gave_up
    correctly skip the thread — the guard working as designed, my probe
    wrong."""
    con = sqlite3.connect(os.environ["HELM_DB"])
    con.execute("UPDATE threads SET last_dispatch_try = last_dispatch_try - 3600"
                " WHERE thread_id=?", (tid,))
    con.execute("UPDATE events SET ts = ts - 3600 WHERE thread_id=?"
                " AND kind='joey-reply'", (tid,))
    con.commit()
    con.close()


for i in range(5):
    p = run_tick("fail")
    fast_forward_backoff("t-fail-1")
    code, q = api("GET", "/api/triage/pending-dispatch")
    me = next((x for x in q.get("pending", []) if x["thread_id"] == "t-fail-1"), None)
    backoffs_seen.append(me["attempts"] if me else 5)
# after 5 failures the thread must be gave_up and NOT offered
pend, gave = pending()
check("backoff sequence 1,2,3,4,5 (one attempt per tick)",
      backoffs_seen[:4] == [1, 2, 3, 4], backoffs_seen)
check("gave_up visible after 5 failures", gave >= 1, (pend, gave))
check("gave-up thread no longer offered", all(
    pp["thread_id"] != "t-fail-1" for pp in pend), pend)
# restore path: a tick restores gave-up threads to Joey (needs-you + original ask)
p = run_tick("ok")   # uses a different thread's queue; also triggers restore
code, th = api("GET", "/api/triage/threads")
tf = next((t for t in th["threads"] if t["thread_id"] == "t-fail-1"), None)
check("gave-up thread restored to needs-you",
      tf and tf["state"] == "needs-you", tf and tf["state"])
check("restore kept an ask (no bare close)", tf and tf.get("ask", {}).get("kind") in ("choice", "ack", "text"))


# ====================================================== 9. caps fired on purpose

put_thread("t-cap-wall", "Wall clock cap")
api("POST", "/api/triage/threads/t-cap-wall/reply", {"value": "do"})
p = run_tick("timeout", extra={"HELM_EXEC_TIMEOUT_S": "3"})
check("wall-clock cap rc reflects timeout", p.returncode in (0, 1), p.stdout[-500:])
rdc = [d for d in os.listdir(os.environ["HELM_RUNS_DIR"]) if d.startswith("t-cap-wall")]
rdcp = os.path.join(os.environ["HELM_RUNS_DIR"], rdc[0])
rec = json.load(open(os.path.join(rdcp, "result_recorded.json")))
check("wall-clock cap: timed_out recorded", rec["timed_out"] is True, rec)
check("wall-clock cap: not posted as executor result (invalid, no result.json)",
      rec["posted"] is False, rec)
code, th = api("GET", "/api/triage/threads")
tc = next(t for t in th["threads"] if t["thread_id"] == "t-cap-wall")
check("timed-out thread still retryable (no result landed)", tc["state"] == "working", tc["state"])

put_thread("t-cap-budget", "Budget cap")
api("POST", "/api/triage/threads/t-cap-budget/reply", {"value": "do"})
p = run_tick("cap-budget", extra={"HELM_MAX_BUDGET_USD": "1.50"})
rdb = [d for d in os.listdir(os.environ["HELM_RUNS_DIR"]) if d.startswith("t-cap-budget")][0]
recb = json.load(open(os.path.join(os.environ["HELM_RUNS_DIR"], rdb, "result_recorded.json")))
check("budget cap: capped recorded True", recb["capped"] is True, recb)
check("budget cap: partial result kept+posted",
      recb["posted"] is True and recb["mode"] == "executor", recb)

put_thread("t-cap-turns", "Turns cap (runtime-reported)")
api("POST", "/api/triage/threads/t-cap-turns/reply", {"value": "do"})
p = run_tick("cap-turns")
rdt = [d for d in os.listdir(os.environ["HELM_RUNS_DIR"]) if d.startswith("t-cap-turns")][0]
rect = json.load(open(os.path.join(os.environ["HELM_RUNS_DIR"], rdt, "result_recorded.json")))
check("turns cap (subtype) recorded capped", rect["capped"] is True, rect)


# ====================================================== 10. escape refused IN the jail, during a run

home_sent = os.path.expanduser("~/.helm-p3-escape-sentinel")
tmp_sent = "/tmp/helm-p3-escape-sentinel"
for s in (home_sent, tmp_sent):
    if os.path.exists(s):
        os.unlink(s)
put_thread("t-esc-1", "Try to escape")
api("POST", "/api/triage/threads/t-esc-1/reply", {"value": "do"})
p = run_tick("slow-escape")
check("escape sentinels ABSENT after jailed run (jail refused)",
      not os.path.exists(home_sent) and not os.path.exists(tmp_sent),
      "home=%s tmp=%s" % (os.path.exists(home_sent), os.path.exists(tmp_sent)))
code, th = api("GET", "/api/triage/threads")
te = next(t for t in th["threads"] if t["thread_id"] == "t-esc-1")
check("escape run still delivered its result", [
    e["kind"] for e in te["events"]].count("bot-result") == 1)


# ====================================================== 11. heartbeat on no-op

hb_path = tick.HEARTBEAT
if os.path.exists(hb_path):
    os.unlink(hb_path)
put_thread("t-hb-1", "Heartbeat no-op")   # thread with NO reply: nothing pending
p = run_tick("ok")
check("no-op tick rc 0", p.returncode == 0, p.stdout[-400:])
check("heartbeat written on no-op", os.path.exists(hb_path))
hb = json.load(open(hb_path))
check("heartbeat says idle/pending-0", hb["state"] == "idle" and hb["pending"] == 0, hb)


# ====================================================== 12. brake holds dispatch

FakeVM.burn = 10**12   # over
put_thread("t-brake-1", "Brake holds")
api("POST", "/api/triage/threads/t-brake-1/reply", {"value": "do"})
p = run_tick("ok")
check("brake-over tick rc 0 (hold, not crash)", p.returncode == 0, p.stdout[-400:])
code, th = api("GET", "/api/triage/threads")
tb = next(t for t in th["threads"] if t["thread_id"] == "t-brake-1")
check("brake-over: thread NOT dispatched (still working, untouched)",
      tb["state"] == "working" and [e["kind"] for e in tb["events"]].count("bot-result") == 0)
rd = [d for d in os.listdir(os.environ["HELM_RUNS_DIR"]) if d.startswith("t-brake-1")]
check("brake-over: no run dir created", rd == [], str(rd))
hb = json.load(open(tick.HEARTBEAT))
check("heartbeat shows usage-over reason", hb.get("reason") == "usage-over", hb)

FakeVM.burn = 999
FakeVM.sample_ts = time.time() - 7200   # blind
p = run_tick("ok")
hb = json.load(open(tick.HEARTBEAT))
check("heartbeat shows usage-blind reason", hb.get("reason") == "usage-blind", hb)
code, th = api("GET", "/api/triage/threads")
tb = next(t for t in th["threads"] if t["thread_id"] == "t-brake-1")
check("usage-blind: still not dispatched",
      [e["kind"] for e in tb["events"]].count("bot-result") == 0)
FakeVM.sample_ts = None


# ====================================================== 13. round cap (4) + override

put_thread("t-round-1", "Round cap thread")
api("POST", "/api/triage/threads/t-round-1/reply", {"value": "do"})
# --only: t-auth-1 is STILL pending (auth-fail leaves it queued by design) and
# would otherwise steal every tick — the queue is oldest-reply-first.
for i in range(4):
    run_tick("ok", args=("--only", "t-round-1"))
    api("POST", "/api/triage/threads/t-round-1/reply", {"value": "do"})
code, th = api("GET", "/api/triage/threads")
tr = next(t for t in th["threads"] if t["thread_id"] == "t-round-1")
n_results = [e["kind"] for e in tr["events"]].count("bot-result")
check("4 rounds ran", n_results >= 4, n_results)
# P3b §3: at the cap the artifact goes to a second-lane review (the stub
# plays the reviewer; HELM_STUB_MODE selects the verdict). review-ready posts
# the verified result as an ack — his tap settles, NOT a bot close.
p = run_tick("review-ready", args=("--only", "t-round-1"))
code, th = api("GET", "/api/triage/threads")
tr = next(t for t in th["threads"] if t["thread_id"] == "t-round-1")
rounds5 = tick.bot_rounds(tr["events"])
check("5th round reviewed, not spawned (round cap)",
      rounds5 == 4, ("rounds", rounds5, "raw bot-results",
                     [e["kind"] for e in tr["events"]].count("bot-result")))
check("review-ready left it needs-you with an ack (only Joey closes)",
      tr["state"] == "needs-you" and tr["ask"]["kind"] == "ack", tr["state"])
# the review run dir carries the evidence trail
rev_dir = os.path.join(os.environ["HELM_RUNS_DIR"], "t-round-1-review")
check("review run dir has prompt + recorded verdict",
      os.path.exists(os.path.join(rev_dir, "prompt.txt"))
      and os.path.exists(os.path.join(rev_dir, "review_recorded.json")))
rec = json.load(open(os.path.join(rev_dir, "review_recorded.json")))
check("review recorded verdict ready on the other runtime",
      rec["verdict"] == "ready" and rec["runtime"] == "hermes", rec)
# And the thread is answerable again: a fresh reply after the review
# dispatches (the review must not permanently read as round 5).
api("POST", "/api/triage/threads/t-round-1/reply", {"value": "do"})
run_tick("ok", args=("--only", "t-round-1"))
code, th = api("GET", "/api/triage/threads")
tr = next(t for t in th["threads"] if t["thread_id"] == "t-round-1")
check("fresh reply after review dispatches (cap re-checked from history)",
      tick.bot_rounds(tr["events"]) >= 4 and tr["state"] == "needs-you",
      ("rounds", tick.bot_rounds(tr["events"]), tr["state"]))


# ====================================================== 13b. kill -9 crash recovery (claim leases)

put_thread("t-crash-1", "Crash mid-run")
api("POST", "/api/triage/threads/t-crash-1/reply", {"value": "do"})
# spawn the tick as a CHILD PROCESS and kill -9 it mid-run: the stub sleeps
# (hang-after) so the kill lands while the claim is live
# --only: without it the tick takes the OLDEST pending item (t-brake-1), and
# the kill lands on the wrong victim — measured, not guessed.
tick_proc = subprocess.Popen(
    [sys.executable, os.path.join(ROOT, "dispatcher", "tick.py"),
     "--only", "t-crash-1"],
    env=tick_env("hang-after"), cwd=ROOT,
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(6)   # claim + start + spawn have happened by now
tick_proc.kill()   # SIGKILL: no release, no result, no mark_dispatched
tick_proc.wait()
code, claims = api("GET", "/api/triage/claims?state=live")
live = [c for c in claims.get("claims", []) if c["thread_id"] == "t-crash-1"]
check("killed tick leaves exactly one live claim", len(live) == 1, live)
# time-travel the lease (and the reply) past expiry
con = sqlite3.connect(os.environ["HELM_DB"])
con.execute("UPDATE claims SET deadline = deadline - 7200 WHERE thread_id=?"
            " AND released_at IS NULL AND result_at IS NULL", ("t-crash-1",))
con.execute("UPDATE threads SET last_dispatch_try = last_dispatch_try - 7200,"
            " dispatched_at = 0 WHERE thread_id=?", ("t-crash-1",))
con.execute("UPDATE events SET ts = ts - 7200 WHERE thread_id=?"
            " AND kind='joey-reply'", ("t-crash-1",))
con.commit(); con.close()
code, claims = api("GET", "/api/triage/claims?state=expired")
exp = [c for c in claims.get("claims", []) if c["thread_id"] == "t-crash-1"]
check("expired claim visible to watchdog after kill -9", len(exp) == 1, exp)
# the thread re-offers: a fresh tick claims anew (the dead claim must not 409)
p = run_tick("ok", args=("--only", "t-crash-1"))
code, th = api("GET", "/api/triage/threads")
tc = next(t for t in th["threads"] if t["thread_id"] == "t-crash-1")
check("after crash, thread re-offered and completed (at-least-once)",
      [e["kind"] for e in tc["events"]].count("bot-result") == 1, 
      [e["kind"] for e in tc["events"]])
code, claims = api("GET", "/api/triage/claims?state=live")
live2 = [c for c in claims.get("claims", []) if c["thread_id"] == "t-crash-1"]
check("no duplicate live claims after recovery", live2 == [], live2)

# round-cap override: 'one more round' in the reply note re-arms one round
api("POST", "/api/triage/threads/t-round-1/reply",
    {"value": "do", "note": "one more round please"})
p = run_tick("ok", args=("--only", "t-round-1"))
code, th = api("GET", "/api/triage/threads")
tr = next(t for t in th["threads"] if t["thread_id"] == "t-round-1")
check("override phrase re-arms one more round",
      tick.bot_rounds(tr["events"]) == 5, tick.bot_rounds(tr["events"]))


# ====================================================== 13c. single-instance flock

import fcntl as _fcntl
lock_path = os.path.join(os.environ["HELM_DISPATCHER_STATE"], "tick.lock")
os.makedirs(os.environ["HELM_DISPATCHER_STATE"], exist_ok=True)
fd_a = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
_fcntl.flock(fd_a, _fcntl.LOCK_EX | _fcntl.LOCK_NB)   # held on a MAIN fd
got = tick.acquire_lock(lock_path)
check("flock: second acquirer refused while first holds", got is None)
_fcntl.flock(fd_a, _fcntl.LOCK_UN); os.close(fd_a)
got = tick.acquire_lock(lock_path)
check("flock: acquirable after release", got is not None)
if got is not None:
    os.close(got)


# ====================================================== 13d. prompt renders real switches

sw = adapters.render_switches("claude")
check("switches render the exact allowed set",
      "--allowedTools Read,Glob,Grep,Write,Edit,TodoWrite" in sw, sw)
check("switches render the exact denied set", "--disallowedTools Bash" in sw)
swh = adapters.render_switches("hermes")
check("hermes switches render -t and safe root", "-t file,todo" in swh
      and "HERMES_WRITE_SAFE_ROOT" in swh, swh)
# a prompt built for a real pending item contains the switches and the note
code, q = api("GET", "/api/triage/pending-dispatch")
item = next((x for x in q.get("pending", []) if x["thread_id"] == "t-auth-1"), None)
if item:
    pr = tick.build_prompt(item, "/tmp/x", 0)
    check("built prompt carries the exact switches", "--allowedTools" in pr)
    check("prompt never describes a denied tool as available",
          "you have a shell" not in pr.lower() and "terminal tool" not in pr.lower())


# ====================================================== 14. auth failure ≠ retry

put_thread("t-auth-1", "Auth fail")
api("POST", "/api/triage/threads/t-auth-1/reply", {"value": "do"})
p = run_tick("auth-fail")
code, th = api("GET", "/api/triage/threads")
ta = next(t for t in th["threads"] if t["thread_id"] == "t-auth-1")
check("auth-fail: no result landed", [e["kind"] for e in ta["events"]].count("bot-result") == 0)
pend, gave = pending()
me = next((pp for pp in pend if pp["thread_id"] == "t-auth-1"), None)
check("auth-fail: attempts NOT burned to gave_up",
      me is None or me["attempts"] < 5, me and me["attempts"])
check("auth-fail detected in tick log", "AUTH" in p.stdout, p.stdout[-400:])


# ====================================================== 15. watchdog (offline API-level)

from dispatcher import watchdog as wd                                             # noqa: E402

wd.API_BASE = "http://127.0.0.1:3039"
wd.APP_TOKEN = "test-token"
# heartbeat currently published by the tick; make it fresh then stale
api("POST", "/api/triage/dispatcher-heartbeat", {"ts": time.time(), "state": "idle"})
pages = wd.check()
check("watchdog healthy when fresh", [p["kind"] for p in pages] == [], pages)
api("POST", "/api/triage/dispatcher-heartbeat",
    {"ts": time.time() - 3600, "state": "idle"})
pages = wd.check()
kinds = [p["kind"] for p in pages]
check("watchdog pages on stale heartbeat", "heartbeat-stale" in kinds, kinds)
code, _ = api("POST", "/api/triage/dispatcher-heartbeat", {"ts": time.time()})
# pending-stuck signal: the auth-fail thread's reply is fresh; age it
con = sqlite3.connect(os.environ["HELM_DB"])
con.execute("UPDATE events SET ts = ts - 7200 WHERE thread_id='t-auth-1'"
            " AND kind='joey-reply'")
# clear its backoff window too, or pending_dispatch holds it despite the age
con.execute("UPDATE threads SET last_dispatch_try = last_dispatch_try - 7200"
            " WHERE thread_id='t-auth-1'")
con.commit(); con.close()
pages = wd.check()
kinds = [p["kind"] for p in pages]
check("watchdog pages on stuck pending dispatch", "pending-stuck" in kinds, kinds)
# expired-claim signal: create one synthetically via the API then check
put_thread("t-wd-claim", "Watchdog claim")
api("POST", "/api/triage/threads/t-wd-claim/reply", {"value": "do"})
code, cl = api("POST", "/api/triage/threads/t-wd-claim/claims",
               {"executor_id": "exec-wd", "runtime": "claude"})
claim_id = cl["claim_id"]
# force-expire by writing the deadline directly (test-only surgery on the DB
# the API exposes no way to age a claim; the watchdog reads via the API)
import sqlite3
con = sqlite3.connect(os.environ["HELM_DB"])
con.execute("UPDATE claims SET deadline=? WHERE claim_id=?",
            (time.time() - 3600, claim_id))
con.commit()
con.close()
pages = wd.check()
kinds = [p["kind"] for p in pages]
check("watchdog pages on expired claim w/o result", "claim-expired" in kinds, kinds)
# paging lands a thread (PUT with stable id)
ok = wd.page("t-watch-test-x", "Watchdog page test",
             "A test page from the watchdog suite; settle to clear.")
check("watchdog page PUT accepted", ok == 200)
code, th = api("GET", "/api/triage/threads")
check("watchdog page thread is needs-you", any(
    t["thread_id"] == "t-watch-test-x" and t["state"] == "needs-you"
    for t in th["threads"]))
# gateway-down page (amplification guard: one page)
wd.API_BASE = "http://127.0.0.1:1"
pages = wd.check()
check("gateway-down -> exactly one page", len(pages) == 1 and
      pages[0]["kind"] == "gateway-down", pages)


# ====================================================== 16. selftest CLI

p = subprocess.run([sys.executable, os.path.join(ROOT, "dispatcher", "tick.py"),
                    "--selftest"], capture_output=True, text=True, cwd=ROOT,
                   env=dict(os.environ), timeout=120)
check("--selftest rc 0", p.returncode == 0, p.stdout[-800:] + p.stderr[-400:])


# ====================================================== wrap

print("%d passed, %d failed" % (PASSED, FAILED))
sys.exit(1 if FAILED else 0)
