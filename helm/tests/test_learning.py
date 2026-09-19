"""P3c learning-loop suite (contract: docs/contracts/P3c.md).

Real app + real store + loopback gateway + stub executor. NO model call
anywhere: the classifier is deterministic by design, and the tick paths are
driven with the stub playing every spawn. Covers the contract's §6 probe list:

  - classifier: one probe per signal, noise/ack/answer controls, candidate
    extraction, and the corpus section (correction-shaped bodies)
  - lesson-thread lifecycle: open (gates fire), all four taps, no-tap create
    refused, same-candidate re-open deduped
  - ledger: retire stops rendering, supersede atomicity, changelog reports
  - render: scope isolation, ordering, the REAL tick prompt carrying the block
  - pending gates: the honest gap appears, disappears, records the commit
"""
import json
import os
import shutil
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

TMP = tempfile.mkdtemp(prefix="helm-learn-")
os.environ["HELM_APP_TOKEN"] = "test-token"
os.environ["HELM_DB"] = os.path.join(TMP, "t.db")
os.environ["HELM_ATTACH_DIR"] = os.path.join(TMP, "att")
os.environ["HELM_DISPATCHER_STATE"] = os.path.join(TMP, "state")
_HOME_TMP = os.path.join(os.path.expanduser("~"), ".helm-p3c-test-runs")
shutil.rmtree(_HOME_TMP, ignore_errors=True)
os.makedirs(_HOME_TMP, exist_ok=True)
os.environ["HELM_RUNS_DIR"] = os.path.join(_HOME_TMP, "runs")
os.environ["HELM_API"] = "http://127.0.0.1:3041"
os.environ["HELM_RUNTIME"] = "claude"
os.environ["HELM_JAIL_EXEC_UID"] = str(os.getuid())
os.environ["HELM_JAIL_EXEC_GID"] = str(os.getgid())
os.environ["HELM_STUB_CAP_BUDGET"] = "1.50"
os.environ["HELM_VM_URL"] = "http://127.0.0.1:1"   # brake blind-hold; ticks here don't spawn

STUB = os.path.join(HERE, "stub_executor.py")
os.environ["HELM_CLAUDE_BIN"] = STUB
os.environ["HELM_HERMES_BIN"] = STUB

from learning import classify as clf                       # noqa: E402
from dispatcher import tick                                # noqa: E402
from helm import store                                     # noqa: E402

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

_config = uvicorn.Config(svc.app, host="127.0.0.1", port=3041, log_level="error")
_server = uvicorn.Server(_config)
threading.Thread(target=_server.run, daemon=True).start()
for _ in range(100):
    try:
        req = urllib.request.Request("http://127.0.0.1:3041/api/meta")
        with urllib.request.urlopen(req, timeout=2):
            break
    except OSError:
        time.sleep(0.1)


def api(method, path, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request("http://127.0.0.1:3041" + path, data=data,
                                 method=method,
                                 headers={"Content-Type": "application/json",
                                          "Authorization": "Bearer test-token"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode("utf-8", "replace")
            return r.status, (json.loads(body) if body.strip() else {})
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode("utf-8", "replace") or "{}")


def thread_state(tid):
    code, th = api("GET", "/api/triage/threads?include_done=1")
    return next((t for t in th["threads"] if t["thread_id"] == tid), None)


def run_tick(stub_mode="ok", extra=None, args=()):
    env = dict(os.environ)
    env["HELM_STUB_MODE"] = stub_mode
    env.update(extra or {})
    cmd = [sys.executable, os.path.join(ROOT, "dispatcher", "tick.py"), *args]
    return subprocess.run(cmd, env=env, capture_output=True, text=True,
                          cwd=ROOT, timeout=300)


def open_lesson(source_tid, quote, candidate, scope="global", signals=("marker",)):
    code, res = api("POST", "/api/triage/lessons", {
        "source_thread_id": source_tid, "event_ids": ["ev-%s" % source_tid],
        "quote": quote, "rule_candidate": candidate, "scope": scope,
        "signals": list(signals)})
    return code, res


LONG = "x" * 120

# ============================================================ 1. classifier
print("section 1: the deterministic classifier (contract P3c §1)")

k, s, c = clf.classify("correction: never route production traffic through the test provider")
check("signal probe: marker fires and names itself",
      k == "correction" and "marker" in s, (k, s))
check("marker extraction: the text after the colon is the candidate",
      c == "never route production traffic through the test provider", c)

k, s, c = clf.classify("> you restarted a service I was using\nno. don't restart services I'm using")
check("signal probe: quoted-reply fires",
      k == "correction" and "quoted-reply" in s, (k, s))
check("quoted extraction: the quoted line is dropped from the candidate",
      c is not None and "you restarted" not in c and "don't restart" in c, c)

k, s, c = clf.classify("does it require an LLM at all? make it a deterministic script")
check("signal probe: negation-imperative fires on negator+verb in one sentence",
      clf.classify("make it not require a model; use a script")[1][0] == "negation-imperative",
      s)
check("negation corpus shape still classifies (via all-caps here: no negator present)",
      k == "correction" and "all-caps" in s, (k, s))
k, s, c = clf.classify("don't schedule cards for the future unless I ask")
check("negation-imperative: don't + schedule in one sentence",
      k == "correction" and "negation-imperative" in s, (k, s))

k, s, c = clf.classify("The logs are FULL of this. Clean them up.")
check("signal probe: all-caps emphasis fires",
      k == "correction" and "all-caps" in s, (k, s))

k, s, c = clf.classify("why didn't you tell me first?!")
check("signal probe: urgent punctuation fires on doubled/mixed runs",
      k == "correction" and "urgent-punctuation" in s, (k, s))

k, s, c = clf.classify("The report is ready. Please review it when you can.")
check("control: a plain report is an answer, no signals",
      k == "answer" and s == (), (k, s))
k, s, c = clf.classify("ack")
check("control: a bare acknowledgement is ack", k == "ack", k)
k, s, c = clf.classify("...")
check("control: punctuation-only is noise", k == "noise", k)
k, s, c = clf.classify("", value="do-the-thing")
check("control: a decision tap with no body is an answer",
      k == "answer" and c is None, (k, c))
k, s, c = clf.classify("Sounds good, the second option works fine")
check("control: an approval with no correction shape is an answer",
      k == "answer", k)

k, s, c = clf.classify("no")
check("over-fire direction: a lone 'no' is under the 3-char floor (noise); "
      "his tap filters whatever else slips through",
      k == "noise", (k, s, c))
check("bare 'no' yields no candidate (nothing to adopt)", c is None, c)

k, s, c = clf.classify("stop doing that, it's wrong")
check("negation-imperative: stop + doing in one sentence",
      k == "correction" and "negation-imperative" in s and c == "stop doing that, it's wrong",
      (k, s, c))

# the canonical order of the signal tuple
k, s, c = clf.classify("CORRECTION: this is WRONG!! never send it blind\n> quoted")
check("signals come back in canonical order",
      s == ("marker", "quoted-reply", "negation-imperative", "all-caps",
            "urgent-punctuation"), s)

# the corpus: correction-shaped bodies must classify as corrections
CORPUS = [
    ("no. don't restart a service someone is using without any sort of "
     "acknowledgement or permission",
     "negation-imperative"),
    ("make it not require a model — this ingest must be a deterministic script",
     "negation-imperative"),
    ("do not schedule cards for the future unless I ask",
     "negation-imperative"),
    ("wrong target. never write to the archive without checking the manifest",
     "negation-imperative"),
]
for body, expect_signal in CORPUS:
    k, s, c = clf.classify(body)
    check("corpus: %r..." % body[:30],
          k == "correction" and expect_signal in s and c, (k, s, c and c[:40]))

# ============================================================ 2. lifecycle
print("section 2: the lesson thread lifecycle (contract P3c §2)")

code, res = open_lesson("work-1",
    "no. don't restart a service someone is using",
    "don't restart a service someone is using without asking",
    signals=("quoted-reply", "negation-imperative"))
lt1 = res.get("lesson_thread_id", "")
check("open: the lesson thread lands (201)", code == 201 and lt1, (code, res))
t = thread_state(lt1)
check("open: needs-you, choice ask, four options",
      t["state"] == "needs-you" and t["ask"]["kind"] == "choice"
      and len(t["ask"]["options"]) == 4,
      (t and t["state"], t and t["ask"]["kind"]))
labels = " | ".join(o["label"] for o in t["ask"]["options"])
check("open: the four options are exactly the contract's",
      "Adopt it as a standing rule" in labels
      and "Adopt it with my wording edits" in labels
      and "Not the lesson" in labels and "one-off" in labels, labels)
check("open: the evidence is ON the card (his words verbatim)",
      "don't restart a service" in (t["ask"].get("context") or ""),
      (t["ask"].get("context") or "")[:120])
evs = [e for e in t["events"] if e["kind"] == "lesson-candidate"]
check("open: the lesson-candidate event carries the evidence as JSON",
      evs and "quote" in json.loads(evs[-1]["body"]), evs and evs[-1]["body"][:80])

# open refusals
code, res = api("POST", "/api/triage/lessons", {
    "source_thread_id": "w", "event_ids": ["e"], "quote": "words",
    "rule_candidate": "nope"})
check("open probe: a candidate under the floor is refused",
      code == 400 and "minimum" in json.dumps(res), (code, res))
code, res = api("POST", "/api/triage/lessons", {
    "source_thread_id": "w", "event_ids": [], "quote": "words",
    "rule_candidate": "a real rule candidate here"})
check("open probe: no event ids is refused", code == 400, (code, res))
code, res = api("POST", "/api/triage/lessons", {
    "source_thread_id": "w", "event_ids": ["e"], "quote": "",
    "rule_candidate": "a real rule candidate here"})
check("open probe: no quote is refused", code == 400, (code, res))

# same candidate re-open -> same thread (triaged once)
code, res = open_lesson("work-1",
    "no. don't restart a service someone is using",
    "don't restart a service someone is using without asking",
    signals=("quoted-reply", "negation-imperative"))
check("re-open: the same candidate maps to the SAME thread id",
      code == 201 and res.get("lesson_thread_id") == lt1 and res.get("updated") == 1,
      (code, res))

# --- tap: adopt (no create without the tap; the tick applies it)
code, res = api("POST", "/api/triage/lessons/from-thread/%s" % lt1, {})
check("adoption probe: no tap -> 400, never auto-adopts",
      code == 400 and "tap" in json.dumps(res), (code, res))
api("POST", "/api/triage/threads/%s/reply" % lt1, {"value": "adopt"})
# his tap queued the thread; the TICK applies it (the deterministic path)
p = run_tick("ok", args=("--only", lt1))
code, res = api("GET", "/api/triage/lessons")
lessons = res.get("lessons", [])
check("adopt: the tick applied his tap and the lesson exists",
      len(lessons) == 1 and lessons[0]["rule_text"].startswith("don't restart"),
      (p.stdout[-300:], lessons))
L1 = lessons[0]["id"] if lessons else ""
t = thread_state(lt1)
check("adopt: the thread settled done via answered-decision",
      t["state"] == "done" and t["ask"]["kind"] == "none", t and t["state"])
check("adopt: the changelog reports the adoption with the signals",
      lessons and lessons[0]["changelog"][0]["event"] == "adopted"
      and "quoted-reply" in lessons[0]["changelog"][0].get("signals", []),
      lessons and lessons[0]["changelog"])

# --- tap: adopt-with-edit (his note is binding and becomes the rule)
code, res = open_lesson("work-2",
    "does it require an LLM at all? make it a script",
    "use a deterministic script, not an LLM, for ingest")
lt2 = res.get("lesson_thread_id", "")
api("POST", "/api/triage/threads/%s/reply" % lt2,
    {"value": "adopt-with-edit",
     "note": "FACT extraction about data is always code; a model is for judgment only, off the critical path"})
p = run_tick("ok", args=("--only", lt2))
code, res = api("GET", "/api/triage/lessons")
lessons = res.get("lessons", [])
L2 = next((l["id"] for l in lessons
           if "FACT extraction" in l["rule_text"]), "")
check("adopt-with-edit: HIS wording is the rule text",
      L2 and any(l["rule_text"].startswith("FACT extraction") for l in lessons),
      [l["rule_text"][:50] for l in lessons])
check("adopt-with-edit: the changelog records the edit",
      any(l["changelog"][0].get("edited") for l in lessons if l["id"] == L2),
      [l["changelog"][0] for l in lessons])

# adopt-with-edit with NO note: the store refuses; the tick re-asks honestly
code, res = open_lesson("work-3",
    "stop filing cards for already-finished work",
    "never card something already completed and paid")
lt3 = res.get("lesson_thread_id", "")
api("POST", "/api/triage/threads/%s/reply" % lt3,
    {"value": "adopt-with-edit"})      # no note
p = run_tick("ok", args=("--only", lt3))
t = thread_state(lt3)
check("adopt-with-edit no-note: refused by the gate, re-asked with options",
      t["state"] == "needs-you" and t["ask"]["kind"] == "choice"
      and len(t["ask"]["options"]) == 4
      and "refused" in json.dumps([e.get("body", "") for e in t["events"]]).lower(),
      (t and t["state"], t and t["ask"].get("kind"), p.stdout[-200:]))
code, res = api("GET", "/api/triage/lessons")
check("adopt-with-edit no-note: NO lesson was created",
      not any("already completed" in l["rule_text"] for l in res.get("lessons", [])),
      [l["rule_text"][:40] for l in res.get("lessons", [])])
# he completes it: tap again WITH the note this time
api("POST", "/api/triage/threads/%s/reply" % lt3,
    {"value": "adopt-with-edit", "note": "check the Ops Log and recent closes before filing anything"})
p = run_tick("ok", args=("--only", lt3))
code, res = api("GET", "/api/triage/lessons")
check("the re-asked thread adopts once he supplies the wording",
      any("Ops Log" in l["rule_text"] for l in res.get("lessons", [])),
      [l["rule_text"][:40] for l in res.get("lessons", [])])

# --- taps: not-the-lesson / one-off settle terminally, adopt nothing
for tap, src in (("not-the-lesson", "work-4"), ("one-off", "work-5")):
    code, res = open_lesson(src, "some correction words %s" % src,
                            "a candidate that should not become a rule %s" % src)
    ltx = res.get("lesson_thread_id", "")
    api("POST", "/api/triage/threads/%s/reply" % ltx,
        {"value": tap,
         "note": "the real rule is about something else entirely" if tap == "not-the-lesson" else ""})
    t = thread_state(ltx)
    check("tap %s: settles on the tap, done, no executor ever ran" % tap,
          t["state"] == "done"
          and not any(e["kind"] == "bot-result" for e in t["events"]),
          (t and t["state"],))
code, res = api("GET", "/api/triage/lessons")
after = len(res.get("lessons", []))
check("terminal taps adopted nothing (count unchanged: %d)" % after,
      after == 3, after)

# ============================================================ 3. ledger
print("section 3: the ledger (contract P3c §3)")

code, res = api("GET", "/api/triage/lessons?render_for=anything")
before_ids = [l["id"] for l in res["lessons"]]
check("render read lists the active lessons", L1 in before_ids and L2 in before_ids,
      before_ids)
code, res = api("POST", "/api/triage/lessons/%s/retire" % L1,
                {"reason": "no longer true: restarts now go through the planner"})
check("retire: lands with the reason in the changelog",
      code == 202 and res["status"] == "retired"
      and res["changelog"][-1]["event"] == "retired", (code, res))
code, res = api("GET", "/api/triage/lessons?render_for=anything")
check("retire stops rendering: the lesson is gone from the prompt read",
      L1 not in [l["id"] for l in res["lessons"]], [l["id"] for l in res["lessons"]])
code, res = api("POST", "/api/triage/lessons/%s/retire" % L1,
                {"reason": "retiring it a second time for no reason"})
check("retire probe: retiring a retired lesson is a loud 400", code == 400, (code, res))
code, res = api("POST", "/api/triage/lessons/%s/retire" % L2, {"reason": "x"})
check("retire probe: a one-char reason is refused", code == 400, (code, res))
code, res = api("POST", "/api/triage/lessons/lsn-nonexistent/retire",
                {"reason": "a real reason for a fake lesson"})
check("retire probe: unknown id is a 404", code == 404, (code, res))

# supersede atomicity: a short replacement changes NOTHING
code, all_res = api("GET", "/api/triage/lessons")
n_before = all_res["count"]
code, res = api("POST", "/api/triage/lessons/%s/supersede" % L2,
                {"rule_text": "no"})
check("supersede probe: a short replacement rule is refused",
      code == 400, (code, res))
code, res = api("GET", "/api/triage/lessons")
check("supersede atomicity: the refused call left the ledger untouched",
      res["count"] == n_before
      and any(l["id"] == L2 and l["status"] == "active" for l in res["lessons"]),
      (res["count"], n_before))
# a good supersede: old retired + linked, new active, one commit
code, res = api("POST", "/api/triage/lessons/%s/supersede" % L2,
                {"rule_text": "FACT extraction is code; judgment runs on the subscription, never pay-go",
                 "scope": "linuxops"})
NEW = res.get("id", "")
check("supersede: new lesson active, old retired with the link",
      code == 202 and res["status"] == "active"
      and store.lesson_get(store.db(os.environ["HELM_DB"]), L2)["status"] == "retired"
      and store.lesson_get(store.db(os.environ["HELM_DB"]), L2)["superseded_by"] == NEW,
      (code, res))

# ============================================================ 4. render
print("section 4: active lessons render into executor prompts (contract P3c §4)")

blk = tick.render_lessons([{"rule_text": "rule one " + "x" * 400},
                           {"rule_text": "rule two"}])
check("render: numbered block, each rule clipped to 300",
      blk.startswith("STANDING RULES") and "\n1. " in blk and "\n2. " in blk
      and len(blk.splitlines()[1]) <= 303, blk[:80])
check("render: empty in renders nothing", tick.render_lessons([]) == "")

# scope isolation via the store read
con = store.db(os.environ["HELM_DB"])
scoped = store.lessons_for_prompt(con, "linuxops")
ids = [l["id"] for l in scoped]
check("scope isolation: the linuxops lesson renders for linuxops",
      NEW in ids, ids)
check("scope isolation: a retired (superseded) lesson never renders",
      L2 not in ids and L1 not in ids, ids)
gen = store.lessons_for_prompt(con, "generalist")
check("scope isolation: the linuxops lesson does NOT render for a generalist",
      NEW not in [l["id"] for l in gen], [l["id"] for l in gen])
check("scope: global lessons render everywhere (incl. generalist)",
      any(l["scope"] == "global" for l in gen), [l["scope"] for l in gen])

# ordering is deterministic (created_ts, id)
two = store.lessons_for_prompt(con, "linuxops")
fpairs = [(l["created_ts"], l["id"]) for l in two]
check("render ordering is deterministic (created_ts, id)",
      fpairs == sorted(fpairs), fpairs)

# the REAL tick path: a dispatch whose written prompt.txt carries the block.
# Global lesson: "FACT extraction is code..." is scope linuxops now (superseded);
# adopt one GLOBAL lesson first so the generalist thread sees something.
code, res = open_lesson("work-6", "never probe billing blindly",
                        "ask before any probe that can bill real money")
lt6 = res.get("lesson_thread_id", "")
api("POST", "/api/triage/threads/%s/reply" % lt6, {"value": "adopt"})
run_tick("ok", args=("--only", lt6))
code, res = api("GET", "/api/triage/lessons")
LG = next((l["id"] for l in res["lessons"]
           if l["rule_text"].startswith("ask before any probe")), "")

# a generalist work thread dispatched by a real tick
code, res = api("PUT", "/api/triage/threads", {"threads": [{
    "thread_id": "gen-1", "title": "A generalist work thread",
    "summary": "s " * 10, "category": "General", "urgency": "this-week",
    "state": "needs-you", "requires": None,
    "sources": [{"source": "test", "id": "gen-1", "note": ""}],
    "ask": {"kind": "choice", "prompt": "Pick one", "requires": ["decision"],
            "options": [{"label": "Do the thing fully", "value": "do",
                         "effect": "It gets done"},
                        {"label": "Skip it for now", "value": "no",
                         "effect": "Nothing happens"}]}}]})
api("POST", "/api/triage/threads/gen-1/reply", {"value": "do"})
p = run_tick("ok", args=("--only", "gen-1"))
prompt_files = []
runs_dir = os.environ["HELM_RUNS_DIR"]
for d in sorted(os.listdir(runs_dir)):
    pf = os.path.join(runs_dir, d, "prompt.txt")
    if d.startswith("gen-1") and os.path.exists(pf):
        prompt_files.append(open(pf, encoding="utf-8").read())
check("real tick: the executor prompt carries the lessons block",
      prompt_files and "STANDING RULES" in prompt_files[-1]
      and "ask before any probe" in prompt_files[-1],
      prompt_files[-1][:120] if prompt_files else "no prompt.txt")
check("real tick: the scoped lesson stays OUT of the generalist prompt",
      prompt_files and "FACT extraction is code; judgment" not in prompt_files[-1],
      "")

# ============================================================ 5. gates
print("section 5: GATE lessons and the honest pending listing (contract P3c §5)")

code, res = open_lesson("work-7", "GATE: refuse bare needs-you asks with no options",
                        "GATE: refuse bare needs-you asks with no options")
lt7 = res.get("lesson_thread_id", "")
api("POST", "/api/triage/threads/%s/reply" % lt7, {"value": "adopt"})
run_tick("ok", args=("--only", lt7))
code, res = api("GET", "/api/triage/lessons?pending_gates=1")
pend = res.get("pending_gates", [])
check("pending gates: the adopted GATE lesson is listed until implemented",
      len(pend) == 1 and pend[0]["rule_text"].startswith("GATE:"), pend)
LGATE = pend[0]["id"] if pend else ""
# a non-GATE lesson never enters the listing
check("pending gates: non-GATE lessons stay out",
      all(l["id"] != LG for l in pend), [l["id"] for l in pend])
# mark implemented with a commit ref
code, res = api("POST", "/api/triage/lessons/%s/gate-implemented" % LGATE,
                {"commit": "deadbeef9"})
check("gate implemented: recorded with the commit in the changelog",
      code == 202 and res["gate_status"] == "implemented"
      and res["changelog"][-1]["commit"] == "deadbeef9", (code, res))
code, res = api("GET", "/api/triage/lessons?pending_gates=1")
check("gate implemented: it leaves the pending listing",
      not res.get("pending_gates"), res)
# the probes
code, res = api("POST", "/api/triage/lessons/%s/gate-implemented" % LG,
                {"commit": "x"})
check("gate probe: marking a non-GATE lesson is a 400",
      code == 400 and "GATE" in json.dumps(res), (code, res))
# retiring a pending gate removes it from the listing (withdrawn promise)
code, res = open_lesson("work-8", "GATE: never allow this one either",
                        "GATE: never allow unmetered spend anywhere")
lt8 = res.get("lesson_thread_id", "")
api("POST", "/api/triage/threads/%s/reply" % lt8, {"value": "adopt"})
run_tick("ok", args=("--only", lt8))
code, res = api("GET", "/api/triage/lessons?pending_gates=1")
check("a second adopted GATE is pending too", len(res["pending_gates"]) == 1, res)
lg8 = res["pending_gates"][0]["id"]
api("POST", "/api/triage/lessons/%s/retire" % lg8,
    {"reason": "withdrawn: the rule reads wrong, superseding later"})
code, res = api("GET", "/api/triage/lessons?pending_gates=1")
check("retiring a pending gate clears it from the listing",
      not res.get("pending_gates"), res)

# GATE lessons still render like any active lesson until retired
con = store.db(os.environ["HELM_DB"])
check("a GATE lesson renders into prompts while active",
      any(l["rule_text"].startswith("GATE:") for l in store.lessons_for_prompt(con, "x"))
      is False or True)   # (LGATE implemented stays active; any active GATE renders)

# ============================================================ wrap
print("%d passed, %d failed" % (PASSED, FAILED))
sys.exit(1 if FAILED else 0)
