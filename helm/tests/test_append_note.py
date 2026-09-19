"""Safe note-append route: state never flips, history never wiped, no dupes/drops.

Run: python3 tests/test_append_note.py   (from the repo root)

Slice A (2026-09-15): POST /api/triage/threads/{id}/note must append one
kind='note' event plus the last_note mirror and touch nothing else, on any
state -- especially parked/waiting-external. Offline only: throwaway DBs,
no network listener (API level via raw ASGI TestClient like test_api.py).
"""
from __future__ import annotations

import hashlib
import os
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helm import store  # noqa: E402

PASS = FAIL = 0
LONG = "The run did the work and this is the long-enough report about it. " + "y" * 40


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("  ok   " + name)
    else:
        FAIL += 1
        print("  FAIL " + name + ("  <- " + str(detail) if detail else ""))


def status_of(fn, *a, **kw):
    try:
        fn(*a, **kw)
        return 202
    except store.StoreError as e:
        return e.status


_DIG = {"0": "zero", "1": "one", "2": "two", "3": "three", "4": "four",
        "5": "five", "6": "six", "7": "seven", "8": "eight", "9": "nine"}


def parked_thread(con, tid):
    """A parked waiting-external thread, built the honest way (answer then park)."""
    ask = {"kind": "choice", "prompt": "Chase it, or leave it?",
           "requires": ["decision"],
           "options": [{"label": "Chase the vendor", "value": "chase",
                        "effect": "I chase them and report back"}]}
    store.put_threads(con, {"threads": [{
        "thread_id": tid, "title": "concern " + "".join(_DIG.get(c, c) for c in tid),
        "summary": "a summary", "category": "Fleet", "urgency": "this-week",
        "state": "needs-you", "ask": ask}]})
    store.reply(con, tid, "chase")
    out = store.post_result(con, tid, {"outcome": "parked", "state": "waiting-external",
                                       "note": LONG, "blocked_on": "vendor case 4242"})
    assert out.get("state", "waiting-external") == "waiting-external", out
    store.set_parked(con, tid, True,
                     reason="Waiting on vendor case 4242 with a long enough reason text " + "z" * 40,
                     source="test")
    return tid


def row_snapshot(con, tid):
    r = con.execute("SELECT * FROM threads WHERE thread_id=?", (tid,)).fetchone()
    return dict(r)


def events_snapshot(con, tid):
    return [tuple(e) for e in con.execute(
        "SELECT ts, source, source_id, kind, body FROM events"
        " WHERE thread_id=? ORDER BY ts ASC", (tid,))]


def state_hash(d):
    skip = {"last_note"}
    items = sorted((k, v) for k, v in d.items() if k not in skip)
    return hashlib.sha256(repr(items).encode()).hexdigest()[:16]


def run():
    tmp = os.path.join(tempfile.mkdtemp(prefix="helm-append-"), "t.db")
    con = store.db(tmp)

    print("\nappend to a parked/waiting-external thread preserves state")
    parked_thread(con, "n-park-1")
    before = row_snapshot(con, "n-park-1")
    assert before["state"] == "waiting-external", before["state"]
    ev_before = events_snapshot(con, "n-park-1")
    h_before = state_hash(before)
    out = store.append_note(con, "n-park-1", "Progress ping: vendor acknowledged case 4242, "
                                             "ETA Thursday; no action needed from Joey.",
                            actor="test", dedupe_key="ping-1")
    check("append accepted", out.get("accepted") is True, out)
    check("state unchanged", out.get("state") == "waiting-external", out)
    after = row_snapshot(con, "n-park-1")
    check("thread row identical except last_note",
          state_hash(after) == h_before, "hash drift")
    for col in ("state", "ask_kind", "ask_prompt", "ask_options", "ask_context",
                "blocked_on", "recheck_at", "gate_cmd", "parked_since", "parked",
                "parked_reason", "wake_at", "wake_note", "wake_ask",
                "dispatched_at", "dispatch_attempts", "last_dispatch_try",
                "last_change", "tombstoned", "suppressed", "responder",
                "urgency", "category", "title", "summary", "sources", "links", "label"):
        check("col %s preserved" % col, after[col] == before[col],
              "%r -> %r" % (before[col], after[col]))
    check("last_note mirrors the note", after["last_note"].startswith("Progress ping"), after["last_note"][:40])

    print("\nappend preserves full history")
    ev_after = events_snapshot(con, "n-park-1")
    check("exactly one row added", len(ev_after) == len(ev_before) + 1,
          "%d -> %d" % (len(ev_before), len(ev_after)))
    check("prior events byte-identical", ev_after[:len(ev_before)] == ev_before)
    check("new row is kind note", ev_after[-1][3] == "note" and "Progress ping" in ev_after[-1][4],
          ev_after[-1][3:])

    print("\nappend on needs-you and done threads preserves their states")
    store.put_threads(con, {"threads": [{
        "thread_id": "n-needs-1", "title": "concern needs one",
        "summary": "a summary", "category": "Fleet", "urgency": "now",
        "state": "needs-you",
        "ask": {"kind": "ack", "prompt": "Read this report", "requires": []}}]})
    b = row_snapshot(con, "n-needs-1")
    store.append_note(con, "n-needs-1", "FYI note on a lit thread with sufficient length " + "q" * 20)
    a = row_snapshot(con, "n-needs-1")
    check("needs-you ask_kind kept", a["ask_kind"] == b["ask_kind"] == "ack")
    check("needs-you state kept", a["state"] == "needs-you")
    check("needs-you row hash kept", state_hash(a) == state_hash(b))

    print("\nvalidation gates")
    check("empty note 400", status_of(store.append_note, con, "n-park-1", "   ") == 400)
    check("non-string note 400", status_of(store.append_note, con, "n-park-1", 123) == 400)
    check("oversize note 400", status_of(store.append_note, con, "n-park-1", "x" * 8001) == 400)
    check("unknown thread 404", status_of(store.append_note, con, "no-such", "hello world") == 404)

    print("\nrepeated appends with the same dedupe_key do not duplicate")
    c0 = con.execute("SELECT COUNT(*) FROM events WHERE thread_id=?", ("n-park-1",)).fetchone()[0]
    r1 = store.append_note(con, "n-park-1", "Same payload sent twice with a stable key " + "w" * 20,
                           dedupe_key="stable-9")
    r2 = store.append_note(con, "n-park-1", "Same payload sent twice with a stable key " + "w" * 20,
                           dedupe_key="stable-9")
    c1 = con.execute("SELECT COUNT(*) FROM events WHERE thread_id=?", ("n-park-1",)).fetchone()[0]
    check("first append adds one", c1 == c0 + 1, "%d -> %d" % (c0, c1))
    check("repeat deduped", r2.get("deduped") is True, r2)
    c2 = con.execute("SELECT COUNT(*) FROM events WHERE thread_id=?", ("n-park-1",)).fetchone()[0]
    check("repeat adds nothing", c2 == c1, "%d -> %d" % (c1, c2))
    check("first was not deduped", r1.get("deduped") is False, r1)

    print("\nconcurrent distinct appends neither duplicate nor drop")
    con.commit()
    db_path = tmp
    bodies = ["concurrent note %d " % i + "v" * 30 for i in range(8)]
    errors = []

    def worker(i):
        try:
            c = store.db(db_path)
            try:
                store.append_note(c, "n-park-1", bodies[i], actor="w%d" % i,
                                  dedupe_key="conc-%d" % i)
            finally:
                c.close()
        except Exception as e:  # noqa: BLE001
            errors.append(repr(e))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    check("no worker errors", not errors, errors[:2])
    got = [e[4] for e in events_snapshot(con, "n-park-1") if e[2].startswith("conc-")]
    check("all 8 concurrent rows present", len(got) == 8, len(got))
    check("all bodies distinct and intact", sorted(got) == sorted(bodies))

    print("\nAPI level: POST /note on a waiting-external thread, no dispatch re-arm")
    os.environ["HELM_APP_TOKEN"] = "test-token"
    api_tmp = tempfile.mkdtemp(prefix="helm-append-api-")
    os.environ["HELM_DB"] = os.path.join(api_tmp, "t.db")
    os.environ["HELM_ATTACH_DIR"] = os.path.join(api_tmp, "att")
    from starlette.testclient import TestClient  # noqa: E402
    import app.service as svc  # noqa: E402
    client = TestClient(svc.app)
    AUTH = {"Authorization": "Bearer test-token"}
    r = client.post("/api/triage/threads/create", json={
        "title": "Append demo scratch", "summary": "scratch", "category": "Test",
        "urgency": "whenever", "intent": "waiting",
        "blocked_on": "external vendor queue 7"}, headers=AUTH)
    check("scratch created waiting", r.status_code == 201, r.text[:200])
    tid = r.json().get("thread_id")
    g0 = client.get("/api/triage/threads", headers=AUTH).json()
    t0 = [t for t in g0["threads"] if t["thread_id"] == tid][0]
    h0 = hashlib.sha256(repr(sorted((k, v) for k, v in t0.items()
                                    if k not in ("last_note", "events"))).encode()).hexdigest()[:16]
    n0 = len(t0.get("events", []))
    p0 = client.get("/api/triage/pending-dispatch", headers=AUTH).json()["count"]
    r = client.post("/api/triage/threads/%s/note" % tid, json={
        "note": "Pump backoff armed at 15m; next sweep will skip this mirror quietly " + "k" * 10,
        "actor": "test", "dedupe_key": "api-1"}, headers=AUTH)
    check("POST /note 202", r.status_code == 202, r.text[:200])
    check("response state waiting-external", r.json().get("state") == "waiting-external", r.text[:200])
    g1 = client.get("/api/triage/threads", headers=AUTH).json()
    t1 = [t for t in g1["threads"] if t["thread_id"] == tid][0]
    h1 = hashlib.sha256(repr(sorted((k, v) for k, v in t1.items()
                                    if k not in ("last_note", "events"))).encode()).hexdigest()[:16]
    check("GET state identical", h1 == h0, "%s vs %s" % (h0, h1))
    check("GET history +1", len(t1.get("events", [])) == n0 + 1,
          "%d -> %d" % (n0, len(t1.get("events", []))))
    p1 = client.get("/api/triage/pending-dispatch", headers=AUTH).json()["count"]
    check("pending-dispatch unmoved (no re-arm)", p1 == p0, "%r -> %r" % (p0, p1))
    r = client.post("/api/triage/threads/%s/note" % tid, json={
        "note": "Pump backoff armed at 15m; next sweep will skip this mirror quietly " + "k" * 10,
        "dedupe_key": "api-1"}, headers=AUTH)
    check("API repeat deduped", r.status_code == 202 and r.json().get("deduped") is True,
          r.text[:200])
    r = client.post("/api/triage/threads/%s/note" % tid, json={"note": "  "}, headers=AUTH)
    check("API empty note 400", r.status_code == 400, r.text[:200])
    r = client.post("/api/triage/threads/no-such-thread/note", json={"note": "hello world"},
                    headers=AUTH)
    check("API unknown thread 404", r.status_code == 404, r.text[:200])

    print("\n%d passed, %d failed" % (PASS, FAIL))


if __name__ == "__main__":
    run()
    raise SystemExit(1 if FAIL else 0)
