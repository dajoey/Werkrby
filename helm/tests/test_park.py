"""Park contract: nothing waits on nobody, and nothing waits forever.

Run: python3 tests/test_park.py   (from the repo root)

Contract: docs/contracts/P1.md §1. Lessons 4 (every parked state carries a
machine-checkable exit), 5 (rejected loudly at creation), 6 (a snooze is its own exit),
10 (a card must never lie about its state).

The failure these cases are written from: `waiting-external` was a hole in the state
machine. Neither queue could see a parked thread, so it was invisible to every agent
while rendering to the owner as "waiting on someone else" with no button to tap. Bots
reached it honestly — forbidden to close, rightly blocked from a lit card with no ask —
and wrote notes promising a final tap that the state never offers.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
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


def status(fn, *a, **kw):
    """Run a store call and report the HTTP status it would produce: 202 if it was
    accepted, or the status carried by the rejection."""
    try:
        fn(*a, **kw)
        return 202
    except store.StoreError as e:
        return e.status


def detail_of(fn, *a, **kw):
    try:
        fn(*a, **kw)
        return ""
    except store.StoreError as e:
        return e.detail


_DIG = {"0": "zero", "1": "one", "2": "two", "3": "three", "4": "four",
        "5": "five", "6": "six", "7": "seven", "8": "eight", "9": "nine"}


def armed(con, tid, ask=None):
    """A thread with an answer on it, i.e. one an executor could be reporting about."""
    ask = ask or {"kind": "choice", "prompt": "Chase it, or leave it?",
                  "options": [{"label": "Chase the vendor", "value": "chase",
                               "effect": "I chase them and report back"}]}
    # P3b §1.5: the ask carries its T1 marker so the four-test admits it.
    # P3b §1.5: T1 marker on the ask + a dedupe-distinct title (T3
    # fingerprints titles; a shared default made every thread a duplicate).
    ask = dict(ask, requires=ask.get("requires", ["decision"]))
    store.put_threads(con, {"threads": [{
        "thread_id": tid, "title": "concern " + "".join(_DIG.get(c, c) for c in tid),
        "summary": "a summary",
        "category": "Fleet", "urgency": "this-week", "state": "needs-you", "ask": ask}]})
    store.reply(con, tid, "chase")
    return tid


def run():
    tmp = os.path.join(tempfile.mkdtemp(prefix="helm-park-"), "t.db")
    con = store.db(tmp)
    now = time.time()

    print("\na park must name who or what it waits on")
    armed(con, "p-1")

    def park(**kw):
        body = {"outcome": "parked", "state": "waiting-external", "note": LONG}
        body.update(kw)
        return lambda: store.post_result(con, "p-1", body)

    check("no blocked_on at all is refused", status(park()) == 400)
    check("and the refusal says which field is missing",
          "blocked_on" in detail_of(park()), detail_of(park()))
    check("a token blocked_on is refused", status(park(blocked_on="vendor")) == 400)
    check("and says how long it has to be",
          "8" in detail_of(park(blocked_on="vendor")), detail_of(park(blocked_on="vendor")))
    check("a park aimed at Joey is refused", status(park(blocked_on="Joey")) == 400)
    check("as is one aimed at 'the user'", status(park(blocked_on="the user")) == 400)
    check("and it names the state to use instead",
          "needs-you" in detail_of(park(blocked_on="   joey!  ")),
          detail_of(park(blocked_on="   joey!  ")))

    print("\na park must say when we look again, and 30 days is the ceiling")
    check("45 days out is refused",
          status(park(blocked_on="Acme support, case 4821",
                      recheck_at=now + 45 * 86400)) == 400)
    check("and says what the ceiling is",
          "30" in detail_of(park(blocked_on="Acme support, case 4821",
                                 recheck_at=now + 45 * 86400)))
    check("a garbage recheck_at is refused, not silently defaulted",
          status(park(blocked_on="Acme support, case 4821",
                      recheck_at="next tuesday-ish")) == 400)
    check("a boolean recheck_at is refused",
          status(park(blocked_on="Acme support, case 4821", recheck_at=True)) == 400)

    print("\na park that names a real blocker lands, and defaults its own recheck")
    out = store.post_result(con, "p-1", {
        "outcome": "parked", "state": "waiting-external", "note": LONG,
        "blocked_on": "Acme support, case 4821"})
    check("accepted", out["accepted"] is True, out)
    check("the response reports the exit it was given",
          out.get("blocked_on") == "Acme support, case 4821" and out.get("recheck_at"),
          out)
    row = con.execute("SELECT * FROM threads WHERE thread_id='p-1'").fetchone()
    check("blocked_on is stored", row["blocked_on"] == "Acme support, case 4821", dict(row))
    check("recheck defaults to 6 hours out, not never",
          abs(row["recheck_at"] - (time.time() + store.DEFAULT_RECHECK_S)) < 120,
          row["recheck_at"] - time.time())
    check("the park records when it started, so 30 days can be measured",
          row["parked_since"] > 0, dict(row))
    got = next(t for t in store.list_threads(con)["threads"] if t["thread_id"] == "p-1")
    check("the list carries the exit, so a reader can see what it waits on",
          got["blocked_on"] == "Acme support, case 4821" and got["recheck_at"] > 0, got)

    print("\na recheck date in the past is a promise nobody kept: coerced, not honoured")
    armed(con, "p-past")
    store.post_result(con, "p-past", {
        "outcome": "parked", "state": "waiting-external", "note": LONG,
        "blocked_on": "the vendor's shipping ETA", "recheck_at": now - 5000})
    row = con.execute("SELECT recheck_at FROM threads WHERE thread_id='p-past'").fetchone()
    check("pulled forward to the default instead of being left in the past",
          row["recheck_at"] > time.time(), row["recheck_at"])

    print("\nISO-8601 is accepted, because that is what a person or a model writes")
    armed(con, "p-iso")
    iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(now + 5 * 86400))
    store.post_result(con, "p-iso", {
        "outcome": "parked", "state": "waiting-external", "note": LONG,
        "blocked_on": "the vendor contact uploading the cert bundle", "recheck_at": iso})
    row = con.execute("SELECT recheck_at FROM threads WHERE thread_id='p-iso'").fetchone()
    check("parsed to roughly five days out", abs(row["recheck_at"] - (now + 5 * 86400)) < 90,
          row["recheck_at"] - now)

    print("\nthe optional GATE is stored as data, never run by the store")
    armed(con, "p-gate")
    store.post_result(con, "p-gate", {
        "outcome": "parked", "state": "waiting-external", "note": LONG,
        "blocked_on": "the cert to appear on the host",
        "gate": "GATE: test -f /etc/ssl/certs/acme.pem"})
    row = con.execute("SELECT gate_cmd FROM threads WHERE thread_id='p-gate'").fetchone()
    check("the GATE: prefix is stripped and the command kept verbatim",
          row["gate_cmd"] == "test -f /etc/ssl/certs/acme.pem", dict(row))
    src = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "helm", "store.py")).read()
    check("the store imports no shell at all (boundary §6)",
          "import subprocess" not in src and "os.system" not in src
          and "os.popen" not in src)

    def gate(cmd):
        return lambda: store.post_result(con, "p-gate", {
            "outcome": "parked", "state": "waiting-external", "note": LONG,
            "blocked_on": "the cert to appear on the host", "gate": cmd})

    check("a multi-line gate is refused", status(gate("echo one\necho two")) == 400)
    check("an over-long gate is refused", status(gate("x" * 501)) == 400)
    check("an empty gate is refused rather than quietly meaning 'none'",
          status(gate("   ")) == 400)
    check("a non-string gate is refused", status(gate(["test", "-f", "/x"])) == 400)

    print("\na snooze is exempt: wake_at IS its machine-checkable exit")
    armed(con, "p-snooze")
    check("snoozed needs no blocked_on", status(
        store.post_result, con, "p-snooze",
        {"outcome": "deferred", "state": "snoozed", "note": LONG,
         "wake_at": now + 3 * 86400}) == 202)
    row = con.execute("SELECT blocked_on, recheck_at, wake_at FROM threads"
                      " WHERE thread_id='p-snooze'").fetchone()
    check("and carries no park claim either, so the two exits cannot be confused",
          row["blocked_on"] == "" and row["recheck_at"] == 0 and row["wake_at"] > 0,
          dict(row))
    check("store.snooze() is exempt too", status(
        store.snooze, con, "p-snooze", now + 4 * 86400, "later") == 202)

    print("\nleaving the park clears the claim: no stale blocker on a live thread")
    store.reply(con, "p-1", "chase")
    store.post_result(con, "p-1", {
        "outcome": "chased", "state": "needs-you", "note": LONG,
        "ask": {"kind": "ack", "prompt": "Nothing further; read and close."}})
    row = con.execute("SELECT blocked_on, recheck_at, gate_cmd, parked_since FROM threads"
                      " WHERE thread_id='p-1'").fetchone()
    check("blocked_on, recheck_at, gate and parked_since all cleared",
          row["blocked_on"] == "" and row["recheck_at"] == 0
          and row["gate_cmd"] == "" and row["parked_since"] == 0, dict(row))

    print("\nthe producer's door enforces the same contract, thread by thread")
    res = store.put_threads(con, {"threads": [
        {"thread_id": "p-put-bad", "title": "no exit", "state": "waiting-external",
         "ask": {"kind": "none"}},
        {"thread_id": "p-put-ok", "title": "has an exit", "state": "waiting-external",
         "ask": {"kind": "none"}, "blocked_on": "finance approval, PO 4242"},
    ]})
    check("the parked thread with no blocker is rejected", res["rejected_count"] == 1, res)
    check("and the good one in the same payload still lands", res["created"] == 1, res)
    errs = " ".join(r["errors"][0] for r in res.get("rejected", []))
    check("the rejection explains itself to the producer", "blocked_on" in errs, errs)
    row = con.execute("SELECT blocked_on, recheck_at FROM threads"
                      " WHERE thread_id='p-put-ok'").fetchone()
    check("the good one carries its exit", row["blocked_on"] == "finance approval, PO 4242"
          and row["recheck_at"] > time.time(), dict(row))

    print("\na hand-written 'waiting' card is still a wait")
    check("no blocked_on is refused at creation, loudly", status(
        store.create_thread, con,
        {"title": "Waiting on the cert upload", "intent": "waiting"}) == 400)
    r = store.create_thread(con, {"title": "Waiting on the cert upload",
                                  "intent": "waiting",
                                  "blocked_on": "the vendor contact, who has the cert bundle"})
    row = con.execute("SELECT state, blocked_on, recheck_at FROM threads WHERE thread_id=?",
                      (r["thread_id"],)).fetchone()
    check("with one it lands parked, carrying its exit",
          row["state"] == "waiting-external" and row["recheck_at"] > time.time(), dict(row))

    print("\nthe sweeper runs on the system's clock, not the owner's next app-open")
    check("nothing is due yet", store.due_rechecks(con) == [], store.due_rechecks(con))
    con.execute("UPDATE threads SET recheck_at=? WHERE thread_id='p-gate'", (now - 10,))
    con.commit()
    due = store.due_rechecks(con)
    check("the overdue park is due", [d["thread_id"] for d in due] == ["p-gate"], due)
    check("and hands the sweeper the command to run",
          due[0]["gate_cmd"] == "test -f /etc/ssl/certs/acme.pem", due[0])
    check("due_rechecks writes nothing", con.execute(
        "SELECT recheck_at FROM threads WHERE thread_id='p-gate'"
    ).fetchone()["recheck_at"] == now - 10)

    print("\na gate that still fails keeps the thread parked, and says so")
    seen = []

    def gate_fails(thread_id, cmd):
        seen.append((thread_id, cmd))
        return False, "exit 1: /etc/ssl/certs/acme.pem does not exist yet"

    out = store.sweep_rechecks(con, run_gate=gate_fails)
    check("the sweeper ran the gate it was handed, nothing else",
          seen == [("p-gate", "test -f /etc/ssl/certs/acme.pem")], seen)
    check("one thread rechecked", len(out) == 1 and out[0]["outcome"] == "still-blocked", out)
    row = con.execute("SELECT state, recheck_at, recheck_count FROM threads"
                      " WHERE thread_id='p-gate'").fetchone()
    check("still parked", row["state"] == "waiting-external", dict(row))
    check("re-armed for another look rather than left inert",
          row["recheck_at"] > time.time(), row["recheck_at"] - time.time())
    check("and the recheck is counted, so a thread that never clears is visible",
          row["recheck_count"] == 1, dict(row))
    hist = " ".join(e["body"] for e in con.execute(
        "SELECT body FROM events WHERE thread_id='p-gate'"))
    check("the gate's own output is in the history, not just a verdict",
          "does not exist yet" in hist, hist[-200:])

    print("\na gate that passes returns the thread to the work queue")
    con.execute("UPDATE threads SET recheck_at=? WHERE thread_id='p-gate'", (now - 10,))
    con.commit()
    out = store.sweep_rechecks(con, run_gate=lambda t, c: (True, "exit 0: the file is there"))
    check("reported as requeued", out and out[0]["outcome"] == "requeued", out)
    row = con.execute("SELECT state, recheck_at, dispatched_at, dispatch_attempts,"
                      " blocked_on FROM threads WHERE thread_id='p-gate'").fetchone()
    check("back in the work queue", row["state"] == "working", dict(row))
    check("with no recheck left armed", row["recheck_at"] == 0, dict(row))
    check("and a clean dispatch slate so it is offered on the next pass",
          row["dispatched_at"] == 0 and row["dispatch_attempts"] == 0, dict(row))

    print("\nwith no gate at all, the date alone is the exit")
    armed(con, "p-nogate")
    store.post_result(con, "p-nogate", {
        "outcome": "parked", "state": "waiting-external", "note": LONG,
        "blocked_on": "the vendor's next release"})
    con.execute("UPDATE threads SET recheck_at=? WHERE thread_id='p-nogate'", (now - 10,))
    con.commit()
    out = store.sweep_rechecks(con)  # no runner injected at all
    check("it comes back on its own", any(o["thread_id"] == "p-nogate"
                                          and o["outcome"] == "requeued" for o in out), out)
    hist = [e["body"] for e in con.execute(
        "SELECT body FROM events WHERE thread_id='p-nogate' ORDER BY ts DESC")]
    check("the history says which recheck it was and what it was blocked on",
          hist and "recheck #1" in hist[0] and "vendor's next release" in hist[0], hist[:1])

    print("\na park that reaches 30 days expires into a real ask")
    armed(con, "p-old")
    store.post_result(con, "p-old", {
        "outcome": "parked", "state": "waiting-external", "note": LONG,
        "blocked_on": "Acme support, case 4821",
        "gate": "test -f /etc/ssl/certs/acme.pem"})
    con.execute("UPDATE threads SET recheck_at=?, parked_since=? WHERE thread_id='p-old'",
                (now - 10, now - 31 * 86400))
    con.commit()
    out = store.sweep_rechecks(con, run_gate=lambda t, c: (False, "still nothing"))
    check("reported as expired", any(o["thread_id"] == "p-old"
                                     and o["outcome"] == "expired" for o in out), out)
    row = con.execute("SELECT state, ask_kind, ask_prompt, ask_options, blocked_on,"
                      " recheck_at FROM threads WHERE thread_id='p-old'").fetchone()
    check("it is lit, not parked again", row["state"] == "needs-you", dict(row))
    check("carrying a real question with options, never a bare Close",
          row["ask_kind"] == "choice" and len(json.loads(row["ask_options"])) >= 2, dict(row))
    check("the question names the blocker it waited on",
          "Acme support" in row["ask_prompt"], row["ask_prompt"])
    check("and the stale park claim is gone",
          row["blocked_on"] == "" and row["recheck_at"] == 0, dict(row))
    opts = json.loads(row["ask_options"])
    check("one option settles it without claiming the work is finished",
          any(o["terminal"] and not o.get("resolved") for o in opts), opts)
    check("and one settles it asserting the work IS finished",
          any(o["terminal"] and o.get("resolved") for o in opts), opts)

    print("\nrechecking something that is not parked is a loud refusal, not a no-op")
    check("a live thread cannot be rechecked",
          status(store.apply_recheck, con, "p-old") == 400)

    con.close()
    print("\n%d passed, %d failed" % (PASS, FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(run())
