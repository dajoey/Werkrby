"""Offline invariant tests. No HTTP, no network, throwaway database.

Run: python3 tests/test_store.py   (from the repo root)

Every case here is a failure that shipped in the reference implementation at
least once. A green run is the only evidence that the gates are actually
wired, as opposed to reading correctly.
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


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("  ok   " + name)
    else:
        FAIL += 1
        print("  FAIL " + name + ("  <- " + str(detail) if detail else ""))


def thread(tid, **kw):
    ask = kw.get("ask", {"kind": "ack", "prompt": "Read this?"})
    if str(ask.get("kind")) in ("choice", "text"):
        # P3b §1.5: the four-test gates newly-lit choice/text asks at the
        # producer's door; the helper supplies the T1 marker the same way it
        # supplies a title (same treatment as blocked_on below). Assertions
        # are untouched.
        ask = dict(ask, requires=ask.get("requires", ["decision"]))
    # P3b §1.5/§1.2 T3: a distinct default title per thread, same reason the
    # attachments suite varies its titles for mute() -- the four-test's dedupe
    # fingerprints titles, and fingerprint() strips digits AND single-letter
    # stopwords by design (the "11.4% free" vs "7.8% free" case), so the
    # default spells the thread's digits out as words instead. Explicit titles
    # (the two resync assertions below) are untouched.
    _DIG = {"0": "zero", "1": "one", "2": "two", "3": "three", "4": "four",
            "5": "five", "6": "six", "7": "seven", "8": "eight", "9": "nine"}
    t = {"thread_id": tid, "title": kw.get(
        "title", "concern " + "".join(_DIG.get(c, c) for c in tid)),
         "summary": kw.get("summary", "a summary"),
         "category": kw.get("category", "Fleet"), "urgency": kw.get("urgency", "this-week"),
         "state": kw.get("state", "needs-you"),
         "ask": ask,
         "sources": kw.get("sources", [{"source": "planka", "id": "card-1"}])}
    if "events" in kw:
        t["events"] = kw["events"]
    if "first_seen" in kw:
        t["first_seen"] = kw["first_seen"]
    # The park contract (contract: P1 §1) applies at the producer's door too: a
    # parked thread names who it waits on or it is rejected. These fixtures were
    # written before the contract existed, so the helper supplies the blocker
    # the same way it supplies a title; the assertions below are untouched.
    if t["state"] == "waiting-external":
        t["blocked_on"] = kw.get("blocked_on", "the vendor's support desk, case 4821")
    return t


def rejected_errors(res, tid):
    for r in res.get("rejected", []):
        if r.get("thread_id") == tid:
            return " ".join(r["errors"])
    return ""


def run():
    tmp = os.path.join(tempfile.mkdtemp(prefix="helm-test-"), "t.db")
    con = store.db(tmp)

    print("\nupsert and round-trip")
    res = store.put_threads(con, {"threads": [thread("t-1"), thread("t-2", urgency="now")]})
    check("two threads created", res["created"] == 2, res)
    h0 = store.health(con)
    check("health carries origin accounting", "created" in h0 and h0["created"]["created"] == 2,
          h0.get("created"))
    check("producer-created threads are not run-created",
          h0["created"]["by_run"] == 0 and h0["created"]["replacement_rate"] == 0.0,
          h0["created"])
    got = store.list_threads(con)
    check("both returned", len(got["threads"]) == 2, got["threads"])
    check("sorted by urgency, now first", got["threads"][0]["thread_id"] == "t-2",
          [t["thread_id"] for t in got["threads"]])
    check("categories computed", got["categories"][0]["count"] == 2, got["categories"])

    print("\nevents are content-addressed, so a re-run is idempotent not additive")
    ev = [{"ts": 1000.0, "source": "planka", "kind": "update", "body": "found this"}]
    store.put_threads(con, {"threads": [thread("t-3", events=ev)]})
    n1 = con.execute("SELECT COUNT(*) c FROM events WHERE thread_id='t-3'").fetchone()["c"]
    store.put_threads(con, {"threads": [thread("t-3", events=ev)]})
    n2 = con.execute("SELECT COUNT(*) c FROM events WHERE thread_id='t-3'").fetchone()["c"]
    check("same event re-sent does not duplicate", n1 == n2 == 1, (n1, n2))

    print("\nInvariant 1: a short set deletes nothing")
    store.put_threads(con, {"threads": [thread("t-1", title="only this one")]})
    check("t-2 survives a pass that did not mention it",
          con.execute("SELECT 1 FROM threads WHERE thread_id='t-2'").fetchone() is not None)
    check("nothing tombstoned without being named",
          con.execute("SELECT COUNT(*) c FROM threads WHERE tombstoned=1").fetchone()["c"] == 0)

    print("\nInvariant 2: done is never dragged back")
    store.close_thread(con, "t-2", "handled")
    res = store.put_threads(con, {"threads": [thread("t-2", state="needs-you")]})
    row = con.execute("SELECT state FROM threads WHERE thread_id='t-2'").fetchone()
    check("stays done after a later PUT", row["state"] == "done", row["state"])
    check("preserved_done counted", res["preserved_done"] == 1, res)

    print("\nInvariant 3: an answered question is not re-armed")
    store.put_threads(con, {"threads": [thread(
        "t-4", ask={"kind": "choice", "prompt": "Original question?",
                    "options": [{"label": "Do A", "value": "a", "effect": "A happens"}]})]})
    store.reply(con, "t-4", "a", note="binding note")
    res = store.put_threads(con, {"threads": [thread(
        "t-4", state="needs-you", ask={"kind": "choice", "prompt": "REWRITTEN question?",
                                       "options": [{"label": "Do B", "value": "b"}]})]})
    row = con.execute("SELECT state, ask_prompt FROM threads WHERE thread_id='t-4'").fetchone()
    check("state stays working", row["state"] == "working", row["state"])
    check("the tapped ask survives", row["ask_prompt"] == "Original question?", row["ask_prompt"])
    check("preserved_answered counted", res["preserved_answered"] == 1, res)
    check("last_note is the human's note", con.execute(
        "SELECT last_note FROM threads WHERE thread_id='t-4'").fetchone()["last_note"]
        == "binding note")

    print("\npartial success: a bad thread costs only itself")
    res = store.put_threads(con, {"threads": [
        thread("t-good"),
        {"thread_id": "t-bad", "title": "x", "urgency": "immediately",
         "ask": {"kind": "ack"}},
        thread("t-good2")]})
    check("two good applied", res["accepted"] == 2, res)
    check("one rejected", res["rejected_count"] == 1, res)
    check("rejection names the thread", res["rejected"][0]["thread_id"] == "t-bad", res)
    check("rejection says why", "urgency" in rejected_errors(res, "t-bad"), res)
    check("good thread really landed",
          con.execute("SELECT 1 FROM threads WHERE thread_id='t-good'").fetchone() is not None)

    print("\nthread_id is required, never auto-generated")
    res = store.put_threads(con, {"threads": [{"title": "no id", "ask": {"kind": "ack"}}]})
    check("missing thread_id is rejected", res["rejected_count"] == 1, res)
    check("and says why", "thread_id is required" in res["rejected"][0]["errors"][0], res)

    print("\nInvariant 4a at the producer's door")
    res = store.put_threads(con, {"threads": [
        thread("t-bare", state="needs-you", ask={"kind": "none"})]})
    check("needs-you with no ask is rejected", res["rejected_count"] == 1, res)
    check("and explains the bare Close button",
          "bare Close" in rejected_errors(res, "t-bare"), res)

    print("\noption normalization")
    res = store.put_threads(con, {"threads": [thread(
        "t-opts", ask={"kind": "choice", "prompt": "p?", "options": ["Do the thing", "Skip it"]})]})
    check("bare strings are repaired, not rejected", res["rejected_count"] == 0, res)
    opts = json.loads(con.execute(
        "SELECT ask_options FROM threads WHERE thread_id='t-opts'").fetchone()["ask_options"])
    check("value derived from label", opts[0]["value"] == "do-the-thing", opts)
    res = store.put_threads(con, {"threads": [thread(
        "t-opts2", ask={"kind": "choice", "prompt": "p?", "options": [1, 2]})]})
    check("integers are refused", res["rejected_count"] == 1, res)

    print("\nresult gates")
    store.put_threads(con, {"threads": [thread(
        "t-r", ask={"kind": "choice", "prompt": "q?",
                    # contract: P1 §3 -- a one-character label ("L") is a
                    # position in a list the reader cannot see, so it is now
                    # rejected at the door. The value "v" every assertion below
                    # keys on is unchanged.
                    "options": [{"label": "Look into it", "value": "v",
                                 "effect": "I go and read the logs"}]})]})
    store.reply(con, "t-r", "v")

    def result_status(body):
        try:
            store.post_result(con, "t-r", body)
            return 202
        except store.StoreError as e:
            return e.status

    long_note = "x" * 120
    check("1 no note", result_status({"outcome": "needs-you", "state": "needs-you"}) == 400)
    check("2 note too short",
          result_status({"outcome": "ok", "state": "needs-you", "note": "done"}) == 400)
    check("3 stringified dict in outcome", result_status(
        {"outcome": "{'state':'needs-you','note':'...'}", "state": "needs-you",
         "note": long_note, "ask": {"kind": "ack"}}) == 400)
    check("4 a bot may not close a choice thread",
          result_status({"outcome": "ok", "state": "done", "note": long_note}) == 400)
    check("5 needs-you with no ask",
          result_status({"outcome": "ok", "state": "needs-you", "note": long_note}) == 400)
    check("6 snooze with no wake_at",
          result_status({"outcome": "ok", "state": "snoozed", "note": long_note}) == 400)
    check("7 wake_at in the past", result_status(
        {"outcome": "ok", "state": "snoozed", "note": long_note, "wake_at": 1000.0}) == 400)
    check("8 wake_at 500 days out", result_status(
        {"outcome": "ok", "state": "snoozed", "note": long_note,
         "wake_at": time.time() + 500 * 86400}) == 400)
    check("9 choice with no usable options", result_status(
        {"outcome": "ok", "state": "needs-you", "note": long_note,
         "ask": {"kind": "choice", "options": []}}) == 400)

    real = {"outcome": "drafted", "state": "needs-you",
            "note": "I read the case and wrote the reply below. " + "y" * 90,
            "ask": {"kind": "choice", "prompt": "Send it?",
                    "options": [{"label": "Send", "value": "send", "effect": "goes out"}]}}
    out = store.post_result(con, "t-r", real)
    check("10 a valid result is accepted", out["accepted"] is True, out)
    check("note_len is reported back", out["note_len"] == len(real["note"]), out)
    body = con.execute("SELECT body FROM events WHERE thread_id='t-r' AND kind='bot-result'"
                       " ORDER BY ts DESC LIMIT 1").fetchone()["body"]
    check("history renders the note, prefixed by the outcome",
          body.startswith("[drafted] ") and real["note"] in body, body[:80])

    # The other branch: when the outcome word is already in the opening of the
    # note, prefixing it would just stutter, so it is left off.
    echo = {"outcome": "drafted", "state": "needs-you",
            "note": "I drafted the reply and it is below in full. " + "y" * 90,
            "ask": {"kind": "ack", "prompt": "Read it?"}}
    store.post_result(con, "t-r", echo)
    body2 = con.execute("SELECT body FROM events WHERE thread_id='t-r' AND kind='bot-result'"
                        " ORDER BY ts DESC LIMIT 1").fetchone()["body"]
    check("no stuttering prefix when the outcome is already in the note",
          not body2.startswith("[") and body2 == echo["note"], body2[:80])
    check("the report did NOT overwrite the human's note", con.execute(
        "SELECT last_note FROM threads WHERE thread_id='t-r'").fetchone()["last_note"] == "")

    print("\nsnooze hides, then wakes on read")
    store.put_threads(con, {"threads": [thread("t-s", ask={
        "kind": "choice", "prompt": "Look into it?",
        "options": [{"label": "Yes", "value": "go", "effect": "I go look into it"}]})]})
    store.reply(con, "t-s", "go")
    store.post_result(con, "t-s", {
        "outcome": "deferred", "state": "snoozed", "note": "Parking this for a week. " + "z" * 90,
        "wake_at": time.time() + 7 * 86400, "wake_note": "Back as promised. " + "w" * 90})
    ids = [t["thread_id"] for t in store.list_threads(con)["threads"]]
    check("hidden from the default list", "t-s" not in ids, ids)
    ids = [t["thread_id"] for t in store.list_threads(con, include_snoozed=True)["threads"]]
    check("visible with include_snoozed", "t-s" in ids, ids)
    con.execute("UPDATE threads SET wake_at=? WHERE thread_id='t-s'", (time.time() - 5,))
    con.commit()
    got = store.list_threads(con)
    woken = [t for t in got["threads"] if t["thread_id"] == "t-s"]
    check("woke count reported", got["woke"] == 1, got["woke"])
    check("comes back lit", woken and woken[0]["state"] == "needs-you", woken)
    check("never wakes into a bare Close", woken and woken[0]["ask"]["kind"] != "none", woken)
    hist = " ".join(e["body"] for e in woken[0]["events"]) if woken else ""
    check("carrying the promised report", "Back as promised" in hist, hist[:80])

    print("\nmute suppresses the class, not the string")
    store.put_threads(con, {"threads": [
        thread("t-m1", title="[disk-audit] E: at 11.4% free on host-a")]})
    m = store.mute(con, "t-m1", "I know, I am not fixing it")
    check("rule keyed to the producer", m["producer"] == "disk-audit", m)
    res = store.put_threads(con, {"threads": [
        thread("t-m2", title="[disk-audit] E: at 7.8% free on host-a")]})
    check("a differently-measured restatement is auto-retired",
          res["suppressed_on_arrival"] == 1, res)
    ids = [t["thread_id"] for t in store.list_threads(con, include_done=True)["threads"]]
    check("and never shown", "t-m2" not in ids, ids)
    rules = store.list_suppressions(con)
    check("the rule is auditable with a hit count",
          rules and rules[0]["hits"] == 1, rules)

    print("\ntombstones retire only what is named")
    res = store.put_threads(con, {"threads": [], "tombstones": ["t-good2"]})
    check("one retired", res["retired"] == 1, res)
    ids = [t["thread_id"] for t in store.list_threads(con)["threads"]]
    check("gone from the list", "t-good2" not in ids, ids)
    check("t-good untouched", "t-good" in ids, ids)

    print("\npending-dispatch keys to the reply, not the thread")
    q = store.pending_dispatch(con)
    pend = [p["thread_id"] for p in q["pending"]]
    check("answered threads appear", "t-4" in pend, pend)
    check("unanswered ones do not", "t-good" not in pend, pend)
    check("the chosen value rides along",
          next(p["reply_value"] for p in q["pending"] if p["thread_id"] == "t-4") == "a")
    check("so does the binding note",
          next(p["reply_note"] for p in q["pending"] if p["thread_id"] == "t-4")
          == "binding note")

    print("\na terminal option settles instead of queueing an executor")
    store.put_threads(con, {"threads": [thread(
        "t-term", ask={"kind": "choice", "prompt": "Still a problem?", "options": [
            {"label": "Look into it", "value": "look", "effect": "I go and read the logs"},
            {"label": "Already fixed, stop asking", "value": "fixed",
             "effect": "I settle this and stop asking", "terminal": True}]})]})
    out = store.reply(con, "t-term", "fixed")
    check("settles immediately", out["state"] == "done" and out["terminal"] is True, out)
    check("and never reaches the dispatch queue", "t-term" not in
          [p["thread_id"] for p in store.pending_dispatch(con)["pending"]])
    store.put_threads(con, {"threads": [thread(
        "t-term2", ask={"kind": "choice", "prompt": "Still a problem?", "options": [
            {"label": "Look into it", "value": "look", "effect": "I go and read the logs"},
            {"label": "Already fixed", "value": "fixed", "effect": "stop", "terminal": True}]})]})
    out = store.reply(con, "t-term2", "look")
    check("a non-terminal option still queues work", out["state"] == "working", out)
    check("and does reach the queue", "t-term2" in
          [p["thread_id"] for p in store.pending_dispatch(con)["pending"]])

    print("\nan ack settles on the tap instead of looping through an executor")
    store.put_threads(con, {"threads": [thread(
        "t-ack", ask={"kind": "ack", "prompt": "Nothing further to do here. Close this out?"})]})
    out = store.reply(con, "t-ack", "ack")
    check("settles immediately", out["state"] == "done" and out["terminal"] is True, out)
    check("and never reaches the dispatch queue", "t-ack" not in
          [p["thread_id"] for p in store.pending_dispatch(con)["pending"]])
    row = con.execute("SELECT ask_kind FROM threads WHERE thread_id='t-ack'").fetchone()
    check("ask is cleared, same as an explicit close", row["ask_kind"] == "none", dict(row))

    # The exact shape an executor actually produced in production: an ack ask
    # carrying an ask_options entry for "ack" itself, marked terminal False,
    # because it was reasoning under the old (buggy) belief that only the
    # Close button could ever settle a thread. That belief must not survive
    # in the option data and re-open this loop; "ack" settles regardless of
    # what an option dict says about it.
    store.put_threads(con, {"threads": [thread(
        "t-ack-opts", ask={"kind": "ack", "prompt": "Close this out?"})]})
    con.execute("UPDATE threads SET ask_options=? WHERE thread_id='t-ack-opts'",
                (json.dumps([{"label": "Got it", "value": "ack", "effect": "clears for now",
                             "terminal": False}]),))
    con.commit()
    out = store.reply(con, "t-ack-opts", "ack")
    check("settles even when its own option claims non-terminal",
          out["state"] == "done" and out["terminal"] is True, out)

    # A redirect still needs a look: it carries a DIFFERENT value
    # ("__redirect__"), so it must not be caught by the ack short-circuit.
    store.put_threads(con, {"threads": [thread(
        "t-ack-redirect", ask={"kind": "ack", "prompt": "Close this out?"})]})
    out = store.reply(con, "t-ack-redirect", "__redirect__", note="Actually, one more thing.")
    check("a redirect on an ack thread still queues work", out["state"] == "working", out)
    check("and does reach the queue", "t-ack-redirect" in
          [p["thread_id"] for p in store.pending_dispatch(con)["pending"]])

    print("\ndispatch is idempotent against the reply")
    item = next(p for p in q["pending"] if p["thread_id"] == "t-4")
    store.mark_dispatched(con, "t-4", reply_ts=item["reply_ts"], ok=True, mode="reply")
    pend = [p["thread_id"] for p in store.pending_dispatch(con)["pending"]]
    check("the same answer does not dispatch twice", "t-4" not in pend, pend)
    store.reply(con, "t-4", "b", note="changed my mind")
    pend = [p["thread_id"] for p in store.pending_dispatch(con)["pending"]]
    check("a NEW answer on the same thread does dispatch", "t-4" in pend, pend)

    print("\nthe attempt is claimed BEFORE the executor is spawned")
    store.put_threads(con, {"threads": [thread("t-claim", ask={
        "kind": "choice", "prompt": "Look into it?",
        "options": [{"label": "Yes", "value": "go", "effect": "I go look into it"}]})]})
    store.reply(con, "t-claim", "go")
    store.mark_dispatched(con, "t-claim", mode="start")
    row = con.execute("SELECT dispatch_attempts, last_dispatch_try FROM threads"
                      " WHERE thread_id='t-claim'").fetchone()
    check("attempt claimed up front", row["dispatch_attempts"] == 1, dict(row))
    check("a poller killed right here does not re-offer immediately",
          "t-claim" not in [p["thread_id"] for p in store.pending_dispatch(con)["pending"]])
    check("but it is not lost either, it comes back after the backoff",
          row["last_dispatch_try"] > 0)

    print("\na failed dispatch burns exactly one attempt, start plus failure together")
    # This is the real poll.py sequence: "start" claims the attempt before the
    # executor is spawned, THEN the failed "reply" report comes in for that
    # SAME attempt. It must cost 1 against the 5-attempt budget, not 2; the
    # double-charge (one on start, one again on failure) is what silently
    # burned all 5 attempts in 3 real timeouts and wedged
    # t3-ssh-audit-20260901 in 'working' for two days.
    store.mark_dispatched(con, "t-4", mode="start")
    store.mark_dispatched(con, "t-4", ok=False, detail="executor exploded")
    row = con.execute("SELECT dispatch_attempts, last_dispatch_try FROM threads"
                      " WHERE thread_id='t-4'").fetchone()
    check("one attempt counted, not two", row["dispatch_attempts"] == 1, dict(row))
    check("held inside the backoff window",
          "t-4" not in [p["thread_id"] for p in store.pending_dispatch(con)["pending"]])
    con.execute("UPDATE threads SET last_dispatch_try=? WHERE thread_id='t-4'",
                (time.time() - 3600,))
    con.commit()
    check("offered again once the backoff expires",
          "t-4" in [p["thread_id"] for p in store.pending_dispatch(con)["pending"]])
    check("failure is visible in the history", any(
        "dispatch FAILED" in r["body"] for r in con.execute(
            "SELECT body FROM events WHERE thread_id='t-4'")))

    print("\na second start-then-fail cycle burns one more, not two more")
    store.mark_dispatched(con, "t-4", mode="start")
    store.mark_dispatched(con, "t-4", ok=False, detail="executor exploded again")
    row = con.execute("SELECT dispatch_attempts FROM threads WHERE thread_id='t-4'").fetchone()
    check("two attempts total after two failed cycles", row["dispatch_attempts"] == 2,
          dict(row))

    print("\ngiving up is loud, not silent")
    # last_dispatch_try is set to a realistic recent time, not epoch-0: gave_up
    # classification does not consult it, but restore_gave_up's own race guard
    # does (it refuses to restore a reply newer than the last real attempt),
    # and a real dispatcher never leaves it at 0 once a thread has been tried.
    con.execute("UPDATE threads SET dispatch_attempts=5, last_dispatch_try=?"
                " WHERE thread_id='t-4'", (time.time(),))
    con.commit()
    q = store.pending_dispatch(con)
    check("capped thread leaves the queue",
          "t-4" not in [p["thread_id"] for p in q["pending"]])
    check("and is counted as given up, not forgotten", q["gave_up"] == 1, q)

    print("\na thread that gave up is auto-restored to needs-you, same ask")
    reply_ts_before = con.execute(
        "SELECT MAX(ts) ts FROM events WHERE thread_id='t-4' AND kind='joey-reply'"
    ).fetchone()["ts"]
    restored = store.restore_gave_up(con)
    check("t-4 comes back", "t-4" in [r["thread_id"] for r in restored], restored)
    check("attempts reported as 5, the cap it hit", next(
        r["attempts"] for r in restored if r["thread_id"] == "t-4") == 5, restored)
    row = con.execute("SELECT state, ask_kind, ask_prompt, ask_options, dispatch_attempts,"
                      " dispatched_at FROM threads WHERE thread_id='t-4'").fetchone()
    check("state is needs-you again", row["state"] == "needs-you", dict(row))
    check("the original question survives untouched",
          row["ask_prompt"] == "Original question?", dict(row))
    check("the original options survive untouched",
          json.loads(row["ask_options"]) == [
              {"label": "Do A", "value": "a", "effect": "A happens",
               "terminal": False, "resolved": False}], row["ask_options"])
    check("attempt budget reset", row["dispatch_attempts"] == 0, dict(row))
    check("the stale reply was consumed, not left dangling",
          row["dispatched_at"] == reply_ts_before, dict(row))
    check("not re-offered as pending (no NEW reply was given)",
          "t-4" not in [p["thread_id"] for p in store.pending_dispatch(con)["pending"]])
    check("no longer counted as given up either",
          store.pending_dispatch(con)["gave_up"] == 0, store.pending_dispatch(con))
    check("explains itself in the history", any(
        "gave up" in r["body"] and "restoring" in r["body"].lower()
        for r in con.execute(
            "SELECT body FROM events WHERE thread_id='t-4' AND kind='bot-result'")),
          [dict(r) for r in con.execute(
              "SELECT body FROM events WHERE thread_id='t-4'")])
    check("calling it again with nothing new to restore is a no-op",
          store.restore_gave_up(con) == [])

    print("\na landed result resets the retry budget")
    con.execute("UPDATE threads SET dispatch_attempts=3, state='working',"
                " ask_kind='ack' WHERE thread_id='t-4'")
    con.commit()
    store.post_result(con, "t-4", {
        "outcome": "did-it", "state": "waiting-external",
        "blocked_on": "the vendor, who now has the item",  # contract: P1 §1
        "note": "Worked the item and handed it to the vendor. " + "q" * 60})
    check("attempts back to zero", con.execute(
        "SELECT dispatch_attempts FROM threads WHERE thread_id='t-4'"
    ).fetchone()["dispatch_attempts"] == 0)

    print("\na landed result also catches dispatched_at up, not just the retry budget")
    # Reproduces a live wedge found 2026-09-03: an ack-kind ask gets acked,
    # which settles the thread to 'done' (reply()'s ack branch, which
    # correctly never touches dispatched_at, because 'done' is never
    # reconsidered on its own). Then a SEPARATE, older, still-in-flight
    # dispatch's report lands late and overwrites state back to 'needs-you'
    # with a fresh ask, because post_result() applies whatever the executor
    # sends regardless of what happened to the thread meanwhile. Before this
    # fix, dispatched_at was left wherever it was before either event, so the
    # just-consumed ack reply looked unconsumed again and pending_dispatch()
    # re-offered it, not against the ack question but against whatever ask
    # happens to be live now. build_prompt() (dispatcher/poll.py) then finds
    # no offered option whose value is "ack" and renders "NONE OF THE OFFERED
    # OPTIONS", with no actual note from Joey behind it, because he never
    # wrote one: the reply being redispatched was already fully handled.
    store.put_threads(con, {"threads": [thread("t-race", ask={
        "kind": "ack", "prompt": "FYI, read this."})]})
    store.reply(con, "t-race", "ack")
    row = con.execute("SELECT state FROM threads WHERE thread_id='t-race'").fetchone()
    check("ack settles it to done", row["state"] == "done", dict(row))
    ack_ts = con.execute(
        "SELECT MAX(ts) ts FROM events WHERE thread_id='t-race' AND kind='joey-reply'"
    ).fetchone()["ts"]

    store.post_result(con, "t-race", {
        "outcome": "verified-landed", "state": "needs-you",
        "note": "An unrelated older run finished late and is reporting in. " + "z" * 60,
        "ask": {"kind": "choice", "prompt": "Close it, or do more?",
                "options": [{"label": "Close", "value": "close"},
                            {"label": "Do more", "value": "do-more"}]}})
    row = con.execute("SELECT state, dispatched_at FROM threads WHERE thread_id='t-race'"
                      ).fetchone()
    check("resurrected to needs-you by the late report", row["state"] == "needs-you", dict(row))
    check("dispatched_at caught up past the already-settled ack reply",
          row["dispatched_at"] >= ack_ts, (dict(row), ack_ts))
    pend = [p["thread_id"] for p in store.pending_dispatch(con)["pending"]]
    check("not re-offered using the stale, already-settled ack reply", "t-race" not in pend, pend)

    store.reply(con, "t-race", "close")
    pend = [p["thread_id"] for p in store.pending_dispatch(con)["pending"]]
    check("a genuinely new reply to the fresh ask still dispatches normally",
          "t-race" in pend, pend)

    print("\nmark_dispatched's own reply-success branch is monotonic too")
    store.put_threads(con, {"threads": [thread("t-mono", ask={
        "kind": "choice", "prompt": "Go?", "options": [{"label": "Yes", "value": "go"}]})]})
    store.reply(con, "t-mono", "go")
    ahead = time.time() + 500
    con.execute("UPDATE threads SET dispatched_at=? WHERE thread_id='t-mono'", (ahead,))
    con.commit()
    store.mark_dispatched(con, "t-mono", reply_ts=ahead - 1000, ok=True, mode="reply")
    after = con.execute(
        "SELECT dispatched_at FROM threads WHERE thread_id='t-mono'").fetchone()["dispatched_at"]
    check("a reply-success call with an older reply_ts does not move dispatched_at backward",
          after == ahead, (ahead, after))

    print("\na dispatched run that never reports becomes visible")
    long_ago = time.time() - 3 * store.RESULT_TIMEOUT_S
    store.put_threads(con, {"threads": [thread("t-quiet", ask={
        "kind": "choice", "prompt": "Look into it?",
        "options": [{"label": "Yes", "value": "go", "effect": "I go look into it"}]})]})
    store.reply(con, "t-quiet", "go")
    con.execute("UPDATE threads SET state='working', dispatched_at=? WHERE thread_id='t-quiet'",
                (long_ago,))
    con.commit()
    stale = [s["thread_id"] for s in store.stale_dispatches(con)]
    check("surfaced as stale rather than sitting silently", "t-quiet" in stale, stale)

    # The case that made the view lie: it ran, it reported, and it was then
    # answered AGAIN. dispatched_at still points at the previous round, so an
    # age-only test calls a live thread a dead run. Four real threads were
    # flagged this way on 2026-08-31 with real reports sitting in their history.
    store.add_event(con, "t-quiet", long_ago + 60, "bot", "", "bot-result",
                    "It reported properly after that dispatch. " + "n" * 80)
    con.commit()
    stale = [s["thread_id"] for s in store.stale_dispatches(con)]
    check("a run that DID report is not called quiet", "t-quiet" not in stale, stale)

    print("\nreopen reverses a settle, honestly rather than by reconstruction")
    store.put_threads(con, {"threads": [thread("t-reopen-close")]})
    store.close_thread(con, "t-reopen-close", "wrong tap")
    store.reopen_thread(con, "t-reopen-close", "misclick")
    row = con.execute("SELECT * FROM threads WHERE thread_id='t-reopen-close'").fetchone()
    check("back to needs-you", row["state"] == "needs-you", row["state"])
    check("asks fresh rather than pretending to recall the original options",
          row["ask_kind"] == "text" and row["ask_options"] == "[]", dict(row))
    check("the reopen itself is in the history", any(
        "reopened by you" in r["body"] for r in con.execute(
            "SELECT body FROM events WHERE thread_id='t-reopen-close'")))

    store.put_threads(con, {"threads": [thread(
        "t-reopen-terminal", ask={"kind": "choice", "prompt": "which way",
                                  "options": [{"label": "Park it", "value": "park",
                                              "terminal": True}]})]})
    store.reply(con, "t-reopen-terminal", "park")
    check("terminal reply already settled it", con.execute(
        "SELECT state FROM threads WHERE thread_id='t-reopen-terminal'"
    ).fetchone()["state"] == "done")
    store.reopen_thread(con, "t-reopen-terminal")
    check("a terminal-settled thread reopens the same way as an explicit close",
          con.execute("SELECT state FROM threads WHERE thread_id='t-reopen-terminal'"
                      ).fetchone()["state"] == "needs-you")

    print("\nreopen refuses anything that is not actually done")
    store.put_threads(con, {"threads": [thread("t-reopen-live")]})
    try:
        store.reopen_thread(con, "t-reopen-live")
        check("live thread rejected", False, "did not raise")
    except store.StoreError as e:
        check("live thread rejected", e.status == 400, e.detail)

    print("\nparking is orthogonal to state, and only ever a deliberate call")
    store.put_threads(con, {"threads": [
        thread("t-park-lit"),  # default state=needs-you, ask=ack
        thread("t-park-wait", state="waiting-external", ask={"kind": "none"}),
    ]})
    try:
        store.set_parked(con, "t-park-lit", True, "x" * 90)
        check("a needs-you thread cannot be parked", False, "did not raise")
    except store.StoreError as e:
        check("a needs-you thread cannot be parked", e.status == 400, e.detail)

    try:
        store.set_parked(con, "t-park-wait", True, "")
        check("parking with no reason is refused", False, "did not raise")
    except store.StoreError as e:
        check("parking with no reason is refused", e.status == 400, e.detail)

    try:
        store.set_parked(con, "t-park-wait", True, "too short")
        check("parking with a token reason is refused", False, "did not raise")
    except store.StoreError as e:
        check("parking with a token reason is refused", e.status == 400, e.detail)

    reason = "Waiting on nothing in particular; parked here only to prove the feature works end to end."
    r = store.set_parked(con, "t-park-wait", True, reason)
    check("park accepted", r == {"accepted": True, "thread_id": "t-park-wait",
                                 "parked": True, "reason": reason})
    got = next(t for t in store.list_threads(con)["threads"] if t["thread_id"] == "t-park-wait")
    check("list_threads reports it parked", got["parked"] is True)
    check("list_threads carries the reason", got["parked_reason"] == reason)
    check("a parked thread is still visible, not hidden like a snooze",
          any(t["thread_id"] == "t-park-wait" for t in store.list_threads(con)["threads"]))

    h = store.health(con)
    check("health counts it as parked", h["parked"] >= 1, h["parked"])
    check("health's in_flight excludes it", h["in_flight"] == h["threads_total"]
          - h["by_state"].get("done", 0) - h["parked"], h)

    print("\na producer resync can never silently un-park what a human set aside")
    store.put_threads(con, {"threads": [
        thread("t-park-wait", state="waiting-external", ask={"kind": "none"},
              title="resynced title")]})
    row = con.execute("SELECT parked, parked_reason, title FROM threads"
                      " WHERE thread_id='t-park-wait'").fetchone()
    check("still parked after an unrelated resync", bool(row["parked"]))
    check("reason untouched by the resync", row["parked_reason"] == reason)
    check("but the resync's own fields DO land, so this is not just inert",
          row["title"] == "resynced title")

    print("\nunparking needs no reason, and only ever the opposite call fixes it")
    r = store.set_parked(con, "t-park-wait", False)
    check("unpark accepted", r["accepted"] and r["parked"] is False)
    got = next(t for t in store.list_threads(con)["threads"] if t["thread_id"] == "t-park-wait")
    check("no longer parked", got["parked"] is False)

    r = store.set_parked(con, "t-park-wait", False, "")
    check("unparking an already-unparked thread is a harmless no-op", r["accepted"])

    print("\nnormalize_links: forgive the shape we can repair, refuse the shape we cannot")
    links, errs = store.normalize_links(None)
    check("None is a no-op, not an error", links == [] and errs == [])
    links, errs = store.normalize_links(["https://x.example/a"])
    check("a bare string url gets the url as its own label",
          links == [{"label": "https://x.example/a", "url": "https://x.example/a"}], links)
    links, errs = store.normalize_links([{"label": "the draft", "url": "https://x.example/b"}])
    check("a dict keeps its own label", links == [{"label": "the draft", "url": "https://x.example/b"}])
    links, errs = store.normalize_links([{"label": "no url here"}])
    check("a link with no url is rejected, not silently dropped-with-blank-url",
          links == [] and errs and "no url" in errs[0], (links, errs))
    links, errs = store.normalize_links([42])
    check("a link that is neither string nor object is rejected", links == [] and errs, errs)
    links, errs = store.normalize_links("not a list")
    check("links as a bare string (not a list) is rejected outright", links == [] and errs, errs)
    links, errs = store.normalize_links([{"url": "x"}] * (store.MAX_LINKS + 1))
    check("more than MAX_LINKS is rejected rather than silently truncated",
          links == [] and errs, errs)

    print("\nlinks round-trip through a producer PUT, same treatment as sources")
    store.put_threads(con, {"threads": [thread(
        "t-links-put", ask={"kind": "ack", "prompt": "Read this?"})]})
    store.put_threads(con, {"threads": [
        {**thread("t-links-put", ask={"kind": "ack", "prompt": "Read this?"}),
         "links": [{"label": "KB draft", "url": "https://wiki.example.com/x/1"}]}]})
    got = next(t for t in store.list_threads(con)["threads"] if t["thread_id"] == "t-links-put")
    check("the link is on the thread after a PUT", got["links"] ==
          [{"label": "KB draft", "url": "https://wiki.example.com/x/1"}], got["links"])
    store.put_threads(con, {"threads": [thread(
        "t-links-put", ask={"kind": "ack", "prompt": "Read this?"})]})
    got = next(t for t in store.list_threads(con)["threads"] if t["thread_id"] == "t-links-put")
    check("a resync that omits links wipes it, same as title/summary/sources would",
          got["links"] == [], got["links"])

    # A choice ask with no terminal option, reused across rounds: 'ack' would
    # settle the thread on the very first reply (see reply()'s docstring) and
    # never actually reach 'working', which is the state this is meant to
    # exercise.
    continue_ask = {"kind": "choice", "prompt": "Continue?",
                    "options": [{"label": "Continue", "value": "continue"}]}

    print("\npost_result: an omitted 'links' key preserves what was there; [] explicitly clears it")
    store.put_threads(con, {"threads": [thread("t-links-bot", ask=continue_ask)]})
    store.reply(con, "t-links-bot", "continue")
    store.post_result(con, "t-links-bot", {
        "outcome": "built", "state": "needs-you",
        "note": "Published the draft. " + "z" * 90, "ask": continue_ask,
        "links": [{"label": "the new page", "url": "https://wiki.example.com/x/2"}]})
    got = next(t for t in store.list_threads(con)["threads"] if t["thread_id"] == "t-links-bot")
    check("a bot-result's links land on the thread", got["links"] ==
          [{"label": "the new page", "url": "https://wiki.example.com/x/2"}],
          got["links"])
    store.reply(con, "t-links-bot", "continue")
    store.post_result(con, "t-links-bot", {
        "outcome": "checked", "state": "needs-you",
        "note": "Nothing new to report, still waiting on you. " + "z" * 60, "ask": continue_ask})
    got = next(t for t in store.list_threads(con)["threads"] if t["thread_id"] == "t-links-bot")
    check("a later result that omits 'links' entirely does not erase the earlier link",
          got["links"] == [{"label": "the new page",
                            "url": "https://wiki.example.com/x/2"}], got["links"])
    store.reply(con, "t-links-bot", "continue")
    store.post_result(con, "t-links-bot", {
        "outcome": "superseded", "state": "needs-you",
        "note": "That page got replaced by a different one entirely. " + "z" * 40,
        "ask": continue_ask, "links": []})
    got = next(t for t in store.list_threads(con)["threads"] if t["thread_id"] == "t-links-bot")
    check("an explicit empty list DOES clear it", got["links"] == [], got["links"])

    print("\na routine resync cannot clobber a bot-result's link while the answer is in flight")
    store.put_threads(con, {"threads": [thread("t-links-race", ask=continue_ask)]})
    store.reply(con, "t-links-race", "continue")  # state -> working
    # No 'ask' in this result: there is nothing left for Joey to decide right
    # now, so ask_kind clears to 'none'. That, plus joey-reply and bot-result
    # both now in history, is the actual third branch _answer_is_in_flight
    # checks (see its docstring: "ask cleared, a reply given, a report back:
    # the work is done and only Close remains") -- unlike a bot-result that
    # hands back a FRESH ask, which the invariant does not cover at all, the
    # same way an ordinary ask_prompt/ask_options change is not covered once
    # a new question is live.
    store.post_result(con, "t-links-race", {
        "outcome": "built", "state": "waiting-external",
        "blocked_on": "the reviewers on the published page",  # contract: P1 §1
        "note": "Here is the page it built; nothing more needed from you right now. " + "z" * 60,
        "links": [{"label": "the built page", "url": "https://wiki.example.com/x/3"}]})
    store.put_threads(con, {"threads": [thread("t-links-race", title="resynced")]})
    got = next(t for t in store.list_threads(con)["threads"] if t["thread_id"] == "t-links-race")
    check("the bot-result's link survives the resync", got["links"] ==
          [{"label": "the built page", "url": "https://wiki.example.com/x/3"}],
          got["links"])
    check("but the resync's own fields still land, so this is not just inert",
          got["title"] == "resynced", got["title"])

    print("\ncards written by hand from the app")
    try:
        store.create_thread(con, {"summary": "no title"})
        check("a card with no title is refused", False, "did not raise")
    except store.StoreError as e:
        check("a card with no title is refused", e.status == 400, e.detail)
    try:
        store.create_thread(con, {"title": "x", "urgency": "soon"})
        check("an urgency outside the set is refused, not coerced", False, "did not raise")
    except store.StoreError as e:
        check("an urgency outside the set is refused, not coerced", e.status == 400, e.detail)
    try:
        store.create_thread(con, {"title": "x", "intent": "someone else"})
        check("an intent outside the set is refused", False, "did not raise")
    except store.StoreError as e:
        check("an intent outside the set is refused", e.status == 400, e.detail)
    try:
        store.create_thread(con, {"title": "x", "intent": "claude"})
        check("handing over with no instruction is refused", False, "did not raise")
    except store.StoreError as e:
        check("handing over with no instruction is refused", e.status == 400, e.detail)
    try:
        store.create_thread(con, {"title": "x", "links": [{"label": "no url"}]})
        check("a malformed link is refused rather than stored blank", False, "did not raise")
    except store.StoreError as e:
        check("a malformed link is refused rather than stored blank", e.status == 400, e.detail)

    r = store.create_thread(con, {
        "title": "Chase the Acme cert bundle", "summary": "vendor has not replied",
        "category": "Certificates", "urgency": "this-week",
        "links": [{"label": "the case", "url": "https://example.invalid/c"}]})
    mine = r["thread_id"]
    check("the id says who wrote it and when", mine.startswith("joey-"), mine)
    check("the id carries the title, so it is readable in a log", "acme" in mine, mine)
    got = next(t for t in store.list_threads(con)["threads"] if t["thread_id"] == mine)
    check("it lands lit, waiting on you", got["state"] == "needs-you", got["state"])
    # Invariant 4: a lit thread carries a question. A form cannot author one,
    # so create_thread supplies it rather than letting a lit card exist
    # without one.
    check("it carries a question, not a bare Close button",
          got["ask"]["kind"] == "choice" and len(got["ask"]["options"]) == 1, got["ask"])
    check("its one option settles it and asserts the work is finished",
          got["ask"]["options"][0]["terminal"] and got["ask"]["options"][0]["resolved"],
          got["ask"]["options"][0])
    check("the link it was given is on it", got["links"] ==
          [{"label": "the case", "url": "https://example.invalid/c"}], got["links"])
    check("nothing is claimed as a source, because nothing derived it",
          got["sources"] == [], got["sources"])
    check("the history says you wrote it", any(
        e["kind"] == "state-change" and "yourself" in e["body"] for e in got["events"]),
        got["events"])
    check("tapping its one option settles it", store.reply(con, mine, "done")["state"] == "done")

    same = store.create_thread(con, {"title": "Chase the Acme cert bundle"})
    check("the same title twice in one day is a second card, not a collision",
          same["thread_id"] != mine, same["thread_id"])

    r = store.create_thread(con, {"title": "Draft the migration note",
                                  "intent": "claude",
                                  "instruction": "Draft it against the last two cases."})
    handed = r["thread_id"]
    check("handing it over answers it in the same act", r["handed_off"] is True)
    check("which puts it in working, not lit", r["state"] == "working", r["state"])
    pend = store.pending_dispatch(con)["pending"]
    row = next((p for p in pend if p["thread_id"] == handed), None)
    check("and the dispatcher sees it with no second queue", row is not None)
    check("the reply value says it was written by hand",
          row and row["reply_value"] == store.HAND_TASK_VALUE, row and row["reply_value"])
    check("the instruction rides along as the binding note",
          row and row["reply_note"] == "Draft it against the last two cases.",
          row and row["reply_note"])

    r = store.create_thread(con, {"title": "Waiting on the vendor for the cert upload",
                                  "intent": "waiting",
                                  # contract: P1 §1 -- a hand-written wait names
                                  # its blocker like any other park.
                                  "blocked_on": "the vendor contact, who has the cert bundle"})
    got = next(t for t in store.list_threads(con)["threads"]
               if t["thread_id"] == r["thread_id"])
    check("a card you are only tracking is not lit",
          got["state"] == "waiting-external", got["state"])
    check("and asks nothing, which is allowed because it is not lit",
          got["ask"]["kind"] == "none", got["ask"])
    check("it is visible, not hidden like a snooze", got["wake_at"] in (0, 0.0))

    # A mute rule exists to stop a producer re-deriving the same nag forever.
    # A card someone just typed is not that, and auto-retiring it on arrival
    # would look exactly like the app dropping what they wrote.
    # Same title AND same producer as the hand-written card below (no sources
    # on either, so producer_of returns "unknown" for both), which is what it
    # takes for the rule to match at all: a rule that could not have fired
    # would make this test prove nothing.
    store.put_threads(con, {"threads": [thread("t-hand-mute", title="Backup job keeps failing",
                                               sources=[])]})
    store.mute(con, "t-hand-mute")
    blocked = store.put_threads(con, {"threads": [thread("t-hand-mute-2",
                                                         title="Backup job keeps failing",
                                                         sources=[])]})
    check("the rule really does retire a producer's copy on arrival",
          blocked["suppressed_on_arrival"] == 1, blocked)
    r = store.create_thread(con, {"title": "Backup job keeps failing"})
    check("a mute rule does not silently retire a card you wrote by hand",
          any(t["thread_id"] == r["thread_id"] for t in store.list_threads(con)["threads"]),
          r)

    con.close()
    print("\n%d passed, %d failed" % (PASS, FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(run())
