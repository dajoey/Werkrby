"""Adversarial probe: fire every rejection path and fail if any of them is accepted.

Run: python3 tests/probe_400s.py   (from anywhere; it builds its own DB in a temp dir)

PLAN standing rule 2: "A gate is not a gate until a probe that should 400 returns 400."
LESSON 40: an unfired guard is an untested guard. The suites assert what the store DOES;
this asserts what it REFUSES, from the other side -- one throwaway database, every bad
payload the gates exist for, and a nonzero exit if a single one of them lands.

The eight LATTICE cases below are the verification list in docs/PLAN.md's state lattice
("every state has a machine-checkable exit"): one probe per transition rule it states,
in the order it states them. Everything after them walks contract P1 §1-§5 path by path.
A rejection counts only if it is a StoreError with status 400 (or, at the producer's
door, a named entry in rejected[] while the rest of the payload still lands) AND the
thread is left in the state it was in before: a guard that refuses and mutates anyway is
not a guard.
"""
from __future__ import annotations

import hashlib
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helm import store  # noqa: E402

PASS = FAIL = 0
NOTE = ("The run did the work and this is the long-enough report about what happened. "
        + "z" * 40)
LONG = "x" * 120
_SEQ = [0]


def _title():
    _SEQ[0] += 1
    # Distinct per card: mute() suppresses by title fingerprint, and shared
    # titles would auto-retire every card filed after the mute probe.
    return "concern " + chr(96 + (_SEQ[0] % 26 or 26)) * 3 + " " + chr(96 + _SEQ[0] % 7 + 1)


def fired(name, ref, fn, *a, **kw):
    """Assert this call is REFUSED with a 400."""
    global PASS, FAIL
    try:
        out = fn(*a, **kw)
    except store.StoreError as e:
        if e.status == 400:
            PASS += 1
            print("  400  %-58s %s" % (name, ref))
            return e.detail
        FAIL += 1
        print("  FAIL %-58s %s  <- status %d, expected 400" % (name, ref, e.status))
        return None
    FAIL += 1
    print("  FAIL %-58s %s  <- ACCEPTED: %r" % (name, ref, out))
    return None


def fired_404(name, ref, fn, *a, **kw):
    """The one refusal that is deliberately NOT a 400: there is no such thread,
    which is the caller's id being wrong, not its payload."""
    global PASS, FAIL
    try:
        out = fn(*a, **kw)
    except store.StoreError as e:
        if e.status == 404:
            PASS += 1
            print("  404  %-58s %s" % (name, ref))
            return e.detail
        FAIL += 1
        print("  FAIL %-58s %s  <- status %d, expected 404" % (name, ref, e.status))
        return None
    FAIL += 1
    print("  FAIL %-58s %s  <- ACCEPTED: %r" % (name, ref, out))
    return None


def refused_at_door(name, ref, res, tid):
    """Assert put_threads named this thread in rejected[] and landed the rest."""
    global PASS, FAIL
    named = [r for r in res.get("rejected", []) if r.get("thread_id") == tid]
    if named and res.get("created", 0) + res.get("updated", 0) >= 1:
        PASS += 1
        print("  400  %-58s %s" % (name, ref))
        return " ".join(named[0]["errors"])
    FAIL += 1
    print("  FAIL %-58s %s  <- %r" % (name, ref, res))
    return None


def holds(name, cond, detail=""):
    """Assert the refusal left nothing behind."""
    global PASS, FAIL
    if cond:
        PASS += 1
        print("  ok   %-58s %s" % (name, detail or ""))
    else:
        FAIL += 1
        print("  FAIL %-58s %s" % (name, detail or ""))


def thread(tid, **kw):
    t = {"thread_id": tid, "title": _title(), "summary": "a summary", "category": "Fleet",
         "urgency": "this-week", "state": "needs-you",
         "ask": {"kind": "ack", "prompt": "Read this and close it."}}
    t.update(kw)
    return t


def put(con, *threads):
    return store.put_threads(con, {"threads": list(threads)})


def state_of(con, tid):
    row = con.execute("SELECT state FROM threads WHERE thread_id=?", (tid,)).fetchone()
    return row[0] if row else None


CHOICE = {"kind": "choice", "prompt": "The vendor wants an answer by Friday. Which way?",
          "requires": ["decision"],
          "options": [{"label": "Accept the quote and book the install", "value": "accept",
                       "effect": "I accept it and book the install"},
                      {"label": "Push them for a better price", "value": "push",
                       "effect": "I go back to them on price"}]}
ACK = {"kind": "ack", "prompt": "Read this and close it when you have."}


def att(name="invoice.pdf", **kw):
    m = {"filename": name, "mime": "application/pdf", "size": 184320,
         "sha256": hashlib.sha256(name.encode()).hexdigest(),
         "url": "https://helm.local/files/" + name}
    m.update(kw)
    return m


def run():
    tmpdir = tempfile.mkdtemp(prefix="helm-probe-")
    con = store.db(os.path.join(tmpdir, "probe.db"))
    R = lambda tid, body: store.post_result(con, tid, body)  # noqa: E731

    print("\nthe eight lattice cases (docs/PLAN.md, 'every state has a machine-checkable exit')")

    # L1. needs-you --(tap)--> done: only a terminal option or an ack tap, and
    #     never while a file he just sent is unconsumed.
    put(con, thread("L1"))
    store.add_attachment(con, "L1", att("L1-scan.pdf"))
    fired("L1 close-tap while a sent file is unconsumed", "lattice 1 / P1 §5",
          store.close_thread, con, "L1")
    holds("    and the card is still his to answer", state_of(con, "L1") == "needs-you")

    # L2. needs-you --(tap)--> working: the answer has to say which option.
    put(con, thread("L2", ask=CHOICE))
    fired("L2 an answer with no value", "lattice 2", store.reply, con, "L2", "")

    # L3. working --> needs-you: MUST carry an ask, and the answer gate applies.
    put(con, thread("L3", ask=CHOICE))
    store.reply(con, "L3", "accept")
    fired("L3 a lit card with no question on it", "lattice 3 / invariant 4a",
          R, "L3", {"outcome": "ok", "state": "needs-you", "note": LONG})

    # L4. working --> waiting-external: a park names who it waits on and when we look.
    put(con, thread("L4", ask=CHOICE))
    store.reply(con, "L4", "accept")
    fired("L4 a park with nobody to wait on", "lattice 4 / P1 §1",
          R, "L4", {"outcome": "blocked", "state": "waiting-external", "note": LONG})
    fired("L4 a park that outlives its own deadline (>30d)", "lattice 4 / P1 §1",
          R, "L4", {"outcome": "blocked", "state": "waiting-external", "note": LONG,
                    "blocked_on": "Acme support, case 4821",
                    "recheck_at": time.time() + 90 * 86400})
    holds("    and the thread never left the work queue", state_of(con, "L4") == "working")

    # L5. working --> snoozed: a deferral is a promise, so it needs a date.
    fired("L5 a deferral with no date", "lattice 5",
          R, "L4", {"outcome": "later", "state": "snoozed", "note": LONG})
    fired("L5 a deferral into the past", "lattice 5",
          R, "L4", {"outcome": "later", "state": "snoozed", "note": LONG,
                    "wake_at": time.time() - 60})

    # L6. working --> done: ONLY the mechanical bot-close exceptions.
    fired("L6 a bot closing a thread he answered but nobody finished", "lattice 6 / P1 §4",
          R, "L4", {"outcome": "done", "state": "done", "note": LONG})
    holds("    and the thread is still in flight", state_of(con, "L4") == "working")

    # L7. waiting-external --> working: the exit is the recheck, and a park with
    #     an unrunnable gate has no machine-checkable exit at all.
    put(con, thread("L7", ask=CHOICE))
    store.reply(con, "L7", "accept")
    fired("L7 a park whose gate cannot be run by a sweeper", "lattice 7 / P1 §1",
          R, "L7", {"outcome": "blocked", "state": "waiting-external", "note": LONG,
                    "blocked_on": "the DNS registrar's support queue",
                    "gate": "check-it.sh\nrm -rf /"})

    # L8. snoozed --> needs-you: not 400 years from now.
    fired("L8 a deferral past the far horizon", "lattice 8",
          R, "L7", {"outcome": "later", "state": "snoozed", "note": LONG,
                    "wake_at": time.time() + 500 * 86400})

    # The lattice's closing line: "A PUT may never resurrect done."
    put(con, thread("L9", ask=CHOICE))
    store.reply(con, "L9", "accept")
    store.close_thread(con, "L9")
    res = put(con, thread("L9", ask=CHOICE, state="needs-you"))
    holds("L9 a PUT may never resurrect a closed thread",
          state_of(con, "L9") == "done" and res["preserved_done"] == 1, res)

    print("\nP1 §1 -- the park contract: nothing waits on nobody, nothing waits forever")
    put(con, thread("p1", ask=CHOICE))
    store.reply(con, "p1", "accept")
    park = {"outcome": "blocked", "state": "waiting-external", "note": LONG}
    fired("a blocker too short to name anyone", "P1 §1",
          R, "p1", dict(park, blocked_on="them"))
    d = fired("a park aimed at the person the list is for", "P1 §1",
              R, "p1", dict(park, blocked_on="the owner"))
    holds("    and it says it would wait on him invisibly, forever",
          d and "invisibly" in d, d)
    fired("a park aimed at 'the user'", "P1 §1", R, "p1", dict(park, blocked_on="the user"))
    fired("a park aimed at 'Joey' (too short to name anyone either)", "P1 §1",
          R, "p1", dict(park, blocked_on="Joey"))
    fired("a gate command longer than a sweeper should run", "P1 §1",
          R, "p1", dict(park, blocked_on="Acme support, case 4821",
                        gate="x" * 600))
    refused_at_door("a producer parking a thread with no blocker", "P1 §1",
                    put(con, thread("p2", state="waiting-external", ask={"kind": "none"}),
                        thread("p3")), "p2")
    fired("a hand-written wait with no blocker", "P1 §1",
          store.create_thread, con, {"title": "Chase the cert bundle", "intent": "waiting"})
    fired("a hand-written card with an unknown intent", "P1 §1 / PLAN (unknown executor)",
          store.create_thread, con, {"title": "Do the thing", "intent": "gpt-9"})

    print("\nP1 §2 -- the answer gate: his question is answered FIRST or it is a 400")
    put(con, thread("a1", ask=CHOICE))
    store.reply(con, "a1", "accept", note="Did the vendor ever send the revised quote?")
    d = fired("a report that ignores the question in his note", "P1 §2",
              R, "a1", {"outcome": "ok", "state": "needs-you", "note": LONG, "ask": ACK})
    holds("    and it quotes the question back", d and "revised quote" in d, d)
    fired("a one-word answer to a typed question", "P1 §2",
          R, "a1", {"outcome": "ok", "state": "needs-you", "note": LONG, "ask": ACK,
                    "answers": "yes"})
    fired("parking a thread does not excuse the question", "P1 §2",
          R, "a1", {"outcome": "blocked", "state": "waiting-external", "note": LONG,
                    "blocked_on": "Acme support, case 4821"})
    fired("nor does closing it", "P1 §2",
          R, "a1", {"outcome": "done", "state": "done", "note": LONG,
                    "answer_applied": True, "nothing_owed": True})
    fired("an answers list with no answer in it", "P1 §2",
          R, "a1", {"outcome": "ok", "state": "needs-you", "note": LONG, "ask": ACK,
                    "answers": [{"question": "Did the vendor send it?"}]})

    print("\nP1 §3 -- self-contained: an option that needs the list is a riddle on a phone")
    def ask_with(label, context=None, effect="something specific happens"):
        a = {"kind": "choice", "prompt": "Which way do you want this to go?",
             "options": [{"label": label, "value": "v", "effect": effect},
                         {"label": "Leave it alone for now", "value": "leave",
                          "effect": "nothing happens"}]}
        if context is not None:
            a["context"] = context
        return a

    put(con, thread("s1", ask=CHOICE))
    store.reply(con, "s1", "accept")
    for label, why in [("Do it as discussed", "an external reference"),
                       ("Go with the first one", "counting an invisible list"),
                       ("See the email and decide", "sending the reader elsewhere"),
                       ("that one", "a pointer with nothing to point at"),
                       ("The second", "a position, not an action"),
                       ("C+D", "letters from a list that is not on the card"),
                       ("Option D", "an option named by letter"),
                       ("A and B", "a combination of letters")]:
        fired("a run asking with %-38s" % ("%r (%s)" % (label, why)), "P1 §3",
              R, "s1", {"outcome": "asked", "state": "needs-you", "note": NOTE,
                        "ask": ask_with(label)})
    fired("an effect that points off the card", "P1 §3",
          R, "s1", {"outcome": "asked", "state": "needs-you", "note": NOTE,
                    "ask": ask_with("Send it now", effect="Sends the draft we agreed above")})
    fired("a context that defines only some of the letters", "P1 §3a",
          R, "s1", {"outcome": "asked", "state": "needs-you", "note": NOTE,
                    "ask": ask_with("C+D", context="A) renew it\nB) let it lapse")})
    fired("a context that itself points off the card", "P1 §3a",
          R, "s1", {"outcome": "asked", "state": "needs-you", "note": NOTE,
                    "ask": ask_with("A", context="A) the one we discussed above")})
    refused_at_door("a producer sending two buttons that read the same", "P1 §3",
                    put(con, thread("s2", ask={
                        "kind": "choice", "prompt": "Which way?",
                        "options": [{"label": "Send it", "value": "a", "effect": "as written"},
                                    {"label": "send it", "value": "b", "effect": "rewritten"}]}),
                        thread("s3")), "s2")
    refused_at_door("a producer sending buttons with no question", "P1 §3",
                    put(con, thread("s4", ask={
                        "kind": "choice", "prompt": "",
                        "options": [{"label": "Send the draft", "value": "a", "effect": "out"},
                                    {"label": "Hold the draft", "value": "b", "effect": "no"}]}),
                        thread("s5")), "s4")
    holds("    the whole payload did not go down with the bad thread",
          state_of(con, "s5") == "needs-you")

    print("\nP1 §4 -- bot-close: only Joey closes")
    put(con, thread("b1", ask=CHOICE))
    d = fired("a bot closing a thread nobody has answered", "P1 §4",
              R, "b1", {"outcome": "done", "state": "done", "note": LONG})
    holds("    and it says who does close threads", d and "only Joey closes" in d, d)
    put(con, thread("b2", ask=CHOICE))
    store.reply(con, "b2", "accept")
    fired("closing an answered decision with no assertions", "P1 §4",
          R, "b2", {"outcome": "done", "state": "done", "note": LONG})
    fired("closing it with only half the assertions", "P1 §4",
          R, "b2", {"outcome": "done", "state": "done", "note": LONG, "answer_applied": True})
    fired("asserting completion as prose instead of a boolean", "P1 §4 / LESSON 12",
          R, "b2", {"outcome": "done", "state": "done", "note": LONG,
                    "answer_applied": "true", "nothing_owed": "yes"})
    fired("a done that asks a fresh question", "P1 §4",
          R, "b2", {"outcome": "done", "state": "done", "note": LONG, "ask": CHOICE,
                    "answer_applied": True, "nothing_owed": True})
    put(con, thread("b3", ask=ACK))
    store.reply(con, "b3", "__redirect__", note="Chase the other invoice first")
    d = fired("closing an ack card on a redirect", "P1 §4 / LESSON 13",
              R, "b3", {"outcome": "done", "state": "done", "note": LONG,
                        "answer_applied": True, "nothing_owed": True})
    holds("    and the refusal quotes what he actually sent",
          d and "__redirect__" in d, d)
    put(con, thread("b4", state="waiting-external", ask={"kind": "none"},
                    blocked_on="Acme support, case 4821"))
    d = fired("closing an ever-parked thread with no ack in the post", "P1 §4",
              R, "b4", {"outcome": "done", "state": "done", "note": LONG})
    holds("    and the refusal says to put the ack ask IN the post",
          d and "IN this post" in d, d)
    put(con, thread("b5", ask=CHOICE))
    store.reply(con, "b5", "accept")
    R("b5", {"outcome": "asked", "state": "needs-you", "note": NOTE, "ask": CHOICE})
    fired("closing on an answer older than the question", "P1 §4",
          R, "b5", {"outcome": "done", "state": "done", "note": LONG,
                    "answer_applied": True, "nothing_owed": True})
    put(con, thread("b6", ask=CHOICE))
    fired("a bot at the human close door", "P1 §4",
          store.close_thread, con, "b6", "", "executor-3")
    holds("    and the thread is untouched", state_of(con, "b6") == "needs-you")

    print("\nP1 §5 -- attachments are instructions")
    put(con, thread("f1", ask=CHOICE))
    for bad, why in [(att(size=0), "a zero-byte file"),
                     (att(size=40 * 1024 * 1024), "a file over the size cap"),
                     (att(sha256="nope"), "no usable sha256"),
                     (att(mime=""), "no mime type"),
                     (att(name=""), "no filename"),
                     (att(name="../../etc/passwd"), "a path instead of a filename")]:
        fired("an attachment with %s" % why, "P1 §5", store.add_attachment, con, "f1", bad)
    holds("    and none of them landed", store.attachments_for(con, "f1") == [])
    store.add_attachment(con, "f1", att("f1-invoice.pdf"))
    d = fired("closing a card he has just sent a file to", "P1 §5 / LESSON 19",
              store.close_thread, con, "f1")
    holds("    and it says how to proceed anyway", d and "force=true" in d, d)
    fired("muting a card he has just sent a file to", "P1 §5 / LESSON 19",
          store.mute, con, "f1", "stop asking")
    holds("    the card is still his", state_of(con, "f1") == "needs-you")
    # Not a 400: a tap that must not settle, and must not pretend it did.
    put(con, thread("f2", ask=ACK))
    out = store.reply(con, "f2", "ack", attachments=[att("f2-quote.pdf")])
    holds("an ack tap carrying a file queues work instead of closing",
          out["state"] == "working" and out.get("settlement_suppressed") is True, out)
    holds("    and the file goes out with the answer",
          any(a["filename"] == "f2-quote.pdf" for p in store.pending_dispatch(con)["pending"]
              if p["thread_id"] == "f2" for a in p["attachments"]))

    print("\nported guards (the port is the baseline; these must never quietly lapse)")
    fired("a result with no note at all", "port",
          R, "b6", {"outcome": "needs-you", "state": "needs-you"})
    fired("a status word where the report should be", "port / LESSON 11",
          R, "b6", {"outcome": "ok", "state": "needs-you", "note": "done"})
    fired("the real report trapped inside 'outcome'", "port",
          R, "b6", {"outcome": "{'state':'needs-you','note':'...'}", "state": "needs-you",
                    "note": LONG, "ask": ACK})
    fired("an unknown state", "port",
          R, "b6", {"outcome": "ok", "state": "elsewhere", "note": LONG})
    fired("reopening something that is not done", "port", store.reopen_thread, con, "b6")
    fired_404("a result on a thread that does not exist", "port",
              R, "no-such-thread", {"outcome": "ok", "state": "needs-you", "note": LONG,
                                    "ask": ACK})
    fired_404("a file attached to a thread that does not exist", "P1 §5",
              store.add_attachment, con, "no-such-thread", att())
    bad = put(con, thread("q1", urgency="immediately"), thread("q2"))
    refused_at_door("an urgency outside the set, quietly coerced by nobody", "port",
                    bad, "q1")
    nameless = store.put_threads(con, {"threads": [{"title": "no id here"}]})
    holds("a thread with no stable id is refused", nameless["rejected_count"] == 1, nameless)

    print("\nP3b §1 -- the needs-you four-test: any test failing = NOT needs-you")
    # T1: a markerless choice ask at the producer's door
    refused_at_door("a choice ask with no only-he-can markers", "P3b §1 T1",
                    put(con, thread("nj1", ask={
                        "kind": "choice", "prompt": "Which way do you want this to go?",
                        "options": [{"label": "Accept the quote and book it",
                                     "value": "accept", "effect": "I book it"},
                                    {"label": "Push them on price",
                                     "value": "push", "effect": "I renegotiate"}]}),
                        thread("nj1b")), "nj1")
    # T1: an unknown marker is a validation 400, never a silent T1 pass
    refused_at_door("an unknown requires marker", "P3b §1.3",
                    put(con, thread("nj2", ask=dict(CHOICE, requires=["whim"])),
                        thread("nj2b")), "nj2")
    # T3: a duplicate concern at the producer's door
    put(con, thread("nj3", ask=CHOICE))
    refused_at_door("a second card for a concern already asked", "P3b §1 T3",
                    put(con, thread("nj4", title=con.execute(
                        "SELECT title FROM threads WHERE thread_id='nj3'"
                    ).fetchone()[0], ask=CHOICE), thread("nj4b")), "nj4")
    # T1/T2/T4 through the function itself (their door embodiments are the
    # ported invariants): each failing test named
    ok, failed = store.needs_joey(
        {"thread_id": "x", "title": "t", "ask": {"kind": "choice"}},
        {"duplicates": []})
    holds("T1 fires without markers", failed == "T1-only-he-can", failed)
    ok, failed = store.needs_joey(
        {"thread_id": "x", "title": "t", "ask": {"kind": "choice",
                                                 "requires": ["decision"]}},
        {"ask_ts": 100.0, "owner_event_ts": 200.0, "duplicates": []})
    holds("T2 fires on a newer owner event", failed == "T2-still-true-now", failed)
    ok, failed = store.needs_joey(
        {"thread_id": "x", "title": "t", "ask": {"kind": "choice",
                                                 "requires": ["decision"]}},
        {"duplicates": [{"thread_id": "y", "state": "needs-you",
                         "ask_kind": "choice", "last_change": 1}]})
    holds("T3 fires on an open duplicate ask", failed == "T3-he-doesnt-know", failed)
    ok, failed = store.needs_joey(
        {"thread_id": "x", "title": "t", "ask": {"kind": "choice",
                                                 "requires": ["decision"]}},
        {"duplicates": [], "ladder": {"retry_budget_left": 2,
                                      "self_fix_paths": []}})
    holds("T4 fires with retry budget left", failed == "T4-agent-cannot-fix", failed)
    ok, failed = store.needs_joey(
        {"thread_id": "x", "title": "t", "ask": {"kind": "choice",
                                                 "requires": ["decision"]}},
        {"duplicates": [], "ladder": {"retry_budget_left": 0,
                                      "self_fix_paths": []}})
    holds("all four pass with markers, freshness, no dupes, exhausted ladder",
          ok is True, (ok, failed))
    # positive controls at the door: markers land; a licensed resync lands
    res = put(con, thread("nj5", ask=dict(CHOICE, requires=["money"])))
    holds("markers land when stated", res.get("created") == 1, res)
    res = put(con, thread("nj5", ask=dict(CHOICE, requires=["money"])))
    holds("a licensed resync of the same ask lands", res.get("updated") == 1, res)

    print("\nP3b §4 -- bare cards never light 'what do you want'")
    put(con, thread("ba1", ask=CHOICE))
    store.reply(con, "ba1", "accept")
    d = fired("the banned phrase in an ack prompt (bot door)", "P3b §4 R1",
              R, "ba1", {"outcome": "asked", "state": "needs-you", "note": LONG,
                         "ask": {"kind": "ack",
                                 "prompt": "What do you want done about this?"}})
    holds("    and the refusal names the ban", d and "banned" in d, d)
    fired("the banned phrase in a choice prompt (bot door)", "P3b §4 R1",
          R, "ba1", {"outcome": "asked", "state": "needs-you", "note": LONG,
                     "ask": dict(CHOICE,
                                 prompt="What do you want: renew or replace?")})
    refused_at_door("the banned phrase at the producer's door", "P3b §4 R1",
                    put(con, thread("ba2", ask={"kind": "ack",
                                                "prompt": "what do you want to do"}),
                        thread("ba3")), "ba2")
    d = fired("a text ask with no question mark (bot door)", "P3b §4 R2",
              R, "ba1", {"outcome": "asked", "state": "needs-you", "note": LONG,
                         "ask": {"kind": "text",
                                 "prompt": "The vendor replied with a new quote."}})
    holds("    and it says to use an ack", d and "ack" in d, d)
    refused_at_door("a producer text statement (no '?')", "P3b §4 R2",
                    put(con, thread("ba4", ask={"kind": "text",
                                                "prompt": "It finished overnight."},
                                    requires=["decision"]), thread("ba5")), "ba4")
    # R3: the pure direction-demand is REWRITTEN at the bot's door
    out = R("ba1", {"outcome": "blocked-external", "state": "needs-you",
                    "note": LONG, "ask": {"kind": "text",
                                          "prompt": "How should we proceed?"}})
    holds("R3: a pure direction-demand is rewritten, not refused",
          out.get("rewritten_ask") is True, out)
    row = con.execute("SELECT ask_kind, ask_prompt FROM threads"
                      " WHERE thread_id='ba1'").fetchone()
    holds("R3: the stored ask is the ack-style summary",
          row["ask_kind"] == "ack" and row["ask_prompt"].startswith("Work stopped at"),
          dict(row))
    holds("R3: reply-anything-to-reopen is on the card",
          "Reply anything to reopen" in row["ask_prompt"], row["ask_prompt"])
    # the sweep: NO stored ask in this DB contains the banned phrase
    swept = 0
    for r in con.execute("SELECT ask_prompt, ask_options, ask_context FROM threads"):
        blob = " ".join([r["ask_prompt"] or "", r["ask_context"] or "",
                         r["ask_options"] or ""]).lower()
        if store.BANNED_ASK_PHRASE in blob:
            swept += 1
    holds("the banned phrase appears in NO stored ask", swept == 0, swept)

    print("\nP3c -- the learning loop: adoption is his tap, never code")
    # open: no/short candidate, no quote, no event ids, bad scope
    fired("a lesson candidate under the 12-char floor", "P3c §2",
          store.open_lesson_thread, con, "w", ["e"], "his words verbatim", "no")
    fired("a lesson with no quote (no verbatim evidence)", "P3c §2",
          store.open_lesson_thread, con, "w", ["e"], "", "a real rule candidate")
    fired("a lesson with no event ids (untraceable evidence)", "P3c §2",
          store.open_lesson_thread, con, "w", [], "his words", "a real rule candidate")
    fired("a lesson with an invalid scope key", "P3c §2",
          store.open_lesson_thread, con, "w", ["e"], "his words",
          "a real rule candidate", scope="Not A Slug!")
    # open: the door's own candidate is banned-phrase-checked (the producer
    # door fires inside open_lesson_thread's put_threads call)
    d = fired("a candidate carrying the banned phrase", "P3c §2 / P3b §4 R1",
              store.open_lesson_thread, con, "w", ["e"], "stop asking what do you want",
              "never store what do you want in an ask")
    holds("    and the door's refusal is the producer door's own",
          d and "banned" in d, d)
    # open: the same candidate again maps to the SAME stable id and lands as
    # a licensed resync -- triaged once, no second thread (P3b §1.2 license)
    out = store.open_lesson_thread(con, "w", ["e1"], "his words",
                                   "a rule about never rebooting in-use hosts",
                                   signals=("negation-imperative",))
    lt = out["lesson_thread_id"]
    out_again = store.open_lesson_thread(con, "w2", ["e2"], "his words again",
                                        "a rule about never rebooting in-use hosts")
    holds("re-opening the same candidate is a resync, never a second thread",
          out_again["lesson_thread_id"] == lt and out_again.get("updated") == 1
          and out_again.get("created") == 0, out_again)
    n_lesson_threads = con.execute(
        "SELECT COUNT(*) FROM threads WHERE thread_id LIKE 'lesson-%'"
    ).fetchone()[0]
    holds("    exactly one lesson thread exists for the candidate",
          n_lesson_threads == 1, n_lesson_threads)
    # adoption: never without his tap
    fired("adopting with no tap at all", "P3c §3",
          store.create_lesson_from_thread, con, lt)
    fired("adopting from a thread that is not a lesson thread", "P3c §3",
          store.create_lesson_from_thread, con, "b6")
    store.reply(con, lt, "adopt-with-edit")   # the tap, no note
    fired("adopt-with-edit with no edited wording", "P3c §3",
          store.create_lesson_from_thread, con, lt)
    holds("    and the tap with no note adopted nothing",
          store.lessons_list(con) == [], store.lessons_list(con))
    # a non-joey actor may not adopt even WITH a tap
    store.reply(con, lt, "adopt")
    fired("a programmatic actor adopting a lesson", "P3c §3",
          store.create_lesson_from_thread, con, lt, "executor-7")
    lesson = store.create_lesson_from_thread(con, lt)
    holds("his tap adopts: the lesson is active",
          lesson["status"] == "active" and store.lessons_list(con), lesson)
    # retire / supersede / gates
    fired("retiring with no reason", "P3c §3",
          store.retire_lesson, con, lesson["id"], "")
    fired_404("retiring a lesson that does not exist", "P3c §3",
              store.retire_lesson, con, "lsn-nope", "a real reason, just too late")
    store.retire_lesson(con, lesson["id"], "no longer true: the planner owns reboots")
    fired("retiring a lesson twice (a no-op wearing an audit trail)", "P3c §3",
          store.retire_lesson, con, lesson["id"], "retiring it again for nothing")
    holds("a retired lesson never renders into prompts again", "P3c §3 / LESSON 10",
          all(l["id"] != lesson["id"] for l in store.lessons_for_prompt(con, "any")))
    # supersede: nothing half-lands
    out2 = store.open_lesson_thread(con, "w", ["e3"], "his words",
                                    "a rule about always asking before probes")
    lt2 = out2["lesson_thread_id"]
    store.reply(con, lt2, "adopt")
    l2 = store.create_lesson_from_thread(con, lt2)
    n_before = len(store.lessons_list(con))
    fired("superseding with a non-rule", "P3c §3",
          store.supersede_lesson, con, l2["id"], "no")
    holds("    the refused supersede left the ledger untouched",
          len(store.lessons_list(con)) == n_before
          and store.lesson_get(con, l2["id"])["status"] == "active",
          (len(store.lessons_list(con)), n_before))
    fired("superseding a retired lesson", "P3c §3",
          store.supersede_lesson, con, lesson["id"], "a valid replacement rule text")
    fired("marking a non-GATE lesson implemented", "P3c §5",
          store.mark_gate_implemented, con, l2["id"], "abc123")
    # GATE lifecycle: pending -> implemented, the commit recorded
    out3 = store.open_lesson_thread(con, "w", ["e4"], "gate words",
                                    "GATE: refuse unmetered probes everywhere")
    lt3 = out3["lesson_thread_id"]
    store.reply(con, lt3, "adopt")
    l3 = store.create_lesson_from_thread(con, lt3)
    holds("an adopted GATE lesson is pending until implemented",
          l3["gate_status"] == "pending"
          and l3["id"] in [p["id"] for p in store.lessons_pending_gates(con)],
          l3)
    store.mark_gate_implemented(con, l3["id"], "f00f00f")
    holds("an implemented gate leaves the pending listing with the commit",
          l3["id"] not in [p["id"] for p in store.lessons_pending_gates(con)]
          and store.lesson_get(con, l3["id"])["changelog"][-1]["commit"] == "f00f00f",
          store.lessons_pending_gates(con))
    fired("marking a gate implemented twice", "P3c §5",
          store.mark_gate_implemented, con, l3["id"], "again")
    # the sweep: no stored LESSON ask anywhere contains the banned phrase
    for r in con.execute(
            "SELECT ask_prompt, ask_options, ask_context FROM threads"
            " WHERE thread_id LIKE 'lesson-%'"):
        blob = " ".join([r["ask_prompt"] or "", r["ask_context"] or "",
                         r["ask_options"] or ""]).lower()
        holds("lesson ask clean of the banned phrase",
              store.BANNED_ASK_PHRASE not in blob, blob[:80])

    # ---------------------------------------------------------------
    print("\nP7: the terminal no-action verdict (docs/contracts/P7.md §2)")
    # The verdict is a literal-boolean assertion that settles ONLY on his
    # reply; every other shape must 400 and leave the thread untouched.
    put(con, thread("P7a", ask=CHOICE))
    store.reply(con, "P7a", "note-only", note="record this, nothing to do")
    fired("P7 no_action as a string (prose is not an assertion)", "P7 §2 / LESSON 12",
          R, "P7a", {"outcome": "note-only", "state": "done",
                     "no_action": "true", "note": NOTE})
    holds("    and the thread never left the work queue",
          state_of(con, "P7a") == "working")
    fired("P7 no_action without settling (state != done)", "P7 §2",
          R, "P7a", {"outcome": "note-only", "state": "needs-you",
                     "no_action": True, "note": NOTE,
                     "ask": {"kind": "ack", "prompt": "Read and settle."}})
    holds("    and the thread never left the work queue",
          state_of(con, "P7a") == "working")
    put(con, thread("P7b", ask=CHOICE))
    fired("P7 no_action with no reply of his to settle on", "P7 §2 / LESSON 15",
          R, "P7b", {"outcome": "note-only", "state": "done",
                     "no_action": True, "note": NOTE})
    holds("    and the card is still his to answer",
          state_of(con, "P7b") == "needs-you")
    # the answer gate is not exempted by the verdict
    put(con, thread("P7c", ask=CHOICE))
    store.reply(con, "P7c", "note-only",
                note="record this; also, which port did you pick?")
    fired("P7 no_action that drops his question", "P7 §2 / P1 §2",
          R, "P7c", {"outcome": "note-only", "state": "done",
                     "no_action": True, "note": NOTE})
    holds("    and the thread never left the work queue",
          state_of(con, "P7c") == "working")
    # the SAME close shape without the flag still hits the old refusal
    fired("P7 state done with no verdict at all (bot-close unchanged)",
          "P7 §2 / P1 §4",
          R, "P7c", {"outcome": "done", "state": "done", "note": NOTE,
                     "answers": "Port 3038, the one it publishes."})
    holds("    and the thread never left the work queue",
          state_of(con, "P7c") == "working")
    # and the verdict itself DOES settle when every gate is satisfied (the
    # positive control: a probe suite that only ever refuses proves nothing)
    out = store.post_result(con, "P7c", {
        "outcome": "note-only", "state": "done", "no_action": True,
        "note": NOTE, "answers": "Port 3038, the one it publishes."})
    holds("P7 the satisfied verdict settles done (positive control)",
          out.get("state") == "done" and out.get("closed_by") == "no-action-settle",
          out)

    con.close()
    print("\n%d passed, %d failed" % (PASS, FAIL))
    if FAIL:
        print("A gate that does not fire is not a gate (PLAN standing rule 2).")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(run())
