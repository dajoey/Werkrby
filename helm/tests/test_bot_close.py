"""Bot-close rules: only Joey closes, and the two exceptions are his own taps.

Run: python3 tests/test_bot_close.py   (from the repo root)

Contract: docs/contracts/P1.md §4. LESSONS 13 (ack settles on the tap; a redirect is not
an ack), 18 (only Joey closes), 12 (completion is asserted, never inferred from prose),
10 (a card never lies about its state).

The failure: a run decides for itself that a thread is finished, sets 'done', and the
work leaves the list without anyone having agreed it was over. The list is only worth
reading if the things on it are the things that are actually outstanding.
"""
from __future__ import annotations

import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helm import store  # noqa: E402

PASS = FAIL = 0
NOTE = ("The run did the work and this is the long-enough report about what happened. "
        + "z" * 40)


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("  ok   " + name)
    else:
        FAIL += 1
        print("  FAIL " + name + ("  <- " + str(detail) if detail else ""))


CHOICE = {"kind": "choice", "prompt": "The vendor wants an answer by Friday. Which way?",
          "options": [{"label": "Accept the quote and book the install", "value": "accept",
                       "effect": "I accept it and book the install"},
                      {"label": "Push them for a better price", "value": "push",
                       "effect": "I go back to them on price"}],
            "requires": ["decision"]}
ACK = {"kind": "ack", "prompt": "Read this and close it when you have."}


_DIG = {"0": "zero", "1": "one", "2": "two", "3": "three", "4": "four",
        "5": "five", "6": "six", "7": "seven", "8": "eight", "9": "nine"}


def put(con, tid, ask=None, state="needs-you", extra=None):
    # P3b §1.5: a dedupe-distinct title (T3 fingerprints titles and strips
    # digits, so the digits are SPELLED OUT) + the T1 marker on default
    # choice asks. An explicit ask keeps whatever markers it carries.
    ask = CHOICE if ask is None else ask
    if str(ask.get("kind")) in ("choice", "text"):
        ask = dict(ask, requires=ask.get("requires", ["decision"]))
    t = {"thread_id": tid,
         "title": "concern " + "".join(_DIG.get(c, c) for c in tid),
         "summary": "a summary",
         "category": "Fleet", "urgency": "this-week", "state": state,
         "ask": ask}
    t.update(extra or {})
    return store.put_threads(con, {"threads": [t]})


def result(con, tid, body):
    """(202, out) or (status, detail) -- what the bot's door said."""
    try:
        return 202, store.post_result(con, tid, body)
    except store.StoreError as e:
        return e.status, e.detail


def state_of(con, tid):
    return con.execute("SELECT state FROM threads WHERE thread_id=?", (tid,)).fetchone()[0]


def thread_of(con, tid):
    return next(t for t in store.list_threads(con, include_done=True)["threads"]
                if t["thread_id"] == tid)


def run():
    tmp = os.path.join(tempfile.mkdtemp(prefix="helm-botclose-"), "t.db")
    con = store.db(tmp)
    done = {"outcome": "finished", "state": "done", "note": NOTE}

    print("\na bot may not close a thread nobody has answered")
    put(con, "bc-1")
    st, detail = result(con, "bc-1", dict(done))
    check("refused", st == 400, detail)
    check("and it says who does close threads", "only Joey closes" in detail, detail)
    check("the thread is still lit", state_of(con, "bc-1") == "needs-you")

    print("\nan answered decision needs BOTH assertions, in this result, as booleans")
    put(con, "bc-2")
    store.reply(con, "bc-2", "accept")
    st, detail = result(con, "bc-2", dict(done))
    check("refused with neither flag", st == 400, detail)
    check("and it names both flags",
          "answer_applied" in detail and "nothing_owed" in detail, detail)
    st, detail = result(con, "bc-2", dict(done, answer_applied=True))
    check("refused with only answer_applied", st == 400, detail)
    st, detail = result(con, "bc-2", dict(done, answer_applied=True, nothing_owed=False))
    check("refused when nothing_owed is explicitly false", st == 400, detail)
    st, detail = result(con, "bc-2", dict(done, answer_applied="true", nothing_owed=True))
    check("refused when an assertion arrives as prose", st == 400, detail)
    check("and it says it has to be a literal true/false",
          "literal true or false" in str(detail), detail)
    st, out = result(con, "bc-2", dict(done, answer_applied=True, nothing_owed=True))
    check("accepted when both are asserted", st == 202, out)
    check("the thread is done", state_of(con, "bc-2") == "done")
    check("and the exception it used is recorded",
          isinstance(out, dict) and out.get("closed_by") == "answered-decision", out)
    got = thread_of(con, "bc-2")
    check("the assertions are stored on the thread, not merely believed",
          got["answer_applied"] and got["nothing_owed"], got)
    check("and a closed card is not still lit", got["ask"]["kind"] == "none", got["ask"])

    print("\nthe reply has to be NEWER than the question on the card")
    put(con, "bc-3")
    store.reply(con, "bc-3", "accept")
    st, out = result(con, "bc-3", {"outcome": "asked", "state": "needs-you", "note": NOTE,
                                   "ask": CHOICE})
    check("the run asks again", st == 202, out)
    st, detail = result(con, "bc-3", dict(done, answer_applied=True, nothing_owed=True))
    check("his older answer cannot close the newer question", st == 400, detail)
    check("and it says why", "newer than the question" in str(detail), detail)
    store.reply(con, "bc-3", "push")
    st, out = result(con, "bc-3", dict(done, answer_applied=True, nothing_owed=True))
    check("his answer to THAT question can", st == 202, out)

    print("\na tap is not a decision")
    for body in ["ack", "close", "note-only", "__redirect__"]:
        tid = "bc-tap-" + body.strip("_")
        put(con, tid)
        store.reply(con, tid, body)
        st, detail = result(con, tid, dict(done, answer_applied=True, nothing_owed=True))
        check("refused after a %r reply" % body, st == 400, detail)

    print("\na new answer from him wipes the assertions made about the last one")
    put(con, "bc-4")
    store.reply(con, "bc-4", "accept")
    st, out = result(con, "bc-4", {"outcome": "asked", "state": "needs-you", "note": NOTE,
                                   "ask": CHOICE, "answer_applied": True,
                                   "nothing_owed": True})
    check("a result that asks a new question lands", st == 202, out)
    got = thread_of(con, "bc-4")
    check("but a new question means something is owed again",
          not got["answer_applied"] and not got["nothing_owed"], got)
    store.reply(con, "bc-4", "push")
    got = thread_of(con, "bc-4")
    check("and a fresh answer clears them too",
          not got["answer_applied"] and not got["nothing_owed"], got)
    st, detail = result(con, "bc-4", dict(done))
    check("so the close has to be asserted again, about this answer", st == 400, detail)

    print("\na done result may not carry a new question")
    put(con, "bc-5")
    store.reply(con, "bc-5", "accept")
    st, detail = result(con, "bc-5", dict(done, answer_applied=True, nothing_owed=True,
                                          ask=CHOICE))
    check("refused", st == 400, detail)
    check("and it says a card that is asking is needs-you", "not done" in str(detail), detail)

    print("\nLESSON 13: a redirect on an ack card is not an ack")
    put(con, "bc-6", ask=ACK)
    store.reply(con, "bc-6", "__redirect__", note="Actually chase the other invoice first")
    check("the redirect did not settle it", state_of(con, "bc-6") == "working")
    st, detail = result(con, "bc-6", dict(done, answer_applied=True, nothing_owed=True))
    check("and it cannot be closed on that", st == 400, detail)
    check("the refusal quotes what he actually sent", "__redirect__" in str(detail), detail)

    print("\nack-tapped: the one close he really did make")
    # This is the row an ack tap leaves behind when the settle is suppressed
    # (gate 5: he attached a file with the tap, so the files have to reach a run
    # first). Built directly here because this suite asserts the RULE;
    # test_attachments.py drives the whole route end to end.
    put(con, "bc-7", ask=ACK)
    store.reply(con, "bc-7", "__redirect__")
    store.add_event(con, "bc-7", time.time(), "joey", "", "joey-reply", "ack")
    con.execute("UPDATE threads SET state='working', ask_kind='ack' WHERE thread_id=?",
                ("bc-7",))
    con.commit()
    st, out = result(con, "bc-7", dict(done))
    check("accepted with no assertions: he closed it, the store is catching up",
          st == 202, out)
    check("recorded as the ack exception",
          isinstance(out, dict) and out.get("closed_by") == "ack-tapped", out)
    check("and it is done", state_of(con, "bc-7") == "done")

    print("\nan ever-parked thread: the ack ask has to be IN the done post")
    put(con, "bc-8", ask={"kind": "none"}, state="waiting-external",
        extra={"blocked_on": "Acme support, case 4821"})
    check("nothing has ever been asked of him on it",
          state_of(con, "bc-8") == "waiting-external")
    st, detail = result(con, "bc-8", dict(done))
    check("a bare done is refused", st == 400, detail)
    check("and the refusal says exactly how to finish the card",
          "ack" in str(detail) and "IN this post" in str(detail), detail)
    st, out = result(con, "bc-8", dict(done, ask=ACK))
    check("done carrying the ack ask is accepted", st == 202, out)
    check("but it is NOT reported as closed", out["state"] == "needs-you", out)
    check("it is reported as a close waiting on his tap",
          out.get("close_requested") is True and out.get("closes_on") == "his ack tap", out)
    check("the card is lit with the ack", state_of(con, "bc-8") == "needs-you")
    hist = [e["body"] for e in con.execute(
        "SELECT body FROM events WHERE thread_id='bc-8' ORDER BY ts")]
    check("and history says, in words, that a bot asked to close it",
          any("asked to close it" in b for b in hist), hist)
    check("his tap ends it", store.reply(con, "bc-8", "ack")["state"] == "done")

    print("\nthe question is an event, so 'newer than the ask' survives the column rewrite")
    put(con, "bc-9")
    store.reply(con, "bc-9", "accept")
    result(con, "bc-9", {"outcome": "asked", "state": "needs-you", "note": NOTE, "ask": ACK})
    asks = con.execute("SELECT body FROM events WHERE thread_id='bc-9' AND kind='bot-ask'"
                       ).fetchall()
    check("a bot-ask event was written", len(asks) == 1, asks)
    check("carrying the question as text", "Read this" in asks[0]["body"], asks[0]["body"])

    print("\na re-filed id does not inherit the last life's answer")
    put(con, "bc-10")
    store.reply(con, "bc-10", "accept")
    store.add_event(con, "bc-10", time.time(), "helm", "", "state-change",
                    "retired: no longer true")
    result(con, "bc-10", {"outcome": "asked", "state": "needs-you", "note": NOTE,
                          "ask": CHOICE})
    st, detail = result(con, "bc-10", dict(done, answer_applied=True, nothing_owed=True))
    check("the previous life's reply cannot close this one", st == 400, detail)

    print("\na late result on an already-closed thread is not a bot closing anything")
    put(con, "bc-11", ask=ACK)
    store.reply(con, "bc-11", "ack")
    check("his tap closed it", state_of(con, "bc-11") == "done")
    st, out = result(con, "bc-11", dict(done))
    check("the late report lands instead of looking like a failed run", st == 202, out)
    check("recorded as already closed",
          isinstance(out, dict) and out.get("closed_by") == "already-closed", out)

    print("\nthe human close door is human-only")
    put(con, "bc-12")
    try:
        store.close_thread(con, "bc-12", "", actor="executor-3")
        st, detail = 202, ""
    except store.StoreError as e:
        st, detail = e.status, e.detail
    check("a bot at the close door is refused", st == 400, detail)
    check("and is pointed at the two mechanical exceptions",
          "answer_applied" in str(detail) and "ack" in str(detail), detail)
    check("the thread is untouched", state_of(con, "bc-12") == "needs-you")
    check("his own close still works",
          store.close_thread(con, "bc-12", "not worth doing")["state"] == "done")
    put(con, "bc-13")
    check("and the default caller is still him (the UI's Close button)",
          store.close_thread(con, "bc-13")["state"] == "done")

    con.close()
    print("\n%d passed, %d failed" % (PASS, FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(run())
