"""Answer gate: a question in the owner's note is answered before anything else.

Run: python3 tests/test_answer_gate.py   (from the repo root)

Contract: docs/contracts/P1.md §2. LESSON 16 (a question in the owner's note requires an
`answers` field rendered first), supported by 11 (history is a report, not a status word)
and 33 (facts about the data are decided in code, not by a model).

The failure: a binding note is read by whoever executes the answer, and a question inside
it has nowhere to be answered. The run would go and do the work, report what it did, and
the question would simply never be mentioned again -- so the card looked like it had been
dealt with while the one thing actually asked for was dropped.
"""
from __future__ import annotations

import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helm import store  # noqa: E402

PASS = FAIL = 0
NOTE = "I went and did the thing, and here is the long-enough report about it. " + "z" * 40


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("  ok   " + name)
    else:
        FAIL += 1
        print("  FAIL " + name + ("  <- " + str(detail) if detail else ""))


def status(fn, *a, **kw):
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


ASK = {"kind": "choice", "prompt": "Send the draft, or hold it?",
       "requires": ["decision"],   # P3b §1.5: T1 marker, four-test at the door
       "options": [{"label": "Send the draft", "value": "send", "effect": "it goes out"},
                   {"label": "Hold the draft", "value": "hold", "effect": "nothing goes out"}]}


_DIG = {"0": "zero", "1": "one", "2": "two", "3": "three", "4": "four",
        "5": "five", "6": "six", "7": "seven", "8": "eight", "9": "nine"}


def armed(con, tid, note):
    """A thread the owner has answered, with a binding note riding along."""
    # P3b §1.5: T1 marker on the ask + a dedupe-distinct title (T3
    # fingerprints titles; a shared default made every thread a duplicate).
    store.put_threads(con, {"threads": [{
        "thread_id": tid, "title": "concern " + "".join(_DIG.get(c, c) for c in tid),
        "summary": "a summary",
        "category": "Fleet", "urgency": "this-week", "state": "needs-you",
        "ask": dict(ASK, requires=ASK.get("requires", ["decision"]))}]})
    store.reply(con, tid, "send", note=note)
    return tid


def last_body(con, tid):
    r = con.execute("SELECT body FROM events WHERE thread_id=? AND kind='bot-result'"
                    " ORDER BY ts DESC LIMIT 1", (tid,)).fetchone()
    return r["body"] if r else ""


def run():
    tmp = os.path.join(tempfile.mkdtemp(prefix="helm-answers-"), "t.db")
    con = store.db(tmp)

    print("\na question in the note must be answered, not worked around")
    armed(con, "a-1", "Send it, but why did the first attempt bounce?")

    def result(**kw):
        body = {"outcome": "sent", "state": "needs-you", "note": NOTE,
                "ask": {"kind": "ack", "prompt": "Read the report and close this."}}
        body.update(kw)
        return lambda: store.post_result(con, "a-1", body)

    check("a result with no answers is refused", status(result()) == 400)
    d = detail_of(result())
    check("and quotes the question back verbatim, so the run can see what it missed",
          "why did the first attempt bounce?" in d, d)
    check("and names the field that fixes it", "'answers'" in d or "answers" in d, d)

    print("\nan answer that says nothing is the status-word failure again")
    check("a one-word answer is refused", status(result(answers="yes")) == 400)
    check("and says how long it has to be", "12" in detail_of(result(answers="yes")),
          detail_of(result(answers="yes")))
    check("a non-string, non-list answers is refused", status(result(answers=42)) == 400)
    check("a pair with no answer in it is refused",
          status(result(answers=[{"question": "why did it bounce?"}])) == 400)
    check("an empty list is refused too, rather than reading as an answer",
          status(result(answers=[])) == 400)

    print("\na real answer lands, and is rendered FIRST")
    out = store.post_result(con, "a-1", {
        "outcome": "sent", "state": "needs-you", "note": NOTE,
        "answers": "It bounced because the recipient domain rejected the old relay IP; "
                   "the new relay is authorised and this one went through.",
        "ask": {"kind": "ack", "prompt": "Read the report and close this."}})
    check("accepted", out["accepted"] is True, out)
    check("the response says the question was answered", out.get("answered") is True, out)
    body = last_body(con, "a-1")
    check("the history opens with the answer, above the report",
          body.startswith("ANSWERS TO YOUR QUESTION"), body[:80])
    check("the answer text is in it", "rejected the old relay IP" in body, body[:200])
    check("and the report is still there, below it", NOTE[:40] in body, body[-120:])
    check("the outcome prefix did not jump ahead of the answer",
          body.index("ANSWERS") < body.index("[sent]") if "[sent]" in body else True, body[:120])

    print("\nonce answered, the question is consumed and not demanded again")
    store.reply(con, "a-1", "ack")
    check("a later result on the same thread needs no answers", status(
        store.post_result, con, "a-1",
        {"outcome": "checked", "state": "waiting-external", "note": NOTE,
         "blocked_on": "the relay vendor's confirmation"}) == 202)

    print("\nthe question/answer pair form renders the question with its answer")
    armed(con, "a-pairs", "Two things: is the relay fixed? and did the batch resend?")
    d = detail_of(lambda: store.post_result(con, "a-pairs", {
        "outcome": "done", "state": "needs-you", "note": NOTE,
        "ask": {"kind": "ack", "prompt": "Read it and close."}}))
    check("both questions are quoted, not just the first",
          "is the relay fixed?" in d and "did the batch resend?" in d, d)
    store.post_result(con, "a-pairs", {
        "outcome": "done", "state": "needs-you", "note": NOTE,
        "answers": [{"question": "is the relay fixed?",
                     "answer": "Yes, the new relay has been authorised since 09:40."},
                    {"question": "did the batch resend?",
                     "answer": "Yes, all 14 messages went out on the second pass."}],
        "ask": {"kind": "ack", "prompt": "Read it and close."}})
    body = last_body(con, "a-pairs")
    check("the pairs render as question then answer",
          body.index("is the relay fixed?") < body.index("authorised since 09:40"), body[:200])
    check("the second pair is rendered too", "second pass" in body, body[:300])
    check("all of it above the report", body.index("second pass") < body.index(NOTE[:20]),
          body[:300])

    print("\nno question, no ceremony")
    armed(con, "a-none", "Send it. The draft reads fine to me.")
    check("a note with no question needs no answers", status(
        store.post_result, con, "a-none",
        {"outcome": "sent", "state": "needs-you", "note": NOTE,
         "ask": {"kind": "ack", "prompt": "Read it and close."}}) == 202)
    check("answers volunteered anyway are still rendered, not rejected", status(
        store.post_result, con, "a-none",
        {"outcome": "sent", "state": "needs-you", "note": NOTE,
         "answers": "Nothing was asked, but here is the detail you would want.",
         "ask": {"kind": "ack", "prompt": "Read it and close."}}) == 202)
    check("and rendered first all the same",
          last_body(con, "a-none").startswith("ANSWERS TO YOUR QUESTION"),
          last_body(con, "a-none")[:60])

    print("\nno state excuses leaving the question unanswered")
    armed(con, "a-park", "Do it, but is the vendor case still open?")
    check("parking does not excuse it", status(
        store.post_result, con, "a-park",
        {"outcome": "parked", "state": "waiting-external", "note": NOTE,
         "blocked_on": "the vendor, case 4821"}) == 400)
    check("nor does snoozing", status(
        store.post_result, con, "a-park",
        {"outcome": "deferred", "state": "snoozed", "note": NOTE,
         "wake_at": time.time() + 3 * 86400}) == 400)
    check("with the answer, the park lands", status(
        store.post_result, con, "a-park",
        {"outcome": "parked", "state": "waiting-external", "note": NOTE,
         "blocked_on": "the vendor, case 4821",
         "answers": "The case is still open; the vendor last touched it on Tuesday."})
          == 202)

    print("\nevery unconsumed note is read, not just the newest one")
    armed(con, "a-two", "Do it, but which relay is it using now?")
    store.reply(con, "a-two", "send", note="Actually just go ahead, no rush.")
    d = detail_of(lambda: store.post_result(con, "a-two", {
        "outcome": "sent", "state": "needs-you", "note": NOTE,
        "ask": {"kind": "ack", "prompt": "Read it and close."}}))
    check("a question in an EARLIER unanswered note still gates the result",
          "which relay is it using now?" in d, d)

    print("\nthe detection is code, not judgement")
    qs = store.questions_in("Ship it. Why did it bounce? Also, when does the cert expire?")
    check("each question is pulled out on its own", len(qs) == 2, qs)
    check("and kept verbatim for quoting back",
          qs[0].endswith("Why did it bounce?") and qs[1].endswith("when does the cert expire?"),
          qs)
    check("prose with no question mark yields nothing",
          store.questions_in("Ship it. It reads fine.") == [], store.questions_in("Ship it."))

    con.close()
    print("\n%d passed, %d failed" % (PASS, FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(run())
