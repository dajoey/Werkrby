"""Attachments are instructions: a file sent with a tap never settles the card.

Run: python3 tests/test_attachments.py   (from the repo root)

Contract: docs/contracts/P1.md §5. LESSON 19 (a file attached to a reply is an
instruction, like a binding note), supporting 15 (the note rides with the answer and
wins), 11 (history is event bodies, not status words).

The failure: he sends the invoice and taps Done in the same motion. The tap settles the
card, the file goes nowhere, and nobody finds out until the work comes back wrong -- or
never comes back at all. Sending a file and closing in one act is the single combination
that discards the thing the person just took the trouble to send.
"""
from __future__ import annotations

import hashlib
import os
import sys
import tempfile

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


def sha(seed):
    return hashlib.sha256(str(seed).encode()).hexdigest()


def meta(name="invoice-4821.pdf", **kw):
    m = {"filename": name, "mime": "application/pdf", "size": 184320,
         "sha256": sha(name), "url": "https://helm.local/files/" + name,
         "caption": "the invoice they sent this morning"}
    m.update(kw)
    return m


ACK = {"kind": "ack", "prompt": "Read this and close it when you have."}
# P3b §1.5/§4: the original prompt ("What do you want done?") hits the R1
# banned-phrase ban -- a fixture text a new gate legitimately rejects, named
# here per P1 §0. Rephrased; assertion untouched.
TERMINAL = {"kind": "choice", "prompt": "The vendor invoice arrived. How should it be handled?",
            "requires": ["decision"],
            "options": [{"label": "Nothing, it is already paid", "value": "paid",
                         "effect": "Settles it here; nothing is sent anywhere",
                         "terminal": True},
                        {"label": "Query the amount with them", "value": "query",
                         "effect": "I go back to the vendor on the amount"}]}


_TITLES = iter(["the relay cert", "the vendor invoice", "the site survey", "the spare tills",
                "the courier account", "the lease renewal", "the phone bill",
                "the stock count", "the door code", "the printer lease",
                "the fuel cards", "the alarm contract"])


def put(con, tid, ask=None, state="needs-you"):
    # A distinct title per thread on purpose: mute() suppresses by fingerprint,
    # and a shared title would auto-retire every card filed after the mute test.
    return store.put_threads(con, {"threads": [{
        "thread_id": tid, "title": next(_TITLES), "summary": "a summary",
        "category": "Fleet", "urgency": "this-week", "state": state,
        "ask": ACK if ask is None else ask}]})


def state_of(con, tid):
    return con.execute("SELECT state FROM threads WHERE thread_id=?", (tid,)).fetchone()[0]


def refused(fn, *a, **kw):
    try:
        return None, fn(*a, **kw)
    except store.StoreError as e:
        return e.detail, None


def run():
    tmp = os.path.join(tempfile.mkdtemp(prefix="helm-att-"), "t.db")
    con = store.db(tmp)

    print("\nwhat the store keeps about a file is metadata, and it says so in history")
    put(con, "at-1")
    out = store.add_attachment(con, "at-1", meta())
    check("accepted", out["filename"] == "invoice-4821.pdf", out)
    body = con.execute("SELECT body FROM events WHERE thread_id='at-1'"
                       " AND kind='joey-attachment'").fetchone()["body"]
    check("history names the file", "invoice-4821.pdf" in body, body)
    check("and its size in words a person reads", "180.0 KB" in body, body)
    check("and where it is", "helm.local/files" in body, body)
    check("and tells the reader to open it before answering",
          "OPEN IT BEFORE YOU ANSWER" in body, body)
    check("the caption rides along", "invoice they sent" in body, body)

    print("\nmetadata that would produce an unopenable file is refused")
    for bad, why in [(meta(size=0), "zero bytes"),
                     (meta(size=40 * 1024 * 1024), "over the size cap"),
                     (meta(sha256="not-a-hash"), "no usable sha256"),
                     (meta(mime=""), "no mime type"),
                     (meta(name=""), "no filename"),
                     (meta(name="../../etc/passwd"), "a path instead of a name")]:
        detail, ok = refused(store.add_attachment, con, "at-1", bad)
        check("refused: " + why, detail is not None, ok)

    print("\nthe same bytes twice is a retry, not a second instruction")
    again = store.add_attachment(con, "at-1", meta())
    check("deduped on sha256", again.get("duplicate") is True, again)
    check("one row, not two", len(store.attachments_for(con, "at-1")) == 1)

    print("\nan ack tap that carries a file does not settle -- it queues the work")
    put(con, "at-2")
    out = store.reply(con, "at-2", "ack", note="here's the invoice",
                      attachments=[meta("po-77.pdf")])
    check("not done", out["state"] == "working", out)
    check("and it says so, rather than pretending the tap worked",
          out.get("settlement_suppressed") is True, out)
    check("the reason names the file", "po-77.pdf" in out.get("reason", ""), out)
    check("the reason says a file is an instruction",
          "instruction" in out.get("reason", ""), out)
    check("the ask is still on the card, so he can still close it",
          con.execute("SELECT ask_kind FROM threads WHERE thread_id='at-2'"
                      ).fetchone()[0] == "ack")
    hist = [e["body"] for e in con.execute(
        "SELECT body FROM events WHERE thread_id='at-2' ORDER BY ts")]
    check("history records that the tap did not settle it",
          any("not settled on this tap" in b for b in hist), hist)

    print("\nthe file reaches the executor with the answer")
    pend = store.pending_dispatch(con)["pending"]
    mine = next(p for p in pend if p["thread_id"] == "at-2")
    check("pending_dispatch carries it",
          [a["filename"] for a in mine["attachments"]] == ["po-77.pdf"], mine)
    check("with the url the bytes are actually at",
          mine["attachments"][0]["url"].endswith("po-77.pdf"), mine)

    print("\nonce a run has reported, the file is consumed and the tap settles again")
    store.post_result(con, "at-2", {"outcome": "read it", "state": "needs-you",
                                    "note": NOTE, "ask": ACK})
    check("nothing is unconsumed now", store.unconsumed_attachments(con, "at-2") == [])
    check("and the ack tap closes it", store.reply(con, "at-2", "ack")["state"] == "done")

    print("\na terminal option with a file attached does not settle either")
    put(con, "at-3", ask=TERMINAL)
    out = store.reply(con, "at-3", "paid", attachments=[meta("remittance.pdf")])
    check("the terminal option did not end it", out["state"] == "working", out)
    check("and said why", out.get("settlement_suppressed") is True, out)

    print("\na file attached separately counts the same as one sent with the tap")
    put(con, "at-4", ask=TERMINAL)
    store.add_attachment(con, "at-4", meta("statement.pdf"))
    out = store.reply(con, "at-4", "paid")
    check("still not settled", out["state"] == "working", out)
    check("the files are listed on the reply",
          out.get("attachments") == ["statement.pdf"], out)

    print("\na non-settling answer is unaffected: it was always going to queue work")
    put(con, "at-5", ask=TERMINAL)
    out = store.reply(con, "at-5", "query", attachments=[meta("thread.eml", mime="message/rfc822")])
    check("working, as it would have been anyway", out["state"] == "working", out)
    check("and it is not reported as a suppressed settle",
          "settlement_suppressed" not in out, out)

    print("\nthe Close button refuses to drop a file he just sent")
    put(con, "at-6")
    store.add_attachment(con, "at-6", meta("contract.pdf"))
    detail, ok = refused(store.close_thread, con, "at-6")
    check("refused", detail is not None, ok)
    check("the refusal names the file", "contract.pdf" in str(detail), detail)
    check("and says exactly how to proceed", "force=true" in str(detail), detail)
    check("the thread is untouched", state_of(con, "at-6") == "needs-you")
    check("and the tap still works when it is meant",
          store.close_thread(con, "at-6", force=True)["state"] == "done")

    print("\nso does mute, which is a close tap with a rule attached")
    put(con, "at-7")
    store.add_attachment(con, "at-7", meta("scan.png", mime="image/png"))
    detail, ok = refused(store.mute, con, "at-7")
    check("refused", detail is not None, ok)
    check("the thread is untouched", state_of(con, "at-7") == "needs-you")
    check("forced mute still works", store.mute(con, "at-7", "not this again", force=True)
          ["accepted"] is True)

    print("\nthe whole route: file + ack tap -> a run -> his close")
    put(con, "at-8")
    store.reply(con, "at-8", "ack", attachments=[meta("quote-9.pdf")])
    check("the tap queued work instead of closing", state_of(con, "at-8") == "working")
    ack_reply = con.execute("SELECT body FROM events WHERE thread_id='at-8'"
                            " AND kind='joey-reply'").fetchone()["body"]
    check("his ack is on the record", ack_reply == "ack", ack_reply)
    # The run reports, and because he DID tap the ack, the bot-close rule's
    # first exception applies (contract: P1 §4a): the store is catching up with
    # a close he already made, not making one.
    out = store.post_result(con, "at-8", {"outcome": "filed it", "state": "done",
                                          "note": NOTE})
    check("the run may close it on the ack he tapped",
          out["state"] == "done" and out.get("closed_by") == "ack-tapped", out)

    print("\nthe file is never in the store; only what it is and where it is")
    cols = {r["name"] for r in con.execute("PRAGMA table_info(attachments)")}
    check("no blob column", "data" not in cols and "bytes" not in cols and "blob" not in cols,
          cols)
    check("the columns are the contract's",
          {"att_id", "thread_id", "ts", "filename", "mime", "size", "sha256", "url",
           "uploader", "caption"} == cols, cols)

    print("\nan unknown thread is a 404, not a silent file in the void")
    detail, ok = refused(store.add_attachment, con, "no-such-thread", meta())
    check("refused", detail is not None, ok)

    con.close()
    print("\n%d passed, %d failed" % (PASS, FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(run())
