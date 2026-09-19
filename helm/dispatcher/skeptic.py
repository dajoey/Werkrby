"""dispatcher/skeptic.py — the second-lane review (contract: P3b §3).

At the round cap the artifact no longer hands back bare: it goes to a
skeptic-lite review — a REAL executor spawn at RUNTIME on the other runtime
(in tests the stub executor plays the reviewer), claimed before spawn, jailed
like any run. This module holds the deterministic parts: the review prompt
template, the verdict reader, and the three outcome shapes (ready result,
needs-joey escalation with derived options, fallback ack summary). No model
in here, no network, no state — the tick orchestrates, this renders and
parses (LESSON 33: the shapes are facts, not judgment).

Verdict rules (rendered into the prompt verbatim):
  ready       the artifact plainly satisfies the decision he made
  needs-work  ONE more bounded round could finish it
  needs-joey  the next step is his decision, money, credential, presence,
              or a person only he can contact
"""
from __future__ import annotations

import json
import os

from .common import clip, read_text

REVIEW_RESULT = "review-result.json"
VERDICTS = ("ready", "needs-work", "needs-joey")
MIN_REVIEW_NOTE = 80   # LESSON 11: a verdict word alone says nothing

REVIEWER_EXECUTOR_ID = "helm-skeptic-lite"
# The responder the review artifacts post under. Excluded from bot_rounds()
# in tick.py: a review is not a run-produced round (same class as the P3
# roundcap hand-back bug).
REVIEW_RESPONDER = "dispatcher-skeptic"

REVIEW_PROMPT = """You are the second-lane reviewer on a personal ops board.
An executor climbed as far as it could and hit the round cap. Your job is to
look at what the rounds produced and return ONE verdict. Work ONLY inside
the current directory. You have NO network access.

{switches}

THREAD: {title}
THE DECISION THAT WAS MADE (what the rounds were carrying out):
  choice: {reply_value}
  note: {reply_note}

ROUNDS SO FAR: {rounds} (the cap; no further regular round will be spawned)
{history_block}
VERDICT RULES — pick exactly one:
  ready       the artifact plainly satisfies the decision above: the work it
              describes is done and verifiable from what is on this thread
  needs-work  one more bounded round could finish it, and you can say what
              that round should do
  needs-joey  the next step is genuinely his: his decision, his money, his
              account or credential, his physical presence, or a person only
              he can contact. Anything an agent could do itself is NOT this.

Write a file named {result_file} in the current directory with EXACTLY:
  {{"verdict": "ready|needs-work|needs-joey",
    "note": "what you checked, what you found, and (needs-work) what the one
             more round should do — MINIMUM {min_note} chars, this is the
             only thing anyone will read of your review"}}
Write it as your LAST action. Do not write anywhere else."""


def render_review_prompt(item: dict, rounds: int, switches: str,
                         result_file: str = REVIEW_RESULT) -> str:
    hist_lines = []
    for e in (item.get("history") or [])[-10:]:
        body = clip((e or {}).get("body"), 200).replace("\n", " ")
        hist_lines.append("- [%s] %s %s" % ((e or {}).get("kind"),
                                            (e or {}).get("source") or "", body))
    history_block = ("RECENT THREAD HISTORY (newest last):\n" + "\n".join(hist_lines)
                     + "\n" if hist_lines else "")
    prompt = REVIEW_PROMPT.format(
        switches=switches,
        title=clip(item.get("title"), 200),
        reply_value=clip(item.get("reply_value"), 200),
        reply_note=clip(item.get("reply_note") or "(none)", 400),
        rounds=rounds,
        history_block=history_block,
        result_file=result_file,
        min_note=MIN_REVIEW_NOTE,
    )
    return prompt[:8000]


def parse_verdict(run_dir: str):
    """Read the reviewer's artifact. Returns (verdict|None, note, raw).

    verdict is None when the review cannot be trusted: no artifact, bad JSON,
    unknown verdict, or a note under the floor. A broken reviewer must not
    buy silence OR a page (contract: P3b §3.3) — the caller falls back to the
    honest ack summary.
    """
    raw = read_text(os.path.join(run_dir, REVIEW_RESULT), 4000)
    if not raw.strip():
        return None, "", raw
    try:
        d = json.loads(raw)
    except ValueError:
        return None, "", raw
    if not isinstance(d, dict):
        return None, "", raw
    verdict = str(d.get("verdict") or "").strip().lower()
    note = str(d.get("note") or "").strip()
    if verdict not in VERDICTS:
        return None, note, raw
    if len(note) < MIN_REVIEW_NOTE:
        return None, note, raw
    return verdict, note, raw


# ------------------------------------------------------------- the outcomes

def ready_result(review_note: str) -> dict:
    """Verdict ready: post the reviewed result. His tap settles it (LESSON 18:
    independent verification never closes on its own)."""
    return {
        "outcome": "skeptic-ready",
        "state": "needs-you",
        "note": ("Second-lane review (skeptic-lite) read every round this thread "
                 "produced and verified the artifact satisfies the decision. "
                 + clip(review_note, 3000)),
        "responder": REVIEW_RESPONDER,
        "ask": {"kind": "ack",
                "prompt": ("The second-lane review verified this work is done as "
                           "decided. Acknowledging settles the thread.")},
    }


def escalation_ask(title: str) -> dict:
    """Verdict needs-joey: the ask lights WITH options, derived here
    (LESSON 15), each self-contained (P1 §3), and carries the T1 marker the
    four-test demands (P3b §1.3)."""
    return {
        "kind": "choice",
        "requires": ["decision"],
        "prompt": ("The executor rounds hit the round cap on %s and the "
                   "second-lane review says the next step needs you. Which way "
                   "should this go?" % clip(title, 160)),
        "options": [
            {"label": "Run one more executor round on it",
             "value": "run-one-more",
             "effect": "Spawns one more executor round past the cap, carrying "
                       "the reviewer's findings; nothing else changes.",
             "terminal": False, "resolved": False},
            {"label": "Keep the work exactly as it stands",
             "value": "keep-as-stands",
             "effect": "Settles this thread here; the artifact stays as the "
                       "rounds produced it and nothing further runs.",
             "terminal": True, "resolved": False},
            {"label": "It is finished as it stands",
             "value": "finished-as-stands",
             "effect": "Settles the thread and records the underlying work as "
                       "finished.",
             "terminal": True, "resolved": True},
        ],
    }


def escalation_result(item: dict, review_note: str) -> dict:
    return {
        "outcome": "needs-joey",
        "state": "needs-you",
        "note": ("The executor rounds hit the round cap and the second-lane "
                 "review could not finish this without you. Review findings: "
                 + clip(review_note, 3000)),
        "responder": REVIEW_RESPONDER,
        "ask": escalation_ask(item.get("title") or ""),
    }


def fallback_result(why: str) -> dict:
    """A review that cannot produce a valid verdict never produces a bare
    card and never pages: the honest ack summary (contract: P3b §3.3/§4).
    Deliberately contains no '?' and none of the banned phrase."""
    return {
        "outcome": "round-cap-review-unavailable",
        "state": "needs-you",
        "note": ("This thread hit the executor round cap and the second-lane "
                 "review could not run or produced no usable verdict (%s). "
                 "Everything the rounds produced is preserved in the history "
                 "above; acknowledging settles the thread, and any reply "
                 "reopens it." % clip(why, 200)),
        "responder": REVIEW_RESPONDER,
        "ask": {"kind": "ack",
                "prompt": ("Work stopped at the round cap — the second-lane "
                           "review could not run. Acknowledging settles this "
                           "thread; reply anything to reopen it.")},
    }


def dupe_ack_result(other_tid: str) -> dict:
    """The escalation's four-test said T3: an ask covering this same concern
    is already open elsewhere, so this thread does not page too (contract:
    P3b §3.2). Settle toward his tap with the pointer, not a second card."""
    return {
        "outcome": "round-cap-duplicate",
        "state": "needs-you",
        "note": ("This thread hit the round cap, and the second-lane review "
                 "wanted to escalate — but an ask covering this same concern "
                 "is already open on thread %s, so this one does not add a "
                 "second card. Everything the rounds produced is in the "
                 "history above; acknowledging settles this thread."
                 % clip(other_tid, 80)),
        "responder": REVIEW_RESPONDER,
        "ask": {"kind": "ack",
                "prompt": ("Round cap reached; the same concern is already "
                           "being asked about on another thread. Acknowledging "
                           "settles this one.")},
    }
