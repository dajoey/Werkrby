"""learning/classify.py — the deterministic correction classifier (contract: P3c §1).

Classify an incoming event body (his reply/note) as correction | answer | ack |
noise using lexical signals ONLY. No model, no embeddings, no network — this is
FACT work about the text (LESSON 33), and the classifier NEVER adopts anything:
its output is a proposal a human taps on (work order item 1; P3c §2's state
gate).

Fail direction is OVER-fire (contract §1): a false correction costs one thread
he taps 'one-off' on; a missed correction is lost forever. The four signals are
deliberately loose; his tap is the filter, which is the whole design.
"""
from __future__ import annotations

import re

# ------------------------------------------------------------- the signals

# He said it outright: "correction:" anywhere at the start of a line, forgiving
# of markdown quoting/emphasis in front of it.
_MARKER = re.compile(r"^\s*[>*_~\-]*\s*correction\s*:", re.I | re.M)

# He is quoting what the agent said: a markdown "> quoted" line, or the
# "On ... wrote:" header a mail client leaves in. Corrections quote the thing
# they are correcting; answers and acks do not.
_QUOTED_LINE = re.compile(r"^\s*>", re.M)
_ON_WROTE = re.compile(r"^on\s+.*\bwrote\s*:", re.I | re.M)

# The shape of a correction: negator + action verb in ONE sentence.
_NEGATORS = (r"don'?t", r"do\s+not", r"never", r"stop", r"wrong", r"\bno\b",
             r"\bnot\b")
_ACTION_VERBS = (r"do(?:ing)?", r"us(?:e|ing)", r"put", r"send", r"write",
                 r"run", r"post", r"file", r"clos(?:e|ing)", r"open",
                 r"delet(?:e|ing)", r"mov(?:e|ing)", r"set", r"add",
                 r"check", r"ask", r"tell", r"call", r"mak(?:e|ing)",
                 r"tak(?:e|ing)", r"keep", r"drop", r"block", r"refus(?:e|ing)",
                 r"requir(?:e|ing)", r"reboot", r"restart", r"kill",
                 r"schedule", r"patch", r"fix", r"ship", r"deploy", r"merge",
                 r"commit")
_SENTENCE = re.compile(r"[^.!?\n]+[.!?]?", re.S)
_NEG = re.compile(r"\b(?:" + "|".join(_NEGATORS) + r")\b", re.I)
_VERB = re.compile(r"\b(?:" + "|".join(_ACTION_VERBS) + r")\b", re.I)

# Emphasis and urgency he typed himself. A bare '?' or '!' is just punctuation;
# only doubled/mixed runs ( !! , ?? , ?! , !? ) are urgency.
_ALL_CAPS = re.compile(r"\b[A-Z]{3,}\b")
_URGENT = re.compile(r"[!?]{2,}")

# Whole-body acknowledgements: reading them was the whole job (P1 §4's own
# reading of the ack kind); nothing to correct and nothing to answer.
_ACKS = {"ack", "ok", "okay", "k", "done", "got it", "got it, thanks",
         "thanks", "lgtm", "read"}

# The bodies that are not even that.
_TAP_VALUES = {"", "ack", "close", "note-only", "__redirect__"}

KINDS = ("correction", "answer", "ack", "noise")

# Canonical signal order, so the tuple is comparable across runs.
SIGNAL_ORDER = ("marker", "quoted-reply", "negation-imperative", "all-caps",
                "urgent-punctuation")

MIN_RULE_CANDIDATE = 12      # contract §open-questions 7: the MIN_ANSWER floor
MAX_RULE_CANDIDATE = 240


def _sentences(body: str):
    return [s.strip() for s in _SENTENCE.findall(body or "") if s.strip()]


def _is_quoted_line(line: str) -> bool:
    return bool(_QUOTED_LINE.match(line) or _ON_WROTE.match(line))


def rule_candidate_of(body: str, signals) -> str | None:
    """His words reshaped into a rule proposal, deterministically (contract §1).

    Never grammar-rewritten: the candidate is a PROPOSAL rendered verbatim for
    him to adopt or edit (LESSON 15 — his words win; adopt-with-edit exists for
    the wording). Returns None when nothing over the 12-char floor survives.
    """
    body = str(body or "").strip()
    if not body:
        return None
    lines = [ln for ln in body.splitlines()]
    # Drop the quoted agent text: the candidate is HIS words, not the quote.
    kept = [ln for ln in lines if not _is_quoted_line(ln)]
    text = " ".join(" ".join(kept).split())
    if "marker" in signals:
        m = _MARKER.search(text)
        if m:
            after = text[m.end():].strip()
            if len(after) >= MIN_RULE_CANDIDATE:
                return after[:MAX_RULE_CANDIDATE]
    sents = _sentences(text)
    if "negation-imperative" in signals:
        for s in sents:
            if _NEG.search(s) and _VERB.search(s):
                if len(s) >= MIN_RULE_CANDIDATE:
                    return s[:MAX_RULE_CANDIDATE]
    for s in sents:
        if len(s) >= MIN_RULE_CANDIDATE:
            return s[:MAX_RULE_CANDIDATE]
    return None


def classify(body, value=None):
    """Classify one event body. Returns (kind, signals, rule_candidate).

    kind    'correction' | 'answer' | 'ack' | 'noise'
    signals tuple of matched signal names in canonical order; () when none
    rule_candidate  str or None (contract §1 extraction rules)
    """
    body = str(body or "")
    stripped = body.strip()

    signals = []
    if _MARKER.search(stripped):
        signals.append("marker")
    if _QUOTED_LINE.search(stripped) or _ON_WROTE.search(stripped):
        signals.append("quoted-reply")
    for s in _sentences(stripped):
        if _NEG.search(s) and _VERB.search(s):
            signals.append("negation-imperative")
            break
    if _ALL_CAPS.search(stripped):
        signals.append("all-caps")
    if _URGENT.search(stripped):
        signals.append("urgent-punctuation")
    signals = tuple(s for s in SIGNAL_ORDER if s in signals)

    # Kind, in the contract's order.
    if len(re.sub(r"[^0-9a-z]", "", stripped, flags=re.I)) < 3:
        # A decision tap with no body is still his answer (step 5); a body of
        # pure punctuation is noise.
        if value is not None and str(value).strip().lower() not in _TAP_VALUES:
            return "answer", signals, None
        return "noise", signals, None
    if stripped.lower() in _ACKS:
        return "ack", signals, None
    if signals:
        return "correction", signals, rule_candidate_of(stripped, signals)
    return "answer", signals, None
