# Helm Learning DOX

## Purpose

- Own the deterministic correction classifier: `classify.py` reads one event body — the owner's reply or note — and returns `(kind, signals, rule_candidate)`.
- Own only that. The lesson ledger and its adoption gate live in the store, the lesson door is an HTTP route, and applying an adoption tap belongs to the dispatcher tick.
- Produce proposals, never decisions: what this package emits is something a human taps on.

## Local Contracts

- The package is pure and stdlib-only (`re`). It must not import the store, the app or the dispatcher, must make no model, embedding, network or subprocess call, and `__init__.py` stays docstring-only so `from learning import classify` pulls in nothing else. Classification is fact work about text; judgement happens elsewhere.
- `classify(body, value=None) -> (kind, signals, rule_candidate)`. `kind` is one of `KINDS` — `correction`, `answer`, `ack`, `noise`. `signals` is a tuple filtered through `SIGNAL_ORDER` (`marker`, `quoted-reply`, `negation-imperative`, `all-caps`, `urgent-punctuation`) so the tuple is comparable across runs regardless of match order. `rule_candidate` is non-`None` only for a `correction`.
- The kind decision order is binding: fewer than 3 alphanumeric characters is `noise`, except that a decision tap whose `value` falls outside `_TAP_VALUES` (`""`, `ack`, `close`, `note-only`, `__redirect__`) rescues it to `answer` — the tap itself is the answer; then a whole body in `_ACKS` is `ack`; then any matched signal is `correction`; otherwise `answer`, because a real reply is presumptively an answer. Reordering these changes what gets triaged.
- The classifier never adopts. Adoption is the owner's tap and the store refuses to create a lesson without one; nothing here may be wired to write the ledger.
- Fail direction is deliberately **over-fire**: the four signals are loose on purpose. A false correction costs one thread the owner taps away as a one-off; a missed correction is lost forever. Do not tighten a signal to reduce noise before the contract changes.
- `rule_candidate_of()` renders the owner's own words, never a rewrite: it drops quoted agent lines first, then prefers the text after a `correction:` marker, then the first sentence carrying both a negator and an action verb, then the first sentence long enough. Grammar-fixing the proposal is out of scope — edited wording arrives as a separate adopt-with-edit tap.
- The candidate bounds are `MIN_RULE_CANDIDATE = 12` and `MAX_RULE_CANDIDATE = 240` characters: nothing shorter is proposed (return `None`), and longer text is truncated, never summarised.
- The signal regexes carry meaning that must survive edits — `_MARKER` is line-anchored and forgives leading markdown quoting or emphasis; `_QUOTED_LINE` and `_ON_WROTE` detect a quoted reply because corrections quote what they correct and answers do not; `_NEG` and `_VERB` must both match inside **one** sentence, not across the body; and a single `!` or `?` is punctuation, only a run of two or more is urgency.
- The downstream tag must stay in step: lesson threads are marked `source: "learning"` in the store and the dispatcher recognises a lesson item by exactly that string. Renaming it on one side detaches the loop silently.
- Docstrings here cite sections of `docs/contracts/P3c.md`. That contract is the historical spec, frozen before the code was written; where it and the code disagree, the code is what ships.

## Child DOX Index

No child DOX files.
