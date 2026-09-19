# Contract P3c — The learning loop

Owner: P3c worker. Issue: helm#6. Depends: P1 store, P2 API, P3 dispatcher, P3b
ladder (all merged at 14cfc49, 685 checks green). Status: spec frozen before any
learning code was written (work order step 0). Every rule cites the LESSONS.md number
or PLAN.md rule it implements; a rule with neither does not belong in this file.

The one-sentence spec (PLAN, P3c row): **Joey's corrections stop evaporating.** When he
corrects an agent, the correction becomes a lesson that is (a) triaged once, (b) stored
durably, (c) rendered into future prompts, (d) implementable at gate level with a
changelog. The full loop on a REAL correction is the orchestrator's live E2E gate
(PLAN P3c row); this contract delivers every mechanism offline, stub-driven, no model.

The shape, end to end:

```
his reply/note ──> classify() (deterministic signals; NEVER adopts)      §1  L33
     └─ correction ──> open_lesson_thread() via put_threads               §2  L3,15
            four-test + self-contained + bare-ask gates ALL fire
            ask = adopt / adopt-with-edit / not-the-lesson / one-off
his tap ──> adopt            -> create_lesson_from_thread() (tap-gated)   §3  L18
            adopt-with-edit  -> same, rule text = his note (binding)      §3  L15
            not-the-lesson   -> terminal tap settles, reason on thread    §2  L13
            one-off          -> terminal tap settles, nothing adopted     §2  L13
lesson (active) ──> render_lessons(scope) prepends a numbered block       §4  L2,33
GATE: lesson   ──> lessons_pending_gates() until the code lands           §5  L23,39
```

## §0 Scope and port rules (PLAN rules 1, 2, 9; LESSONS 39, 41)

Scope: `learning/` (new package: `classify.py`, pure), `helm/store.py` (lesson ledger +
lesson-thread door + adoption gate; store edits cite this contract in their commit
messages per PLAN rule 9), `app/service.py` (lesson routes), `dispatcher/tick.py`
(lesson rendering into executor prompts + the deterministic adoption path),
`tests/test_learning.py` (new suite), `tests/probe_400s.py` (extended).

No pip, no push, no live systems, no secrets, no real executor invocations in tests,
no model calls anywhere in this phase — the classifier is deterministic by requirement
(work order item 1) and everything else is store/dispatcher code. The stub executor
plays every spawn. Suites are discovered by `tests/run_all.py`, so the new suite cannot
be forgotten there.

The store stays pure (P1 §6): no HTTP, no model, no subprocess. The classifier lives
OUTSIDE the store (`learning/classify.py`) and imports nothing from it; the store's
lesson door accepts already-classified candidates as data. Classification is FACT work
(L33); adoption is JUDGMENT and is his tap, never code (work order item 1: "the
classifier never auto-adopts").

## §1 The correction classifier — deterministic, no LLM (LESSONS 33, 24; PLAN rule 3)

`learning/classify.py`:

```
classify(body, value=None) -> (kind, signals, rule_candidate)
  kind    in {"correction", "answer", "ack", "noise"}
  signals tuple of matched signal names, canonical order; () when none
  rule_candidate  his words reshaped into a rule proposal, or None
```

Signals, all regex/lexical, in canonical order:

| name | fires on | why |
|---|---|---|
| `marker` | `^\s*[*_>\-]*\s*correction\s*:` (case-insensitive) | he said it outright |
| `quoted-reply` | a line starting `>` or an `On ... wrote:` line | he is quoting what the agent said — corrections quote, answers don't |
| `negation-imperative` | ONE sentence contains both a negator (`don't`, `dont`, `do not`, `never`, `stop`, `wrong`, `no`, `not`) and an action verb (do/use/put/send/write/run/post/file/close/open/delete/move/set/add/check/ask/tell/call/make/take/keep/drop/block/refuse/require/reboot/restart/kill/schedule/patch/fix/ship/deploy/merge/commit) | the shape of a correction: "don't restart services that are in use" |
| `all-caps` | `\b[A-Z]{3,}\b` | emphasis he typed |
| `urgent-punctuation` | `!!`+ / `??`+ / `?!` / `!?` | urgency he typed |

Kind decision, in order:

1. empty / under 3 alphanumeric chars -> `noise` (unless rescued at 5);
2. whole body in `{"ack","ok","okay","k","done","got it"}` -> `ack`;
3. any signal matched -> `correction` (signals returned);
4. otherwise -> `answer` (a real reply from him is presumptively an answer);
5. a decision tap with no body (`value` not in `{"", "ack", "close", "note-only",
   "__redirect__"}` and kind would be `noise`) -> `answer` — the tap is the answer.

Fail direction — OVER-fire (P1 §open-questions 4's direction, and LESSON 24 read for
detection): a false correction costs one thread he taps `one-off` on; a missed
correction is lost forever. `no`/`not` as negators are deliberately loose ("not sure …
should we reboot" fires); the human tap is the filter, which is the whole design.

Rule-candidate extraction (deterministic): drop `>`-quoted and `On … wrote:` lines
(his words, not the quoted agent text); if `marker`, the text after the colon; else the
first sentence that matched `negation-imperative`; else the first remaining sentence.
Whitespace-normalized, clipped to 240 chars. Under 12 chars after that -> `None` (a
correction with no extractable candidate is logged by the glue, not threaded — §2
refuses a candidate under the same floor). The candidate is a PROPOSAL rendered
verbatim for him to adopt or edit; it is never grammar-rewritten (LESSON 15: his words
win; `adopt-with-edit` exists for the wording).

## §2 The lesson thread — opened through the existing gates (LESSONS 3, 15, 17, 13, 18; P3b §1)

`open_lesson_thread(con, source_thread_id, event_ids, quote, rule_candidate,
scope="global", signals=())` in `helm/store.py`:

- Validates inputs: `rule_candidate` >= 12 chars (the `MIN_ANSWER` floor — a one-word
  rule is the status-word failure LESSON 11 exists to stop), `quote` non-empty,
  `event_ids` a non-empty list, `scope` a valid slug or `global` (§3).
- Builds one thread payload and passes it through **`put_threads`** — not a raw INSERT.
  The producer's-door gates therefore all fire on the store's own filing: the P3b
  four-test (T1 passes on `requires: ["decision"]` — only-he-can is his judgment call
  on adopting, exactly the work order's "it will pass"), the P1 §3 self-contained
  gate, the P3b §4 bare-ask rules. A store door that bypassed the gates it enforces on
  everyone else would be the second rule set this repo exists not to have.
- Stable id `lesson-<sha256(fingerprint(candidate))[:12]>`: the same candidate re-opens
  the SAME thread (triaged once — a resync, which P1 invariant 3 protects), and an
  adopted-then-reopened candidate re-uses it too (§3 idempotency).
- The ask is exactly the four options the work order names, each self-contained
  (P1 §3), each saying what picking it DOES:

| label | value | terminal | what picking it does |
|---|---|---|---|
| Adopt it as a standing rule | `adopt` | no | creates an active lesson; renders into executor prompts for its scope |
| Adopt it with my wording edits | `adopt-with-edit` | no | adopts using the exact wording in the note sent with the tap; the note is binding |
| Not the lesson — the real one differs | `not-the-lesson` | yes | closes the candidate, nothing adopted; a note with the real lesson is filed as a new candidate |
| No lesson — that was a one-off | `one-off` | yes | closes the candidate; the correction applied to that thread only |

- The thread body carries the evidence: `ask.context` holds the candidate + his words
  verbatim (clipped to the 2000-char context cap) + the event ids + the matched
  signals, and a `lesson-candidate` event stores the same as JSON (append-only,
  content-addressed — the durable record `create_lesson_from_thread` reads back).
- `not-the-lesson` / `one-off` are TERMINAL options: the tap settles the thread
  (LESSON 13 — the tap is the whole action) with the reason in the settle event and
  his note if any. No lesson is created; the thread's own history is the record of
  why (events are append-only). Nothing here may page him again for the same
  candidate: the stable id plus T3 dedupe close that hole.

State gate (work order item 2), enforced in `create_lesson_from_thread` (§3): adopting
requires his tap; `adopt-with-edit` requires his tap AND his edited text; the
classifier, the glue, and the dispatcher may none of them adopt.

## §3 The lesson ledger (LESSONS 10, 11, 18, 39, 41; PLAN rules 6, 9)

Table `lessons` (in the main DDL — `CREATE TABLE IF NOT EXISTS`, so an existing DB
adopts it on next open without a migration step):

```
id, created_ts, source_thread, rule_text, scope, status, superseded_by,
gate_status ('' | 'pending' | 'implemented'), changelog (JSON list)
```

- `scope` holds `'global'` or a lane/skill key (a slug: `linuxops`, `media`, a skill
  name). `global` renders everywhere; a key renders only for that key (§4). The
  lane/skill DISTINCTION is ledger semantics (who owns re-reviewing it); render treats
  both as exact key match.
- `status` is `active` | `retired`. **A retired lesson NEVER renders into a prompt
  again** — probed, not asserted (LESSON 10: a rendered retired lesson is a card lying
  about its state).
- `changelog` is a report, not a status word (LESSON 11): every mutation appends
  `{ts, event, actor, ...}` — `adopted` (his tap, the signals, the source thread),
  `retired` (reason), `superseded-by` / `supersedes`, `gate-implemented` (the commit).

Store API:

- `open_lesson_thread(...)` — §2.
- `create_lesson_from_thread(con, thread_id, actor="joey")` — the adoption gate.
  Verifies the thread is a lesson thread (its `sources` carry `source: "learning"`)
  AND carries a decision tap (`joey-reply` with value `adopt` / `adopt-with-edit`
  newer than this generation's newest `bot-ask`). Rule text: his note for
  `adopt-with-edit` (required, >= 12 chars — the binding note, LESSON 15; missing note
  is a 400 naming the fix), the stored candidate for `adopt`. Then, in one commit:
  the lesson row is created (or read back — see idempotency) AND the thread is
  settled through `post_result` under the **answered-decision** mechanical close
  (P1 §4): his reply is the decision, `answer_applied: true` (the lesson exists),
  `nothing_owed: true`, responder `dispatcher-learning`. Only Joey closes — this close
  is legal precisely because HE already did the deciding (LESSON 18). A create with no
  tap is a 400; that probe is the "never auto-adopts" rule as code.
- Idempotency: a lesson already exists for this thread → same rule fingerprint returns
  it (`already: true`); a DIFFERENT rule (his re-edit after a reopen) supersedes the
  old row via the supersede path — his tap is the authority, one live lesson per
  thread.
- `lessons_list(con, scope=None, status=None)`, `lesson_get(con, id)` — ordered
  `(created_ts, id)`, deterministic.
- `lessons_for_prompt(con, scope)` — active only, `scope in ('global', key)`, ordered
  `(created_ts, id)`, capped at 20 — the render read.
- `retire_lesson(con, id, reason, actor)` — active -> retired, reason required (>= 12
  chars), changelog entry; retiring a retired lesson is a loud 400, not a no-op.
- `supersede_lesson(con, id, rule_text, scope=None, actor)` — ATOMIC: validates the
  new rule (>= 12 chars) and scope BEFORE touching the old row; then one commit
  creates the new active lesson (`changelog: supersedes <old>`), flips the old to
  `retired` with `superseded_by = <new id>`. A failed supersede leaves the old row
  untouched and nothing new — probed (LESSON 41: the multi-writer-edit lesson; a
  supersede that half-landed would be two active lessons for one concern).

## §4 Active lessons render into executor prompts (LESSONS 2, 33, 34; PLAN P3 row)

- `dispatcher/tick.py` gains `render_lessons(rules)` — a pure formatter: active rules
  as a numbered block, each clipped to 300 chars, deterministically ordered by the
  store's `(created_ts, id)`. `build_prompt(item, run_dir, rounds, lessons=None)`
  prepends the block to the executor prompt template; the 8000-char prompt cap and
  everything else about the template are unchanged. Block cap: 20 lessons (the store
  read is capped at the same number — LESSON 34: the fixed head is the cost, so the
  head stays fixed).
- The tick fetches lessons per dispatch: `GET /api/triage/lessons?render_for=<scope>`
  where scope = `slug(item.category)`. Fetch failure or non-200 renders NOTHING and
  logs loudly (fail open — LESSON 24 tension resolved in §open-questions 3): a dead
  lesson read must not stop the board's work from running; the miss is a logged line,
  never silence.
- Scope isolation is the probe the work order names: a lesson with scope `linuxops`
  renders into a prompt for a `LinuxOps`-category thread and does NOT render into a
  generalist prompt; a `global` lesson renders into both.
- The reviewer prompt (P3b §3) deliberately does NOT get lessons: the reviewer's job
  is a verdict on an artifact, not doing the work the lessons guide (§open-questions
  4). The bonus round prepends to `build_prompt` and therefore inherits the block.
- The dispatcher's lesson-adoption path: a pending item whose `sources` carry
  `source: "learning"` is NEVER spawned to an executor (an executor has no network and
  cannot write the ledger; spawning one would be ceremony plus five wasted attempts).
  The tick applies it deterministically: `POST /api/triage/lessons/from-thread`, then
  logs + manifest entry (`runtime: "none", executor_id: "dispatcher-learning"`). A 400
  from the store gate (e.g. `adopt-with-edit` with no note) is reposted honestly: a
  needs-you result re-presenting the same four options with the refusal detail in the
  note — never a bare card, never a loop.

## §5 Gate-level lessons, honestly pending (LESSONS 23, 39; PLAN rule 6)

- A lesson whose `rule_text` starts with the machine marker `GATE:` (case-insensitive,
  start-of-text only — a `GATE:` mid-sentence is prose about gates) gets
  `gate_status = 'pending'` at adoption. Adopting it is a PROMISE to change code.
- The contract between phases: a `GATE:` lesson is the orchestrator's TODO — it must
  land as an assertion in the test suite; when it lands, `mark_gate_implemented(con,
  id, commit)` records the implementation commit in the ledger changelog
  (`gate_status = 'implemented'`).
- Until then, the gap is VISIBLE, not hidden (LESSON 23: the worse condition must
  never wear the better-looking number): `lessons_pending_gates(con)` lists active
  lessons with `gate_status = 'pending'` — rule text, scope, when adopted, from which
  thread. The offline probe adopts a GATE lesson, leaves it unimplemented, and asserts
  it appears; marks it implemented with a commit ref and asserts it disappears and the
  changelog carries the commit.
- A RETIRED pending gate leaves the listing: a withdrawn lesson is a withdrawn
  promise (§open-questions 6). GATE lessons render into prompts like any active
  lesson until retired or superseded.

## §6 Evidence (what "done" means here) — PLAN standing rule 2

- `python3 tests/run_all.py` green including the new `tests/test_learning.py` and the
  extended `tests/probe_400s.py`.
- Classifier: one probe per signal (each fires on a constructed body; named-signal
  tuple returned), noise/ack/answer controls, candidate extraction (marker text,
  negation sentence, quote lines dropped, floor -> None), and a corpus section of
  correction-shaped bodies classified as corrections.
- Lifecycle: open (gates fire — T1 evidence present, options self-contained, evidence
  on the card), all four taps probed (`adopt` -> lesson + settled thread; `adopt-with-edit`
  with note -> his wording is the rule; without note -> 400 with the fix + the tick's
  honest re-ask; `not-the-lesson` / `one-off` -> terminal settle, no lesson), no-tap
  create refused, same-candidate re-open deduped (T3 / stable id).
- Ledger: retire stops rendering (probe reads the prompt before/after), supersede
  atomicity (bad new rule changes nothing), retired never renders, changelog reports
  every mutation.
- Render: scope isolation both ways, global everywhere, deterministic order, prompt
  block present/absent, and the REAL tick path — a stubbed dispatch whose written
  `prompt.txt` contains the block (the wiring, not just the formatter).
- Gates: pending listing appears/disappears, commit recorded.
- Adversarial (probe_400s.py): open with no/short candidate, adopt with no tap,
  adopt-with-edit with no note, retire with no reason / already retired / unknown id,
  supersede with a short rule (nothing lands), gate-implemented on a non-GATE lesson,
  and the banned-phrase candidate refused at the lesson door (the door's own rules
  bind the store's filing too).
- **Live gates NOT covered offline** (the orchestrator's E2E, like P3's and P3b's
  pending lists): a real correction from his phone classified -> threaded -> tapped ->
  adopted -> rendered into the next real executor run; and the P4 producer glue that
  feeds replies to the classifier (§open-questions 2).

## §open-questions (each answered conservatively, flagged for the orchestrator)

1. **Scope semantics.** The work order's `global|lane|skill` is stored as one column:
   `global` or a slug key. Lane vs skill differ in ownership semantics only; render is
   exact-key + global for both. If P4/P5 want per-skill render keys on threads, that
   is a thread-side field, not a ledger change.
2. **The glue is not wired live.** Who calls `classify` -> `open_lesson_thread` at
   runtime (a P4 producer pass over unconsumed `joey-note`s, or the tick) is a
   producer decision; this phase ships both halves plus the offline loop, and the live
   wiring is the E2E gate above. The stable id + T3 make the glue idempotent.
3. **Render fails open** (§4). The alternative — hold dispatch when the lesson read
   fails — makes the ledger a single point of failure for the whole board. Chosen:
   proceed, log, and let the heartbeat carry the miss. Flagged: if lessons become
   load-bearing GATEs, revisit (a missing GATE lesson is then a silent policy miss).
4. **The reviewer prompt gets no lessons** (§4). The reviewer judges an artifact;
   lessons guide work. Revisit if a review ever needs standing policy.
5. **His verbatim evidence vs the doors.** The quote rides in `ask.context`, which the
   self-contained gate itself polices: if his words contain an external-reference or
   banned pattern, `open_lesson_thread` is REFUSED by the same door producers face.
   Chosen conservative (refuse; the glue clips and retries) rather than exempting the
   store's own filing — an exemption would be a second rule set.
6. **A retired pending GATE leaves `lessons_pending_gates`** (§5). The alternative —
   haunting the listing forever — would make retirement meaningless. If a GATE was
   promised to CODE and the lesson is retired instead, the retirement reason is the
   audit trail.
7. **Candidate floor 12 chars** matches `MIN_ANSWER` (P1 §2) — the same "one word is
   not a real answer" reading. `adopt-with-edit` notes inherit the same floor.
8. **`answer` is the default kind** for real prose (§1 step 4). The classifier's job
   is finding corrections; `answer`/`ack`/`noise` label the rest well enough to route
   (only corrections thread). Precision on non-corrections is not this mechanism's
   promise; over-firing corrections into threads is bounded by his one tap.

As landed: **11 suites, 782 passed, 0 failed** (was 10/685) — store 160, park 59,
answer gate 29, self-contained 66, bot-close 54, attachments 44, api 30, dispatcher 90,
ladder 54, learning 69, probe 127. Commits 0c81576 (spec) + c20c29a (code).
