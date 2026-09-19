# Contract P3b — The self-resolution ladder

Owner: P3b worker. Issue: helm#5. Depends: P1 store, P2 API, P3 dispatcher (all
merged at dc3ede7, 608 checks green). Status: spec frozen before any ladder code was
written (work order step 0). Every rule cites the LESSONS.md number it implements; a
rule with no lesson number does not belong in this file.

The one-sentence spec (PLAN, P3b row): **work climbs as far as it can on its own and
only becomes a needs-you ask when it genuinely needs Joey.** The ladder, bottom to top:

```
spawn (attempt 1..5, backoff)                      LESSONS 9, 26
  └─ last attempt fails -> ONE more round on the OTHER executor   §2
       └─ still fails -> gave_up (visible) + restore-to-ack       P1/P3, unchanged
round cap (4 run-produced rounds)                                  LESSON 35
  └─ second-lane skeptic-lite review of the artifact               §3  LESSONS 28, 32
       ├─ ready        -> result posted, his tap settles           LESSON 18
       ├─ needs-work   -> ONE bonus round (cap 4->5, once ever)    §3.3
       └─ needs-joey   -> escalate: choice ask WITH options        LESSON 15
any escalation                                        §1  LESSON 3
  └─ the four-test, as deterministic code, must PASS first
anything that still has no options -> ack-style summary, never "what do you want"  §4
```

## §0 Scope and port rules (LESSONS 39, 41)

Scope: `helm/store.py` (four-test + bare-ask gates; store edits cite this contract in
their commit messages per PLAN rule 9), `app/service.py` (one read-only self-check
route), `dispatcher/tick.py` + new `dispatcher/skeptic.py` (extend, never fork the
pump), `dispatcher/watchdog.py` (one line: markers), `tests/stub_executor.py` (review
modes), `tests/test_ladder.py` (new suite), `tests/probe_400s.py` (extended), and the
mechanical fixture edits listed in §1.5.

No pip, no push, no live systems, no secrets, no real executor invocations in tests.
Everything offline-deterministic; the only model calls are executor spawns at RUNTIME
by the dispatcher (never in tests, where the stub plays every role).

## §1 The needs-you four-test as code (LESSONS 3, 14, 15, 33; the fleet filing rule)

> An ask pages Joey ONLY if ALL FOUR hold. Any test failing = NOT needs-you; the
> ladder continues or the thread settles elsewhere.

### §1.1 The function

`needs_joey(thread, context) -> (needs_you: bool, failed_test: str)` in
`helm/store.py`. Pure decision over supplied data — no model, no embeddings, no
network (LESSON 33: FACT -> code). `thread` is `{thread_id, title, ask{kind, prompt,
options, requires}}`; `context` carries everything the tests need to see:
`existing` (row or None), `owner_event_ts` (newest joey-reply/joey-note/settle event
on the thread), `ask_ts` (the generation the ask belongs to), `duplicates` (rows the
caller already deduped, or `con` to query), `ladder` (`{retry_budget_left,
self_fix_paths}` or None), `ops_log_hits` (list, default empty), `licensed` (bool).

| test | passes when | fails with |
|------|-------------|------------|
| T1 only-he-can | `ask.requires` names >=1 marker from `{decision, money, credential, presence, contact}` | `T1-only-he-can` |
| T2 still-true-now | no owner/settle event newer than `ask_ts` | `T2-still-true-now` |
| T3 he-doesnt-know | no duplicate ask, no answered-decision covering it, no ops-log hit | `T3-he-doesnt-know` |
| T4 agent-cannot-fix | `ladder` is None (caller is not the ladder), OR budget exhausted AND no self-fix path | `T4-agent-cannot-fix` |

Tests run in order T1..T4; the first failure is returned. Unknown marker values are a
400 at validation time (§1.4), never a silent T1 failure. T3 dedupe is deterministic
tokenization only: same stable `thread_id`, or `fingerprint(title)` equality — the
store's existing number/unit/stopword normalizer (proven on the mute surface; strips
"11%" vs "7%" noise so re-derived concerns match). NO embeddings, NO LLM.

`assert_needs_joey(thread, context)` raises `StoreError(400, "needs-you refused:
<Tn-name>: <what to send instead>")` with `extra={"failed_test": ...}` — the same
rejection shape every other gate uses, readable by the producer that sent the payload
(P1 §6). `put_threads` calls it; the dispatcher's escalation calls it via the §1.4
route; probes call it directly.

### §1.2 Where the four-test is wired (the doors)

- **The producer's door (`put_threads`)** — the pitfall-report fix as code: 71.5% of
  the retired board's cards were system-generated (LESSONS top-of-file; A 3.1). A PUT
  that would LIGHT a needs-you ask (`state == "needs-you"`, `ask.kind != "none"`) is
  gated unless **licensed**: a thread whose stable id already carries an ask on the
  board is a resync of the same concern, not a new card — the ask may refresh (P1
  invariant 3 still protects in-flight answers) but is not re-interrogated. So the
  gate fires exactly when a NEW ask reaches the board: fresh thread, or re-lighting a
  thread whose stored ask is `none`. Threads preserved by invariants 2/3 (done,
  answered, snoozed) never reach the gate — preservation IS their "not needs-you".
  Rejection is per-thread in `rejected[]` with the rest of the payload landing (P1's
  partial-success property, kept).
- **The bot's door (`post_result`)** is NOT gated by the four-test: a run reporting
  back on work he already dispatched is not filing a new card, and P1 §4's two
  mechanical close exceptions already police it. The reviewer's needs-joey verdict
  and the gave-up restore are the ladder's own voice and self-certify through §1.4.
- **Hand doors** (`create_thread`, `reopen_thread`, snooze/wake, park expiry) are
  exempt: they are his own acts or the store's own sweeper, where T1 is trivially
  true and T3 is the same object by definition (LESSON 1: one object model).
- **The ladder (`dispatcher/tick.py`)** runs the four-test through the §1.4 route
  BEFORE posting any needs-joey escalation (skeptic verdict, gave-up restore), with
  `ask_ts` = the ask's generation and a real `ladder` context. T2/T4 are live here
  and in `needs_joey()` itself; at the producer's door they are embodied by the
  ported invariants (§1.2 bullet 1) — a resync that arrives superseded is preserved,
  not rejected, because a resync is not an ask.

### §1.3 T1 evidence: the `requires` markers (LESSONS 15 — and its source, A 3.7)

Producers must state which only-he-can capability the next step needs, as data:
`ask.requires = "decision"` or `["decision", "money"]`. Valid markers:
`decision | money | credential | presence | contact`. Absent markers = T1 fails = the
ask is refused with the fix named. An unknown marker is a 400 naming the valid set
(loud at validation, P1 §6; never coerced).

Decided point — **ack asks at the producer's door are exempt from T1** (probed as a
positive control): an ack claims nothing that needs T1 evidence — reading it is the
whole job (P1 §4's own words) and a tap settles it, so there is no decision to
offload. choice and text asks carry markers or are refused. The fleet four-test
philosophy ("FYI pages nobody") is served by producers simply not filing acks for
information he need not see; when they must (watchdog pages), they file choice asks
with markers.

`requires` is door evidence, not row state: it is checked and dropped. A licensed
resync therefore never re-proves T1 — the stable id already carries the ask.

### §1.4 The self-check route (the fix as code, for every producer)

`POST /api/triage/needs-joey-check` with `{thread: {...}, context?: {ladder,
ask_ts}}` returns `{needs_you, failed_test, detail}` (200 — it is a read). The
service builds the store-side context (existing row, dedupe scan, newest owner
event). This is the route `spark.file_issue` and every P4 producer calls BEFORE
filing; it is how the pitfall report's rule becomes enforceable by the system rather
than remembered by prompts. The dispatcher's escalation path calls it and, on a T2/T3
failure, does not page (T2: his answer already re-queued the work; T3: the existing
ask stands — both log loudly and release).

### §1.5 Ported-fixture edits this gate legitimately requires (P1 §0: named here)

The gate applies to newly-lit choice/text asks, so fixtures that CREATE lit choice
threads through `put_threads` must now state markers. Mechanical, one helper each;
no ported assertion is weakened:

- `tests/test_store.py` `thread()` — setdefault `requires` on choice/text asks.
- `tests/test_park.py` `armed()` — same.
- `tests/test_self_contained.py` `put()` — same (rejections under test fire in
  `validate_thread` BEFORE the four-test, so their reasons are unchanged).
- `tests/test_bot_close.py` `CHOICE` — markers on the const.
- `tests/test_attachments.py` `CHOICE` — same.
- `tests/test_api.py` `put_thread()` — same.
- `tests/test_dispatcher.py` `put_thread()` — same, plus `HELM_HERMES_BIN` pointed
  at the stub (the switched runtime and the reviewer would otherwise spawn the real
  `hermes` binary from the suite — measured risk, not hypothetical).
- `tests/probe_400s.py` `CHOICE` — same.
- `dispatcher/watchdog.py` `page()` — `requires: ["decision"]` on its ask.

## §2 Retries, backoff, one-more-round (LESSONS 9, 26, 35)

The P3 pump stays: claim-before-spawn, 5 attempts, exponential backoff, auth failure
reported once and never retried, gave_up visible with a restore path. `tick.py` is
extended in place, never forked. NEW:

1. **One-more-round on the last attempt.** When an attempt fails AND it was the last
   regular attempt (`attempts == MAX_DISPATCH_ATTEMPTS` after `mode=start`), the
   tick immediately runs ONE more pass with the OTHER executor — `claude <-> hermes`,
   via the adapter registry (`adapters.READERS`, `spec_claude`/`spec_hermes`); the
   registry knows both, the pump stays executor-agnostic. The switched attempt claims
   anew (LESSON 26), runs under the same jail with the OTHER runtime's default caps,
   and posts through its own claim. If it succeeds, the result lands and the budget
   resets (`post_result` does that); if it fails, `attempts` is past the cap and the
   thread is gave_up on the next tick — the loud give-up LESSON 9 demands, now
   reached only after both executors had the work.
2. **Exceptions.** No switch on jail refusal (the jail is per-host, not per-runtime —
   a degraded jail blocks both; failing closed once is enough). Auth failure DOES
   switch: a dead sign-in on one runtime is exactly what the other is for, and the
   switch is one bounded spawn, not a retry loop. The switched spawn performs no
   separate auth precheck (its own auth markers are detected on its result; LESSON 9
   still stops same-runtime retry).
3. **The manifest.** Every run dir gets `manifest.json`: one entry per attempt —
   `{n, runtime, executor_id, claim_id, rc, posted, duration_s, switched}` — plus the
   review record when §3 fires. Attempts and executor switches are visible on disk
   (LESSON 11's spirit: state you can read, not a status word). The switched attempt
   works in `<run_dir>/switch/` under the same retention sweep.

## §3 Round cap -> skeptic-lite second-lane review (LESSONS 28, 32, 18, 15, 35)

When the round cap fires (4 run-produced rounds, snoozes/store events excluded — P3),
the thread is no longer handed back bare. The artifact goes to a **second-lane
review**:

1. **The reviewer is a real executor spawn at RUNTIME** (the model call the contract
   allows; in tests the stub plays it) on the OTHER runtime from `RUNTIME`, under the
   same jail and default caps, claiming the thread first (`executor_id
   helm-skeptic-lite`, LESSON 26). The review prompt is rendered from a deterministic
   template (`dispatcher/skeptic.py`): thread, the decision, clipped round history,
   the last artifact, and the verdict rules — `ready` only if the artifact plainly
   satisfies the decision; `needs-work` if one more bounded round could finish;
   `needs-joey` only if the next step is his decision/money/credential/presence/
   contact. The reviewer writes `result.json {"verdict", "note"}`; note >= 80 chars
   (LESSON 11 — a verdict word alone says nothing).
2. **Verdicts.**
   - `ready` -> the tick posts the reviewed result (outcome `skeptic-ready`, the
     reviewer's note, an ack ask that says what the review verified). His tap
     settles it (LESSON 18: only Joey closes; independent verification NEVER closes
     on its own — LESSON 28's "never blocks completion on its own outage" read the
     same way).
   - `needs-joey` -> escalate: needs-you with a CHOICE ask, options derived from the
     deterministic template (run-one-more / keep-as-stands / finished-as-stands —
     labels say what picking them DOES, self-contained per P1 §3) and
     `requires: ["decision"]`. The escalation is four-test checked through §1.4
     first (T2/T3 can still stop the page).
   - `needs-work` -> ONE bonus round: the cap becomes 5 total for this thread,
     **once, ever**. The bonus round is spawned immediately in the same tick
     (internal override, not the reply-note phrase) with the reviewer's note
     prefixed to its result — `"round-cap bonus (once per thread): ..."` — and that
     prefix in the event log IS the once-ever marker (the event log is the truth;
     no new column, no dispatcher-local state). A later `needs-work` verdict with
     the marker present escalates as needs-joey: the ladder is exhausted (T4 holds).
3. **Fallback, fail-closed toward honesty.** A review that cannot produce a valid
   verdict (spawn failed, unparseable result, invalid note) never produces a bare
   card: the tick posts the §4 ack-style summary ("work stopped at the round cap —
   the second-lane review could not run; reply anything to reopen"). A broken
   reviewer must not buy silence (LESSON 24) and must not page either.
4. Review events carry responder `dispatcher-skeptic` and are excluded from
   `bot_rounds()` (same class as P3 open-question 7: a hand-back that counted as a
   round would strand the thread at the cap forever). Bonus rounds ARE run-produced
   and DO count as round 5.

Open (conservative, flagged): a crash between a `needs-work` verdict and the bonus
round's result leaves no marker, so the next tick re-reviews and may re-grant —
bounded at one round per crash by the crash itself, and every re-grant is a normal
capped, claimed, jailed attempt.

## §4 Bare cards never light "what do you want" (LESSONS 15, 11, 13, 3)

The archetype of the retired board's worst card: lit, optionless, and asking nothing
— "what do you want". Three deterministic rules, at the doors where bots author asks
(`post_result`), with R1 mirrored at the producer's door:

- **R1 — the literal ban.** The phrase `what do you want` (case-insensitive,
  substring, covers "what do you want to do about this") may appear in NO stored ask
  — prompt, option label/effect, or context — at either door. 400, always. Probed
  per-door AND by a final sweep over every stored ask in the probe DB.
- **R2 — no options AND no question is a statement.** At the bot's door, a
  needs-you `text` ask with no options and no `?` in the prompt is a 400: a
  statement is not an ask, and the ack kind exists precisely for "reading it is the
  whole job". (Choice with no options is already a 400 from P1; ack prompts are
  statements by design and exempt.)
- **R3 — the pure direction-demand is rewritten.** A needs-you `text` ask whose
  whole prompt is a demand for direction with no options — `how should we proceed`,
  `what now`, `your call`, `tell me what to do` (deterministic pattern list) —
  pretends to be a question but offloads the thinking. The store REWRITES it
  deterministically to the ack-style summary: kind `ack`, prompt
  `"Work stopped at <outcome>: <note clip> — next options unknown. Reply anything to
  reopen."` The return carries `rewritten_ask: true`; history shows the rewritten
  ask (never the demand). A text ask that asks a REAL question (`Which certificate
  did you mean, the relay or the site?`) stands as text. The producer's door refuses
  the same shape with the fix named (a producer can correct its payload; a run at
  the end of its rope cannot).

Reading of the work order's "unless ... the ladder could derive options": a
?-carrying text ask that is a real question is allowed (options are not the only
legible form); option derivation is the ladder's job (§3.2 needs-joey, the §1.4
route); when neither the ask nor the ladder produces options, the ack-style summary
is the honest card — flagged in §open-questions.

## §5 Evidence (what "done" means here)

- `python3 tests/run_all.py` green, including the new `tests/test_ladder.py` and the
  extended `tests/probe_400s.py` (PLAN standing rule 2: the suite is the only
  evidence).
- Probes that must 400, one per failing test: T1 (markerless choice at the producer
  door), T3 (duplicate title at the producer door, naming the duplicate), T1-unknown
  marker, R1 at both doors, R2 at the bot's door — plus positive controls (licensed
  resync lands; ack at the producer's door lands) so the gate is proven not to
  over-fire. T2/T4 are fired through `needs_joey()`/the §1.4 route with constructed
  contexts (their producer-door embodiments are the ported invariants, §1.2).
- Ladder behavior (stub executors, no model): the one-more-round switch (last
  attempt ran the other executor; success lands; both-fail reaches gave_up +
  restore), the round-cap review (ready posts and settles on his tap; needs-joey
  lights a choice WITH options; needs-work grants the once-ever bonus; invalid
  review falls back to the ack summary), the R3 rewrite, and the banned-phrase
  sweep.
- **Live gates NOT covered offline** (for the orchestrator's E2E pass, like P3's
  pending list): a real round-cap review with a real second-lane executor (the
  stub plays the reviewer here), a real one-more-round switch (a real `hermes -z`
  picking up after a real claude failure), and a producer integrating the §1.4
  self-check (P4's repoint).

## §open-questions (each answered conservatively, flagged for the orchestrator)

1. **Ack asks exempt from T1 at the producer's door** (§1.3). The alternative —
   refusing producer acks entirely — breaks P1 §4's ever-parked close path and every
   ported ack fixture, to stop a spam vector that choice-marker gating already
   covers. Chosen: exempt; producers that only owe information file nothing.
2. **T2/T4 at the producer's door are the ported invariants, not new rejections**
   (§1.2). A superseded resync is preserved (invariants 2/3), because a resync is
   not an ask. The tests themselves fire in `needs_joey()` and on the dispatcher's
   escalation path, where the ask is real.
3. **T3's ops-log input is deferred.** The store cannot read the wiki; `ops_log_hits`
   is a context slot (default empty) that P4/P5 producers fill. The dedupe and
   answered-decision checks are live now.
4. **Dedupe window: recently-closed = closed within 30d** (`MAX_RECHECK_S`), matching
   the park-expiry horizon. Beyond that a re-derived concern is a new ask.
5. **`requires` markers are not persisted** (§1.3). The license is the stable id; a
   resync refreshes wording without re-proving T1. If a producer's concern CHANGES
   material, that is a new thread id (and the four-test applies).
6. **Second `needs-work` escalates** rather than looping (§3.2): "once, ever" is the
   whole point; a reviewer that twice says one-more-round has told us the work is
   not converging.
7. **The crash window on the bonus marker** (§3 open note): one bounded re-grant per
   crash; every re-grant is a normal capped attempt.
8. **R3's pattern list is deliberately small** (§4): `what/how + do/should/would +
   you/we/I + want/do/proceed/handle`, `what now/next`, `your call`, `awaiting
   decision/instructions`, `tell me what to do`, `how should/do we/I proceed`,
   matched against the WHOLE prompt. Under-matching leaves a weird text ask
   standing (cost: one odd card); over-matching would eat real questions (cost: a
   lost question) — the list errs under, same direction as P1 §2's `?` detector.
