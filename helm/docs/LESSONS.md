# Lessons that govern this build

Merged from: (A) an internal expansion-pitfall report (the personal kanban+Helm pair,
2,266 cards, retired 2026-09-12) and (B) a porting report from an earlier private
implementation (2026-09-12). Each is a REQUIREMENT. The one-line rule first, source in
parens.

## Topology
1. One object model. An ask raised from blocked work routes back to the SAME object; a
   reply may never spawn a fresh top-level work item. (A 3.1 — the glue loop: one title
   class re-dispatched 66 times, 71.5% of cards were Helm-generated.)
2. Topology changes beat prompt changes. 11 kanban patches in one day didn't hold; the
   structural fixes did. Build structure, not prompts. (A 2, MAST study)
3. Helm is not a mirror of anything. It sits in front and asks only what needs the
   owner. Everything else resolves or logs, never surfaces. (B 1)

## States
4. Every parked state carries a machine-checkable exit: a live needs-you ask, a
   `GATE: <shell cmd>` a cron can run, or a `RECHECK: <ISO date>`; parks expire ≤30d;
   rechecked on the system's clock, not the owner's next app-open. (A 3.2)
5. Unknown assignee is rejected loudly at creation, never silently skipped. (A 3.2)
6. A snooze is waiting-external with a future wake_at; wake is honored by an in-store
   sweeper even if every external reader is gone. (A/B)
7. A late result counts the moment it lands; a slow success is not a stuck answer. (B 7)
8. Snoozes and store-generated events are not rounds. Count only events a run produced.
   (B 7)
9. Retries exhaust loudly (5 attempts, backoff, visible give-up counter, restore-to-ack
   endpoint). Auth failure is reported once and leaves the queue intact; it is not a
   retry. (B 7)

## Cards never lie
10. A card must never lie about its state. Every gate exists because its absence once
    produced a card that looked open when finished, finished when open, or answered when
    nobody acted. (B 1)
11. History must be a report, not a status word: 80-char note floor; "[draft]" with no
    draft = failed run. (A 3.7)
12. Settling a thread whose answer says work is finished must finish the work
    (write-back on every settle path; explicit `resolved` flag; never infer completion
    from prose). (B 7)
13. Ack settles on the tap; once an action is verified done, hand back with NO question.
    Reserve needs-you for real open decisions. (B 7)
14. Do not bundle unrelated items into one thread. Same case / same owner of the next
    move / demonstrated duplicate = the only valid links. (B 7)
15. Multiple choice or nothing. Every choice auto-appends an opt-out; rejected-menu
    answers are design input, never re-asked. A free-text note is binding and wins over
    option text. (A 3.7, B 7)
16. A question in the owner's note requires an `answers` field rendered first. (A 3.7)
17. Options must survive being screenshotted alone (self-contained gate). (A)
18. Only Joey closes; mechanical bot-close exists only for ack-tapped threads and the
    answered-decision case (reply newer than ask, applied, nothing owed). (A 09-12)
19. A file attached to a reply is an instruction, like a binding note (suppresses
    close-tap). (A attachments)
20. Hand-written cards are a different act from a producer PUT: minted id, no mute
    swallow, intent sets state+ask together. (B 7)

## Silent failure
21. Heartbeat file touched every run including no-ops; --selftest before any cron is
    trusted; dry-run writes NOTHING. (A 3.3)
22. A monitor sharing a failure domain with its subject is not a monitor. Watchdogs on
    separate hosts/clocks. (A 3.3)
23. The worse condition must never wear the better-looking number: SyntaxError poller
    looked HEALTHIER (attempts:0) than a failing queue; reply-path zeroing disarmed the
    rescue net; cron failures returned True before incident hooks. (A 3.3)
24. Alert filters fail open. A broken classifier must never buy silence. (A 3.3)
25. "Empty board" is answered by pipeline-motion checks, not counts. The health metric
    is needs-you threads Joey can act on. (A 3.3)
26. Claim BEFORE spawn; crash re-offers the answer (at-least-once). (B 7)
27. Fetch → shape → digest → decide → apply, each its own step; the reasoning step never
    runs with tools while reading untrusted text. (B 7)

## Verification
28. Independent verification = a different agent/instance reading the artifact, never
    the worker's prose; it never blocks completion on its own outage. (A 3.4)
29. A user-installed binary is verified by running it and capturing the primary flow. A
    bench measuring a proxy certifies a broken fix. (A 3.4)
30. Workers claiming unmeasured numbers require the ls -l / log line in the same turn;
    result guards live in the API, enforced, not in prompts. (A 3.4)
31. Auditing predecessor claims: re-measure live, re-run tests, import effective values.
    (A)
32. Close on the original request, not the last defect; enforcement = a skeptic review
    loop with raw evidence. (A 3.7)

## Cost
33. FACT about the data → code; JUDGMENT → a model, off the critical path. Ingest was
    3.42M tokens to read 31KB; now a 5-min script. (A 3.5)
34. The fixed head is the cost, not the payload: small purpose-built cwd + ~5KB brief
    per executor; never the repo root. 94% cache reads came AFTER this fix. (B 7)
35. Cap everything: wall clock, turns, spend, rounds; a capped run keeps its work and
    posts a partial. (B 7)
36. Fail closed on the brake: stale/unreadable spending meter = hold with a DIFFERENT
    reason word than "over the line". (B 7)
37. Alert on any config the fleet writes to itself that repoints a provider (the
    15-aux-slots incident; a worker once re-pinned a whole profile to paid). (A 3.5)
38. The owner's subscription keep-list governs routing: anything not on it is dropped
    from routing + named in the audit.

## Change discipline
39. One repo, one owner per mechanism, every change committed same-session, every guard
    proven to FAIL before trusted, changelog line per change. (A 3.6)
40. An unfired guard is an untested guard. (A 3.6)
41. Version control before multi-writer edit; six backups of one script in 24h was the
    alternative. (A 3.6)
42. Fleet agents follow whatever the wiki says: cutover of docs/skills/tools happens in
    the SAME session as the system change, or stale runbooks keep acting on a dead
    board. (A 6)
