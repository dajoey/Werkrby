# Standalone Helm — build plan (2026-09-12)

Owner: Joey. Orchestrator: Hermes (desktop session). Workers: claude/codex/opencode CLI
agents under contract files. Tracker: Gitea `dajoey/helm` issues (kanban retired
2026-09-12; chat is not a scheduler).

## Inputs that govern this build

- `docs/LESSONS.md` — merged lessons from the two post-mortems (the pitfall report and
  a prior internal porting report). Every lesson is a REQUIREMENT here, not advice.
- The subscription keep-list and the audit scope (kept out of this repo). Providers not
  on the keep-list are dropped from routing; the audit reports measured usage per
  subscription and names what to cancel/downgrade.
- `gateway-live/` — a snapshot of the live gateway and its git history, not included in
  this repo. The API the existing Android app + web UI speak today. Compatibility
  target.
- `work-reference/` — a prior internal Helm kernel (store.py + 158-case test suite,
  dispatcher algorithm, boundaries, UI patterns), not included in this repo. Port
  verdicts per its porting notes.

## Product definition

One system: **Helm is the ask surface AND the execution engine.** A thread IS the work
item. There is no second board. An ask raised from blocked work routes back to the SAME
thread when answered. The glue loop of the retired system is structurally impossible:
no component may create a top-level work item in response to another item's state.

## State lattice (every state has a machine-checkable exit)

```
needs-you ──(Joey taps option/ack)──┬─ done            (terminal:true or ack tap)
                                    └─ working        (queued for dispatch)
working ──(executor result)──┬─ needs-you      (must carry ask; answer gate applies)
                             ├─ waiting-external (park: blocked_on ≥8 chars ≠ "Joey",
                             │                    recheck_at ≤30d, optional GATE: cmd)
                             ├─ snoozed        (wake_at; in-store wake sweeper)
                             └─ done           (ONLY mechanical bot-close: ack-tapped,
                                                or answered-decision per 2026-09-12 rule)
waiting-external ──(recheck due | GATE passes | source resolved)──> working | needs-you
snoozed ──(wake_at passes)──> needs-you (ask preserved) | working
done ──(reopen)──> needs-you (fresh ask). A PUT may never resurrect done.
```

Parks expire at 30d into a real ask. Dispatch retries exhaust into a gave-up counter
(visible) with a restore-to-ack endpoint. Unknown assignee/executor rejected at
creation, loudly.

## Phase plan → issues in dajoey/helm

| # | Issue | Deliverable | Gate |
|---|---|---|---|
| P0 | Reconcile truth | this repo; live tree + history captured; subscriptions doc | pushed, issues filed |
| P1 | Store kernel | `helm/store.py` green on work tests + new park/answer/self-contained/bot-close gates as code w/ tests | offline suite green; adversarial probes 400 |
| P2 | Service cutover | new container from this repo on the gateway host :3038, same token, DB migrated reversibly; Android + web verified | real tap flows E2E |
| P2b | Executor runtime spike | headless Hermes one-shot (`hermes -p` or lane profile) with tool deny + hooks + caps; or fallback pattern | working run + cost measured |
| P3 | Dispatcher v2 | work §5 algorithm: claim-before-spawn, caps, partial-keeps-work, snoozes-not-rounds, auth-fail≠retry, gave-up loud; executors on Anthropic sub; spending brake wired to hermes-usage-exporter; separate-host watchdog | one real answered thread executed E2E |
| P3b | Self-resolution ladder | needs_joey four-test as code; retries/backoff/one-more-round; round-cap → second-lane skeptic-lite review of artifact; bare cards never light "what do you want" — options derived | probes + live test |
| P3c | Learning loop | correction classifier (deterministic) → lesson thread (choice: adopt / adopt-edit / not-the-lesson / one-off) → lesson ledger in store → active data rendered into prompts; gate-level lessons implemented w/ changelog | live test on a real correction |
| P4 | Producers | escalation path for every unattended job (stable id/job/day, ack-only, log tail, fallback); repoint `file_issue` + spark `helm_threads`/`planka_cards`/`board_pulse` at Helm; Kuma-down + stack-status sweeps; Planka ingest retires when repointed | dry-run then live for each |
| P4b | Efficiency audit | per-subscription measured usage from hermes-usage-exporter + vendor dashboards; drop-list routed out; cancellation/downgrade recommendations as ONE summary thread for Joey | data-backed report |
| P5 | Fleet cutover session | wiki (~20 playbooks), skills (helm-triage-ops index, kanban-* retire), spark tools, Ops Log entries; same-session as P4 | wiki grep for dead refs = 0 |

## Standing rules for every worker on this repo

1. Contract file `docs/contracts/P<n>.md` is the spec. Read it before touching code.
2. The offline test suite is the only evidence. `python3 tests/run_all.py` green before
   any claim of done. A gate is not a gate until a probe that should 400 returns 400.
3. No LLM on the critical path for facts (freshness, dedup, urgency heuristics):
   deterministic code. Judgment (is-this-still-true, weak asks, lesson drafting) runs
   off-path on local Ollama or the subscription, never pay-go.
4. Executors/agents run on the Anthropic subscription bucket (back off on 429, never
   detour to paid). Pay-go providers get ZERO roles in this system. Auxiliary slots
   (titles, compression, approvals) pinned local-first, no pay-go fallback.
5. Secrets only via Infisical fetch-and-consume; never in argv, files, cards, commits.
5b. Never rebuild the live gateway container outside the P2 cutover runbook — deploy path
    is repo → build → verify code-in-image (COPY app/ trap).
6. Every change lands with: tests, changelog line, and its issue closed by quoting the
   gate that passed (not prose). "Done" = demonstrated.
7. If a contract is ambiguous, file the question on the issue, do not improvise.
8. run dirs under `data/runs/` get 30d retention enforced by the dispatcher (the work
   version hit 1.3 GB in ten days).
9. One owner per mechanism; edits to `helm/store.py` gates require contract-file signoff
   in the commit message ("contract: P1 §x").
10. Watchdogs never share a failure domain with their subject (separate host or clock).

## Decision log

- 2026-09-12 Owner: the subscription keep-list governs routing. Providers not on the
  keep-list are dropped from routing and named in the audit.
- 2026-09-12 Owner: whole thing to be done, divided for delegation. Tracker = this repo's
  issues.
- 2026-09-12 Owner: board retirement not yet ruled on (design leaves it as a P4 producer
  repoint; final call lands as a needs-you thread before P4 closes).
- 2026-09-12 Owner (earlier, standing): no reboots/restart of in-use surfaces without
  explicit go; the live gateway serves a phone right now.
- 2026-09-12 Owner (mid-build): post-migration the retired board gets REPURPOSED as the
  agents' own board (agent-to-agent decomposition, lanes, retries) with one enforced
  rule: nothing on it may ever page the owner — the only door is a Helm thread.
  Issue #10 (P5b).
- 2026-09-12 Owner (mid-build): Helm should be executor-agnostic — a standalone product
  (API/MCP) that whatever agent runtime claims work from it; the Hermes kanban is just
  one claimant, and the core carries no Hermes-specific concepts. Drives: claim-based
  dispatch contract (POST /claims -> run -> POST result), executor identity as data
  (executor_id, runtime, model, cost) not code, and the store/api staying free of
  runtime coupling. Also the monetization lens: what is sellable is exactly what is not
  coupled to this fleet — the boundary is now a design requirement, not a side effect.
