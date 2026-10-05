# Roster DOX

## Purpose

- Own the optional workspace staffing file `.spynel/roster.yaml` and its private per-day usage count.

## Local Contracts

- A missing roster means legacy routing; an invalid roster returns an error the caller logs before using legacy routing. Roster problems never fail a dispatch.
- Staff names and runner kinds are validated identifiers; launch arguments are single-line strings passed to the process without shell expansion.
- A daily cap requires a fallback. A session key keeps the staff it was first given that day, so retries and recoveries count once. The last fallback link is used even when capped, so work always has a runner.
- `escalation` is one mapping or a list of rungs; an implementation attempt goes to the rung with the highest `after_attempt` below it. A task's own valid `staff:` pin outranks every rung except the top one.
- A staff may carry its own `escalate_after` and `escalate_to` (set together; `escalate_after` at least 1, `escalate_to` a different defined staff, no cycles). Once an implementation task's attempt is above `escalate_after` it runs on `escalate_to` instead, whether the task is pinned to that staff or reaches it as the role default; the target may escalate again on its own thresholds. It is per staff on purpose: the global `escalation` ladder sends every task to one seat whatever its job, so a retry can leave its lane; this keeps a retry inside it. It applies after the global ladder and never touches review, goal or chat roles.
- `notify_at` never changes routing: reaching it writes one parked task per staff per day into `tasks/waiting/` with a `joey_ask` choice (continue / stop). The workspace pump asks Joey; the resumed task records his answer in `runtime/roster-decisions.json`. `continue` lifts `daily_cap` for that staff that day, `stop` halts it at once, no answer leaves `daily_cap` as the halt. `notify_at` must be below `daily_cap`.
- Workflow document phases are routed by the orchestrator; `chat`, `notification`, and `heartbeat` roles are routed by the Herdr harness from the session key, only when the turn carries the configured default model (an explicit per-conversation model wins).
- `routing` is the optional machine-readable domain table task filers stamp `staff:` from. Each entry names one `domain`; empty `staff` means filers omit the pin (role default), a `held_until` marks a HELD domain whose named `staff` is the temporary fallback. Domains are unique and single-line, staff names must exist, and a held domain must name its fallback. The authoritative human table lives in the workspace chat-agent instructions; this block is its validated machine copy, and a missing block stays legacy-compatible.
- The usage count under `runtime/` is bookkeeping only and never workflow authority.

## Child DOX Index

No child DOX files.
