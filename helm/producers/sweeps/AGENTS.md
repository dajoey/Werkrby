# Helm Sweeps DOX

## Purpose

- Own three deterministic watchers, each built as `fetch -> shape -> decide -> apply` with zero model on the critical path: `kuma_down` (HTTP liveness of the uptime monitor), `stack_status` (concurrent probes of configured service endpoints), `quota_floor` (local paid-token telemetry).
- Each file is both a standalone script (`--dry-run`, `--selftest`, `--heartbeat`; `kuma_down` also takes `--url`) and an importable module; the P4 suite imports only the `shape_*` and `decide_*` halves and never runs a probe.
- Turn observations into gateway threads through `producers.common`; they own no store access, no SQL against the thread database, and no answer handling.

## Local Contracts

- The four-stage split is the contract. `fetch_*` touches the world, `shape_*` is pure and returns facts, `decide_*` is pure over `(shaped, state)` and returns `{action, new_state, ...}`, and only `run_sweep` applies anything. Keep the clocks and thresholds injectable (`now`, `now_dt`, `alert_after`, `threshold`) or the offline suite cannot exercise them.
- There are exactly four actions: `noop`, `raise`, `update`, `resolve`. `raise`/`update` PUT `decision["thread"]`, `resolve` settles `decision["thread_id"]`, and every action saves `decision["new_state"]`.
- `--dry-run` returns immediately after `decide_*`: no PUT, no state write, no heartbeat. Any new branch must preserve that purity.
- State is a JSON file under `HELM_OPS_DIR`. `new_state` REPLACES the file wholesale, so keys a branch does not write are gone next pass and every read must use `.get` with a default. The write is not atomic; a truncated file is swallowed by `load_state`, which hands back a zeroed dict — the streak resets and an active alert is forgotten, so the next failure raises a second card instead of updating the first.
- Hysteresis lives in that counter: `kuma_down` alerts on the 3rd consecutive failure, `stack_status` on the 2nd, `quota_floor` has none and trips on the first tripped condition. A `raise` only becomes an `update` while the state file still says `alerted`/`tripped`; that flag is the sweep's only memory of the card.
- Thread ids: `kuma_down` and `stack_status` mint `sweep-<name>-<YYYYMMDD>` from UTC, `quota_floor` uses the fixed `t-quota-floor-guard`. Because the daily ids roll at UTC midnight, `resolve` settles `state["active_thread_id"]` rather than today's id — a recovery after midnight must close yesterday's card, not open a new one.
- The store rejects `urgency: "immediate"`; the only valid values are `now`, `this-week`, `whenever`. `kuma_down` raises with `immediate` on every trip and `stack_status` does so whenever a failed service is named in `HELM_CRITICAL_SERVICES`, so those PUTs come back 400 and — since `run_sweep` ignores the result — are still recorded locally as alerted. Correct the urgency before trusting either card.
- Every raised ask must keep `ask.requires` naming a marker from decision / money / credential / presence / contact. The store's four-test refuses a new `needs-you` choice with no marker at T1, and all three sweeps rely on `["decision"]`.
- Title dedupe strips numbers and units before fingerprinting, so `(3 fails)` and `(11 fails)` are one concern. While a previous day's card is open, today's fresh daily id is refused at T3 as a duplicate — that is intended; do not defeat it by varying the title.
- Choice options must survive being read alone: a non-empty prompt, distinct labels, and an `effect` saying what picking it does. The door rejects the rest.
- Nothing in this subtree performs what the options promise. No code here restarts a container, and the quota card's claim that dispatch is paused is not this sweep's doing — the dispatcher's spending brake is a separate mechanism over different inputs. Never write an effect no answer path can deliver.
- `stack_status` ships no probe targets. Its list is parsed at import from `HELM_CORE_SERVICES` (JSON array of `[name, url, [valid_codes]]`), so setting that variable later in the process has no effect, and a malformed value is warned about and ignored. With nothing configured, `run_sweep` returns action `unconfigured` and touches neither state nor heartbeat — which reads as a dead sweep to a heartbeat watcher.
- `kuma_down` counts only 200 and 302 as up, so an auth-protected monitor answering 401/403 reads as down. Its target comes from `KUMA_URL` or `--url`, never from a committed address.
- `quota_floor` reads local telemetry only — never a billing API. It sums `session_model_usage` token columns where `updated_at` is within the window, greps rotated `*.log` files for the literal `Model fallback`, and walks profile `config.yaml` fallback chains. Every read is wrapped, so an unreadable ledger scores zero: a broken ledger is indistinguishable from a clean one and T1 must be treated as best-effort.
- Its provider test is a substring match of `PAID_PROVIDERS` against the MODEL string, not a provider column, and `SANCTIONED_PAID_RUNGS` is a hardcoded `(provider, model)` allowlist. It must move in step with the owner's sanctioned-model policy: a legitimate rung missing from it trips T3 on every single run.
- `import yaml` inside `fetch_dirty_chains` is unguarded, so a missing PyYAML aborts the whole quota sweep with a traceback even when the token and log telemetry succeeded.
- `LOOKBACK_HOURS` is decorative: `run_sweep` never passes it and both fetchers use their own 3600-second defaults. Wire it or change the defaults — do not edit the constant and assume the window moved.
- `--heartbeat` is `store_true` with `default=True` in all three, so the flag can only turn the heartbeat on; add an explicit negative flag if it ever has to be off. `--selftest` must stay runnable with no network, no state file and no gateway.

## Child DOX Index

No child DOX files.
