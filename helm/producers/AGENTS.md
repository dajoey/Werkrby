# Helm Producers DOX

## Purpose

- Own the shared client every unattended producer uses to reach the gateway: `common.py` holds credential/URL loading, `put_thread`, `settle_thread`, `touch_heartbeat` and `needs_joey_self_check`, and nothing else.
- Decide nothing. No judgement about whether a concern is worth a card lives here; that belongs to a sweep's pure `decide_*` function and, finally, to the store's own door.
- `sweeps/` holds the deterministic watchers that consume these helpers.
- The import name is `producers.common`. Callers put this package's PARENT directory on `sys.path` — the Helm subtree root, not the git repository root — which is why the sweeps' `REPO_ROOT` constant is two levels up from a sweep file and stops there.

## Local Contracts

- `load_helm_credentials()` returns the environment pair only when BOTH `HELM_URL` and `HELM_APP_TOKEN` are set. Otherwise it reads the single file named by `HELM_DISPATCH_ENV`, whose only fallback is `helm-dispatch.env` resolved against the process working directory — so an unattended run's cwd is load-bearing. It parses only `KEY=value` lines, skips `#` lines, and strips one layer of surrounding quotes; a shell `export ` prefix is NOT stripped and yields a key that never matches.
- `DEFAULT_HELM_URL` is captured from the environment at import time, so changing `HELM_URL` afterwards does not move the fallback. The loopback default is a sane last resort, never a deployment coordinate: no host, port or path of a real deployment may be committed here.
- No token means no write. `put_thread` and `settle_thread` return `(False, {"error": "HELM_APP_TOKEN not configured"})` and send nothing. That fail-closed behaviour is deliberate; never supply a built-in token or a silent success.
- `put_thread` sends exactly one thread as `{"threads": [thread]}` in a PUT to `/api/triage/threads` and returns `(status in (200, 201), body)`. With a single thread a store refusal arrives as 400, so the boolean is trustworthy; batch several and the route answers 200 with a `rejected` list, and the boolean alone becomes a lie.
- Callers must read that boolean. Every sweep here currently discards it and then saves its "alerted" state anyway, so a refused card leaves local state asserting an alert that exists nowhere. New code in this directory checks the result before recording success.
- `settle_thread` PUTs a body of `thread_id` + `state: "done"` + one event, and the producer door writes every column from the payload it is given. Settling through it therefore blanks title, summary and category, resets urgency to `whenever`, empties sources, and silently drops `answer_applied`/`nothing_owed` (that route never reads them). Against an unknown id it CREATES a blank `done` row rather than failing.
- The settle event is `{ts: epoch float, source: "sweep", kind: "settled", body: note}`. After the blanking above, the event log is the only surviving record of what the card said — treat events as the truth, never the row's summary column.
- `touch_heartbeat` replaces the file with two lines, an ISO-8601 UTC timestamp then an epoch float, creating the parent directory. It returns `False` after a WARN on stderr when the write fails, and writes nothing at all under `dry_run`. Freshness of that file is what an external watcher reads, so never make the touch conditional on the decision.
- `needs_joey_self_check` POSTs `{"thread": ...}` to `/api/triage/needs-joey-check`. It fails OPEN: with no token it returns `(True, {"skipped": true})`, and only a transport failure returns `False`. It is an early warning, not a gate — the store re-runs the same four-test at its own door on every PUT — and no sweep in this tree calls it today.
- `dry_run` must stay total for every helper added here: no network call, no file written, no state mutated. The sweeps' dry-run purity rests entirely on that promise.
- Nothing here retries, backs off, batches or queues. One attempt per call, `timeout` defaulting to 15 seconds (10 for the self-check). A producer that needs delivery guarantees implements them itself.

## Child DOX Index

Direct child DOX files:

| Child | Scope |
| --- | --- |
| [sweeps/AGENTS.md](sweeps/AGENTS.md) | Deterministic fetch/shape/decide/apply watchers. |
