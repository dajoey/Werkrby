# Helm Tools DOX

## Purpose

- Own `file_helm_thread.py`: the command-line escalation path that turns an unattended job's failure — or its request to be looked at — into one Helm thread over HTTP.
- Own `run.py`: a shell-free wrapper that runs one argv and prints the tails of both streams plus the exit code.
- Both are host-side helpers outside the service image; they talk to the running service rather than importing it.

## Local Contracts

- `build_thread()`, `job_slug()` and `read_tail()` are pure apart from reading the log file: no network, no writes, and `now` is injectable so a test pins the date. `tests/test_p4_producers.py` imports them directly — keep them importable and side-effect free.
- The thread id is stable per job per calendar day: `sched-<slug>-failure-<YYYYMMDD>` when the exit code is non-zero, `sched-<slug>-review-<YYYYMMDD>` when it is `0`. Repeated failures the same day upsert the one thread instead of duplicating; the next day gets a fresh id so a thread the owner already settled is never silently reused. Changing the id shape breaks both halves of that.
- `job_slug()` folds every non-alphanumeric character to `-`, collapses runs, strips the ends and falls back to `job`. The result is part of a URL path and must stay path-safe.
- Filed threads are `state: "needs-you"`, `category: "Scheduled Jobs"`, urgency `this-week` for a failure and `whenever` for a review, with an `ask` of `kind: "ack"` — a pure FYI, no decision asked. `summary` is clipped to 2000 characters; the tail is the last `TAIL_LINES` (40) lines of the log, appearing both inside the summary (last 800 characters) and as a `log-tail` event on the failure path.
- A missing or unreadable log is reported in-band as text — `(log not found at ...)` — and never raises. An escalation must still file when its own evidence is gone.
- Deployment coordinates are environment-only: `HELM_URL` (default loopback `http://127.0.0.1:3038`), `HELM_APP_TOKEN`, and `HELM_DISPATCH_ENV` naming a `KEY=VALUE` env file whose `#` comments and quotes are stripped on read, with a fixed working-directory filename as the single fallback. Never commit a host, port or credential path that is only right on one machine.
- The wire format is `PUT /api/triage/threads` with body `{"threads": [thread]}` and a bearer token; success is HTTP 200 or 201. Keep this envelope in step with the service route it targets.
- Exit codes are the caller's contract: `0` filed or dry run, `2` missing token or a Helm refusal. The tool prints the reason and must never mask the job's own exit code, because callers wire it as `<job> || file_helm_thread.py <job> $? <log>`.
- `--dry-run` must stay total: it parses, builds and prints the whole thread with zero network calls and zero disk writes, which is what makes the formatting testable on any host.
- `planka_fallback()` only writes a `[HELM-FALLBACK]` line to stderr and returns `True`. It is a named stub for "Helm did not take it", not a second filing path; do not restore a card-board integration here — Helm is the ask surface.
- `run.py` exists because the spike worker's shell refuses compound commands (pipes, `&&`, redirection). It passes `sys.argv[1:]` to `subprocess.run` with no shell, prints the last 8000 bytes of stdout, the last 4000 of stderr, then `[rc=N]`, and writes nothing to disk. It must never grow shell interpolation, argument rewriting or secret handling — argv is world-readable.

## Child DOX Index

No child DOX files.
