# Finite runtime DOX

## Purpose

- Own the flat, isolated runtime behind the separate, unpackaged experimental `werkrby-finite` command.
- Keep the experiment limited to human-originated finite tasks in disposable test state; ordinary `werkrby` workflows and state remain unchanged.

## Local Contracts

### Command composition

- `cli.go` owns command parsing and delegation for `serve`, `intake`, `status`, `stop`, `answer`, `supervise`, and `test-receiver`; unrecognized commands fail explicitly, and unimplemented behavior returns a not-implemented error rather than a partial substitute.
- `serve` owns the scheduler, control, and outbox loops under the single owner lock. `intake`, `status`, `stop`, and `answer` are bounded non-secret owner-control clients and never mutate SQLite directly.
- `supervise` is the internal per-run child entry point and never opens the workflow database. `test-receiver` is a test-only durable file receiver, never a production channel.
- Dependency direction is `cmd/werkrby-finite -> internal/finite -> internal/harness, internal/core, internal/fsx`. Never import `internal/cli`, `internal/app`, `internal/localapi`, `internal/instance`, `internal/updater`, `internal/orchestrator`, or `internal/runs`; `internal/harness` must not import this package.

### State and authority

- Require an explicit disposable state root distinct from the worker target workdir and every legacy `.werkrby` tree. Never infer a root, share one with ordinary Werkrby, or initialize legacy configuration or workspace state.
- The state root contains `engine.db`, immutable `briefs/<task-id>.md` intake copies, generated `tasks/<task-id>.md` projections, per-run `runs/<run-id>/` directories, and the finite-owned `owner.lock`.
- Intake submits the human Markdown brief through the owner, copies and hashes it immutably, and commits the initial SQLite task before scheduling can observe it. After intake, SQLite is the sole workflow authority; brief and projection edits never drive workflow state.
- Do not add goals, planner rounds, alerts, semantic heartbeat, notification agents, conversation recovery, production channels, multi-primary election, legacy-state import, migrations, or fallback state readers.

### Ownership and platform boundaries

- Acquire the finite-owned nonblocking `owner.lock` before database creation or mutation, control publication, projection, outbox activity, or supervisor spawn. Exactly one owner may write; a lock loser writes and spawns nothing, and there is no election or secondary writer.
- Never let supervisors inherit the owner-lock descriptor. Keep the lock path stable within the explicit state root and separate from ordinary Werkrby election records.
- Support dispatch only on Linux and Windows. On every other platform, report process liveness as unknown and fail closed before dispatch rather than treating a process as absent.

### Supervisor and recovery

- Assign every provider turn an immutable run ID and unique process token before dispatch, with run-private files beneath its run directory. The supervisor uses the file-only child protocol, calls the harness start, one send, and close sequence, and never shares provider session files across runs.
- Recover each open run individually: honor a durable stop fence first, check for a complete run-bound result, discover the exact run ID, and validate the OS process-start identity before adopting a live supervisor. Never start a replacement for an adopted run.
- Incomplete, timed-out, duplicate-wrapper, token-mismatch, or detached-provider evidence leaves the run open and non-dispatchable. Classify loss only after a complete reliable scan proves provider-turn absence; never bulk-close open runs or treat PID evidence alone as identity.
- Commit the durable stop fence before signalling a supervisor. Stop-pending work is non-dispatchable, unresolved termination remains open, and a stopped run is never resumed as ordinary work.

### Projection and delivery

- Regenerate each Markdown projection from committed SQLite state using atomic complete-file publication. Never parse the projection for workflow decisions, use status folders, redispatch, roll back settlement, or create workflow events because a projection is stale, missing, or damaged.
- Keep internal task, run, result, event, and outbox settlement atomic and exactly-once. Delivery is idempotent at-least-once with deterministic delivery IDs and immutable payloads; arbitrary worker side effects are not exactly-once.
- The test receiver persists deliveries beneath its own explicit disposable root, deduplicates visible effects independently by delivery ID, rejects reuse with conflicting payload bytes, and survives acknowledgment loss and restart without becoming workflow authority.

## Child DOX Index

No child DOX files.
