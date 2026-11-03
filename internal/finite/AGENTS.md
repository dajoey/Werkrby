# Finite runtime DOX

## Purpose

- Own the flat, isolated runtime behind the separate, unpackaged experimental `werkrby-finite` command.
- Keep the engine limited to human-originated finite tasks in its own dedicated state root; ordinary `werkrby` workflows and state remain unchanged.

## Local Contracts

### Command composition

- `cli.go` owns command parsing and delegation for `serve`, `intake`, `status`, `stop`, `answer`, `message`, `withdraw`, `ping`, `gateway`, `supervise`, and `test-receiver`; unrecognized commands fail explicitly, and unimplemented behavior returns a not-implemented error rather than a partial substitute.
- `serve` owns the scheduler, control, and outbox loops under the single owner lock. `intake`, `status`, `stop`, `answer`, `message`, `withdraw`, and `ping` are bounded non-secret owner-control clients and never mutate SQLite directly.
- `gateway restart` is the engine-owned drained restart of the finite Hermes gateway (`gateway_restart.go`); it never touches SQLite. Order is fixed: remove `bridge.on` (stop intake), stop the serve unit (KillMode=process keeps supervisors), drain until the procfs supervisor count and `active_agents` are both zero, then TERM (KILL only after the grace) the pid that `gateway_state.json` reports — signalled only after its cmdline contains `gateway run` and its `hermes_home` matches, and never the ordinary `~/.hermes` gateway — then the fresh gateway is booted the way hermes's own clients start it (a detached `gateway run --quiet --idle-exit`; Desktop's `gateway ensure` boot fatal-exits at startup here while the `~/.hermes` gateway holds the telegram token and the api_server port) and `gateway_state.json` must report `running` with a fresh pid, and the verification gate (fresh pid or start time, non-empty `code_sha`, every required MCP server registered in the profile agent logs since the boot, no retry-cooldown lines) must pass before serve starts and `bridge.on` returns. Every refusal or gate failure aborts with serve left stopped and intake left off: Werkrby stays off and the operator decides. A caller inside the serve unit's cgroup (a worker turn, adopted or not) is refused before anything changes; the sanctioned arming path is `systemd-run --user` or an agent outside the serve tree.
- `supervise` is the internal per-run child entry point and never opens the workflow database. `test-receiver` is a test-only durable file receiver, never a production channel. Production `runSupervisorWithBoundary` normalizes the CLI argument shape and activates exact Hermes isolation only for `herdr`; fixture and other providers retain their original context. Production serve configures real Herdr owner recovery/stop across both OS and Hermes gateway liveness and passes the Engine to `ServeControlWithWorkflow`, serializing stops and answers through `scanMu`. There is no test-only composition escape.
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
- On Linux a run's OS boundary is the recorded supervisor's process group (pgid = supervisor PID): a surviving member keeps the run live after the supervisor dies, and stops signal the whole group. Only processes whose executable is the finite binary are read as supervisors or providers; unrelated processes and other state roots' runs never decide this run's liveness.
- Real providers use a dedicated Hermes home carrying the `.finite-hermes-disposable` marker and a `wk-finite` or `wk-finite-<name>` profile; ordinary worker profiles and the default home are refused.

### Supervisor and recovery

- Assign every provider turn an immutable run ID and unique process token before dispatch, with run-private files beneath its run directory. The supervisor uses the file-only child protocol, calls the harness start, one send, and close sequence, and never shares provider session files across runs.
- Recover each open run individually: honor a durable stop fence first, check for a complete run-bound result, discover the exact run ID, and validate the OS process-start identity before adopting a live supervisor. Never start a replacement for an adopted run.
- Incomplete, timed-out, duplicate-wrapper, token-mismatch, or detached-provider evidence leaves the run open and non-dispatchable. Classify loss only after a complete reliable scan proves provider-turn absence; never bulk-close open runs or treat PID evidence alone as identity.
- Commit the durable stop fence before signalling a supervisor. Stop-pending work is non-dispatchable, unresolved termination remains open, and a stopped run is never resumed as ordinary work.

### Run inbox

- Operator steering into a live provider turn is a run-private append-only `inbox.jsonl` beside `prompt.txt`: `POST /v1/message` appends one line `{"schema_version","id","kind","text","created_at"}` (kind `message` or `interrupt`) for the task's single open run, `POST /v1/withdraw` removes an undelivered line, and `GET /v1/inbox` reads messages, the worker's `inbox-ack.jsonl` acknowledgement ids, and the run's last provider event line back. Each route resolves the task through SQLite and refuses with 404/409 when no open run exists; `status` reports the open run's id and phase.
- Delivery is exactly-once per message id (a repeated id answers duplicate and appends nothing; an interrupt with a queued message's id upgrades that line in place; an acknowledged id answers delivered and never changes), and never touches the supervisor or the provider: one attempt stays one provider call. The worker prompt owns the reading side: check the inbox between steps, quote the message when acting on it, and append handled ids to `inbox-ack.jsonl`. A crash-torn append tail is sealed with a newline on the next append and skipped by readers; withdrawal and upgrade rewrite the whole file atomically under the handler's inbox lock.

### Projection and delivery

- Regenerate each Markdown projection from committed SQLite state using atomic complete-file publication. Never parse the projection for workflow decisions, use status folders, redispatch, roll back settlement, or create workflow events because a projection is stale, missing, or damaged.
- Keep internal task, run, result, event, and outbox settlement atomic and exactly-once. Delivery is idempotent at-least-once with deterministic delivery IDs and immutable payloads; arbitrary worker side effects are not exactly-once.
- The test receiver persists deliveries beneath its own explicit disposable root, deduplicates visible effects independently by delivery ID, rejects reuse with conflicting payload bytes, and survives acknowledgment loss and restart without becoming workflow authority.

## Child DOX Index

No child DOX files.
