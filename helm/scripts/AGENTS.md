# Helm Scripts DOX

## Purpose

- Own the worker bootstrap (`bootstrap.sh`) that fetches a Git credential into a shell at runtime.
- Own the P2b executor spike harness: `spike_exec.sh` spawns one capped, scoped, headless run; `spike_report.py` turns it into one result and posts it once; `spike_receiver.py` is a throwaway endpoint that proves the result landed.
- This is measurement tooling, not production dispatch. `dispatcher/` owns spawning real work; the findings these scripts produced are written up in `docs/SPIKE-P2b.md`.
- Nothing here ships in the service image — the `Dockerfile` copies only `app/` and `helm/` — so these run host-side against the service over HTTP.

## Local Contracts

- `bootstrap.sh` is **sourced, never executed**: it exports `GITEA_TOKEN`, `GIT_ASKPASS` and `GIT_TERMINAL_PROMPT=0` into the caller's shell, and its `EXIT` trap removes the askpass helper. Run in a subshell it accomplishes nothing and leaves the caller without a credential.
- Every vault coordinate is caller-supplied: `INFISICAL_IDENTITY_FILE` names the machine-identity env file (required; it must define `INFISICAL_URL`, `INFISICAL_CLIENT_ID`, `INFISICAL_CLIENT_SECRET`, `INFISICAL_PROJECT_ID`), `INFISICAL_SECRET_PATH` is the URL-encoded folder (required, no default), `GITEA_TOKEN_SECRET` defaults to `GITEA_TOKEN` and `INFISICAL_ENV` to `prod`. Never bake a URL, folder or identity path into this file.
- The secret never reaches argv, the repository or a log: the login token is `unset` after the fetch, the askpass helper is a `chmod 700` file under `/dev/shm` that prints `GIT_USERNAME` (default `git`) or the token, and an empty fetched secret is a hard failure — `return 1` when sourced, `exit 1` otherwise.
- `spike_exec.sh <prompt_file> <run_dir> [KEY=VALUE ...]` enforces every cap **in the runner, never by asking the prompt**. Wall clock is `timeout(1)` (`TIMEOUT_S`, default 300s, with `--kill-after=10s`); `MAX_BUDGET_USD` exists only on the claude branch; `MAX_TURNS` is carried as reporting data because neither measured runtime exposes a turn flag. Trailing `KEY=VALUE` arguments are exported; anything else exits 2.
- Exit codes are a contract: 2 bad argument or unknown `RUNTIME`, 3 refused credential, 4 receiver health check failed, 6 cannot enter the run dir; otherwise the executor's own return code, where 124 means `timeout(1)` killed it and is reported as `timed_out`, never as the executor failing on its own.
- Refuse to spend on a pay-go credential before doing any work: any `ANTHROPIC_API_KEY` set, or an `ANTHROPIC_AUTH_TOKEN` matching `sk-ant-api*`, aborts. A subscription token (`sk-ant-oat01-*`) is inherited from the environment and is never echoed, written into the run dir, or committed.
- Prove `RECEIVER`'s `/health` answers before spending anything, so a missing POST afterwards can only mean the POST failed — never "the receiver was never listening".
- **cwd scopes nothing.** Measured: both runtimes, handed an absolute path, wrote outside their run dir. `HERMES_WRITE_SAFE_ROOT` (defaulted here to the run dir) is the only mechanism observed to actually refuse, and `--in` is required because that runtime otherwise resolves relative paths against the home directory. The claude branch has no measured write boundary and is trusted-prompt only. Do not weaken that default or re-document cwd as a wall.
- Shell and delegation tools are denied by name: `Bash,BashOutput,KillShell,Agent,Task,WebFetch,WebSearch,NotebookEdit,SlashCommand` on the claude branch and `HERMES_TOOLSETS` defaulting to `file,todo` on the other. An executor holding a shell makes any write-scope claim theatre, and a denied run was observed reaching for the `Agent` tool as its route around a denial.
- The harness posts the result, not the model: the executor gets no network tool at all, so "exactly one result per run" is a property of `spike_report.py` rather than something the model must cooperate with.
- `spike_report.py` takes its inputs from the environment (`RUN_ID`, `RUN_DIR`, `RC`, `DURATION`, `TIMED_OUT`, `RECEIVER`, `TIMEOUT_S`, `MAX_TURNS`, `MAX_BUDGET_USD`, `MODEL`, `RUNTIME`), not argv, so no run detail lands in a world-readable process list. A new input means a new environment variable in both files.
- One fixed result shape with a small per-runtime reader each, so the dispatcher never learns which CLI ran. What a runtime cannot report stays `null` — that is a measurement, not a hole to guess at — and `caps.*_enforced_by` names who holds each cap, reading `none` when the runtime offers one in name only.
- Costs are only summed when they mean something: a `cost_status` other than `ok` forces `cost_usd` to `null` rather than zero, and a claude run reports the whole `modelUsage` breakdown with the costliest model as the model of record, because one run bills a helper model alongside the one asked for.
- `capped` means the run stopped on our own ceiling and kept its work; it is a partial, not a failure. `answer_file` is parsed from the run dir's `result.json` rather than from the model's prose, which is what makes "it wrote inside its run dir" a measurement.
- `spike_receiver.py` is a deliberate stand-in: stdlib only, binds `127.0.0.1` and never `0.0.0.0`, no auth, no validation, no state, appending one fsynced JSON line per POST and answering `/health` on GET. The real result endpoint belongs to the Helm service; do not grow this file into it.

## Child DOX Index

No child DOX files.
