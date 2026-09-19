# Helm DOX

## Purpose

- Helm is the decision gateway: a Python service that holds the list of asks waiting on one human, and guarantees that a card never lies about its state.
- `app/` serves the HTTP surface, `helm/` holds the durable store kernel, `dispatcher/` runs work against threads under safety rails, `producers/` files threads from unattended jobs and sweeps, `learning/` classifies replies, and `tools/`, `scripts/`, `prompts/`, `docs/` and `tests/` support those four.
- This subtree is imported source, not part of the Go build. Nothing under `cmd/`, `internal/` or `npm/` imports it, and `go build ./...` neither compiles nor tests it.
- It runs as its own container: `Dockerfile` copies only `app/` and `helm/`, serves `app.service:app` on `HELM_PORT`, and keeps state in the `/data` volume. The remaining directories are host-side tooling that talks to that service over HTTP.

## Local Contracts

- This is a scrubbed published snapshot of a private deployment. Every deployment coordinate is read from the environment — `HELM_URL`, `HELM_APP_TOKEN`, `HELM_DISPATCH_ENV`, `HELM_CORE_SERVICES`, `HELM_CRITICAL_SERVICES`, `KUMA_URL`, `HELM_OPS_DIR`, `HELM_EXTRA_PATH`, `INFISICAL_*` — and defaults are loopback or empty. Never commit a host address, an internal hostname, an absolute home path, a credential filename or a real person's name back into this subtree; if a default would only be correct on one machine, make it an environment variable instead.
- The store is the product. Thread state, the event log, asks and answers live in SQLite behind `helm/store.py`; every other module reaches them through its functions, never through raw SQL of its own.
- A thread's truth is its events, not a summary column. Read the last ask and the last owner reply from the event log; state fields and answer flags are derived and have been misread before.
- An agent attaching its own material to a card must never be readable as the owner answering it. The uploader decides the event kind; nothing else does.
- `tests/run_all.py` discovers every `tests/test_*.py` and is the gate: 17 suites, 870 assertions green. Three suites — `test_dispatcher`, `test_ladder`, `test_p7_resilience` — additionally require the dispatcher jail's systemd service mode and fail identically on an unmodified tree inside a plain container; that is an environment limit, not a regression.
- Run the suite with the service dependencies present (`requirements.txt`: starlette, uvicorn, httpx, pyyaml). A bare interpreter skips eight suites on import errors and still prints a pass count.
- Phase contracts P5 and P5b described deployment of the owner's own fleet rather than the product, and were deliberately left out of this import; code comments that cite them refer to material that is not in this repo.
- `docs/contracts/P1.md` through `P7.md` are historical build contracts, not current API documentation. Where a contract and the code disagree, the code is what ships.

## Child DOX Index

Direct child DOX files:

| Child | Scope |
| --- | --- |
| [app/AGENTS.md](app/AGENTS.md) | HTTP surface: routes, middleware, authentication. |
| [dispatcher/AGENTS.md](dispatcher/AGENTS.md) | Worker dispatch loop, jail, brake, caps and watchdog. |
| [docs/AGENTS.md](docs/AGENTS.md) | Plan, lessons, spikes and the phase contracts. |
| [helm/AGENTS.md](helm/AGENTS.md) | The durable store kernel and its invariants. |
| [learning/AGENTS.md](learning/AGENTS.md) | Reply classification. |
| [producers/AGENTS.md](producers/AGENTS.md) | Unattended jobs and sweeps that file threads. |
| [prompts/AGENTS.md](prompts/AGENTS.md) | Prompt texts used by dispatched work. |
| [scripts/AGENTS.md](scripts/AGENTS.md) | Bootstrap and spike harness scripts. |
| [tests/AGENTS.md](tests/AGENTS.md) | The suite runner and what each suite guards. |
| [tools/AGENTS.md](tools/AGENTS.md) | Command-line helpers for filing and running threads. |
