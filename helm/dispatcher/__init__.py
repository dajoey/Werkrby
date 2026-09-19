"""Dispatcher v2 — the executor-agnostic pump (contract: docs/contracts/P3.md).

NO model inside the pump. The tick claims a thread (POST /claims BEFORE any
spawn), runs one executor adapter under an OS-level jail with caps held by the
runner, and records exactly one result through the claim surface. Executors get
no network; the harness posts results via the gateway API.

Modules:
  adapters.py  — the SPIKE-P2b §7 adapter interface + claude/hermes readers
  caps.py      — cap set + WHO enforces each cap (enforced_by is load-bearing)
  jail.py      — OS-level jail outside the CLI, with loud fallback
  brake.py     — spending brake over hermes-usage-exporter artifacts
  retention.py — run-dir 30d sweeper (the work version hit 1.3 GB in 10 days)
  tick.py      — the claim-before-spawn loop; CLI: --dry / --selftest
  watchdog.py  — cross-host monitor; pages via a Helm thread only
"""
