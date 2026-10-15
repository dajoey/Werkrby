# Runs DOX

## Purpose

- Own the SQLite run record (`runs`) and the append-only `intents` inbox that Stage B of the structural program derives attempts from.

## Local Contracts

- A run is one provider turn that happened. Only a finished run that ended `ok`, `fail` or `timeout` is an attempt; open, `lost`, `interrupted` and `wall` runs never are.
- A run ends once: `End` never overwrites an outcome. `LostOpen` closes whatever a previous owner left open, and is called once when an owner opens the store.
- In shadow mode (`orchestrator.runs_mode: shadow`) the store sits beside the task files, which stay authoritative. Nothing in this package changes a claim, a transition or an attempt.
- `Check` compares the attempt a claim charged with the one the document's previous run implies and returns a verdict: `match`, `refund` (the previous run counted but the files did not advance: a known carve-out site or a recovery), `disagree` (what the Stage B check counts), `unseeded` or `previous-open`. The orchestrator logs it as `runs-shadow: attempts <verdict> ...` at every dispatch.
- Intents are appended by other writers and applied by the owner, each at most once.

## Child DOX Index

No child DOX files.
