# Runs DOX

## Purpose

- Own the SQLite run record (`runs`) and the append-only `intents` inbox that Stage B of the structural program derives attempts from.

## Local Contracts

- A run is one provider turn that happened. Only a finished run that ended `ok`, `fail` or `timeout` is an attempt; open, `lost`, `interrupted` and `wall` runs never are.
- A run ends once: `End` never overwrites an outcome. `LostOpen` closes whatever a previous owner left open, and is called once when an owner opens the store.
- In shadow mode (`orchestrator.runs_mode: shadow`) the store sits beside the task files, which stay authoritative. Nothing in this package changes a claim, a transition or an attempt.
- Intents are appended by other writers and applied by the owner, each at most once.

## Child DOX Index

No child DOX files.
