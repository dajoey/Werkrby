# Command DOX

## Purpose

- Own thin executable composition for the `werkrby` binary, its `spyjo`/`spynel` compatibility entry points, and the separate experimental `werkrby-finite` binary.

## Local Contracts

- `werkrby` with no subcommand launches the TUI and all enabled background services.
- `werkrby serve` runs enabled server channels and orchestration without requiring a terminal UI.
- Plain `werkrby send`, `followup`, `notify`, `status`, `conversations`, and `command` entry points remain thin adapters over `internal/cli` and the elected application service; they do not duplicate application behavior here. `notify` queues bounded input through the durable outbox and never starts a harness.
- `werkrby docs` remains a thin offline adapter over the curated `internal/agentdocs` catalog and must never initialize a workspace, contact a primary/local API, or start a harness.
- Ordinary command handlers delegate to `internal/cli`; do not put application logic in `main.go`.
- `werkrby-finite` delegates only to `internal/finite.Run`, never to `internal/cli`; its entry point remains composition-only and the command stays unpackaged and experimental.
- Preserve the npm launcher's dedicated update exit code without printing it as an application error; npm package replacement and restart remain launcher responsibilities.

## Child DOX Index

Direct child DOX files:

| Child | Scope |
| --- | --- |
| [spyjo/AGENTS.md](spyjo/AGENTS.md) | `spyjo` compatibility entry point, same body as `werkrby`. |
| [spynel/AGENTS.md](spynel/AGENTS.md) | `spynel` compatibility entry point, same body as `werkrby`. |
| [werkrby-finite/AGENTS.md](werkrby-finite/AGENTS.md) | Separate unpackaged experimental finite-task executable over `internal/finite`. |
| [werkrby/AGENTS.md](werkrby/AGENTS.md) | Primary `werkrby` executable over the shared CLI entry point. |
