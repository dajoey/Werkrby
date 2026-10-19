# Documentation DOX

## Purpose

- Own the slim Werkrby fork documentation set: fork intent, upstream differences, setup/usage deltas, and pointers to canonical upstream docs. Full product documentation lives upstream; this directory must not regrow verbatim upstream copies.

## Local Contracts

- `werkrby-fork.md` owns intent, the current differences list, setup/usage deltas, and pinned upstream links. `README.md` stays a short index into it.
- Keep public positioning aligned with the root README: Werkrby is Joey's personal fork/customization of Spynel, not a competitor. Adapt copy length to its surface without inventing product facts or treating conceptual scale as a resource guarantee.
- Commands and configuration examples must match executable behavior: the command is `werkrby` (with `spyjo` and `spynel` aliases where installed), built from `./cmd/werkrby` via `scripts/dev.sh build` and installed via `scripts/install-dev.sh`. Never document the upstream `install.sh`/npm paths as the way to install this fork.
- Keep upstream links pinned to the immutable tag recorded in `werkrby-fork.md`; re-pin only when the fork's upstream base moves.
- Keep root-README image assets under `.github/resources/`; `docs/` owns documentation content rather than repository-presentation artwork.

## Child DOX Index

Direct child DOX files:

| Child | Scope |
| --- | --- |
| [assets/AGENTS.md](assets/AGENTS.md) | Generated fork artwork: icon set, social preview, and the generator source. |
| [tracking/AGENTS.md](tracking/AGENTS.md) | Fork-drift alarm pin tracking state for external branches. |

