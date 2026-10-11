# Werkrby CLI DOX

## Purpose

- `main.go` is the primary `werkrby` executable: a thin entry point over the
  shared `internal/cli` package.
- It holds no logic of its own; all command parsing and lifecycle live in
  `internal/cli`.

## Local Contracts

- Keep this entry point thin: never add argument parsing, feature detection,
  or output formatting here.
- Errors print with the `werkrby:` prefix; the dedicated npm update exit code
  passes through without being printed as an application error.

## Child DOX Index

No child DOX files.
