# spyjo Compatibility Entry Point DOX

## Purpose

- `main.go` keeps `go build ./cmd/spyjo` working: a compatibility entry point
  with the same body as `cmd/werkrby` (same behavior and `werkrby:` error
  prefix).
- It holds no logic of its own; all command parsing and lifecycle live in
  `internal/cli`.

## Local Contracts

- Keep this entry point byte-for-byte equivalent to `cmd/werkrby/main.go`;
  never add argument parsing, feature detection, or output formatting here.

## Child DOX Index

No child DOX files.
