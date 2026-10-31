# Werkrby finite command DOX

## Purpose

- `main.go` is the thin entry point for the separate, unpackaged experimental `werkrby-finite` executable.

## Local Contracts

- Delegate arguments and the build version only to `internal/finite.Run`; command parsing, feature detection, output formatting, and application logic never belong in `main.go`.
- Never import or delegate to `internal/cli` or the ordinary `werkrby` command composition.
- Mirror the ordinary thin entry point's error handling: errors implementing an exit code pass through without application-error output, while other errors use the `werkrby-finite:` prefix.
- Keep this command experimental, unpackaged, uninstalled, and outside compatibility aliases, npm, installers, packaging, and releases.

## Child DOX Index

No child DOX files.
