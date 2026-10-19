# Harness Test Data DOX

## Purpose

- Hold golden files the harness tests compare against.

## Local Contracts

- Update a golden file only beside the test that consumes it and the behavior change it verifies; keep captures minimal and free of private history.
- `hermes-startup/` holds captured Hermes terminal startup screens consumed by the composer-detection test in `herdr_hermes_test.go`.

## Child DOX Index

Direct child DOX files:

| Child | Scope |
| --- | --- |
| [hermes-startup/AGENTS.md](hermes-startup/AGENTS.md) | Captured Hermes startup screens exercising runner composer-visibility detection. |
