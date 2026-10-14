# Orchestrator Test Data DOX

## Purpose

- Hold golden files the orchestrator tests compare against.

## Local Contracts

- `flag_off_task_prompt.golden` is the task implementation prompt a dispatch renders for a headless seat without `explicit_outcome`, captured by running `prompt_golden_test.go` on main with `WERKRBY_GOLDEN_UPDATE=1` (workspace root as `<ROOT>`, test binary as `<EXE>`). Regenerate it only from main, never from a branch under review, so it keeps proving that unflagged seats see main's prompt.

## Child DOX Index

No child DOX files.
