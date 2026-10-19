# Admission DOX

## Purpose

- Own the quota admission decision: whether a dispatch may start now and on which seat, read from the roster, recorded seat decisions and holds, and the usage collector's snapshot.

## Local Contracts

- Answer one question per request — run or wait, and which seat — without writing any state; the orchestrator logs the verdict beside its own outcome.
- Keep the compat reading rules (taken from the quota gate so shadow verdicts stay comparable with today's) separate from policy data, and bound fallback/escalation hop chains so seat lookups cannot cycle.
- Render decisions for `werkrby admission explain` verdict-first, then why.

## Child DOX Index

No child DOX files.
