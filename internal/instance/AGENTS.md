# Workspace Instance Election DOX

## Purpose

- Own single-primary election and authenticated rendezvous for all Werkrby processes sharing one workspace.

## Local Contracts

- Publish complete lease records atomically for lock-free discovery, renew at five seconds, and treat records stale after 30 seconds under fenced takeover rules.
- Put a validated non-secret SHA-256 connectivity identifier in every new lease. Derive it from a private random token in the OS per-user configuration home so ordinary processes in one supported environment agree without publishing hostnames, paths, MAC addresses, usernames, machine IDs, boot IDs, or namespace identifiers.
- Treat a missing or invalid environment identifier as an incompatible/legacy owner, never as permission to replace a fresh lease. Preserve its heartbeat fencing until ordinary stale or targeted-handoff rules permit acquisition.
- A process whose own executable was replaced on disk (its `/proc/<pid>/exe` reading `... (deleted)`) refuses to acquire or renew with `ErrExecutableReplaced` instead of churning its own permanently stale lease. Callers must exit non-zero so the service manager restarts the process from the new binary; a deleted executable on any *other* lease still reads as stale so a replacement can take over.
- Serialize compare-and-replace takeover and bind explicit handoff to a live target and expiry so two processes cannot become authoritative from one request.
- Expose a short exact-term ownership guard under the election mutation lock so primary-only durable admissions cannot race a stale takeover; a former term's action must not run after replacement.
- Serialize destructive ownerless cleanup with primary publication through the same election lock. Run it only when the primary lease is absent and no durable clean-release fence remains within one live-client lease duration; stale or malformed leases and release fences fail closed because a former owner or already-open successor may still retain process-local live-client state.
- Keep lock mechanics platform-specific and never expose authentication tokens through status or logs.

## Child DOX Index

No child DOX files.
