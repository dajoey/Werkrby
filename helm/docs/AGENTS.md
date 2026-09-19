# Helm Docs DOX

## Purpose

- Own the governing prose of the `helm/` subsystem: `PLAN.md` (product definition, state lattice, phase table, standing rules, dated decision log), `LESSONS.md` (42 numbered requirements merged from two post-mortems), and `SPIKE-P2b.md` (the measured executor-runtime record).
- Hold the authority the code cites: store gates, dispatcher behaviour, and commit messages quote a `LESSONS.md` number or a `contracts/P<n>.md` section instead of restating the reason.
- Index the per-phase contracts that live under `contracts/`.

## Local Contracts

- `LESSONS.md` numbering is an interface. Contracts, code comments, and commit messages cite lessons by number, so the list is append-only: never renumber, never delete, never reorder. Each numbered line is a requirement, not advice, and carries its source in parentheses.
- `PLAN.md`'s state lattice is the canonical transition table (`needs-you`, `working`, `waiting-external`, `snoozed`, `done`, plus "a PUT may never resurrect done"). Its transition lines are probed one at a time at the top of the adversarial probe suite, in the order it states them; editing a line here means editing that probe.
- `PLAN.md`'s standing rules (1-10, with a 5b) bind every worker in this subtree — in particular rule 2 (the offline suite is the only evidence; a gate is not a gate until a probe that should 400 returns 400), rule 3 (facts are deterministic code, judgment runs off the critical path), rule 7 (ambiguity is filed as a question, never improvised), and rule 9 (store-gate edits quote their contract section in the commit message).
- The decision log at the foot of `PLAN.md` is append-only and dated. Superseded decisions stay put with the newer entry below them; the log is how a later phase proves it was allowed to deviate.
- `SPIKE-P2b.md` is a dated measurement record, not a design document. Its verdict is PARTIAL PASS, its table was measured once on one pair of CLI versions, and its residual-risk list is the work P3 was written to close. Do not restate its numbers from memory or edit them to match current tooling — a re-measure is a new record with its own date.
- The spike's load-bearing distinction survives into the code: a cost of `null` with `cost_basis` "unknown" is not `0.0`, and an unpriced run must never be summed into a budget as if it were free. Likewise a cap a runtime does not offer must never look enforced; the result's `*_enforced_by` fields name who actually holds each cap.
- Phases P5 and P5b were deployment-side (fleet documentation cutover, and repurposing the retired board) and are deliberately absent from this repository. `PLAN.md` still lists them, so its phase table and the `contracts/` directory do not match by design — do not add placeholder files to close the gap.
- Inputs these documents reference but that are not in this repository — the live gateway snapshot, the prior private kernel and its test suite, the subscription keep-list — stay out. Quote the finding; never import the source tree.
- These documents were written in-house and name the owner personally throughout. This repository is public: new or edited text says "the owner", and deployment coordinates appear only as environment-variable names (`HELM_URL`, `HELM_APP_TOKEN`, `HELM_DISPATCH_ENV`, `HELM_OPS_DIR`, `HELM_CORE_SERVICES`, `KUMA_URL`, `HELM_PROVIDER_ENV_VARS`), never as values.

## Child DOX Index

Direct child DOX files:

| Child | Scope |
| --- | --- |
| [contracts/AGENTS.md](contracts/AGENTS.md) | Per-phase specifications, each frozen before its code. |
