# External Branch Tracking DOX

## Purpose

- Own fork-drift alarm pin state and tracking metadata for external upstream or peer branches.

## Local Contracts

- Track external branch pins (such as `rcanavides-router.sha` watched by `.github/workflows/watch-rcanavides-router.yml`).
- Files in this directory are read-only for routine development; pins advance only through the alarm workflow or explicit review of upstream movement.
- Do not port or merge tracked branches into Werkrby until architectural direction is clear and approved.

## Child DOX Index

No child DOX files.
