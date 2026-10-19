# Documentation Assets DOX

## Purpose

- Own the fork's generated Werkrby artwork: the pixel-art worker-bee icon set (PNG sizes and multi-resolution favicon), the social preview card, and the generator source that draws them.

## Local Contracts

- `source/make_icon.py` is the single source of truth: one run renders every image in this directory, including the `werkrby-social.png` card referenced from the root README. Regenerate with the script; never hand-edit a rendered PNG or ICO.
- Ship a regenerated image in the same change as the generator edit that explains it, and review image diffs as reviewed expectations, not generated noise.
- This directory holds presentation artwork only; documentation content stays in the parent `docs/` files.

## Child DOX Index

Direct child DOX files:

| Child | Scope |
| --- | --- |
| [source/AGENTS.md](source/AGENTS.md) | Generator source that renders the whole icon set and social preview. |
