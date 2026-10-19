# Documentation Asset Source DOX

## Purpose

- Own `make_icon.py`, the one generator that draws the 32x32 pixel-art worker bee and renders every artwork file in the parent assets directory.

## Local Contracts

- One run against one output-directory argument writes every rendered size, the multi-resolution ICO, and the 1280x640 social preview; rendered files are regenerated output and are never hand-edited.
- Keep the generator runnable with Pillow alone; a palette, drawing, or wordmark change belongs here and ships together with the regenerated images it explains.

## Child DOX Index

No child DOX files.
