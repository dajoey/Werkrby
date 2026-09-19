"""Run-dir retention: 30 days (contract P3 §7; the work version hit 1.3 GB
in 10 days). Pure function + a sweep() the tick calls every run. Tests use
faked mtimes (os.utime) — no waiting.
"""
from __future__ import annotations

import os
import time

from .common import env_int, log

RETENTION_DAYS = env_int("HELM_RUN_RETENTION_DAYS", 30)
RETENTION_S = RETENTION_DAYS * 86400


def sweep(root: str, now: float | None = None, retention_s: float | None = None,
          dry: bool = False) -> dict:
    """Delete run dirs under root older than retention. NEVER touches the
    root itself or anything outside it. Returns {removed, kept, bytes_freed,
    errors}. A run dir is any direct child directory of root; files directly
    in root are swept by the same age rule (they are artifacts too)."""
    now = now if now is not None else time.time()
    retention_s = retention_s if retention_s is not None else RETENTION_S
    root = os.path.abspath(root)
    removed, kept, freed, errors = [], [], 0, []
    try:
        names = os.listdir(root)
    except OSError as e:
        return {"removed": [], "kept": [], "bytes_freed": 0,
                "errors": ["cannot list %s: %s" % (root, e)]}
    for name in names:
        path = os.path.join(root, name)
        try:
            age = now - os.path.getmtime(path)
        except OSError as e:
            errors.append("%s: %s" % (name, e))
            continue
        if age <= retention_s:
            kept.append(name)
            continue
        if dry:
            removed.append(name)   # dry reports what it WOULD remove
            continue
        try:
            if os.path.islink(path):
                freed += 0
                os.unlink(path)
            elif os.path.isdir(path):
                for dirpath, _dirnames, filenames in os.walk(path):
                    for f in filenames:
                        try:
                            freed += os.path.getsize(os.path.join(dirpath, f))
                        except OSError:
                            pass
                import shutil
                shutil.rmtree(path)
            else:
                freed += os.path.getsize(path)
                os.unlink(path)
            removed.append(name)
        except OSError as e:
            errors.append("%s: %s" % (name, e))
    return {"removed": removed, "kept": kept, "bytes_freed": freed,
            "errors": errors}


def sweep_and_log(root: str, dry: bool = False) -> dict:
    res = sweep(root, dry=dry)
    if res["removed"]:
        log("retention: removed %d dir(s), freed %.1f MB%s"
            % (len(res["removed"]), res["bytes_freed"] / 1e6,
               " (dry)" if dry else ""))
    for e in res["errors"]:
        log("retention ERROR: %s" % e)
    return res
