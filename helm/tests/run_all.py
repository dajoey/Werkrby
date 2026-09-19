"""Every offline suite in this repo, one command, one exit code.

Run: python3 tests/run_all.py   (from anywhere; paths resolve from this file)

Suites are discovered, not listed, so a gate that lands with a new suite cannot be
forgotten here and silently stop running. Each suite is a separate process: a suite that
crashes outright (import error, SystemExit, segfault) is reported as a failure with its
output, instead of taking the harness down with it and leaving the run looking unfinished.

The adversarial probe (probe_400s.py) runs last and counts the same way: it is the only
evidence that the gates are wired rather than merely written, so it is part of "green",
not an extra step someone might skip.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
COUNT_RE = re.compile(r"^(\d+) passed, (\d+) failed", re.M)


def suites():
    names = sorted(f for f in os.listdir(HERE)
                   if f.startswith("test_") and f.endswith(".py"))
    if os.path.exists(os.path.join(HERE, "probe_400s.py")):
        names.append("probe_400s.py")  # last: the adversarial pass over the same gates
    return names


def run_one(name):
    t0 = time.time()
    proc = subprocess.run([sys.executable, os.path.join(HERE, name)],
                          cwd=ROOT, capture_output=True, text=True)
    out = (proc.stdout or "") + (proc.stderr or "")
    last = None
    for last in COUNT_RE.finditer(out):
        pass  # the LAST count line is the suite's own total
    passed = int(last.group(1)) if last else 0
    failed = int(last.group(2)) if last else 0
    if proc.returncode != 0 and failed == 0:
        # A suite that died without reporting counts must not read as 0 failures.
        failed = 1
    return dict(name=name, passed=passed, failed=failed, rc=proc.returncode,
                secs=time.time() - t0, output=out)


def main():
    results = [run_one(n) for n in suites()]
    total_p = sum(r["passed"] for r in results)
    total_f = sum(r["failed"] for r in results)
    print("=" * 72)
    for r in results:
        status = "ok  " if (r["failed"] == 0 and r["rc"] == 0) else "FAIL"
        print("%s %-24s %4d passed %4d failed  rc=%d  %.2fs"
              % (status, r["name"], r["passed"], r["failed"], r["rc"], r["secs"]))
    for r in results:
        if r["failed"] or r["rc"]:
            print("\n--- output of " + r["name"] + " " + "-" * 40)
            print(r["output"].rstrip())
    print("=" * 72)
    print("%d suites, %d passed, %d failed" % (len(results), total_p, total_f))
    return 1 if total_f or any(r["rc"] for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
