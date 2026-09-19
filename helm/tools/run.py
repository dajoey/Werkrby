#!/usr/bin/env python3
"""Run a command and print its tail. Exists for one reason: the spike worker's
shell refuses compound commands (pipes, `&&`, redirections), so `hermes --help |
head` and friends are unavailable. This wrapper takes argv, runs it once, and
prints the tail of each stream plus the exit code -- enough to read a --help page
or drive spike_exec.sh without a shell.

No secrets: argv is passed through untouched and nothing is written to disk.
"""
import subprocess, sys

r = subprocess.run(sys.argv[1:], capture_output=True, text=True)
sys.stdout.write(r.stdout[-8000:])
sys.stderr.write(r.stderr[-4000:])
print("[rc=%s]" % r.returncode)
