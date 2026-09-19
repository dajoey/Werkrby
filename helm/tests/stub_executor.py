#!/usr/bin/env python3
"""The fake executor. Succeeds / fails / caps-out / times-out / escapes ON DEMAND.

HELM_STUB_MODE env selects behaviour (the fake-executor pattern from the P3
task spec: the whole suite stays offline and unbilled):

  ok            writes a valid result.json + a claude-shaped stdout.json, rc 0
  ok-notext     rc 0, no result.json, no final text (the invalid case)
  fail          rc 1, stderr "boom", nothing else
  auth-fail     rc 1, stderr with an auth marker (auth failure ≠ retryable)
  cap-budget    rc 0, stdout.json with total_cost_usd >= cap (budget fired)
  cap-turns     rc 0, subtype "max_turns" (runtime-reported cap fired)
  timeout       sleeps past timeout(1) -> rc 124 from the wrapper
  escape        writes OUTSIDE the run dir ($HOME sentinel + /tmp sentinel)
                then behaves like ok — the jail must have refused the writes
  slow-escape   like escape but ALSO writes result.json so the run reports ok;
                assertions then hinge ONLY on outside files existing or not
  hang-after    writes result.json THEN sleeps forever (kill -9 simulation:
                harness must still record + backoff, not wait forever)
  note-only     P7 §2: writes the terminal no-action verdict (state done,
                no_action true); note-only-malformed sends the flag as a string
  HELM_STUB_AUTH_STATUS=logged-out makes `auth status` answer loggedIn:false

P3b: the stub also plays the second-lane REVIEWER (contract: P3b §3 — in
tests the stub executor is the reviewer; no model is ever called):

  review-ready        writes review-result.json {verdict: ready, note >= 80}
  review-needs-work   verdict needs-work
  review-needs-joey   verdict needs-joey
  review-invalid      writes a review-result.json with an unknown verdict
  review-garbage      writes unparseable JSON
  review-empty        writes nothing at all (spawn-level failure shape)

`claude auth status` (argv contains "auth"+"status") is answered like the
real CLI: machine-readable JSON, rc 0, no side effects — the dispatcher calls
it OUTSIDE the jail before spawning, so the stub must never run its mode
there (measured: the escape mode fired unjailed from the precheck).
Everything the stub writes is deterministic. No model, no network.
"""
import json
import os
import sys
import time

# Per-runtime mode (P3b §2 testing): when spawned hermes-shaped ("-z" is in
# argv — the hermes adapter passes the prompt as an argument, claude reads
# stdin), HELM_STUB_MODE_HERMES wins if set. That is how one tick can have
# claude FAIL its last attempt while the switched hermes pass SUCCEEDS.
MODE = os.environ.get("HELM_STUB_MODE", "ok")
if "-z" in sys.argv and os.environ.get("HELM_STUB_MODE_HERMES"):
    MODE = os.environ["HELM_STUB_MODE_HERMES"]
# The bonus round (P3b §3.3) is an EXECUTOR run that happens to share a tick
# with the reviewer; its prompt is prefixed with the bonus marker. It writes
# result.json like any executor, never the reviewer artifact.
_prompt_is_bonus = False
_argv_text = " ".join(sys.argv[1:])
if "ROUND-CAP BONUS (once per thread)" in _argv_text:
    _prompt_is_bonus = True
elif sys.argv[1:] and not sys.stdin.isatty():
    # claude-shaped: prompt arrives on redirected stdin; peeking is safe
    # because the stub itself never needs stdin afterwards.
    try:
        _stdin_peek = sys.stdin.read()
    except Exception:
        _stdin_peek = ""
    if "ROUND-CAP BONUS (once per thread)" in _stdin_peek:
        _prompt_is_bonus = True
if _prompt_is_bonus and MODE.startswith("review-"):
    MODE = "ok"
RUN_DIR = os.environ.get("HELM_RUN_DIR", os.getcwd())
CAP_BUDGET = float(os.environ.get("HELM_STUB_CAP_BUDGET", "1.50"))

if len(sys.argv) > 2 and "auth" in sys.argv and "status" in sys.argv:
    # P7 §1: HELM_STUB_AUTH_STATUS=logged-out makes the precheck/switch-gate
    # refusal probes real (LESSON 40): the CLI answers like a signed-out one.
    if os.environ.get("HELM_STUB_AUTH_STATUS") == "logged-out":
        print(json.dumps({"loggedIn": False, "authMethod": "none"}))
    else:
        print(json.dumps({"loggedIn": True, "authMethod": "subscription-stub"}))
    sys.exit(0)

RESULT = {
    "outcome": "ok",
    "state": "needs-you",
    "note": ("The decision was carried out end to end and verified against the "
             "artifact on disk; nothing further is owed on this thread."),
    "ask": {"kind": "ack", "prompt": "Done - work executed and verified; tap to settle."},
}


def claude_stdout(cost=0.01, turns=3, subtype="success", final="391"):
    return json.dumps({
        "is_error": False, "duration_api_ms": 700, "num_turns": turns,
        "subtype": subtype, "total_cost_usd": cost,
        "usage": {"input_tokens": 5, "cache_read_input_tokens": 5200,
                  "cache_creation_input_tokens": 330, "output_tokens": 380},
        "modelUsage": {"claude-sonnet-5": {"costUSD": cost}},
        "permission_denials": [],
        "result": final,
    })


def write(name, text):
    with open(os.path.join(RUN_DIR, name), "w", encoding="utf-8") as fh:
        fh.write(text)


_REVIEW_VERDICTS = {"review-ready": "ready", "review-needs-work": "needs-work",
                    "review-needs-joey": "needs-joey"}
if MODE in _REVIEW_VERDICTS:
    # The reviewer's artifact: verdict + a note over the 80-char floor. The
    # claude/hermes envelope files are also written so whichever runtime the
    # review rode on, the adapter reader finds its shape.
    write("review-result.json", json.dumps({
        "verdict": _REVIEW_VERDICTS[MODE],
        "note": ("I read every round this thread produced and checked the "
                 "artifact against the decision that was made; this note is "
                 "deliberately over the eighty-character floor so the verdict "
                 "is admissible."),
    }))
    write("stdout.json", claude_stdout())
    write("stdout.txt", "review complete")
    write("usage.json", json.dumps({"api_calls": 1, "tokens": {}, "model": "stub"}))
    sys.exit(0)
if MODE == "review-invalid":
    write("review-result.json", json.dumps({"verdict": "ship-it", "note": "x" * 100}))
    sys.exit(0)
if MODE == "review-garbage":
    write("review-result.json", "{not json at all")
    sys.exit(0)
if MODE == "review-empty":
    sys.exit(0)


if MODE == "timeout":
    time.sleep(600)
    sys.exit(0)

if MODE == "hang-after":
    write("result.json", json.dumps(RESULT))
    write("stdout.json", claude_stdout())
    time.sleep(600)
    sys.exit(0)

if MODE in ("escape", "slow-escape"):
    # The spike's escape test, re-fired IN the jail (contract: if any write
    # lands outside, P3 does not ship). Sentinels outside the run dir:
    home = os.path.expanduser("~")
    sentinels = [os.path.join(home, ".helm-p3-escape-sentinel"),
                 "/tmp/helm-p3-escape-sentinel"]
    for s in sentinels:
        try:
            with open(s, "w") as fh:
                fh.write("escaped %s\n" % MODE)
            sys.stderr.write("ESCAPED-WRITE-LANDED %s\n" % s)
        except OSError as e:
            sys.stderr.write("escape-write refused: %s %s\n" % (s, e))
    if MODE == "escape":
        sys.exit(0)
    # slow-escape falls through: result.json + rc 0, so the run LOOKS fine

if MODE == "auth-fail":
    sys.stderr.write("authentication_error: OAuth session expired, please run /login\n")
    sys.exit(1)

if MODE == "fail":
    sys.stderr.write("boom: the executor failed on its own\n")
    sys.exit(1)

if MODE == "cap-budget":
    write("result.json", json.dumps(dict(RESULT, outcome="partial-capped")))
    write("stdout.json", claude_stdout(cost=CAP_BUDGET + 0.01, subtype="success_budget"))
    sys.exit(0)

if MODE == "cap-turns":
    write("result.json", json.dumps(dict(RESULT, outcome="partial-capped")))
    write("stdout.json", claude_stdout(subtype="max_turns"))
    sys.exit(0)

if MODE == "ok-notext":
    sys.exit(0)

# P7 §2: the terminal no-action verdict (the pip/scratch wedge shape: the
# decision is note-only, recording is the whole job, the thread must SETTLE).
# "note-only-malformed" sends the flag as a STRING so the dispatcher's
# strip-and-degrade guard fires for real.
if MODE in ("note-only", "note-only-malformed"):
    verdict = {
        "outcome": "note-only",
        "state": "done",
        "no_action": True if MODE == "note-only" else "true",
        "note": ("The decision was note-only: no thread action was owed, his "
                 "point is recorded verbatim in this note, and nothing was "
                 "dispatched, changed, or promised. Settling deliberately."),
    }
    write("result.json", json.dumps(verdict))
    write("stdout.json", claude_stdout())
    write("usage.json", json.dumps({"api_calls": 1, "tokens": {}, "model": "stub"}))
    sys.exit(0)

# ok
write("result.json", json.dumps(RESULT))
write("stdout.json", claude_stdout())
sys.exit(0)
