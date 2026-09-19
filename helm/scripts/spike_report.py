#!/usr/bin/env python3
"""P2b spike: turn one finished run into one JSON result and POST it exactly once.

Split out of spike_exec.sh because parsing the CLI envelope and deciding "was
this capped" is real logic, and the plan says python3 for real logic. Inputs
arrive by environment, not argv, so no run detail lands in a world-readable
process list -- the same habit that keeps secrets out of argv.

Runtime-agnostic by construction (issue #2): each runtime gets a small reader
that maps ITS output onto one fixed result shape. The dispatcher then never
learns which CLI ran -- runtime/model/duration/cost are data in the payload, not
branches in the caller. Whatever a runtime cannot report comes back null, which
is a measurement ("hermes does not count turns"), not a hole to be guessed at.
"""
import json
import os
import sys
import urllib.error
import urllib.request

RUN_DIR = os.environ["RUN_DIR"]


def read(name, limit=None):
    try:
        with open(os.path.join(RUN_DIR, name), encoding="utf-8", errors="replace") as fh:
            s = fh.read()
        return s[:limit] if limit else s
    except OSError:
        return ""


def envelope():
    """--output-format json emits one object. Be liberal about shape anyway: the
    harness must still report a run whose output it could not parse."""
    raw = read("stdout.json")
    try:
        obj = json.loads(raw)
    except ValueError:
        return {}, raw
    if isinstance(obj, list):
        obj = next((x for x in reversed(obj) if isinstance(x, dict)), {})
    return (obj if isinstance(obj, dict) else {}), raw


def from_claude():
    env, raw = envelope()
    u = env.get("usage") or {}
    sub = str(env.get("subtype") or "")
    # One `claude -p` run bills MORE THAN ONE model: a haiku helper runs
    # alongside the model you asked for. Reporting only the first key would
    # attribute a sonnet run to haiku, so report the whole breakdown and call
    # the costliest one the model of record.
    mu = env.get("modelUsage") or {}
    main = max(mu, key=lambda k: (mu[k] or {}).get("costUSD", 0)) if mu else None
    return {
        "subtype": sub or None,
        "is_error": env.get("is_error"),
        "model": main or os.environ.get("MODEL"),
        "models_billed": {k: (v or {}).get("costUSD") for k, v in mu.items()} or None,
        "num_turns": env.get("num_turns"),
        "cost_usd": env.get("total_cost_usd"),
        "cost_basis": "reported",     # claude bills the number it prints
        "duration_api_ms": env.get("duration_api_ms"),
        "usage": {"input": u.get("input_tokens"), "output": u.get("output_tokens"),
                  "cache_read": u.get("cache_read_input_tokens"),
                  "cache_create": u.get("cache_creation_input_tokens")},
        "permission_denials": env.get("permission_denials"),
        "final_text": env.get("result") if isinstance(env.get("result"), str) else raw[:2000],
        # A run stopped on OUR ceiling is not a broken run: it keeps its work and
        # posts a partial (LESSONS 35). Naming it here is what lets a dispatcher
        # tell the two apart without re-deriving it from the transcript.
        "capped": ("max_turns" in sub.lower() or "budget" in sub.lower()),
    }


def from_hermes():
    """-z prints final text only; --usage-file is the whole accounting surface.
    No turn count and no denial list exist to read, so both stay null."""
    text = read("stdout.txt")
    try:
        u = json.loads(read("usage.json") or "{}")
    except ValueError:
        u = {}
    tok = u.get("tokens") or u
    # cost_status/cost_source are hermes telling you whether the number means
    # anything. On a subscription credential it reports 0.0 with status
    # "unknown" -- which is NOT "this run was free", and must not be summed as
    # if it were (LESSONS 30: an unmeasured number is not a measurement).
    cost = u.get("estimated_cost_usd", u.get("cost_usd", u.get("estimated_cost")))
    status = u.get("cost_status")
    if status and status != "ok":
        cost = None
    return {
        "subtype": None,
        "is_error": None,
        "model": u.get("model") or os.environ.get("MODEL"),
        "models_billed": None,
        # api_calls is the closest thing to a turn count this runtime exposes.
        "num_turns": u.get("api_calls"),
        "cost_usd": cost,
        "cost_basis": "%s/%s" % (status or "?", u.get("cost_source") or "?"),
        "duration_api_ms": None,
        "usage": {"input": tok.get("input_tokens"),
                  "output": tok.get("output_tokens"),
                  "cache_read": tok.get("cache_read_tokens"),
                  "cache_create": tok.get("cache_write_tokens")},
        "permission_denials": None,
        "final_text": text[:2000],
        "capped": False,              # no turn/budget cap exists to hit
        "usage_raw": u,
    }


RUNTIME = os.environ.get("RUNTIME", "claude")
adapted = {"claude": from_claude, "hermes": from_hermes}[RUNTIME]()
rc = int(os.environ["RC"])
timed_out = os.environ["TIMED_OUT"] == "true"

# What the executor was told to leave behind. Reading the file here, rather than
# believing the model's prose, is what makes "it wrote inside its run dir" a
# measurement instead of a claim.
answer_raw = read("result.json", 4000)
try:
    answer = json.loads(answer_raw) if answer_raw.strip() else None
except ValueError:
    answer = {"_unparsed": answer_raw[:500]}

result = {
    "run_id": os.environ["RUN_ID"],
    "run_dir": RUN_DIR,
    "runtime": RUNTIME,
    "ok": rc == 0 and not timed_out,
    "rc": rc,
    "timed_out": timed_out,
    "duration_s": float(os.environ["DURATION"]),
    "caps": {
        # Enforced_by says WHO holds each cap. A cap the runtime does not offer
        # reads "none" here rather than silently looking enforced.
        "timeout_s": int(os.environ["TIMEOUT_S"]),
        "timeout_enforced_by": "timeout(1)",
        "max_turns": int(os.environ["MAX_TURNS"]),
        "max_turns_enforced_by": "none (no flag on either runtime)",
        "max_budget_usd": float(os.environ["MAX_BUDGET_USD"]),
        "max_budget_enforced_by": "claude --max-budget-usd" if RUNTIME == "claude" else "none",
    },
    "stderr_tail": read("stderr.txt", 2000),
    "answer_file": answer,
}
result.update(adapted)

with open(os.path.join(RUN_DIR, "result_posted.json"), "w", encoding="utf-8") as fh:
    json.dump(result, fh, indent=2)

req = urllib.request.Request(
    os.environ["RECEIVER"], data=json.dumps(result).encode(),
    headers={"Content-Type": "application/json"}, method="POST")
try:
    with urllib.request.urlopen(req, timeout=15) as r:
        print("[spike_exec] POST %s -> %s" % (os.environ["RECEIVER"], r.status))
except (urllib.error.URLError, OSError) as e:
    print("[spike_exec] POST FAILED: %r" % (e,), file=sys.stderr)
    sys.exit(5)

print("[spike_exec] run=%s runtime=%s rc=%s dur=%ss turns=%s cost=%s capped=%s denials=%s" % (
    result["run_id"], RUNTIME, rc, result["duration_s"], result["num_turns"],
    result["cost_usd"], result["capped"], result["permission_denials"]))
