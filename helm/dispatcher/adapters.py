"""Executor adapters — the SPIKE-P2b §7 interface, ported into the dispatcher.

The dispatcher never learns which CLI ran: runtime, model, duration and cost
are DATA in the result payload, not branches in the caller. Each runtime gets
a small `from_<runtime>()` reader that maps ITS artifacts onto one fixed Result
shape; whatever a runtime cannot report comes back null — a measurement, not a
hole to guess at.

The real `claude`/`hermes` invocations appear ONLY as data (command templates)
here. Tests stub the spawn; nothing in this module executes a model.

Measured on this install 2026-09-12 (docs/SPIKE-P2b.md):
  claude 2.1.246  reports turns + dollars + denials; --max-budget-usd exists;
                 --max-turns is GONE (the work dispatcher assumes it; dead).
  hermes 0.21.2  reports NEITHER turns-as-turns (api_calls only) NOR dollars
                 (estimated_cost_usd: 0.0 with cost_status "unknown" on a
                 subscription credential); no cap flags at all.
"""
from __future__ import annotations

import json
import os
import shlex
from dataclasses import dataclass, field

from . import caps as caps_mod
from .common import read_text

# ---------------------------------------------------------------- Result shape


@dataclass
class Result:
    """One shape for every runtime (SPIKE-P2b §7)."""

    executor_id: str = ""
    run_id: str = ""
    runtime: str = ""
    model: str | None = None
    models_billed: dict | None = None
    ok: bool = False
    rc: int = -1
    timed_out: bool = False
    capped: bool = False
    duration_s: float = 0.0
    duration_api_ms: int | None = None
    num_turns: int | None = None
    cost_usd: float | None = None
    cost_basis: str = "unknown"
    usage: dict = field(default_factory=dict)
    permission_denials: list | None = None
    answer_file: object = None
    final_text: str = ""
    stderr_tail: str = ""
    caps: dict = field(default_factory=dict)
    run_dir: str = ""

    def to_payload(self) -> dict:
        d = dict(self.__dict__)
        d["usage"] = dict(self.usage)
        d["caps"] = dict(self.caps)
        return d

    def compute_capped(self, cap_set: "caps_mod.CapSet") -> bool:
        """Fires when THIS run crossed a cap the runner holds.

        Called by the harness after the spawn returns. Wall clock and budget
        are ours; a runtime-reported subtype is trusted as an extra signal, not
        the only one (spike: claude's escape run exited 0 with subtype success
        — rc/subtype alone never decide anything here).
        """
        capped = False
        if self.timed_out:
            capped = True
        if cap_set.max_budget_usd is not None and self.cost_usd is not None:
            if self.cost_usd >= cap_set.max_budget_usd:
                capped = True
        self.capped = capped or self.capped
        return self.capped


# ------------------------------------------------------------- command templates
#
# argv lists (data). The jail wraps these; caps wrap the jail; the tick wraps
# the caps. NEVER executed inside this module.

CLAUDE_BARE_ARGS = [
    "claude", "-p", "--bare",
    "--output-format", "json",
    "--permission-mode", "dontAsk",
    "--no-session-persistence",
    "--strict-mcp-config",
]
# Measured write-scope facts (SPIKE-P2b §4): with Write allowed and dontAsk,
# claude auto-approved an absolute-path write outside cwd with
# permission_denials: []. cwd is not a wall. The OS jail is the scope; these
# tool switches are a second layer, rendered into the prompt as-is.
CLAUDE_ALLOWED_TOOLS = "Read,Glob,Grep,Write,Edit,TodoWrite"
CLAUDE_DENIED_TOOLS = ("Bash,BashOutput,KillShell,Agent,Task,WebFetch,WebSearch,"
                       "NotebookEdit,SlashCommand")

HERMES_TOOLSETS = "file,todo"  # no terminal, no web: a shell tool makes any
                               # write-scope claim theatre


@dataclass
class CommandSpec:
    """What one runtime needs to be spawned (built by the adapter, run by jail)."""
    argv: list
    stdin_text: str = ""            # claude takes the prompt on stdin
    env: dict = field(default_factory=dict)   # extra env (never secrets in argv)
    cwd_is_run_dir: bool = True
    runtime_out: str = "stdout.json"   # artifact filename the runtime writes


def spec_claude(run_dir: str, prompt: str, cap_set, model: str = "sonnet",
                effort: str = "low") -> CommandSpec:
    args = list(CLAUDE_BARE_ARGS)
    args[0] = os.environ.get("HELM_CLAUDE_BIN", args[0])   # stub-able in tests
    args += ["--model", model, "--effort", effort]
    if cap_set.max_budget_usd is not None:
        args += ["--max-budget-usd", "%.2f" % cap_set.max_budget_usd]
    args += ["--allowedTools", CLAUDE_ALLOWED_TOOLS]
    args += ["--disallowedTools", CLAUDE_DENIED_TOOLS]
    return CommandSpec(
        argv=args,
        stdin_text=prompt,
        env={},
        cwd_is_run_dir=True,
    )


def spec_hermes(run_dir: str, prompt: str, cap_set, model: str | None = None,
                effort: str = "low") -> CommandSpec:
    # -z takes the prompt as an ARGUMENT. It is bounded by caps.py token cap
    # post-hoc and timeout(1) pre-spawn; hermes has no budget flag (measured).
    argv = [
        os.environ.get("HELM_HERMES_BIN", "hermes"), "-z", prompt,
        "--in", run_dir,
        "--usage-file", os.path.join(run_dir, "usage.json"),
        "--reasoning", effort,
        "-t", HERMES_TOOLSETS,     # no terminal, no web
        "--ignore-rules",          # the cold-run knob (nearest --bare)
    ]
    # Provider routing (live-gate 2026-09-13): the ONE-SHOT startup resolver
    # (resolve_startup_model_route) consumes "provider/model" SLASH form or an
    # explicit --provider flag. A bare "-m glm-5.3" attaches the CONFIG default
    # provider (anthropic here) to the model — and colon/dash forms are
    # silently ignored. Explicit --provider + bare model is the verified shape
    # (rc=0, zai call, 2026-09-13 11:20). HELM_HERMES_PROVIDER defaults off so
    # unset = config default (backwards-compatible).
    provider = os.environ.get("HELM_HERMES_PROVIDER")
    model = model or os.environ.get("HELM_HERMES_MODEL")
    if model and not provider:
        if ":" in model:
            provider, model = model.split(":", 1)
        elif "/" in model:
            provider, model = model.split("/", 1)
    if provider:
        argv += ["--provider", provider]
    if model:
        argv += ["-m", model]
    # HERMES_HOME into the run dir (live-gate 2026-09-13): a jailed executor
    # under ProtectHome=read-only dies rc=1 the moment it tries to append its
    # own agent.log (~/.hermes/logs). HERMES_HOME redirects home-anchored
    # state (logs) into the writable run dir. Config is read from the REAL
    # home first: --ignore-rules + explicit --provider/-m/GLM_API_KEY make
    # the run independent of config.yaml, and HERMES_HOME must not hide it.
    hermes_home = os.environ.get("HELM_HERMES_HOME_OVERRIDE") or os.path.join(run_dir, ".hermes-home")
    return CommandSpec(
        argv=argv,
        stdin_text="",
        env={"HERMES_WRITE_SAFE_ROOT": run_dir,
             "HERMES_HOME": hermes_home},
        cwd_is_run_dir=True,
    )


# ------------------------------------------------------------------- readers


def _load_env_json(path: str) -> tuple[dict, str]:
    raw = read_text(path)
    try:
        obj = json.loads(raw)
    except ValueError:
        return {}, raw
    if isinstance(obj, list):
        obj = next((x for x in reversed(obj) if isinstance(x, dict)), {})
    return (obj if isinstance(obj, dict) else {}), raw


def from_claude(run_dir: str, rc: int, timed_out: bool, duration_s: float,
                cap_set, executor_id: str, run_id: str) -> Result:
    """Read claude's JSON envelope off disk into the one Result shape."""
    env_obj, raw = _load_env_json(os.path.join(run_dir, "stdout.json"))
    u = env_obj.get("usage") or {}
    mu = env_obj.get("modelUsage") or {}
    main = max(mu, key=lambda k: (mu[k] or {}).get("costUSD", 0)) if mu else None
    sub = str(env_obj.get("subtype") or "")
    r = Result(
        executor_id=executor_id, run_id=run_id, runtime="claude",
        model=main or cap_set.model or "sonnet",
        models_billed={k: (v or {}).get("costUSD") for k, v in mu.items()} or None,
        ok=(rc == 0 and not timed_out and not env_obj.get("is_error")),
        rc=rc,
        timed_out=timed_out,
        # A run stopped on OUR ceiling keeps its work and posts a partial
        # (LESSONS 35); the subtype is a runtime-reported extra signal.
        capped=("max_turns" in sub.lower() or "budget" in sub.lower()),
        duration_s=duration_s,
        duration_api_ms=env_obj.get("duration_api_ms"),
        num_turns=env_obj.get("num_turns"),
        cost_usd=env_obj.get("total_cost_usd"),
        cost_basis="reported",   # claude bills the number it prints
        usage={"input": u.get("input_tokens"), "output": u.get("output_tokens"),
               "cache_read": u.get("cache_read_input_tokens"),
               "cache_create": u.get("cache_creation_input_tokens")},
        permission_denials=env_obj.get("permission_denials"),
        final_text=(env_obj.get("result") if isinstance(env_obj.get("result"), str)
                    else raw[:2000]) or "",
        stderr_tail=read_text(os.path.join(run_dir, "stderr.txt"), 2000),
        caps=cap_set.enforced_by_map(),
        run_dir=run_dir,
    )
    r.compute_capped(cap_set)
    return r


def from_hermes(run_dir: str, rc: int, timed_out: bool, duration_s: float,
                cap_set, executor_id: str, run_id: str) -> Result:
    """-z prints final text only; --usage-file is the whole accounting surface."""
    text = read_text(os.path.join(run_dir, "stdout.txt"))
    try:
        u = json.loads(read_text(os.path.join(run_dir, "usage.json")) or "{}")
    except ValueError:
        u = {}
    if not isinstance(u, dict):
        u = {}
    tok = u.get("tokens") or u
    # estimated_cost_usd: 0.0 with cost_status "unknown" on a subscription
    # credential is NOT "$0 spent" — it is "unpriced" (LESSONS 30). A zero
    # that means unpriced must never be summed into a budget as free.
    cost = u.get("estimated_cost_usd", u.get("cost_usd", u.get("estimated_cost")))
    status = u.get("cost_status")
    basis = "unknown/none"
    if status and status != "ok":
        cost = None            # unpriced, not free
    elif cost is not None:
        basis = "reported"
    r = Result(
        executor_id=executor_id, run_id=run_id, runtime="hermes",
        model=u.get("model") or cap_set.model,
        models_billed=None,
        ok=(rc == 0 and not timed_out and u.get("failed") is not True),
        rc=rc,
        timed_out=timed_out,
        capped=False,   # no turn/budget cap exists to hit (measured)
        duration_s=duration_s,
        duration_api_ms=None,
        num_turns=u.get("api_calls"),  # closest available; NOT the same number
        cost_usd=cost,
        cost_basis=basis,
        usage={"input": tok.get("input_tokens"), "output": tok.get("output_tokens"),
               "cache_read": tok.get("cache_read_tokens"),
               "cache_create": tok.get("cache_write_tokens")},
        permission_denials=None,
        final_text=text[:2000],
        stderr_tail=read_text(os.path.join(run_dir, "stderr.txt"), 2000),
        # hermes stderr is config-warning noise on every run (spike §6.8):
        # non-empty stderr is never read as failure.
        caps=cap_set.enforced_by_map(),
        run_dir=run_dir,
    )
    r.compute_capped(cap_set)
    # hermes token brake (caps.py): cost is unreportable, so tokens are the
    # only observable spend signal.
    if cap_set.max_tokens_hermes and r.usage.get("output") is not None:
        tot = sum(int(r.usage.get(k) or 0)
                  for k in ("input", "output", "cache_read", "cache_create"))
        if tot >= cap_set.max_tokens_hermes:
            r.capped = True
    return r


READERS = {"claude": from_claude, "hermes": from_hermes}


def from_runtime(runtime: str, run_dir: str, rc: int, timed_out: bool,
                 duration_s: float, cap_set, executor_id: str, run_id: str) -> Result:
    try:
        fn = READERS[runtime]
    except KeyError:
        raise ValueError("unknown runtime %r (want one of %s); Helm is "
                         "executor-agnostic, so a new runtime is a new reader, "
                         "not an if-branch" % (runtime, sorted(READERS))) from None
    return fn(run_dir, rc, timed_out, duration_s, cap_set, executor_id, run_id)


# ------------------------------------------------------ answer-file extraction

def render_switches(runtime: str) -> str:
    """The tool switches AS THEY ARE on this run (contract P3 §5).

    Renders the exact flags the spawn carries, per runtime, from the same
    data the adapter builds the argv from — never a description that could
    drift from the actual switch set, and never naming a DENIED tool as if
    it existed ('you have a shell' alongside Bash denial would be theatre).
    """
    if runtime == "claude":
        return (
            "Tool switches on this run (exact):\n"
            "  --allowedTools %s\n"
            "  --disallowedTools %s\n"
            "There is no shell, no sub-agent, no web access. Write with the\n"
            "Write/Edit tools into the CURRENT DIRECTORY only." %
            (CLAUDE_ALLOWED_TOOLS, CLAUDE_DENIED_TOOLS))
    if runtime == "hermes":
        return (
            "Tool switches on this run (exact):\n"
            "  -t %s   (toolsets: file ops + todo ONLY)\n"
            "  HERMES_WRITE_SAFE_ROOT=<the current directory>\n"
            "There is no terminal, no web. The write jail rejects any path\n"
            "outside the current directory." % HERMES_TOOLSETS)
    return "Tool switches: unknown runtime %r" % runtime

def answer_of(result: Result):
    """The artifact the executor was told to leave in run_dir/result.json.

    Read off disk, never from the model's prose (spike §2): "it wrote inside
    its run dir" stays a measurement instead of a claim.
    """
    path = os.path.join(result.run_dir, "result.json")
    raw = read_text(path, 4000)
    if not raw.strip():
        return None
    try:
        return json.loads(raw)
    except ValueError:
        return {"_unparsed": raw[:500]}
