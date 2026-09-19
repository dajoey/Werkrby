"""Caps — who holds each cap (SPIKE-P2b §3, contract P3 §3).

A cap the runtime does not offer must never LOOK enforced. Each cap carries
`*_enforced_by` naming its holder: "timeout(1)" / "claude --max-budget-usd" /
"none (no flag on this runtime)" / "runner (token brake)". Measured gaps on
this install, not papered over by prompting (LESSONS 35: caps belong to the
runner):

  claude 2.1.246: --max-turns is GONE. Turns observed post-hoc only.
  hermes 0.21.2:  NO turn cap, NO budget flag, dollars unreportable
                  (cost_status "unknown" on subscription creds). The only
                  spend signal is TOKENS, so the hermes brake counts tokens.

`cost_basis` never summed as 0: from_hermes() maps unpriced → cost_usd None
with cost_basis "unknown/none" (LESSONS 30).
"""
from __future__ import annotations

import os
from dataclasses import dataclass

from .common import env_float, env_int


@dataclass
class CapSet:
    timeout_s: int = 300
    max_budget_usd: float | None = 1.50
    max_turns: int | None = None
    max_tokens_hermes: int | None = None
    model: str | None = None
    effort: str = "low"

    # ---- what each runtime can actually enforce (measured, SPIKE-P2b §3)
    def enforced_by_map(self, runtime: str | None = None) -> dict:
        budget_holder = {
            "claude": "claude --max-budget-usd",
            "hermes": "none (no flag on this runtime)",
        }.get(runtime or "", "none")
        if self.max_budget_usd is None:
            budget_holder = "none (not configured)"
        turns_holder = "none (no flag on claude 2.1.246 or hermes 0.21.2)"
        if self.max_turns is not None:
            turns_holder = "runner (observed post-hoc; alert only)"
        return {
            "timeout_s": self.timeout_s,
            "timeout_enforced_by": "timeout(1)",
            "max_budget_usd": self.max_budget_usd,
            "max_budget_enforced_by": budget_holder,
            "max_turns": self.max_turns,
            "max_turns_enforced_by": turns_holder,
            "max_tokens_hermes": self.max_tokens_hermes,
            "max_tokens_hermes_enforced_by": (
                "runner (post-hoc token brake)" if self.max_tokens_hermes else "none"),
            "model": self.model,
            "effort": self.effort,
        }

    def for_runtime(self, runtime: str) -> "CapSet":
        return self


def from_caps_payload(payload: dict | None) -> CapSet:
    """Claims carry caps{timeout_s, max_budget_usd, max_turns, model}."""
    p = payload or {}
    timeout_s = p.get("timeout_s")
    budget = p.get("max_budget_usd")
    turns = p.get("max_turns")
    return CapSet(
        timeout_s=int(timeout_s) if timeout_s else 300,
        max_budget_usd=(float(budget) if budget not in (None, "",) else None),
        max_turns=(int(turns) if turns not in (None, "") else None),
        max_tokens_hermes=int(p["max_tokens_hermes"]) if p.get("max_tokens_hermes") else None,
        model=p.get("model"),
        effort=p.get("effort") or "low",
    )


def default_caps(runtime: str) -> CapSet:
    """Defaults per runtime, env-overridable (HELM_* knobs)."""
    return CapSet(
        timeout_s=env_int("HELM_EXEC_TIMEOUT_S", 300),
        max_budget_usd=(env_float("HELM_MAX_BUDGET_USD", 1.50)
                        if runtime == "claude" else None),
        max_turns=None,   # no runtime flag exists; observed only
        max_tokens_hermes=(env_int("HELM_HERMES_TOKEN_CAP", 2_000_000)
                           if runtime == "hermes" else None),
        model=os.environ.get("HELM_MODEL") or None,
    )
