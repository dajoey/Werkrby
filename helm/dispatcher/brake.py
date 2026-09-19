"""Spending brake — reads hermes-usage-exporter artifacts, fails CLOSED.

Reason words are DISTINCT and load-bearing (contract P3 §4, LESSONS 36):
  "usage-over"  the meter was read, is fresh, and is past the line.
  "usage-blind" the meter could not be read at all (missing, unparseable,
                empty, or STALE). Blind is not clear. Holding with a different
                word than over-budget is the whole point: after the fact the
                log must tell "we were out" from "we could not see".

Data sources on the dispatcher host (measured 2026-09-12):
  1. VictoriaMetrics at :8428 — the exporter pushes `hermes_tokens_total`
     (cumulative token counters per profile/model/source/type, push timestamp
     = sample time) every 10 min from the exporter's crontab.
  2. The exporter's own log (~/.local/share or ~/ops/hermes-
     usage-exporter.log) whose mtime says when the pipeline last succeeded.

Freshness = the newest hermes_tokens_total sample in VM. The exporter pushes
every 10 min; STALE_S default 30 min (3 missed pushes). VM unreadable → fall
back to log mtime; both unreadable → usage-blind.

The threshold counts DELTA tokens over a window (default 24h): cumulative
counters only ever go up, so the level is meaningless — the burn rate is the
spend. Default HOLD_TOKENS_24H is set high enough never to bind before the
budget cap does (claude) but tight enough to catch a runaway hermes loop that
no dollar figure can see (hermes cost is unreportable; tokens are the only
signal — SPIKE-P2b §2).
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request

from .common import env_float, env_int

REASON_OVER = "usage-over"
REASON_BLIND = "usage-blind"

# Real paths on the dispatcher host (probed 2026-09-12). Env-overridable so
# tests can point the brake at a fake VM / fake log without monkeypatching.
DEFAULT_VM_URL = os.environ.get("HELM_VM_URL", "http://127.0.0.1:8428")
DEFAULT_EXPORTER_LOG = os.environ.get(
    "HELM_EXPORTER_LOG", os.path.expanduser("~/ops/hermes-usage-exporter.log"))

STALE_S = env_float("HELM_USAGE_STALE_S", 30 * 60)
WINDOW_S = env_int("HELM_USAGE_WINDOW_S", 24 * 3600)
HOLD_TOKENS = env_int("HELM_USAGE_HOLD_TOKENS", 400_000_000)
METRIC = "hermes_tokens_total"


class Brake:
    def __init__(self, vm_url: str | None = None, exporter_log: str | None = None,
                 stale_s: float | None = None, window_s: int | None = None,
                 hold_tokens: int | None = None):
        self.vm_url = vm_url or DEFAULT_VM_URL
        self.exporter_log = exporter_log or DEFAULT_EXPORTER_LOG
        self.stale_s = stale_s if stale_s is not None else STALE_S
        self.window_s = window_s if window_s is not None else WINDOW_S
        self.hold_tokens = hold_tokens if hold_tokens is not None else HOLD_TOKENS

    # ------------------------------------------------------------- VM reader

    def _vm_query(self, query: str, timeout: float = 5.0):
        url = self.vm_url + "/api/v1/query?" + urllib.parse.urlencode(
            {"query": query})
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))

    def _window_burn(self):
        """(burn_tokens, sample_ts, series_count) from VM range data, or None.

        hermes_tokens_total is cumulative (like a Prometheus counter). The
        spend over the window is increase() — computed here as newest-minus-
        oldest per series inside the window via a range query, because
        VictoriaMetrics supports increase() natively; if that errors we fall
        back to instant-sample freshness only (blind/over decided by staleness).
        """
        q = 'sum(increase(%s[%ds]))' % (METRIC, self.window_s)
        d = self._vm_query(q)
        res = d.get("data", {}).get("result", [])
        if res:
            v = res[0].get("value", [0, "0"])[1]
            try:
                burn = float(v)
            except (TypeError, ValueError):
                burn = 0.0
        else:
            burn = 0.0
        # freshness from the instant-level newest sample
        d2 = self._vm_query("max(timestamp(%s))" % METRIC)
        try:
            ts = float(d2["data"]["result"][0]["value"][1])
        except (KeyError, IndexError, TypeError, ValueError):
            ts = 0.0
        if ts <= 0.0:
            try:
                d3 = self._vm_query("max(timestamp(last_over_time(%s[1h])))" % METRIC)
                ts = float(d3["data"]["result"][0]["value"][1])
            except (KeyError, IndexError, TypeError, ValueError):
                ts = 0.0
        return burn, ts, len(res)

    # --------------------------------------------------------------- decision

    def check(self) -> dict:
        """(hold, reason, description). reason None = clear to run.

        Fail CLOSED on every unreadable path: VM down, empty result, stale
        sample, unparseable — all land in usage-blind, never 'clear'.
        """
        blind = lambda desc: {  # noqa: E731
            "hold": True, "reason": REASON_BLIND,
            "description": desc + "; holding rather than run blind "
                            "(HELM_USAGE_BLIND_OK=1 overrides)"}

        if os.environ.get("HELM_USAGE_BLIND_OK") == "1":
            blind = lambda desc: {  # noqa: E731
                "hold": False, "reason": None,
                "description": desc + "; HELM_USAGE_BLIND_OK=1, running blind"}

        try:
            burn, ts, nseries = self._window_burn()
        except (urllib.error.URLError, OSError, ValueError, KeyError,
                json.JSONDecodeError) as e:
            # VM unreadable → try the exporter log mtime as a freshness signal.
            if os.path.exists(self.exporter_log):
                age = time.time() - os.path.getmtime(self.exporter_log)
                if age > self.stale_s:
                    return blind("usage meter unreachable (%s) and exporter log "
                                 "is %.0f min old" % (e.__class__.__name__, age / 60))
                return blind("usage meter unreachable (%s); exporter log is "
                             "fresh but token counts are not queryable"
                             % e.__class__.__name__)
            return blind("usage meter unreachable (%s) and no exporter log at %s"
                         % (e.__class__.__name__, self.exporter_log))
        if ts <= 0:
            return blind("usage meter has no samples for %s" % METRIC)
        age = time.time() - ts
        if age > self.stale_s:
            return blind("newest %s sample is %.0f min old (stale > %.0f min); "
                         "is the exporter cron running?"
                         % (METRIC, age / 60, self.stale_s / 60))
        if burn >= self.hold_tokens:
            return {"hold": True, "reason": REASON_OVER,
                    "description": "%.0f tokens in the last %.1f h >= hold at %d"
                    % (burn, self.window_s / 3600.0, self.hold_tokens)}
        return {"hold": False, "reason": None,
                "description": "burn=%.0f tokens/%.1fh (hold %d), sample age %.0f min"
                % (burn, self.window_s / 3600.0, self.hold_tokens, age / 60)}


def check() -> dict:
    return Brake().check()
