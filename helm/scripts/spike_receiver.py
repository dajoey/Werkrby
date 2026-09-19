#!/usr/bin/env python3
"""P2b spike: the smallest possible stand-in for the Helm result endpoint.

Binds 127.0.0.1 only. Accepts POST of one JSON body per executor run, appends it
to a JSONL log, and answers 200. It exists to answer exactly one question --
"did the result actually land?" -- so it deliberately does no validation, no
auth and no state. The real endpoint is /api/triage/threads/<id>/result on the
Helm service; nothing here is meant to survive the spike.

stdlib only (PLAN rule: python3 only). No secrets, no outbound connections.
"""
import argparse
import json
import os
import sys
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

LOG = None


class Handler(BaseHTTPRequestHandler):
    def _reply(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(n).decode("utf-8", "replace")
        try:
            payload = json.loads(raw)
            parsed = True
        except ValueError:
            payload, parsed = {"_unparsed": raw[:4000]}, False
        rec = {"received_at": time.time(), "path": self.path,
               "parsed_json": parsed, "bytes": n, "payload": payload}
        with open(LOG, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        sys.stderr.write("[receiver] POST %s %dB run=%s ok=%s\n" % (
            self.path, n, payload.get("run_id"), payload.get("ok")))
        sys.stderr.flush()
        self._reply(200, {"stored": True, "run_id": payload.get("run_id")})

    def do_GET(self):
        # /health so the runner can prove the receiver was up BEFORE the run,
        # rather than discovering a missing POST and not knowing which end failed.
        if self.path == "/health":
            return self._reply(200, {"ok": True, "log": LOG})
        self._reply(404, {"ok": False})

    def log_message(self, *a):
        pass  # the JSONL log is the record; stock access logging just adds noise


def main():
    global LOG
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=18723)
    ap.add_argument("--log", default=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                  "..", "runs", "receiver.jsonl"))
    a = ap.parse_args()
    LOG = os.path.abspath(a.log)
    os.makedirs(os.path.dirname(LOG), exist_ok=True)
    srv = HTTPServer(("127.0.0.1", a.port), Handler)  # localhost only, never 0.0.0.0
    sys.stderr.write("[receiver] listening on 127.0.0.1:%d -> %s\n" % (a.port, LOG))
    sys.stderr.flush()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
