#!/usr/bin/env python3
"""Minimal stand-in for Dex's table-stats endpoint, for local Optimus dev testing.

Serves GET /dex/tables/{store}/{table_name}/stats matching the real Dex response
schema (see optimus's ext/dex/model.go), and a control endpoint to flip behavior
at runtime without restarting the pod:

  PUT /control/mode  body: {"mode": "complete" | "incomplete" | "5xx"}
  GET /control/mode  -> current mode

"complete"/"incomplete" control the is_complete flag returned for every date in
the requested range. "5xx" makes the stats endpoint return HTTP 503 for every
request, to exercise Optimus's THIRD_PARTY_SENSOR_TOGGLE_SOFT_5XX path.
"""
import json
import re
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

PRODUCER_TYPE = "mock-producer"

state = {"mode": "complete"}

STATS_PATH_RE = re.compile(r"^/dex/tables/[^/]+/[^/]+/stats$")


def parse_iso(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def daterange_days(start, end):
    day = start.date()
    last = end.date()
    while day <= last:
        yield day
        day += timedelta(days=1)


class Handler(BaseHTTPRequestHandler):
    def _write_json(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        if STATS_PATH_RE.match(parsed.path):
            return self._handle_stats(parsed)
        if parsed.path == "/control/mode":
            return self._write_json(200, {"mode": state["mode"]})
        self._write_json(404, {"error": "not found"})

    def do_PUT(self):
        parsed = urlparse(self.path)
        if parsed.path == "/control/mode":
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length) or b"{}")
            mode = body.get("mode")
            if mode not in ("complete", "incomplete", "5xx"):
                return self._write_json(400, {"error": "mode must be complete, incomplete, or 5xx"})
            state["mode"] = mode
            return self._write_json(200, {"mode": state["mode"]})
        self._write_json(404, {"error": "not found"})

    def _handle_stats(self, parsed):
        if state["mode"] == "5xx":
            return self._write_json(503, {"error": "mock dex: simulated 5xx"})

        qs = parse_qs(parsed.query)
        now = datetime.now(timezone.utc)
        start = parse_iso(qs["from"][0]) if "from" in qs else now - timedelta(hours=24)
        end = parse_iso(qs["to"][0]) if "to" in qs else now

        is_complete = state["mode"] == "complete"
        date_breakdown = [
            {"date": datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc).isoformat(), "is_complete": is_complete}
            for day in daterange_days(start, end)
        ]

        self._write_json(200, {
            "stats": {
                "producer_type": PRODUCER_TYPE,
                "producer": {
                    "status": "RUNNING",
                    "stopped_at": "0001-01-01T00:00:00Z",
                },
                "is_complete": is_complete,
                "date_breakdown": date_breakdown,
            }
        })

    def log_message(self, fmt, *args):
        print("[mock-dex] %s - %s" % (self.address_string(), fmt % args))


if __name__ == "__main__":
    server = ThreadingHTTPServer(("0.0.0.0", 8000), Handler)
    print("mock-dex listening on :8000, producer_type=%s" % PRODUCER_TYPE)
    server.serve_forever()
