"""
Overnight 11 req/min Endurance Probe Runner (Zero third-party dependencies).
Tests a single parent category (catalog[]=5, Men, newest_first, pages 1..10)
at 11 polite GET requests/minute with persistent JSONL logging, hourly buckets,
and an automatic 3-strike Circuit Breaker + 30-min canary recovery check.
"""
from __future__ import annotations

import gzip
import json
import os
import platform
import random
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

EXPECTED_KEY = os.environ.get("FLEET_PROBE_KEY", "")
PROVIDER_NAME = os.environ.get("PROVIDER_NAME", "blitz_overnight")
CATALOG_ID = os.environ.get("CATALOG_ID", "5")
REQ_PER_MIN = float(os.environ.get("REQ_PER_MIN", "11"))
MAX_HOURS = float(os.environ.get("MAX_HOURS", "12"))
LOG_PATH = Path(os.environ.get("LOG_PATH", "/app/overnight_log.jsonl"))

STATE_LOCK = threading.Lock()
STOP_EVENT = threading.Event()

STATE: dict = {
    "provider": PROVIDER_NAME,
    "runtime": f"python-{platform.python_version()}",
    "catalog_id": CATALOG_ID,
    "target_rpm": REQ_PER_MIN,
    "max_hours": MAX_HOURS,
    "state": "STARTING",
    "egress": None,
    "started_ts": time.time(),
    "started_iso": datetime.now(timezone.utc).isoformat(),
    "stopped_iso": None,
    "total_requests": 0,
    "ok_200_with_items": 0,
    "silent_empty_0_items": 0,
    "http_429": 0,
    "http_403": 0,
    "http_404": 0,
    "other_errors": 0,
    "consecutive_failures": 0,
    "block_count": 0,
    "first_failure": None,
    "block_events": [],
    "total_wire_bytes": 0,
    "sum_ms": 0,
    "min_items_ok": None,
    "max_items_ok": None,
    "hourly": {},
    "recent": [],
}


def probe_egress() -> dict:
    t0 = time.monotonic()
    try:
        req = urllib.request.Request("https://ipwho.is/", headers={"Accept": "application/json", "User-Agent": "fleet-overnight/1.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            d = json.loads(r.read().decode("utf-8", errors="replace"))
        conn = d.get("connection") or {}
        return {
            "ok": True,
            "ms": int((time.monotonic() - t0) * 1000),
            "ip": d.get("ip"),
            "country": d.get("country_code"),
            "city": d.get("city"),
            "asn": conn.get("asn"),
            "org": conn.get("org") or conn.get("isp"),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def fetch_once(page: int) -> dict:
    url = f"https://www.vinted.lt/catalog?catalog[]={CATALOG_ID}&order=newest_first&page={page}"
    t0 = time.monotonic()
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Encoding": "gzip",
            "Accept-Language": "lt-LT,lt;q=0.9,en;q=0.8",
        },
    )
    status = 0
    final_url = url
    headers_map: dict[str, str] = {}
    raw_bytes = b""
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            status = resp.status
            final_url = resp.geturl()
            headers_map = {k.lower(): v for k, v in resp.headers.items()}
            raw_bytes = resp.read()
    except urllib.error.HTTPError as he:
        status = he.code
        final_url = he.geturl()
        headers_map = {k.lower(): v for k, v in he.headers.items()}
        try:
            raw_bytes = he.read()
        except Exception:
            raw_bytes = b""
    except Exception as e:
        return {
            "ts": datetime.now(timezone.utc).isoformat(),
            "page": page,
            "http_status": 0,
            "ms": int((time.monotonic() - t0) * 1000),
            "wire_bytes": 0,
            "items": 0,
            "final_url": url,
            "sig": "NETWORK_ERROR",
            "error": str(e)[:160],
        }

    ce = headers_map.get("content-encoding", "")
    if "gzip" in ce.lower():
        try:
            text = gzip.decompress(raw_bytes).decode("utf-8", errors="replace")
        except Exception:
            text = raw_bytes.decode("utf-8", errors="replace")
    else:
        text = raw_bytes.decode("utf-8", errors="replace")

    cf_mit = headers_map.get("cf-mitigated")
    retry_after = headers_map.get("retry-after")
    lower_head = text[:8000].lower()
    has_challenge = bool(cf_mit) or any(
        m in lower_head for m in ("just a moment", "datadome", "cf-turnstile", "attention required", "access denied")
    )
    items = text.count("productItem")

    if status == 403:
        sig = "CHALLENGE_403" if has_challenge else "HTTP_403"
    elif status == 429:
        sig = "HTTP_429"
    elif status == 404:
        sig = "HTTP_404"
    elif has_challenge:
        sig = f"CHALLENGE_HTTP_{status}"
    elif status == 200 and items > 0:
        sig = "OK_WITH_ITEMS"
    elif status == 200 and items == 0:
        sig = "SILENT_EMPTY_0_ITEMS"
    else:
        sig = f"HTTP_{status}"

    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "page": page,
        "http_status": status,
        "ms": int((time.monotonic() - t0) * 1000),
        "wire_bytes": len(raw_bytes),
        "items": items,
        "final_url": final_url,
        "retry_after": retry_after,
        "sig": sig,
    }


def append_log(entry: dict) -> None:
    try:
        with LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass


def record_result(res: dict, is_canary: bool = False) -> bool:
    """Record result; return True if request succeeded (HTTP 200 with items > 0)."""
    now = time.time()
    ok = res["sig"] == "OK_WITH_ITEMS"
    with STATE_LOCK:
        elapsed_min = round((now - STATE["started_ts"]) / 60.0, 2)
        hour_key = f"h{int((now - STATE['started_ts']) // 3600):02d}"
        STATE["total_requests"] += 1
        seq = STATE["total_requests"]
        res["seq"] = seq
        res["elapsed_min"] = elapsed_min
        res["canary"] = is_canary

        STATE["total_wire_bytes"] += res["wire_bytes"]
        STATE["sum_ms"] += res["ms"]

        hb = STATE["hourly"].setdefault(hour_key, {"req": 0, "ok": 0, "empty0": 0, "429": 0, "403": 0, "other": 0})
        hb["req"] += 1

        if ok:
            STATE["ok_200_with_items"] += 1
            STATE["consecutive_failures"] = 0
            hb["ok"] += 1
            ic = res["items"]
            STATE["min_items_ok"] = ic if STATE["min_items_ok"] is None else min(STATE["min_items_ok"], ic)
            STATE["max_items_ok"] = ic if STATE["max_items_ok"] is None else max(STATE["max_items_ok"], ic)
        else:
            STATE["consecutive_failures"] += 1
            if res["sig"] == "SILENT_EMPTY_0_ITEMS":
                STATE["silent_empty_0_items"] += 1
                hb["empty0"] += 1
            elif res["http_status"] == 429:
                STATE["http_429"] += 1
                hb["429"] += 1
            elif res["http_status"] == 403:
                STATE["http_403"] += 1
                hb["403"] += 1
            elif res["http_status"] == 404:
                STATE["http_404"] += 1
                hb["other"] += 1
            else:
                STATE["other_errors"] += 1
                hb["other"] += 1
            if STATE["first_failure"] is None:
                STATE["first_failure"] = dict(res)

        STATE["recent"].append(res)
        if len(STATE["recent"]) > 25:
            STATE["recent"] = STATE["recent"][-25:]

    append_log(res)
    return ok


def runner_loop() -> None:
    eg = probe_egress()
    with STATE_LOCK:
        STATE["egress"] = eg
        STATE["state"] = "RUNNING"

    base_interval = 60.0 / max(REQ_PER_MIN, 0.5)
    max_seconds = MAX_HOURS * 3600.0
    seq_page = 0

    while not STOP_EVENT.is_set():
        elapsed = time.time() - STATE["started_ts"]
        if elapsed >= max_seconds:
            with STATE_LOCK:
                STATE["state"] = "STOPPED_COMPLETE_12H"
                STATE["stopped_iso"] = datetime.now(timezone.utc).isoformat()
            break

        with STATE_LOCK:
            cur_state = STATE["state"]

        if cur_state == "COOLDOWN":
            # Wait 30 minutes politely before 1 canary probe
            if STOP_EVENT.wait(1800.0):
                break
            res = fetch_once(1)
            ok = record_result(res, is_canary=True)
            with STATE_LOCK:
                if ok:
                    STATE["block_events"].append({
                        "event": "RECOVERED_CANARY",
                        "ts": res["ts"],
                        "elapsed_min": res["elapsed_min"],
                        "seq": res["seq"],
                    })
                    STATE["state"] = "RUNNING"
                else:
                    STATE["block_events"].append({
                        "event": "STILL_BLOCKED_CANARY",
                        "ts": res["ts"],
                        "elapsed_min": res["elapsed_min"],
                        "sig": res["sig"],
                    })
            continue

        seq_page = (seq_page % 10) + 1
        t_start = time.monotonic()
        res = fetch_once(seq_page)
        ok = record_result(res, is_canary=False)

        with STATE_LOCK:
            consec = STATE["consecutive_failures"]
            if not ok and consec >= 3:
                STATE["block_count"] += 1
                STATE["block_events"].append({
                    "event": f"CIRCUIT_BREAKER_TRIP_{STATE['block_count']}",
                    "ts": res["ts"],
                    "elapsed_min": res["elapsed_min"],
                    "seq": res["seq"],
                    "sig": res["sig"],
                    "http_status": res["http_status"],
                })
                if STATE["block_count"] >= 2:
                    STATE["state"] = "STOPPED_AFTER_2ND_BLOCK"
                    STATE["stopped_iso"] = datetime.now(timezone.utc).isoformat()
                    break
                else:
                    STATE["state"] = "COOLDOWN"
                    continue

        spent = time.monotonic() - t_start
        jitter = random.uniform(-0.35, 0.35)
        wait_s = max(0.5, base_interval - spent + jitter)
        STOP_EVENT.wait(wait_s)

    with STATE_LOCK:
        if STATE["state"] == "RUNNING":
            STATE["state"] = "STOPPED_MANUAL"
            STATE["stopped_iso"] = datetime.now(timezone.utc).isoformat()


def snapshot_status() -> dict:
    with STATE_LOCK:
        s = dict(STATE)
        s["elapsed_min"] = round((time.time() - s["started_ts"]) / 60.0, 2)
        s["total_wire_mb"] = round(s["total_wire_bytes"] / (1024 * 1024), 2)
        s["avg_ms"] = round(s["sum_ms"] / s["total_requests"], 1) if s["total_requests"] else 0.0
        s["success_rate_pct"] = round(100.0 * s["ok_200_with_items"] / s["total_requests"], 2) if s["total_requests"] else 0.0
        s["actual_rpm"] = round(s["total_requests"] / max(s["elapsed_min"], 0.05), 2)
        return s


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args) -> None:
        pass

    def _send_json(self, code: int, payload: dict) -> None:
        body = json.dumps(payload, indent=2).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _authed(self) -> bool:
        return bool(EXPECTED_KEY) and self.headers.get("X-Fleet-Key", "") == EXPECTED_KEY

    def do_GET(self) -> None:
        path = urllib.parse.urlparse(self.path).path
        if path in ("/", "/health"):
            s = snapshot_status()
            return self._send_json(200, {
                "status": "ok",
                "provider": PROVIDER_NAME,
                "state": s["state"],
                "elapsed_min": s["elapsed_min"],
                "total_requests": s["total_requests"],
                "ok_200_with_items": s["ok_200_with_items"],
                "success_rate_pct": s["success_rate_pct"],
            })
        if path == "/status":
            if not self._authed():
                return self._send_json(401, {"error": "unauthorized"})
            return self._send_json(200, snapshot_status())
        self._send_json(404, {"error": "not_found"})

    def do_POST(self) -> None:
        path = urllib.parse.urlparse(self.path).path
        if not self._authed():
            return self._send_json(401, {"error": "unauthorized"})
        if path == "/stop":
            STOP_EVENT.set()
            return self._send_json(200, {"action": "stop_requested", "state": snapshot_status()["state"]})
        self._send_json(404, {"error": "not_found"})


if __name__ == "__main__":
    t = threading.Thread(target=runner_loop, daemon=True)
    t.start()
    port = int(os.environ.get("PORT", "7860"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"Overnight runner listening on 0.0.0.0:{port} ({PROVIDER_NAME}, {REQ_PER_MIN} rpm)", flush=True)
    server.serve_forever()
