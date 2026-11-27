"""
Multi-Server 12-Hour Endurance & Fleet Telemetry Coordinator (Zero third-party dependencies).
Designed to run 24/7 on Hostless (FI Helsinki, zero cold starts, 1 GB RAM).
- Keeps /health and /probe backward compatible.
- Runs independent worker loops for all 12 active HTTP cloud networks in parallel,
  each with its own quota-safe RPM, dedicated parent/leaf category, pages 1..10 cycling,
  and an automatic 3-strike Circuit Breaker + 30-min canary recovery check.
- Auto-stops cleanly after MAX_HOURS (default 12h).
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

EXPECTED_KEY = os.environ.get("FLEET_PROBE_KEY", "")
APPWRITE_API_KEY = os.environ.get("APPWRITE_API_KEY", "")
APPWRITE_PROJECT_ID = os.environ.get("APPWRITE_PROJECT_ID", "6b08454f001664079cb8")
PROVIDER_NAME = os.environ.get("PROVIDER_NAME", "hostless_fleet_coordinator")
MAX_HOURS = float(os.environ.get("MAX_HOURS", "12"))

STOP_EVENT = threading.Event()
LOCK = threading.Lock()
STARTED_TS = time.time()
STARTED_ISO = datetime.now(timezone.utc).isoformat()

# 12 active HTTP workers across 11 distinct networks, each strictly inside its free-tier budget
WORKERS = [
    {"id": "hostless_direct_fi", "mode": "direct", "rpm": 11.0, "cat": "5", "net": "Hetzner FI"},
    {"id": "cloudflare_se", "mode": "get", "url": "https://fleet-probe-cf.serveriaivintedui.workers.dev/", "no_egress": True, "rpm": 6.0, "cat": "18", "net": "Cloudflare SE"},
    {"id": "encore_be_ipv6", "mode": "get", "url": "https://staging-fleet-probe-encore-izz2.encr.app/probe", "no_egress": True, "rpm": 4.0, "cat": "529", "net": "GCP BE IPv6"},
    {"id": "wasmer_ovh_fr", "mode": "get", "url": "https://fleet-probe-wasmer.wasmer.app/", "no_egress": True, "rpm": 3.0, "cat": "1059", "net": "OVH FR"},
    {"id": "supabase_de_ipv6", "mode": "get", "url": "https://popimvecxezywrlacfai.supabase.co/functions/v1/probe", "no_egress": False, "rpm": 3.0, "cat": "1875", "net": "AWS DE IPv6"},
    {"id": "nhost_de", "mode": "get", "url": "https://jqfmgfgtwrqiotcvjgpy.functions.eu-central-1.nhost.run/v1/probe", "no_egress": False, "rpm": 3.0, "cat": "1087", "net": "AWS DE"},
    {"id": "render_de", "mode": "get", "url": "https://fleet-probe-render.onrender.com/probe", "no_egress": False, "rpm": 2.0, "cat": "1043", "net": "Render DE"},
    {"id": "vercel_fra1", "mode": "get", "url": "https://fleet-probe-vercel.vercel.app/api/probe", "no_egress": False, "rpm": 2.0, "cat": "2560", "net": "AWS DE"},
    {"id": "appwrite_do_de", "mode": "appwrite", "url": "https://fra.cloud.appwrite.io/v1/functions/fleet-probe/executions", "rpm": 2.0, "cat": "2319", "net": "DigitalOcean DE"},
    {"id": "convex_ie", "mode": "get", "url": "https://silent-tern-458.eu-west-1.convex.site/probe", "no_egress": False, "rpm": 1.0, "cat": "1845", "net": "AWS IE"},
    {"id": "fermyon_us", "mode": "get_fixed", "url": "https://fermyon-o9kpob8c.fermyon.app/", "rpm": 1.0, "cat": "root", "net": "AWS US"},
    {"id": "snapdeploy_us", "mode": "get", "url": "https://fleet-probe-sd.containers.snapdeploy.app/probe", "no_egress": False, "rpm": 1.0, "cat": "222", "net": "AWS US"},
]

STATS: dict[str, dict] = {}


def init_worker_stat(w: dict) -> dict:
    return {
        "id": w["id"],
        "net": w["net"],
        "cat": w["cat"],
        "target_rpm": w["rpm"],
        "state": "STARTING",
        "total": 0,
        "ok": 0,
        "empty0": 0,
        "http_429": 0,
        "http_403": 0,
        "worker_err": 0,
        "other_err": 0,
        "consec_fail": 0,
        "blocks": 0,
        "sum_ms": 0,
        "min_items": None,
        "max_items": None,
        "last_ip": None,
        "first_failure": None,
        "block_events": [],
        "hourly": {},
        "last_5": [],
    }


def direct_fetch(target: str) -> dict:
    t0 = time.monotonic()
    req = urllib.request.Request(
        target,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Encoding": "gzip",
            "Accept-Language": "lt-LT,lt;q=0.9,en;q=0.8",
        },
    )
    status = 0
    headers_map: dict[str, str] = {}
    raw = b""
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            status = resp.status
            headers_map = {k.lower(): v for k, v in resp.headers.items()}
            raw = resp.read()
    except urllib.error.HTTPError as he:
        status = he.code
        headers_map = {k.lower(): v for k, v in he.headers.items()}
        try:
            raw = he.read()
        except Exception:
            raw = b""
    except Exception as e:
        return {"http_status": 0, "ms": int((time.monotonic() - t0) * 1000), "items": 0, "sig": "NETWORK_ERROR", "err": str(e)[:120]}

    ce = headers_map.get("content-encoding", "")
    text = raw.decode("utf-8", errors="replace")
    if "gzip" in ce.lower():
        try:
            text = gzip.decompress(raw).decode("utf-8", errors="replace")
        except Exception:
            pass
    items = text.count("productItem")
    if status == 200 and items > 0:
        sig = "OK_WITH_ITEMS"
    elif status == 200 and items == 0:
        sig = "SILENT_EMPTY_0_ITEMS"
    elif status == 429:
        sig = "HTTP_429"
    elif status == 403:
        sig = "HTTP_403"
    else:
        sig = f"HTTP_{status}"
    return {"http_status": status, "ms": int((time.monotonic() - t0) * 1000), "items": items, "sig": sig}


def call_worker(w: dict, target: str) -> dict:
    mode = w["mode"]
    if mode == "direct":
        return direct_fetch(target)

    t0 = time.monotonic()
    try:
        if mode == "appwrite":
            if not APPWRITE_API_KEY:
                return {"http_status": 0, "ms": 0, "items": 0, "sig": "WORKER_NO_KEY"}
            path = "/?target=" + urllib.parse.quote(target, safe="")
            body = json.dumps({
                "path": path,
                "method": "GET",
                "async": False,
                "headers": {"x-fleet-key": EXPECTED_KEY},
            }).encode("utf-8")
            req = urllib.request.Request(
                w["url"],
                data=body,
                method="POST",
                headers={
                    "X-Appwrite-Project": APPWRITE_PROJECT_ID,
                    "X-Appwrite-Key": APPWRITE_API_KEY,
                    "Content-Type": "application/json",
                    "User-Agent": "Mozilla/5.0",
                },
            )
            with urllib.request.urlopen(req, timeout=30) as r:
                outer = json.loads(r.read().decode("utf-8", errors="replace"))
            inner_str = outer.get("responseBody", "{}")
            d = json.loads(inner_str) if inner_str else {}
        else:
            url = w["url"]
            if mode == "get":
                sep = "&" if "?" in url else "?"
                qs = f"target={urllib.parse.quote(target, safe='')}"
                if w.get("no_egress"):
                    qs += "&no_egress=1"
                url = url + sep + qs
            req = urllib.request.Request(
                url,
                headers={"X-Fleet-Key": EXPECTED_KEY, "User-Agent": "Mozilla/5.0"},
            )
            with urllib.request.urlopen(req, timeout=30) as r:
                d = json.loads(r.read().decode("utf-8", errors="replace"))

        tp = d.get("target_probe") or {}
        eg = d.get("egress") or {}
        return {
            "http_status": int(tp.get("http_status") or 0),
            "ms": int(tp.get("ms") or int((time.monotonic() - t0) * 1000)),
            "items": int(tp.get("product_item_markers") or 0),
            "sig": tp.get("block_signature") or "UNKNOWN",
            "ip": eg.get("ip") if isinstance(eg, dict) else None,
        }
    except urllib.error.HTTPError as he:
        return {"http_status": he.code, "ms": int((time.monotonic() - t0) * 1000), "items": 0, "sig": f"WORKER_HTTP_{he.code}"}
    except Exception as e:
        return {"http_status": 0, "ms": int((time.monotonic() - t0) * 1000), "items": 0, "sig": "WORKER_ERR", "err": str(e)[:120]}


def record(wid: str, page: int, res: dict, canary: bool = False) -> bool:
    ok = res.get("sig") == "OK_WITH_ITEMS"
    now = time.time()
    elapsed_min = round((now - STARTED_TS) / 60.0, 2)
    hkey = f"h{int((now - STARTED_TS) // 3600):02d}"
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "elapsed_min": elapsed_min,
        "page": page,
        "status": res.get("http_status"),
        "items": res.get("items"),
        "ms": res.get("ms"),
        "sig": res.get("sig"),
        "canary": canary,
    }
    with LOCK:
        st = STATS[wid]
        st["total"] += 1
        st["sum_ms"] += res.get("ms") or 0
        if res.get("ip"):
            st["last_ip"] = res["ip"]
        hb = st["hourly"].setdefault(hkey, {"req": 0, "ok": 0, "fail": 0})
        hb["req"] += 1
        if ok:
            st["ok"] += 1
            st["consec_fail"] = 0
            hb["ok"] += 1
            ic = res.get("items") or 0
            st["min_items"] = ic if st["min_items"] is None else min(st["min_items"], ic)
            st["max_items"] = ic if st["max_items"] is None else max(st["max_items"], ic)
        else:
            st["consec_fail"] += 1
            hb["fail"] += 1
            sig = res.get("sig", "")
            if sig == "SILENT_EMPTY_0_ITEMS":
                st["empty0"] += 1
            elif sig == "HTTP_429" or res.get("http_status") == 429:
                st["http_429"] += 1
            elif "403" in sig or res.get("http_status") == 403:
                st["http_403"] += 1
            elif sig.startswith("WORKER_"):
                st["worker_err"] += 1
            else:
                st["other_err"] += 1
            if st["first_failure"] is None:
                st["first_failure"] = entry
        st["last_5"].append(entry)
        if len(st["last_5"]) > 5:
            st["last_5"] = st["last_5"][-5:]
    return ok


def worker_thread(w: dict, start_delay: float) -> None:
    wid = w["id"]
    if STOP_EVENT.wait(start_delay):
        return
    with LOCK:
        STATS[wid]["state"] = "RUNNING"

    base_interval = 60.0 / max(w["rpm"], 0.2)
    max_seconds = MAX_HOURS * 3600.0
    page = 0

    while not STOP_EVENT.is_set():
        if (time.time() - STARTED_TS) >= max_seconds:
            with LOCK:
                STATS[wid]["state"] = "COMPLETE_12H"
            break

        with LOCK:
            cur_state = STATS[wid]["state"]

        if cur_state == "COOLDOWN":
            if STOP_EVENT.wait(1800.0):
                break
            target = f"https://www.vinted.lt/catalog?catalog[]={w['cat']}&order=newest_first&page=1" if w["cat"] != "root" else "https://www.vinted.lt/catalog?order=newest_first&page=1"
            res = call_worker(w, target)
            ok = record(wid, 1, res, canary=True)
            with LOCK:
                if ok:
                    STATS[wid]["block_events"].append({"event": "RECOVERED", "elapsed_min": round((time.time() - STARTED_TS) / 60.0, 2)})
                    STATS[wid]["state"] = "RUNNING"
                else:
                    STATS[wid]["block_events"].append({"event": "STILL_BLOCKED", "sig": res.get("sig"), "elapsed_min": round((time.time() - STARTED_TS) / 60.0, 2)})
            continue

        page = (page % 10) + 1
        target = f"https://www.vinted.lt/catalog?catalog[]={w['cat']}&order=newest_first&page={page}" if w["cat"] != "root" else "https://www.vinted.lt/catalog?order=newest_first&page=1"
        t0 = time.monotonic()
        res = call_worker(w, target)
        ok = record(wid, page, res, canary=False)

        with LOCK:
            if not ok and STATS[wid]["consec_fail"] >= 3:
                STATS[wid]["blocks"] += 1
                STATS[wid]["block_events"].append({
                    "event": f"TRIP_{STATS[wid]['blocks']}",
                    "sig": res.get("sig"),
                    "elapsed_min": round((time.time() - STARTED_TS) / 60.0, 2),
                    "total": STATS[wid]["total"],
                })
                if STATS[wid]["blocks"] >= 2:
                    STATS[wid]["state"] = "STOPPED_2ND_BLOCK"
                    break
                else:
                    STATS[wid]["state"] = "COOLDOWN"
                    continue

        spent = time.monotonic() - t0
        jitter = random.uniform(-0.4, 0.4)
        wait_s = max(0.5, base_interval - spent + jitter)
        STOP_EVENT.wait(wait_s)

    with LOCK:
        if STATS[wid]["state"] == "RUNNING":
            STATS[wid]["state"] = "STOPPED_MANUAL"


def fleet_snapshot() -> dict:
    elapsed_min = round((time.time() - STARTED_TS) / 60.0, 2)
    with LOCK:
        workers_out = []
        tot_req = 0
        tot_ok = 0
        for w in WORKERS:
            st = dict(STATS[w["id"]])
            tot_req += st["total"]
            tot_ok += st["ok"]
            st["avg_ms"] = round(st["sum_ms"] / st["total"], 1) if st["total"] else 0.0
            st["success_pct"] = round(100.0 * st["ok"] / st["total"], 2) if st["total"] else 0.0
            st["actual_rpm"] = round(st["total"] / max(elapsed_min, 0.05), 2)
            workers_out.append(st)
    return {
        "coordinator": PROVIDER_NAME,
        "runtime": f"python-{platform.python_version()}",
        "started_iso": STARTED_ISO,
        "elapsed_min": elapsed_min,
        "max_hours": MAX_HOURS,
        "stopped": STOP_EVENT.is_set(),
        "fleet_total_requests": tot_req,
        "fleet_total_ok": tot_ok,
        "fleet_success_pct": round(100.0 * tot_ok / tot_req, 2) if tot_req else 0.0,
        "fleet_actual_rpm": round(tot_req / max(elapsed_min, 0.05), 2),
        "workers": workers_out,
    }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args) -> None:
        pass

    def _send(self, code: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _authed(self) -> bool:
        return bool(EXPECTED_KEY) and self.headers.get("X-Fleet-Key", "") == EXPECTED_KEY

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path in ("/", "/health"):
            snap = fleet_snapshot()
            return self._send(200, {
                "status": "ok",
                "provider": PROVIDER_NAME,
                "elapsed_min": snap["elapsed_min"],
                "fleet_total_requests": snap["fleet_total_requests"],
                "fleet_success_pct": snap["fleet_success_pct"],
            })
        if parsed.path == "/fleet/status":
            if not self._authed():
                return self._send(401, {"error": "unauthorized"})
            return self._send(200, fleet_snapshot())
        if parsed.path == "/probe":
            if not self._authed():
                return self._send(401, {"error": "unauthorized"})
            qs = urllib.parse.parse_qs(parsed.query)
            target = qs.get("target", ["https://www.vinted.lt/catalog?order=newest_first&page=1"])[0]
            return self._send(200, {"provider": PROVIDER_NAME, "target_probe": direct_fetch(target)})
        self._send(404, {"error": "not_found"})

    def do_POST(self) -> None:
        path = urllib.parse.urlparse(self.path).path
        if not self._authed():
            return self._send(401, {"error": "unauthorized"})
        if path == "/fleet/stop":
            STOP_EVENT.set()
            return self._send(200, {"action": "stopped"})
        self._send(404, {"error": "not_found"})


if __name__ == "__main__":
    for idx, w in enumerate(WORKERS):
        STATS[w["id"]] = init_worker_stat(w)
        t = threading.Thread(target=worker_thread, args=(w, idx * 1.5), daemon=True)
        t.start()
    port = int(os.environ.get("PORT", "7860"))
    srv = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"Fleet coordinator listening on 0.0.0.0:{port} ({len(WORKERS)} workers)", flush=True)
    srv.serve_forever()
