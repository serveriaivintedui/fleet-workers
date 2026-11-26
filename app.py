"""
Minimal Python 3.14 stdlib HTTP Probe Server (Zero third-party dependencies).
Works identically on Hugging Face Spaces (PORT=7860), Render (PORT=10000), and Hostless.
"""
from __future__ import annotations

import gzip
import json
import os
import platform
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

try:
    from compression import zstd as _zstd
    HAS_ZSTD = True
except ImportError:
    _zstd = None
    HAS_ZSTD = False

PROVIDER_NAME = os.environ.get("PROVIDER_NAME", "python_container")
EXPECTED_KEY = os.environ.get("FLEET_PROBE_KEY", "")


def probe_egress() -> dict:
    t0 = time.monotonic()
    try:
        req = urllib.request.Request("https://ipwho.is/", headers={"Accept": "application/json", "User-Agent": "fleet-probe/1.0"})
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
        return {"ok": False, "ms": int((time.monotonic() - t0) * 1000), "error": str(e)}


def benchmark_compression() -> dict:
    sample = json.dumps({
        "items": [{"id": 10960000000 + i, "title": "Sample jacket M", "price_c": 1500, "likes": i % 17} for i in range(500)]
    }).encode("utf-8")
    t0 = time.monotonic()
    gz = gzip.compress(sample, compresslevel=9)
    gz_ms = int((time.monotonic() - t0) * 1000)
    zs_bytes = None
    zs_ms = None
    if HAS_ZSTD:
        t1 = time.monotonic()
        zs = _zstd.compress(sample, level=9)
        zs_ms = int((time.monotonic() - t1) * 1000)
        zs_bytes = len(zs)
    return {
        "raw_bytes": len(sample),
        "gzip_ok": True,
        "gzip_bytes": len(gz),
        "gzip_ms": gz_ms,
        "zstd_ok": HAS_ZSTD,
        "zstd_bytes": zs_bytes,
        "zstd_ms": zs_ms,
    }


def probe_target(url: str) -> dict:
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
    headers_map: dict[str, str] = {}
    wire_bytes = 0
    raw_bytes = b""
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            status = resp.status
            headers_map = {k.lower(): v for k, v in resp.headers.items()}
            wire_bytes = len(raw_bytes := resp.read())
    except urllib.error.HTTPError as he:
        status = he.code
        headers_map = {k.lower(): v for k, v in he.headers.items()}
        try:
            wire_bytes = len(raw_bytes := he.read())
        except Exception:
            raw_bytes = b""
    except Exception as e:
        return {"url": url, "http_status": 0, "ms": int((time.monotonic() - t0) * 1000), "block_signature": "NETWORK_ERROR", "error": str(e)}

    ce = headers_map.get("content-encoding", "")
    if "gzip" in ce.lower():
        try:
            text = gzip.decompress(raw_bytes).decode("utf-8", errors="replace")
        except Exception:
            text = raw_bytes.decode("utf-8", errors="replace")
    else:
        text = raw_bytes.decode("utf-8", errors="replace")

    cf_mit = headers_map.get("cf-mitigated")
    srv = headers_map.get("server")
    lower_head = text[:8000].lower()
    has_challenge = bool(cf_mit) or any(
        m in lower_head for m in ("just a moment", "datadome", "cf-turnstile", "attention required", "access denied")
    )
    items_count = text.count("productItem")

    if status == 403:
        sig = "CHALLENGE_403" if has_challenge else "HTTP_403"
    elif status == 429:
        sig = "HTTP_429"
    elif has_challenge:
        sig = f"CHALLENGE_HTTP_{status}"
    elif status == 200 and items_count > 0:
        sig = "OK_WITH_ITEMS"
    elif status == 200 and items_count == 0:
        sig = "SILENT_EMPTY_0_ITEMS"
    else:
        sig = f"HTTP_{status}"

    return {
        "url": url,
        "http_status": status,
        "ms": int((time.monotonic() - t0) * 1000),
        "wire_bytes": wire_bytes,
        "raw_bytes": len(text),
        "content_encoding": ce,
        "server_header": srv,
        "cf_mitigated": cf_mit,
        "product_item_markers": items_count,
        "block_signature": sig,
        "snippet": " ".join(text[:220].split()),
    }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args) -> None:
        pass  # keep public logs clean

    def _send_json(self, code: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path in ("/", "/health"):
            return self._send_json(200, {"status": "ok", "provider": PROVIDER_NAME})

        if parsed.path == "/probe":
            provided = self.headers.get("X-Fleet-Key", "")
            if not EXPECTED_KEY or provided != EXPECTED_KEY:
                return self._send_json(401, {"error": "unauthorized"})
            qs = urllib.parse.parse_qs(parsed.query)
            target = qs.get("target", ["https://www.vinted.lt/catalog?order=newest_first&page=1"])[0]
            skip_target = qs.get("egress_only", ["0"])[0] == "1"
            return self._send_json(200, {
                "provider": PROVIDER_NAME,
                "runtime": f"python-{platform.python_version()}",
                "cpu_count": os.cpu_count(),
                "ts": int(time.time()),
                "egress": probe_egress(),
                "compression": benchmark_compression(),
                "target_probe": None if skip_target else probe_target(target),
            })

        self._send_json(404, {"error": "not_found"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "7860"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"Probe listening on 0.0.0.0:{port} ({PROVIDER_NAME})", flush=True)
    server.serve_forever()
