import gzip
import json
import os
import platform
import time
import urllib.error
import urllib.request

try:
    from compression import zstd as _zstd
    HAS_ZSTD = True
except ImportError:
    try:
        import zstandard as _zstd
        HAS_ZSTD = True
    except ImportError:
        _zstd = None
        HAS_ZSTD = False


def probe_egress():
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


def benchmark_compression():
    sample = json.dumps({
        "items": [{"id": 10960000000 + i, "title": "Sample jacket M", "price_c": 1500, "likes": i % 17} for i in range(500)]
    }).encode("utf-8")
    t0 = time.monotonic()
    gz = gzip.compress(sample, compresslevel=9)
    gz_ms = int((time.monotonic() - t0) * 1000)
    return {"raw_bytes": len(sample), "gzip_ok": True, "gzip_bytes": len(gz), "gzip_ms": gz_ms, "zstd_ok": HAS_ZSTD}


def probe_target(url):
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
    headers_map = {}
    raw_bytes = b""
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            status = resp.status
            headers_map = {k.lower(): v for k, v in resp.headers.items()}
            raw_bytes = resp.read()
    except urllib.error.HTTPError as he:
        status = he.code
        headers_map = {k.lower(): v for k, v in he.headers.items()}
        try:
            raw_bytes = he.read()
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
        "wire_bytes": len(raw_bytes),
        "raw_bytes": len(text),
        "content_encoding": ce,
        "server_header": srv,
        "cf_mitigated": cf_mit,
        "product_item_markers": items_count,
        "block_signature": sig,
        "snippet": " ".join(text[:220].split()),
    }


result = {
    "provider": "github_actions_azure",
    "runtime": f"python-{platform.python_version()}",
    "cpu_count": os.cpu_count(),
    "ts": int(time.time()),
    "egress": probe_egress(),
    "compression": benchmark_compression(),
    "target_probe": probe_target("https://www.vinted.lt/catalog?order=newest_first&page=1"),
}

with open("probe_result.json", "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2)

print("PROBE_JSON_START")
print(json.dumps(result))
print("PROBE_JSON_END")
