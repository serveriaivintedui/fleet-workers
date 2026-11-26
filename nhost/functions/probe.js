// Vercel Serverless Function Stage 2 Probe (Node.js runtime)
const zlib = require("zlib");

async function probeEgress() {
  const t0 = Date.now();
  try {
    const r = await fetch("https://ipwho.is/", { headers: { Accept: "application/json" } });
    const d = await r.json();
    return {
      ok: true,
      ms: Date.now() - t0,
      ip: d.ip || null,
      country: d.country_code || null,
      city: d.city || null,
      asn: d.connection?.asn || null,
      org: d.connection?.org || d.connection?.isp || null,
    };
  } catch (e) {
    return { ok: false, ms: Date.now() - t0, error: String(e) };
  }
}

function benchmarkCompression() {
  const sample = Buffer.from(
    JSON.stringify({
      items: Array.from({ length: 500 }, (_, i) => ({
        id: 10960000000 + i,
        title: "Sample jacket M",
        price_c: 1500,
        likes: i % 17,
      })),
    })
  );
  const t0 = Date.now();
  const gz = zlib.gzipSync(sample, { level: 9 });
  const gzMs = Date.now() - t0;
  let zstdOk = false;
  let zstdBytes = null;
  let zstdMs = null;
  if (typeof zlib.zstdCompressSync === "function") {
    const t1 = Date.now();
    const zs = zlib.zstdCompressSync(sample);
    zstdMs = Date.now() - t1;
    zstdBytes = zs.length;
    zstdOk = true;
  }
  return {
    gzip_ok: true,
    gzip_bytes: gz.length,
    gzip_ms: gzMs,
    zstd_ok: zstdOk,
    zstd_bytes: zstdBytes,
    zstd_ms: zstdMs,
    raw_bytes: sample.length,
  };
}

async function probeTarget(url) {
  const t0 = Date.now();
  try {
    const resp = await fetch(url, {
      method: "GET",
      redirect: "follow",
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "lt-LT,lt;q=0.9,en;q=0.8",
      },
    });
    const status = resp.status;
    const cfMitigated = resp.headers.get("cf-mitigated") || null;
    const serverHdr = resp.headers.get("server") || null;
    const text = await resp.text();
    const lowerHead = text.slice(0, 8000).toLowerCase();
    const hasChallenge =
      Boolean(cfMitigated) ||
      lowerHead.includes("just a moment") ||
      lowerHead.includes("datadome") ||
      lowerHead.includes("cf-turnstile") ||
      lowerHead.includes("attention required") ||
      lowerHead.includes("access denied");

    let itemsCount = 0;
    let idx = 0;
    while (itemsCount < 200) {
      const found = text.indexOf("productItem", idx);
      if (found === -1) break;
      itemsCount++;
      idx = found + 11;
    }

    let sig = "UNKNOWN";
    if (status === 403) sig = hasChallenge ? "CHALLENGE_403" : "HTTP_403";
    else if (status === 429) sig = "HTTP_429";
    else if (hasChallenge) sig = `CHALLENGE_HTTP_${status}`;
    else if (status === 200 && itemsCount > 0) sig = "OK_WITH_ITEMS";
    else if (status === 200 && itemsCount === 0) sig = "SILENT_EMPTY_0_ITEMS";
    else sig = `HTTP_${status}`;

    return {
      url,
      http_status: status,
      ms: Date.now() - t0,
      raw_bytes: text.length,
      server_header: serverHdr,
      cf_mitigated: cfMitigated,
      product_item_markers: itemsCount,
      block_signature: sig,
      snippet: text.slice(0, 220).replace(/\s+/g, " "),
    };
  } catch (e) {
    return { url, http_status: 0, ms: Date.now() - t0, block_signature: "NETWORK_ERROR", error: String(e) };
  }
}

module.exports = async (req, res) => {
  const expected = process.env.FLEET_PROBE_KEY;
  if (!expected || req.headers["x-fleet-key"] !== expected) {
    return res.status(401).json({ error: "unauthorized" });
  }
  const target = req.query.target || "https://www.vinted.lt/catalog?order=newest_first&page=1";
  const skipTarget = req.query.egress_only === "1";

  const egress = await probeEgress();
  const compression = benchmarkCompression();
  const target_probe = skipTarget ? null : await probeTarget(target);

  return res.status(200).json({
    provider: "nhost_eu_fra",
    runtime: `node-${process.version}`,
    region: process.env.VERCEL_REGION || null,
    ts: Math.floor(Date.now() / 1000),
    egress,
    compression,
    target_probe,
  });
};
