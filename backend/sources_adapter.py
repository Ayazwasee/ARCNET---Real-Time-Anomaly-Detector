"""
Stage 3 — Unified Multi-Domain Ingestion Adapter (5 Real-Time & Paced Data Sources).

Normalizes all 5 sources into ARCNET's canonical graph event format so every downstream
module (Dynamic DiGraph, Louvain Communities, Personalized PageRank, Online Isolation Forest,
Neo4j Forensics, and Partitioning) works seamlessly across any domain while eliminating
source-specific trade-offs:

1. `blockchain` — Blockchain.com Mainnet Unconfirmed Tx (`wss://ws.blockchain.info/inv`)
2. `ripe_bgp`   — RIPE RIS Live BGP Routing (`wss://ris-live.ripe.net/v1/ws/`, collector `rrc21`,
                  enriched with Tier-1/Indian ISP ASN-to-Org names to eliminate raw ASN opacity)
3. `wikimedia`  — Wikimedia EventStreams (`enwiki` filtered SSE, Editor -> Article & Co-Edit links)
4. `bluesky`    — Bluesky Jetstream (`app.bsky.feed.post` reply/mention social interaction graph)
5. `gdelt`      — GDELT 2.0 Global Knowledge Graph (`lastupdate.txt` -> top 120 GKG entity rows
                  streamed in memory and paced smoothly at ~1.5 events/sec so 15-min batch files
                  behave identically to a live WebSocket stream without memory spikes)
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import random
import re
import time
import urllib.request
import uuid
import zipfile
from typing import Awaitable, Callable, Dict, List, Optional

try:
    import websockets
except ImportError:
    websockets = None

from backend.config import settings

SUPPORTED_SOURCES = {
    "blockchain": {
        "id": "blockchain",
        "label": "Blockchain.com (BTC)",
        "unit": "BTC",
        "node_noun": "Wallets",
        "edge_noun": "Transfers",
        "description": "Live Bitcoin mainnet mempool transactions (Wallet -> Wallet, weight = BTC).",
    },
    "ripe_bgp": {
        "id": "ripe_bgp",
        "label": "RIPE RIS Live (BGP)",
        "unit": "Prefixes",
        "node_noun": "Autonomous Systems",
        "edge_noun": "BGP Paths",
        "description": "Live Internet backbone routing updates from collector rrc21 (AS -> AS).",
    },
    "wikimedia": {
        "id": "wikimedia",
        "label": "Wikimedia (enwiki)",
        "unit": "KB Δ",
        "node_noun": "Editors & Pages",
        "edge_noun": "Live Edits",
        "description": "English Wikipedia real-time edit interaction graph (Editor -> Article).",
    },
    "bluesky": {
        "id": "bluesky",
        "label": "Bluesky Jetstream",
        "unit": "Interactions",
        "node_noun": "Accounts",
        "edge_noun": "Replies & Mentions",
        "description": "Real-time decentralized social reply/mention graph (User -> User).",
    },
    "gdelt": {
        "id": "gdelt",
        "label": "GDELT 2.0 Knowledge Graph",
        "unit": "Mentions",
        "node_noun": "Entities & Orgs",
        "edge_noun": "Co-Occurrences",
        "description": "Paced live stream of global news organisations, people & locations.",
    },
}

# Built-in ASN -> Organization enrichment so BGP graphs show readable names instead of raw numbers
ASN_NAMES: Dict[str, str] = {
    "13335": "AS13335 (Cloudflare)",
    "15169": "AS15169 (Google)",
    "16509": "AS16509 (Amazon AWS)",
    "8075": "AS8075 (Microsoft)",
    "32934": "AS32934 (Meta)",
    "20940": "AS20940 (Akamai)",
    "174": "AS174 (Cogent)",
    "3356": "AS3356 (Lumen)",
    "1299": "AS1299 (Arelion)",
    "2914": "AS2914 (NTT)",
    "6939": "AS6939 (Hurricane Electric)",
    "6453": "AS6453 (TATA Comm)",
    "9498": "AS9498 (Bharti Airtel)",
    "55836": "AS55836 (Reliance Jio)",
    "4755": "AS4755 (TATA Teleservices)",
    "9829": "AS9829 (BSNL India)",
    "4134": "AS4134 (Chinanet)",
    "4837": "AS4837 (China Unicom)",
    "7018": "AS7018 (AT&T)",
    "701": "AS701 (Verizon)",
    "3320": "AS3320 (Deutsche Telekom)",
    "54113": "AS54113 (Fastly)",
}


def format_asn(asn: str) -> str:
    clean = str(asn).strip().lstrip("ASas")
    return ASN_NAMES.get(clean, f"AS{clean}")


def normalize_raw_event(source: str, payload: dict) -> Optional[dict]:
    """
    Converts a raw payload from ANY of the 5 sources (server-fetched OR browser-relayed)
    into ARCNET's canonical graph event dict:
      {
        "source": str,
        "hash": str,
        "timestamp": float,
        "senders": [str, ...],
        "receivers": [(addr, weight), ...],
        "unit": str,
        "domain_context": str,
      }
    """
    now = float(payload.get("_override_ts", time.time()))
    source = source if source in SUPPORTED_SOURCES else "blockchain"

    # Already normalized canonical event
    if "senders" in payload and "receivers" in payload:
        return payload

    # 1. Blockchain.com Unconfirmed Transaction (`utx`)
    if source == "blockchain":
        tx = payload.get("x", payload)
        senders = []
        for inp in tx.get("inputs", []):
            addr = (inp.get("prev_out") or {}).get("addr")
            if addr and addr not in senders:
                senders.append(addr)

        recv_map: Dict[str, float] = {}
        for out in tx.get("out", []):
            addr = out.get("addr")
            btc = (out.get("value", 0) or 0) / 1e8
            if addr:
                recv_map[addr] = recv_map.get(addr, 0.0) + btc

        if not senders or not recv_map:
            return None

        return {
            "source": "blockchain",
            "hash": str(tx.get("hash", uuid.uuid4().hex[:14])),
            "timestamp": now,
            "senders": senders,
            "receivers": list(recv_map.items()),
            "unit": "BTC",
            "domain_context": "Bitcoin Mainnet Mempool",
        }

    # 2. RIPE RIS Live BGP Routing Update (`ris_message`)
    if source == "ripe_bgp":
        data = payload.get("data", payload)
        path = data.get("path") or []
        if not isinstance(path, list) or len(path) < 2:
            return None

        # Deduplicate consecutive AS prepending (e.g. [174, 174, 13335] -> [174, 13335])
        dedup_path: List[str] = []
        for asn in path:
            s_asn = format_asn(str(asn))
            if not dedup_path or dedup_path[-1] != s_asn:
                dedup_path.append(s_asn)

        if len(dedup_path) < 2:
            return None

        announcements = data.get("announcements") or []
        prefix_count = sum(len(a.get("prefixes") or []) for a in announcements) or 1
        weight = round(max(0.5, float(prefix_count)), 3)

        # Build sequential hop edges along the AS-PATH (originator -> transit -> peer)
        senders = dedup_path[:-1][:4]
        receivers = [(hop, round(weight / max(1, len(dedup_path) - 1), 3)) for hop in dedup_path[1:][:8]]
        return {
            "source": "ripe_bgp",
            "hash": f"bgp_{uuid.uuid4().hex[:10]}",
            "timestamp": now,
            "senders": senders,
            "receivers": receivers,
            "unit": "Prefixes",
            "domain_context": f"BGP AS-PATH ({len(dedup_path)} hops, {prefix_count} prefixes)",
        }

    # 3. Wikimedia EventStreams (`recentchange` filtered to `enwiki`)
    if source == "wikimedia":
        wiki = payload.get("wiki", "enwiki")
        if wiki != "enwiki":
            return None
        user = str(payload.get("user") or "").strip()
        title = str(payload.get("title") or "").strip()
        if not user or not title or title.startswith("Special:"):
            return None

        length_obj = payload.get("length") or {}
        delta_bytes = abs(int(length_obj.get("new", 0) or 0) - int(length_obj.get("old", 0) or 0))
        kb_weight = round(max(0.15, delta_bytes / 1024.0), 3)
        editor_node = f"Editor:{user[:22]}"
        page_node = f"Page:{title[:26]}"

        return {
            "source": "wikimedia",
            "hash": f"wiki_{payload.get('id', uuid.uuid4().hex[:10])}",
            "timestamp": now,
            "senders": [editor_node],
            "receivers": [(page_node, kb_weight)],
            "unit": "KB Δ",
            "domain_context": f"enwiki {payload.get('type', 'edit')} ({kb_weight} KB)",
        }

    # 4. Bluesky Jetstream (`app.bsky.feed.post` reply/mention interaction)
    if source == "bluesky":
        author = str(payload.get("did") or payload.get("author") or "").strip()
        commit = payload.get("commit") or {}
        record = commit.get("record") or payload.get("record") or {}
        if not author or not isinstance(record, dict):
            return None

        targets: List[str] = []
        reply = record.get("reply") or {}
        parent_uri = (reply.get("parent") or {}).get("uri", "")
        if parent_uri.startswith("at://"):
            parts = parent_uri.split("/")
            if len(parts) >= 3 and parts[2] != author:
                targets.append(parts[2])

        for facet in record.get("facets") or []:
            for feat in facet.get("features") or []:
                mentioned = feat.get("did")
                if mentioned and mentioned != author and mentioned not in targets:
                    targets.append(mentioned)

        if not targets:
            return None

        short_author = f"bsky:{author[-10:]}"
        receivers = [(f"bsky:{t[-10:]}", 1.0) for t in targets[:8]]
        return {
            "source": "bluesky",
            "hash": f"bsky_{uuid.uuid4().hex[:10]}",
            "timestamp": now,
            "senders": [short_author],
            "receivers": receivers,
            "unit": "Interactions",
            "domain_context": "Bluesky Social Reply/Mention",
        }

    # 5. GDELT 2.0 Global Knowledge Graph Row
    if source == "gdelt":
        entities = payload.get("entities") or []
        if len(entities) < 2:
            return None
        tone_weight = round(max(0.5, float(payload.get("weight", 1.5))), 3)
        primary = entities[0]
        receivers = [(ent, round(tone_weight / max(1, len(entities) - 1), 3)) for ent in entities[1:7] if ent != primary]
        if not receivers:
            return None
        return {
            "source": "gdelt",
            "hash": f"gdelt_{uuid.uuid4().hex[:10]}",
            "timestamp": now,
            "senders": [primary],
            "receivers": receivers,
            "unit": "Mentions",
            "domain_context": str(payload.get("theme", "GDELT Global Event")),
        }

    return None


def build_domain_synthetic_event(source: str, force_anomaly: bool = False) -> dict:
    """Generates realistic synthetic events tailored to the active domain (for tests & offline mode)."""
    now = time.time()
    source = source if source in SUPPORTED_SOURCES else "blockchain"

    if source == "ripe_bgp":
        tiers = [
            "AS174 (Cogent)", "AS3356 (Lumen)", "AS1299 (Arelion)", "AS2914 (NTT)",
            "AS6939 (Hurricane Electric)", "AS13335 (Cloudflare)", "AS15169 (Google)",
            "AS9498 (Bharti Airtel)", "AS55836 (Reliance Jio)", "AS6453 (TATA Comm)",
        ]
        if force_anomaly:
            # Simulated BGP Route Leak / Prefix Hijack Fan-Out
            leaker = f"AS{random.randint(45000, 64000)} (Edge-ISP)"
            victims = random.sample(tiers, 8)
            return {
                "source": "ripe_bgp",
                "hash": f"bgp_{uuid.uuid4().hex[:10]}",
                "timestamp": now,
                "senders": [leaker],
                "receivers": [(v, round(random.uniform(1.5, 4.0), 2)) for v in victims],
                "unit": "Prefixes",
                "domain_context": "BGP Route Leak Burst",
            }
        a, b, c = random.sample(tiers, 3)
        return {
            "source": "ripe_bgp",
            "hash": f"bgp_{uuid.uuid4().hex[:10]}",
            "timestamp": now,
            "senders": [a, b],
            "receivers": [(b, 1.0), (c, 1.0)],
            "unit": "Prefixes",
            "domain_context": "BGP Path Announcement",
        }

    if source == "wikimedia":
        pages = [
            "Page:Artificial_intelligence", "Page:Bitcoin", "Page:2026_FIFA_World_Cup",
            "Page:Graph_theory", "Page:New_Delhi", "Page:Quantum_computing",
            "Page:OpenAI", "Page:Linux_kernel", "Page:Cybersecurity", "Page:Apache_Spark",
        ]
        if force_anomaly:
            bot = f"Editor:BotSweep_{random.randint(10, 99)}"
            targets = random.sample(pages, 8)
            return {
                "source": "wikimedia",
                "hash": f"wiki_{uuid.uuid4().hex[:10]}",
                "timestamp": now,
                "senders": [bot],
                "receivers": [(p, round(random.uniform(2.5, 8.0), 2)) for p in targets],
                "unit": "KB Δ",
                "domain_context": "Mass Bot Edit Burst",
            }
        ed = f"Editor:User_{random.randint(100, 145)}"
        pg = random.choice(pages)
        return {
            "source": "wikimedia",
            "hash": f"wiki_{uuid.uuid4().hex[:10]}",
            "timestamp": now,
            "senders": [ed],
            "receivers": [(pg, round(random.uniform(0.2, 1.8), 2))],
            "unit": "KB Δ",
            "domain_context": "enwiki Live Edit",
        }

    if source == "bluesky":
        pool = [f"bsky:acct_{i:02d}" for i in range(1, 36)]
        if force_anomaly:
            spammer = f"bsky:swarm_{random.randint(10, 99)}"
            targets = random.sample(pool, 9)
            return {
                "source": "bluesky",
                "hash": f"bsky_{uuid.uuid4().hex[:10]}",
                "timestamp": now,
                "senders": [spammer],
                "receivers": [(t, 1.5) for t in targets],
                "unit": "Interactions",
                "domain_context": "Coordinated Reply Swarm",
            }
        u, v = random.sample(pool, 2)
        return {
            "source": "bluesky",
            "hash": f"bsky_{uuid.uuid4().hex[:10]}",
            "timestamp": now,
            "senders": [u],
            "receivers": [(v, 1.0)],
            "unit": "Interactions",
            "domain_context": "Bluesky Reply",
        }

    if source == "gdelt":
        orgs = [
            "Org:United_Nations", "Org:European_Union", "Org:Reserve_Bank_of_India",
            "Org:World_Bank", "Org:NATO", "Loc:New_Delhi", "Loc:Geneva",
            "Loc:Singapore", "Loc:Tokyo", "Org:IMF", "Org:CERN", "Loc:London",
        ]
        if force_anomaly:
            hub = "Org:Global_Summit_Alert"
            targets = random.sample(orgs, 7)
            return {
                "source": "gdelt",
                "hash": f"gdelt_{uuid.uuid4().hex[:10]}",
                "timestamp": now,
                "senders": [hub],
                "receivers": [(t, round(random.uniform(1.8, 3.5), 2)) for t in targets],
                "unit": "Mentions",
                "domain_context": "Breaking Multi-Entity Event Surge",
            }
        a, b, c = random.sample(orgs, 3)
        return {
            "source": "gdelt",
            "hash": f"gdelt_{uuid.uuid4().hex[:10]}",
            "timestamp": now,
            "senders": [a],
            "receivers": [(b, 1.2), (c, 1.1)],
            "unit": "Mentions",
            "domain_context": "GDELT 2.0 GKG Co-Occurrence",
        }

    # Default: blockchain
    def rand_addr():
        return "bc1q" + uuid.uuid4().hex[:24]

    if force_anomaly:
        sender = rand_addr()
        return {
            "source": "blockchain",
            "hash": uuid.uuid4().hex[:16],
            "timestamp": now,
            "senders": [sender],
            "receivers": [(rand_addr(), round(random.uniform(0.4, 0.95), 4)) for _ in range(11)],
            "unit": "BTC",
            "domain_context": "High Fan-Out Whale Burst",
        }

    sender = rand_addr()
    cnt = random.choices([1, 2, 3], weights=[0.55, 0.3, 0.15])[0]
    return {
        "source": "blockchain",
        "hash": uuid.uuid4().hex[:16],
        "timestamp": now,
        "senders": [sender],
        "receivers": [(rand_addr(), round(random.uniform(0.01, 0.45), 4)) for _ in range(cnt)],
        "unit": "BTC",
        "domain_context": "Bitcoin Mainnet Transfer",
    }


def _fetch_gdelt_top_rows_sync(max_rows: int = 120) -> List[dict]:
    """
    Trade-off neutralizer for GDELT 2.0 GKG:
    Downloads only the latest 15-min GKG update pointer, extracts up to 120 entity rows in RAM,
    and returns lightweight dicts ready for smooth 1.5 events/sec paced streaming.
    """
    req = urllib.request.Request(settings.gdelt_lastupdate_url, headers={"User-Agent": "ARCNET/3.0"})
    with urllib.request.urlopen(req, timeout=6.0) as resp:
        manifest = resp.read().decode("utf-8", errors="ignore")

    gkg_url = None
    for line in manifest.splitlines():
        parts = line.strip().split()
        if len(parts) >= 3 and parts[2].endswith(".gkg.csv.zip"):
            gkg_url = parts[2]
            break

    if not gkg_url:
        return []

    req_zip = urllib.request.Request(gkg_url, headers={"User-Agent": "ARCNET/3.0"})
    with urllib.request.urlopen(req_zip, timeout=10.0) as resp:
        zip_bytes = resp.read()

    rows: List[dict] = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_name = zf.namelist()[0]
        with zf.open(csv_name) as raw_csv:
            wrapper = io.TextIOWrapper(raw_csv, encoding="utf-8", errors="ignore")
            reader = csv.reader(wrapper, delimiter="	")
            for row in reader:
                if len(rows) >= max_rows:
                    break
                if len(row) < 15:
                    continue
                # Col 11 = V1PERSONS, Col 13 = V1ORGANIZATIONS, Col 9 = V1LOCATIONS
                orgs = [x.strip() for x in row[13].split(";") if x.strip()][:3]
                persons = [x.strip() for x in row[11].split(";") if x.strip()][:2]
                locs = [x.split("#")[0].strip() for x in row[9].split(";") if "#" in x][:2]
                entities = (
                    [f"Org:{o[:22]}" for o in orgs]
                    + [f"Person:{p[:20]}" for p in persons]
                    + [f"Loc:{l[:18]}" for l in locs]
                )
                entities = list(dict.fromkeys(entities))
                if len(entities) >= 2:
                    theme = (row[7].split(";")[0] if row[7] else "GLOBAL_EVENT")[:28]
                    rows.append({"entities": entities, "weight": round(1.0 + len(entities) * 0.4, 2), "theme": theme})
    return rows


async def stream_domain_source(
    source_getter: Callable[[], str],
    mode_getter: Callable[[], str],
    browser_relay_event: asyncio.Event,
    on_canonical_event: Callable[[dict], Awaitable[None]],
    on_status: Callable[[str, str], Awaitable[None]],
    stop_event: asyncio.Event,
):
    """
    Master async ingestion loop supporting all 5 data sources with smooth pacing and
    automatic fallback if cloud datacenter IPs are blocked.
    """
    active_src = ""
    while not stop_event.is_set():
        src = source_getter()
        if mode_getter() != "live":
            await asyncio.sleep(0.3)
            continue

        if src != active_src:
            active_src = src
            browser_relay_event.clear()

        try:
            await on_status("connecting", src)

            if src == "blockchain":
                async with websockets.connect(
                    settings.blockchain_ws_url, ping_interval=25, ping_timeout=15, open_timeout=6
                ) as ws:
                    await ws.send(json.dumps({"op": "unconfirmed_sub"}))
                    await on_status("live", "blockchain")
                    while not stop_event.is_set() and source_getter() == "blockchain" and mode_getter() == "live":
                        if browser_relay_event.is_set():
                            await asyncio.sleep(0.4)
                            continue
                        raw = await asyncio.wait_for(ws.recv(), timeout=10.0)
                        data = json.loads(raw)
                        if data.get("op") == "utx":
                            ev = normalize_raw_event("blockchain", data)
                            if ev:
                                await on_canonical_event(ev)
                                await asyncio.sleep(0.28)

            elif src == "ripe_bgp":
                async with websockets.connect(
                    settings.ripe_ris_ws_url, ping_interval=25, ping_timeout=15, open_timeout=7
                ) as ws:
                    # Filter directly to collector rrc21 for a calm, high-signal BGP stream
                    await ws.send(json.dumps({"type": "ris_subscribe", "data": {"host": "rrc21", "type": "UPDATE"}}))
                    await on_status("live", "ripe_bgp")
                    while not stop_event.is_set() and source_getter() == "ripe_bgp" and mode_getter() == "live":
                        raw = await asyncio.wait_for(ws.recv(), timeout=10.0)
                        data = json.loads(raw)
                        if data.get("type") == "ris_message":
                            ev = normalize_raw_event("ripe_bgp", data)
                            if ev:
                                await on_canonical_event(ev)
                                await asyncio.sleep(0.35)

            elif src == "bluesky":
                async with websockets.connect(
                    settings.bluesky_ws_url, ping_interval=25, ping_timeout=15, open_timeout=7
                ) as ws:
                    await on_status("live", "bluesky")
                    while not stop_event.is_set() and source_getter() == "bluesky" and mode_getter() == "live":
                        raw = await asyncio.wait_for(ws.recv(), timeout=10.0)
                        data = json.loads(raw)
                        if data.get("kind") == "commit":
                            ev = normalize_raw_event("bluesky", data)
                            if ev:
                                await on_canonical_event(ev)
                                await asyncio.sleep(0.32)

            elif src == "wikimedia":
                # Stream SSE over standard asyncio HTTP reader (zero extra dependencies)
                import urllib.parse
                parsed = urllib.parse.urlparse(settings.wikimedia_sse_url)
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(parsed.hostname, 443, ssl=True), timeout=7.0
                )
                req_str = "\r\n".join([
                    f"GET {parsed.path} HTTP/1.1",
                    f"Host: {parsed.hostname}",
                    "Accept: text/event-stream",
                    "User-Agent: ARCNET-LSGA/3.0",
                    "",
                    "",
                ])
                writer.write(req_str.encode("utf-8"))
                await writer.drain()
                await on_status("live", "wikimedia")
                try:
                    while not stop_event.is_set() and source_getter() == "wikimedia" and mode_getter() == "live":
                        line_bytes = await asyncio.wait_for(reader.readline(), timeout=12.0)
                        if not line_bytes:
                            break
                        line = line_bytes.decode("utf-8", errors="ignore").strip()
                        if line.startswith("data: "):
                            payload = json.loads(line[6:])
                            ev = normalize_raw_event("wikimedia", payload)
                            if ev:
                                await on_canonical_event(ev)
                                await asyncio.sleep(0.35)
                finally:
                    writer.close()

            elif src == "gdelt":
                gdelt_rows = await asyncio.to_thread(_fetch_gdelt_top_rows_sync, 120)
                if not gdelt_rows:
                    raise RuntimeError("GDELT manifest empty")
                await on_status("live", "gdelt")
                idx = 0
                while not stop_event.is_set() and source_getter() == "gdelt" and mode_getter() == "live":
                    row = gdelt_rows[idx % len(gdelt_rows)]
                    idx += 1
                    ev = normalize_raw_event("gdelt", row)
                    if ev:
                        await on_canonical_event(ev)
                    await asyncio.sleep(0.55)

        except asyncio.CancelledError:
            return
        except Exception as exc:
            print(f"[ARCNET] Source '{src}' upstream notice ({exc}); activating resilient stream.")
            if stop_event.is_set():
                return

            if src in ("blockchain", "ripe_bgp", "bluesky"):
                await on_status("upstream_blocked", src)
                try:
                    await asyncio.wait_for(browser_relay_event.wait(), timeout=6.0)
                    while not stop_event.is_set() and source_getter() == src and mode_getter() == "live":
                        await asyncio.sleep(0.5)
                    continue
                except asyncio.TimeoutError:
                    pass

            # Smooth domain-specific fallback stream so UI never stalls
            await on_status("fallback", src)
            while not stop_event.is_set() and source_getter() == src and mode_getter() == "live":
                if browser_relay_event.is_set():
                    await asyncio.sleep(0.4)
                    continue
                ev = build_domain_synthetic_event(src, force_anomaly=(random.random() < 0.09))
                await on_canonical_event(ev)
                await asyncio.sleep(0.75)
