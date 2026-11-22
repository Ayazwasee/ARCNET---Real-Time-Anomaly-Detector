"""
ARCNET Stage 3 FastAPI Server.

Supports live switching across all 5 graph domains (`blockchain`, `ripe_bgp`, `wikimedia`,
`bluesky`, `gdelt`), Time-Travel Replay, Neo4j Cypher Forensics, Louvain Community Evolution,
Online Isolation Forest ML, Incremental vs. Full Runtime Benchmarks, and Partitioning.
"""

from __future__ import annotations

import asyncio
import json
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from pydantic import BaseModel

from backend.blockchain_graph import (
    DynamicBlockchainGraph,
    build_peel_chain_and_wash_loop_txs,
    build_synthetic_tx,
)
from backend.config import settings
from backend.sources_adapter import (
    SUPPORTED_SOURCES,
    normalize_raw_event,
    stream_domain_source,
)
from backend.storage_neo4j import Neo4jForensicStore

FRONTEND_INDEX = Path(__file__).resolve().parent.parent / "frontend" / "index.html"
RECORDING_FILE = Path(__file__).resolve().parent.parent / settings.recording_path

default_config = {
    "source": settings.default_source,
    "window_seconds": settings.window_seconds,
    "whale_btc_threshold": settings.whale_btc_threshold,
    "ml_score_threshold": settings.ml_score_threshold,
    "partition_count": settings.partition_count,
}
active_engines: Set[DynamicBlockchainGraph] = set()

forensic_store = Neo4jForensicStore(
    uri=settings.neo4j_uri,
    user=settings.neo4j_user,
    password=settings.neo4j_password,
    database=settings.neo4j_database,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await forensic_store.connect()
    yield
    await forensic_store.close()


app = FastAPI(
    title="ARCNET Stage 3 — Multi-Domain Dynamic Graph & Forensic Platform",
    lifespan=lifespan,
)


class ConfigUpdate(BaseModel):
    window_seconds: int = 180
    whale_btc_threshold: float = 2.0
    ml_score_threshold: float = 74.0
    partition_count: int = 4


def apply_config_to_engine(
    engine: DynamicBlockchainGraph,
    window_seconds: int,
    whale_btc_threshold: float,
    ml_score_threshold: float = 74.0,
    partition_count: int = 4,
):
    engine.window_seconds = max(30, min(int(window_seconds), 600))
    engine.whale_btc_threshold = max(0.5, float(whale_btc_threshold))
    engine.ml_detector.ml_score_threshold = max(50.0, min(float(ml_score_threshold), 95.0))
    engine.partitioner.num_partitions = max(2, min(int(partition_count), 8))


def load_replay_batch(source: str) -> List[dict]:
    txs: List[dict] = []
    if RECORDING_FILE.exists():
        try:
            with RECORDING_FILE.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        txs.append(json.loads(line))
        except Exception:
            txs = []

    if len(txs) < 15:
        txs = [build_synthetic_tx(force_anomaly=(i % 8 == 0), source=source) for i in range(26)]
        txs.extend(build_peel_chain_and_wash_loop_txs(source=source))
    return txs


@app.get("/")
async def serve_dashboard():
    return FileResponse(FRONTEND_INDEX)


@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "service": "ARCNET Stage 3",
        "sources": list(SUPPORTED_SOURCES.keys()),
        "config": default_config,
        "storage": forensic_store.status_summary(),
    }


@app.post("/api/config")
async def update_config_http(cfg: ConfigUpdate):
    default_config["window_seconds"] = max(30, min(int(cfg.window_seconds), 600))
    default_config["whale_btc_threshold"] = max(0.5, float(cfg.whale_btc_threshold))
    default_config["ml_score_threshold"] = max(50.0, min(float(cfg.ml_score_threshold), 95.0))
    default_config["partition_count"] = max(2, min(int(cfg.partition_count), 8))

    for eng in list(active_engines):
        apply_config_to_engine(
            eng,
            default_config["window_seconds"],
            default_config["whale_btc_threshold"],
            default_config["ml_score_threshold"],
            default_config["partition_count"],
        )
    return {"status": "updated", "config": default_config}


@app.websocket("/ws/stream")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()

    engine = DynamicBlockchainGraph(
        window_seconds=default_config["window_seconds"],
        max_nodes=settings.max_nodes,
        whale_btc_threshold=default_config["whale_btc_threshold"],
        fanout_threshold=settings.fanout_threshold,
        fanin_threshold=settings.fanin_threshold,
        z_threshold=settings.z_threshold,
        ml_score_threshold=default_config["ml_score_threshold"],
        partition_count=default_config["partition_count"],
        recording_path=str(RECORDING_FILE),
    )
    engine.switch_domain(default_config["source"])
    active_engines.add(engine)

    unread_alerts: List[dict] = []
    inbox_alerts: List[dict] = []

    send_lock = asyncio.Lock()
    stop_event = asyncio.Event()
    browser_relay_active = asyncio.Event()
    stream_mode = {"mode": "live", "speed": 1.0}

    async def send_safe(payload: dict):
        async with send_lock:
            await ws.send_json(payload)

    async def process_and_emit_event(ev: dict):
        update = engine.add_canonical_event(ev)
        if not update:
            return

        raw_edges = update.pop("raw_edges", [])
        if raw_edges:
            forensic_store.record_edges_local(raw_edges)

        for alert in update.get("alerts", []):
            unread_alerts.insert(0, alert)
            if len(unread_alerts) > 40:
                unread_alerts.pop()
            ego_edges = engine.get_ego_edges(alert["primary_node"], radius=2)
            asyncio.create_task(forensic_store.persist_anomaly(alert, ego_edges))

        await send_safe(update)

    async def on_source_status(status: str, source_name: str):
        await send_safe({"type": "status", "status": status, "source": source_name})

    async def replay_loop():
        idx = 0
        cached_src = ""
        batch: List[dict] = []
        while not stop_event.is_set():
            if stream_mode["mode"] != "replay":
                await asyncio.sleep(0.25)
                continue
            if cached_src != engine.active_source or not batch:
                cached_src = engine.active_source
                batch = load_replay_batch(cached_src)
                idx = 0
            ev = dict(batch[idx % len(batch)])
            ev["_override_ts"] = time.time()
            idx += 1
            await process_and_emit_event(ev)
            speed = max(0.5, float(stream_mode["speed"]))
            await asyncio.sleep(max(0.15, 0.7 / speed))

    await send_safe(
        {
            "type": "init",
            "status": "connecting",
            "sources": SUPPORTED_SOURCES,
            "snapshot": engine.get_snapshot(),
            "unread": unread_alerts,
            "inbox": inbox_alerts,
            "storage": forensic_store.status_summary(),
        }
    )

    upstream_task = asyncio.create_task(
        stream_domain_source(
            source_getter=lambda: engine.active_source,
            mode_getter=lambda: stream_mode["mode"],
            browser_relay_event=browser_relay_active,
            on_canonical_event=process_and_emit_event,
            on_status=on_source_status,
            stop_event=stop_event,
        )
    )
    replay_task = asyncio.create_task(replay_loop())

    try:
        while True:
            raw_text = await ws.receive_text()
            try:
                cmd = json.loads(raw_text)
            except json.JSONDecodeError:
                continue

            action = cmd.get("action")

            if action == "switch_source":
                new_src = str(cmd.get("source", "blockchain"))
                if new_src in SUPPORTED_SOURCES:
                    default_config["source"] = new_src
                    browser_relay_active.clear()
                    engine.switch_domain(new_src)
                    await send_safe(
                        {
                            "type": "source_switched",
                            "source": new_src,
                            "source_meta": SUPPORTED_SOURCES[new_src],
                            "snapshot": engine.get_snapshot(),
                        }
                    )

            elif action == "ingest_tx":
                if stream_mode["mode"] != "live":
                    continue
                if not browser_relay_active.is_set():
                    browser_relay_active.set()
                    await send_safe({"type": "status", "status": "live", "source": "browser_relay"})
                raw_tx = cmd.get("tx")
                src_hint = str(cmd.get("source", engine.active_source))
                if isinstance(raw_tx, dict) and src_hint == engine.active_source:
                    ev = normalize_raw_event(src_hint, raw_tx)
                    if ev:
                        await process_and_emit_event(ev)

            elif action == "simulate_anomaly":
                ev = build_synthetic_tx(force_anomaly=True, source=engine.active_source)
                await process_and_emit_event(ev)

            elif action == "simulate_peel_chain":
                for ev in build_peel_chain_and_wash_loop_txs(source=engine.active_source):
                    await process_and_emit_event(ev)
                    await asyncio.sleep(0.08)

            elif action == "mark_read":
                alert_id = cmd.get("alert_id")
                target = None
                for idx, item in enumerate(unread_alerts):
                    if item["id"] == alert_id:
                        target = unread_alerts.pop(idx)
                        break
                if target:
                    target["read_at"] = time.time()
                    inbox_alerts.insert(0, target)
                    if len(inbox_alerts) > 100:
                        inbox_alerts.pop()
                await send_safe(
                    {
                        "type": "alerts_synced",
                        "unread": unread_alerts,
                        "inbox": inbox_alerts,
                    }
                )

            elif action == "update_config":
                win = cmd.get("window_seconds", engine.window_seconds)
                whale = cmd.get("whale_btc_threshold", engine.whale_btc_threshold)
                ml_thr = cmd.get("ml_score_threshold", engine.ml_detector.ml_score_threshold)
                parts = cmd.get("partition_count", engine.partitioner.num_partitions)
                apply_config_to_engine(engine, win, whale, ml_thr, parts)
                default_config["window_seconds"] = engine.window_seconds
                default_config["whale_btc_threshold"] = engine.whale_btc_threshold
                default_config["ml_score_threshold"] = engine.ml_detector.ml_score_threshold
                default_config["partition_count"] = engine.partitioner.num_partitions

                report = engine.partitioner.compute(engine.G)
                await send_safe(
                    {
                        "type": "config_updated",
                        "stats": engine.get_stats(time.time()),
                        "partitioning": report,
                        "ml": engine.ml_detector.get_summary(),
                    }
                )

            elif action == "get_snapshot":
                await send_safe({"type": "snapshot", "snapshot": engine.get_snapshot()})

            elif action == "run_forensics":
                wallet_q = cmd.get("wallet", "")
                peels = await forensic_store.find_peel_chains(engine.G)
                loops = await forensic_store.find_wash_loops(engine.G)
                history = (
                    await forensic_store.get_wallet_history(wallet_q, engine.G)
                    if wallet_q
                    else None
                )
                await send_safe(
                    {
                        "type": "forensics_result",
                        "storage": forensic_store.status_summary(),
                        "peel_chains": peels,
                        "wash_loops": loops,
                        "wallet_history": history,
                    }
                )

            elif action == "get_partition_report":
                k = cmd.get("k", engine.partitioner.num_partitions)
                report = engine.partitioner.compute(engine.G, k=k)
                await send_safe(
                    {
                        "type": "partition_report",
                        "partitioning": report,
                        "nodes": [engine._serialize_node(n) for n in engine.G.nodes],
                    }
                )

            elif action == "set_mode":
                mode = cmd.get("mode", "live")
                speed = float(cmd.get("speed", 1.0))
                stream_mode["mode"] = "replay" if mode == "replay" else "live"
                stream_mode["speed"] = max(0.5, min(speed, 5.0))
                await send_safe(
                    {
                        "type": "mode_updated",
                        "mode": stream_mode["mode"],
                        "speed": stream_mode["speed"],
                    }
                )

    except WebSocketDisconnect:
        pass
    finally:
        active_engines.discard(engine)
        stop_event.set()
        upstream_task.cancel()
        replay_task.cancel()
