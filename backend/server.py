"""
ARCNET Phase 2 FastAPI Server.

Works seamlessly on:
- Localhost / Codespaces (`python3 -m uvicorn backend.server:app --reload`)
- Docker Compose (with local Neo4j + ARCNET)
- Vercel Fluid Compute (`pyproject.toml` entrypoint `backend.server:app`)
"""

from __future__ import annotations

import asyncio
import json
import random
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List, Set

import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from pydantic import BaseModel

from backend.blockchain_graph import (
    BLOCKCHAIN_WS_URL,
    DynamicBlockchainGraph,
    build_peel_chain_and_wash_loop_txs,
    build_synthetic_tx,
)
from backend.config import settings
from backend.storage_neo4j import Neo4jForensicStore

FRONTEND_INDEX = Path(__file__).resolve().parent.parent / "frontend" / "index.html"
RECORDING_FILE = Path(__file__).resolve().parent.parent / settings.recording_path

default_config = {
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
    title="ARCNET Phase 2 — Real-Time Dynamic Graph & Forensic Anomaly Platform",
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


def load_replay_batch() -> List[dict]:
    """Loads recorded JSONL transactions if present; otherwise generates a rich demo replay set."""
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
        txs = [build_synthetic_tx(force_anomaly=(i % 9 == 0)) for i in range(28)]
        txs.extend(build_peel_chain_and_wash_loop_txs())
    return txs


@app.get("/")
async def serve_dashboard():
    return FileResponse(FRONTEND_INDEX)


@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "service": "ARCNET Phase 2",
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
    active_engines.add(engine)

    unread_alerts: List[dict] = []
    inbox_alerts: List[dict] = []

    send_lock = asyncio.Lock()
    stop_event = asyncio.Event()
    browser_relay_active = asyncio.Event()
    stream_mode = {"mode": "live", "speed": 1.0}  # "live" | "replay"

    async def send_safe(payload: dict):
        async with send_lock:
            await ws.send_json(payload)

    async def process_and_emit_tx(tx: dict):
        update = engine.add_transaction(tx)
        if not update:
            return

        # Mirror edges locally for fast forensic fallback
        raw_edges = update.pop("raw_edges", [])
        if raw_edges:
            forensic_store.record_edges_local(raw_edges)

        for alert in update.get("alerts", []):
            unread_alerts.insert(0, alert)
            if len(unread_alerts) > 40:
                unread_alerts.pop()
            # Persist flagged anomaly + 2-hop ego-network to Neo4j asynchronously
            ego_edges = engine.get_ego_edges(alert["primary_node"], radius=2)
            asyncio.create_task(forensic_store.persist_anomaly(alert, ego_edges))

        await send_safe(update)

    async def server_upstream_loop():
        try:
            await send_safe({"type": "status", "status": "connecting"})
            async with websockets.connect(
                BLOCKCHAIN_WS_URL,
                ping_interval=25,
                ping_timeout=15,
                open_timeout=6,
            ) as upstream_ws:
                await upstream_ws.send(json.dumps({"op": "unconfirmed_sub"}))
                if browser_relay_active.is_set():
                    return
                await send_safe({"type": "status", "status": "live", "source": "server"})

                while not stop_event.is_set() and not browser_relay_active.is_set():
                    if stream_mode["mode"] != "live":
                        await asyncio.sleep(0.3)
                        continue
                    raw_msg = await asyncio.wait_for(upstream_ws.recv(), timeout=10.0)
                    if browser_relay_active.is_set() or stream_mode["mode"] != "live":
                        continue
                    data = json.loads(raw_msg)
                    if data.get("op") != "utx":
                        continue
                    await process_and_emit_tx(data.get("x", {}))
                    await asyncio.sleep(0.25)

        except asyncio.CancelledError:
            return
        except Exception as exc:
            print(f"[ARCNET] Server upstream unavailable ({exc}); requesting browser relay.")
            if stop_event.is_set() or browser_relay_active.is_set():
                return

            await send_safe({"type": "status", "status": "upstream_blocked"})
            try:
                await asyncio.wait_for(browser_relay_active.wait(), timeout=8.0)
                return
            except asyncio.TimeoutError:
                pass

            await send_safe({"type": "status", "status": "fallback"})
            while not stop_event.is_set() and not browser_relay_active.is_set():
                if stream_mode["mode"] != "live":
                    await asyncio.sleep(0.3)
                    continue
                tx = build_synthetic_tx(force_anomaly=(random.random() < 0.08))
                await process_and_emit_tx(tx)
                await asyncio.sleep(0.85)

    async def replay_loop():
        """Milestone 4: Time-Travel Stream Replay loop (1x / 2x / 5x)."""
        batch = load_replay_batch()
        idx = 0
        while not stop_event.is_set():
            if stream_mode["mode"] != "replay":
                await asyncio.sleep(0.25)
                continue
            tx = dict(batch[idx % len(batch)])
            tx["_override_ts"] = time.time()
            idx += 1
            await process_and_emit_tx(tx)
            speed = max(0.5, float(stream_mode["speed"]))
            await asyncio.sleep(max(0.12, 0.75 / speed))

    await send_safe(
        {
            "type": "init",
            "status": "connecting",
            "snapshot": engine.get_snapshot(),
            "unread": unread_alerts,
            "inbox": inbox_alerts,
            "storage": forensic_store.status_summary(),
        }
    )

    upstream_task = asyncio.create_task(server_upstream_loop())
    replay_task = asyncio.create_task(replay_loop())

    try:
        while True:
            raw_text = await ws.receive_text()
            try:
                cmd = json.loads(raw_text)
            except json.JSONDecodeError:
                continue

            action = cmd.get("action")

            if action == "ingest_tx":
                if stream_mode["mode"] != "live":
                    continue
                if not browser_relay_active.is_set():
                    browser_relay_active.set()
                    if not upstream_task.done():
                        upstream_task.cancel()
                    await send_safe(
                        {"type": "status", "status": "live", "source": "browser_relay"}
                    )
                tx = cmd.get("tx")
                if isinstance(tx, dict):
                    await process_and_emit_tx(tx)

            elif action == "simulate_anomaly":
                tx = build_synthetic_tx(force_anomaly=True)
                await process_and_emit_tx(tx)

            elif action == "simulate_peel_chain":
                for tx in build_peel_chain_and_wash_loop_txs():
                    await process_and_emit_tx(tx)
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

            elif action == "toggle_recording":
                engine.recording_enabled = bool(cmd.get("enabled", False))
                await send_safe(
                    {
                        "type": "recording_status",
                        "enabled": engine.recording_enabled,
                        "recorded_count": engine._recorded_count,
                    }
                )

    except WebSocketDisconnect:
        pass
    finally:
        active_engines.discard(engine)
        stop_event.set()
        upstream_task.cancel()
        replay_task.cancel()
