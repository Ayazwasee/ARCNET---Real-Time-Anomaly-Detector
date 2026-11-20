import asyncio
import json
import random
import time
from pathlib import Path
from typing import List

import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

from backend.blockchain_graph import (
    BLOCKCHAIN_WS_URL,
    DynamicBlockchainGraph,
    build_synthetic_tx,
)

FRONTEND_INDEX = Path(__file__).resolve().parent.parent / "frontend" / "index.html"

app = FastAPI(title="ARCNET — Real-Time Dynamic Graph Anomaly Detector")


@app.get("/")
async def serve_dashboard():
    return FileResponse(FRONTEND_INDEX)


@app.get("/api/health")
async def health():
    return {"status": "ok", "service": "ARCNET"}


@app.websocket("/ws/stream")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()

    # Session-pinned dynamic graph engine (prevents Vercel multi-instance state splits)
    engine = DynamicBlockchainGraph(
        window_seconds=180,
        max_nodes=110,
        whale_btc_threshold=5.0,
        fanout_threshold=10,
    )
    unread_alerts: List[dict] = []
    inbox_alerts: List[dict] = []

    send_lock = asyncio.Lock()
    stop_event = asyncio.Event()
    browser_relay_active = asyncio.Event()

    async def send_safe(payload: dict):
        async with send_lock:
            await ws.send_json(payload)

    async def process_and_emit_tx(tx: dict):
        update = engine.add_transaction(tx)
        if not update:
            return
        for alert in update.get("alerts", []):
            unread_alerts.insert(0, alert)
            if len(unread_alerts) > 40:
                unread_alerts.pop()
        await send_safe(update)

    async def server_upstream_loop():
        """
        Primary path: connect directly from backend to Blockchain.com.
        If cloud/Vercel IP is blocked or times out, signal the browser to relay
        real live mainnet transactions into this pinned Python graph engine.
        """
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
                    raw_msg = await asyncio.wait_for(upstream_ws.recv(), timeout=10.0)
                    if browser_relay_active.is_set():
                        return
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

            # Tell frontend to open direct wss://ws.blockchain.info/inv relay
            await send_safe({"type": "status", "status": "upstream_blocked"})

            # Give browser relay 8 seconds to start feeding real mainnet transactions
            try:
                await asyncio.wait_for(browser_relay_active.wait(), timeout=8.0)
                return  # Browser relay took over with real mainnet data
            except asyncio.TimeoutError:
                pass

            # Only if BOTH server and browser are blocked (e.g., offline/firewall), run fallback
            await send_safe({"type": "status", "status": "fallback"})
            while not stop_event.is_set() and not browser_relay_active.is_set():
                tx = build_synthetic_tx(force_anomaly=(random.random() < 0.08))
                await process_and_emit_tx(tx)
                await asyncio.sleep(0.85)

    # Send initial state immediately
    await send_safe(
        {
            "type": "init",
            "status": "connecting",
            "snapshot": engine.get_snapshot(),
            "unread": unread_alerts,
            "inbox": inbox_alerts,
        }
    )

    upstream_task = asyncio.create_task(server_upstream_loop())

    try:
        while True:
            raw_text = await ws.receive_text()
            try:
                cmd = json.loads(raw_text)
            except json.JSONDecodeError:
                continue

            action = cmd.get("action")

            if action == "ingest_tx":
                # Live mainnet tx relayed from user's browser directly into Python NetworkX engine
                if not browser_relay_active.is_set():
                    browser_relay_active.set()
                    if not upstream_task.done():
                        upstream_task.cancel()
                    await send_safe({"type": "status", "status": "live", "source": "browser_relay"})

                tx = cmd.get("tx")
                if isinstance(tx, dict):
                    await process_and_emit_tx(tx)

            elif action == "simulate_anomaly":
                tx = build_synthetic_tx(force_anomaly=True)
                await process_and_emit_tx(tx)

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
                win = int(cmd.get("window_seconds", engine.window_seconds))
                whale = float(cmd.get("whale_btc_threshold", engine.whale_btc_threshold))
                engine.window_seconds = max(30, min(win, 600))
                engine.whale_btc_threshold = max(0.5, whale)
                await send_safe(
                    {"type": "config_updated", "stats": engine.get_stats(time.time())}
                )

            elif action == "get_snapshot":
                await send_safe({"type": "snapshot", "snapshot": engine.get_snapshot()})

    except WebSocketDisconnect:
        pass
    finally:
        stop_event.set()
        upstream_task.cancel()
