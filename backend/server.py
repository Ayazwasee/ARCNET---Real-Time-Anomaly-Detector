import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from pydantic import BaseModel

from backend.blockchain_graph import (
    DynamicBlockchainGraph,
    build_synthetic_tx,
    stream_blockchain_to_graph,
)

FRONTEND_INDEX = Path(__file__).resolve().parent.parent / "frontend" / "index.html"

graph_engine = DynamicBlockchainGraph(
    window_seconds=180,
    max_nodes=110,
    whale_btc_threshold=5.0,
    fanout_threshold=10,
)

connected_clients: Set[WebSocket] = set()
unread_alerts: List[dict] = []
inbox_alerts: List[dict] = []
feed_status: str = "connecting"
stop_event = asyncio.Event()


async def broadcast(payload: dict):
    dead = []
    for ws in list(connected_clients):
        try:
            await ws.send_json(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        connected_clients.discard(ws)


async def handle_graph_update(update: dict):
    for alert in update.get("alerts", []):
        unread_alerts.insert(0, alert)
        if len(unread_alerts) > 40:
            unread_alerts.pop()
    await broadcast(update)


async def handle_status_change(status: str):
    global feed_status
    feed_status = status
    await broadcast({"type": "status", "status": status})


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(
        stream_blockchain_to_graph(
            graph_engine=graph_engine,
            on_update=handle_graph_update,
            on_status=handle_status_change,
            stop_event=stop_event,
        )
    )
    yield
    stop_event.set()
    task.cancel()


app = FastAPI(title="ARCNET — Real-Time Dynamic Graph Anomaly Detector", lifespan=lifespan)


class ConfigUpdate(BaseModel):
    window_seconds: int = 180
    whale_btc_threshold: float = 5.0


@app.get("/")
async def serve_dashboard():
    return FileResponse(FRONTEND_INDEX)


@app.get("/api/snapshot")
async def get_graph_snapshot():
    """Returns the latest live graph state when resuming from Focus/Paused mode."""
    return graph_engine.get_snapshot()


@app.post("/api/alerts/{alert_id}/read")
async def mark_alert_read(alert_id: str):
    """Moves an active side-panel anomaly into the permanent Anomaly Inbox."""
    target = None
    for idx, item in enumerate(unread_alerts):
        if item["id"] == alert_id:
            target = unread_alerts.pop(idx)
            break

    if target:
        target["read_at"] = asyncio.get_event_loop().time()
        inbox_alerts.insert(0, target)
        if len(inbox_alerts) > 100:
            inbox_alerts.pop()
        await broadcast(
            {
                "type": "alerts_synced",
                "unread": unread_alerts,
                "inbox": inbox_alerts,
            }
        )
        return {"status": "moved_to_inbox", "alert": target}
    return {"status": "not_found"}


@app.post("/api/simulate-anomaly")
async def simulate_anomaly():
    """Injects a Fan-Out + Whale anomaly into the live graph for instant testing/demos."""
    tx = build_synthetic_tx(force_anomaly=True)
    update = graph_engine.add_transaction(tx)
    if update:
        await handle_graph_update(update)
    return {"status": "injected", "alerts_triggered": len(update.get("alerts", []))}


@app.post("/api/config")
async def update_config(cfg: ConfigUpdate):
    graph_engine.window_seconds = max(30, min(cfg.window_seconds, 600))
    graph_engine.whale_btc_threshold = max(0.5, cfg.whale_btc_threshold)
    return {"status": "updated", "stats": graph_engine.get_stats(0)}


@app.websocket("/ws/stream")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    connected_clients.add(ws)
    try:
        await ws.send_json(
            {
                "type": "init",
                "status": feed_status,
                "snapshot": graph_engine.get_snapshot(),
                "unread": unread_alerts,
                "inbox": inbox_alerts,
            }
        )
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        connected_clients.discard(ws)
