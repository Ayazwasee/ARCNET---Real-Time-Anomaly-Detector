import asyncio
import json
import math
import random
import statistics
import time
import uuid
from collections import deque
from typing import Awaitable, Callable, Dict, List, Optional, Set, Tuple

import networkx as nx
import websockets

BLOCKCHAIN_WS_URL = "https://www.blockchain.com/explorer"
SATOSHI_PER_BTC = 1e8


class DynamicBlockchainGraph:
    """
    Self-updating directed weighted graph for real-time Bitcoin transactions.
    Uses adjacency lists (NetworkX DiGraph) with sliding-window pruning
    and local 1-hop incremental anomaly scoring.
    """

    def __init__(
        self,
        window_seconds: int = 180,
        max_nodes: int = 110,
        whale_btc_threshold: float = 5.0,
        fanout_threshold: int = 10,
        z_threshold: float = 2.8,
    ):
        self.G = nx.DiGraph()
        self.window_seconds = window_seconds
        self.max_nodes = max_nodes
        self.whale_btc_threshold = whale_btc_threshold
        self.fanout_threshold = fanout_threshold
        self.z_threshold = z_threshold

        self.tx_timestamps: deque = deque(maxlen=120)
        self.recent_alert_cooldown: Dict[str, float] = {}  # Prevent duplicate spam per node
        self.total_tx_processed = 0

    @staticmethod
    def short_addr(addr: str) -> str:
        if len(addr) <= 12:
            return addr
        return f"{addr[:6]}...{addr[-4:]}"

    def _is_on_cooldown(self, key: str, cooldown_sec: float = 25.0) -> bool:
        now = time.time()
        last = self.recent_alert_cooldown.get(key, 0.0)
        if now - last < cooldown_sec:
            return True
        self.recent_alert_cooldown[key] = now
        return False

    def _check_cluster_bridge(self, senders: List[str], receivers: List[str]) -> Optional[Tuple[str, str, int, int]]:
        """
        Module 4 (Connectivity): Check BEFORE adding edges whether this tx merges
        two separate active wallet clusters of size >= 3.
        """
        if self.G.number_of_nodes() < 8:
            return None
        undirected = self.G.to_undirected(as_view=True)
        for u in senders[:3]:
            if u not in undirected:
                continue
            comp_u = nx.node_connected_component(undirected, u)
            if len(comp_u) < 3:
                continue
            for v in receivers[:5]:
                if v in undirected and v not in comp_u:
                    comp_v = nx.node_connected_component(undirected, v)
                    if len(comp_v) >= 3:
                        return (u, v, len(comp_u), len(comp_v))
        return None

    def add_transaction(self, tx: dict) -> dict:
        """
        Ingests a single Blockchain.com 'utx' transaction object, updates the
        dynamic graph, prunes expired edges, and runs local anomaly detection.
        """
        now = time.time()
        tx_hash = tx.get("hash", uuid.uuid4().hex[:16])

        # Safely extract sender and receiver addresses
        senders_raw: List[Tuple[str, float]] = []
        for inp in tx.get("inputs", []):
            prev = inp.get("prev_out") or {}
            addr = prev.get("addr")
            val_btc = (prev.get("value", 0) or 0) / SATOSHI_PER_BTC
            if addr:
                senders_raw.append((addr, val_btc))

        receivers_raw: List[Tuple[str, float]] = []
        for out in tx.get("out", []):
            addr = out.get("addr")
            val_btc = (out.get("value", 0) or 0) / SATOSHI_PER_BTC
            if addr:
                receivers_raw.append((addr, val_btc))

        if not senders_raw or not receivers_raw:
            return {}

        # Deduplicate while preserving order & track raw fan-in / fan-out counts
        sender_addrs = list(dict.fromkeys([s[0] for s in senders_raw]))
        receiver_map: Dict[str, float] = {}
        for addr, btc in receivers_raw:
            receiver_map[addr] = receiver_map.get(addr, 0.0) + btc
        receiver_addrs = list(receiver_map.keys())

        raw_in_count = len(sender_addrs)
        raw_out_count = len(receiver_addrs)
        total_btc = sum(receiver_map.values())

        # Check structural cluster bridge BEFORE adding new edges
        bridge_info = self._check_cluster_bridge(sender_addrs, receiver_addrs)

        # Cap edges added per transaction to prevent bipartite visual explosion
        # Primary sender links to top receivers; extra senders link to primary receiver
        capped_senders = sender_addrs[:4]
        capped_receivers = sorted(
            receiver_addrs, key=lambda a: receiver_map[a], reverse=True
        )[:12]

        touched_nodes: Set[str] = set()
        new_edges_payload: List[dict] = []

        primary_sender = capped_senders[0]
        for r in capped_receivers:
            if r == primary_sender:
                continue
            btc_amt = round(receiver_map[r], 6)
            self._upsert_edge(primary_sender, r, btc_amt, now, tx_hash)
            touched_nodes.update([primary_sender, r])
            new_edges_payload.append(
                {
                    "id": f"{primary_sender}__{r}",
                    "source": primary_sender,
                    "target": r,
                    "weight": btc_amt,
                    "tx_hash": tx_hash[:10],
                }
            )

        # Link additional senders to top receiver for consolidation patterns
        if len(capped_senders) > 1 and capped_receivers:
            primary_receiver = capped_receivers[0]
            for s in capped_senders[1:]:
                if s == primary_receiver:
                    continue
                btc_amt = round(receiver_map[primary_receiver] / len(capped_senders), 6)
                self._upsert_edge(s, primary_receiver, btc_amt, now, tx_hash)
                touched_nodes.update([s, primary_receiver])
                new_edges_payload.append(
                    {
                        "id": f"{s}__{primary_receiver}",
                        "source": s,
                        "target": primary_receiver,
                        "weight": btc_amt,
                        "tx_hash": tx_hash[:10],
                    }
                )

        self.total_tx_processed += 1
        self.tx_timestamps.append(now)

        # Prune expired edges & enforce smooth visual node cap
        removed_nodes, removed_edges = self.prune_expired(now)

        # Run incremental local anomaly detection on touched nodes
        alerts = self._detect_anomalies(
            primary_sender=primary_sender,
            capped_receivers=capped_receivers,
            raw_in_count=raw_in_count,
            raw_out_count=raw_out_count,
            total_btc=total_btc,
            tx_hash=tx_hash,
            bridge_info=bridge_info,
            now=now,
        )

        # Mark flagged nodes/edges inside the graph
        for alert in alerts:
            n_id = alert["primary_node"]
            if n_id in self.G.nodes:
                self.G.nodes[n_id]["is_anomaly"] = True
                self.G.nodes[n_id]["anomaly_type"] = alert["type"]

        return {
            "type": "graph_update",
            "timestamp": now,
            "added_nodes": [self._serialize_node(n) for n in touched_nodes if n in self.G],
            "added_edges": [
                self._serialize_edge(e["source"], e["target"])
                for e in new_edges_payload
                if self.G.has_edge(e["source"], e["target"])
            ],
            "removed_nodes": removed_nodes,
            "removed_edges": removed_edges,
            "alerts": alerts,
            "stats": self.get_stats(now),
            "tx_summary": {
                "hash": tx_hash[:12],
                "sender": self.short_addr(primary_sender),
                "outputs": raw_out_count,
                "btc": round(total_btc, 4),
                "is_anomaly": len(alerts) > 0,
            },
        }

    def _upsert_edge(self, u: str, v: str, btc: float, now: float, tx_hash: str):
        for node_id in (u, v):
            if node_id not in self.G:
                self.G.add_node(
                    node_id,
                    label=self.short_addr(node_id),
                    first_seen=now,
                    last_seen=now,
                    total_in_btc=0.0,
                    total_out_btc=0.0,
                    tx_count=0,
                    is_anomaly=False,
                    anomaly_type=None,
                )
            self.G.nodes[node_id]["last_seen"] = now
            self.G.nodes[node_id]["tx_count"] += 1

        self.G.nodes[u]["total_out_btc"] += btc
        self.G.nodes[v]["total_in_btc"] += btc

        if self.G.has_edge(u, v):
            self.G[u][v]["weight"] += btc
            self.G[u][v]["count"] += 1
            self.G[u][v]["timestamp"] = now
            self.G[u][v]["tx_hash"] = tx_hash
        else:
            self.G.add_edge(
                u,
                v,
                weight=btc,
                count=1,
                timestamp=now,
                tx_hash=tx_hash,
            )

    def prune_expired(self, now: float) -> Tuple[List[str], List[str]]:
        """Removes edges outside the sliding time window and enforces node cap."""
        cutoff = now - self.window_seconds
        expired_edges = [
            (u, v) for u, v, d in self.G.edges(data=True) if d.get("timestamp", 0) < cutoff
        ]
        removed_edge_ids = [f"{u}__{v}" for u, v in expired_edges]
        self.G.remove_edges_from(expired_edges)

        # Remove isolated nodes
        isolated = [n for n in list(self.G.nodes) if self.G.degree(n) == 0]
        self.G.remove_nodes_from(isolated)
        removed_nodes = list(isolated)

        # Soft cap on total active nodes (drop oldest non-anomalous nodes first)
        if self.G.number_of_nodes() > self.max_nodes:
            excess = self.G.number_of_nodes() - self.max_nodes
            candidates = sorted(
                self.G.nodes(data=True),
                key=lambda item: (item[1].get("is_anomaly", False), item[1].get("last_seen", 0)),
            )
            for node_id, _ in candidates[:excess]:
                for nbr in list(self.G.successors(node_id)):
                    removed_edge_ids.append(f"{node_id}__{nbr}")
                for nbr in list(self.G.predecessors(node_id)):
                    removed_edge_ids.append(f"{nbr}__{node_id}")
                self.G.remove_node(node_id)
                removed_nodes.append(node_id)

        return removed_nodes, list(set(removed_edge_ids))

    def _detect_anomalies(
        self,
        primary_sender: str,
        capped_receivers: List[str],
        raw_in_count: int,
        raw_out_count: int,
        total_btc: float,
        tx_hash: str,
        bridge_info: Optional[Tuple[str, str, int, int]],
        now: float,
    ) -> List[dict]:
        """Incremental 1-hop LSGA anomaly evaluation."""
        alerts: List[dict] = []
        undirected = self.G.to_undirected(as_view=True)

        # Rolling degree statistics across active graph
        out_degrees = [d for _, d in self.G.out_degree()]
        mean_out = statistics.mean(out_degrees) if out_degrees else 1.0
        std_out = statistics.stdev(out_degrees) if len(out_degrees) >= 5 else 1.0

        sender_out_deg = self.G.out_degree(primary_sender) if primary_sender in self.G else 0
        sender_z = (sender_out_deg - mean_out) / (std_out + 1e-5)
        sender_clustering = (
            round(nx.clustering(undirected, primary_sender), 3)
            if primary_sender in undirected
            else 0.0
        )

        # 1. Fan-Out Star Burst (Module 5: Degree Centrality + Low Clustering)
        if (
            raw_out_count >= self.fanout_threshold
            or (sender_out_deg >= 6 and sender_z >= self.z_threshold and sender_clustering < 0.1)
        ) and not self._is_on_cooldown(f"fanout:{primary_sender}"):
            score = min(99, int(68 + min(raw_out_count, 25) + max(0, sender_z * 3)))
            alerts.append(
                {
                    "id": uuid.uuid4().hex[:10],
                    "type": "Fan-Out Star Burst",
                    "severity": "high" if raw_out_count >= 15 else "medium",
                    "score": score,
                    "timestamp": now,
                    "primary_node": primary_sender,
                    "short_node": self.short_addr(primary_sender),
                    "tx_hash": tx_hash,
                    "summary": (
                        f"Wallet {self.short_addr(primary_sender)} rapidly dispersed "
                        f"{total_btc:.4f} BTC across {raw_out_count} distinct destination wallets "
                        f"in a star topology (local clustering = {sender_clustering})."
                    ),
                    "explanation": (
                        "In dynamic graph analysis, a sudden spike in Out-Degree Centrality "
                        "paired with near-zero local clustering indicates automated batch payouts, "
                        "crypto-mixing distribution, or a dust attack."
                    ),
                    "metrics": {
                        "Out-Degree (Raw)": raw_out_count,
                        "Degree Z-Score": round(sender_z, 2),
                        "Clustering Coeff": sender_clustering,
                        "Volume (BTC)": round(total_btc, 4),
                    },
                }
            )

        # 2. Whale Volume Spike (Module 1: Weighted Graph Outlier)
        if total_btc >= self.whale_btc_threshold and not self._is_on_cooldown(
            f"whale:{primary_sender}"
        ):
            score = min(99, int(72 + math.log1p(total_btc) * 8))
            alerts.append(
                {
                    "id": uuid.uuid4().hex[:10],
                    "type": "Whale Volume Spike",
                    "severity": "high" if total_btc >= self.whale_btc_threshold * 3 else "medium",
                    "score": score,
                    "timestamp": now,
                    "primary_node": primary_sender,
                    "short_node": self.short_addr(primary_sender),
                    "tx_hash": tx_hash,
                    "summary": (
                        f"High-value transfer of {total_btc:.4f} BTC initiated by "
                        f"{self.short_addr(primary_sender)} (threshold: {self.whale_btc_threshold} BTC)."
                    ),
                    "explanation": (
                        "Edge weight jumped sharply above the active sliding-window baseline. "
                        "Large sudden value movements often signal institutional treasury shifts, "
                        "exchange cold-wallet sweeps, or potential liquidity shocks."
                    ),
                    "metrics": {
                        "Transfer (BTC)": round(total_btc, 4),
                        "Inputs / Outputs": f"{raw_in_count} -> {raw_out_count}",
                        "Active Out-Degree": sender_out_deg,
                        "Clustering Coeff": sender_clustering,
                    },
                }
            )

        # 3. Fan-In Consolidation Surge (Module 5: In-Degree Centrality)
        if raw_in_count >= 8 and capped_receivers:
            target = capped_receivers[0]
            if not self._is_on_cooldown(f"fanin:{target}"):
                alerts.append(
                    {
                        "id": uuid.uuid4().hex[:10],
                        "type": "Fan-In Consolidation Surge",
                        "severity": "medium",
                        "score": min(95, 65 + raw_in_count * 2),
                        "timestamp": now,
                        "primary_node": target,
                        "short_node": self.short_addr(target),
                        "tx_hash": tx_hash,
                        "summary": (
                            f"Wallet {self.short_addr(target)} consolidated funds from "
                            f"{raw_in_count} distinct input addresses totaling {total_btc:.4f} BTC."
                        ),
                        "explanation": (
                            "A sharp rise in In-Degree Centrality indicates multiple wallets "
                            "sweeping balances into a single hub—commonly seen in exchange deposit "
                            "sweeps or coordinated fund aggregation."
                        ),
                        "metrics": {
                            "Input Wallets": raw_in_count,
                            "Volume (BTC)": round(total_btc, 4),
                            "Active In-Degree": self.G.in_degree(target) if target in self.G else 1,
                        },
                    }
                )

        # 4. Structural Cluster Bridge (Module 4: Connectivity & Component Merge)
        if bridge_info is not None:
            u, v, size_u, size_v = bridge_info
            if not self._is_on_cooldown(f"bridge:{u}:{v}"):
                alerts.append(
                    {
                        "id": uuid.uuid4().hex[:10],
                        "type": "Structural Cluster Bridge",
                        "severity": "high",
                        "score": 86,
                        "timestamp": now,
                        "primary_node": u,
                        "short_node": self.short_addr(u),
                        "tx_hash": tx_hash,
                        "summary": (
                            f"Transaction linked two previously isolated wallet clusters "
                            f"({size_u} nodes <-> {size_v} nodes) via {self.short_addr(u)} -> {self.short_addr(v)}."
                        ),
                        "explanation": (
                            "Shortest-path distance between two independent connected components "
                            "dropped from infinity to 1. Cross-cluster bridges are key structural "
                            "indicators when tracking fund trails across distinct entities."
                        ),
                        "metrics": {
                            "Cluster A Size": size_u,
                            "Cluster B Size": size_v,
                            "Bridge Value (BTC)": round(total_btc, 4),
                        },
                    }
                )

        return alerts

    def _serialize_node(self, n: str) -> dict:
        data = self.G.nodes[n]
        deg = self.G.degree(n)
        return {
            "id": n,
            "label": data.get("label", self.short_addr(n)),
            "degree": deg,
            "in_degree": self.G.in_degree(n),
            "out_degree": self.G.out_degree(n),
            "total_in_btc": round(data.get("total_in_btc", 0.0), 5),
            "total_out_btc": round(data.get("total_out_btc", 0.0), 5),
            "tx_count": data.get("tx_count", 1),
            "is_anomaly": data.get("is_anomaly", False),
            "anomaly_type": data.get("anomaly_type"),
        }

    def _serialize_edge(self, u: str, v: str) -> dict:
        data = self.G[u][v]
        return {
            "id": f"{u}__{v}",
            "source": u,
            "target": v,
            "weight": round(data.get("weight", 0.0), 5),
            "count": data.get("count", 1),
            "tx_hash": data.get("tx_hash", "")[:10],
        }

    def get_snapshot(self) -> dict:
        return {
            "nodes": [self._serialize_node(n) for n in self.G.nodes],
            "edges": [self._serialize_edge(u, v) for u, v in self.G.edges],
            "stats": self.get_stats(time.time()),
        }

    def get_stats(self, now: float) -> dict:
        recent_10s = sum(1 for t in self.tx_timestamps if now - t <= 10.0)
        undirected = self.G.to_undirected(as_view=True)
        components = nx.number_connected_components(undirected) if self.G.number_of_nodes() > 0 else 0
        anomaly_nodes = sum(1 for _, d in self.G.nodes(data=True) if d.get("is_anomaly"))
        return {
            "active_nodes": self.G.number_of_nodes(),
            "active_edges": self.G.number_of_edges(),
            "tx_per_sec": round(recent_10s / 10.0, 2),
            "components": components,
            "anomaly_nodes": anomaly_nodes,
            "total_tx": self.total_tx_processed,
            "window_seconds": self.window_seconds,
            "whale_btc_threshold": self.whale_btc_threshold,
        }


def build_synthetic_tx(force_anomaly: bool = False) -> dict:
    """Fallback / Demo generator producing Blockchain.com-shaped UTX payloads."""
    def rand_addr():
        return "bc1q" + uuid.uuid4().hex[:24]

    if force_anomaly:
        sender = rand_addr()
        outputs = [{"addr": rand_addr(), "value": int(random.uniform(0.3, 1.2) * SATOSHI_PER_BTC)} for _ in range(14)]
        return {
            "hash": uuid.uuid4().hex,
            "time": int(time.time()),
            "inputs": [{"prev_out": {"addr": sender, "value": int(10.5 * SATOSHI_PER_BTC)}}],
            "out": outputs,
        }

    sender = rand_addr()
    out_cnt = random.choices([1, 2, 3], weights=[0.5, 0.35, 0.15])[0]
    outputs = [{"addr": rand_addr(), "value": int(random.uniform(0.005, 0.4) * SATOSHI_PER_BTC)} for _ in range(out_cnt)]
    return {
        "hash": uuid.uuid4().hex,
        "time": int(time.time()),
        "inputs": [{"prev_out": {"addr": sender, "value": int(0.5 * SATOSHI_PER_BTC)}}],
        "out": outputs,
    }


async def stream_blockchain_to_graph(
    graph_engine: DynamicBlockchainGraph,
    on_update: Callable[[dict], Awaitable[None]],
    on_status: Callable[[str], Awaitable[None]],
    stop_event: asyncio.Event,
):
    """
    Core Phase 1 Function:
    Gathers live unconfirmed Bitcoin transactions from Blockchain.com's WebSocket,
    converts them into a self-updating dynamic graph in real time, and invokes
    `on_update` with new nodes, edges, and detected anomalies.
    """
    retry_delay = 2
    while not stop_event.is_set():
        try:
            await on_status("connecting")
            async with websockets.connect(
                BLOCKCHAIN_WS_URL, ping_interval=25, ping_timeout=15, open_timeout=10
            ) as ws:
                await ws.send(json.dumps({"op": "unconfirmed_sub"}))
                await on_status("live")
                retry_delay = 2

                while not stop_event.is_set():
                    raw_msg = await asyncio.wait_for(ws.recv(), timeout=20.0)
                    data = json.loads(raw_msg)
                    if data.get("op") != "utx":
                        continue

                    tx = data.get("x", {})
                    update = graph_engine.add_transaction(tx)
                    if update:
                        await on_update(update)

                    # Gentle pacing (max ~4 updates/sec) so live animations stay silky smooth
                    await asyncio.sleep(0.25)

        except Exception as exc:
            print(f"[Ingestor] Live stream interrupted ({exc}). Running fallback stream for {retry_delay}s...")
            await on_status("fallback")
            # Stream realistic fallback transactions until reconnect attempt
            for _ in range(max(4, retry_delay * 2)):
                if stop_event.is_set():
                    break
                tx = build_synthetic_tx(force_anomaly=(random.random() < 0.08))
                update = graph_engine.add_transaction(tx)
                if update:
                    await on_update(update)
                await asyncio.sleep(0.8)
            retry_delay = min(retry_delay * 2, 16)
