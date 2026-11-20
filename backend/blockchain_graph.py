"""
ARCNET Core Dynamic Graph Engine (Phase 1 + Phase 2).

Combines:
- Self-updating directed weighted NetworkX DiGraph with sliding-window pruning
- 4 Explainable Phase 1 LSGA anomaly detectors (Fan-Out, Whale, Fan-In, Cluster Bridge)
- Phase 2 Louvain Community Detection, Personalized PageRank Taint & Betweenness
- Phase 2 Online Isolation Forest ML Behavioral Anomaly Scoring (8D embeddings)
- Phase 2 Edge-Cut vs. Vertex-Cut Graph Partitioning
- Phase 2 JSONL Stream Recorder & Synthetic Peel-Chain / Wash-Loop Generators
"""

from __future__ import annotations

import json
import math
import os
import random
import statistics
import time
import uuid
from collections import deque
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import networkx as nx

from backend.analytics_advanced import AdvancedGraphAnalytics
from backend.ml_detector import GraphMLDetector
from backend.partitioning import GraphPartitioner

BLOCKCHAIN_WS_URL = "wss://ws.blockchain.info/inv"
SATOSHI_PER_BTC = 1e8


class DynamicBlockchainGraph:
    """Self-updating dynamic blockchain graph with Phase 2 analytics, ML & partitioning."""

    def __init__(
        self,
        window_seconds: int = 180,
        max_nodes: int = 120,
        whale_btc_threshold: float = 2.0,
        fanout_threshold: int = 6,
        fanin_threshold: int = 5,
        z_threshold: float = 2.6,
        ml_score_threshold: float = 74.0,
        partition_count: int = 4,
        recording_path: Optional[str] = None,
    ):
        self.G = nx.DiGraph()
        self.window_seconds = window_seconds
        self.max_nodes = max_nodes
        self.whale_btc_threshold = whale_btc_threshold
        self.fanout_threshold = fanout_threshold
        self.fanin_threshold = fanin_threshold
        self.z_threshold = z_threshold

        # Phase 2 modular subsystems
        self.analytics = AdvancedGraphAnalytics(recompute_interval_sec=2.0)
        self.ml_detector = GraphMLDetector(ml_score_threshold=ml_score_threshold, retrain_every=10)
        self.partitioner = GraphPartitioner(num_partitions=partition_count)

        self.anomaly_seeds: Dict[str, float] = {}
        self.tx_timestamps: deque = deque(maxlen=150)
        self.recent_alert_cooldown: Dict[str, float] = {}
        self.total_tx_processed = 0
        self.recording_path = recording_path
        self.recording_enabled = False
        self._recorded_count = 0

    @staticmethod
    def short_addr(addr: str) -> str:
        if len(addr) <= 12:
            return addr
        return f"{addr[:6]}...{addr[-4:]}"

    def _is_on_cooldown(self, key: str, cooldown_sec: float = 20.0) -> bool:
        now = time.time()
        last = self.recent_alert_cooldown.get(key, 0.0)
        if now - last < cooldown_sec:
            return True
        self.recent_alert_cooldown[key] = now
        return False

    def _record_tx_if_enabled(self, tx: dict):
        if not self.recording_enabled or not self.recording_path or self._recorded_count >= 500:
            return
        try:
            path = Path(self.recording_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(tx) + "\n")
            self._recorded_count += 1
        except Exception:
            # Read-only filesystem (e.g. Vercel serverless) — disable gracefully
            self.recording_enabled = False

    def _check_cluster_bridge(
        self, senders: List[str], receivers: List[str]
    ) -> Optional[Tuple[str, str, int, int]]:
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

    def get_ego_edges(self, center_node: str, radius: int = 2) -> List[dict]:
        """Extracts up to 2-hop ego-network edges around a flagged node for Neo4j persistence."""
        if center_node not in self.G:
            return []
        try:
            ego = nx.ego_graph(self.G.to_undirected(as_view=True), center_node, radius=radius)
            sub = self.G.subgraph(ego.nodes)
            return [
                {
                    "source": u,
                    "target": v,
                    "weight": round(float(d.get("weight", 0.0)), 6),
                    "timestamp": float(d.get("timestamp", time.time())),
                    "tx_hash": str(d.get("tx_hash", "")),
                }
                for u, v, d in sub.edges(data=True)
            ]
        except Exception:
            return []

    def add_transaction(self, tx: dict) -> dict:
        now = float(tx.get("_override_ts", time.time()))
        tx_hash = tx.get("hash", uuid.uuid4().hex[:16])
        self._record_tx_if_enabled(tx)

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

        sender_addrs = list(dict.fromkeys([s[0] for s in senders_raw]))
        receiver_map: Dict[str, float] = {}
        for addr, btc in receivers_raw:
            receiver_map[addr] = receiver_map.get(addr, 0.0) + btc
        receiver_addrs = list(receiver_map.keys())

        raw_in_count = len(sender_addrs)
        raw_out_count = len(receiver_addrs)
        total_btc = sum(receiver_map.values())

        bridge_info = self._check_cluster_bridge(sender_addrs, receiver_addrs)

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
                    "timestamp": now,
                    "tx_hash": tx_hash[:10],
                }
            )

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
                        "timestamp": now,
                        "tx_hash": tx_hash[:10],
                    }
                )

        self.total_tx_processed += 1
        self.tx_timestamps.append(now)

        removed_nodes, removed_edges = self.prune_expired(now)

        # Phase 2 ML Evaluation on primary sender
        taint_before = self.analytics.risk_taint.get(primary_sender, 0.0)
        ml_score, ml_attributions = self.ml_detector.evaluate_node(
            self.G, primary_sender, risk_taint=taint_before
        )
        if primary_sender in self.G.nodes:
            self.G.nodes[primary_sender]["ml_score"] = ml_score

        # Run Rule + ML Hybrid Anomaly Detection
        alerts = self._detect_anomalies(
            primary_sender=primary_sender,
            capped_receivers=capped_receivers,
            raw_in_count=raw_in_count,
            raw_out_count=raw_out_count,
            total_btc=total_btc,
            tx_hash=tx_hash,
            bridge_info=bridge_info,
            ml_score=ml_score,
            ml_attributions=ml_attributions,
            now=now,
        )

        # Seed PageRank risk propagation & flag anomalous nodes
        for alert in alerts:
            n_id = alert["primary_node"]
            self.anomaly_seeds[n_id] = max(self.anomaly_seeds.get(n_id, 0.0), float(alert["score"]))
            if n_id in self.G.nodes:
                self.G.nodes[n_id]["is_anomaly"] = True
                self.G.nodes[n_id]["anomaly_type"] = alert["type"]

        # Periodic Phase 2 Analytics (Louvain + Personalized PageRank + Betweenness) & Partitioning
        recomputed = self.analytics.maybe_recompute(
            self.G, self.anomaly_seeds, force=(len(alerts) > 0)
        )
        if recomputed:
            self.partitioner.compute(self.G)

        # If analytics refreshed, include updated node styling for top affected nodes
        nodes_to_emit = set(touched_nodes)
        if recomputed:
            nodes_to_emit.update(list(self.G.nodes))

        return {
            "type": "graph_update",
            "timestamp": now,
            "added_nodes": [self._serialize_node(n) for n in nodes_to_emit if n in self.G],
            "added_edges": [
                self._serialize_edge(e["source"], e["target"])
                for e in new_edges_payload
                if self.G.has_edge(e["source"], e["target"])
            ],
            "removed_nodes": removed_nodes,
            "removed_edges": removed_edges,
            "alerts": alerts,
            "raw_edges": new_edges_payload,
            "stats": self.get_stats(now),
            "analytics": self.analytics.get_summary() if recomputed else None,
            "ml": self.ml_detector.get_summary() if recomputed else None,
            "partitioning": self.partitioner.last_report if recomputed else None,
            "tx_summary": {
                "hash": tx_hash[:12],
                "sender": self.short_addr(primary_sender),
                "outputs": raw_out_count,
                "btc": round(total_btc, 4),
                "ml_score": ml_score,
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
                    ml_score=0.0,
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
            self.G.add_edge(u, v, weight=btc, count=1, timestamp=now, tx_hash=tx_hash)

    def prune_expired(self, now: float) -> Tuple[List[str], List[str]]:
        cutoff = now - self.window_seconds
        expired_edges = [
            (u, v) for u, v, d in self.G.edges(data=True) if d.get("timestamp", 0) < cutoff
        ]
        removed_edge_ids = [f"{u}__{v}" for u, v in expired_edges]
        self.G.remove_edges_from(expired_edges)

        isolated = [n for n in list(self.G.nodes) if self.G.degree(n) == 0]
        self.G.remove_nodes_from(isolated)
        removed_nodes = list(isolated)

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

        for n in removed_nodes:
            self.anomaly_seeds.pop(n, None)

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
        ml_score: float,
        ml_attributions: List[dict],
        now: float,
    ) -> List[dict]:
        alerts: List[dict] = []
        undirected = self.G.to_undirected(as_view=True)

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

        # 1. Fan-Out Star Burst
        if (
            raw_out_count >= self.fanout_threshold
            or (sender_out_deg >= 5 and sender_z >= self.z_threshold and sender_clustering < 0.1)
        ) and not self._is_on_cooldown(f"fanout:{primary_sender}"):
            score = min(99, int(68 + min(raw_out_count, 24) + max(0, sender_z * 2)))
            alerts.append(
                {
                    "id": uuid.uuid4().hex[:10],
                    "type": "Fan-Out Star Burst",
                    "severity": "high" if raw_out_count >= 12 else "medium",
                    "score": score,
                    "ml_score": ml_score,
                    "ml_attributions": ml_attributions,
                    "timestamp": now,
                    "primary_node": primary_sender,
                    "short_node": self.short_addr(primary_sender),
                    "tx_hash": tx_hash,
                    "summary": (
                        f"Wallet {self.short_addr(primary_sender)} dispersed "
                        f"{total_btc:.4f} BTC across {raw_out_count} distinct wallets "
                        f"(Out-Degree Z = {sender_z:.2f}, Clustering = {sender_clustering})."
                    ),
                    "explanation": (
                        "A sharp spike in Out-Degree Centrality paired with near-zero local clustering "
                        "signals automated batch payouts, mixing dispersion, or dust attacks."
                    ),
                    "metrics": {
                        "Out-Degree (Raw)": raw_out_count,
                        "Degree Z-Score": round(sender_z, 2),
                        "Clustering Coeff": sender_clustering,
                        "Volume (BTC)": round(total_btc, 4),
                    },
                }
            )

        # 2. Whale Volume Spike
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
                    "ml_score": ml_score,
                    "ml_attributions": ml_attributions,
                    "timestamp": now,
                    "primary_node": primary_sender,
                    "short_node": self.short_addr(primary_sender),
                    "tx_hash": tx_hash,
                    "summary": (
                        f"High-value transfer of {total_btc:.4f} BTC initiated by "
                        f"{self.short_addr(primary_sender)} (threshold: {self.whale_btc_threshold:.1f} BTC)."
                    ),
                    "explanation": (
                        "Edge weight jumped sharply above the active sliding-window baseline, "
                        "indicating institutional treasury shifts or exchange cold-wallet sweeps."
                    ),
                    "metrics": {
                        "Transfer (BTC)": round(total_btc, 4),
                        "Inputs / Outputs": f"{raw_in_count} -> {raw_out_count}",
                        "Active Out-Degree": sender_out_deg,
                        "ML Risk Score": ml_score,
                    },
                }
            )

        # 3. Fan-In Consolidation Surge
        if raw_in_count >= self.fanin_threshold and capped_receivers:
            target = capped_receivers[0]
            if not self._is_on_cooldown(f"fanin:{target}"):
                alerts.append(
                    {
                        "id": uuid.uuid4().hex[:10],
                        "type": "Fan-In Consolidation Surge",
                        "severity": "high" if raw_in_count >= 10 else "medium",
                        "score": min(96, 66 + raw_in_count * 2),
                        "ml_score": ml_score,
                        "ml_attributions": ml_attributions,
                        "timestamp": now,
                        "primary_node": target,
                        "short_node": self.short_addr(target),
                        "tx_hash": tx_hash,
                        "summary": (
                            f"Wallet {self.short_addr(target)} consolidated funds from "
                            f"{raw_in_count} distinct input addresses totaling {total_btc:.4f} BTC."
                        ),
                        "explanation": (
                            "A rapid rise in In-Degree Centrality signals multiple wallets sweeping "
                            "balances into a single hub—typical of deposit sweeps or fund aggregation."
                        ),
                        "metrics": {
                            "Input Wallets": raw_in_count,
                            "Volume (BTC)": round(total_btc, 4),
                            "Active In-Degree": self.G.in_degree(target) if target in self.G else 1,
                            "ML Risk Score": ml_score,
                        },
                    }
                )

        # 4. Structural Cluster Bridge
        if bridge_info is not None:
            u, v, size_u, size_v = bridge_info
            if not self._is_on_cooldown(f"bridge:{u}:{v}"):
                alerts.append(
                    {
                        "id": uuid.uuid4().hex[:10],
                        "type": "Structural Cluster Bridge",
                        "severity": "high",
                        "score": 86,
                        "ml_score": ml_score,
                        "ml_attributions": ml_attributions,
                        "timestamp": now,
                        "primary_node": u,
                        "short_node": self.short_addr(u),
                        "tx_hash": tx_hash,
                        "summary": (
                            f"Transaction linked two isolated wallet clusters "
                            f"({size_u} nodes <-> {size_v} nodes) via {self.short_addr(u)} -> {self.short_addr(v)}."
                        ),
                        "explanation": (
                            "Shortest-path distance between two independent connected components "
                            "dropped from infinity to 1, flagging a cross-cluster structural bridge."
                        ),
                        "metrics": {
                            "Cluster A Size": size_u,
                            "Cluster B Size": size_v,
                            "Bridge Value (BTC)": round(total_btc, 4),
                            "ML Risk Score": ml_score,
                        },
                    }
                )

        # 5. Phase 2 ML Behavioral Anomaly (catches subtle multi-feature outliers below fixed rules)
        if (
            not alerts
            and ml_score >= self.ml_detector.ml_score_threshold
            and not self._is_on_cooldown(f"ml:{primary_sender}", cooldown_sec=30.0)
        ):
            top_feat = ml_attributions[0]["feature"] if ml_attributions else "Structural Embedding"
            alerts.append(
                {
                    "id": uuid.uuid4().hex[:10],
                    "type": "ML Behavioral Anomaly",
                    "severity": "high" if ml_score >= 85 else "medium",
                    "score": int(round(ml_score)),
                    "ml_score": ml_score,
                    "ml_attributions": ml_attributions,
                    "timestamp": now,
                    "primary_node": primary_sender,
                    "short_node": self.short_addr(primary_sender),
                    "tx_hash": tx_hash,
                    "summary": (
                        f"Online Isolation Forest flagged {self.short_addr(primary_sender)} "
                        f"(ML Risk = {ml_score}/100; primary driver: {top_feat})."
                    ),
                    "explanation": (
                        "Even without tripping fixed volume or degree cutoffs, this wallet's 8D "
                        "structural embedding isolated rapidly in the online forest—detecting stealth "
                        "structuring or abnormal flow asymmetry."
                    ),
                    "metrics": {
                        "ML Risk Score": f"{ml_score}/100",
                        "Primary Driver": top_feat,
                        "Volume (BTC)": round(total_btc, 4),
                        "Out-Degree": sender_out_deg,
                    },
                }
            )

        return alerts

    def _serialize_node(self, n: str) -> dict:
        data = self.G.nodes[n]
        adv = self.analytics.get_node_metrics(n)
        return {
            "id": n,
            "label": data.get("label", self.short_addr(n)),
            "degree": self.G.degree(n),
            "in_degree": self.G.in_degree(n),
            "out_degree": self.G.out_degree(n),
            "total_in_btc": round(data.get("total_in_btc", 0.0), 5),
            "total_out_btc": round(data.get("total_out_btc", 0.0), 5),
            "tx_count": data.get("tx_count", 1),
            "is_anomaly": data.get("is_anomaly", False),
            "anomaly_type": data.get("anomaly_type"),
            "community_id": adv["community_id"],
            "risk_taint": adv["risk_taint"],
            "betweenness": adv["betweenness"],
            "ml_score": round(float(data.get("ml_score", self.ml_detector.node_scores.get(n, 0.0))), 1),
            "partition_id": self.partitioner.get_partition(n),
        }

    def _serialize_edge(self, u: str, v: str) -> dict:
        data = self.G[u][v]
        return {
            "id": f"{u}__{v}",
            "source": u,
            "target": v,
            "weight": round(float(data.get("weight", 0.0)), 5),
            "count": data.get("count", 1),
            "tx_hash": str(data.get("tx_hash", ""))[:10],
        }

    def get_snapshot(self) -> dict:
        self.analytics.maybe_recompute(self.G, self.anomaly_seeds, force=False)
        if not self.partitioner.last_report:
            self.partitioner.compute(self.G)
        return {
            "nodes": [self._serialize_node(n) for n in self.G.nodes],
            "edges": [self._serialize_edge(u, v) for u, v in self.G.edges],
            "stats": self.get_stats(time.time()),
            "analytics": self.analytics.get_summary(),
            "ml": self.ml_detector.get_summary(),
            "partitioning": self.partitioner.last_report,
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
            "ml_threshold": self.ml_detector.ml_score_threshold,
            "partitions": self.partitioner.num_partitions,
            "modularity": self.analytics.modularity,
            "communities": self.analytics.num_communities,
        }


def build_synthetic_tx(force_anomaly: bool = False) -> dict:
    def rand_addr():
        return "bc1q" + uuid.uuid4().hex[:24]

    if force_anomaly:
        sender = rand_addr()
        outputs = [
            {"addr": rand_addr(), "value": int(random.uniform(0.35, 0.95) * SATOSHI_PER_BTC)}
            for _ in range(12)
        ]
        return {
            "hash": uuid.uuid4().hex,
            "time": int(time.time()),
            "inputs": [{"prev_out": {"addr": sender, "value": int(9.2 * SATOSHI_PER_BTC)}}],
            "out": outputs,
        }

    sender = rand_addr()
    out_cnt = random.choices([1, 2, 3], weights=[0.5, 0.35, 0.15])[0]
    outputs = [
        {"addr": rand_addr(), "value": int(random.uniform(0.01, 0.45) * SATOSHI_PER_BTC)}
        for _ in range(out_cnt)
    ]
    return {
        "hash": uuid.uuid4().hex,
        "time": int(time.time()),
        "inputs": [{"prev_out": {"addr": sender, "value": int(0.6 * SATOSHI_PER_BTC)}}],
        "out": outputs,
    }


def build_peel_chain_and_wash_loop_txs() -> List[dict]:
    """
    Generates a realistic 4-hop Peel Chain + 3-hop Wash-Trading Cycle so you can
    immediately test and demonstrate Milestone 1 Cypher Forensics & Milestone 2 Taint Flow.
    """
    now = time.time()
    w = [f"bc1qforensic{i}{uuid.uuid4().hex[:14]}" for i in range(6)]
    txs = []

    # 4-hop Peel Chain: w0 -> w1 -> w2 -> w3 (peeling ~0.2 BTC each hop)
    peel_amounts = [3.6, 3.3, 2.95]
    for idx, amt in enumerate(peel_amounts):
        peel_off = f"bc1qpeel{idx}{uuid.uuid4().hex[:16]}"
        txs.append(
            {
                "hash": uuid.uuid4().hex,
                "time": int(now + idx * 3),
                "_override_ts": now + idx * 3,
                "inputs": [{"prev_out": {"addr": w[idx], "value": int((amt + 0.25) * SATOSHI_PER_BTC)}}],
                "out": [
                    {"addr": w[idx + 1], "value": int(amt * SATOSHI_PER_BTC)},
                    {"addr": peel_off, "value": int(0.22 * SATOSHI_PER_BTC)},
                ],
            }
        )

    # 3-hop Wash Loop: w3 -> w4 -> w5 -> w3
    loop_pairs = [(w[3], w[4], 1.8), (w[4], w[5], 1.75), (w[5], w[3], 1.7)]
    for idx, (u, v, amt) in enumerate(loop_pairs):
        txs.append(
            {
                "hash": uuid.uuid4().hex,
                "time": int(now + 12 + idx * 2),
                "_override_ts": now + 12 + idx * 2,
                "inputs": [{"prev_out": {"addr": u, "value": int(amt * SATOSHI_PER_BTC)}}],
                "out": [{"addr": v, "value": int(amt * SATOSHI_PER_BTC)}],
            }
        )
    return txs
