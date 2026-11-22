"""
ARCNET Stage 3 Core Dynamic Graph Engine.

Upgrades:
- Domain-agnostic canonical ingestion (`add_canonical_event`) across all 5 sources
- Diff-only WebSocket payloads (never resends unchanged nodes -> eliminates browser lag)
- Live Incremental vs. Full Graph Runtime Benchmark (`LSGA.txt` Improvement #1)
- Multi-domain anomaly descriptions (BTC Whale, BGP Route Leak/Hijack, Wiki Bot Burst,
  Bluesky Reply Swarm, GDELT Entity Surge, plus Online ML Behavioral Outliers)
"""

from __future__ import annotations

import json
import math
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
from backend.sources_adapter import (
    SUPPORTED_SOURCES,
    build_domain_synthetic_event,
    normalize_raw_event,
)

SATOSHI_PER_BTC = 1e8


class DynamicBlockchainGraph:
    """Self-updating dynamic graph supporting 5 real-time domains + LSGA benchmarks."""

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
        self.active_source = "blockchain"
        self.window_seconds = window_seconds
        self.max_nodes = max_nodes
        self.whale_btc_threshold = whale_btc_threshold
        self.fanout_threshold = fanout_threshold
        self.fanin_threshold = fanin_threshold
        self.z_threshold = z_threshold

        self.analytics = AdvancedGraphAnalytics(recompute_interval_sec=2.5)
        self.ml_detector = GraphMLDetector(ml_score_threshold=ml_score_threshold, retrain_every=10)
        self.partitioner = GraphPartitioner(num_partitions=partition_count)

        self.anomaly_seeds: Dict[str, float] = {}
        self.tx_timestamps: deque = deque(maxlen=150)
        self.recent_alert_cooldown: Dict[str, float] = {}
        self.total_tx_processed = 0

        # Stage 3: Live Incremental vs. Full Graph Runtime Benchmark (LSGA.txt Improvement #1)
        self.benchmark_history: deque = deque(maxlen=25)
        self._last_full_bench_at: float = 0.0
        self._last_full_ms: float = 0.0

        self.recording_path = recording_path
        self.recording_enabled = False
        self._recorded_count = 0

    def switch_domain(self, new_source: str):
        if new_source not in SUPPORTED_SOURCES:
            new_source = "blockchain"
        self.active_source = new_source
        self.G.clear()
        self.anomaly_seeds.clear()
        self.tx_timestamps.clear()
        self.recent_alert_cooldown.clear()
        self.benchmark_history.clear()
        self._last_full_ms = 0.0
        self.total_tx_processed = 0
        self.analytics.reset()
        self.ml_detector.reset()

    @staticmethod
    def short_addr(addr: str) -> str:
        if any(addr.startswith(p) for p in ("AS", "Editor:", "Page:", "bsky:", "Org:", "Person:", "Loc:")):
            return addr if len(addr) <= 24 else addr[:22] + "…"
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

    def _record_event_if_enabled(self, ev: dict):
        if not self.recording_enabled or not self.recording_path or self._recorded_count >= 500:
            return
        try:
            path = Path(self.recording_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(ev) + "\n")
            self._recorded_count += 1
        except Exception:
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

    def _measure_full_vs_incremental(self, touched_nodes: Set[str], inc_elapsed_ms: float, now: float) -> Optional[dict]:
        """
        Benchmarks Incremental Neighbourhood Metric Update O(Δ) vs. Full Graph Recalculation O(V+E)
        (LSGA.txt Improvement #1). Runs full-graph reference pass every ~2.5s to avoid slowing stream.
        """
        n = self.G.number_of_nodes()
        m = self.G.number_of_edges()
        if n < 4:
            return None

        if (now - self._last_full_bench_at) >= 2.2 or self._last_full_ms <= 0.0:
            self._last_full_bench_at = now
            t0 = time.perf_counter()
            undir = self.G.to_undirected(as_view=True)
            _ = dict(self.G.in_degree())
            _ = dict(self.G.out_degree())
            _ = nx.clustering(undir)
            if m > 0:
                _ = nx.pagerank(self.G, alpha=0.85, max_iter=25)
            self._last_full_ms = max(0.05, (time.perf_counter() - t0) * 1000.0)

        inc_ms = max(0.01, inc_elapsed_ms)
        speedup = round(self._last_full_ms / inc_ms, 2)
        point = {
            "nodes": n,
            "edges": m,
            "touched": len(touched_nodes),
            "incremental_ms": round(inc_ms, 3),
            "full_ms": round(self._last_full_ms, 3),
            "speedup": speedup,
            "timestamp": now,
        }
        self.benchmark_history.append(point)
        return point

    def add_transaction(self, tx: dict) -> dict:
        """Backward-compatible wrapper that normalizes raw Blockchain or canonical events."""
        ev = normalize_raw_event(self.active_source, tx)
        if not ev:
            return {}
        return self.add_canonical_event(ev)

    def add_canonical_event(self, ev: dict) -> dict:
        t_inc_start = time.perf_counter()
        now = float(ev.get("_override_ts", ev.get("timestamp", time.time())))
        tx_hash = str(ev.get("hash", uuid.uuid4().hex[:14]))
        unit = str(ev.get("unit", SUPPORTED_SOURCES.get(self.active_source, {}).get("unit", "BTC")))
        domain_ctx = str(ev.get("domain_context", self.active_source))
        self._record_event_if_enabled(ev)

        sender_addrs = list(dict.fromkeys([s for s in ev.get("senders", []) if s]))
        receiver_map: Dict[str, float] = {}
        for addr, val in ev.get("receivers", []):
            if addr:
                receiver_map[addr] = receiver_map.get(addr, 0.0) + float(val)
        receiver_addrs = list(receiver_map.keys())

        if not sender_addrs or not receiver_addrs:
            return {}

        raw_in_count = len(sender_addrs)
        raw_out_count = len(receiver_addrs)
        total_weight = sum(receiver_map.values())

        bridge_info = self._check_cluster_bridge(sender_addrs, receiver_addrs)

        capped_senders = sender_addrs[:4]
        capped_receivers = sorted(receiver_addrs, key=lambda a: receiver_map[a], reverse=True)[:12]

        touched_nodes: Set[str] = set()
        new_edges_payload: List[dict] = []

        primary_sender = capped_senders[0]
        for r in capped_receivers:
            if r == primary_sender:
                continue
            w_amt = round(receiver_map[r], 5)
            self._upsert_edge(primary_sender, r, w_amt, now, tx_hash)
            touched_nodes.update([primary_sender, r])
            new_edges_payload.append(
                {
                    "id": f"{primary_sender}__{r}",
                    "source": primary_sender,
                    "target": r,
                    "weight": w_amt,
                    "timestamp": now,
                    "tx_hash": tx_hash[:10],
                }
            )

        if len(capped_senders) > 1 and capped_receivers:
            primary_receiver = capped_receivers[0]
            for s in capped_senders[1:]:
                if s == primary_receiver:
                    continue
                w_amt = round(receiver_map[primary_receiver] / len(capped_senders), 5)
                self._upsert_edge(s, primary_receiver, w_amt, now, tx_hash)
                touched_nodes.update([s, primary_receiver])
                new_edges_payload.append(
                    {
                        "id": f"{s}__{primary_receiver}",
                        "source": s,
                        "target": primary_receiver,
                        "weight": w_amt,
                        "timestamp": now,
                        "tx_hash": tx_hash[:10],
                    }
                )

        self.total_tx_processed += 1
        self.tx_timestamps.append(now)

        removed_nodes, removed_edges = self.prune_expired(now)

        # Incremental local ML & anomaly check ONLY on affected neighbourhood
        taint_before = self.analytics.risk_taint.get(primary_sender, 0.0)
        ml_score, ml_attributions = self.ml_detector.evaluate_node(
            self.G, primary_sender, risk_taint=taint_before
        )
        if primary_sender in self.G.nodes:
            self.G.nodes[primary_sender]["ml_score"] = ml_score

        alerts = self._detect_anomalies(
            primary_sender=primary_sender,
            capped_receivers=capped_receivers,
            raw_in_count=raw_in_count,
            raw_out_count=raw_out_count,
            total_weight=total_weight,
            unit=unit,
            domain_ctx=domain_ctx,
            tx_hash=tx_hash,
            bridge_info=bridge_info,
            ml_score=ml_score,
            ml_attributions=ml_attributions,
            now=now,
        )

        inc_elapsed_ms = (time.perf_counter() - t_inc_start) * 1000.0
        bench_point = self._measure_full_vs_incremental(touched_nodes, inc_elapsed_ms, now)

        for alert in alerts:
            n_id = alert["primary_node"]
            self.anomaly_seeds[n_id] = max(self.anomaly_seeds.get(n_id, 0.0), float(alert["score"]))
            if n_id in self.G.nodes:
                self.G.nodes[n_id]["is_anomaly"] = True
                self.G.nodes[n_id]["anomaly_type"] = alert["type"]

        # Amortized global analytics every 2.5s (and emit ONLY changed community/taint nodes to prevent UI lag)
        prev_comm_snap = dict(self.analytics.communities)
        recomputed = self.analytics.maybe_recompute(
            self.G, self.anomaly_seeds, force=(len(alerts) > 0)
        )
        if recomputed:
            self.partitioner.compute(self.G)

        nodes_to_emit = set(touched_nodes)
        if recomputed:
            for nid in self.G.nodes:
                if (
                    prev_comm_snap.get(nid) != self.analytics.communities.get(nid)
                    or self.analytics.risk_taint.get(nid, 0.0) >= 25.0
                ):
                    nodes_to_emit.add(nid)

        return {
            "type": "graph_update",
            "source": self.active_source,
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
            "benchmark": bench_point,
            "tx_summary": {
                "hash": tx_hash[:12],
                "sender": self.short_addr(primary_sender),
                "outputs": raw_out_count,
                "btc": round(total_weight, 3),
                "unit": unit,
                "ml_score": ml_score,
                "is_anomaly": len(alerts) > 0,
            },
        }

    def _upsert_edge(self, u: str, v: str, weight: float, now: float, tx_hash: str):
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

        self.G.nodes[u]["total_out_btc"] += weight
        self.G.nodes[v]["total_in_btc"] += weight

        if self.G.has_edge(u, v):
            self.G[u][v]["weight"] += weight
            self.G[u][v]["count"] += 1
            self.G[u][v]["timestamp"] = now
            self.G[u][v]["tx_hash"] = tx_hash
        else:
            self.G.add_edge(u, v, weight=weight, count=1, timestamp=now, tx_hash=tx_hash)

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
        total_weight: float,
        unit: str,
        domain_ctx: str,
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

        domain_titles = {
            "blockchain": ("Fan-Out Star Burst", "Whale Volume Spike", "Fan-In Consolidation Surge", "Structural Cluster Bridge"),
            "ripe_bgp": ("BGP Route Leak Fan-Out", "Prefix Announcement Surge", "BGP Transit Convergence", "Cross-Tier AS Hijack Bridge"),
            "wikimedia": ("Mass Bot Edit Burst", "Large Article Overwrite", "Edit-War Convergence", "Cross-Topic Edit Bridge"),
            "bluesky": ("Coordinated Reply Swarm", "Viral Mention Spike", "Trending Hub Convergence", "Cross-Community Bridge"),
            "gdelt": ("Multi-Entity Event Burst", "High-Salience Media Surge", "Entity Convergence Hub", "Cross-Region Event Bridge"),
        }
        t_fanout, t_heavy, t_fanin, t_bridge = domain_titles.get(
            self.active_source, domain_titles["blockchain"]
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
                    "type": t_fanout,
                    "severity": "high" if raw_out_count >= 12 else "medium",
                    "score": score,
                    "ml_score": ml_score,
                    "ml_attributions": ml_attributions,
                    "timestamp": now,
                    "primary_node": primary_sender,
                    "short_node": self.short_addr(primary_sender),
                    "tx_hash": tx_hash,
                    "summary": (
                        f"{self.short_addr(primary_sender)} fanned out {total_weight:.3f} {unit} "
                        f"across {raw_out_count} targets (Out-Degree Z = {sender_z:.2f}, Clustering = {sender_clustering})."
                    ),
                    "explanation": (
                        f"[{domain_ctx}] Sharp spike in Out-Degree Centrality with near-zero local "
                        "clustering indicates star-shaped dispersion, route leaks, or automated bursts."
                    ),
                    "metrics": {
                        "Out-Degree (Raw)": raw_out_count,
                        "Degree Z-Score": round(sender_z, 2),
                        "Clustering Coeff": sender_clustering,
                        f"Volume ({unit})": round(total_weight, 3),
                    },
                }
            )

        # 2. Heavy Edge / Whale Volume Spike
        if total_weight >= self.whale_btc_threshold and not self._is_on_cooldown(
            f"whale:{primary_sender}"
        ):
            score = min(99, int(72 + math.log1p(total_weight) * 8))
            alerts.append(
                {
                    "id": uuid.uuid4().hex[:10],
                    "type": t_heavy,
                    "severity": "high" if total_weight >= self.whale_btc_threshold * 3 else "medium",
                    "score": score,
                    "ml_score": ml_score,
                    "ml_attributions": ml_attributions,
                    "timestamp": now,
                    "primary_node": primary_sender,
                    "short_node": self.short_addr(primary_sender),
                    "tx_hash": tx_hash,
                    "summary": (
                        f"High-magnitude event of {total_weight:.3f} {unit} from "
                        f"{self.short_addr(primary_sender)} (threshold: {self.whale_btc_threshold:.1f} {unit})."
                    ),
                    "explanation": (
                        f"[{domain_ctx}] Edge weight spiked above the active sliding-window cutoff."
                    ),
                    "metrics": {
                        f"Magnitude ({unit})": round(total_weight, 3),
                        "Sources / Targets": f"{raw_in_count} -> {raw_out_count}",
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
                        "type": t_fanin,
                        "severity": "high" if raw_in_count >= 10 else "medium",
                        "score": min(96, 66 + raw_in_count * 2),
                        "ml_score": ml_score,
                        "ml_attributions": ml_attributions,
                        "timestamp": now,
                        "primary_node": target,
                        "short_node": self.short_addr(target),
                        "tx_hash": tx_hash,
                        "summary": (
                            f"{self.short_addr(target)} converged {raw_in_count} distinct sources "
                            f"totaling {total_weight:.3f} {unit}."
                        ),
                        "explanation": (
                            f"[{domain_ctx}] Rapid rise in In-Degree Centrality indicates multiple nodes converging on a single hub."
                        ),
                        "metrics": {
                            "Input Sources": raw_in_count,
                            f"Volume ({unit})": round(total_weight, 3),
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
                        "type": t_bridge,
                        "severity": "high",
                        "score": 86,
                        "ml_score": ml_score,
                        "ml_attributions": ml_attributions,
                        "timestamp": now,
                        "primary_node": u,
                        "short_node": self.short_addr(u),
                        "tx_hash": tx_hash,
                        "summary": (
                            f"Link bridged two isolated clusters ({size_u} nodes <-> {size_v} nodes) "
                            f"via {self.short_addr(u)} -> {self.short_addr(v)}."
                        ),
                        "explanation": (
                            f"[{domain_ctx}] Shortest-path distance between two independent connected components dropped from infinity to 1."
                        ),
                        "metrics": {
                            "Cluster A Size": size_u,
                            "Cluster B Size": size_v,
                            f"Bridge Weight ({unit})": round(total_weight, 3),
                            "ML Risk Score": ml_score,
                        },
                    }
                )

        # 5. Online ML Behavioral Anomaly
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
                        f"[{domain_ctx}] 8D structural embedding isolated rapidly in the online forest without tripping fixed cutoffs."
                    ),
                    "metrics": {
                        "ML Risk Score": f"{ml_score}/100",
                        "Primary Driver": top_feat,
                        f"Weight ({unit})": round(total_weight, 3),
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
            "total_in_btc": round(data.get("total_in_btc", 0.0), 4),
            "total_out_btc": round(data.get("total_out_btc", 0.0), 4),
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
            "weight": round(float(data.get("weight", 0.0)), 4),
            "count": data.get("count", 1),
            "tx_hash": str(data.get("tx_hash", ""))[:10],
        }

    def get_snapshot(self) -> dict:
        self.analytics.maybe_recompute(self.G, self.anomaly_seeds, force=False)
        if not self.partitioner.last_report:
            self.partitioner.compute(self.G)
        return {
            "source": self.active_source,
            "source_meta": SUPPORTED_SOURCES.get(self.active_source, SUPPORTED_SOURCES["blockchain"]),
            "nodes": [self._serialize_node(n) for n in self.G.nodes],
            "edges": [self._serialize_edge(u, v) for u, v in self.G.edges],
            "stats": self.get_stats(time.time()),
            "analytics": self.analytics.get_summary(),
            "ml": self.ml_detector.get_summary(),
            "partitioning": self.partitioner.last_report,
            "benchmarks": list(self.benchmark_history),
        }

    def get_stats(self, now: float) -> dict:
        recent_10s = sum(1 for t in self.tx_timestamps if now - t <= 10.0)
        undirected = self.G.to_undirected(as_view=True)
        components = nx.number_connected_components(undirected) if self.G.number_of_nodes() > 0 else 0
        anomaly_nodes = sum(1 for _, d in self.G.nodes(data=True) if d.get("is_anomaly"))
        src_meta = SUPPORTED_SOURCES.get(self.active_source, SUPPORTED_SOURCES["blockchain"])
        return {
            "source": self.active_source,
            "unit": src_meta["unit"],
            "node_noun": src_meta["node_noun"],
            "edge_noun": src_meta["edge_noun"],
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


def build_synthetic_tx(force_anomaly: bool = False, source: str = "blockchain") -> dict:
    return build_domain_synthetic_event(source=source, force_anomaly=force_anomaly)


def build_peel_chain_and_wash_loop_txs(source: str = "blockchain") -> List[dict]:
    """Generates a 4-hop Peel/Propagation Chain + 3-hop Circular Loop adapted to the active domain."""
    now = time.time()
    prefixes = {
        "blockchain": ("bc1qforensic", "BTC"),
        "ripe_bgp": ("AS6451", "Prefixes"),
        "wikimedia": ("Editor:Ring_", "KB Δ"),
        "bluesky": ("bsky:ring_", "Interactions"),
        "gdelt": ("Org:Syndicate_", "Mentions"),
    }
    pref, unit = prefixes.get(source, ("bc1qforensic", "BTC"))
    w = [f"{pref}{i}_{uuid.uuid4().hex[:8]}" for i in range(6)]
    events = []

    amounts = [3.6, 3.3, 2.95]
    for idx, amt in enumerate(amounts):
        peel_off = f"{pref}peel_{idx}_{uuid.uuid4().hex[:6]}"
        events.append(
            {
                "source": source,
                "hash": uuid.uuid4().hex[:14],
                "_override_ts": now + idx * 3,
                "senders": [w[idx]],
                "receivers": [(w[idx + 1], amt), (peel_off, 0.22)],
                "unit": unit,
                "domain_context": "Multi-Hop Peel / Propagation Chain",
            }
        )

    loop_pairs = [(w[3], w[4], 1.8), (w[4], w[5], 1.75), (w[5], w[3], 1.7)]
    for idx, (u, v, amt) in enumerate(loop_pairs):
        events.append(
            {
                "source": source,
                "hash": uuid.uuid4().hex[:14],
                "_override_ts": now + 12 + idx * 2,
                "senders": [u],
                "receivers": [(v, amt)],
                "unit": unit,
                "domain_context": "Circular Loop Pattern",
            }
        )
    return events
