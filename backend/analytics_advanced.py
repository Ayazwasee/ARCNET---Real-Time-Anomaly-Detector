"""
Stage 2 & 3 — Advanced Graph Analytics + Louvain Community Evolution Tracker.

Computes on the active dynamic graph:
1. Louvain Community Detection + Modularity (Q)
2. Temporal Community Evolution Tracker (detects cluster births, merges, splits, and stability)
3. Personalized PageRank Risk / Taint Diffusion seeded from anomalous entities
4. Betweenness Centrality Hubs
"""

from __future__ import annotations

import time
from collections import deque
from typing import Dict, List, Set

import networkx as nx


class AdvancedGraphAnalytics:
    """Periodically computes Louvain communities, temporal evolution deltas, PageRank taint & Betweenness."""

    def __init__(self, recompute_interval_sec: float = 2.5):
        self.recompute_interval_sec = recompute_interval_sec
        self.last_computed_at: float = 0.0

        self.communities: Dict[str, int] = {}
        self.prev_communities: List[Set[str]] = []
        self.prev_modularity: float = 0.0
        self.modularity: float = 0.0
        self.num_communities: int = 0

        self.risk_taint: Dict[str, float] = {}
        self.betweenness: Dict[str, float] = {}
        self.top_tainted: List[dict] = []
        self.top_bridges: List[dict] = []
        self.evolution_log: deque = deque(maxlen=12)
        self.community_shifts_total: int = 0

    def reset(self):
        self.communities.clear()
        self.prev_communities = []
        self.prev_modularity = 0.0
        self.modularity = 0.0
        self.num_communities = 0
        self.risk_taint.clear()
        self.betweenness.clear()
        self.top_tainted = []
        self.top_bridges = []
        self.evolution_log.clear()
        self.community_shifts_total = 0

    def _track_community_evolution(self, current_comms: List[Set[str]], now: float):
        """Compares current Louvain partition against previous snapshot (LSGA.txt Improvement #2)."""
        if not self.prev_communities:
            self.prev_communities = [set(c) for c in current_comms]
            self.prev_modularity = self.modularity
            return

        prev_count = len(self.prev_communities)
        curr_count = len(current_comms)
        q_delta = round(self.modularity - self.prev_modularity, 4)

        # Measure Jaccard overlap of largest cluster to spot splits/merges/births
        best_jaccard = 0.0
        if current_comms and self.prev_communities:
            top_curr = current_comms[0]
            for old_c in self.prev_communities[:5]:
                union = len(top_curr | old_c)
                if union > 0:
                    best_jaccard = max(best_jaccard, len(top_curr & old_c) / union)

        event_label = None
        if curr_count > prev_count + 1:
            event_label = f"Cluster Split / New Births ({prev_count} -> {curr_count} clusters)"
        elif curr_count < prev_count - 1:
            event_label = f"Cluster Merge ({prev_count} -> {curr_count} clusters)"
        elif abs(q_delta) >= 0.04:
            direction = "tightened" if q_delta > 0 else "loosened"
            event_label = f"Modularity {direction} (ΔQ = {q_delta:+.3f})"

        if event_label:
            self.community_shifts_total += 1
            self.evolution_log.appendleft(
                {
                    "timestamp": now,
                    "event": event_label,
                    "communities": curr_count,
                    "modularity": self.modularity,
                    "q_delta": q_delta,
                    "top_cluster_stability": round(best_jaccard * 100.0, 1),
                }
            )

        self.prev_communities = [set(c) for c in current_comms]
        self.prev_modularity = self.modularity

    def maybe_recompute(
        self,
        G: nx.DiGraph,
        anomaly_seeds: Dict[str, float],
        force: bool = False,
    ) -> bool:
        now = time.time()
        if not force and (now - self.last_computed_at) < self.recompute_interval_sec:
            return False

        self.last_computed_at = now
        n = G.number_of_nodes()
        if n == 0:
            self.communities.clear()
            self.risk_taint.clear()
            self.betweenness.clear()
            self.modularity = 0.0
            self.num_communities = 0
            self.top_tainted = []
            self.top_bridges = []
            return True

        # 1. Louvain Community Detection + Temporal Evolution
        undirected = G.to_undirected(as_view=False)
        try:
            if undirected.number_of_edges() > 0:
                comms = list(nx.community.louvain_communities(undirected, weight="weight", seed=42))
                self.modularity = round(
                    float(nx.community.modularity(undirected, comms, weight="weight")), 4
                )
            else:
                comms = [{node} for node in undirected.nodes]
                self.modularity = 0.0

            comms.sort(key=len, reverse=True)
            self.communities = {}
            for cid, members in enumerate(comms):
                for m in members:
                    self.communities[m] = cid
            self.num_communities = len(comms)
            self._track_community_evolution(comms, now)
        except Exception:
            self.communities = {node: 0 for node in G.nodes}
            self.num_communities = 1
            self.modularity = 0.0

        # 2. Personalized PageRank Risk / Taint Propagation
        active_seeds = {k: v for k, v in anomaly_seeds.items() if k in G and v > 0}
        if active_seeds and G.number_of_edges() > 0:
            total_seed = sum(active_seeds.values())
            personalization = {node: (active_seeds.get(node, 0.0) / total_seed) for node in G.nodes}
            try:
                ppr = nx.pagerank(G, alpha=0.82, personalization=personalization, weight="weight", max_iter=60)
                max_val = max(ppr.values()) or 1.0
                self.risk_taint = {
                    node: round(min(100.0, (val / max_val) * 100.0), 1)
                    for node, val in ppr.items()
                }
            except Exception:
                self.risk_taint = {node: (95.0 if node in active_seeds else 0.0) for node in G.nodes}
        else:
            try:
                pr = nx.pagerank(G, alpha=0.85, weight="weight", max_iter=40) if G.number_of_edges() > 0 else {}
                max_pr = max(pr.values()) if pr else 1.0
                self.risk_taint = {
                    node: round((pr.get(node, 0.0) / (max_pr or 1.0)) * 20.0, 1)
                    for node in G.nodes
                }
            except Exception:
                self.risk_taint = {node: 0.0 for node in G.nodes}

        # 3. Betweenness Centrality Hubs
        try:
            k_sample = min(40, n) if n > 40 else None
            bc = nx.betweenness_centrality(G, k=k_sample, normalized=True, seed=42)
            self.betweenness = {node: round(float(val), 4) for node, val in bc.items()}
        except Exception:
            self.betweenness = {node: 0.0 for node in G.nodes}

        self.top_tainted = [
            {
                "node": node,
                "label": G.nodes[node].get("label", node[:14]),
                "risk_taint": score,
                "is_seed": node in active_seeds,
                "community_id": self.communities.get(node, 0),
            }
            for node, score in sorted(self.risk_taint.items(), key=lambda kv: kv[1], reverse=True)[:6]
            if score >= 15.0
        ]

        self.top_bridges = [
            {
                "node": node,
                "label": G.nodes[node].get("label", node[:14]),
                "betweenness": score,
                "degree": G.degree(node),
                "community_id": self.communities.get(node, 0),
            }
            for node, score in sorted(self.betweenness.items(), key=lambda kv: kv[1], reverse=True)[:5]
            if score > 0.0
        ]
        return True

    def get_node_metrics(self, node_id: str) -> dict:
        return {
            "community_id": self.communities.get(node_id, 0),
            "risk_taint": self.risk_taint.get(node_id, 0.0),
            "betweenness": self.betweenness.get(node_id, 0.0),
        }

    def get_summary(self) -> dict:
        return {
            "num_communities": self.num_communities,
            "modularity": self.modularity,
            "community_shifts_total": self.community_shifts_total,
            "evolution_log": list(self.evolution_log)[:6],
            "top_tainted": self.top_tainted,
            "top_bridges": self.top_bridges,
        }
