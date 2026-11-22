"""
Stage 4 — Graph Partitioning & Processing-Cost Benchmark (Module 3 & LSGA.txt Improvement #3).

Compares two core large-scale graph partitioning strategies on the live evolving graph:
1. Edge-Cut Partitioning (Linear Deterministic Greedy / LDG): assigns vertices to K shards;
   cross-shard edges incur network message passing (measured by Edge-Cut Ratio & Network Comm Cost).
2. Vertex-Cut Partitioning (PowerGraph-style Greedy): assigns edges to K shards and replicates
   high-degree hub vertices across shards (measured by Replication Factor & State Sync Overhead).
"""

from __future__ import annotations

from collections import defaultdict
from typing import Dict, Set

import networkx as nx


class GraphPartitioner:
    """Computes live Edge-Cut vs. Vertex-Cut partitioning metrics & processing requirements."""

    def __init__(self, num_partitions: int = 4):
        self.num_partitions = max(2, min(int(num_partitions), 8))
        self.node_partition: Dict[str, int] = {}
        self.last_report: dict = {}

    def compute(self, G: nx.DiGraph, k: int | None = None) -> dict:
        if k is not None:
            self.num_partitions = max(2, min(int(k), 8))

        K = self.num_partitions
        n = G.number_of_nodes()
        m = G.number_of_edges()

        if n == 0 or m == 0:
            self.node_partition = {}
            self.last_report = {
                "k": K,
                "nodes": n,
                "edges": m,
                "power_law_skew": 1.0,
                "edge_cut": {
                    "cut_edges": 0,
                    "cut_ratio_pct": 0.0,
                    "balance_ratio": 1.0,
                    "comm_cost_units": 0,
                    "shard_loads": [0] * K,
                },
                "vertex_cut": {
                    "replication_factor": 1.0,
                    "mirrored_vertices": 0,
                    "balance_ratio": 1.0,
                    "sync_cost_units": 0,
                    "shard_loads": [0] * K,
                },
                "recommendation": "Waiting for active edges to benchmark sharding...",
            }
            return self.last_report

        # 1. Edge-Cut (Linear Deterministic Greedy - LDG)
        max_cap = max(1.0, (n / K) * 1.15)
        ec_assign: Dict[str, int] = {}
        ec_loads = [0] * K
        undirected = G.to_undirected(as_view=True)

        ordered_nodes = sorted(G.nodes, key=lambda u: G.degree(u), reverse=True)
        for u in ordered_nodes:
            nbr_counts = [0] * K
            for nb in undirected.neighbors(u):
                p = ec_assign.get(nb)
                if p is not None:
                    nbr_counts[p] += 1

            best_p = 0
            best_score = -1e9
            for p in range(K):
                penalty = max(0.05, 1.0 - (ec_loads[p] / max_cap))
                score = (nbr_counts[p] + 0.1) * penalty
                if score > best_score:
                    best_score = score
                    best_p = p
            ec_assign[u] = best_p
            ec_loads[best_p] += 1

        cut_edges = sum(1 for u, v in G.edges if ec_assign[u] != ec_assign[v])
        cut_ratio = round((cut_edges / m) * 100.0, 2)
        ec_balance = round(max(ec_loads) / max(1e-6, (n / K)), 2)
        # Processing requirement: each cut edge requires 2 cross-worker RPC messages per superstep
        ec_comm_cost = cut_edges * 2

        # 2. Vertex-Cut (PowerGraph Greedy Edge Assignment)
        vc_vertex_mirrors: Dict[str, Set[int]] = defaultdict(set)
        vc_edge_loads = [0] * K
        avg_edges = max(1.0, m / K)

        for u, v in G.edges:
            pu = vc_vertex_mirrors[u]
            pv = vc_vertex_mirrors[v]
            common = pu.intersection(pv)
            if common:
                chosen = min(common, key=lambda p: vc_edge_loads[p])
            elif pu or pv:
                candidates = pu if (pu and (not pv or G.degree(u) <= G.degree(v))) else pv
                chosen = min(candidates, key=lambda p: vc_edge_loads[p])
            else:
                chosen = min(range(K), key=lambda p: vc_edge_loads[p])

            if vc_edge_loads[chosen] > avg_edges * 1.45:
                chosen = min(range(K), key=lambda p: vc_edge_loads[p])

            vc_edge_loads[chosen] += 1
            vc_vertex_mirrors[u].add(chosen)
            vc_vertex_mirrors[v].add(chosen)

        total_replicas = sum(len(s) for s in vc_vertex_mirrors.values())
        replication_factor = round(total_replicas / max(1, len(vc_vertex_mirrors)), 3)
        mirrored_vertices = sum(1 for s in vc_vertex_mirrors.values() if len(s) > 1)
        vc_balance = round(max(vc_edge_loads) / max(1e-6, (m / K)), 2)
        # Processing requirement: only mirrored vertices synchronize state across their replica shards
        vc_sync_cost = sum(max(0, len(s) - 1) for s in vc_vertex_mirrors.values())

        self.node_partition = ec_assign

        degrees = [d for _, d in G.degree()]
        max_deg = max(degrees) if degrees else 0
        avg_deg = (sum(degrees) / len(degrees)) if degrees else 1.0
        skew_ratio = round(max_deg / max(0.1, avg_deg), 2)

        if vc_sync_cost < ec_comm_cost or skew_ratio >= 2.8:
            rec = (
                f"Vertex-Cut wins (Sync Cost {vc_sync_cost} vs Edge-Cut Comm {ec_comm_cost} msgs): "
                f"Hub skew = {skew_ratio}x, Replication Factor = {replication_factor}."
            )
        else:
            rec = (
                f"Edge-Cut (LDG) is efficient ({cut_ratio}% cut, {ec_comm_cost} msgs vs "
                f"Vertex-Cut {vc_sync_cost} sync ops, RF = {replication_factor})."
            )

        self.last_report = {
            "k": K,
            "nodes": n,
            "edges": m,
            "power_law_skew": skew_ratio,
            "edge_cut": {
                "cut_edges": cut_edges,
                "cut_ratio_pct": cut_ratio,
                "balance_ratio": ec_balance,
                "comm_cost_units": ec_comm_cost,
                "shard_loads": ec_loads,
            },
            "vertex_cut": {
                "replication_factor": replication_factor,
                "mirrored_vertices": mirrored_vertices,
                "balance_ratio": vc_balance,
                "sync_cost_units": vc_sync_cost,
                "shard_loads": vc_edge_loads,
            },
            "recommendation": rec,
        }
        return self.last_report

    def get_partition(self, node_id: str) -> int:
        return self.node_partition.get(node_id, 0)
