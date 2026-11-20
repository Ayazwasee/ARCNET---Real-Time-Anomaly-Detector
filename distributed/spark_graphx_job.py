"""
Milestone 4 — Distributed Graph Processing: Pregel / BSP & Spark GraphX Worker (Module 6).

Supports two execution modes over recorded ARCNET JSONL transaction dumps:
1. `--mode bsp-sim` (Zero-install pure-Python Bulk Synchronous Parallel / Pregel simulator):
   Demonstrates vertex-centric Supersteps (`send_messages -> aggregate -> compute -> vote_to_halt`)
   for Global PageRank and Connected Components directly on your laptop or Codespace.
2. `--mode spark` (PySpark + GraphFrames / GraphX distributed cluster job):
   Runs distributed PageRank and Connected Components inside Apache Spark / Docker.

Usage:
    python3 distributed/spark_graphx_job.py --mode bsp-sim --input data/recorded_stream.jsonl
    spark-submit distributed/spark_graphx_job.py --mode spark --input data/recorded_stream.jsonl
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Set, Tuple


def load_edges_from_jsonl(path: Path) -> Tuple[Set[str], List[Tuple[str, str, float]]]:
    nodes: Set[str] = set()
    edges: List[Tuple[str, str, float]] = []

    if not path.exists():
        # Built-in sample graph if no recorded file exists yet
        sample = [
            ("W_Exchange", "W_A", 4.5), ("W_Exchange", "W_B", 3.1), ("W_Exchange", "W_C", 2.8),
            ("W_A", "W_D", 2.0), ("W_D", "W_E", 1.9), ("W_E", "W_A", 1.8),
            ("W_B", "W_F", 1.2), ("W_F", "W_G", 1.1), ("W_H", "W_I", 0.5),
        ]
        for u, v, w in sample:
            nodes.update([u, v])
            edges.append((u, v, w))
        return nodes, edges

    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            tx = json.loads(line)
            senders = [i.get("prev_out", {}).get("addr") for i in tx.get("inputs", []) if i.get("prev_out", {}).get("addr")]
            receivers = [(o.get("addr"), (o.get("value", 0) or 0) / 1e8) for o in tx.get("out", []) if o.get("addr")]
            if not senders or not receivers:
                continue
            u = senders[0]
            for v, btc in receivers[:10]:
                if u != v:
                    nodes.update([u, v])
                    edges.append((u, v, btc))
    return nodes, edges


def run_pregel_bsp_simulation(nodes: Set[str], edges: List[Tuple[str, str, float]], max_supersteps: int = 12):
    """
    Vertex-centric Bulk Synchronous Parallel (Pregel) implementation of:
    - Global PageRank
    - Weakly Connected Components (minimum-ID label propagation)
    """
    print(f"\n[Pregel-BSP] Loaded {len(nodes)} vertices, {len(edges)} directed edges.")
    out_adj: Dict[str, List[str]] = defaultdict(list)
    undir_adj: Dict[str, Set[str]] = defaultdict(set)
    for u, v, _ in edges:
        out_adj[u].append(v)
        undir_adj[u].add(v)
        undir_adj[v].add(u)

    # --- Job 1: Pregel PageRank ---
    n = max(1, len(nodes))
    damping = 0.85
    pr: Dict[str, float] = {u: 1.0 / n for u in nodes}

    for step in range(1, max_supersteps + 1):
        inbox: Dict[str, float] = defaultdict(float)
        # Superstep: Send messages along outgoing edges
        for u in nodes:
            nbrs = out_adj.get(u, [])
            if nbrs:
                share = pr[u] / len(nbrs)
                for v in nbrs:
                    inbox[v] += share

        # Barrier Synchronization + Vertex Compute
        max_delta = 0.0
        new_pr: Dict[str, float] = {}
        for u in nodes:
            val = (1.0 - damping) / n + damping * inbox[u]
            max_delta = max(max_delta, abs(val - pr[u]))
            new_pr[u] = val
        pr = new_pr
        print(f"  [PageRank Superstep {step:02d}] max_delta = {max_delta:.6f}")
        if max_delta < 1e-4:
            print(f"  [PageRank] All vertices voted to halt at superstep {step}.")
            break

    top_pr = sorted(pr.items(), key=lambda kv: kv[1], reverse=True)[:5]
    print("\n[Pregel-BSP] Top 5 Wallets by Global PageRank:")
    for addr, score in top_pr:
        print(f"  - {addr[:18]}... : {score:.5f}")

    # --- Job 2: Pregel Connected Components ---
    comp_id: Dict[str, str] = {u: u for u in nodes}
    active: Set[str] = set(nodes)

    for step in range(1, max_supersteps + 1):
        inbox_cc: Dict[str, List[str]] = defaultdict(list)
        for u in active:
            for nb in undir_adj[u]:
                inbox_cc[nb].append(comp_id[u])

        next_active: Set[str] = set()
        for u, msgs in inbox_cc.items():
            best = min(msgs)
            if best < comp_id[u]:
                comp_id[u] = best
                next_active.add(u)

        print(f"  [Components Superstep {step:02d}] active_vertices = {len(next_active)}")
        active = next_active
        if not active:
            print(f"  [Components] Converged & halted at superstep {step}.")
            break

    unique_comps = len(set(comp_id.values()))
    print(f"\n[Pregel-BSP] Total Connected Components Found: {unique_comps}")


def run_spark_graphx_job(input_path: Path):
    """Distributed PySpark + GraphFrames / RDD Pregel job for multi-node clusters."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("ARCNET-Phase2-GraphX").getOrCreate()
    nodes, edges = load_edges_from_jsonl(input_path)

    vertices_df = spark.createDataFrame([(u,) for u in nodes], ["id"])
    edges_df = spark.createDataFrame(edges, ["src", "dst", "weight"])

    print(f"[Spark] Distributed DataFrames ready: {vertices_df.count()} vertices, {edges_df.count()} edges.")

    try:
        from graphframes import GraphFrame
        g = GraphFrame(vertices_df, edges_df)
        pr_result = g.pageRank(resetProbability=0.15, maxIter=10)
        print("[Spark GraphFrames] Top PageRank Wallets:")
        pr_result.vertices.orderBy("pagerank", ascending=False).show(10, truncate=False)
    except ImportError:
        print("[Spark] GraphFrames package not attached; running native PySpark RDD PageRank...")
        links = edges_df.rdd.map(lambda r: (r.src, r.dst)).distinct().groupByKey().cache()
        ranks = links.mapValues(lambda _: 1.0)
        for _ in range(10):
            contribs = links.join(ranks).flatMap(
                lambda pair: [(dst, pair[1][1] / len(pair[1][0])) for dst in pair[1][0]]
            )
            ranks = contribs.reduceByKey(lambda a, b: a + b).mapValues(lambda r: 0.15 + 0.85 * r)
        for addr, score in ranks.takeOrdered(10, key=lambda x: -x[1]):
            print(f"  - {addr[:18]}... : {score:.5f}")

    spark.stop()


def main():
    parser = argparse.ArgumentParser(description="ARCNET Phase 2 Distributed Pregel/Spark Worker")
    parser.add_argument("--mode", choices=["bsp-sim", "spark"], default="bsp-sim")
    parser.add_argument("--input", default="data/recorded_stream.jsonl")
    args = parser.parse_args()

    path = Path(args.input)
    if args.mode == "spark":
        run_spark_graphx_job(path)
    else:
        nodes, edges = load_edges_from_jsonl(path)
        run_pregel_bsp_simulation(nodes, edges)


if __name__ == "__main__":
    main()
