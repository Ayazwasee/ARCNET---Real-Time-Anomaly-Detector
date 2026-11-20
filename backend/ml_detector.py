"""
Milestone 3 — Graph Machine Learning Anomaly Engine (Module 8).

Implements a lightweight, zero-heavy-dependency Graph ML pipeline ideal for real-time
execution and serverless (Vercel) deployment:
1. 8-Dimensional Dynamic Node Structural & Behavioral Embeddings
2. Pure-Python Online Isolation Forest (Liu et al. path-length anomaly scoring)
3. Per-Feature Z-Score Attribution explaining WHICH graph features triggered ML risk
"""

from __future__ import annotations

import math
import random
import statistics
from collections import deque
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import networkx as nx

FEATURE_NAMES = [
    "In-Degree (log)",
    "Out-Degree (log)",
    "Out/In Ratio",
    "Local Clustering",
    "Avg Neighbor Deg",
    "BTC Volume (log)",
    "BTC Asymmetry",
    "Risk Taint Flow",
]


def _c_factor(n: int) -> float:
    """Average path length of unsuccessful search in a Binary Search Tree (Isolation Forest normalization)."""
    if n <= 1:
        return 1.0
    if n == 2:
        return 1.0
    euler_gamma = 0.5772156649
    return 2.0 * (math.log(n - 1) + euler_gamma) - (2.0 * (n - 1) / n)


@dataclass
class _IsoNode:
    size: int
    split_attr: Optional[int] = None
    split_val: Optional[float] = None
    left: Optional["_IsoNode"] = None
    right: Optional["_IsoNode"] = None


class OnlineIsolationForest:
    """Pure-Python Isolation Forest retrained periodically on rolling node embeddings."""

    def __init__(self, n_trees: int = 36, subsample_size: int = 48, seed: int = 42):
        self.n_trees = n_trees
        self.subsample_size = subsample_size
        self.rng = random.Random(seed)
        self.trees: List[_IsoNode] = []
        self.trained_samples: int = 0
        self.feature_means: List[float] = [0.0] * len(FEATURE_NAMES)
        self.feature_stds: List[float] = [1.0] * len(FEATURE_NAMES)

    def _build_tree(self, data: List[List[float]], depth: int, max_depth: int) -> _IsoNode:
        n = len(data)
        if depth >= max_depth or n <= 1:
            return _IsoNode(size=n)

        n_dims = len(data[0])
        # Choose a random feature with non-zero variance
        candidate_dims = list(range(n_dims))
        self.rng.shuffle(candidate_dims)
        for dim in candidate_dims:
            col = [row[dim] for row in data]
            lo, hi = min(col), max(col)
            if hi - lo > 1e-9:
                split_val = self.rng.uniform(lo, hi)
                left_data = [row for row in data if row[dim] < split_val]
                right_data = [row for row in data if row[dim] >= split_val]
                if left_data and right_data:
                    return _IsoNode(
                        size=n,
                        split_attr=dim,
                        split_val=split_val,
                        left=self._build_tree(left_data, depth + 1, max_depth),
                        right=self._build_tree(right_data, depth + 1, max_depth),
                    )
        return _IsoNode(size=n)

    def _path_length(self, vec: List[float], node: _IsoNode, depth: int = 0) -> float:
        if node.split_attr is None or node.left is None or node.right is None:
            return depth + _c_factor(node.size)
        if vec[node.split_attr] < node.split_val:
            return self._path_length(vec, node.left, depth + 1)
        return self._path_length(vec, node.right, depth + 1)

    def fit(self, vectors: List[List[float]]):
        if len(vectors) < 12:
            return

        n_dims = len(vectors[0])
        self.feature_means = []
        self.feature_stds = []
        for d in range(n_dims):
            col = [v[d] for v in vectors]
            mu = statistics.mean(col)
            sd = statistics.stdev(col) if len(col) >= 2 else 1.0
            self.feature_means.append(mu)
            self.feature_stds.append(sd if sd > 1e-6 else 1.0)

        sample_n = min(self.subsample_size, len(vectors))
        max_depth = max(3, int(math.ceil(math.log2(max(2, sample_n)))))
        self.trees = []
        for _ in range(self.n_trees):
            sample = self.rng.sample(vectors, sample_n)
            self.trees.append(self._build_tree(sample, 0, max_depth))
        self.trained_samples = len(vectors)

    def score(self, vec: List[float]) -> Tuple[float, List[dict]]:
        """Returns (anomaly_score_0_to_100, top_feature_attributions)."""
        if not self.trees:
            return 0.0, []

        sample_n = min(self.subsample_size, max(2, self.trained_samples))
        avg_depth = sum(self._path_length(vec, t, 0) for t in self.trees) / len(self.trees)
        raw_score = 2.0 ** (-avg_depth / _c_factor(sample_n))
        # Map Isolation Forest [0.35, 0.85] -> intuitive [0, 100] score
        scaled = max(0.0, min(99.0, (raw_score - 0.35) / 0.45 * 100.0))

        # Per-feature Z-score attribution for explainability
        attributions = []
        for idx, name in enumerate(FEATURE_NAMES):
            z = (vec[idx] - self.feature_means[idx]) / self.feature_stds[idx]
            attributions.append(
                {
                    "feature": name,
                    "value": round(vec[idx], 3),
                    "z_score": round(z, 2),
                    "impact": round(min(100.0, abs(z) * 24.0), 1),
                }
            )
        attributions.sort(key=lambda x: abs(x["z_score"]), reverse=True)
        return round(scaled, 1), attributions[:4]


class GraphMLDetector:
    """Extracts dynamic node embeddings and runs online Isolation Forest scoring."""

    def __init__(self, ml_score_threshold: float = 74.0, retrain_every: int = 12):
        self.ml_score_threshold = ml_score_threshold
        self.retrain_every = retrain_every
        self.forest = OnlineIsolationForest(n_trees=36, subsample_size=48)
        self.embedding_buffer: deque = deque(maxlen=260)
        self.node_scores: Dict[str, float] = {}
        self.node_attributions: Dict[str, List[dict]] = {}
        self._ingest_counter = 0
        self.retrain_count = 0

    @staticmethod
    def extract_embedding(G: nx.DiGraph, node_id: str, risk_taint: float = 0.0) -> List[float]:
        """Builds an 8D structural + behavioral embedding vector for a wallet node."""
        if node_id not in G:
            return [0.0] * len(FEATURE_NAMES)

        in_deg = G.in_degree(node_id)
        out_deg = G.out_degree(node_id)
        out_in_ratio = (out_deg + 1.0) / (in_deg + 1.0)

        undirected = G.to_undirected(as_view=True)
        try:
            clustering = float(nx.clustering(undirected, node_id))
        except Exception:
            clustering = 0.0

        nbrs = list(undirected.neighbors(node_id))
        avg_nbr_deg = (sum(G.degree(nb) for nb in nbrs) / len(nbrs)) if nbrs else 0.0

        data = G.nodes[node_id]
        in_btc = float(data.get("total_in_btc", 0.0))
        out_btc = float(data.get("total_out_btc", 0.0))
        total_btc = in_btc + out_btc
        btc_asym = (out_btc - in_btc) / (total_btc + 1e-6)

        return [
            math.log1p(in_deg),
            math.log1p(out_deg),
            min(12.0, out_in_ratio),
            clustering,
            math.log1p(avg_nbr_deg),
            math.log1p(total_btc),
            btc_asym,
            risk_taint / 100.0,
        ]

    def evaluate_node(self, G: nx.DiGraph, node_id: str, risk_taint: float = 0.0) -> Tuple[float, List[dict]]:
        vec = self.extract_embedding(G, node_id, risk_taint)
        self.embedding_buffer.append(vec)
        self._ingest_counter += 1

        if (
            len(self.embedding_buffer) >= 18
            and (not self.forest.trees or self._ingest_counter % self.retrain_every == 0)
        ):
            self.forest.fit(list(self.embedding_buffer))
            self.retrain_count += 1

        if not self.forest.trees:
            return 0.0, []

        score, attrs = self.forest.score(vec)
        self.node_scores[node_id] = score
        self.node_attributions[node_id] = attrs
        return score, attrs

    def get_summary(self) -> dict:
        top_ml = [
            {
                "node": n,
                "ml_score": s,
                "top_feature": (self.node_attributions.get(n) or [{}])[0].get("feature", "N/A"),
            }
            for n, s in sorted(self.node_scores.items(), key=lambda kv: kv[1], reverse=True)[:5]
            if s > 0
        ]
        return {
            "model": "Online Isolation Forest (8D Structural Embeddings)",
            "ready": bool(self.forest.trees),
            "buffer_size": len(self.embedding_buffer),
            "retrain_count": self.retrain_count,
            "ml_threshold": self.ml_score_threshold,
            "feature_names": FEATURE_NAMES,
            "top_ml_nodes": top_ml,
        }
