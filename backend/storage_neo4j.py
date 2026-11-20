"""
Milestone 1 — Persistent Property Graph Storage & Cypher Forensics (Module 2 & 7).

Supports Neo4j AuraDB / local Docker Neo4j via the official async `neo4j` driver (5.x/6.x).
If NEO4J_URI / NEO4J_PASSWORD are not configured (e.g. quick local run or Vercel preview),
it degrades gracefully to an in-memory forensic mirror while still exposing the exact
Cypher queries and peel-chain / wash-loop forensic results.
"""

from __future__ import annotations

import time
from collections import deque
from typing import Any, Dict, List, Optional

import networkx as nx

try:
    from neo4j import AsyncGraphDatabase, RoutingControl
    NEO4J_DRIVER_AVAILABLE = True
except Exception:
    AsyncGraphDatabase = None
    RoutingControl = None
    NEO4J_DRIVER_AVAILABLE = False


CYPHER_PEEL_CHAIN = """
MATCH p = (a:Wallet)-[r1:SENT]->(b:Wallet)-[r2:SENT]->(c:Wallet)-[r3:SENT]->(d:Wallet)
WHERE a.address <> c.address AND b.address <> d.address
  AND r1.timestamp <= r2.timestamp AND r2.timestamp <= r3.timestamp
  AND (r3.timestamp - r1.timestamp) <= $max_span_sec
RETURN [n IN nodes(p) | n.address] AS chain,
       [r IN relationships(p) | r.btc] AS amounts,
       (r3.timestamp - r1.timestamp) AS span_sec
ORDER BY span_sec ASC
LIMIT $limit
""".strip()

CYPHER_WASH_LOOP = """
MATCH p = (w:Wallet)-[r:SENT*3..5]->(w)
WITH w, p, [rel IN relationships(p) | rel.btc] AS amounts
RETURN w.address AS origin,
       [n IN nodes(p) | n.address] AS cycle_nodes,
       length(p) AS hops,
       reduce(s = 0.0, x IN amounts | s + x) AS total_btc
ORDER BY total_btc DESC
LIMIT $limit
""".strip()

CYPHER_WALLET_HISTORY = """
MATCH (w:Wallet {address: $address})
OPTIONAL MATCH (a:Anomaly)-[:FLAGS]->(w)
OPTIONAL MATCH (w)-[out_r:SENT]->(dst:Wallet)
OPTIONAL MATCH (src:Wallet)-[in_r:SENT]->(w)
RETURN w.address AS address,
       collect(DISTINCT {id: a.id, type: a.type, score: a.score, timestamp: a.timestamp, summary: a.summary}) AS anomalies,
       count(DISTINCT out_r) AS out_txs,
       count(DISTINCT in_r) AS in_txs,
       coalesce(sum(out_r.btc), 0.0) AS total_out_btc,
       coalesce(sum(in_r.btc), 0.0) AS total_in_btc
""".strip()


class Neo4jForensicStore:
    """Async Neo4j Property Graph Store with automatic in-memory fallback."""

    def __init__(
        self,
        uri: str = "",
        user: str = "neo4j",
        password: str = "",
        database: str = "neo4j",
    ):
        self.uri = uri
        self.user = user
        self.password = password
        self.database = database
        self.driver = None
        self.connected = False
        self.mode = "in-memory-fallback"
        self.last_error: Optional[str] = None

        # Lightweight persistent-in-session mirror for instant fallback forensics
        self._mirror_graph = nx.DiGraph()
        self._archived_alerts: deque = deque(maxlen=250)
        self._wallet_alerts: Dict[str, List[dict]] = {}

    async def connect(self) -> bool:
        """Attempts async Bolt/Neo4j connection and initializes indexes."""
        if not self.uri or not self.password or not NEO4J_DRIVER_AVAILABLE:
            self.connected = False
            self.mode = "in-memory-fallback"
            return False

        try:
            self.driver = AsyncGraphDatabase.driver(
                self.uri,
                auth=(self.user, self.password),
                connection_timeout=5.0,
            )
            await self.driver.verify_connectivity()
            await self._ensure_constraints()
            self.connected = True
            self.mode = "neo4j"
            self.last_error = None
            return True
        except Exception as exc:
            self.last_error = str(exc)
            self.connected = False
            self.mode = "in-memory-fallback"
            if self.driver is not None:
                try:
                    await self.driver.close()
                except Exception:
                    pass
                self.driver = None
            return False

    async def close(self):
        if self.driver is not None:
            try:
                await self.driver.close()
            except Exception:
                pass
            self.driver = None
        self.connected = False

    async def _ensure_constraints(self):
        if not self.driver:
            return
        queries = [
            "CREATE CONSTRAINT wallet_addr_unique IF NOT EXISTS FOR (w:Wallet) REQUIRE w.address IS UNIQUE",
            "CREATE CONSTRAINT anomaly_id_unique IF NOT EXISTS FOR (a:Anomaly) REQUIRE a.id IS UNIQUE",
        ]
        for q in queries:
            try:
                await self.driver.execute_query(q, database_=self.database)
            except Exception:
                pass

    def status_summary(self) -> dict:
        return {
            "mode": self.mode,
            "connected": self.connected,
            "driver_installed": NEO4J_DRIVER_AVAILABLE,
            "mirror_nodes": self._mirror_graph.number_of_nodes(),
            "mirror_edges": self._mirror_graph.number_of_edges(),
            "persisted_anomalies": len(self._archived_alerts),
            "last_error": self.last_error,
        }

    def record_edges_local(self, edges: List[dict]):
        """Maintains a rolling historical subgraph (up to 600 nodes) for fallback forensics."""
        now = time.time()
        for e in edges:
            u, v = e["source"], e["target"]
            btc = float(e.get("weight", 0.0))
            ts = float(e.get("timestamp", now))
            tx_hash = e.get("tx_hash", "")
            self._mirror_graph.add_node(u, last_seen=ts)
            self._mirror_graph.add_node(v, last_seen=ts)
            if self._mirror_graph.has_edge(u, v):
                self._mirror_graph[u][v]["weight"] += btc
                self._mirror_graph[u][v]["timestamp"] = ts
                self._mirror_graph[u][v]["count"] += 1
            else:
                self._mirror_graph.add_edge(u, v, weight=btc, timestamp=ts, tx_hash=tx_hash, count=1)

        # Cap memory cleanly for serverless environments
        if self._mirror_graph.number_of_nodes() > 600:
            oldest = sorted(
                self._mirror_graph.nodes(data=True),
                key=lambda item: item[1].get("last_seen", 0.0),
            )[:120]
            self._mirror_graph.remove_nodes_from([n for n, _ in oldest])

    async def persist_anomaly(self, alert: dict, ego_edges: Optional[List[dict]] = None):
        """Persists an anomaly + its 1-2 hop neighbourhood into Neo4j (and local mirror)."""
        self._archived_alerts.appendleft(alert)
        node_id = alert.get("primary_node", "")
        if node_id:
            self._wallet_alerts.setdefault(node_id, []).insert(0, alert)
            self._wallet_alerts[node_id] = self._wallet_alerts[node_id][:25]

        if ego_edges:
            self.record_edges_local(ego_edges)

        if not self.connected or not self.driver:
            return

        cypher_alert = """
        MERGE (w:Wallet {address: $primary_node})
        SET w.short_label = $short_node,
            w.last_flagged = $timestamp,
            w.is_anomaly = true
        MERGE (a:Anomaly {id: $id})
        SET a.type = $type,
            a.severity = $severity,
            a.score = $score,
            a.summary = $summary,
            a.tx_hash = $tx_hash,
            a.timestamp = $timestamp
        MERGE (a)-[:FLAGS]->(w)
        """
        try:
            await self.driver.execute_query(
                cypher_alert,
                id=alert["id"],
                primary_node=alert["primary_node"],
                short_node=alert.get("short_node", ""),
                type=alert.get("type", "Anomaly"),
                severity=alert.get("severity", "medium"),
                score=float(alert.get("score", 0)),
                summary=alert.get("summary", ""),
                tx_hash=alert.get("tx_hash", ""),
                timestamp=float(alert.get("timestamp", time.time())),
                database_=self.database,
            )

            if ego_edges:
                cypher_edges = """
                UNWIND $edges AS row
                MERGE (u:Wallet {address: row.source})
                MERGE (v:Wallet {address: row.target})
                MERGE (u)-[r:SENT {tx_hash: row.tx_hash}]->(v)
                SET r.btc = row.weight,
                    r.timestamp = row.timestamp
                """
                clean_edges = [
                    {
                        "source": e["source"],
                        "target": e["target"],
                        "weight": float(e.get("weight", 0.0)),
                        "tx_hash": str(e.get("tx_hash", "")),
                        "timestamp": float(e.get("timestamp", time.time())),
                    }
                    for e in ego_edges[:35]
                ]
                await self.driver.execute_query(
                    cypher_edges,
                    edges=clean_edges,
                    database_=self.database,
                )
        except Exception as exc:
            self.last_error = str(exc)

    async def find_peel_chains(self, active_graph: nx.DiGraph, max_span_sec: float = 300.0, limit: int = 8) -> dict:
        """Detects 3-to-4 hop sequential fund forwarding (peel chains)."""
        if self.connected and self.driver:
            try:
                records, _, _ = await self.driver.execute_query(
                    CYPHER_PEEL_CHAIN,
                    max_span_sec=max_span_sec,
                    limit=limit,
                    database_=self.database,
                    routing_=RoutingControl.READ,
                )
                chains = [
                    {
                        "chain": r["chain"],
                        "amounts": [round(float(x), 5) for x in r["amounts"]],
                        "span_sec": round(float(r["span_sec"]), 1),
                    }
                    for r in records
                ]
                return {"source": "neo4j", "cypher": CYPHER_PEEL_CHAIN, "chains": chains}
            except Exception as exc:
                self.last_error = str(exc)

        # Fallback: scan merged active + historical mirror DiGraph for length-3 monotonic paths
        G = nx.compose(self._mirror_graph, active_graph)
        chains = []
        seen = set()

        for a in list(G.nodes):
            if len(chains) >= limit:
                break
            for b in G.successors(a):
                if b == a:
                    continue
                e1 = G[a][b]
                t1 = float(e1.get("timestamp", 0.0))
                w1 = float(e1.get("weight", 0.0))
                for c in G.successors(b):
                    if c in (a, b):
                        continue
                    e2 = G[b][c]
                    t2 = float(e2.get("timestamp", 0.0))
                    w2 = float(e2.get("weight", 0.0))
                    if t2 < t1 or (t2 - t1) > max_span_sec:
                        continue
                    for d in G.successors(c):
                        if d in (a, b, c):
                            continue
                        e3 = G[c][d]
                        t3 = float(e3.get("timestamp", 0.0))
                        w3 = float(e3.get("weight", 0.0))
                        if t3 < t2 or (t3 - t1) > max_span_sec:
                            continue
                        key = (a, b, c, d)
                        if key in seen:
                            continue
                        seen.add(key)
                        chains.append(
                            {
                                "chain": [a, b, c, d],
                                "amounts": [round(w1, 5), round(w2, 5), round(w3, 5)],
                                "span_sec": round(t3 - t1, 1),
                            }
                        )
                        if len(chains) >= limit:
                            break

        return {"source": "in-memory-fallback", "cypher": CYPHER_PEEL_CHAIN, "chains": chains}

    async def find_wash_loops(self, active_graph: nx.DiGraph, limit: int = 8) -> dict:
        """Detects 3-to-5 hop circular wash-trading / fund-return cycles."""
        if self.connected and self.driver:
            try:
                records, _, _ = await self.driver.execute_query(
                    CYPHER_WASH_LOOP,
                    limit=limit,
                    database_=self.database,
                    routing_=RoutingControl.READ,
                )
                loops = [
                    {
                        "origin": r["origin"],
                        "cycle_nodes": r["cycle_nodes"],
                        "hops": int(r["hops"]),
                        "total_btc": round(float(r["total_btc"]), 5),
                    }
                    for r in records
                ]
                return {"source": "neo4j", "cypher": CYPHER_WASH_LOOP, "loops": loops}
            except Exception as exc:
                self.last_error = str(exc)

        G = nx.compose(self._mirror_graph, active_graph)
        loops = []
        try:
            for cyc in nx.simple_cycles(G, length_bound=5):
                if len(cyc) < 3:
                    continue
                total_btc = 0.0
                for idx in range(len(cyc)):
                    u = cyc[idx]
                    v = cyc[(idx + 1) % len(cyc)]
                    if G.has_edge(u, v):
                        total_btc += float(G[u][v].get("weight", 0.0))
                loops.append(
                    {
                        "origin": cyc[0],
                        "cycle_nodes": cyc + [cyc[0]],
                        "hops": len(cyc),
                        "total_btc": round(total_btc, 5),
                    }
                )
                if len(loops) >= limit:
                    break
        except Exception:
            pass

        loops.sort(key=lambda x: x["total_btc"], reverse=True)
        return {"source": "in-memory-fallback", "cypher": CYPHER_WASH_LOOP, "loops": loops}

    async def get_wallet_history(self, address: str, active_graph: nx.DiGraph) -> dict:
        """Retrieves historical anomaly records & counterparty totals for a wallet address."""
        address = (address or "").strip()
        if not address:
            return {"source": self.mode, "cypher": CYPHER_WALLET_HISTORY, "found": False}

        if self.connected and self.driver:
            try:
                records, _, _ = await self.driver.execute_query(
                    CYPHER_WALLET_HISTORY,
                    address=address,
                    database_=self.database,
                    routing_=RoutingControl.READ,
                )
                if records:
                    r = records[0]
                    anoms = [a for a in r["anomalies"] if a.get("id")]
                    return {
                        "source": "neo4j",
                        "cypher": CYPHER_WALLET_HISTORY,
                        "found": True,
                        "address": r["address"],
                        "anomalies": anoms,
                        "out_txs": int(r["out_txs"]),
                        "in_txs": int(r["in_txs"]),
                        "total_out_btc": round(float(r["total_out_btc"]), 5),
                        "total_in_btc": round(float(r["total_in_btc"]), 5),
                    }
            except Exception as exc:
                self.last_error = str(exc)

        G = nx.compose(self._mirror_graph, active_graph)
        # Support prefix/substring match for usability
        matched_addr = address if address in G else next(
            (n for n in G.nodes if address.lower() in n.lower()), None
        )
        if not matched_addr:
            matched_addr = next(
                (k for k in self._wallet_alerts if address.lower() in k.lower()), None
            )

        if not matched_addr:
            return {
                "source": "in-memory-fallback",
                "cypher": CYPHER_WALLET_HISTORY,
                "found": False,
                "address": address,
            }

        out_edges = list(G.out_edges(matched_addr, data=True)) if matched_addr in G else []
        in_edges = list(G.in_edges(matched_addr, data=True)) if matched_addr in G else []
        return {
            "source": "in-memory-fallback",
            "cypher": CYPHER_WALLET_HISTORY,
            "found": True,
            "address": matched_addr,
            "anomalies": self._wallet_alerts.get(matched_addr, []),
            "out_txs": len(out_edges),
            "in_txs": len(in_edges),
            "total_out_btc": round(sum(float(d.get("weight", 0.0)) for _, _, d in out_edges), 5),
            "total_in_btc": round(sum(float(d.get("weight", 0.0)) for _, _, d in in_edges), 5),
        }