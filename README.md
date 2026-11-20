# ARCNET: A Real Time Anomaly Detector

**Real-Time Dynamic Graph Analysis for Early Detection of Anomalous Behaviour in Evolving Networks**

ARCNET is a real-time dynamic graph analytics platform that ingests live unconfirmed Bitcoin transactions from Blockchain.com, maintains a continuously evolving directed weighted transaction graph, and flags structural and volumetric anomalies as they emerge—without rebuilding the graph from scratch.

---

## Core Concept

Traditional graph analysis builds a static snapshot once and evaluates it offline. In live transaction and communication networks, topology changes every second. ARCNET models the Bitcoin mempool as a **self-updating dynamic graph** backed by adjacency lists ($O(1)$ edge insertions and deletions) with an active **sliding time window** ($\Delta t$):

1. **Ingest:** Connects to the live Blockchain.com unconfirmed transaction WebSocket stream (`wss://ws.blockchain.info/inv`).
2. **Update & Prune:** Incrementally inserts directed weighted edges (`Sender Wallet -> Receiver Wallet`, weighted by BTC) and automatically removes expired edges outside the active window along with isolated zero-degree nodes.
3. **Detect Locally:** Runs incremental 1-hop ego-network analytics strictly around newly touched wallets so anomaly scoring stays fast under continuous traffic.
4. **Alert & Archive:** Displays an immediate pop-up explaining *what* happened and *why*; once read, the alert moves into the **Anomaly Inbox** with one-click node highlighting on the live graph.

---

## System Architecture

```text
Blockchain.com WebSocket (wss://ws.blockchain.info/inv)
   │  (Auto-Fallback Synthetic Stream if offline/blocked)
   ▼
[Stage 1] Live Stream Ingestor (backend/blockchain_graph.py)
   │  Parses UTX JSON -> (sender, receiver, weight_btc, timestamp)
   ▼
[Stage 2] Self-Updating Dynamic Graph Engine (NetworkX DiGraph)
   │  Adjacency-list upserts + Sliding-window edge/node pruning
   ▼
[Stage 3] Incremental 1-Hop Anomaly Detector
   │  Out-Degree Z-Score, Local Clustering, Edge Weight, Cluster Bridges
   ▼
[Stage 4] FastAPI Server & State Manager (backend/server.py)
   │  WebSocket /ws/stream + REST /api/alerts/{id}/read (Inbox)
   ▼
[Stage 5] Bright Minimalist Live Dashboard (frontend/index.html)
      Toggleable Cytoscape.js Graph + Real-Time Pop-Ups + Anomaly Inbox
```

---

## Key Features

- **Real-Time Blockchain Ingestion with Demo Fail-Safe:** Streams live Bitcoin mainnet unconfirmed transactions (`utx`) and automatically switches to a realistic synthetic generator if network firewalls block WebSockets.
- **Self-Updating Sliding-Window Graph:** Continuously expires stale edges older than $\Delta t$ seconds and removes isolated nodes to keep memory bounded and focus detection on current behaviour.
- **Pop-Up to Anomaly Inbox Workflow:** Every detected anomaly triggers a contextual pop-up containing the severity score, plain-English explanation, and underlying graph metrics. Clicking **Mark as Read** archives the alert in the slide-over **Anomaly Inbox**.
- **Interactive Toggleable Active Graph:** Clean, bright minimalist interface built with **Cytoscape.js**, offering smooth force-directed layouts, zoom/pan, node dragging, layout pause/resume, pulsing anomaly rings, and a floating wallet inspector panel.
- **One-Click Graph Localization:** Click **Focus Node** from an active pop-up or **Locate in Graph** inside the Anomaly Inbox to center and inspect the flagged wallet directly on the canvas.
- **Live Runtime Controls:** Adjust the sliding time window ($\Delta t$) and whale BTC threshold dynamically from the dashboard, or click **+ Test Anomaly** to inject a synthetic burst for testing and live demonstrations.

---

## Anomaly Detection Engine

ARCNET evaluates incoming transactions incrementally around the affected nodes using four graph-theoretic signals:

### 1. Fan-Out Star Burst (Centrality + Local Clustering)
Tracks a rolling mean ($\mu_{\text{out}}$) and standard deviation ($\sigma_{\text{out}}$) of out-degrees across active nodes:

$$Z_{\text{out}}(v) = \frac{\text{deg}^{+}(v) - \mu_{\text{out}}}{\sigma_{\text{out}} + \epsilon}$$

Combined with the node's local clustering coefficient $C(v)$:

$$C(v) = \frac{2 e_v}{k_v (k_v - 1)}$$

where $k_v$ is the degree of node $v$ in the undirected view and $e_v$ is the number of edges between its neighbours.

- **Trigger:** Raw output count $\ge 10$, OR active out-degree $\ge 6$ with $Z_{\text{out}}(v) \ge 2.8$ and $C(v) < 0.1$.
- **Real-World Meaning:** Automated batch payouts, crypto-mixing dispersion, or dust attacks creating star topologies with no triangular neighbour relationships.

### 2. Whale Volume Spike (Weighted Edge Outlier)
- **Trigger:** Total transaction volume exceeds the configurable BTC threshold (default $\ge 5.0\text{ BTC}$).
- **Real-World Meaning:** Institutional treasury transfers, exchange cold-wallet sweeps, or sudden liquidity movements.

### 3. Fan-In Consolidation Surge (In-Degree Centrality)
- **Trigger:** $\ge 8$ distinct input addresses converging into a primary receiving address in a single transaction.
- **Real-World Meaning:** Exchange deposit sweeps or coordinated balance aggregation across multiple source wallets.

### 4. Structural Cluster Bridge (Connectivity & Reachability)
- **Trigger:** Before inserting new edges, checks whether the transaction connects two previously disconnected connected components, each having size $\ge 3$.
- **Real-World Meaning:** Shortest-path distance between two independent wallet clusters drops from $\infty$ to $1$, signalling a cross-entity fund transfer.

---

## LSGA Concept Alignment

| Project Component | Large Scale Graph Analysis Concept |
| :--- | :--- |
| Transaction Network Model | Directed, Weighted Property Graphs |
| In-Memory Graph Storage | Adjacency List Representation (`NetworkX DiGraph`) for $O(1)$ updates |
| Cluster Bridge Detector | Graph Connectivity, Reachability, and Connected Components |
| Burst & Consolidation Detector | In-Degree / Out-Degree Centrality and Local Clustering Coefficient |
| Live Monitoring Dashboard | Interactive Real-Time Graph Visualization |

---

## Project Structure

```text
ARCNET---Real-Time-Anomaly-Detector/
├── README.md
├── requirements.txt
├── anomaly engine/
│   └── server.py              # FastAPI server, WebSocket broadcaster & Anomaly Inbox endpoints
├── backend/
│   ├── blockchain_graph.py    # WebSocket ingestion, dynamic graph engine, pruner & anomaly detector
└── frontend/
    └── index.html             # Bright minimalist UI, Cytoscape.js live graph, pop-ups & Inbox drawer
```

---

## Tech Stack

- **Backend & Async Streaming:** Python 3.10+, FastAPI, Uvicorn, `websockets`
- **Dynamic Graph Analytics:** NetworkX
- **Data Source:** Blockchain.com Real-Time WebSocket API (`wss://ws.blockchain.info/inv`)
- **Frontend & Visualization:** HTML5, CSS3, Vanilla JavaScript, Cytoscape.js

---

## Getting Started

### 1. Prerequisites
- Python **3.10 or higher**
- Internet access for the live Blockchain.com stream (offline fallback activates automatically if unavailable)

### 2. Clone & Install Dependencies
```bash
git clone <your-repo-url>
cd ARCNET---Real-Time-Anomaly-Detector
pip install -r requirements.txt
```

### 3. Launch the Server
Run Uvicorn from the project root directory:
```bash
uvicorn backend.server:app --reload
```

### 4. Open the Dashboard
Visit the local server in your browser:
```text
http://127.0.0.1:8000
```

### 5. Quick Verification Walkthrough
1. **Verify Live Ingestion:** Confirm the top-right status pill turns green (`Live · Blockchain.com Mainnet`) and KPI counters begin incrementing.
2. **Explore the Active Graph:** Drag, zoom, or click any wallet node on the canvas to open the **Wallet Node Details** inspector. Toggle the **Hide / Show Active Graph** switch to test collapsible view mode.
3. **Test Pop-Up -> Inbox Flow:** Click **+ Test Anomaly** in the header (or wait for a live mainnet burst). Read the pop-up breakdown, click **Mark as Read -> Inbox**, open the **Anomaly Inbox**, and click **Locate in Graph** to focus the flagged wallet.

---

## API & WebSocket Reference

| Protocol / Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/` | Serves the live dashboard (`frontend/index.html`) |
| `WS` | `/ws/stream` | Streams initial graph snapshot, incremental node/edge diffs, alerts, inbox updates, and feed status |
| `POST` | `/api/alerts/{alert_id}/read` | Marks an active pop-up alert as read and moves it into the Anomaly Inbox |
| `POST` | `/api/simulate-anomaly` | Injects a synthetic Fan-Out + Whale transaction for immediate testing |
| `POST` | `/api/config` | Updates `window_seconds` and `whale_btc_threshold` dynamically at runtime |

---

## Default Engine Parameters

Configured in `backend/server.py` when initializing `DynamicBlockchainGraph`:

| Parameter | Default | Description |
| :--- | :--- | :--- |
| `window_seconds` | `180` | Active sliding window duration $\Delta t$ (seconds) before edges expire |
| `max_nodes` | `110` | Soft node cap to keep browser graph rendering smooth |
| `whale_btc_threshold` | `5.0` | Minimum BTC transfer value to trigger a Whale Volume Spike alert |
| `fanout_threshold` | `10` | Minimum distinct output wallets to trigger a Fan-Out Star Burst |
| `z_threshold` | `2.8` | Rolling out-degree $Z$-score threshold for burst anomaly detection |

---

## Phase 2 Roadmap (Planned Expansion)

- **Persistent Graph Database:** Migrate active and archived graph snapshots to **Neo4j** with **Cypher** queries for multi-hop historical fund tracing.
- **Distributed Stream & Graph Processing:** Introduce partitioned message streaming and distributed graph frameworks (Spark GraphX / Pregel-style computation) to scale beyond single-machine memory.
- **Graph Machine Learning:** Integrate dynamic node embeddings and **Graph Neural Networks (GNNs)** to detect multi-hop peel chains and subtle laundering subgraphs.
- **Deterministic Stream Replay:** Log live WebSocket streams to `.jsonl` files for reproducible offline evaluation, benchmarking, and precision/recall analysis.
