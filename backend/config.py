"""Centralized configuration for ARCNET Phase 2."""

import os
from dataclasses import dataclass

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


@dataclass
class Settings:
    # Live Blockchain feed
    blockchain_ws_url: str = os.getenv("BLOCKCHAIN_WS_URL", "wss://ws.blockchain.info/inv")

    # Dynamic graph defaults
    window_seconds: int = int(os.getenv("ARCNET_WINDOW_SECONDS", "180"))
    max_nodes: int = int(os.getenv("ARCNET_MAX_NODES", "120"))
    whale_btc_threshold: float = float(os.getenv("ARCNET_WHALE_BTC", "2.0"))
    fanout_threshold: int = int(os.getenv("ARCNET_FANOUT_THRESHOLD", "6"))
    fanin_threshold: int = int(os.getenv("ARCNET_FANIN_THRESHOLD", "5"))
    z_threshold: float = float(os.getenv("ARCNET_Z_THRESHOLD", "2.6"))

    # Phase 2: ML & Partitioning defaults
    ml_score_threshold: float = float(os.getenv("ARCNET_ML_THRESHOLD", "74.0"))
    partition_count: int = int(os.getenv("ARCNET_PARTITIONS", "4"))

    # Phase 2: Neo4j AuraDB / Local Docker credentials (optional; graceful fallback if unset)
    neo4j_uri: str = os.getenv("NEO4J_URI", "").strip()
    neo4j_user: str = os.getenv("NEO4J_USER", "neo4j").strip()
    neo4j_password: str = os.getenv("NEO4J_PASSWORD", "").strip()
    neo4j_database: str = os.getenv("NEO4J_DATABASE", "neo4j").strip()

    # Phase 2: Stream recorder / replay file
    recording_path: str = os.getenv("ARCNET_RECORDING_PATH", "data/recorded_stream.jsonl")


settings = Settings()