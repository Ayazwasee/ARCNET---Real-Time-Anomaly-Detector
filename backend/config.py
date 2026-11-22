"""Centralized configuration for ARCNET Stage 3."""

import os
from dataclasses import dataclass

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


@dataclass
class Settings:
    # Live Multi-Source Endpoints
    blockchain_ws_url: str = os.getenv("BLOCKCHAIN_WS_URL", "wss://ws.blockchain.info/inv")
    ripe_ris_ws_url: str = os.getenv("RIPE_RIS_WS_URL", "wss://ris-live.ripe.net/v1/ws/?client=arcnet-lsga")
    wikimedia_sse_url: str = os.getenv("WIKIMEDIA_SSE_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
    bluesky_ws_url: str = os.getenv(
        "BLUESKY_WS_URL",
        "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post",
    )
    gdelt_lastupdate_url: str = os.getenv(
        "GDELT_LASTUPDATE_URL",
        "http://data.gdeltproject.org/gdeltv2/lastupdate.txt",
    )

    # Dynamic graph defaults
    default_source: str = os.getenv("ARCNET_DEFAULT_SOURCE", "blockchain")
    window_seconds: int = int(os.getenv("ARCNET_WINDOW_SECONDS", "180"))
    max_nodes: int = int(os.getenv("ARCNET_MAX_NODES", "120"))
    whale_btc_threshold: float = float(os.getenv("ARCNET_WHALE_BTC", "2.0"))
    fanout_threshold: int = int(os.getenv("ARCNET_FANOUT_THRESHOLD", "6"))
    fanin_threshold: int = int(os.getenv("ARCNET_FANIN_THRESHOLD", "5"))
    z_threshold: float = float(os.getenv("ARCNET_Z_THRESHOLD", "2.6"))

    # Stage 2/3/4: ML & Partitioning defaults
    ml_score_threshold: float = float(os.getenv("ARCNET_ML_THRESHOLD", "74.0"))
    partition_count: int = int(os.getenv("ARCNET_PARTITIONS", "4"))

    # Neo4j AuraDB / Local Docker credentials
    neo4j_uri: str = os.getenv("NEO4J_URI", "").strip()
    neo4j_user: str = os.getenv("NEO4J_USER", "neo4j").strip()
    neo4j_password: str = os.getenv("NEO4J_PASSWORD", "").strip()
    neo4j_database: str = os.getenv("NEO4J_DATABASE", "neo4j").strip()

    # Stream recorder / replay file
    recording_path: str = os.getenv("ARCNET_RECORDING_PATH", "data/recorded_stream.jsonl")


settings = Settings()
