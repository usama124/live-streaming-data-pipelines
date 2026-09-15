import json

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    pipeline_id: str = "123456789"
    source_type: str = "mock"
    redis_url: str = "redis://localhost:6379/0"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "12345678"

    mock_min_delay_ms: int = 100
    mock_max_delay_ms: int = 500

    heartbeat_interval_seconds: int = 5

    # ── OPC UA ──────────────────────────────────────────────────────────────
    # Endpoint of the OPC UA server this producer should connect to.
    # Example: opc.tcp://opcua-server:4840/stratahub/server/
    opcua_endpoint: str = "opc.tcp://localhost:4840/stratahub/server/"

    # Comma-separated NodeID strings.
    # Example: ns=2;i=2,ns=2;i=3,ns=2;i=4
    opcua_node_ids: str = ""

    # Optional JSON object mapping NodeID → display name.
    # Example: '{"ns=2;i=2":"Temperature","ns=2;i=3":"Pressure"}'
    opcua_node_names_json: str = "{}"

    # Server publish interval in milliseconds.
    opcua_publishing_interval_ms: int = 500

    # Reconnect backoff bounds.
    opcua_reconnect_base_s: float = 2.0
    opcua_reconnect_max_s: float = 60.0

    # Internal event buffer depth before back-pressure kicks in.
    opcua_queue_maxsize: int = 1000

    @field_validator("opcua_node_ids", mode="before")
    @classmethod
    def _strip_node_ids(cls, v: str) -> str:
        return v.strip()

    def opcua_source_options(self) -> dict:
        """Return a source_options dict ready to pass to OpcUaStreamingSource."""
        node_ids = [n.strip() for n in self.opcua_node_ids.split(",") if n.strip()]
        try:
            node_names: dict = json.loads(self.opcua_node_names_json)
        except json.JSONDecodeError:
            node_names = {}

        return {
            "endpoint": self.opcua_endpoint,
            "node_ids": node_ids,
            "node_names": node_names,
            "publishing_interval_ms": self.opcua_publishing_interval_ms,
            "reconnect_base_s": self.opcua_reconnect_base_s,
            "reconnect_max_s": self.opcua_reconnect_max_s,
            "queue_maxsize": self.opcua_queue_maxsize,
        }


settings = Settings()
