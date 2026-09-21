from __future__ import annotations

"""
Shared settings — single source of truth for all three services.

All three services (task-manager, controller, watchdog) import from here:

    from common.app_common.config import settings

One .env file at the project root controls every service.
Service-specific knobs (controller_interval_s, watchdog_interval_s, etc.)
live here too — each service just reads the fields it cares about and
ignores the rest (extra="ignore").

Environment variable names map directly to field names (uppercase).
Example:  REDIS_URL=redis://redis:6379/0
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",       # unknown env vars don't cause validation errors
    )

    # ── Infrastructure ────────────────────────────────────────────────────
    redis_url:               str = "redis://172.17.0.1:6379/2"
    kafka_bootstrap_servers: str = "172.17.0.1:9092"

    # ── Runtime ───────────────────────────────────────────────────────────
    # "docker"     → DockerRuntimeAdapter  (local dev / CI)
    # "redis_only" → RedisOnlyAdapter      (unit tests, no Docker)
    runtime_mode:  str = "docker"
    docker_network: str = "data-platform_backend"
    producer_image: str = "data-platform-producer:latest"

    # ── ClickHouse (passed through to consumer containers) ────────────────
    clickhouse_host:     str = "172.17.0.1"
    clickhouse_port:     int = 8123
    clickhouse_username: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "data_platform"
    clickhouse_table:    str = "pipeline_events"

    # ── Airflow (optional — for normal/batch pipelines) ───────────────────
    airflow_enabled:  bool = False
    airflow_base_url: str  = "http://localhost:8080"
    airflow_username: str  = "airflow"
    airflow_password: str  = "airflow"

    # ── Folder watcher (task-manager only) ───────────────────────────────
    # Drop a <pipeline_id>.json file here to register a new pipeline.
    pipeline_definitions_dir: str   = "/opt/pipeline_definitions"
    watcher_poll_interval_s:  float = 5.0
    watcher_enabled:          bool  = True

    # ── Desired-state controller (controller only) ────────────────────────
    # How often the reconcile loop checks all pipelines for state drift.
    controller_interval_s: int = 3

    # ── Watchdog (watchdog only) ──────────────────────────────────────────
    # Seconds without a heartbeat before the watchdog restarts a pipeline.
    # Producer/consumer heartbeat every ~5 s → 30 s = 6 missed beats.
    watchdog_interval_s:          int = 15
    watchdog_heartbeat_timeout_s: int = 30


settings = Settings()
