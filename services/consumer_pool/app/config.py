from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    redis_url: str = "redis://redis:6379/0"
    kafka_bootstrap_servers: str = "kafka:9092"

    # Pattern, not a topic list. A new pipeline's topic is picked up without
    # restarting the pool — customers create pipelines at times we do not
    # control, so restarting shared infrastructure to learn about one is not an
    # option. Discovery happens within metadata_max_age_ms, not instantly.
    topic_pattern: str = r"pipeline\..*\.events"
    metadata_max_age_ms: int = 10_000

    group_id: str = "consumer-pool"
    batch_size: int = 500
    flush_interval_seconds: float = 5.0

    clickhouse_host: str = "clickhouse"
    clickhouse_port: int = 8123
    clickhouse_username: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "data_platform"

    metrics_port: int = 9100


settings = Settings()
