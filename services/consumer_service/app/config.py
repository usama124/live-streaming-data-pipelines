from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    pipeline_id: str = "123456789"
    redis_url: str = "redis://localhost:6379/2"

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "12345678"
    kafka_group_id: str = "consumer_group"

    # Controls Quix commit_every (flush after N messages)
    batch_size: int = 500
    # Controls Quix commit_interval (flush after N seconds, whichever fires first)
    flush_interval_seconds: int = 60

    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_username: str = "chadmin"
    clickhouse_password: str = "chadmin123"
    clickhouse_database: str = "default"
    clickhouse_table: str = "pipeline_events"

    heartbeat_interval_seconds: int = 5


settings = Settings()
