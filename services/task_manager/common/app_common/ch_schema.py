from __future__ import annotations

"""ClickHouse DDL for live pipelines — one source for every component.

task_manager creates a pipeline's table when the pipeline is created; the
consumer pool makes sure it exists before writing. Both call these functions, so
the two can never disagree about what the table looks like.

Tables are built from `PipelineConfig.table_schema`. Nothing here inspects an
event: inferring columns from the first event seen is the collision bug this
phase removes.
"""

from common.app_common.models import PipelineConfig

DLQ_TABLE = "dead_letter_events"


def create_database_ddl(database: str) -> str:
    return f"CREATE DATABASE IF NOT EXISTS {database}"


def create_table_ddl(database: str, config: PipelineConfig) -> str:
    columns = ", ".join(
        f"`{name}` {ch_type}" for name, ch_type in config.table_schema.items()
    )
    order_by = "event_time" if "event_time" in config.table_schema else "tuple()"
    return (
        f"CREATE TABLE IF NOT EXISTS {database}.`{config.ch_unique_identifier}` "
        f"({columns}) ENGINE = MergeTree ORDER BY {order_by}"
    )


def create_dlq_table_ddl(database: str) -> str:
    """One shared dead-letter table. The schema is fixed, unlike pipeline tables,
    so there is nothing to gain from one per pipeline — `pipeline_id` is a column
    and leads the sort key."""
    return f"""
CREATE TABLE IF NOT EXISTS {database}.{DLQ_TABLE} (
    failed_at     DateTime64(3) DEFAULT now64(3),
    pipeline_id   LowCardinality(String),
    topic         LowCardinality(String),
    partition     Int32,
    offset        Int64,
    target_table  String,
    error_message String,
    payload       String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(failed_at)
ORDER BY (pipeline_id, failed_at)
"""
