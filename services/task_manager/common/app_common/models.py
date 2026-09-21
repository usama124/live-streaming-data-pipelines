from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

from common.app_common.ch_naming import ch_unique_identifier


class PipelineType(StrEnum):
    NORMAL = "normal"
    LIVE   = "live"


class PipelineStatus(StrEnum):
    CREATED  = "created"
    STARTING = "starting"
    RUNNING  = "running"
    STOPPING = "stopping"
    STOPPED  = "stopped"
    FAILED   = "failed"


class DesiredState(StrEnum):
    RUNNING = "running"
    STOPPED = "stopped"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# Columns every live pipeline gets unless it declares its own. Explicit by
# design: inferring columns from the first event seen is the schema-collision
# bug Phase 2 exists to remove. `value` is split because an OPC UA node may read
# numeric or string, and a column has one type.
DEFAULT_TABLE_SCHEMA: dict[str, str] = {
    "event_time":  "DateTime64(3)",
    "pipeline_id": "LowCardinality(String)",
    "source":      "LowCardinality(String)",
    "sensor":      "LowCardinality(String)",
    "quality":     "LowCardinality(String)",
    "node_id":     "String",
    "sequence":    "Int64",
    "value":       "Nullable(Float64)",
    "value_text":  "Nullable(String)",
    "ingested_at": "DateTime64(3)",
}


class PipelineCreateRequest(BaseModel):
    pipeline_id:   str = Field(..., min_length=1, pattern=r"^[a-zA-Z0-9_.-]+$")
    pipeline_type: PipelineType

    airflow_dag_id: str | None = None

    # ClickHouse table identity — see common/app_common/ch_naming.py
    user_id:           str | None = None
    collection_number: int | None = None
    table_name:        str | None = None
    table_schema:      dict[str, str] = Field(default_factory=lambda: dict(DEFAULT_TABLE_SCHEMA))

    source_type:            str            = "opcua"
    topic:                  str | None     = None
    batch_size:             int            = Field(default=500, ge=1, le=100_000)
    flush_interval_seconds: int            = Field(default=60, ge=1, le=3600)
    source_options:         dict[str, Any] = Field(default_factory=dict)


class PipelineConfig(BaseModel):
    pipeline_id:            str
    pipeline_type:          PipelineType
    airflow_dag_id:         str | None     = None

    # ClickHouse table identity. Separate from `topic`, which is an internal
    # routing id — the consumer pool resolves topic -> config -> table and must
    # never derive one name from the other.
    user_id:                str            = "0"
    collection_number:      int            = 1
    table_name:             str            = "events"
    table_schema:           dict[str, str] = Field(default_factory=lambda: dict(DEFAULT_TABLE_SCHEMA))

    source_type:            str            = "opcua"
    topic:                  str
    batch_size:             int            = 500
    flush_interval_seconds: int            = 60
    source_options:         dict[str, Any] = Field(default_factory=dict)
    created_at:             str            = Field(default_factory=utc_now_iso)
    updated_at:             str            = Field(default_factory=utc_now_iso)

    @property
    def ch_unique_identifier(self) -> str:
        """The ClickHouse table this pipeline writes to."""
        return ch_unique_identifier(self.user_id, self.collection_number, self.table_name)

    @field_validator("table_name")
    @classmethod
    def _table_name_is_an_identifier(cls, v: str) -> str:
        # Fail at pipeline-creation time, not mid-insert with half a batch in.
        ch_unique_identifier("0", 1, v)
        return v


class PipelineState(BaseModel):
    pipeline_id:        str
    pipeline_type:      PipelineType
    desired_state:      DesiredState   = DesiredState.STOPPED
    status:             PipelineStatus = PipelineStatus.CREATED
    topic:              str
    producer_container: str | None     = None
    consumer_container: str | None     = None
    last_heartbeat_at:  str | None     = None
    last_error:         str | None     = None
    updated_at:         str            = Field(default_factory=utc_now_iso)


class PipelineStateEvent(BaseModel):
    pipeline_id:   str
    event_type:    Literal["start", "stop", "status"]
    desired_state: DesiredState
    status:        PipelineStatus
    timestamp:     str = Field(default_factory=utc_now_iso)
