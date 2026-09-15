from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, Field


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


class PipelineCreateRequest(BaseModel):
    pipeline_id:   str = Field(..., min_length=1, pattern=r"^[a-zA-Z0-9_.-]+$")
    pipeline_type: PipelineType

    airflow_dag_id: str | None = None

    source_type:            str            = "mock"
    topic:                  str | None     = None
    batch_size:             int            = Field(default=500, ge=1, le=100_000)
    flush_interval_seconds: int            = Field(default=60, ge=1, le=3600)
    source_options:         dict[str, Any] = Field(default_factory=dict)


class PipelineConfig(BaseModel):
    pipeline_id:            str
    pipeline_type:          PipelineType
    airflow_dag_id:         str | None     = None
    source_type:            str            = "mock"
    topic:                  str
    batch_size:             int            = 500
    flush_interval_seconds: int            = 60
    source_options:         dict[str, Any] = Field(default_factory=dict)
    created_at:             str            = Field(default_factory=utc_now_iso)
    updated_at:             str            = Field(default_factory=utc_now_iso)


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
