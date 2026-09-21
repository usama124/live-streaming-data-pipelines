from __future__ import annotations
from typing import Any
from pydantic import BaseModel, Field

class PipelineDefinition(BaseModel):
    pipeline_id:            str            = Field(..., min_length=1, pattern=r"^[a-zA-Z0-9_.-]+$")
    source_type:            str            = "opcua"
    topic:                  str | None     = None
    batch_size:             int            = Field(default=500, ge=1, le=100_000)
    flush_interval_seconds: int            = Field(default=60, ge=1, le=3600)
    source_options:         dict[str, Any] = Field(default_factory=dict)
