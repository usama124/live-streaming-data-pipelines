from __future__ import annotations

"""What a source type is — options schema, environment, and how it runs.

`SOURCES` is the single source of truth. The `SourceType` enum is generated from
it, so adding a source means adding one entry here rather than editing an enum,
a validator and a renderer that can drift apart.

A source is one of two shapes:

- **connector** — Telegraf has no native input for the protocol, so `command`
  points at a script under `connectors/` that `inputs.execd` runs.
- **native** — Telegraf has an input plugin, so `command` is `None` and the
  template configures that plugin directly.

Note the liveness consequence: a native source has no connector process, so
nothing serves `/healthz`. See docs/ARCHITECTURE.md §3.4 before adding one.
"""

from dataclasses import dataclass
from enum import StrEnum
from typing import Annotated, Callable, Literal, Union

from pydantic import BaseModel, Field


class SourceOptionsBase(BaseModel):
    """Options every source shares, whatever its protocol."""

    # How long the source may produce nothing before the liveness probe fails.
    # Runtime-level, not protocol-level, so it lives here rather than on one
    # source's model. None means "use the per-source-type default".
    staleness_threshold_s: float | None = None


class OpcUaOptions(SourceOptionsBase):
    source_type: Literal["opcua"] = "opcua"

    endpoint: str = Field(..., min_length=1)
    node_ids: list[str] = Field(..., min_length=1)
    node_names: dict[str, str] = Field(default_factory=dict)
    publishing_interval_ms: int = Field(default=500, ge=1)
    connect_timeout_s: float | None = None


def _opcua_environment(config) -> list[str]:  # noqa: ANN001  (avoids a circular import)
    import json

    opts = config.source_options
    env = {
        "PIPELINE_ID": config.pipeline_id,
        "OPCUA_ENDPOINT": opts.endpoint,
        "OPCUA_NODE_IDS": ",".join(opts.node_ids),
        "OPCUA_NODE_NAMES_JSON": json.dumps(opts.node_names),
        "OPCUA_PUBLISHING_INTERVAL_MS": str(opts.publishing_interval_ms),
    }
    if opts.connect_timeout_s is not None:
        env["OPCUA_CONNECT_TIMEOUT_S"] = str(opts.connect_timeout_s)
    return [f"{k}={v}" for k, v in env.items()]


@dataclass(frozen=True)
class SourceSpec:
    """Everything that differs between one source type and another."""

    options_model: type[SourceOptionsBase]
    environment: Callable[..., list[str]]
    # None = native Telegraf input, nothing to exec.
    command: Callable[[str], list[str]] | None
    # Seconds of silence before the liveness probe fails, unless a pipeline
    # overrides it. Not a poll interval: OPC UA publishes on change, so a
    # legitimately static sensor sends nothing and too low a number restarts
    # healthy pipelines. See docs/BACKLOG.md — this wants a real measurement.
    default_staleness_threshold_s: float


SOURCES: dict[str, SourceSpec] = {
    "opcua": SourceSpec(
        options_model=OpcUaOptions,
        environment=_opcua_environment,
        command=lambda connector_dir: ["python3", f"{connector_dir}/opcua/connector.py"],
        default_staleness_threshold_s=60.0,
    ),
}

# Generated, never hand-maintained — two lists would drift.
SourceType = StrEnum("SourceType", {name.upper(): name for name in SOURCES})

# Discriminated union over every registered source's options model.
_OPTION_MODELS = tuple(spec.options_model for spec in SOURCES.values())
SourceOptions = Annotated[
    Union[_OPTION_MODELS] if len(_OPTION_MODELS) > 1 else _OPTION_MODELS[0],
    Field(discriminator="source_type"),
]


def spec_for(source_type: str) -> SourceSpec:
    try:
        return SOURCES[str(source_type)]
    except KeyError:
        raise ValueError(
            f"unknown source_type {source_type!r}; registered: {sorted(SOURCES)}"
        ) from None
