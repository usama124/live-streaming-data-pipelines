"""source_options is validated per source type, at pipeline-creation time.

The point: a missing endpoint is a 400 when the pipeline is created, not a
container that crash-loops an hour later with the reason buried in a log.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from common.app_common.models import PipelineConfig, PipelineCreateRequest, PipelineType
from common.app_common.sources import SourceType

VALID_OPCUA = {
    "endpoint": "opc.tcp://plant-1:4840/stratahub/server/",
    "node_ids": ["ns=2;i=2", "ns=2;i=3"],
}


def _config(**overrides):
    body = dict(
        pipeline_id="p1", pipeline_type=PipelineType.LIVE, topic="pipeline.p1.events",
        source_type=SourceType.OPCUA, source_options=dict(VALID_OPCUA),
    )
    body.update(overrides)
    return PipelineConfig(**body)


def test_valid_options_are_accepted_and_typed() -> None:
    config = _config()

    assert config.source_options.endpoint == VALID_OPCUA["endpoint"]
    assert config.source_options.node_ids == VALID_OPCUA["node_ids"]


def test_missing_endpoint_is_rejected_at_creation() -> None:
    with pytest.raises(ValidationError, match="endpoint"):
        _config(source_options={"node_ids": ["ns=2;i=2"]})


def test_empty_node_ids_is_rejected_at_creation() -> None:
    """An OPC UA pipeline subscribed to nothing is misconfigured, not empty."""
    with pytest.raises(ValidationError, match="node_ids"):
        _config(source_options={"endpoint": "opc.tcp://h:4840/", "node_ids": []})


def test_unknown_source_type_is_rejected() -> None:
    with pytest.raises(ValidationError):
        _config(source_type="opc-ua")  # typo — used to be accepted silently


def test_create_request_rejects_bad_options_too() -> None:
    """The API surface, not just the internal config."""
    with pytest.raises(ValidationError):
        PipelineCreateRequest(
            pipeline_id="p1", pipeline_type=PipelineType.LIVE,
            source_type=SourceType.OPCUA, source_options={},
        )


def test_options_survive_a_redis_round_trip() -> None:
    """The pool re-parses config from Redis; the discriminator has to be there."""
    restored = PipelineConfig.model_validate_json(_config().model_dump_json())

    assert restored.source_options.endpoint == VALID_OPCUA["endpoint"]


def test_staleness_threshold_is_a_common_option_not_a_protocol_one() -> None:
    """The runtime reads it for every source type, so it lives on the base."""
    config = _config(source_options={**VALID_OPCUA, "staleness_threshold_s": 20})

    assert config.source_options.staleness_threshold_s == 20
