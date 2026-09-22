"""PipelineConfig carries identity and an explicit schema — nothing is inferred.

The schema-collision bug this phase exists to fix came from inferring columns
from the first event seen. The config has to declare them instead.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from common.app_common.models import PipelineConfig, PipelineType


def _config(**overrides) -> PipelineConfig:
    body = dict(
        pipeline_id="p1",
        pipeline_type=PipelineType.LIVE,
        topic="pipeline.p1.events",
        user_id="1",
        collection_number=22,
        table_name="aveva_iot",
        source_options={"endpoint": "opc.tcp://source:4840/", "node_ids": ["ns=2;i=2"]},
    )
    body.update(overrides)
    return PipelineConfig(**body)


def test_config_exposes_the_clickhouse_table_name() -> None:
    assert _config().ch_unique_identifier == "user_1_collection_22_aveva_iot"


def test_table_name_is_independent_of_the_kafka_topic() -> None:
    """The topic is an internal routing id. Deriving the table from it is what
    the consumer pool must never do."""
    config = _config(topic="pipeline.something-else.events")

    assert config.ch_unique_identifier == "user_1_collection_22_aveva_iot"
    assert config.topic not in config.ch_unique_identifier


def test_two_pipelines_of_one_user_get_different_tables() -> None:
    a = _config(pipeline_id="a", table_name="line_1")
    b = _config(pipeline_id="b", table_name="line_2")

    assert a.ch_unique_identifier != b.ch_unique_identifier


def test_config_declares_its_columns() -> None:
    schema = _config().table_schema

    assert schema, "a pipeline must declare its columns, never infer them"
    assert "event_time" in schema and "pipeline_id" in schema


def test_declared_schema_can_be_overridden_per_pipeline() -> None:
    custom = {"event_time": "DateTime64(3)", "reading": "Float64"}

    assert _config(table_schema=custom).table_schema == custom


def test_invalid_table_name_is_rejected_at_config_time() -> None:
    """Not at insert time, when it is already too late and half a batch is in."""
    with pytest.raises((ValidationError, ValueError)):
        _config(table_name="drop table; --")
