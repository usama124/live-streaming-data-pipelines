"""The source registry — one place that defines what a source type is.

Adding a source must mean adding one entry here, not editing an enum, a
validator and a renderer that can drift out of step.
"""

from __future__ import annotations

import tomllib

import pytest

from common.app_common.models import PipelineConfig, PipelineType
from common.app_common.sources import SOURCES, SourceSpec, SourceType
from common.app_common.telegraf_config import render_telegraf_config


def test_enum_is_generated_from_the_registry() -> None:
    """Not hand-maintained alongside it — the same list, or they drift."""
    assert {s.value for s in SourceType} == set(SOURCES)


def test_every_registered_source_is_fully_specified() -> None:
    for name, spec in SOURCES.items():
        assert isinstance(spec, SourceSpec), name
        assert spec.options_model is not None, name
        assert callable(spec.environment), name
        # command is None for a native Telegraf input, callable for a connector.
        assert spec.command is None or callable(spec.command), name


def test_every_registered_source_has_a_template() -> None:
    from common.app_common.telegraf_config import _template

    for name in SOURCES:
        _template(name)  # raises FileNotFoundError if missing


def test_opcua_is_a_connector_source() -> None:
    spec = SOURCES[SourceType.OPCUA]

    assert spec.command is not None, "OPC UA runs our connector under execd"
    assert "connector.py" in " ".join(spec.command("/opt/connectors"))


def test_unknown_source_type_is_rejected_with_a_useful_message() -> None:
    config = PipelineConfig(
        pipeline_id="p", pipeline_type=PipelineType.LIVE, topic="pipeline.p.events",
        source_type="opcua",
        source_options={"endpoint": "opc.tcp://h:4840/", "node_ids": ["ns=2;i=2"]},
    )
    object.__setattr__(config, "source_type", "not-a-source")  # bypass validation

    with pytest.raises(ValueError, match="not-a-source"):
        render_telegraf_config(config, kafka_brokers="kafka:9092")


def test_rendering_tolerates_a_template_that_omits_placeholders() -> None:
    """A native-input template has no $command or $environment. safe_substitute
    means it renders anyway instead of raising KeyError."""
    from string import Template

    from common.app_common.telegraf_config import _substitutions

    config = PipelineConfig(
        pipeline_id="p", pipeline_type=PipelineType.LIVE, topic="pipeline.p.events",
        source_type=SourceType.OPCUA,
        source_options={"endpoint": "opc.tcp://h:4840/", "node_ids": ["ns=2;i=2"]},
    )
    minimal = Template('[[outputs.kafka]]\n  topic = $topic\n  brokers = $brokers\n')

    rendered = minimal.safe_substitute(_substitutions(config, kafka_brokers="kafka:9092"))

    assert tomllib.loads(rendered)["outputs"]["kafka"][0]["topic"] == "pipeline.p.events"
