"""Registry #3 — Telegraf forwards connector output to the right topic, and only that one.

Real Telegraf, real Kafka, real OPC UA mock servers. Mocking Telegraf here would
test nothing: the whole question is whether our rendered config drives the real
binary correctly, including the JSON parser settings that silently drop fields
when they are wrong.
"""

from __future__ import annotations

import pytest

from common.app_common.models import PipelineConfig, PipelineType
from common.app_common.telegraf_config import render_telegraf_config
from tests.phase1.conftest import (
    KAFKA_INTERNAL,
    consume,
    unique_id,
    mock_server_container,
    opcua_source_options,
    telegraf_container,
    wait_for_port,
)


def _pipeline(pipeline_id: str, source_host: str) -> PipelineConfig:
    return PipelineConfig(
        pipeline_id=pipeline_id,
        pipeline_type=PipelineType.LIVE,
        source_type="opcua",
        topic=f"pipeline.{pipeline_id}.events",
        source_options=opcua_source_options(source_host),
    )


@pytest.mark.usefixtures("producer_image")
def test_records_reach_the_pipelines_own_topic(kafka) -> None:
    config = _pipeline(unique_id("t3-single"), "t3-mock-a")

    with mock_server_container("t3-mock-a"), telegraf_container(
        "t3-telegraf-a", render_telegraf_config(config, kafka_brokers=KAFKA_INTERNAL)
    ) as telegraf:
        messages = consume(config.topic, count=5)

        assert messages, f"nothing reached {config.topic}\n{telegraf.logs()[-3000:]}"

    for message in messages:
        assert message["name"] == config.topic
        assert message["tags"]["pipeline_id"] == config.pipeline_id
        assert message["tags"]["source"] == "opcua"
        # value must survive Telegraf's JSON parser — numeric or string
        assert "value" in message["fields"], message["fields"]


@pytest.mark.usefixtures("producer_image")
def test_two_concurrent_pipelines_do_not_leak_into_each_others_topics(kafka) -> None:
    config_a = _pipeline(unique_id("t3-leak-a"), "t3-mock-la")
    config_b = _pipeline(unique_id("t3-leak-b"), "t3-mock-lb")

    with mock_server_container("t3-mock-la"), mock_server_container("t3-mock-lb"), \
            telegraf_container(
                "t3-tg-la", render_telegraf_config(config_a, kafka_brokers=KAFKA_INTERNAL)
            ), telegraf_container(
                "t3-tg-lb", render_telegraf_config(config_b, kafka_brokers=KAFKA_INTERNAL)
            ):
        messages_a = consume(config_a.topic, count=5)
        messages_b = consume(config_b.topic, count=5)

    assert messages_a and messages_b

    ids_a = {m["tags"]["pipeline_id"] for m in messages_a}
    ids_b = {m["tags"]["pipeline_id"] for m in messages_b}
    assert ids_a == {config_a.pipeline_id}, f"B's records appeared on A's topic: {ids_a}"
    assert ids_b == {config_b.pipeline_id}, f"A's records appeared on B's topic: {ids_b}"


@pytest.mark.usefixtures("producer_image")
def test_string_readings_are_not_silently_dropped(kafka) -> None:
    """MachineStatus is a string. Telegraf's JSON parser drops non-numeric values
    unless the field is named in json_string_fields — with no error anywhere."""
    config = _pipeline(unique_id("t3-strings"), "t3-mock-s")

    with mock_server_container("t3-mock-s"), telegraf_container(
        "t3-tg-s", render_telegraf_config(config, kafka_brokers=KAFKA_INTERNAL)
    ) as telegraf:
        messages = consume(config.topic, count=40, timeout=60)

    statuses = [m for m in messages if m["tags"]["sensor"] == "machinestatus"]
    assert statuses, (
        "no string-valued readings reached Kafka at all\n" + telegraf.logs()[-2000:]
    )
    dropped = [m for m in statuses if "value" not in m["fields"]]
    assert not dropped, (
        "Telegraf dropped the string value and kept the tags — json_string_fields "
        f"does not list 'value'. Example: {dropped[0]}"
    )
    assert all(isinstance(m["fields"]["value"], str) for m in statuses), statuses[:3]
