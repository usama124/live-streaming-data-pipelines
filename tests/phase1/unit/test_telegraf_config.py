"""Registry #6 — pipeline creation renders a Telegraf config that matches PipelineConfig.

The rendered file is parsed back with tomllib rather than string-matched: a
config that looks right but does not parse is not a passing case, and quoting is
exactly where this breaks (node-name maps are JSON *inside* a TOML string).
"""

from __future__ import annotations

import tomllib

from common.app_common.models import PipelineConfig, PipelineType
from common.app_common.telegraf_config import render_telegraf_config

NODE_NAMES = {"ns=2;i=2": "Temperature", "ns=2;i=3": "Pressure"}


def _config(pipeline_id: str = "pipe-42") -> PipelineConfig:
    return PipelineConfig(
        pipeline_id=pipeline_id,
        pipeline_type=PipelineType.LIVE,
        source_type="opcua",
        topic=f"pipeline.{pipeline_id}.events",
        source_options={
            "endpoint": "opc.tcp://plant-1.example.com:4840/stratahub/server/",
            "node_ids": ["ns=2;i=2", "ns=2;i=3"],
            "node_names": NODE_NAMES,
            "publishing_interval_ms": 250,
        },
    )


def _env(rendered: dict) -> dict[str, str]:
    entries = rendered["inputs"]["execd"][0]["environment"]
    return dict(entry.split("=", 1) for entry in entries)


def test_rendered_config_is_valid_toml() -> None:
    tomllib.loads(render_telegraf_config(_config(), kafka_brokers="kafka:9092"))


def test_output_topic_matches_the_pipeline_config() -> None:
    rendered = tomllib.loads(render_telegraf_config(_config("pipe-42"), kafka_brokers="kafka:9092"))

    kafka = rendered["outputs"]["kafka"][0]
    assert kafka["topic"] == "pipeline.pipe-42.events"
    assert kafka["brokers"] == ["kafka:9092"]


def test_connector_args_match_the_pipeline_source_options() -> None:
    cfg = _config()
    rendered = tomllib.loads(render_telegraf_config(cfg, kafka_brokers="kafka:9092"))

    execd = rendered["inputs"]["execd"][0]
    assert "connector.py" in " ".join(execd["command"]), execd["command"]

    env = _env(rendered)
    assert env["OPCUA_ENDPOINT"] == cfg.source_options["endpoint"]
    assert env["OPCUA_NODE_IDS"] == "ns=2;i=2,ns=2;i=3"
    assert env["OPCUA_PUBLISHING_INTERVAL_MS"] == "250"
    assert env["PIPELINE_ID"] == "pipe-42"


def test_node_names_survive_toml_quoting() -> None:
    """The node-name map is JSON inside a TOML string — the quoting trap."""
    import json

    rendered = tomllib.loads(render_telegraf_config(_config(), kafka_brokers="kafka:9092"))

    assert json.loads(_env(rendered)["OPCUA_NODE_NAMES_JSON"]) == NODE_NAMES


def test_execd_restarts_the_connector_on_exit() -> None:
    """The connector exits on failure by design; restart_delay is what recovers it."""
    rendered = tomllib.loads(render_telegraf_config(_config(), kafka_brokers="kafka:9092"))

    assert rendered["inputs"]["execd"][0]["restart_delay"] == "10s"


def test_two_pipelines_render_different_topics() -> None:
    """Direct guard against every pipeline's Telegraf writing to one topic."""
    a = tomllib.loads(render_telegraf_config(_config("aaa"), kafka_brokers="kafka:9092"))
    b = tomllib.loads(render_telegraf_config(_config("bbb"), kafka_brokers="kafka:9092"))

    assert a["outputs"]["kafka"][0]["topic"] != b["outputs"]["kafka"][0]["topic"]


def test_outputs_are_filtered_so_internal_metrics_stay_out_of_the_data() -> None:
    """Telegraf routes every input to every output. Without namepass, its own
    internal metrics are published to the pipeline's Kafka topic and land as rows
    in its ClickHouse table."""
    cfg = _config()
    rendered = tomllib.loads(render_telegraf_config(cfg, kafka_brokers="kafka:9092"))

    assert rendered["outputs"]["kafka"][0]["namepass"] == [cfg.topic]
    assert rendered["outputs"]["prometheus_client"][0]["namedrop"] == [cfg.topic]
