"""Registry #4 and #7 — what Telegraf's supervision does and does not cover.

#4: the connector exits (crash, or its own deliberate exit on session loss) and
    Telegraf brings it back within restart_delay, with no intervention.
#7: the connector stays alive and stops producing. Telegraf cannot see this.
    That test is marked xfail on purpose — Phase 3's liveness probe is what
    closes it, and the marker is what stops the gap quietly going stale.
"""

from __future__ import annotations

import subprocess
import time

import pytest

from common.app_common.models import PipelineConfig, PipelineType
from common.app_common.telegraf_config import render_telegraf_config
from tests.phase1.conftest import (
    KAFKA_INTERNAL,
    consume,
    mock_server_container,
    opcua_source_options,
    telegraf_container,
    unique_id,
)

RESTART_DELAY_S = 10  # the template's restart_delay; the watchdog it replaces allowed 30s


def _pipeline(pipeline_id: str, source_host: str) -> PipelineConfig:
    return PipelineConfig(
        pipeline_id=pipeline_id,
        pipeline_type=PipelineType.LIVE,
        source_type="opcua",
        topic=f"pipeline.{pipeline_id}.events",
        source_options=opcua_source_options(source_host),
    )


def _kill_connector(container_name: str) -> None:
    result = subprocess.run(
        ["docker", "exec", container_name, "pkill", "-f", "connector.py"],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, f"nothing killed: {result.stdout}{result.stderr}"


@pytest.mark.usefixtures("producer_image")
def test_telegraf_restarts_the_connector_after_it_exits(kafka) -> None:
    """The connector's sequence counter restarts at 1, so a fresh process is
    visible in the data itself rather than only in a log line."""
    config = _pipeline(unique_id("t4-restart"), "t4-mock")

    with mock_server_container("t4-mock"), telegraf_container(
        "t4-telegraf", render_telegraf_config(config, kafka_brokers=KAFKA_INTERNAL)
    ) as telegraf:
        assert consume(config.topic, count=5), "no data before the kill"

        _kill_connector("t4-telegraf")
        killed_at = time.time()

        # Long enough for restart_delay plus a reconnect and a few notifications.
        messages = consume(config.topic, count=200, timeout=RESTART_DELAY_S + 40)
        logs = telegraf.logs()

    sequences = [m["fields"]["sequence"] for m in messages]
    restarted = any(b <= a for a, b in zip(sequences, sequences[1:]))
    assert restarted, (
        "sequence never restarted — the connector was not respawned\n"
        f"sequences={sequences[:40]}\n{logs[-3000:]}"
    )
    assert time.time() - killed_at < RESTART_DELAY_S + 40


@pytest.mark.xfail(
    strict=True,
    reason="KNOWN GAP (registry #7): Telegraf restarts a subprocess only when it "
           "exits. A connected-but-stalled source leaves the connector alive and "
           "silent, and nothing here notices. Closed by Phase 3's liveness probe "
           "(registry #19) — when this starts passing, remove the marker rather "
           "than the test.",
)
@pytest.mark.usefixtures("producer_image")
def test_stalled_source_is_detected(kafka) -> None:
    """The source keeps its session open and stops changing values.

    Asserts the behaviour we *want*: data resumes, because something noticed and
    recycled the producer. Today nothing does, so this fails — deliberately.
    """
    config = _pipeline(unique_id("t7-stalled"), "t7-mock")

    with mock_server_container("t7-mock", freeze_after_s=10), telegraf_container(
        "t7-telegraf", render_telegraf_config(config, kafka_brokers=KAFKA_INTERNAL)
    ):
        assert consume(config.topic, count=5, timeout=60), "no data before the freeze"

        before = len(consume(config.topic, count=10_000, timeout=25))
        time.sleep(45)
        after = len(consume(config.topic, count=10_000, timeout=25))

    assert after > before, (
        "source stalled and data never resumed — nothing detected a "
        "hung-but-alive connector"
    )
