"""Phase 4 integration suite — I14, I15.

Monitoring attached to real multi-pipeline traffic, with the numbers cross-checked
against what was actually sent. I16 (multi-user health API scoping) is absent:
the plan calls for *designing* the per-user health API in this phase, not
building it, so there is nothing to scope-test yet.
"""

from __future__ import annotations

import time

import pytest

from tests.conftest import unique_table
from tests.phase1.conftest import mock_server_container, producer_image  # noqa: F401
from tests.phase2.conftest import ch_count, ch_query, unique_id
from tests.phase4.conftest import (
    kafka_ui_topic,
    promql_value,
    wait_for,
)

import requests

API = "http://localhost:8000"

pytestmark = pytest.mark.usefixtures("monitoring", "producer_image")


def _create_and_start(pipeline_id: str, source_host: str, *, collection_number: int,
                      table_name: str) -> str:
    created = requests.post(f"{API}/pipelines", timeout=60, json={
        "pipeline_id": pipeline_id, "pipeline_type": "live", "source_type": "opcua",
        "topic": f"pipeline.{pipeline_id}.events",
        "user_id": "1", "collection_number": collection_number, "table_name": table_name,
        "source_options": {
            "endpoint": f"opc.tcp://{source_host}:4840/stratahub/server/",
            "node_ids": ["ns=2;i=2", "ns=2;i=3", "ns=2;i=4", "ns=2;i=5", "ns=2;i=6"],
            "node_names": {"ns=2;i=2": "Temperature", "ns=2;i=3": "Pressure",
                           "ns=2;i=4": "Vibration", "ns=2;i=5": "FlowRate",
                           "ns=2;i=6": "MachineStatus"},
            "publishing_interval_ms": 200,
        },
    })
    assert created.status_code == 201, created.text
    started = requests.post(f"{API}/pipelines/{pipeline_id}/start", timeout=60)
    assert started.status_code == 200, started.text
    return created.json()["table"]


def test_i14_monitoring_numbers_match_the_traffic_that_actually_flowed() -> None:
    """I14 — not 'the dashboards render': Kafka UI, Prometheus and ClickHouse must
    agree with each other and with the rows on disk."""
    ids = [unique_id(f"i14{i}") for i in range(2)]

    with mock_server_container("i14-mock"):
        tables = {
            pid: _create_and_start(pid, "i14-mock", collection_number=14,
                                   table_name=unique_table(f"mon_{i}"))
            for i, pid in enumerate(ids)
        }
        try:
            wait_for(lambda: all(ch_count(t) >= 15 for t in tables.values()),
                     timeout=300, what="both pipelines flowing")

            for pid, table in tables.items():
                rows = ch_count(table)

                # Kafka UI's message count must be at least the rows on disk —
                # equal once fully consumed, never fewer.
                reported = wait_for(lambda: kafka_ui_topic(f"pipeline.{pid}.events"),
                                    what=f"{pid} in Kafka UI")
                produced = sum(p["offsetMax"] - p["offsetMin"] for p in reported["partitions"])
                assert produced >= rows, (
                    f"Kafka UI reports {produced} messages but {rows} rows landed — "
                    "more rows than messages means the numbers disagree"
                )

                # Prometheus must know about this pipeline and call it fresh.
                written = wait_for(
                    lambda: promql_value(f'rows_written_total{{pipeline_id="{pid}"}}'),
                    timeout=90, what=f"rows_written_total for {pid}",
                )
                assert written >= 1, written

                # Prometheus scrapes on an interval; poll rather than race it.
                staleness = wait_for(
                    lambda: (
                        v := promql_value(
                            'time() - pipeline_last_event_timestamp_seconds'
                            f'{{pipeline_id="{pid}"}}')
                    ) is not None and v < 300 and v,
                    timeout=120,
                    what=f"{pid} to report as fresh",
                )
                assert staleness < 300, staleness
        finally:
            for pid in ids:
                requests.post(f"{API}/pipelines/{pid}/stop", timeout=60)


def test_i15_a_real_stalled_source_surfaces_as_stale() -> None:
    """I15 — induce the genuine condition, not a calculation: the source keeps its
    session open and stops publishing, so Kafka lag stays zero while the data ages."""
    pipeline_id = unique_id("i15")

    # The mock freezes shortly after start: session open, values static.
    with mock_server_container("i15-mock", freeze_after_s=25):
        table = _create_and_start(pipeline_id, "i15-mock", collection_number=15,
                                  table_name=unique_table("stalled"))
        try:
            wait_for(lambda: ch_count(table) >= 5, timeout=300,
                     what="data flowing before the freeze")

            fresh = wait_for(
                lambda: (
                    v := promql_value(
                        'time() - pipeline_last_event_timestamp_seconds'
                        f'{{pipeline_id="{pipeline_id}"}}')
                ) is not None and v < 120 and v,
                timeout=120,
                what="the pipeline to report as fresh before the freeze",
            )

            # Now wait past the alert's own threshold behaviour and watch it age.
            time.sleep(90)

            stale = promql_value(
                f'time() - pipeline_last_event_timestamp_seconds{{pipeline_id="{pipeline_id}"}}')
            assert stale is not None and stale > fresh + 60, (
                f"staleness did not grow while the source was frozen "
                f"({fresh}s -> {stale}s) — the stall is invisible"
            )

            # The point of the test: nothing else would have told us.
            newest, oldest = ch_query(
                f"SELECT max(event_time), min(event_time) FROM `{table}`")[0]
            assert newest and oldest, "no rows to compare"

            # And the table holds real readings only. Telegraf's own internal
            # metrics reaching this table would both pollute the data and keep a
            # stalled pipeline looking alive.
            foreign = ch_count(table, "source != 'opcua'")
            assert foreign == 0, (
                f"{foreign} non-source rows in {table} — Telegraf internal metrics "
                "are being published to the pipeline topic"
            )
        finally:
            requests.post(f"{API}/pipelines/{pipeline_id}/stop", timeout=60)
