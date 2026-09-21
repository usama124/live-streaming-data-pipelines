"""Phase 2 integration suite — I5, I6, I7, I8.

The whole path, nothing simulated: API create → Telegraf + connector against a
real OPC UA server → Kafka → the shared pool → the pipeline's own ClickHouse
table. Phase 2's unit tier produces to Kafka directly; this tier does not, so a
disagreement between the producer's output shape and the pool's row mapping is
caught here rather than in production.
"""

from __future__ import annotations

import time

import pytest
import requests

from tests.phase1.conftest import mock_server_container, producer_image  # noqa: F401
from tests.conftest import unique_table
from tests.phase2.conftest import (
    ch_count,
    ch_query,
    restart_pool,
    unique_id,
    wait_for,
)

API = "http://localhost:8000"

pytestmark = pytest.mark.usefixtures("platform", "producer_image")


def _create_and_start(pipeline_id: str, source_host: str, *, collection_number: int,
                      table_name: str, table_schema: dict | None = None) -> str:
    body = {
        "pipeline_id": pipeline_id,
        "pipeline_type": "live",
        "source_type": "opcua",
        "topic": f"pipeline.{pipeline_id}.events",
        "user_id": "1",
        "collection_number": collection_number,
        "table_name": table_name,
        "source_options": {
            "endpoint": f"opc.tcp://{source_host}:4840/stratahub/server/",
            "node_ids": ["ns=2;i=2", "ns=2;i=3", "ns=2;i=4", "ns=2;i=5", "ns=2;i=6"],
            "node_names": {"ns=2;i=2": "Temperature", "ns=2;i=3": "Pressure",
                           "ns=2;i=4": "Vibration", "ns=2;i=5": "FlowRate",
                           "ns=2;i=6": "MachineStatus"},
            "publishing_interval_ms": 200,
        },
    }
    if table_schema:
        body["table_schema"] = table_schema

    created = requests.post(f"{API}/pipelines", json=body, timeout=60)
    assert created.status_code == 201, created.text
    started = requests.post(f"{API}/pipelines/{pipeline_id}/start", timeout=60)
    assert started.status_code == 200, started.text
    return created.json()["table"]


def _stop(pipeline_id: str) -> None:
    requests.post(f"{API}/pipelines/{pipeline_id}/stop", timeout=60)


def test_i5_api_create_to_the_right_clickhouse_table() -> None:
    """I5 — one pipeline, end to end, verified in ClickHouse rather than at an
    intermediate hop."""
    pipeline_id = unique_id("i5")

    with mock_server_container("i5-mock"):
        table = _create_and_start(pipeline_id, "i5-mock",
                                  collection_number=5, table_name=unique_table("end_to_end"))
        try:
            wait_for(lambda: ch_count(table) >= 10, timeout=240,
                     what=f"10 rows in {table} via the full path")
        finally:
            _stop(pipeline_id)

    assert table.startswith("user_1_collection_5_end_to_end")

    # The values are the source's, not just "some rows".
    rows = ch_query(
        f"SELECT sensor, value, value_text FROM `{table}` "
        f"WHERE pipeline_id = '{pipeline_id}' LIMIT 50"
    )
    sensors = {row[0] for row in rows}
    assert sensors & {"temperature", "pressure", "vibration", "flowrate"}, sensors

    for sensor, value, value_text in rows:
        if sensor == "temperature" and value is not None:
            assert 10.0 <= value <= 40.0, f"temperature {value} outside the source's range"
        if sensor == "machinestatus":
            assert value_text, "string readings must survive the whole path"


def test_i6_concurrent_pipelines_with_different_schemas() -> None:
    """I6 — a soak with varying shapes; no cross-contamination, nothing dropped."""
    id_a, id_b = unique_id("i6a"), unique_id("i6b")
    narrow = {
        "event_time": "DateTime64(3)",
        "pipeline_id": "LowCardinality(String)",
        "sensor": "LowCardinality(String)",
        "value": "Nullable(Float64)",
        "value_text": "Nullable(String)",
        "ingested_at": "DateTime64(3)",
    }

    with mock_server_container("i6-mock-a"), mock_server_container("i6-mock-b"):
        table_a = _create_and_start(id_a, "i6-mock-a", collection_number=6, table_name=unique_table("soak_a"))
        table_b = _create_and_start(id_b, "i6-mock-b", collection_number=6,
                                    table_name=unique_table("soak_b"), table_schema=narrow)
        try:
            wait_for(lambda: ch_count(table_a) >= 20 and ch_count(table_b) >= 20,
                     timeout=300, what="both pipelines sustained a flow")
        finally:
            _stop(id_a)
            _stop(id_b)

    assert ch_count(table_a, f"pipeline_id = '{id_b}'") == 0
    assert ch_count(table_b, f"pipeline_id = '{id_a}'") == 0

    columns_b = {row[0] for row in ch_query(
        f"SELECT name FROM system.columns WHERE table = '{table_b}'")}
    assert "node_id" not in columns_b, f"narrow table was widened: {columns_b}"


def test_i7_killing_the_pool_mid_stream_recovers_every_pipeline() -> None:
    """I7 — fault injection under multi-pipeline load, not a single isolated one."""
    ids = [unique_id(f"i7{i}") for i in range(3)]

    with mock_server_container("i7-mock"):
        tables = {
            pid: _create_and_start(pid, "i7-mock", collection_number=7, table_name=unique_table(f"chaos_{i}"))
            for i, pid in enumerate(ids)
        }
        try:
            wait_for(lambda: all(ch_count(t) >= 5 for t in tables.values()),
                     timeout=240, what="all three pipelines flowing before the kill")

            before = {pid: ch_count(table) for pid, table in tables.items()}
            restart_pool()

            wait_for(
                lambda: all(ch_count(tables[pid]) > before[pid] for pid in ids),
                timeout=300,
                what="every pipeline resumed after the pool was killed mid-stream",
            )
        finally:
            for pid in ids:
                _stop(pid)


def test_i8_new_pipeline_joins_while_the_pool_is_under_load() -> None:
    """I8 — creation under load must not need a restart, and must not stall the
    pipelines already running."""
    busy_ids = [unique_id(f"i8busy{i}") for i in range(3)]
    late_id = unique_id("i8late")

    with mock_server_container("i8-mock"):
        busy_tables = {
            pid: _create_and_start(pid, "i8-mock", collection_number=8, table_name=unique_table(f"busy_{i}"))
            for i, pid in enumerate(busy_ids)
        }
        try:
            wait_for(lambda: all(ch_count(t) >= 5 for t in busy_tables.values()),
                     timeout=240, what="existing pipelines under load")
            baseline = {pid: ch_count(t) for pid, t in busy_tables.items()}

            # No restart of the pool anywhere in here.
            late_table = _create_and_start(late_id, "i8-mock", collection_number=8,
                                           table_name=unique_table("joined_late"))
            wait_for(lambda: ch_count(late_table) >= 5, timeout=240,
                     what="the late pipeline was picked up without a restart")

            for pid, table in busy_tables.items():
                assert ch_count(table) > baseline[pid], (
                    f"{pid} stopped progressing while a new pipeline was added"
                )
        finally:
            for pid in [*busy_ids, late_id]:
                _stop(pid)
