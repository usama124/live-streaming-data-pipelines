"""Phase 2 fixtures — real Kafka, real ClickHouse, the real consumer pool.

The pool's whole job is to move records between two systems without losing or
misrouting them. Mocking either end would test the mock.
"""

from __future__ import annotations

import json
import subprocess
import time
import uuid
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]

KAFKA_EXTERNAL = "localhost:9094"
CLICKHOUSE_URL = "http://localhost:8123"
DATABASE = "data_platform"
POOL_IMAGE = "data-platform-consumer-pool:latest"
NETWORK = "data-platform_backend"


def unique_id(prefix: str) -> str:
    """Fresh ids per run: topics and tables outlive a test, and reusing a name
    lets a test pass on the previous run's data."""
    return f"{prefix}{uuid.uuid4().hex[:8]}"


def _compose(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", "compose", *args], cwd=REPO, check=check,
                          capture_output=True, text=True, timeout=900)


def ch_query(sql: str) -> list[list]:
    """Run SQL against ClickHouse over HTTP, returning parsed JSON rows."""
    import requests

    response = requests.post(
        CLICKHOUSE_URL, params={"database": DATABASE},
        data=f"{sql} FORMAT JSONCompact".encode(), timeout=30,
    )
    response.raise_for_status()
    return response.json()["data"] if response.text.strip() else []


def ch_count(table: str, where: str = "1") -> int:
    try:
        rows = ch_query(f"SELECT count() FROM `{table}` WHERE {where}")
    except Exception:
        return 0
    return int(rows[0][0]) if rows else 0


def wait_for(predicate, timeout: float = 90.0, interval: float = 2.0, what: str = "condition"):
    """Poll until true. The pool is asynchronous — everything here is eventual."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"timed out waiting for {what}; last={last}")


def produce(topic: str, messages: list[dict]) -> None:
    """Publish Telegraf-shaped messages straight to Kafka.

    Phase 1 already proves Telegraf produces this shape; going direct here keeps
    these tests about the pool rather than re-testing the producer.
    """
    import asyncio

    from aiokafka import AIOKafkaProducer

    async def _run() -> None:
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_EXTERNAL,
            value_serializer=lambda v: (v if isinstance(v, bytes) else json.dumps(v).encode()),
        )
        await producer.start()
        try:
            for message in messages:
                await producer.send_and_wait(topic, value=message)
        finally:
            await producer.stop()

    asyncio.run(_run())


def telegraf_message(pipeline_id: str, *, sensor: str = "temperature",
                     value=25.5, sequence: int = 1, timestamp_ms: int | None = None) -> dict:
    return {
        "name": f"pipeline.{pipeline_id}.events",
        "timestamp": timestamp_ms or int(time.time() * 1000),
        "tags": {"pipeline_id": pipeline_id, "sensor": sensor, "source": "opcua",
                 "quality": "good", "node_id": "ns=2;i=2"},
        "fields": {"value": value, "sequence": sequence},
    }


def create_pipeline(pipeline_id: str, *, user_id: str = "1", collection_number: int = 1,
                    table_name: str = "events", table_schema: dict | None = None) -> dict:
    """Create through the real API, which also provisions the topic and table."""
    import requests

    body = {
        "pipeline_id": pipeline_id,
        "pipeline_type": "live",
        "source_type": "opcua",
        "topic": f"pipeline.{pipeline_id}.events",
        "user_id": user_id,
        "collection_number": collection_number,
        "table_name": table_name,
        "source_options": {"endpoint": "opc.tcp://unused:4840/", "node_ids": ["ns=2;i=2"]},
    }
    if table_schema:
        body["table_schema"] = table_schema
    response = requests.post("http://localhost:8000/pipelines", json=body, timeout=60)
    assert response.status_code == 201, response.text
    return response.json()


def restart_pool() -> None:
    _compose("restart", "consumer-pool")


def pool_logs(tail: int = 200) -> str:
    return _compose("logs", "--tail", str(tail), "consumer-pool", check=False).stdout


def dlq_count(pipeline_id: str) -> int:
    return ch_count("dead_letter_events", f"pipeline_id = '{pipeline_id}'")
