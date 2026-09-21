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


@pytest.fixture(scope="session")
def infra():
    """Kafka + ClickHouse + Redis, from the root compose stack."""
    _compose("up", "-d", "--wait", "kafka", "clickhouse", "redis")
    yield


@pytest.fixture(scope="session")
def pool_image() -> str:
    subprocess.run(
        ["docker", "build", "-q", "-f", "services/consumer_pool/Dockerfile", "-t", POOL_IMAGE, "."],
        cwd=REPO, check=True, capture_output=True, text=True, timeout=1800,
    )
    return POOL_IMAGE


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
