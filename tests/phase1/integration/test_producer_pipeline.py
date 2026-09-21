"""Phase 1 integration suite — I2, I3, I4.

The real task_manager API, the real controller, the real DockerRuntime, real
Telegraf and a real OPC UA server. Nothing here is mocked: the point of the
integration tier is to catch what per-component tests cannot, which is the
pieces disagreeing about how they fit together.

Kubernetes does not exist yet, so this runs against DockerRuntime — the runtime
parity re-run against KubernetesRuntime is Phase 3's I12.
"""

from __future__ import annotations

import time

import pytest
import requests

from tests.phase1.conftest import (
    NETWORK,
    consume,
    mock_server_container,
    unique_id,
)

API = "http://localhost:8000"

# The mock server's simulated ranges. Values landing on Kafka must fall inside
# these — "some message arrived" is not evidence the right data arrived.
EXPECTED_RANGES = {
    "temperature": (10.0, 40.0),
    "pressure": (95.0, 108.0),
    "vibration": (0.0, 0.2),
    "flowrate": (95.0, 145.0),
}


def _create_and_start(pipeline_id: str, source_host: str) -> dict:
    body = {
        "pipeline_id": pipeline_id,
        "pipeline_type": "live",
        "source_type": "opcua",
        "topic": f"pipeline.{pipeline_id}.events",
        "source_options": {
            "endpoint": f"opc.tcp://{source_host}:4840/stratahub/server/",
            "node_ids": ["ns=2;i=2", "ns=2;i=3", "ns=2;i=4", "ns=2;i=5", "ns=2;i=6"],
            "node_names": {
                "ns=2;i=2": "Temperature", "ns=2;i=3": "Pressure",
                "ns=2;i=4": "Vibration", "ns=2;i=5": "FlowRate",
                "ns=2;i=6": "MachineStatus",
            },
            "publishing_interval_ms": 200,
        },
    }
    created = requests.post(f"{API}/pipelines", json=body, timeout=30)
    assert created.status_code == 201, created.text

    started = requests.post(f"{API}/pipelines/{pipeline_id}/start", timeout=30)
    assert started.status_code == 200, started.text
    return created.json()


def _wait_for_status(pipeline_id: str, statuses: set[str], timeout: float = 90.0) -> dict:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = requests.get(f"{API}/pipelines/{pipeline_id}/status", timeout=30).json()
        if last["state"] and last["state"]["status"] in statuses:
            return last
        time.sleep(2)
    raise AssertionError(f"{pipeline_id} never reached {statuses}; last={last}")


def _producer_container(pipeline_id: str) -> str:
    state = requests.get(f"{API}/pipelines/{pipeline_id}/status", timeout=30).json()["state"]
    return state["producer_container"]


def _container_exists(name: str) -> bool:
    import docker

    try:
        docker.from_env().containers.get(name)
        return True
    except Exception:
        return False


@pytest.mark.usefixtures("producer_image", "consumer_image", "platform_stack")
def test_i2_values_on_kafka_match_the_source(kafka) -> None:
    """I2 — end to end through the real API, checking the values, not just that
    'some message' arrived."""
    pipeline_id = unique_id("i2")

    with mock_server_container("i2-mock"):
        _create_and_start(pipeline_id, "i2-mock")
        _wait_for_status(pipeline_id, {"running", "starting"})

        messages = consume(f"pipeline.{pipeline_id}.events", count=30, timeout=120)
        requests.post(f"{API}/pipelines/{pipeline_id}/stop", timeout=30)

    assert messages, "no data reached Kafka through the real API path"

    checked = 0
    for message in messages:
        sensor = message["tags"]["sensor"]
        if sensor not in EXPECTED_RANGES:
            continue
        low, high = EXPECTED_RANGES[sensor]
        value = message["fields"]["value"]
        assert low <= value <= high, f"{sensor}={value} outside the source's range {low}..{high}"
        checked += 1

    assert checked >= 10, f"only {checked} numeric readings verified against the source"


@pytest.mark.usefixtures("producer_image", "consumer_image", "platform_stack")
def test_i3_concurrent_pipelines_keep_their_topics_separate(kafka) -> None:
    """I3 — two pipelines, two sources, running at the same time."""
    id_a, id_b = unique_id("i3a"), unique_id("i3b")

    with mock_server_container("i3-mock-a"), mock_server_container("i3-mock-b"):
        _create_and_start(id_a, "i3-mock-a")
        _create_and_start(id_b, "i3-mock-b")
        _wait_for_status(id_a, {"running", "starting"})
        _wait_for_status(id_b, {"running", "starting"})

        messages_a = consume(f"pipeline.{id_a}.events", count=20, timeout=120)
        messages_b = consume(f"pipeline.{id_b}.events", count=20, timeout=120)

        requests.post(f"{API}/pipelines/{id_a}/stop", timeout=30)
        requests.post(f"{API}/pipelines/{id_b}/stop", timeout=30)

    assert messages_a and messages_b
    assert {m["tags"]["pipeline_id"] for m in messages_a} == {id_a}
    assert {m["tags"]["pipeline_id"] for m in messages_b} == {id_b}


@pytest.mark.usefixtures("producer_image", "consumer_image", "platform_stack")
def test_i4_full_lifecycle_tears_the_producer_down(kafka) -> None:
    """I4 — create, sustained flow, stop, and the container is actually gone."""
    pipeline_id = unique_id("i4")

    with mock_server_container("i4-mock"):
        _create_and_start(pipeline_id, "i4-mock")
        _wait_for_status(pipeline_id, {"running", "starting"})

        first = consume(f"pipeline.{pipeline_id}.events", count=10, timeout=120)
        assert first, "no data after create"

        producer = _producer_container(pipeline_id)
        assert producer and _container_exists(producer), f"no producer container: {producer}"

        # Sustained: still producing a while later, not just a first burst.
        time.sleep(20)
        later = consume(f"pipeline.{pipeline_id}.events", count=10_000, timeout=30)
        assert len(later) > len(first), "flow did not continue past the initial burst"

        stopped = requests.post(f"{API}/pipelines/{pipeline_id}/stop", timeout=30)
        assert stopped.status_code == 200, stopped.text
        _wait_for_status(pipeline_id, {"stopped"})

    deadline = time.time() + 60
    while time.time() < deadline and _container_exists(producer):
        time.sleep(2)
    assert not _container_exists(producer), (
        f"producer container {producer} survived the stop — resources leaked"
    )
