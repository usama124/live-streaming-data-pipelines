"""Phase 4 fixtures — the monitoring stack, against real traffic.

Everything here checks *numbers*, not that a dashboard renders. A monitoring
stack that is up but reporting the wrong thing is worse than none, because it is
believed.
"""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path

import pytest
import requests

REPO = Path(__file__).resolve().parents[2]

PROMETHEUS = "http://localhost:9090"
KAFKA_UI = "http://localhost:8090"
KAFKA_UI_CLUSTER = "live"


def _compose(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", "compose", *args], cwd=REPO, check=check,
                          capture_output=True, text=True, timeout=900)


@pytest.fixture(scope="session")
def monitoring(platform):
    """The Phase 2 stack plus Kafka UI and Prometheus."""
    _compose("up", "-d", "--build", "prometheus", "kafka-ui")

    for name, url in [("prometheus", f"{PROMETHEUS}/-/ready"),
                      ("kafka-ui", f"{KAFKA_UI}/api/clusters")]:
        deadline = time.time() + 180
        while time.time() < deadline:
            try:
                if requests.get(url, timeout=5).status_code == 200:
                    break
            except Exception:
                time.sleep(3)
        else:
            raise AssertionError(f"{name} never became ready")
    yield


def promql(query: str) -> list[dict]:
    response = requests.get(f"{PROMETHEUS}/api/v1/query",
                            params={"query": query}, timeout=30)
    response.raise_for_status()
    body = response.json()
    assert body["status"] == "success", body
    return body["data"]["result"]


def promql_value(query: str) -> float | None:
    result = promql(query)
    return float(result[0]["value"][1]) if result else None


def prometheus_targets() -> dict[str, list[dict]]:
    response = requests.get(f"{PROMETHEUS}/api/v1/targets",
                            params={"state": "active"}, timeout=30)
    response.raise_for_status()
    targets: dict[str, list[dict]] = {}
    for target in response.json()["data"]["activeTargets"]:
        targets.setdefault(target["labels"].get("job", "?"), []).append(target)
    return targets


def kafka_ui_topic(topic: str) -> dict | None:
    response = requests.get(
        f"{KAFKA_UI}/api/clusters/{KAFKA_UI_CLUSTER}/topics/{topic}", timeout=30)
    return response.json() if response.status_code == 200 else None


def kafka_ui_consumer_group(group: str = "consumer-pool") -> dict | None:
    response = requests.get(
        f"{KAFKA_UI}/api/clusters/{KAFKA_UI_CLUSTER}/consumer-groups/{group}", timeout=30)
    return response.json() if response.status_code == 200 else None


def wait_for(predicate, timeout: float = 120.0, interval: float = 3.0, what: str = "condition"):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"timed out waiting for {what}; last={last}")
