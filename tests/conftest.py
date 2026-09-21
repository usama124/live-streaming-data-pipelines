"""Fixtures shared across phases.

These live here rather than in one phase's conftest because pytest only loads a
conftest for tests beneath it, and Phase 4's monitoring tests need the same
running stack Phase 2's tests do.
"""

from __future__ import annotations

import json
import subprocess
import time
import uuid
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
POOL_IMAGE = "data-platform-consumer-pool:latest"

# ClickHouse tables outlive a test run exactly as Kafka topics do. A fixed table
# name means the next run reads the previous run's rows — which both breaks
# "no foreign rows" assertions and, worse, can satisfy a "at least N rows landed"
# assertion without the run under test writing anything at all.
RUN_TOKEN = uuid.uuid4().hex[:6]


def unique_table(base: str) -> str:
    """A table name unique to this run, still readable in ClickHouse."""
    return f"{base}_{RUN_TOKEN}"


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



@pytest.fixture(scope="session")
def platform(infra, pool_image):
    """API, controller and the pool, all real."""
    _compose("up", "-d", "--build", "--wait", "task-manager", "controller")
    _compose("up", "-d", "--build", "consumer-pool")

    import urllib.request

    deadline = time.time() + 120
    while time.time() < deadline:
        try:
            with urllib.request.urlopen("http://localhost:8000/health", timeout=5) as r:
                if json.load(r)["status"] == "ok":
                    break
        except Exception:
            time.sleep(2)
    else:
        raise AssertionError("task-manager never became healthy")
    yield


