"""Registry #19 (connector half) — the staleness endpoint that closes gap #7.

Telegraf restarts a connector that exits. Nothing catches a connector that stays
alive while its source stops producing. This endpoint is what kubelet probes to
catch exactly that, so it must be driven by *data received*, not by whether the
session still answers — a hung source answers fine.
"""

from __future__ import annotations

import subprocess
import sys
import time
import urllib.error
import urllib.request

import pytest

from tests.phase1.conftest import REPO, MockServer, connector_env

HEALTH_PORT = 18099
THRESHOLD_S = 8


def _health() -> int:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{HEALTH_PORT}/healthz", timeout=5) as r:
            return r.status
    except urllib.error.HTTPError as exc:
        return exc.code
    except (urllib.error.URLError, OSError):
        return 0  # not listening yet — keep polling


def _wait_for_health(expected: int, timeout: float) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = _health()
        if last == expected:
            return
        time.sleep(1)
    raise AssertionError(f"health never became {expected}; last={last}")


@pytest.fixture
def connector_against_frozen_source():
    """A source that serves data, then keeps its session open and goes quiet."""
    with MockServer(freeze_after_s=6) as server:
        proc = subprocess.Popen(
            [sys.executable, "-m", "connectors.opcua.connector"],
            cwd=REPO,
            env=connector_env(
                server.endpoint,
                pipeline_id="health-test",
                HEALTH_PORT=str(HEALTH_PORT),
                OPCUA_STALENESS_THRESHOLD_S=str(THRESHOLD_S),
            ),
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        try:
            yield proc
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()


def test_healthy_while_data_is_flowing(connector_against_frozen_source) -> None:
    _wait_for_health(200, timeout=30)


def test_goes_unhealthy_once_the_source_stops_producing(connector_against_frozen_source) -> None:
    """The process stays alive throughout — that is the whole point."""
    proc = connector_against_frozen_source
    _wait_for_health(200, timeout=30)

    _wait_for_health(503, timeout=THRESHOLD_S + 40)

    assert proc.poll() is None, (
        "the connector exited instead of reporting unhealthy — kubelet, not the "
        "process, decides what happens to a stalled pod"
    )
