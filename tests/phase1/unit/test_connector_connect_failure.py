"""Registry #5 — connector fails to connect: clear failure, no silent hang.

Two shapes of "cannot connect", because they fail differently:
  - nothing listening        → the OS refuses immediately
  - something listening that
    never speaks OPC UA      → TCP connects, the handshake never completes

The second is the dangerous one. Without an explicit timeout the connector sits
there forever, alive and silent, which is precisely the state Telegraf cannot
detect and the whole design is arranged to avoid.
"""

from __future__ import annotations

import socket
import subprocess
import sys

from tests.phase1.conftest import REPO, connector_env, free_port


def _run_connector(endpoint: str, timeout: float, **env_overrides: str):
    proc = subprocess.Popen(
        [sys.executable, "-m", "connectors.opcua.connector"],
        cwd=REPO,
        env=connector_env(endpoint, **env_overrides),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _, stderr = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        _, stderr = proc.communicate()
        raise AssertionError(
            f"connector hung instead of failing — no exit after {timeout}s\n{stderr}"
        )
    return proc.returncode, stderr


def test_unreachable_host_exits_nonzero_with_a_clear_message() -> None:
    endpoint = f"opc.tcp://127.0.0.1:{free_port()}/stratahub/server/"

    returncode, stderr = _run_connector(endpoint, timeout=60)

    assert returncode != 0, "a connector that cannot connect must not exit 0"
    assert endpoint in stderr, f"failure message must name the endpoint:\n{stderr}"
    assert "OPC UA connector failed" in stderr, stderr


def test_unresponsive_endpoint_times_out_rather_than_hanging() -> None:
    """TCP accepts, OPC UA never answers — the connector must give up, not wait."""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)  # accepts the TCP connection, then says nothing at all
    port = listener.getsockname()[1]
    endpoint = f"opc.tcp://127.0.0.1:{port}/stratahub/server/"

    try:
        returncode, stderr = _run_connector(
            endpoint, timeout=60, OPCUA_CONNECT_TIMEOUT_S="5"
        )
    finally:
        listener.close()

    assert returncode != 0, "a connector that cannot handshake must not exit 0"
    assert "OPC UA connector failed" in stderr, stderr
