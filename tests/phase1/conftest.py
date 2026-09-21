"""Shared helpers for Phase 1 tests.

The connector is a subprocess that talks to a real OPC UA server, so these
helpers start a real mock server rather than mocking asyncua. A mocked OPC UA
client would prove nothing about whether the connector actually speaks OPC UA.
"""

from __future__ import annotations

import json
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]

# The mock server exposes these under ns=2; i=2.. in NODE_SCHEMA declaration order.
NODE_IDS = ["ns=2;i=2", "ns=2;i=3", "ns=2;i=4", "ns=2;i=5", "ns=2;i=6"]
NODE_NAMES = {
    "ns=2;i=2": "Temperature",
    "ns=2;i=3": "Pressure",
    "ns=2;i=4": "Vibration",
    "ns=2;i=5": "FlowRate",
    "ns=2;i=6": "MachineStatus",
}


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_port(port: int, timeout: float = 30.0, proc: subprocess.Popen | None = None,
                  log=None) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc is not None and proc.poll() is not None:
            raise AssertionError(f"process exited early (rc={proc.returncode}):\n{_tail(log)}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return
        except OSError:
            time.sleep(0.2)
    raise TimeoutError(f"nothing listening on {port} after {timeout}s\n{_tail(log)}")


def _tail(log, lines: int = 20) -> str:
    """Last few lines of a subprocess log file, for failure messages."""
    if log is None:
        return "(no log captured)"
    log.flush()
    log.seek(0)
    return "".join(log.read().decode(errors="replace").splitlines(keepends=True)[-lines:])


class MockServer:
    """A real OPC UA server in a subprocess."""

    def __init__(self) -> None:
        self.port = free_port()
        self.endpoint = f"opc.tcp://127.0.0.1:{self.port}/stratahub/server/"
        self._proc: subprocess.Popen | None = None
        self._log = None

    def __enter__(self) -> "MockServer":
        # asyncua logs heavily at startup. stderr=PIPE with nobody draining it
        # fills the 64K pipe buffer and the server blocks before it ever listens
        # — use a file, which also gives us the log on failure.
        self._log = tempfile.TemporaryFile()
        self._proc = subprocess.Popen(
            [sys.executable, "-m", "connectors.opcua.mock_server"],
            cwd=REPO,
            env={"PATH": "/usr/bin:/bin", "OPCUA_SERVER_PORT": str(self.port),
                 "OPCUA_SERVER_HOST": "127.0.0.1"},
            stdout=self._log,
            stderr=subprocess.STDOUT,
        )
        wait_for_port(self.port, proc=self._proc, log=self._log)
        return self

    def __exit__(self, *_: object) -> None:
        if self._log:
            self._log.close()
        if self._proc:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._proc.kill()


def connector_env(endpoint: str, pipeline_id: str = "pipe-1", **overrides: str) -> dict[str, str]:
    env = {
        "PATH": "/usr/bin:/bin",
        "PIPELINE_ID": pipeline_id,
        "OPCUA_ENDPOINT": endpoint,
        "OPCUA_NODE_IDS": ",".join(NODE_IDS),
        "OPCUA_NODE_NAMES_JSON": json.dumps(NODE_NAMES),
        "OPCUA_PUBLISHING_INTERVAL_MS": "200",
    }
    env.update(overrides)
    return env


def read_json_lines(proc: subprocess.Popen, count: int, timeout: float = 45.0) -> list[dict]:
    """Read `count` JSON records from the connector's stdout, or fail loudly."""
    records: list[dict] = []
    deadline = time.time() + timeout
    while len(records) < count and time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            break
        line = line.strip()
        if line:
            records.append(json.loads(line))
    if len(records) < count:
        raise AssertionError(f"got {len(records)}/{count} records before timeout")
    return records


@pytest.fixture
def mock_server():
    with MockServer() as srv:
        yield srv
