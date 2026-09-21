"""Shared helpers for Phase 1 tests.

The connector is a subprocess that talks to a real OPC UA server, so these
helpers start a real mock server rather than mocking asyncua. A mocked OPC UA
client would prove nothing about whether the connector actually speaks OPC UA.
"""

from __future__ import annotations

import json
import socket
import subprocess
import uuid
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


def unique_id(prefix: str) -> str:
    """A fresh pipeline id per run, so each test gets a brand-new Kafka topic.

    Topics outlive a test run. A fixed id means a consumer reading from
    `earliest` sees the *previous* run's messages and the test passes on stale
    data — including when the thing it checks is actually broken.
    """
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


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


# ── Docker-backed helpers ────────────────────────────────────────────────────
# Telegraf, the mock OPC UA servers and Kafka all run as containers on the
# compose network. Running the mock servers in containers too (rather than on
# the host) keeps every endpoint a plain service name and avoids host-gateway
# plumbing that behaves differently on Linux and macOS.

NETWORK = "data-platform_backend"
PRODUCER_IMAGE = "data-platform-producer:latest"
KAFKA_INTERNAL = "kafka:9092"
KAFKA_EXTERNAL = "localhost:9094"


def _compose(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose", *args], cwd=REPO, check=check,
        capture_output=True, text=True, timeout=600,
    )


@pytest.fixture(scope="session")
def kafka() -> str:
    """Kafka from the root compose stack. Left as found: only torn down if we started it."""
    running = _compose("ps", "--status", "running", "--services").stdout.split()
    started_it = "kafka" not in running

    _compose("up", "-d", "--wait", "kafka")
    try:
        yield KAFKA_INTERNAL
    finally:
        if started_it:
            _compose("rm", "-sf", "kafka", check=False)


@pytest.fixture(scope="session")
def producer_image() -> str:
    subprocess.run(
        ["docker", "build", "-q", "-f", "services/producer_service/Dockerfile",
         "-t", PRODUCER_IMAGE, "."],
        cwd=REPO, check=True, capture_output=True, text=True, timeout=1800,
    )
    return PRODUCER_IMAGE


class Container:
    """A container that is always removed, however the test ends."""

    def __init__(self, image: str, name: str, **kwargs) -> None:
        self.image, self.name, self.kwargs = image, name, kwargs
        self._c = None

    def __enter__(self) -> "Container":
        import docker

        client = docker.from_env()
        try:
            client.containers.get(self.name).remove(force=True)
        except Exception:
            pass
        self._c = client.containers.run(
            self.image, name=self.name, detach=True, network=NETWORK, **self.kwargs
        )
        return self

    def logs(self) -> str:
        self._c.reload()
        return self._c.logs().decode(errors="replace")

    def status(self) -> str:
        self._c.reload()
        return self._c.status

    def __exit__(self, *_: object) -> None:
        if self._c:
            try:
                self._c.remove(force=True)
            except Exception:
                pass


def mock_server_container(name: str, freeze_after_s: float = 0.0) -> Container:
    """The mock OPC UA server, from the producer image (it already has asyncua)."""
    return Container(
        PRODUCER_IMAGE, name,
        entrypoint=["python3", "-m", "connectors.opcua.mock_server"],
        working_dir="/opt",
        environment={"OPCUA_SERVER_PORT": "4840", "PYTHONPATH": "/opt",
                     "OPCUA_STATUS_EVERY": "2",
                     "OPCUA_FREEZE_AFTER_S": str(freeze_after_s)},
    )


def telegraf_container(name: str, config_text: str) -> Container:
    return Container(PRODUCER_IMAGE, name, environment={"TELEGRAF_CONFIG": config_text})


def opcua_source_options(host: str) -> dict:
    return {
        "endpoint": f"opc.tcp://{host}:4840/stratahub/server/",
        "node_ids": NODE_IDS,
        "node_names": NODE_NAMES,
        "publishing_interval_ms": 200,
    }


def consume(topic: str, count: int, timeout: float = 90.0) -> list[dict]:
    """Read up to `count` JSON messages from `topic` via Kafka's external listener."""
    import asyncio

    from aiokafka import AIOKafkaConsumer

    async def _run() -> list[dict]:
        consumer = AIOKafkaConsumer(
            topic, bootstrap_servers=KAFKA_EXTERNAL,
            auto_offset_reset="earliest", group_id=None,
            value_deserializer=lambda v: json.loads(v.decode()),
        )
        await consumer.start()
        try:
            out: list[dict] = []
            deadline = time.time() + timeout
            while len(out) < count and time.time() < deadline:
                batch = await consumer.getmany(timeout_ms=2000)
                for records in batch.values():
                    out.extend(r.value for r in records)
            return out
        finally:
            await consumer.stop()

    return asyncio.run(_run())


@pytest.fixture(scope="session")
def platform_stack(kafka):
    """The real API and controller, from the root compose file."""
    _compose("up", "-d", "--wait", "clickhouse", "task-manager", "controller")

    deadline = time.time() + 120
    while time.time() < deadline:
        try:
            import urllib.request

            with urllib.request.urlopen(f"http://localhost:8000/health", timeout=5) as r:
                if json.load(r)["status"] == "ok":
                    break
        except Exception:
            time.sleep(2)
    else:
        raise AssertionError("task-manager never became healthy")

    yield
