"""Phase 3 fixtures — a local kind cluster, never a shared one.

kubectl on this machine defaults to a live shared EKS dev cluster. Every test
here pins the kind context explicitly and asserts it, because a runtime that
silently picks up the ambient kubeconfig would create Deployments on shared
infrastructure.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
CLUSTER = "stratahub-live"
CONTEXT = f"kind-{CLUSTER}"
NAMESPACE = "default"
PRODUCER_IMAGE = "data-platform-producer:latest"


_LOCAL_HOSTS = ("127.0.0.1", "localhost", "0.0.0.0", "[::1]")
_verified_local = False


def _assert_context_is_local() -> None:
    """Refuse to touch a cluster that is not demonstrably local.

    The name is checked, but the real test is the API server address: a kind
    cluster's server is on 127.0.0.1, a client-managed EKS cluster's is an AWS
    endpoint. Naming alone would not stop a context called `kind-prod`.

    This exists because this machine's default kubectl context is a live
    client-managed EKS cluster. Nothing here may run against it — see CLAUDE.md
    and the first invariant in the project skill.
    """
    global _verified_local
    if _verified_local:
        return

    assert CONTEXT.startswith("kind-"), f"refusing to run against context {CONTEXT!r}"

    got = subprocess.run(
        ["kubectl", "config", "view", "-o",
         f'jsonpath={{.clusters[?(@.name=="{CONTEXT}")].cluster.server}}'],
        capture_output=True, text=True, timeout=60,
    )
    server = got.stdout.strip()
    assert server, f"no cluster entry for context {CONTEXT!r}"
    assert any(host in server for host in _LOCAL_HOSTS), (
        f"context {CONTEXT!r} points at {server}, which is not a local cluster. "
        "Refusing to run — these tests must never touch a shared or client cluster."
    )
    _verified_local = True


def kubectl(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    """Every cluster-touching call in the suite goes through here, so the guard
    cannot be bypassed by forgetting --context on one command."""
    _assert_context_is_local()
    return subprocess.run(["kubectl", "--context", CONTEXT, "-n", NAMESPACE, *args],
                          capture_output=True, text=True, timeout=300, check=check)


@pytest.fixture(scope="session")
def kind_cluster() -> str:
    """Assert we are pointed at kind, then hand back the context name."""
    got = subprocess.run(["kubectl", "config", "get-contexts", "-o", "name"],
                         capture_output=True, text=True, timeout=60)
    assert CONTEXT in got.stdout.split(), (
        f"no {CONTEXT} context — create it with `kind create cluster --name {CLUSTER}`. "
        "These tests must not run against a shared cluster."
    )
    nodes = kubectl("get", "nodes", "-o", "name")
    assert "node/" in nodes.stdout, nodes.stderr
    return CONTEXT


@pytest.fixture(scope="session")
def producer_image_in_kind(kind_cluster) -> str:
    """Build the producer image and load it into the cluster's node."""
    subprocess.run(
        ["docker", "build", "-q", "-f", "services/producer_service/Dockerfile",
         "-t", PRODUCER_IMAGE, "."],
        cwd=REPO, check=True, capture_output=True, text=True, timeout=1800,
    )
    subprocess.run(
        [str(Path.home() / ".local/bin/kind"), "load", "docker-image",
         PRODUCER_IMAGE, "--name", CLUSTER],
        check=True, capture_output=True, text=True, timeout=900,
    )
    return PRODUCER_IMAGE


class _Settings:
    """Just the fields the runtime reads."""
    kafka_bootstrap_servers = "kafka:9092"
    producer_image = PRODUCER_IMAGE
    image_pull_policy = "IfNotPresent"


@pytest.fixture
def runtime(kind_cluster):
    from common.app_common.runtime.kubernetes_runtime import KubernetesRuntimeAdapter

    return KubernetesRuntimeAdapter(_Settings(), namespace=NAMESPACE, kube_context=CONTEXT)


@pytest.fixture
def cleanup_pipelines():
    """Delete everything a test created, however it ended."""
    created: list[str] = []
    yield created
    for pipeline_id in created:
        kubectl("delete", "deployment", f"producer-{pipeline_id}",
                "--ignore-not-found", "--wait=false", check=False)
        kubectl("delete", "configmap", f"producer-{pipeline_id}",
                "--ignore-not-found", check=False)


@pytest.fixture(scope="session")
def in_cluster_stack(kind_cluster, producer_image_in_kind):
    """The whole system inside kind: Redis, Kafka, ClickHouse, the OPC UA mock,
    task_manager and the consumer pool.

    Everything in-cluster on purpose — a producer pod reaching a broker on the
    developer's host would be testing host networking, not the system.
    """
    import subprocess as sp

    for image in ["data-platform-task-manager:latest", "data-platform-consumer-pool:latest"]:
        sp.run(["docker", "build", "-q", "-f",
                f"services/{'task_manager/task_manager' if 'task-manager' in image else 'consumer_pool'}/Dockerfile",
                "-t", image, "."], cwd=REPO, check=True, capture_output=True, timeout=1800)
        sp.run([str(Path.home() / ".local/bin/kind"), "load", "docker-image", image,
                "--name", CLUSTER], check=True, capture_output=True, timeout=900)

    kubectl("apply", "-f", str(REPO / "k8s/dev"))
    kubectl("rollout", "restart", "deployment/task-manager", "deployment/consumer-pool")
    for deployment in ["redis", "kafka", "clickhouse", "opcua-mock",
                       "task-manager", "consumer-pool"]:
        kubectl("rollout", "status", f"deployment/{deployment}", "--timeout=420s")
    yield


class PortForward:
    """kubectl port-forward as a context manager."""

    def __init__(self, target: str, local_port: int, remote_port: int) -> None:
        self.target, self.local_port, self.remote_port = target, local_port, remote_port
        self._proc = None

    def __enter__(self) -> "PortForward":
        import socket
        import time

        _assert_context_is_local()

        self._proc = subprocess.Popen(
            ["kubectl", "--context", CONTEXT, "-n", NAMESPACE, "port-forward",
             self.target, f"{self.local_port}:{self.remote_port}"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", self.local_port), timeout=1):
                    return self
            except OSError:
                time.sleep(0.5)
        raise AssertionError(f"port-forward to {self.target} never came up")

    def __exit__(self, *_: object) -> None:
        if self._proc:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._proc.kill()


@pytest.fixture(scope="session")
def api(in_cluster_stack):
    with PortForward("service/task-manager", 18000, 8000):
        yield "http://localhost:18000"


@pytest.fixture(scope="session")
def clickhouse(in_cluster_stack):
    with PortForward("service/clickhouse", 18123, 8123):
        yield "http://localhost:18123"
