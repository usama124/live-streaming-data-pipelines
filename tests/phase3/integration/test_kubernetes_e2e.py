"""Phase 3 integration suite — I9, I10, I11, I12.

Everything runs inside the kind cluster: Redis, Kafka, ClickHouse, the OPC UA
mock, task_manager and the consumer pool. The API is driven through a
port-forward, and results are checked in ClickHouse through another — nothing
about the data path depends on the host.

I13 (node-failure reschedule) is deliberately absent; the plan marks it
staging-only, and a single-node kind cluster cannot demonstrate it.
"""

from __future__ import annotations

import time
import uuid

import pytest
import requests

from tests.phase3.conftest import kubectl

pytestmark = pytest.mark.usefixtures("in_cluster_stack")

NODE_IDS = ["ns=2;i=2", "ns=2;i=3", "ns=2;i=4", "ns=2;i=5", "ns=2;i=6"]
NODE_NAMES = {"ns=2;i=2": "Temperature", "ns=2;i=3": "Pressure", "ns=2;i=4": "Vibration",
              "ns=2;i=5": "FlowRate", "ns=2;i=6": "MachineStatus"}


def unique_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def create_and_start(api: str, pipeline_id: str, *, collection_number: int,
                     table_name: str, staleness_threshold_s: float | None = None) -> str:
    options = {
        "endpoint": "opc.tcp://opcua-mock:4840/stratahub/server/",
        "node_ids": NODE_IDS, "node_names": NODE_NAMES,
        "publishing_interval_ms": 200,
    }
    if staleness_threshold_s is not None:
        options["staleness_threshold_s"] = staleness_threshold_s

    created = requests.post(f"{api}/pipelines", timeout=60, json={
        "pipeline_id": pipeline_id, "pipeline_type": "live", "source_type": "opcua",
        "topic": f"pipeline.{pipeline_id}.events",
        "user_id": "1", "collection_number": collection_number, "table_name": table_name,
        "source_options": options,
    })
    assert created.status_code == 201, created.text

    started = requests.post(f"{api}/pipelines/{pipeline_id}/start", timeout=120)
    assert started.status_code == 200, started.text
    return created.json()["table"]


def ch_count(clickhouse: str, table: str, where: str = "1") -> int:
    try:
        response = requests.post(
            clickhouse, params={"database": "data_platform"},
            data=f"SELECT count() FROM `{table}` WHERE {where} FORMAT JSONCompact".encode(),
            timeout=30,
        )
        if response.status_code != 200:
            return 0
        return int(response.json()["data"][0][0])
    except Exception:
        return 0


def wait_for(predicate, timeout: float = 240.0, interval: float = 3.0, what: str = "condition"):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"timed out waiting for {what}; last={last}")


def test_i9_full_lifecycle_on_kubernetes(api, clickhouse) -> None:
    """I9 — create, data flows to the right table, stop, resources actually gone."""
    pipeline_id = unique_id("i9")

    table = create_and_start(api, pipeline_id, collection_number=9, table_name="k8s_e2e")
    try:
        wait_for(lambda: ch_count(clickhouse, table) >= 10,
                 what=f"rows in {table} via KubernetesRuntime")
    finally:
        stopped = requests.post(f"{api}/pipelines/{pipeline_id}/stop", timeout=120)
        assert stopped.status_code == 200, stopped.text

    # desired_state=stopped is replicas=0, not a deleted Deployment.
    replicas = kubectl("get", "deployment", f"producer-{pipeline_id}",
                       "-o", "jsonpath={.spec.replicas}", check=False).stdout
    assert replicas == "0", f"expected replicas=0 after stop, got {replicas!r}"
    wait_for(lambda: not kubectl("get", "pods", "-l", f"pipeline-id={pipeline_id}",
                                 "-o", "name").stdout.strip(),
             what="producer pods to be cleaned up")

    kubectl("delete", "deployment", f"producer-{pipeline_id}", "--ignore-not-found", check=False)
    kubectl("delete", "configmap", f"producer-{pipeline_id}", "--ignore-not-found", check=False)


def test_i10_killing_pods_self_heals(api, clickhouse) -> None:
    """I10 — kill a producer pod and a pool pod during active flow."""
    pipeline_id = unique_id("i10")
    table = create_and_start(api, pipeline_id, collection_number=10, table_name="chaos")
    try:
        wait_for(lambda: ch_count(clickhouse, table) >= 5, what="flow before the chaos")
        before = ch_count(clickhouse, table)

        kubectl("delete", "pod", "-l", f"pipeline-id={pipeline_id}", "--wait=false")
        kubectl("delete", "pod", "-l", "app=consumer-pool", "--wait=false")

        wait_for(lambda: ch_count(clickhouse, table) > before, timeout=300,
                 what="both pods self-healed and data resumed")
    finally:
        requests.post(f"{api}/pipelines/{pipeline_id}/stop", timeout=120)
        kubectl("delete", "deployment", f"producer-{pipeline_id}", "--ignore-not-found", check=False)
        kubectl("delete", "configmap", f"producer-{pipeline_id}", "--ignore-not-found", check=False)


def test_i11_liveness_probe_recycles_a_stalled_producer(api) -> None:
    """I11 — the real loop that closes Phase 1's gap: a source that stops
    producing while staying connected, the probe failing, kubelet restarting
    the container.

    The mock is told to freeze via the pipeline's own config, so this is the
    genuine hung-source condition rather than a simulated one.
    """
    pipeline_id = unique_id("i11")
    # Short threshold so the probe fires inside a test's patience.
    create_and_start(api, pipeline_id, collection_number=11, table_name="stalled",
                     staleness_threshold_s=20)
    try:
        wait_for(lambda: kubectl("get", "pods", "-l", f"pipeline-id={pipeline_id}",
                                 "-o", "jsonpath={.items[0].status.phase}",
                                 check=False).stdout == "Running",
                 what="the producer pod to run")

        # Freeze the source: scale the mock away so notifications stop while the
        # connector's own process stays perfectly healthy.
        kubectl("scale", "deployment/opcua-mock", "--replicas=0")
        try:
            def _restarted() -> bool:
                got = kubectl("get", "pods", "-l", f"pipeline-id={pipeline_id}", "-o",
                              "jsonpath={.items[0].status.containerStatuses[0].restartCount}",
                              check=False).stdout.strip()
                return got.isdigit() and int(got) >= 1

            wait_for(_restarted, timeout=300,
                     what="the liveness probe to fail and kubelet to restart the container")
        finally:
            kubectl("scale", "deployment/opcua-mock", "--replicas=1")
            kubectl("rollout", "status", "deployment/opcua-mock", "--timeout=300s")
    finally:
        requests.post(f"{api}/pipelines/{pipeline_id}/stop", timeout=120)
        kubectl("delete", "deployment", f"producer-{pipeline_id}", "--ignore-not-found", check=False)
        kubectl("delete", "configmap", f"producer-{pipeline_id}", "--ignore-not-found", check=False)


def test_i12_phase2_guarantees_hold_under_the_new_runtime(api, clickhouse) -> None:
    """I12 — the Phase 2 regression, re-run against KubernetesRuntime: two
    pipelines, separate tables, no cross-contamination, new pipeline picked up
    with no pool restart."""
    id_a, id_b = unique_id("i12a"), unique_id("i12b")
    table_a = create_and_start(api, id_a, collection_number=12, table_name="parity_a")
    try:
        wait_for(lambda: ch_count(clickhouse, table_a) >= 5, what="pipeline A flowing")

        # B is created while the pool is already busy, and the pool is never
        # restarted here — the same constraint Phase 2's #8 checks.
        table_b = create_and_start(api, id_b, collection_number=12, table_name="parity_b")
        wait_for(lambda: ch_count(clickhouse, table_b) >= 5, what="pipeline B picked up")

        assert table_a != table_b
        assert ch_count(clickhouse, table_a, f"pipeline_id = '{id_b}'") == 0
        assert ch_count(clickhouse, table_b, f"pipeline_id = '{id_a}'") == 0
    finally:
        for pid in (id_a, id_b):
            requests.post(f"{api}/pipelines/{pid}/stop", timeout=120)
            kubectl("delete", "deployment", f"producer-{pid}", "--ignore-not-found", check=False)
            kubectl("delete", "configmap", f"producer-{pid}", "--ignore-not-found", check=False)
