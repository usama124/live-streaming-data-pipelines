"""Registry #15-#18, #20 — lifecycle through KubernetesRuntime on a real cluster.

`desired_state` is a replica count. There is no reconcile loop of ours to test
because there isn't one any more: kube-controller-manager is it.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid

import pytest

from common.app_common.models import PipelineConfig, PipelineState, PipelineType
from common.app_common.runtime.kubernetes_runtime import (
    deployment_name,
    render_producer_deployment,
    staleness_threshold_for,
)
from tests.phase3.conftest import kubectl

pytestmark = pytest.mark.usefixtures("producer_image_in_kind")


def _pipeline(pipeline_id: str) -> PipelineConfig:
    return PipelineConfig(
        pipeline_id=pipeline_id, pipeline_type=PipelineType.LIVE,
        topic=f"pipeline.{pipeline_id}.events",
        user_id="1", collection_number=3, table_name="k8s_case",
        source_type="opcua",
        source_options={"endpoint": "opc.tcp://unreachable:4840/",
                        "node_ids": ["ns=2;i=2"]},
    )


def _state(config: PipelineConfig) -> PipelineState:
    return PipelineState(pipeline_id=config.pipeline_id,
                         pipeline_type=PipelineType.LIVE, topic=config.topic)


def _replicas(name: str) -> int:
    got = kubectl("get", "deployment", name, "-o", "jsonpath={.spec.replicas}", check=False)
    return int(got.stdout) if got.stdout.strip().isdigit() else -1


def _wait(predicate, timeout: float = 120.0, what: str = "condition"):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(2)
    raise AssertionError(f"timed out waiting for {what}; last={last}")


def test_15_start_creates_a_deployment_with_one_replica(runtime, cleanup_pipelines) -> None:
    pipeline_id = f"t15-{uuid.uuid4().hex[:8]}"
    cleanup_pipelines.append(pipeline_id)
    config = _pipeline(pipeline_id)

    name, consumer = asyncio.run(runtime.start_live_pipeline(config, _state(config)))

    assert name == deployment_name(pipeline_id)
    assert consumer == "", "a pipeline must not start its own consumer — the pool is shared"
    assert _replicas(name) == 1


def test_16_stop_scales_to_zero_rather_than_deleting(runtime, cleanup_pipelines) -> None:
    """Scaling to zero keeps the Deployment and its history; a stopped pipeline
    is still a pipeline."""
    pipeline_id = f"t16-{uuid.uuid4().hex[:8]}"
    cleanup_pipelines.append(pipeline_id)
    config = _pipeline(pipeline_id)
    name = deployment_name(pipeline_id)

    asyncio.run(runtime.start_live_pipeline(config, _state(config)))
    assert _replicas(name) == 1

    asyncio.run(runtime.stop_live_pipeline(config, _state(config)))

    assert _replicas(name) == 0
    _wait(lambda: not json.loads(
        kubectl("get", "pods", "-l", f"pipeline-id={pipeline_id}", "-o", "json").stdout
    )["items"], what="pods to go away after scaling to zero")


def test_17_restart_rolls_the_pods(runtime, cleanup_pipelines) -> None:
    pipeline_id = f"t17-{uuid.uuid4().hex[:8]}"
    cleanup_pipelines.append(pipeline_id)
    config = _pipeline(pipeline_id)
    name = deployment_name(pipeline_id)

    asyncio.run(runtime.start_live_pipeline(config, _state(config)))
    _wait(lambda: json.loads(
        kubectl("get", "pods", "-l", f"pipeline-id={pipeline_id}", "-o", "json").stdout
    )["items"], what="the first pod")
    before = {p["metadata"]["name"] for p in json.loads(
        kubectl("get", "pods", "-l", f"pipeline-id={pipeline_id}", "-o", "json").stdout)["items"]}

    asyncio.run(runtime.restart_live_pipeline(config, _state(config)))

    assert _replicas(name) == 1
    _wait(
        lambda: {p["metadata"]["name"] for p in json.loads(
            kubectl("get", "pods", "-l", f"pipeline-id={pipeline_id}", "-o", "json").stdout
        )["items"]} - before,
        what="a new pod after the rollout",
    )


def test_18_a_crashed_container_is_restarted_by_kubernetes(runtime, cleanup_pipelines) -> None:
    """restartPolicy: Always is what replaces the Watchdog. The connector cannot
    reach its source here, so it exits — and Kubernetes keeps bringing it back."""
    pipeline_id = f"t18-{uuid.uuid4().hex[:8]}"
    cleanup_pipelines.append(pipeline_id)
    config = _pipeline(pipeline_id)

    asyncio.run(runtime.start_live_pipeline(config, _state(config)))

    def _pod_is_being_restarted() -> bool:
        pods = json.loads(
            kubectl("get", "pods", "-l", f"pipeline-id={pipeline_id}", "-o", "json").stdout
        )["items"]
        return bool(pods) and pods[0]["status"]["phase"] in {"Running", "Pending"}

    _wait(_pod_is_being_restarted, timeout=180, what="the pod to be scheduled and kept alive")

    spec = json.loads(kubectl("get", "deployment", deployment_name(pipeline_id),
                              "-o", "json").stdout)["spec"]["template"]["spec"]
    assert spec["restartPolicy"] == "Always"


def test_20_a_failed_start_raises_rather_than_going_quiet(runtime) -> None:
    """The API turns this into a synchronous error. Before, a bad start showed up
    on the next 3-second controller tick, if at all."""
    config = _pipeline("Invalid_Name_With_Underscores_And_Caps")

    with pytest.raises(Exception) as caught:
        asyncio.run(runtime.start_live_pipeline(config, _state(config)))

    assert "Invalid" in str(caught.value) or "invalid" in str(caught.value), caught.value


def test_liveness_probe_is_present_with_a_real_threshold() -> None:
    """A producer without the probe is the Phase 1 gap shipped to production."""
    config = _pipeline("probe-check")
    manifest = render_producer_deployment(
        config, replicas=1, producer_image="data-platform-producer:latest")

    container = manifest["spec"]["template"]["spec"]["containers"][0]
    probe = container["livenessProbe"]

    assert probe["httpGet"]["path"] == "/healthz"
    assert staleness_threshold_for(config) == 60.0
    # The probe must not be able to fire before the connector has had a chance
    # to connect and read once.
    assert probe["initialDelaySeconds"] > staleness_threshold_for(config)
