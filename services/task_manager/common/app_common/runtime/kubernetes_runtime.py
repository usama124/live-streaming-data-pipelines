from __future__ import annotations

"""Kubernetes implementation of RuntimeAdapter.

`desired_state` is a replica count: running is 1, stopped is 0. There is no
reconcile loop here because `kube-controller-manager` already is one — that is
the whole point of the move, and it is why the Controller, Watchdog and leader
lock become deletable.

Every call is synchronous against the API server, so a failed start raises here
and the API can return the error to its caller instead of surfacing it on some
later poll.
"""

import asyncio
import logging
from pathlib import Path
from string import Template
from typing import Any

import yaml
from kubernetes import client, config as kube_config
from kubernetes.client.rest import ApiException

from common.app_common.models import PipelineConfig, PipelineState
from common.app_common.runtime.base import RuntimeAdapter
from common.app_common.sources import spec_for
from common.app_common.telegraf_config import render_telegraf_config

logger = logging.getLogger("runtime.kubernetes")

HEALTH_PORT = 8080

# Per-source defaults live on the source registry (common/app_common/sources.py)
# so a new source type carries its own number rather than silently inheriting
# OPC UA's. Override per pipeline with source_options.staleness_threshold_s.
DEFAULT_STALENESS_THRESHOLD_S = 60.0

_TEMPLATE_NAME = "producer-deployment-template.yaml"


def _template_path() -> Path:
    candidates = [Path("/opt/k8s")] + [
        parent / "k8s" for parent in Path(__file__).resolve().parents
        if (parent / "k8s").is_dir()
    ]
    for directory in candidates:
        path = directory / _TEMPLATE_NAME
        if path.is_file():
            return path
    raise FileNotFoundError(
        f"{_TEMPLATE_NAME} not found in {[str(c) for c in candidates]}"
    )


def staleness_threshold_for(config: PipelineConfig) -> float:
    if config.source_options.staleness_threshold_s is not None:
        return float(config.source_options.staleness_threshold_s)
    try:
        return spec_for(config.source_type).default_staleness_threshold_s
    except ValueError:
        return DEFAULT_STALENESS_THRESHOLD_S


def deployment_name(pipeline_id: str) -> str:
    return f"producer-{pipeline_id}"


def render_producer_deployment(
    config: PipelineConfig, *, replicas: int, producer_image: str,
    image_pull_policy: str = "IfNotPresent",
) -> dict[str, Any]:
    """The Deployment manifest for one pipeline's producer."""
    rendered = Template(_template_path().read_text()).substitute(
        PIPELINE_ID=config.pipeline_id,
        REPLICAS=replicas,
        PRODUCER_IMAGE=producer_image,
        IMAGE_PULL_POLICY=image_pull_policy,
        HEALTH_PORT=HEALTH_PORT,
        STALENESS_THRESHOLD_S=staleness_threshold_for(config),
        # Give the connector time to connect and read once before the probe can
        # fail it, or a slow source restarts forever without ever starting.
        PROBE_INITIAL_DELAY_S=int(staleness_threshold_for(config)) + 30,
    )
    return yaml.safe_load(rendered)


class KubernetesRuntimeAdapter(RuntimeAdapter):
    def __init__(self, settings: Any, namespace: str = "default",
                 kube_context: str | None = None) -> None:
        self._s = settings
        self._namespace = namespace
        self._context = kube_context
        self._loaded = False

    # ── RuntimeAdapter ────────────────────────────────────────────────────

    async def start_live_pipeline(self, config: PipelineConfig, state: PipelineState) -> tuple[str, str]:
        name = deployment_name(config.pipeline_id)
        await asyncio.to_thread(self._apply_config_map, config)
        await asyncio.to_thread(self._apply_deployment, config, 1)
        logger.info("Deployment %s at replicas=1", name)
        # The pool is a separate, shared Deployment — a pipeline never starts one.
        return name, ""

    async def stop_live_pipeline(self, config: PipelineConfig, state: PipelineState) -> None:
        await asyncio.to_thread(self._scale, deployment_name(config.pipeline_id), 0)
        logger.info("Deployment %s scaled to 0", deployment_name(config.pipeline_id))

    async def restart_live_pipeline(self, config: PipelineConfig, state: PipelineState) -> tuple[str, str]:
        """Re-applies the config and rolls the pods, rather than scaling to zero
        and back — a rollout keeps the Deployment's own history and avoids a
        window where the pipeline simply does not exist."""
        name = deployment_name(config.pipeline_id)
        await asyncio.to_thread(self._apply_config_map, config)
        await asyncio.to_thread(self._apply_deployment, config, 1)
        await asyncio.to_thread(self._restart_rollout, name)
        logger.info("Deployment %s rolled", name)
        return name, ""

    async def get_logs(self, container_name: str, *, tail: int = 200) -> str:
        return await asyncio.to_thread(self._pod_logs, container_name, tail)

    async def is_running(self, container_name: str) -> bool:
        return await asyncio.to_thread(self._ready, container_name)

    # ── Kubernetes plumbing ───────────────────────────────────────────────

    def _load(self) -> None:
        """In-cluster config, or an explicitly named context — never the ambient one.

        Falling back to whatever `kubectl config current-context` happens to be
        is how a developer running this locally deploys into a shared or
        client-managed cluster by accident. There is no safe default here, so
        there is no default: outside a cluster, the context must be named.
        """
        if self._loaded:
            return
        try:
            kube_config.load_incluster_config()
        except kube_config.ConfigException:
            if not self._context:
                raise RuntimeError(
                    "No in-cluster Kubernetes config and no explicit kube_context. "
                    "Refusing to fall back to the ambient kubectl context, which may "
                    "point at a shared or client-managed cluster. Set KUBE_CONTEXT "
                    "(e.g. kind-stratahub-live) to run against a local cluster."
                ) from None
            kube_config.load_kube_config(context=self._context)
        self._loaded = True

    def _apps(self) -> client.AppsV1Api:
        self._load()
        return client.AppsV1Api()

    def _core(self) -> client.CoreV1Api:
        self._load()
        return client.CoreV1Api()

    def _apply_config_map(self, config: PipelineConfig) -> None:
        """The rendered Telegraf config, mounted into the pod as a file."""
        name = deployment_name(config.pipeline_id)
        body = client.V1ConfigMap(
            metadata=client.V1ObjectMeta(
                name=name,
                labels={"app": "producer", "pipeline-id": config.pipeline_id,
                        "managed-by": "task-manager"},
            ),
            data={"telegraf.conf": render_telegraf_config(
                config, kafka_brokers=self._s.kafka_bootstrap_servers
            )},
        )
        core = self._core()
        try:
            core.create_namespaced_config_map(self._namespace, body)
        except ApiException as exc:
            if exc.status != 409:
                raise
            core.replace_namespaced_config_map(name, self._namespace, body)

    def _apply_deployment(self, config: PipelineConfig, replicas: int) -> None:
        manifest = render_producer_deployment(
            config, replicas=replicas, producer_image=self._s.producer_image,
            image_pull_policy=getattr(self._s, "image_pull_policy", "IfNotPresent"),
        )
        apps = self._apps()
        try:
            apps.create_namespaced_deployment(self._namespace, manifest)
        except ApiException as exc:
            if exc.status != 409:
                raise
            apps.patch_namespaced_deployment(
                manifest["metadata"]["name"], self._namespace, manifest
            )

    def _scale(self, name: str, replicas: int) -> None:
        try:
            self._apps().patch_namespaced_deployment_scale(
                name, self._namespace, {"spec": {"replicas": replicas}}
            )
        except ApiException as exc:
            if exc.status == 404:
                logger.info("Deployment %s already gone", name)
                return
            raise

    def _restart_rollout(self, name: str) -> None:
        from datetime import datetime, timezone

        self._apps().patch_namespaced_deployment(
            name, self._namespace,
            {"spec": {"template": {"metadata": {"annotations": {
                "kubectl.kubernetes.io/restartedAt": datetime.now(timezone.utc).isoformat()
            }}}}},
        )

    def _pods_for(self, name: str) -> list[Any]:
        pipeline_id = name.removeprefix("producer-")
        return self._core().list_namespaced_pod(
            self._namespace, label_selector=f"pipeline-id={pipeline_id}"
        ).items

    def _pod_logs(self, name: str, tail: int) -> str:
        try:
            pods = self._pods_for(name)
            if not pods:
                return f"[no pods for {name}]"
            return self._core().read_namespaced_pod_log(
                pods[0].metadata.name, self._namespace, tail_lines=tail
            )
        except ApiException as exc:
            return f"[error fetching logs: {exc.status} {exc.reason}]"

    def _ready(self, name: str) -> bool:
        try:
            deployment = self._apps().read_namespaced_deployment(name, self._namespace)
        except ApiException as exc:
            if exc.status == 404:
                return False
            raise
        return bool(deployment.status.ready_replicas or 0)
