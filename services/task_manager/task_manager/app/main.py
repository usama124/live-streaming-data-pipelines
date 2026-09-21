from __future__ import annotations

"""
Task Manager API — 2 replicas, stateless.

Responsibilities:
  - REST API for pipeline CRUD and lifecycle signals
  - Folder watcher — registers new pipelines from definition files
    (idempotent: if both replicas pick up the same file, the second
     create_pipeline() raises ValueError which is caught and ignored)

Does NOT:
  - Start/stop containers (Controller does that)
  - Monitor heartbeats (Watchdog does that)
  - Hold distributed locks (no shared mutable state between replicas)

Scaling:
  replicas: 2  — both handle API requests, both run the folder watcher
  The folder watcher is safe to run on both replicas because:
    - create_pipeline() is idempotent via a ValueError guard
    - Files are moved to processed/ atomically (OS rename is atomic)
    - Two replicas racing on the same file: one succeeds, one gets
      FileNotFoundError on the move (caught and logged, not fatal)
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, status, Response
from redis.asyncio import Redis

from task_manager.app.config import settings
from task_manager.app.folder_watcher import folder_watcher_loop
from task_manager.app.provisioning import provision
from common.app_common.models import (
    DesiredState,
    PipelineConfig,
    PipelineCreateRequest,
    PipelineStatus,
    PipelineType,
)
from common.app_common.redis_repo import PipelineRedisRepository
from common.app_common.runtime.docker_runtime import DockerRuntimeAdapter, RedisOnlyAdapter
from common.app_common.runtime.kubernetes_runtime import KubernetesRuntimeAdapter
from common.app_common.runtime.base import RuntimeAdapter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("task-manager")

_redis: Redis | None = None
_bg_tasks: list[asyncio.Task] = []  # type: ignore[type-arg]


def _get_runtime() -> RuntimeAdapter:
    if settings.runtime_mode == "kubernetes":
        return KubernetesRuntimeAdapter(
            settings, namespace=settings.k8s_namespace, kube_context=settings.kube_context
        )
    if settings.runtime_mode == "docker":
        return DockerRuntimeAdapter(settings)
    return RedisOnlyAdapter()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _redis, _bg_tasks

    _redis = Redis.from_url(settings.redis_url, decode_responses=False)
    await _redis.ping()
    logger.info("Task Manager connected to Redis")

    if settings.watcher_enabled:
        repo = PipelineRedisRepository(_redis)
        _bg_tasks.append(asyncio.create_task(
            folder_watcher_loop(repo), name="folder-watcher"
        ))
        logger.info("Folder watcher started — dir=%s", settings.pipeline_definitions_dir)

    logger.info("Task Manager API ready (replicas=2 capable)")
    yield

    for t in _bg_tasks:
        t.cancel()
    await asyncio.gather(*_bg_tasks, return_exceptions=True)
    _bg_tasks.clear()
    if _redis:
        await _redis.aclose()
    logger.info("Task Manager stopped")


app = FastAPI(
    title="Data Platform — Task Manager API",
    version="4.0.0",
    description="Stateless API. Run 2 replicas. Controller and Watchdog run as separate singleton services.",
    lifespan=lifespan,
)


# Batch/normal pipelines run as Airflow DAGs outside this repo. The API still
# accepts them so definitions round-trip, but lifecycle calls belong to Airflow.
_NORMAL_NOT_HERE = "normal pipelines are managed by Airflow, not by this service"


def _repo() -> PipelineRedisRepository:
    if _redis is None:
        raise RuntimeError("Redis not initialised")
    return PipelineRedisRepository(_redis)


@app.get("/health", tags=["ops"])
async def health() -> dict[str, Any]:
    if _redis:
        await _redis.ping()
    return {"status": "ok", "service": "task-manager", "runtime_mode": settings.runtime_mode}


@app.post("/pipelines", status_code=status.HTTP_201_CREATED, tags=["pipelines"])
async def create_pipeline(
    request: PipelineCreateRequest,
    repo: PipelineRedisRepository = Depends(_repo),
) -> dict[str, Any]:
    if request.pipeline_type == PipelineType.NORMAL and not request.airflow_dag_id:
        raise HTTPException(400, "airflow_dag_id required for normal pipelines")
    topic  = request.topic or f"pipeline.{request.pipeline_id}.events"
    config = PipelineConfig(
        pipeline_id=request.pipeline_id,
        pipeline_type=request.pipeline_type,
        airflow_dag_id=request.airflow_dag_id,
        source_type=request.source_type,
        topic=topic,
        user_id=request.user_id or "0",
        collection_number=request.collection_number or 1,
        table_name=request.table_name or "events",
        table_schema=request.table_schema,
        batch_size=request.batch_size,
        flush_interval_seconds=request.flush_interval_seconds,
        source_options=request.source_options,
    )
    try:
        state = await repo.create_pipeline(config)
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc

    # Topic and table before the producer can start, so the first event has
    # somewhere to land instead of racing table creation.
    if config.pipeline_type == PipelineType.LIVE:
        try:
            await provision(config)
        except Exception as exc:
            logger.exception("provisioning failed for %s", config.pipeline_id)
            await repo.update_state(config.pipeline_id, status=PipelineStatus.FAILED,
                                    last_error=f"provisioning failed: {exc}")
            raise HTTPException(502, f"provisioning failed: {exc}") from exc

    return {"config": config.model_dump(), "state": state.model_dump(),
            "table": config.ch_unique_identifier}


@app.get("/pipelines", tags=["pipelines"])
async def list_pipelines(repo: PipelineRedisRepository = Depends(_repo)) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for pid in await repo.list_pipeline_ids():
        config = await repo.get_config(pid)
        state  = await repo.get_state(pid)
        if config and state:
            result.append({"config": config.model_dump(), "state": state.model_dump()})
    return result


@app.get("/pipelines/{pipeline_id}/status", tags=["pipelines"])
async def get_status(
    pipeline_id: str,
    repo: PipelineRedisRepository = Depends(_repo),
) -> dict[str, Any]:
    config = await repo.get_config(pipeline_id)
    if config is None:
        raise HTTPException(404, "Pipeline not found")
    if config.pipeline_type == PipelineType.NORMAL:
        raise HTTPException(400, _NORMAL_NOT_HERE)
    state = await repo.get_state(pipeline_id)
    return {"config": config.model_dump(), "state": state.model_dump() if state else None}


@app.post("/pipelines/{pipeline_id}/start", tags=["pipelines"])
async def start_pipeline(
    pipeline_id: str,
    repo: PipelineRedisRepository = Depends(_repo),
) -> dict[str, Any]:
    """Start the pipeline and report the outcome now, not on a later poll."""
    config = await repo.get_config(pipeline_id)
    if config is None:
        raise HTTPException(404, "Pipeline not found")
    if config.pipeline_type == PipelineType.NORMAL:
        raise HTTPException(400, _NORMAL_NOT_HERE)
    state = await repo.get_state(pipeline_id)
    if state and state.desired_state == DesiredState.RUNNING:
        return {"message": "Already running", "state": state.model_dump()}

    state = await repo.update_state(pipeline_id, desired_state=DesiredState.RUNNING,
                                    status=PipelineStatus.STARTING, last_error="")
    try:
        producer, _ = await _get_runtime().start_live_pipeline(config, state)
    except Exception as exc:
        # The point of calling the runtime here: the caller finds out now.
        logger.exception("start failed for %s", pipeline_id)
        state = await repo.update_state(pipeline_id, desired_state=DesiredState.STOPPED,
                                        status=PipelineStatus.FAILED, last_error=str(exc))
        raise HTTPException(502, f"failed to start pipeline: {exc}") from exc

    state = await repo.update_state(pipeline_id, status=PipelineStatus.RUNNING,
                                    producer_container=producer, last_error="")
    await repo.publish_state_event(state, "start")
    return {"message": "Started", "state": state.model_dump()}


@app.post("/pipelines/{pipeline_id}/stop", tags=["pipelines"])
async def stop_pipeline(
    pipeline_id: str,
    repo: PipelineRedisRepository = Depends(_repo),
) -> dict[str, Any]:
    """Stop the pipeline and report the outcome now, not on a later poll."""
    config = await repo.get_config(pipeline_id)
    if config is None:
        raise HTTPException(404, "Pipeline not found")
    if config.pipeline_type == PipelineType.NORMAL:
        raise HTTPException(400, _NORMAL_NOT_HERE)
    state = await repo.get_state(pipeline_id)
    if state and state.desired_state == DesiredState.STOPPED:
        return {"message": "Already stopped", "state": state.model_dump()}

    state = await repo.update_state(pipeline_id, desired_state=DesiredState.STOPPED,
                                    status=PipelineStatus.STOPPING)
    try:
        await _get_runtime().stop_live_pipeline(config, state)
    except Exception as exc:
        logger.exception("stop failed for %s", pipeline_id)
        state = await repo.update_state(pipeline_id, status=PipelineStatus.FAILED,
                                        last_error=str(exc))
        raise HTTPException(502, f"failed to stop pipeline: {exc}") from exc

    state = await repo.update_state(pipeline_id, status=PipelineStatus.STOPPED,
                                    producer_container=None, last_error="")
    await repo.publish_state_event(state, "stop")
    return {"message": "Stopped", "state": state.model_dump()}


@app.post("/pipelines/{pipeline_id}/restart", tags=["pipelines"])
async def restart_pipeline(
    pipeline_id: str,
    repo: PipelineRedisRepository = Depends(_repo),
) -> dict[str, Any]:
    config = await repo.get_config(pipeline_id)
    if config is None:
        raise HTTPException(404, "Pipeline not found")
    if config.pipeline_type == PipelineType.NORMAL:
        raise HTTPException(400, _NORMAL_NOT_HERE)
    state = await repo.get_state(pipeline_id)
    if state is None:
        raise HTTPException(404, "Pipeline state not found")

    try:
        producer, _ = await _get_runtime().restart_live_pipeline(config, state)
    except Exception as exc:
        logger.exception("restart failed for %s", pipeline_id)
        state = await repo.update_state(pipeline_id, status=PipelineStatus.FAILED,
                                        last_error=str(exc))
        raise HTTPException(502, f"failed to restart pipeline: {exc}") from exc

    state = await repo.update_state(pipeline_id, desired_state=DesiredState.RUNNING,
                                    status=PipelineStatus.RUNNING,
                                    producer_container=producer, last_error="")
    await repo.publish_state_event(state, "start")
    return {"message": "Restarted", "state": state.model_dump()}


@app.get("/pipelines/{pipeline_id}/logs", tags=["pipelines"])
async def get_logs(
    pipeline_id: str,
    role: str = Query(default="consumer", pattern="^(producer|consumer)$"),
    tail: int = Query(default=200, ge=1, le=5000),
    repo: PipelineRedisRepository = Depends(_repo),
) -> dict[str, Any]:
    state = await repo.get_state(pipeline_id)
    if state is None:
        raise HTTPException(404, "Pipeline not found")
    container_name = state.consumer_container if role == "consumer" else state.producer_container
    if not container_name:
        raise HTTPException(404, f"No {role} container registered")
    logs = await _get_runtime().get_logs(container_name, tail=tail)
    return {"pipeline_id": pipeline_id, "role": role, "container": container_name, "logs": logs}


@app.delete("/pipelines/{pipeline_id}", status_code=status.HTTP_204_NO_CONTENT, tags=["pipelines"])
async def delete_pipeline(
    pipeline_id: str,
    repo: PipelineRedisRepository = Depends(_repo),
):
    config = await repo.get_config(pipeline_id)
    if config is None:
        raise HTTPException(404, "Pipeline not found")
    state = await repo.get_state(pipeline_id)
    if state and state.desired_state == DesiredState.RUNNING:
        updated = await repo.update_state(pipeline_id, desired_state=DesiredState.STOPPED)
        await repo.publish_state_event(updated, "stop")
        await asyncio.sleep(3)
    await repo.delete_pipeline(pipeline_id)
    return Response(status_code=204)
