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

from task_manager.app.airflow_client import AirflowClient
from task_manager.app.config import settings
from task_manager.app.folder_watcher import folder_watcher_loop
from common.app_common.models import (
    DesiredState,
    PipelineConfig,
    PipelineCreateRequest,
    PipelineStatus,
    PipelineType,
)
from common.app_common.redis_repo import PipelineRedisRepository
from common.app_common.runtime.docker_runtime import DockerRuntimeAdapter, RedisOnlyAdapter
from common.app_common.runtime.base import RuntimeAdapter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("task-manager")

_redis: Redis | None = None
_bg_tasks: list[asyncio.Task] = []  # type: ignore[type-arg]


def _get_runtime() -> RuntimeAdapter:
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
        batch_size=request.batch_size,
        flush_interval_seconds=request.flush_interval_seconds,
        source_options=request.source_options,
    )
    try:
        state = await repo.create_pipeline(config)
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc
    return {"config": config.model_dump(), "state": state.model_dump()}


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
        return {"pipeline_id": pipeline_id, "pipeline_type": "normal",
                "airflow": await AirflowClient().get_dag(config.airflow_dag_id or pipeline_id)}
    state = await repo.get_state(pipeline_id)
    return {"config": config.model_dump(), "state": state.model_dump() if state else None}


@app.post("/pipelines/{pipeline_id}/start", tags=["pipelines"])
async def start_pipeline(
    pipeline_id: str,
    repo: PipelineRedisRepository = Depends(_repo),
) -> dict[str, Any]:
    """Set desired_state=running. Controller picks it up and starts containers."""
    config = await repo.get_config(pipeline_id)
    if config is None:
        raise HTTPException(404, "Pipeline not found")
    if config.pipeline_type == PipelineType.NORMAL:
        return {"pipeline_id": pipeline_id, "pipeline_type": "normal",
                "airflow": await AirflowClient().trigger_dag(config.airflow_dag_id or pipeline_id)}
    state = await repo.get_state(pipeline_id)
    if state and state.desired_state == DesiredState.RUNNING:
        return {"message": "Already running", "state": state.model_dump()}
    state = await repo.update_state(pipeline_id, desired_state=DesiredState.RUNNING, last_error="")
    await repo.publish_state_event(state, "start")
    return {"message": "Start signal sent — controller launching containers shortly", "state": state.model_dump()}


@app.post("/pipelines/{pipeline_id}/stop", tags=["pipelines"])
async def stop_pipeline(
    pipeline_id: str,
    repo: PipelineRedisRepository = Depends(_repo),
) -> dict[str, Any]:
    """Set desired_state=stopped. Controller picks it up and stops containers."""
    config = await repo.get_config(pipeline_id)
    if config is None:
        raise HTTPException(404, "Pipeline not found")
    if config.pipeline_type == PipelineType.NORMAL:
        return {"pipeline_id": pipeline_id, "pipeline_type": "normal",
                "airflow": await AirflowClient().pause_dag(config.airflow_dag_id or pipeline_id, paused=True)}
    state = await repo.get_state(pipeline_id)
    if state and state.desired_state == DesiredState.STOPPED:
        return {"message": "Already stopped", "state": state.model_dump()}
    state = await repo.update_state(pipeline_id, desired_state=DesiredState.STOPPED)
    await repo.publish_state_event(state, "stop")
    return {"message": "Stop signal sent — controller stopping containers shortly", "state": state.model_dump()}


@app.post("/pipelines/{pipeline_id}/restart", tags=["pipelines"])
async def restart_pipeline(
    pipeline_id: str,
    repo: PipelineRedisRepository = Depends(_repo),
) -> dict[str, Any]:
    config = await repo.get_config(pipeline_id)
    if config is None:
        raise HTTPException(404, "Pipeline not found")
    state = await repo.get_state(pipeline_id)
    if state is None:
        raise HTTPException(404, "Pipeline state not found")
    if state.desired_state == DesiredState.RUNNING:
        state = await repo.update_state(pipeline_id, desired_state=DesiredState.STOPPED)
        await repo.publish_state_event(state, "stop")
        await asyncio.sleep(3)
    state = await repo.update_state(pipeline_id, desired_state=DesiredState.RUNNING, last_error="")
    await repo.publish_state_event(state, "start")
    return {"message": "Restart signal sent", "state": state.model_dump()}


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
