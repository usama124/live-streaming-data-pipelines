from __future__ import annotations

"""
Desired-State Controller — singleton service, replicas=1.

Acquires a Redis leader lock on startup. If two instances overlap during a
rolling deploy, only the lock holder runs the main loop. The other instance
waits and takes over if the lock expires (holder died).

Responsibilities:
  - Subscribe to pipeline_state_events pub/sub → react instantly
  - Poll all pipelines every controller_interval_s → reconcile drift
  - Start/stop Docker containers when desired_state changes
"""

import asyncio
import json
import logging
import signal

from redis.asyncio import Redis

from controller.app.config import settings
from common.app_common.leader_lock import LeaderLock
from common.app_common.models import DesiredState, PipelineStatus
from common.app_common.redis_keys import PIPELINE_STATE_EVENTS_CHANNEL
from common.app_common.redis_repo import PipelineRedisRepository
from common.app_common.runtime.docker_runtime import DockerRuntimeAdapter, RedisOnlyAdapter
from common.app_common.runtime.base import RuntimeAdapter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("controller")

_IDLE           = {PipelineStatus.CREATED, PipelineStatus.STOPPED, PipelineStatus.FAILED}
_ALREADY_STOPPED = {PipelineStatus.STOPPED, PipelineStatus.STOPPING,
                    PipelineStatus.CREATED, PipelineStatus.FAILED}


def _get_runtime() -> RuntimeAdapter:
    if settings.runtime_mode == "docker":
        return DockerRuntimeAdapter(settings)
    return RedisOnlyAdapter()


async def run() -> None:
    stop = asyncio.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT,  lambda *_: stop.set())

    redis = Redis.from_url(settings.redis_url, decode_responses=False)
    await redis.ping()
    logger.info("Controller connected to Redis")

    lock    = LeaderLock(redis, "controller")
    repo    = PipelineRedisRepository(redis)
    runtime = _get_runtime()

    logger.info("Waiting to acquire leader lock...")
    await lock.acquire_with_retry()
    logger.info("Controller is now leader — starting main loop")

    locks: dict[str, asyncio.Lock] = {}

    pubsub_task    = asyncio.create_task(_pubsub_listener(redis, repo, runtime, locks), name="controller-pubsub")
    reconcile_task = asyncio.create_task(_reconcile_loop(repo, runtime, locks), name="controller-reconcile")
    stop_task      = asyncio.create_task(stop.wait(), name="stop-signal")

    try:
        done, pending = await asyncio.wait(
            [pubsub_task, reconcile_task, stop_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for t in done:
            if not t.cancelled() and t.exception():
                raise t.exception()
    finally:
        await lock.release()
        await redis.aclose()
        logger.info("Controller stopped")


async def _pubsub_listener(
    redis: Redis,
    repo: PipelineRedisRepository,
    runtime: RuntimeAdapter,
    locks: dict[str, asyncio.Lock],
) -> None:
    pubsub = redis.pubsub()
    await pubsub.subscribe(PIPELINE_STATE_EVENTS_CHANNEL)
    logger.info("Controller subscribed to '%s'", PIPELINE_STATE_EVENTS_CHANNEL)
    async for msg in pubsub.listen():
        if msg["type"] != "message":
            continue
        try:
            raw     = msg["data"]
            payload = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
            pid     = payload.get("pipeline_id")
            if not pid:
                continue
            lock = _lock_for(locks, pid)
            if lock.locked():
                continue
            asyncio.create_task(_act(pid, repo, runtime, lock), name=f"act-{pid}")
        except Exception:
            logger.exception("Controller: error in pub/sub handler")


async def _reconcile_loop(
    repo: PipelineRedisRepository,
    runtime: RuntimeAdapter,
    locks: dict[str, asyncio.Lock],
) -> None:
    while True:
        await asyncio.sleep(settings.controller_interval_s)
        try:
            for pid in await repo.list_pipeline_ids():
                state  = await repo.get_state(pid)
                config = await repo.get_config(pid)
                if not state or not config or config.pipeline_type.value != "live":
                    continue
                lock = _lock_for(locks, pid)
                if lock.locked():
                    continue
                needs_start = state.desired_state == DesiredState.RUNNING and state.status in _IDLE
                needs_stop  = state.desired_state == DesiredState.STOPPED and state.status not in _ALREADY_STOPPED
                if needs_start or needs_stop:
                    asyncio.create_task(_act(pid, repo, runtime, lock), name=f"reconcile-{pid}")
        except Exception:
            logger.exception("Controller: error in reconcile loop")


async def _act(
    pid: str,
    repo: PipelineRedisRepository,
    runtime: RuntimeAdapter,
    lock: asyncio.Lock,
) -> None:
    async with lock:
        state  = await repo.get_state(pid)
        config = await repo.get_config(pid)
        if not state or not config:
            return

        if state.desired_state == DesiredState.RUNNING:
            if state.status in {PipelineStatus.RUNNING, PipelineStatus.STARTING}:
                return
            logger.info("Controller: STARTING %s", pid)
            await repo.update_state(pid, status=PipelineStatus.STARTING, last_error="")
            try:
                pname, cname = await runtime.start_live_pipeline(config, state)
                await repo.update_state(pid, status=PipelineStatus.STARTING,
                                        producer_container=pname, consumer_container=cname)
                logger.info("Controller: %s launched producer=%s consumer=%s", pid, pname, cname)
            except Exception as exc:
                logger.exception("Controller: failed to start %s", pid)
                await repo.update_state(pid, desired_state=DesiredState.STOPPED,
                                        status=PipelineStatus.FAILED, last_error=str(exc))

        elif state.desired_state == DesiredState.STOPPED:
            if state.status in _ALREADY_STOPPED:
                return
            logger.info("Controller: STOPPING %s", pid)
            await repo.update_state(pid, status=PipelineStatus.STOPPING)
            try:
                await runtime.stop_live_pipeline(config, state)
                await repo.update_state(pid, status=PipelineStatus.STOPPED,
                                        producer_container=None, consumer_container=None)
                logger.info("Controller: %s stopped", pid)
            except Exception as exc:
                logger.exception("Controller: failed to stop %s", pid)
                await repo.update_state(pid, status=PipelineStatus.FAILED, last_error=str(exc))


def _lock_for(locks: dict[str, asyncio.Lock], pid: str) -> asyncio.Lock:
    if pid not in locks:
        locks[pid] = asyncio.Lock()
    return locks[pid]


if __name__ == "__main__":
    asyncio.run(run())
