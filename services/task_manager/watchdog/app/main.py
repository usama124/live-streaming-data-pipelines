from __future__ import annotations

"""
Watchdog — singleton service, replicas=1.

Acquires a Redis leader lock on startup. Only the lock holder runs the
health-check loop. If the holder crashes, the standby instance takes over
within LOCK_TTL_S seconds.

Responsibilities:
  - Every watchdog_interval_s, scan all desired_state=running pipelines
  - If heartbeat age > watchdog_heartbeat_timeout_s → restart containers
  - If a container is found stopped → restart containers

Does NOT react to desired_state changes (that is the Controller's job).
"""

import asyncio
import logging
import signal
from datetime import datetime, timezone

from redis.asyncio import Redis

from watchdog.app.config import settings
from common.app_common.leader_lock import LeaderLock
from common.app_common.models import DesiredState, PipelineStatus
from common.app_common.redis_repo import PipelineRedisRepository
from common.app_common.runtime.docker_runtime import DockerRuntimeAdapter, RedisOnlyAdapter
from common.app_common.runtime.base import RuntimeAdapter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("watchdog")


def _get_runtime() -> RuntimeAdapter:
    if settings.runtime_mode == "docker":
        return DockerRuntimeAdapter(settings)
    return RedisOnlyAdapter()


async def run() -> None:
    stop = asyncio.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT,  lambda *_: stop.set())

    redis   = Redis.from_url(settings.redis_url, decode_responses=False)
    await redis.ping()
    logger.info("Watchdog connected to Redis")

    lock    = LeaderLock(redis, "watchdog")
    repo    = PipelineRedisRepository(redis)
    runtime = _get_runtime()

    logger.info("Waiting to acquire leader lock...")
    await lock.acquire_with_retry()
    logger.info("Watchdog is now leader — starting health-check loop")

    try:
        await _watchdog_loop(repo, runtime, stop)
    finally:
        await lock.release()
        await redis.aclose()
        logger.info("Watchdog stopped")


async def _watchdog_loop(
    repo: PipelineRedisRepository,
    runtime: RuntimeAdapter,
    stop: asyncio.Event,
) -> None:
    hb_timeout = settings.watchdog_heartbeat_timeout_s
    interval   = settings.watchdog_interval_s

    logger.info("Watchdog loop — interval=%ds hb_timeout=%ds", interval, hb_timeout)

    while not stop.is_set():
        await asyncio.sleep(interval)
        try:
            for pid in await repo.list_pipeline_ids():
                try:
                    await _check(pid, repo, runtime, hb_timeout)
                except Exception:
                    logger.exception("Watchdog: error checking pipeline %s", pid)
        except Exception:
            logger.exception("Watchdog: error listing pipeline IDs")


async def _check(
    pid: str,
    repo: PipelineRedisRepository,
    runtime: RuntimeAdapter,
    hb_timeout: int,
) -> None:
    state  = await repo.get_state(pid)
    config = await repo.get_config(pid)
    if not state or not config:
        return
    if state.desired_state != DesiredState.RUNNING:
        return
    if state.status == PipelineStatus.STARTING:
        return  # controller is already acting

    now = datetime.now(timezone.utc)

    if state.last_heartbeat_at:
        try:
            age_s = (now - datetime.fromisoformat(state.last_heartbeat_at)).total_seconds()
        except ValueError:
            age_s = hb_timeout + 1
        if age_s > hb_timeout:
            logger.warning("Watchdog: %s missed heartbeat (%.0fs) — restarting", pid, age_s)
            await _restart(pid, repo, runtime, config, state)
            return

    for cname, role in [(state.producer_container, "producer"), (state.consumer_container, "consumer")]:
        if cname and not await runtime.is_running(cname):
            logger.warning("Watchdog: %s %s container '%s' stopped — restarting", pid, role, cname)
            await _restart(pid, repo, runtime, config, state)
            return


async def _restart(pid, repo, runtime, config, state) -> None:
    try:
        await repo.update_state(pid, status=PipelineStatus.STARTING)
        pname, cname = await runtime.restart_live_pipeline(config, state)
        await repo.update_state(pid, status=PipelineStatus.STARTING,
                                producer_container=pname, consumer_container=cname, last_error="")
        logger.info("Watchdog: restarted %s — producer=%s consumer=%s", pid, pname, cname)
    except Exception as exc:
        logger.exception("Watchdog: failed to restart %s", pid)
        await repo.update_state(pid, status=PipelineStatus.FAILED,
                                last_error=f"watchdog restart failed: {exc}")


if __name__ == "__main__":
    asyncio.run(run())
