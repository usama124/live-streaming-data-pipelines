from __future__ import annotations

"""
Redis-based leader election lock for singleton services.

Problem
-------
Controller and Watchdog run as replicas=1 Deployments but during a rolling
deploy there is a brief window where two instances overlap. Without a lock,
both would act on the same pipeline simultaneously — double-starting or
double-stopping containers.

Solution
--------
On startup, each process tries to acquire a Redis key with a short TTL
(the lock). Only the holder runs the main loop. The holder renews the TTL
in a background task every RENEW_INTERVAL_S seconds. If the holder crashes,
the TTL expires and a waiting replica acquires the lock within LOCK_TTL_S.

Usage
-----
    async with LeaderLock(redis_client, "controller") as lock:
        if lock.is_leader:
            await run_main_loop()

Or for services that should just wait and retry until they are leader:

    lock = LeaderLock(redis_client, "watchdog")
    await lock.acquire_with_retry()   # blocks until leader
    try:
        await run_main_loop()
    finally:
        await lock.release()
"""

import asyncio
import logging
import os
import socket

from redis.asyncio import Redis

from common.app_common.redis_keys import leader_lock_key

logger = logging.getLogger("leader-lock")

LOCK_TTL_S     = 15   # seconds before lock auto-expires if holder dies
RENEW_INTERVAL = 5    # seconds between TTL renewals
RETRY_INTERVAL = 3    # seconds between acquire attempts when not leader


def _identity() -> str:
    """Unique identity for this process instance."""
    return f"{socket.gethostname()}:{os.getpid()}"


class LeaderLock:
    """
    Async context manager and manual-acquire interface for a Redis leader lock.

    The lock key holds this instance's identity string. SET NX PX is atomic —
    only one process wins. The winner renews the TTL in a background task.
    """

    def __init__(self, redis: Redis, service_name: str) -> None:
        self._redis     = redis
        self._key       = leader_lock_key(service_name)
        self._identity  = _identity()
        self._renew_task: asyncio.Task | None = None
        self.is_leader  = False

    # ── Context-manager interface (non-blocking attempt) ──────────────────

    async def __aenter__(self) -> "LeaderLock":
        self.is_leader = await self._try_acquire()
        if self.is_leader:
            self._start_renew()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.release()

    # ── Blocking interface (waits until leader) ───────────────────────────

    async def acquire_with_retry(self) -> None:
        """Block until this instance becomes leader."""
        while True:
            if await self._try_acquire():
                self.is_leader = True
                self._start_renew()
                logger.info("Leader lock acquired for key '%s' by %s", self._key, self._identity)
                return
            logger.debug("Not leader yet for '%s' — retrying in %ds", self._key, RETRY_INTERVAL)
            await asyncio.sleep(RETRY_INTERVAL)

    async def release(self) -> None:
        """Release the lock if we still hold it."""
        if self._renew_task:
            self._renew_task.cancel()
            await asyncio.gather(self._renew_task, return_exceptions=True)
            self._renew_task = None

        if self.is_leader:
            current = await self._redis.get(self._key)
            current_str = current.decode() if isinstance(current, bytes) else current
            if current_str == self._identity:
                await self._redis.delete(self._key)
                logger.info("Leader lock released: '%s'", self._key)
            self.is_leader = False

    # ── Internals ─────────────────────────────────────────────────────────

    async def _try_acquire(self) -> bool:
        result = await self._redis.set(
            self._key,
            self._identity,
            nx=True,              # only set if not exists
            ex=LOCK_TTL_S,
        )
        return result is not None

    def _start_renew(self) -> None:
        self._renew_task = asyncio.create_task(
            self._renew_loop(), name=f"leader-renew-{self._key}"
        )

    async def _renew_loop(self) -> None:
        while True:
            await asyncio.sleep(RENEW_INTERVAL)
            current = await self._redis.get(self._key)
            current_str = current.decode() if isinstance(current, bytes) else current
            if current_str != self._identity:
                logger.warning(
                    "Leader lock '%s' lost — another instance took over", self._key
                )
                self.is_leader = False
                return
            await self._redis.expire(self._key, LOCK_TTL_S)
            logger.debug("Leader lock '%s' renewed", self._key)
