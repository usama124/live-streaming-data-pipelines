from __future__ import annotations

"""
Quix Streams consumer — replaces KafkaBatchConsumer.

Architecture
------------
Quix Streams' Application.run() is a blocking synchronous call that owns
its own internal thread-based poll loop.  Our service needs to:

  1. Run Application.run() (blocking, sync)
  2. Simultaneously poll Redis every few seconds to check desired_state
  3. Stop Application cleanly when desired_state=stopped is detected
  4. Write heartbeats to Redis while running
  5. Mark pipeline status in Redis on start / stop / failure

Solution: run Application.run() inside asyncio.to_thread() so it doesn't
block the asyncio event loop.  A separate asyncio task runs the Redis
control loop concurrently.  When the control loop decides to stop, it
calls app.stop() which signals Quix's internal loop to exit cleanly.

The ClickHouseSink.write() is called from Quix's sync thread and bridges
back to the asyncio loop via asyncio.run_coroutine_threadsafe().

Commit / flush timing
---------------------
Quix commits a checkpoint (and therefore calls sink.write()) based on:
  commit_interval  — every N seconds   → maps to settings.flush_interval_seconds
  commit_every     — every N messages   → maps to settings.batch_size

Both are used together: whichever fires first triggers a flush.
"""

import asyncio
import logging
import threading

from quixstreams import Application

from clickhouse_manager import ClickHouseConnectionManager
from clickhouse_sink import ClickHouseSink
from config import settings
from common.app_common.models import DesiredState, PipelineState, PipelineStatus, utc_now_iso
from common.app_common.redis_keys import pipeline_state_key

from redis.asyncio import Redis

logger = logging.getLogger("consumer-service")

# How often (seconds) to poll Redis for desired_state / write heartbeat.
_REDIS_POLL_INTERVAL_S = 3


class QuixConsumer:
    """
    Wraps a Quix Streams Application with Redis-driven lifecycle control.

    Usage:
        consumer = QuixConsumer(redis_client, ch_manager)
        await consumer.run(stop_event)   # blocks until stopped
    """

    def __init__(
        self,
        redis_client: Redis,
        ch_manager: ClickHouseConnectionManager,
    ) -> None:
        self._redis = redis_client
        self._ch_manager = ch_manager

        # Captured in run() — needed by ClickHouseSink to bridge sync→async.
        self._loop: asyncio.AbstractEventLoop | None = None

        # Quix Application instance — created fresh in run() so it can be
        # garbage-collected cleanly if the service restarts.
        self._app: Application | None = None

        # Set by the Redis control loop when it decides to stop.
        self._stop_reason: str = ""

    # ── Public entry point ────────────────────────────────────────────────

    async def run(self, stop_event: asyncio.Event) -> None:
        """
        Start the Quix consumer and run until:
          - stop_event is set  (SIGTERM / SIGINT from main.py)
          - desired_state=stopped is detected in Redis
          - an unrecoverable error occurs inside Quix
        """
        self._loop = asyncio.get_running_loop()

        # ── Build Quix Application ────────────────────────────────────────
        self._app = Application(
            broker_address=settings.kafka_bootstrap_servers,
            consumer_group=settings.kafka_group_id,
            auto_offset_reset="earliest",
            commit_interval=float(settings.flush_interval_seconds),
            commit_every=settings.batch_size,
            processing_guarantee="at-least-once",
            # Suppress Quix's own signal handlers — we manage lifecycle.
            loglevel=None,
        )

        # ── Build ClickHouseSink ──────────────────────────────────────────
        sink = ClickHouseSink(
            manager=self._ch_manager,
            database=settings.clickhouse_database,
            table=settings.clickhouse_table,
            loop=self._loop,
        )

        # ── Wire up the StreamingDataFrame ────────────────────────────────
        topic = self._app.topic(settings.kafka_topic)
        sdf   = self._app.dataframe(topic)
        sdf.sink(sink)

        # ── Mark pipeline as running in Redis ─────────────────────────────
        await self._mark_running()

        logger.info(
            "Quix consumer started — pipeline=%s topic=%s group=%s "
            "flush_interval=%ds batch_size=%d",
            settings.pipeline_id,
            settings.kafka_topic,
            settings.kafka_group_id,
            settings.flush_interval_seconds,
            settings.batch_size,
        )

        # ── Run Quix in a background thread ──────────────────────────────
        # app.run() blocks synchronously inside Quix's internal poll loop.
        # We move it off the asyncio event loop so our async Redis control
        # loop can run concurrently.
        quix_thread_exc: list[Exception] = []

        def _run_quix() -> None:
            try:
                # Quix calls signal.signal() inside run() which only works
                # from the main thread. Since main.py already handles SIGTERM/
                # SIGINT, we patch the method to a no-op before entering run().
                self._app._setup_signal_handlers = lambda: None  # type: ignore[method-assign]
                self._app.run()
            except Exception as exc:
                quix_thread_exc.append(exc)

        quix_future = asyncio.ensure_future(
            asyncio.to_thread(_run_quix)
        )

        # ── Redis control loop ────────────────────────────────────────────
        # Runs concurrently alongside Quix. Stops Quix when desired_state
        # changes to stopped or stop_event fires.
        control_task = asyncio.create_task(
            self._redis_control_loop(stop_event, quix_future),
            name="redis-control-loop",
        )

        try:
            # Wait for whichever finishes first: Quix or control loop.
            done, pending = await asyncio.wait(
                [quix_future, control_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

            # Surface any exception from the Quix thread.
            if quix_thread_exc:
                raise quix_thread_exc[0]

            # Surface any exception from the control loop.
            for task in done:
                exc = task.exception() if not task.cancelled() else None
                if exc:
                    raise exc

        except Exception as exc:
            logger.exception("Consumer error: %s", exc)
            await self._mark_failed(str(exc))
            raise
        finally:
            await self._mark_stopped()
            logger.info(
                "Quix consumer stopped — pipeline=%s reason=%s",
                settings.pipeline_id,
                self._stop_reason or "external stop_event",
            )

    # ── Redis control loop ────────────────────────────────────────────────

    async def _redis_control_loop(
        self,
        stop_event: asyncio.Event,
        quix_future: asyncio.Future,
    ) -> None:
        """
        Polls Redis every _REDIS_POLL_INTERVAL_S seconds.
        Writes heartbeat on every tick.
        Calls app.stop() when desired_state != running or stop_event fires.
        """
        while not stop_event.is_set():
            await asyncio.sleep(_REDIS_POLL_INTERVAL_S)

            # If Quix already exited on its own, stop polling.
            if quix_future.done():
                return

            # Write heartbeat.
            await self._heartbeat()

            # Check desired_state.
            if not await self._should_run():
                self._stop_reason = "desired_state=stopped detected in Redis"
                logger.info(
                    "Consumer: Redis desired_state=stopped — stopping Quix for pipeline=%s",
                    settings.pipeline_id,
                )
                if self._app is not None:
                    self._app.stop()
                return

        # stop_event was set (SIGTERM/SIGINT from outside).
        self._stop_reason = "stop_event set"
        if self._app is not None:
            self._app.stop()

    # ── Redis state helpers ───────────────────────────────────────────────

    async def _get_state(self) -> PipelineState | None:
        raw = await self._redis.get(pipeline_state_key(settings.pipeline_id))
        if raw is None:
            return None
        return PipelineState.model_validate_json(raw)

    async def _should_run(self) -> bool:
        state = await self._get_state()
        return state is not None and state.desired_state == DesiredState.RUNNING

    async def _mark_running(self) -> None:
        state = await self._get_state()
        if state is None:
            return
        state.status = PipelineStatus.RUNNING
        state.last_heartbeat_at = utc_now_iso()
        state.updated_at = utc_now_iso()
        await self._redis.set(
            pipeline_state_key(settings.pipeline_id), state.model_dump_json()
        )

    async def _heartbeat(self) -> None:
        state = await self._get_state()
        if state is None:
            return
        state.last_heartbeat_at = utc_now_iso()
        state.updated_at = utc_now_iso()
        await self._redis.set(
            pipeline_state_key(settings.pipeline_id), state.model_dump_json()
        )

    async def _mark_stopped(self) -> None:
        state = await self._get_state()
        if state is None:
            return
        state.status = PipelineStatus.STOPPED
        state.last_heartbeat_at = utc_now_iso()
        state.updated_at = utc_now_iso()
        await self._redis.set(
            pipeline_state_key(settings.pipeline_id), state.model_dump_json()
        )

    async def _mark_failed(self, error: str) -> None:
        state = await self._get_state()
        if state is None:
            return
        state.status = PipelineStatus.FAILED
        state.last_error = error
        state.updated_at = utc_now_iso()
        await self._redis.set(
            pipeline_state_key(settings.pipeline_id), state.model_dump_json()
        )