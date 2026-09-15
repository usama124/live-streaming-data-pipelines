from __future__ import annotations

import asyncio
import logging
import signal

from redis.asyncio import Redis

from app.config import settings
from app.kafka_publisher import KafkaEventPublisher
from app.redis_control import ProducerControl
from app.sources.base import StreamingSource
from app.sources.mock_source import MockStreamingSource

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("producer-service")


def _build_source() -> StreamingSource:
    if settings.source_type == "mock":
        return MockStreamingSource(
            pipeline_id=settings.pipeline_id,
            min_delay_ms=settings.mock_min_delay_ms,
            max_delay_ms=settings.mock_max_delay_ms,
        )

    if settings.source_type == "opcua":
        # Deferred import — asyncua is an optional dep; mock-only deployments don't need it.
        from app.sources.opcua_source import OpcUaStreamingSource  # noqa: PLC0415

        opts = settings.opcua_source_options()
        if not opts["node_ids"]:
            raise ValueError(
                "OPCUA_NODE_IDS must be set when SOURCE_TYPE=opcua. "
                "Example: OPCUA_NODE_IDS=ns=2;i=2,ns=2;i=3"
            )
        return OpcUaStreamingSource(
            pipeline_id=settings.pipeline_id,
            source_options=opts,
        )

    raise ValueError(f"Unsupported SOURCE_TYPE: {settings.source_type!r}. Valid: mock, opcua")


async def heartbeat_loop(control: ProducerControl, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        await control.heartbeat()
        await asyncio.sleep(settings.heartbeat_interval_seconds)


async def run() -> None:
    stop_event = asyncio.Event()

    def _stop(*_: object) -> None:
        stop_event.set()

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    redis_client = Redis.from_url(settings.redis_url, decode_responses=False)
    control = ProducerControl(redis_client, settings.pipeline_id)

    source = _build_source()
    publisher = KafkaEventPublisher(settings.kafka_bootstrap_servers, settings.kafka_topic)

    await publisher.start()
    hb_task = asyncio.create_task(heartbeat_loop(control, stop_event))

    logger.info("Producer started for pipeline=%s topic=%s", settings.pipeline_id, settings.kafka_topic)
    await control.mark_running()

    try:
        stream_iter = source.stream().__aiter__()
        while True:
            if stop_event.is_set() or not await control.should_run():
                break
            try:
                event = await asyncio.wait_for(
                    stream_iter.__anext__(),
                    timeout=2.0
                )
            except asyncio.TimeoutError:
                continue
            except StopAsyncIteration:
                break
            await publisher.publish(key=settings.pipeline_id, event=event)
    except Exception as exc:
        logger.exception("Producer failed")
        await control.mark_failed(str(exc))
        raise
    finally:
        stop_event.set()
        hb_task.cancel()
        await publisher.stop()
        await control.mark_stopped()
        await redis_client.aclose()
        logger.info("Producer stopped for pipeline=%s", settings.pipeline_id)


if __name__ == "__main__":
    asyncio.run(run())
