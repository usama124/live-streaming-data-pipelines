from __future__ import annotations

import asyncio
import logging
import signal

from redis.asyncio import Redis

from clickhouse_manager import ClickHouseConnectionManager
from config import settings
from quix_consumer import QuixConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("consumer-service")


async def run() -> None:
    stop_event = asyncio.Event()

    def _stop(*_: object) -> None:
        logger.info("Shutdown signal received")
        stop_event.set()

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    redis_client = Redis.from_url(settings.redis_url, decode_responses=False)

    ch_manager = ClickHouseConnectionManager(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_username,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
    )
    await ch_manager.connect()

    consumer = QuixConsumer(redis_client, ch_manager)

    logger.info(
        "Starting consumer — pipeline=%s topic=%s group=%s",
        settings.pipeline_id,
        settings.kafka_topic,
        settings.kafka_group_id,
    )

    try:
        await consumer.run(stop_event)
    finally:
        await ch_manager.close()
        await redis_client.aclose()
        logger.info("Consumer service stopped — pipeline=%s", settings.pipeline_id)


if __name__ == "__main__":
    asyncio.run(run())
