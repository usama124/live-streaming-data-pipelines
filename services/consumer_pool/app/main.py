from __future__ import annotations

"""Shared consumer pool — one deployment for every live pipeline.

    getmany() -> group per topic -> insert -> commit

The order is the contract. Committing before the insert would drop rows on a
crash mid-batch; this way a crash re-reads them, which is at-least-once and the
behaviour the design accepts knowingly.

The pool subscribes by *pattern*, never a topic list, so creating a pipeline
never restarts it. Topic names route: topic -> PipelineConfig -> the pipeline's
ClickHouse table, resolved through Redis and cached. The topic name is never
parsed into a table name — they are separate identifiers by design.
"""

import asyncio
import json
import logging
import re
import signal

from aiokafka import AIOKafkaConsumer
from prometheus_client import start_http_server
from redis.asyncio import Redis

from common.app_common.models import PipelineConfig
from common.app_common.redis_repo import PipelineRedisRepository
from consumer_pool.app.clickhouse_manager import ClickHouseConnectionManager
from consumer_pool.app.config import settings
from consumer_pool.app.sink import ClickHouseSink, IncomingRecord

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("consumer-pool")

_TOPIC_PIPELINE_ID = re.compile(r"^pipeline\.(?P<pipeline_id>.+)\.events$")


class PipelineRegistry:
    """topic -> PipelineConfig, read from Redis on a cache miss.

    A miss must be a lookup, never a restart: the pool learns about pipelines
    created after it started without being bounced.
    """

    def __init__(self, repo: PipelineRedisRepository) -> None:
        self._repo = repo
        self._cache: dict[str, PipelineConfig] = {}

    async def get(self, topic: str) -> PipelineConfig | None:
        if topic in self._cache:
            return self._cache[topic]

        match = _TOPIC_PIPELINE_ID.match(topic)
        if not match:
            logger.warning("topic %s does not match the naming contract — skipping", topic)
            return None

        config = await self._repo.get_config(match.group("pipeline_id"))
        if config is None:
            logger.warning("no config for %s — not consuming it yet", topic)
            return None

        self._cache[topic] = config
        return config


async def run() -> None:
    stop = asyncio.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    start_http_server(settings.metrics_port)

    redis = Redis.from_url(settings.redis_url, decode_responses=False)
    await redis.ping()
    registry = PipelineRegistry(PipelineRedisRepository(redis))

    manager = ClickHouseConnectionManager(
        host=settings.clickhouse_host, port=settings.clickhouse_port,
        username=settings.clickhouse_username, password=settings.clickhouse_password,
        database=settings.clickhouse_database,
    )
    await manager.connect()
    sink = ClickHouseSink(manager, settings.clickhouse_database)
    await sink.ensure_dlq_table()

    consumer = AIOKafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.group_id,
        enable_auto_commit=False,          # commits follow inserts, never precede them
        auto_offset_reset="earliest",
        metadata_max_age_ms=settings.metadata_max_age_ms,
        max_poll_records=settings.batch_size,
    )
    consumer.subscribe(pattern=settings.topic_pattern)
    await consumer.start()
    logger.info("consumer pool started — pattern=%s", settings.topic_pattern)

    try:
        while not stop.is_set():
            batches = await consumer.getmany(
                timeout_ms=int(settings.flush_interval_seconds * 1000),
                max_records=settings.batch_size,
            )
            if batches:
                await _handle(batches, registry, sink, consumer)
    finally:
        await consumer.stop()
        await manager.close()
        await redis.aclose()
        logger.info("consumer pool stopped")


async def _handle(batches, registry: PipelineRegistry, sink: ClickHouseSink, consumer) -> None:
    """Insert every topic's records, then commit — in that order, once."""
    wrote_anything = False

    for partition, messages in batches.items():
        config = await registry.get(partition.topic)
        if config is None:
            continue  # unknown topic: leave the offsets uncommitted

        records = []
        for message in messages:
            raw = message.value.decode(errors="replace")
            try:
                value = json.loads(raw)
            except json.JSONDecodeError:
                value = raw  # to_row rejects it, and it dead-letters with its payload
            records.append(IncomingRecord(
                value=value, topic=partition.topic,
                partition=partition.partition, offset=message.offset, raw=raw,
            ))

        try:
            result = await sink.write(config, records)
        except Exception:
            # ClickHouse is unreachable, not a bad record. Do not commit: the
            # batch is re-read when it comes back.
            logger.exception("write failed for %s — not committing", partition.topic)
            return

        wrote_anything = True
        if result.dead_lettered:
            logger.warning("%s: %d written, %d dead-lettered (%s)",
                           partition.topic, result.written, result.dead_lettered,
                           "; ".join(result.errors))

    if wrote_anything:
        await consumer.commit()


if __name__ == "__main__":
    asyncio.run(run())
