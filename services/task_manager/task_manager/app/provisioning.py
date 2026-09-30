from __future__ import annotations

"""Create a pipeline's Kafka topic and ClickHouse table — before the producer starts.

Per the decision memo's creation flow: the API writes config, creates the topic
and the table, and only then starts the producer. Doing it in that order means
the first event a connector emits has somewhere to land, rather than racing
table creation.

The table comes from the pipeline's declared schema (`common/app_common/ch_schema.py`),
which the consumer pool also uses — one source of DDL, so the two cannot disagree.
"""

import asyncio
import logging

import clickhouse_connect
from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.structs import TopicPartition

from common.app_common.ch_schema import create_database_ddl, create_table_ddl
from common.app_common.models import PipelineConfig
from task_manager.app.config import settings

logger = logging.getLogger("task-manager.provisioning")


async def create_topic(config: PipelineConfig, *, partitions: int = 1, replication: int = 1) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)
    await admin.start()
    try:
        await admin.create_topics(
            [NewTopic(name=config.topic, num_partitions=partitions,
                      replication_factor=replication)]
        )
        logger.info("created topic %s", config.topic)
    except Exception as exc:
        # Already existing is fine and expected on retry; anything else is not.
        if "TopicAlreadyExists" in type(exc).__name__ or "already exists" in str(exc):
            logger.info("topic %s already exists", config.topic)
        else:
            raise
    finally:
        await admin.close()


async def create_table(config: PipelineConfig) -> str:
    """Create the pipeline's table from its declared schema. Returns the name."""
    def _create(client) -> None:
        client.command(create_database_ddl(settings.clickhouse_database))
        client.command(create_table_ddl(settings.clickhouse_database, config))

    client = await asyncio.to_thread(
        clickhouse_connect.get_client,
        host=settings.clickhouse_host, port=settings.clickhouse_port,
        username=settings.clickhouse_username, password=settings.clickhouse_password,
        database=settings.clickhouse_database, connect_timeout=10,
    )
    try:
        await asyncio.to_thread(_create, client)
    finally:
        await asyncio.to_thread(client.close)

    logger.info("created table %s.%s", settings.clickhouse_database, config.ch_unique_identifier)
    return config.ch_unique_identifier


async def wait_for_drain(config: PipelineConfig, *, timeout_s: float = 30.0) -> bool:
    """True once the consumer pool has committed past the topic's last message.

    Deleting the topic before this drops whatever the pool had not yet written.
    """
    consumer = AIOKafkaConsumer(bootstrap_servers=settings.kafka_bootstrap_servers,
                                group_id=None, enable_auto_commit=False)
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)
    await consumer.start()
    await admin.start()
    try:
        await consumer.topics()  # fetch metadata so partitions_for_topic can answer
        partitions = consumer.partitions_for_topic(config.topic)
        if not partitions:
            return True  # no topic, nothing to drain
        tps = [TopicPartition(config.topic, p) for p in partitions]

        deadline = asyncio.get_running_loop().time() + timeout_s
        while True:
            ends = await consumer.end_offsets(tps)
            committed = await admin.list_consumer_group_offsets(
                settings.consumer_group_id, partitions=tps)
            # No commit yet reads as offset -1 (or a missing entry): drained only if empty.
            if all(max(getattr(committed.get(tp), "offset", 0), 0) >= ends[tp] for tp in tps):
                return True
            if asyncio.get_running_loop().time() >= deadline:
                return False
            await asyncio.sleep(1)
    finally:
        await admin.close()
        await consumer.stop()


async def delete_topic(config: PipelineConfig) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)
    await admin.start()
    try:
        response = await admin.delete_topics([config.topic])
    finally:
        await admin.close()
    for topic, code in response.topic_error_codes:
        # 3 = UNKNOWN_TOPIC_OR_PARTITION: already gone, which is the goal.
        if code not in (0, 3):
            raise RuntimeError(f"deleting topic {topic} failed with Kafka error code {code}")
    logger.info("deleted topic %s", config.topic)


async def provision(config: PipelineConfig) -> None:
    """Topic and table, both before the producer is allowed to start."""
    await create_topic(config)
    await create_table(config)
