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
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

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


async def provision(config: PipelineConfig) -> None:
    """Topic and table, both before the producer is allowed to start."""
    await create_topic(config)
    await create_table(config)
