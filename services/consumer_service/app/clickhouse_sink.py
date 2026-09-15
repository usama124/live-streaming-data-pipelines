from __future__ import annotations

"""
ClickHouse sink for Quix Streams.

Implements BatchingSink so Quix Streams calls write(batch) at every
checkpoint commit.  The checkpoint interval is controlled by:
  - commit_interval (seconds)  → maps to flush_interval_seconds
  - commit_every   (messages)  → maps to batch_size

write() is called from Quix's internal sync thread.  All ClickHouse
I/O goes through ClickHouseConnectionManager which uses asyncio.to_thread()
internally, so we bridge the sync→async boundary here with
run_coroutine_threadsafe().
"""

import asyncio
import logging
from typing import Any

from quixstreams.sinks.base import BatchingSink, SinkBatch

from clickhouse_manager import ClickHouseConnectionManager

logger = logging.getLogger("consumer-service.clickhouse")


class ClickHouseSink(BatchingSink):
    """
    Quix Streams BatchingSink that writes to ClickHouse.

    Quix accumulates records between checkpoints and calls write(batch)
    once per topic-partition per checkpoint.  This class fans all
    partition batches into the shared ClickHouseConnectionManager which
    handles connection pooling, reconnect, and retry.

    Schema is created automatically from the first event's keys
    (all columns as String — safe for arbitrary event shapes).
    """

    def __init__(
        self,
        manager: ClickHouseConnectionManager,
        database: str,
        table: str,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        super().__init__()
        self._mgr = manager
        self.database = database
        self.table = table
        self._loop = loop               # the main asyncio loop running in the main thread
        self._table_initialized = False

    # ── BatchingSink interface (called synchronously by Quix internals) ───

    def write(self, batch: SinkBatch) -> None:
        """
        Called by Quix Streams at every checkpoint for each topic-partition.
        Bridges sync→async by submitting a coroutine to the main event loop.
        Blocks until the insert completes or raises — Quix treats any
        exception from write() as a checkpoint failure and will retry.
        """
        events = [item.value for item in batch]
        if not events:
            return

        logger.info(
            "Flushing %d events to ClickHouse %s.%s (topic=%s partition=%d)",
            len(events),
            self.database,
            self.table,
            batch.topic,
            batch.partition,
        )

        future = asyncio.run_coroutine_threadsafe(
            self._write_async(events),
            self._loop,
        )
        # Block until done — raises if the coroutine raised.
        future.result()

    def setup(self) -> None:
        """Called by Quix on startup to verify the sink is reachable."""
        future = asyncio.run_coroutine_threadsafe(
            self._mgr.ping(),
            self._loop,
        )
        ok = future.result(timeout=15)
        if not ok:
            raise RuntimeError(
                f"ClickHouse not reachable at startup "
                f"({self._mgr._cfg['host']}:{self._mgr._cfg['port']})"
            )
        logger.info("ClickHouse sink setup OK")

    # ── Async internals ───────────────────────────────────────────────────

    async def _write_async(self, events: list[dict[str, Any]]) -> None:
        if not self._table_initialized:
            await self._ensure_table(events[0])

        columns = list(events[0].keys())
        data    = [[str(event.get(col, "")) for col in columns] for event in events]
        table_ref = f"{self.database}.{self.table}"

        await self._mgr.execute(
            lambda c: c.insert(table_ref, data=data, column_names=columns)
        )
        logger.debug("Inserted %d rows into %s", len(events), table_ref)

    async def _ensure_table(self, sample: dict[str, Any]) -> None:
        columns = ", ".join(
            f"`{self._sanitize(k)}` String" for k in sample.keys()
        )
        db  = self.database
        tbl = self.table

        await self._mgr.execute(
            lambda c: c.command(f"CREATE DATABASE IF NOT EXISTS {db}")
        )
        ddl = (
            f"CREATE TABLE IF NOT EXISTS {db}.{tbl} "
            f"({columns}) ENGINE = MergeTree ORDER BY tuple()"
        )
        await self._mgr.execute(lambda c: c.command(ddl))
        self._table_initialized = True
        logger.info("ClickHouse table %s.%s ready", db, tbl)

    @staticmethod
    def _sanitize(key: str) -> str:
        return key.replace(".", "_").replace(" ", "_").replace("-", "_").lower()
