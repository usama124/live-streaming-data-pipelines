from __future__ import annotations

"""ClickHouse writes for the shared consumer pool, including dead-lettering.

Two rules this file exists to enforce:

1. **No exception escapes the write path.** The pool is shared, so an exception
   that gets out stalls every other pipeline on it. A record that cannot be
   mapped or inserted goes to `dead_letter_events` and the rest of the batch
   proceeds.
2. **Tables are created from the declared schema**, at pipeline-creation time,
   never inferred from the first event seen.

Failures are isolated per record, not per batch: one bad row must not discard
the ~499 good ones beside it.
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Sequence

from prometheus_client import Counter

from common.app_common.ch_schema import (
    DLQ_TABLE,
    create_database_ddl,
    create_dlq_table_ddl,
    create_table_ddl,
)
from common.app_common.models import PipelineConfig
from consumer_pool.app.clickhouse_manager import ClickHouseConnectionManager
from consumer_pool.app.rows import RecordError, to_row

logger = logging.getLogger("consumer-pool.sink")

dlq_rows_total = Counter(
    "dlq_rows_total", "Records routed to dead_letter_events", ["pipeline_id"]
)
rows_written_total = Counter(
    "rows_written_total", "Records inserted into a pipeline's table", ["pipeline_id"]
)

_DLQ_COLUMNS = ["pipeline_id", "topic", "partition", "offset",
                "target_table", "error_message", "payload"]


@dataclass
class IncomingRecord:
    """One Kafka message, with the coordinates a dead letter needs for replay."""
    value: Any
    topic: str
    partition: int
    offset: int
    raw: str


@dataclass
class WriteResult:
    written: int = 0
    dead_lettered: int = 0
    errors: list[str] = field(default_factory=list)


class ClickHouseSink:
    def __init__(self, manager: ClickHouseConnectionManager, database: str) -> None:
        self._mgr = manager
        self._database = database
        self._known_tables: set[str] = set()

    async def ensure_dlq_table(self) -> None:
        await self._mgr.execute(lambda c: c.command(create_database_ddl(self._database)))
        await self._mgr.execute(lambda c: c.command(create_dlq_table_ddl(self._database)))

    async def ensure_table(self, config: PipelineConfig) -> str:
        """Create the pipeline's table from its declared schema. Idempotent."""
        table = config.ch_unique_identifier
        if table in self._known_tables:
            return table

        await self._mgr.execute(lambda c: c.command(create_database_ddl(self._database)))
        await self._mgr.execute(lambda c: c.command(create_table_ddl(self._database, config)))
        self._known_tables.add(table)
        logger.info("table %s.%s ready", self._database, table)
        return table

    async def write(self, config: PipelineConfig, records: Sequence[IncomingRecord]) -> WriteResult:
        """Insert a batch. Returns counts; raises only if ClickHouse itself is
        unreachable, in which case the caller must not commit."""
        result = WriteResult()
        table = await self.ensure_table(config)
        columns = list(config.table_schema)

        rows: list[list[Any]] = []
        keep: list[IncomingRecord] = []
        dead: list[tuple[IncomingRecord, str]] = []

        for record in records:
            try:
                row = to_row(record.value, config)
            except RecordError as exc:
                dead.append((record, f"map: {exc}"))
                continue
            rows.append([row[c] for c in columns])
            keep.append(record)

        if rows:
            try:
                await self._insert(table, columns, rows)
                result.written = len(rows)
            except Exception as exc:
                # The batch failed as a whole. Retry each record on its own so a
                # single poison row cannot discard its neighbours.
                # ponytail: per-record retry is O(n) inserts on a failing batch;
                # bisecting would be ~log2(n) if failing batches ever get common
                # enough for the extra parts to matter.
                logger.warning("batch insert into %s failed (%s) — isolating records", table, exc)
                written, isolated = await self._insert_individually(table, columns, keep, config)
                result.written = written
                dead.extend(isolated)

        if dead:
            await self._dead_letter(table, dead, config)
            result.dead_lettered = len(dead)
            result.errors = [reason for _, reason in dead[:5]]

        if result.written:
            rows_written_total.labels(pipeline_id=config.pipeline_id).inc(result.written)
        return result

    async def _insert(self, table: str, columns: list[str], rows: list[list[Any]]) -> None:
        await self._mgr.execute(
            lambda c: c.insert(f"{self._database}.{table}", data=rows, column_names=columns)
        )

    async def _insert_individually(
        self, table: str, columns: list[str],
        records: Sequence[IncomingRecord], config: PipelineConfig,
    ) -> tuple[int, list[tuple[IncomingRecord, str]]]:
        written = 0
        failed: list[tuple[IncomingRecord, str]] = []
        for record in records:
            try:
                row = to_row(record.value, config)
                await self._insert(table, columns, [[row[c] for c in columns]])
                written += 1
            except Exception as exc:
                failed.append((record, f"insert: {type(exc).__name__}: {exc}"))
        return written, failed

    async def _dead_letter(
        self, table: str, dead: list[tuple[IncomingRecord, str]], config: PipelineConfig,
    ) -> None:
        rows = [
            [config.pipeline_id, record.topic, record.partition, record.offset,
             table, reason[:2000], record.raw[:64_000]]
            for record, reason in dead
        ]
        await self._mgr.execute(
            lambda c: c.insert(f"{self._database}.{DLQ_TABLE}", data=rows,
                               column_names=_DLQ_COLUMNS)
        )
        dlq_rows_total.labels(pipeline_id=config.pipeline_id).inc(len(rows))
        logger.warning("dead-lettered %d record(s) from %s: %s",
                       len(rows), config.pipeline_id, dead[0][1])
