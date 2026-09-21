"""Registry #10 — insert before commit, never the reverse.

A crash between the two must re-read the batch, not drop it. That ordering is
the whole at-least-once guarantee, so it is asserted directly rather than by
trying to kill a process at exactly the right microsecond.
"""

from __future__ import annotations

import asyncio

import pytest

from common.app_common.models import PipelineConfig, PipelineType
from consumer_pool.app.main import _handle
from consumer_pool.app.sink import WriteResult


class _Partition:
    def __init__(self, topic: str, partition: int = 0) -> None:
        self.topic, self.partition = topic, partition

    def __hash__(self) -> int:
        return hash((self.topic, self.partition))


class _Message:
    def __init__(self, value: bytes, offset: int) -> None:
        self.value, self.offset = value, offset


class _Registry:
    def __init__(self, config: PipelineConfig) -> None:
        self._config = config

    async def get(self, topic: str):
        return self._config


class _RecordingSink:
    """Records the order of operations so the contract can be asserted on it."""

    def __init__(self, log: list[str], fail: bool = False) -> None:
        self.log, self.fail = log, fail
        self.rows_written = 0

    async def write(self, config, records) -> WriteResult:
        if self.fail:
            self.log.append("insert-failed")
            raise RuntimeError("ClickHouse unreachable")
        self.log.append("insert")
        self.rows_written += len(records)
        return WriteResult(written=len(records))


class _Consumer:
    def __init__(self, log: list[str]) -> None:
        self.log = log
        self.commits = 0

    async def commit(self) -> None:
        self.log.append("commit")
        self.commits += 1


CONFIG = PipelineConfig(
    pipeline_id="p1", pipeline_type=PipelineType.LIVE, topic="pipeline.p1.events",
    user_id="1", collection_number=1, table_name="t",
)


def _batch(count: int = 3):
    partition = _Partition("pipeline.p1.events")
    return {partition: [_Message(b'{"timestamp": 1, "fields": {}, "tags": {}}', i)
                        for i in range(count)]}


def test_insert_happens_before_commit() -> None:
    log: list[str] = []
    consumer = _Consumer(log)

    asyncio.run(_handle(_batch(), _Registry(CONFIG), _RecordingSink(log), consumer))

    assert log == ["insert", "commit"], log
    assert consumer.commits == 1


def test_a_failed_insert_does_not_commit() -> None:
    """Offsets stay put, so the batch is re-read rather than lost."""
    log: list[str] = []
    consumer = _Consumer(log)

    asyncio.run(_handle(_batch(), _Registry(CONFIG), _RecordingSink(log, fail=True), consumer))

    assert "commit" not in log, log
    assert consumer.commits == 0


def test_unknown_topic_is_not_committed() -> None:
    """A topic with no config yet must not have its offsets thrown away."""
    class _NoConfig:
        async def get(self, topic):
            return None

    log: list[str] = []
    consumer = _Consumer(log)

    asyncio.run(_handle(_batch(), _NoConfig(), _RecordingSink(log), consumer))

    assert consumer.commits == 0, "committed offsets for a pipeline we cannot write yet"
