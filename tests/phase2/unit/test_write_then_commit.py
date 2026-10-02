"""Registry #10 — insert before commit, never the reverse.

A crash between the two must re-read the batch, not drop it. That ordering is
the whole at-least-once guarantee, so it is asserted directly rather than by
trying to kill a process at exactly the right microsecond.
"""

from __future__ import annotations

import asyncio

import pytest
from aiokafka.errors import CommitFailedError

from common.app_common.models import PipelineConfig, PipelineType
from consumer_pool.app.main import CommitBeforeRevoke, _handle
from consumer_pool.app.sink import WriteResult


class _Partition:
    def __init__(self, topic: str, partition: int = 0) -> None:
        self.topic, self.partition = topic, partition

    def __hash__(self) -> int:
        return hash((self.topic, self.partition))

    def __eq__(self, other) -> bool:
        return (self.topic, self.partition) == (other.topic, other.partition)


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
    def __init__(self, log: list[str], assigned=None, fail_commit: bool = False) -> None:
        self.log, self.assigned, self.fail_commit = log, assigned, fail_commit
        self.commits = 0
        self.committed: dict = {}

    def assignment(self):
        return self.assigned if self.assigned is not None else {_Partition("pipeline.p1.events")}

    async def commit(self, offsets) -> None:
        if self.fail_commit:
            raise CommitFailedError("group has already rebalanced")
        self.log.append("commit")
        self.commits += 1
        self.committed.update(offsets)


CONFIG = PipelineConfig(
    pipeline_id="p1", pipeline_type=PipelineType.LIVE, topic="pipeline.p1.events",
    user_id="1", collection_number=1, table_name="t",
    source_options={"endpoint": "opc.tcp://source:4840/", "node_ids": ["ns=2;i=2"]},
)


def _batch(count: int = 3):
    partition = _Partition("pipeline.p1.events")
    return {partition: [_Message(b'{"timestamp": 1, "fields": {}, "tags": {}}', i)
                        for i in range(count)]}


def test_insert_happens_before_commit() -> None:
    log: list[str] = []
    consumer = _Consumer(log)

    asyncio.run(_handle(_batch(), _Registry(CONFIG), _RecordingSink(log), consumer, asyncio.Lock()))

    assert log == ["insert", "commit"], log
    assert consumer.commits == 1


def test_a_failed_insert_does_not_commit() -> None:
    """Offsets stay put, so the batch is re-read rather than lost."""
    log: list[str] = []
    consumer = _Consumer(log)

    asyncio.run(_handle(_batch(), _Registry(CONFIG), _RecordingSink(log, fail=True), consumer, asyncio.Lock()))

    assert "commit" not in log, log
    assert consumer.commits == 0


def test_unknown_topic_is_not_committed() -> None:
    """A topic with no config yet must not have its offsets thrown away."""
    class _NoConfig:
        async def get(self, topic):
            return None

    log: list[str] = []
    consumer = _Consumer(log)

    asyncio.run(_handle(_batch(), _NoConfig(), _RecordingSink(log), consumer, asyncio.Lock()))

    assert consumer.commits == 0, "committed offsets for a pipeline we cannot write yet"


def test_commits_next_offset_of_written_partitions_only() -> None:
    log: list[str] = []
    consumer = _Consumer(log)

    asyncio.run(_handle(_batch(3), _Registry(CONFIG), _RecordingSink(log), consumer, asyncio.Lock()))

    assert consumer.committed == {_Partition("pipeline.p1.events"): 3}


def test_revoked_partition_is_not_inserted() -> None:
    """Rebalance landed between fetch and handle: the new owner reads it, not us."""
    log: list[str] = []
    sink = _RecordingSink(log)
    consumer = _Consumer(log, assigned=set())

    asyncio.run(_handle(_batch(), _Registry(CONFIG), sink, consumer, asyncio.Lock()))

    assert sink.rows_written == 0 and consumer.commits == 0, log


def test_commit_failure_after_rebalance_does_not_crash_the_pool() -> None:
    """The bug: CommitFailedError escaped and restarted the shared pool."""
    log: list[str] = []
    consumer = _Consumer(log, fail_commit=True)

    asyncio.run(_handle(_batch(), _Registry(CONFIG), _RecordingSink(log), consumer, asyncio.Lock()))

    assert log == ["insert"], log


def test_revoke_waits_for_in_flight_insert_and_commit() -> None:
    """Creating a pipeline rebalances the group. The revoke must not proceed until
    the batch in flight is committed, or the next owner re-inserts it."""
    async def scenario() -> list[str]:
        log: list[str] = []
        lock = asyncio.Lock()
        inserting = asyncio.Event()

        class _SlowSink(_RecordingSink):
            async def write(self, config, records):
                inserting.set()
                await asyncio.sleep(0.05)
                return await super().write(config, records)

        handle = asyncio.create_task(
            _handle(_batch(), _Registry(CONFIG), _SlowSink(log), _Consumer(log), lock))
        await inserting.wait()
        await CommitBeforeRevoke(lock).on_partitions_revoked(set())
        log.append("revoked")
        await handle
        return log

    assert asyncio.run(scenario()) == ["insert", "commit", "revoked"]


def test_failed_topic_does_not_hold_back_commit_of_topics_already_written() -> None:
    """Otherwise the written topic is re-read and re-inserted: duplicates."""
    class _FailSecond(_RecordingSink):
        async def write(self, config, records):
            self.fail = records[0].topic == "pipeline.p2.events"
            return await super().write(config, records)

    ok, bad = _Partition("pipeline.p1.events"), _Partition("pipeline.p2.events")
    batches = {ok: _batch(2)[ok], bad: _batch(2)[ok]}
    log: list[str] = []
    consumer = _Consumer(log, assigned={ok, bad})

    asyncio.run(_handle(batches, _Registry(CONFIG), _FailSecond(log), consumer, asyncio.Lock()))

    assert consumer.committed == {ok: 2}, consumer.committed
