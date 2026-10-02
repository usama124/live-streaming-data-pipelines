"""Each new pipeline gets its own collection, so its own ClickHouse table.

user_id stays "0" until requests carry the logged-in user; the counter is per user.
"""

from __future__ import annotations

import asyncio
import fnmatch

from common.app_common.models import PipelineConfig, PipelineType
from common.app_common.redis_repo import PipelineRedisRepository


class _FakeRedis:
    def __init__(self) -> None:
        self.data: dict[str, bytes] = {}

    async def get(self, key):
        return self.data.get(key)

    async def set(self, key, value, nx: bool = False):
        if nx and key in self.data:
            return None
        self.data[key] = str(value).encode()
        return True

    async def exists(self, key):
        return int(key in self.data)

    async def incr(self, key):
        self.data[key] = str(int(self.data.get(key, b"0")) + 1).encode()
        return int(self.data[key])

    async def keys(self, pattern):
        return [k.encode() for k in self.data if fnmatch.fnmatch(k, pattern)]


def _config(pid: str, collection: int) -> PipelineConfig:
    return PipelineConfig(
        pipeline_id=pid, pipeline_type=PipelineType.LIVE, topic=f"pipeline.{pid}.events",
        user_id="0", collection_number=collection,
        source_options={"endpoint": "opc.tcp://source:4840/", "node_ids": ["ns=2;i=2"]},
    )


def test_each_new_pipeline_gets_the_next_collection() -> None:
    repo = PipelineRedisRepository(_FakeRedis())

    async def scenario() -> list[str]:
        names = []
        for pid in ("a", "b", "c"):
            config = _config(pid, await repo.next_collection_number("0"))
            await repo.create_pipeline(config)
            names.append(config.ch_unique_identifier)
        return names

    assert asyncio.run(scenario()) == [
        "user_0_collection_1_events", "user_0_collection_2_events", "user_0_collection_3_events",
    ]


def test_counter_seeds_past_pipelines_created_before_it_existed() -> None:
    """Pipelines from before the counter all sit on collection 1 — don't hand out 1 again."""
    repo = PipelineRedisRepository(_FakeRedis())

    async def scenario() -> int:
        await repo.create_pipeline(_config("old", 1))
        return await repo.next_collection_number("0")

    assert asyncio.run(scenario()) == 2


def test_counters_are_per_user() -> None:
    repo = PipelineRedisRepository(_FakeRedis())

    async def scenario() -> tuple[int, int]:
        await repo.next_collection_number("0")
        return await repo.next_collection_number("0"), await repo.next_collection_number("7")

    assert asyncio.run(scenario()) == (2, 1)
