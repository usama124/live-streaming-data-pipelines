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

    async def delete(self, key):
        return int(self.data.pop(key, None) is not None)

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


def test_failed_provisioning_leaves_nothing_behind_so_create_can_be_retried(monkeypatch) -> None:
    """Kafka down at create time must not turn every retry into 409 'already exists'."""
    import sys
    import types

    import pytest
    from fastapi import HTTPException

    # The repo's own services/task_manager/watchdog/ shadows the PyPI watchdog the
    # folder watcher imports; main only needs the loop at startup, so stub it.
    stub = types.ModuleType("task_manager.app.folder_watcher")
    stub.folder_watcher_loop = None
    monkeypatch.setitem(sys.modules, "task_manager.app.folder_watcher", stub)
    from common.app_common.models import PipelineCreateRequest
    from task_manager.app import main

    repo = PipelineRedisRepository(_FakeRedis())
    request = PipelineCreateRequest(
        pipeline_id="p5", pipeline_type="live",
        source_options={"endpoint": "opc.tcp://source:4840/", "node_ids": ["ns=2;i=2"]},
    )

    async def kafka_down(config):
        raise ConnectionError("Unable to bootstrap")

    async def ok(config):
        return None

    monkeypatch.setattr(main, "provision", kafka_down)
    with pytest.raises(HTTPException) as exc:
        asyncio.run(main.create_pipeline(request, repo))
    assert exc.value.status_code == 502
    assert asyncio.run(repo.get_config("p5")) is None

    monkeypatch.setattr(main, "provision", ok)
    created = asyncio.run(main.create_pipeline(request, repo))
    assert created["state"]["pipeline_id"] == "p5"
