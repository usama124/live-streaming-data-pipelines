from __future__ import annotations

from redis.asyncio import Redis

from common.app_common.models import DesiredState, PipelineState, PipelineStatus, utc_now_iso
from common.app_common.redis_keys import pipeline_state_key


class ProducerControl:
    def __init__(self, redis_client: Redis, pipeline_id: str) -> None:
        self.redis = redis_client
        self.pipeline_id = pipeline_id

    async def get_state(self) -> PipelineState | None:
        raw = await self.redis.get(pipeline_state_key(self.pipeline_id))
        if raw is None:
            return None
        return PipelineState.model_validate_json(raw)

    async def should_run(self) -> bool:
        state = await self.get_state()
        return state is not None and state.desired_state == DesiredState.RUNNING

    async def mark_running(self) -> None:
        state = await self.get_state()
        if state is None:
            return
        state.status = PipelineStatus.RUNNING
        state.last_heartbeat_at = utc_now_iso()
        state.updated_at = utc_now_iso()
        await self.redis.set(pipeline_state_key(self.pipeline_id), state.model_dump_json())

    async def heartbeat(self) -> None:
        state = await self.get_state()
        if state is None:
            return
        state.last_heartbeat_at = utc_now_iso()
        state.updated_at = utc_now_iso()
        await self.redis.set(pipeline_state_key(self.pipeline_id), state.model_dump_json())

    async def mark_stopped(self) -> None:
        state = await self.get_state()
        if state is None:
            return
        state.status = PipelineStatus.STOPPED
        state.last_heartbeat_at = utc_now_iso()
        state.updated_at = utc_now_iso()
        await self.redis.set(pipeline_state_key(self.pipeline_id), state.model_dump_json())

    async def mark_failed(self, error: str) -> None:
        state = await self.get_state()
        if state is None:
            return
        state.status = PipelineStatus.FAILED
        state.last_error = error
        state.updated_at = utc_now_iso()
        await self.redis.set(pipeline_state_key(self.pipeline_id), state.model_dump_json())
