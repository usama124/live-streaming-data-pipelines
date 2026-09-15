from __future__ import annotations

from redis.asyncio import Redis

from common.app_common.models import (
    DesiredState,
    PipelineConfig,
    PipelineState,
    PipelineStateEvent,
    PipelineStatus,
    PipelineType,
    utc_now_iso,
)
from common.app_common.redis_keys import (
    PIPELINE_STATE_EVENTS_CHANNEL,
    pipeline_config_key,
    pipeline_state_key,
)

_UNSET = object()


class PipelineRedisRepository:
    def __init__(self, redis_client: Redis) -> None:
        self.redis = redis_client

    async def create_pipeline(self, config: PipelineConfig) -> PipelineState:
        if await self.get_config(config.pipeline_id) is not None:
            raise ValueError(f"Pipeline already exists: {config.pipeline_id}")
        state = PipelineState(
            pipeline_id=config.pipeline_id,
            pipeline_type=config.pipeline_type,
            desired_state=DesiredState.STOPPED,
            status=PipelineStatus.CREATED,
            topic=config.topic,
        )
        await self.redis.set(pipeline_config_key(config.pipeline_id), config.model_dump_json())
        await self.redis.set(pipeline_state_key(config.pipeline_id), state.model_dump_json())
        return state

    async def get_config(self, pipeline_id: str) -> PipelineConfig | None:
        raw = await self.redis.get(pipeline_config_key(pipeline_id))
        return PipelineConfig.model_validate_json(raw) if raw else None

    async def get_state(self, pipeline_id: str) -> PipelineState | None:
        raw = await self.redis.get(pipeline_state_key(pipeline_id))
        return PipelineState.model_validate_json(raw) if raw else None

    async def list_pipeline_ids(self) -> list[str]:
        keys = await self.redis.keys("pipeline:*:config")
        ids: list[str] = []
        for key in keys:
            text = key.decode() if isinstance(key, bytes) else str(key)
            ids.append(text.split(":", 2)[1])
        return sorted(ids)

    async def save_state(self, state: PipelineState) -> PipelineState:
        state.updated_at = utc_now_iso()
        await self.redis.set(pipeline_state_key(state.pipeline_id), state.model_dump_json())
        return state

    async def update_state(
        self,
        pipeline_id: str,
        *,
        desired_state: DesiredState | None = None,
        status: PipelineStatus | None = None,
        last_error: str | None = None,
        producer_container: str | None | object = _UNSET,
        consumer_container: str | None | object = _UNSET,
    ) -> PipelineState:
        state = await self.get_state(pipeline_id)
        if state is None:
            raise KeyError(f"Pipeline state not found: {pipeline_id}")
        if desired_state is not None:
            state.desired_state = desired_state
        if status is not None:
            state.status = status
        if last_error is not None:
            state.last_error = last_error
        if producer_container is not _UNSET:
            state.producer_container = producer_container  # type: ignore[assignment]
        if consumer_container is not _UNSET:
            state.consumer_container = consumer_container  # type: ignore[assignment]
        return await self.save_state(state)

    async def delete_pipeline(self, pipeline_id: str) -> None:
        await self.redis.delete(pipeline_config_key(pipeline_id))
        await self.redis.delete(pipeline_state_key(pipeline_id))

    async def publish_state_event(self, state: PipelineState, event_type: str) -> None:
        event = PipelineStateEvent(
            pipeline_id=state.pipeline_id,
            event_type=event_type,  # type: ignore[arg-type]
            desired_state=state.desired_state,
            status=state.status,
        )
        await self.redis.publish(PIPELINE_STATE_EVENTS_CHANNEL, event.model_dump_json())
