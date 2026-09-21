from __future__ import annotations

import asyncio
import logging
from typing import Any

import docker
from docker.errors import NotFound

from common.app_common.models import PipelineConfig, PipelineState
from common.app_common.runtime.base import RuntimeAdapter
from common.app_common.telegraf_config import render_telegraf_config

logger = logging.getLogger("runtime.docker")


class DockerRuntimeAdapter(RuntimeAdapter):

    def __init__(self, settings: Any) -> None:
        self._s = settings

    async def start_live_pipeline(self, config: PipelineConfig, state: PipelineState) -> tuple[str, str]:
        """Starts the producer only. Consumption is the shared pool's job — it
        picks the new topic up by pattern, with no per-pipeline container and no
        restart of anything."""
        pname = _cname("producer", config.pipeline_id)
        await asyncio.to_thread(self._remove_if_exists, pname)
        await asyncio.to_thread(self._run, image=self._s.producer_image, name=pname,
                                env=self._producer_env(config), role="producer",
                                pipeline_id=config.pipeline_id)
        logger.info("Started %s", pname)
        return pname, ""

    async def stop_live_pipeline(self, config: PipelineConfig, state: PipelineState) -> None:
        # Only the producer is per-pipeline; the pool keeps serving everyone else.
        name = state.producer_container or _cname("producer", config.pipeline_id)
        await asyncio.to_thread(self._stop_and_remove, name)
        logger.info("Stopped producer for pipeline %s", config.pipeline_id)

    async def restart_live_pipeline(self, config: PipelineConfig, state: PipelineState) -> tuple[str, str]:
        await self.stop_live_pipeline(config, state)
        await asyncio.sleep(2)
        return await self.start_live_pipeline(config, state)

    async def get_logs(self, container_name: str, *, tail: int = 200) -> str:
        return await asyncio.to_thread(self._fetch_logs, container_name, tail)

    async def is_running(self, container_name: str) -> bool:
        return await asyncio.to_thread(self._container_running, container_name)

    def _producer_env(self, config: PipelineConfig) -> dict[str, str]:
        """The producer is Telegraf now, so it takes one rendered config rather
        than a dozen env vars. The entrypoint writes TELEGRAF_CONFIG to disk —
        we drive the *host* Docker daemon, so a bind-mounted path here would have
        to exist on the host, not in this container."""
        return {
            "TELEGRAF_CONFIG": render_telegraf_config(
                config, kafka_brokers=self._s.kafka_bootstrap_servers
            ),
        }

    def _client(self) -> Any:
        return docker.from_env()

    def _run(self, *, image, name, env, role, pipeline_id) -> Any:
        return self._client().containers.run(
            image=image, name=name, detach=True, environment=env,
            network=self._s.docker_network,
            restart_policy={"Name": "on-failure", "MaximumRetryCount": 5},
            labels={"data-platform.role": role,
                    "data-platform.pipeline_id": pipeline_id,
                    "data-platform.managed": "true"},
        )

    def _remove_if_exists(self, name: str) -> None:
        try:
            self._client().containers.get(name).remove(force=True)
        except NotFound:
            pass

    def _stop_and_remove(self, name: str) -> None:
        try:
            c = self._client().containers.get(name)
            c.stop(timeout=15)
            c.remove(force=True)
        except NotFound:
            pass

    def _fetch_logs(self, name: str, tail: int) -> str:
        try:
            return self._client().containers.get(name).logs(tail=tail).decode(errors="replace")
        except NotFound:
            return f"[container '{name}' not found]"
        except Exception as exc:
            return f"[error fetching logs: {exc}]"

    def _container_running(self, name: str) -> bool:
        try:
            c = self._client().containers.get(name)
            c.reload()
            return c.status == "running"
        except NotFound:
            return False


class RedisOnlyAdapter(RuntimeAdapter):
    async def start_live_pipeline(self, config, state) -> tuple[str, str]:  # type: ignore
        return "redis-only-producer", "redis-only-consumer"
    async def stop_live_pipeline(self, config, state) -> None: pass  # type: ignore
    async def restart_live_pipeline(self, config, state) -> tuple[str, str]:  # type: ignore
        return "redis-only-producer", "redis-only-consumer"
    async def get_logs(self, container_name: str, *, tail: int = 200) -> str:
        return "[redis_only mode]"
    async def is_running(self, container_name: str) -> bool:
        return True


def _cname(role: str, pipeline_id: str) -> str:
    safe = pipeline_id.replace(".", "-").replace("_", "-")
    return f"dp-{role}-{safe}"
