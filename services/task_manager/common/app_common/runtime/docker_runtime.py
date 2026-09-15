from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import docker
from docker.errors import NotFound

from common.app_common.models import PipelineConfig, PipelineState
from common.app_common.runtime.base import RuntimeAdapter

logger = logging.getLogger("runtime.docker")


class DockerRuntimeAdapter(RuntimeAdapter):

    def __init__(self, settings: Any) -> None:
        self._s = settings

    async def start_live_pipeline(self, config: PipelineConfig, state: PipelineState) -> tuple[str, str]:
        pname = _cname("producer", config.pipeline_id)
        cname = _cname("consumer", config.pipeline_id)
        await asyncio.to_thread(self._remove_if_exists, pname)
        await asyncio.to_thread(self._remove_if_exists, cname)
        print(f"Producer ENV: {self._producer_env(config)}")
        print(f"Consumer ENV: {self._consumer_env(config)}")
        await asyncio.to_thread(self._run, image=self._s.producer_image, name=pname,
                                env=self._producer_env(config), role="producer",
                                pipeline_id=config.pipeline_id)
        await asyncio.to_thread(self._run, image=self._s.consumer_image, name=cname,
                                env=self._consumer_env(config), role="consumer",
                                pipeline_id=config.pipeline_id)
        logger.info("Started %s and %s", pname, cname)
        return pname, cname

    async def stop_live_pipeline(self, config: PipelineConfig, state: PipelineState) -> None:
        for name in [
            state.producer_container or _cname("producer", config.pipeline_id),
            state.consumer_container or _cname("consumer", config.pipeline_id),
        ]:
            await asyncio.to_thread(self._stop_and_remove, name)
        logger.info("Stopped containers for pipeline %s", config.pipeline_id)

    async def restart_live_pipeline(self, config: PipelineConfig, state: PipelineState) -> tuple[str, str]:
        await self.stop_live_pipeline(config, state)
        await asyncio.sleep(2)
        return await self.start_live_pipeline(config, state)

    async def get_logs(self, container_name: str, *, tail: int = 200) -> str:
        return await asyncio.to_thread(self._fetch_logs, container_name, tail)

    async def is_running(self, container_name: str) -> bool:
        return await asyncio.to_thread(self._container_running, container_name)

    def _producer_env(self, config: PipelineConfig) -> dict[str, str]:
        s, opts = self._s, config.source_options
        env: dict[str, str] = {
            "PIPELINE_ID":             config.pipeline_id,
            "SOURCE_TYPE":             config.source_type,
            "REDIS_URL":               s.redis_url,
            "KAFKA_BOOTSTRAP_SERVERS": s.kafka_bootstrap_servers,
            "KAFKA_TOPIC":             config.topic,
            "MOCK_MIN_DELAY_MS":       str(opts.get("min_delay_ms", 100)),
            "MOCK_MAX_DELAY_MS":       str(opts.get("max_delay_ms", 500)),
        }
        if config.source_type == "opcua":
            env.update({
                "OPCUA_ENDPOINT":               str(opts.get("endpoint", "")),
                "OPCUA_NODE_IDS":               ",".join(opts.get("node_ids", [])),
                "OPCUA_NODE_NAMES_JSON":        json.dumps(opts.get("node_names", {})),
                "OPCUA_PUBLISHING_INTERVAL_MS": str(opts.get("publishing_interval_ms", 500)),
                "OPCUA_RECONNECT_BASE_S":       str(opts.get("reconnect_base_s", 2.0)),
                "OPCUA_RECONNECT_MAX_S":        str(opts.get("reconnect_max_s", 60.0)),
            })
        return env

    def _consumer_env(self, config: PipelineConfig) -> dict[str, str]:
        s = self._s
        return {
            "PIPELINE_ID":             config.pipeline_id,
            "REDIS_URL":               s.redis_url,
            "KAFKA_BOOTSTRAP_SERVERS": s.kafka_bootstrap_servers,
            "KAFKA_TOPIC":             config.topic,
            "KAFKA_GROUP_ID":          f"consumer-{config.pipeline_id}",
            "BATCH_SIZE":              str(config.batch_size),
            "FLUSH_INTERVAL_SECONDS":  str(config.flush_interval_seconds),
            "CLICKHOUSE_HOST":         s.clickhouse_host,
            "CLICKHOUSE_PORT":         str(s.clickhouse_port),
            "CLICKHOUSE_USERNAME":     s.clickhouse_username,
            "CLICKHOUSE_PASSWORD":     s.clickhouse_password,
            "CLICKHOUSE_DATABASE":     s.clickhouse_database,
            "CLICKHOUSE_TABLE":        s.clickhouse_table,
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
