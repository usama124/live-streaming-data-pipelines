from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, TypeVar

import clickhouse_connect
from clickhouse_connect.driver.exceptions import ClickHouseError

logger = logging.getLogger("consumer-pool.clickhouse")

T = TypeVar("T")

_RETRYABLE = (ClickHouseError, ConnectionError, OSError, TimeoutError)


class ClickHouseConnectionManager:
    """
    Owns the clickhouse-connect client lifecycle.

    - Single client object replaced atomically under asyncio.Lock on reconnect.
    - execute() transparently reconnects and retries on any transient error.
    - ping() is used both by the background health probe and ClickHouseSink.setup().
    - All blocking clickhouse-connect calls are offloaded via asyncio.to_thread().
    - Thread-safe: asyncio.Lock serialises reconnection; the client object itself
      is only ever accessed from within asyncio.to_thread() calls.
    """

    def __init__(
        self,
        *,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        max_retries: int = 6,
        base_backoff_s: float = 1.0,
        max_backoff_s: float = 60.0,
        connect_timeout: int = 10,
        send_receive_timeout: int = 60,
    ) -> None:
        self._cfg: dict[str, Any] = dict(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
            connect_timeout=connect_timeout,
            send_receive_timeout=send_receive_timeout,
        )
        self._client: Any = None
        self._lock = asyncio.Lock()
        self._max_retries = max_retries
        self._base_backoff_s = base_backoff_s
        self._max_backoff_s = max_backoff_s

    async def connect(self) -> None:
        async with self._lock:
            self._client = await self._do_connect()

    async def execute(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        backoff = self._base_backoff_s
        last_exc: Exception | None = None

        for attempt in range(self._max_retries + 1):
            try:
                client = await self._get_client()
                return await asyncio.to_thread(fn, client, *args, **kwargs)
            except _RETRYABLE as exc:
                last_exc = exc
                logger.warning(
                    "ClickHouse error (attempt %d/%d): %s — %s",
                    attempt + 1, self._max_retries + 1,
                    type(exc).__name__, exc,
                )
                async with self._lock:
                    self._client = None
                if attempt < self._max_retries:
                    logger.info("Reconnecting in %.1fs...", backoff)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, self._max_backoff_s)

        raise last_exc  # type: ignore[misc]

    async def ping(self) -> bool:
        try:
            await self.execute(lambda c: c.ping())
            return True
        except Exception:
            return False

    async def close(self) -> None:
        async with self._lock:
            if self._client is not None:
                try:
                    await asyncio.to_thread(self._client.close)
                except Exception:
                    pass
                self._client = None
        logger.info("ClickHouse connection closed")

    async def _get_client(self) -> Any:
        async with self._lock:
            if self._client is None:
                self._client = await self._do_connect()
            return self._client

    async def _do_connect(self) -> Any:
        logger.info("Connecting to ClickHouse at %s:%s...", self._cfg["host"], self._cfg["port"])
        client = await asyncio.to_thread(clickhouse_connect.get_client, **self._cfg)
        logger.info("ClickHouse connected")
        return client
