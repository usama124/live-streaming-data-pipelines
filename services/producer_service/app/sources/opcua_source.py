from __future__ import annotations

import asyncio
import logging
import math
import random
from datetime import datetime, timezone
from typing import Any, AsyncIterator

from asyncua import Client, Node

from app.sources.base import StreamingSource

logger = logging.getLogger("producer-service.opcua")


# ──────────────────────────────────────────────
# OPC UA subscription callback handler
# ──────────────────────────────────────────────

class _DataChangeHandler:
    """
    Invoked by asyncua on every monitored-item change notification.
    Pushes records into a bounded asyncio.Queue that OpcUaStreamingSource
    drains via its async generator.
    """

    def __init__(self, queue: asyncio.Queue[dict[str, Any]], pipeline_id: str) -> None:
        self._queue = queue
        self._pipeline_id = pipeline_id
        self._sequence: int = 0

    def datachange_notification(self, node: Node, val: Any, data: Any) -> None:  # noqa: ANN401
        try:
            src_ts = data.monitored_item.Value.SourceTimestamp
            event_time = (
                src_ts.replace(tzinfo=timezone.utc).isoformat()
                if src_ts
                else datetime.now(timezone.utc).isoformat()
            )
            quality = data.monitored_item.Value.StatusCode
            quality_str = "good" if quality.is_good else ("uncertain" if quality.is_uncertain else "bad")

            self._sequence += 1
            record: dict[str, Any] = {
                "pipeline_id": self._pipeline_id,
                "event_time": event_time,
                "sequence": self._sequence,
                "source": "opcua",
                "sensor": node.nodeid.to_string(),
                "value": val,
                "quality": quality_str,
                "unit": "m/s",
                "asset_id": node.nodeid.to_string()
            }
            self._queue.put_nowait(record)
        except asyncio.QueueFull:
            logger.warning("OPC UA queue full — dropping record for node %s", node.nodeid)
        except Exception:
            logger.exception("Unexpected error in datachange_notification")


# ──────────────────────────────────────────────
# Main source class
# ──────────────────────────────────────────────

class OpcUaStreamingSource(StreamingSource):
    """
    Production-grade OPC UA source that:
      - Connects to an OPC UA server via asyncua
      - Creates a server-push subscription (no polling)
      - Yields events as an async generator compatible with the existing
        StreamingSource interface
      - Reconnects with exponential backoff on session loss

    Config keys (from source_options in PipelineConfig):
        endpoint          OPC UA server URL, e.g. "opc.tcp://host:4840/path"
        node_ids          List of NodeID strings to subscribe, e.g. ["ns=2;i=2", ...]
        node_names        Optional dict mapping NodeID → human-readable name
        publishing_interval_ms  Server publish interval (default 500)
        reconnect_base_s  Initial reconnect delay in seconds (default 2)
        reconnect_max_s   Maximum reconnect delay in seconds (default 60)
        queue_maxsize     Internal buffer depth (default 1000)
    """

    SOURCE_TYPE = "opcua"

    def __init__(self, pipeline_id: str, source_options: dict[str, Any]) -> None:
        self._pipeline_id = pipeline_id

        opts = source_options
        self._endpoint: str = opts["endpoint"]
        self._node_ids: list[str] = opts["node_ids"]
        self._node_names: dict[str, str] = opts.get("node_names", {})
        self._publishing_interval_ms: int = opts.get("publishing_interval_ms", 500)
        self._reconnect_base_s: float = opts.get("reconnect_base_s", 2.0)
        self._reconnect_max_s: float = opts.get("reconnect_max_s", 60.0)

        queue_maxsize: int = opts.get("queue_maxsize", 1000)
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=queue_maxsize)

    # ── StreamingSource interface ──────────────────────────────────────────

    async def stream(self) -> AsyncIterator[dict[str, Any]]:  # type: ignore[override]
        """
        Yields OPC UA data-change events indefinitely.
        Transparently reconnects with exponential backoff on failure.
        """
        backoff = self._reconnect_base_s
        while True:
            try:
                async with self._session() as _:
                    backoff = self._reconnect_base_s  # reset on successful connect
                    while True:
                        record = await asyncio.wait_for(self._queue.get(), timeout=2.0)
                        # Enrich with human-readable name if configured
                        if name := self._node_names.get(record["sensor"]):
                            record["sensor"] = name.lower()
                        yield record
            except asyncio.TimeoutError:
                # No data in 2 s — keep looping (lets caller check stop signal)
                continue
            except Exception:
                logger.exception(
                    "OPC UA session lost (%s) — reconnecting in %.1fs", self._endpoint, backoff
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._reconnect_max_s)

    # ── Internal helpers ───────────────────────────────────────────────────

    def _session(self):  # noqa: ANN201
        """Async context manager: connects, subscribes, yields, then cleans up."""

        class _Ctx:
            def __init__(ctx_self) -> None:  # noqa: N805
                ctx_self._client: Client | None = None
                ctx_self._sub = None

            async def __aenter__(ctx_self):  # noqa: N805
                logger.info("Connecting to OPC UA server: %s", self._endpoint)
                client = Client(url=self._endpoint)
                await client.connect()
                ctx_self._client = client

                handler = _DataChangeHandler(self._queue, self._pipeline_id)
                sub = await client.create_subscription(
                    period=self._publishing_interval_ms,
                    handler=handler,
                )
                ctx_self._sub = sub

                nodes = [client.get_node(nid) for nid in self._node_ids]
                await sub.subscribe_data_change(nodes)
                logger.info(
                    "OPC UA subscribed to %d nodes (interval=%dms)",
                    len(nodes),
                    self._publishing_interval_ms,
                )
                return ctx_self

            async def __aexit__(ctx_self, *_: object) -> None:  # noqa: N805
                try:
                    if ctx_self._sub:
                        await ctx_self._sub.delete()
                except Exception:
                    logger.debug("Error deleting subscription", exc_info=True)
                try:
                    if ctx_self._client:
                        await ctx_self._client.disconnect()
                except Exception:
                    logger.debug("Error disconnecting OPC UA client", exc_info=True)
                logger.info("OPC UA session closed")

        return _Ctx()


# ──────────────────────────────────────────────
# Standalone mock OPC UA server (dev / CI use)
# ──────────────────────────────────────────────

class MockOpcUaServer:
    """
    Minimal OPC UA server for local development and integration testing.
    Exposes 5 nodes under PlantFloor and streams realistic simulated values.

    Usage:
        python -m app.sources.opcua_source          # starts on opc.tcp://0.0.0.0:4840
        OPCUA_SERVER_PORT=4841 python -m app.sources.opcua_source
    """

    # NodeID → (display_name, initial_value, unit)
    NODE_SCHEMA: dict[str, tuple[str, float | str, str]] = {
        "Temperature": ("Temperature", 25.0, "C"),
        "Pressure": ("Pressure", 101.3, "bar"),
        "Vibration": ("Vibration", 0.05, "mm/s"),
        "FlowRate": ("FlowRate", 120.0, "m3/h"),
        "MachineStatus": ("MachineStatus", "RUNNING", ""),
    }
    STATUSES = ["RUNNING", "IDLE", "MAINTENANCE", "ERROR"]

    def __init__(self, host: str = "0.0.0.0", port: int = 4840) -> None:  # noqa: S104
        self._endpoint = f"opc.tcp://{host}:{port}/stratahub/server/"
        self._nodes: dict[str, Any] = {}
        self._running = False

    async def start(self) -> None:
        from asyncua import Server  # local import — only needed when running as server

        server = Server()
        await server.init()
        server.set_endpoint(self._endpoint)
        server.set_server_name("StrataHub Mock Industrial Server")

        idx = await server.register_namespace("http://stratahub.io/opcua")
        objects = server.nodes.objects
        plant = await objects.add_object(idx, "PlantFloor")

        for key, (display_name, initial, _) in self.NODE_SCHEMA.items():
            node = await plant.add_variable(idx, display_name, initial)
            await node.set_writable()
            self._nodes[key] = node

        self._running = True
        logger.info("Mock OPC UA server running at %s", self._endpoint)
        async with server:
            sim = asyncio.create_task(self._simulate())
            try:
                await asyncio.Future()  # run forever
            finally:
                self._running = False
                sim.cancel()

    async def _simulate(self) -> None:
        t = 0
        while self._running:
            t += 1
            updates = {
                "Temperature": round(25.0 + 10.0 * math.sin(t * 0.1) + random.gauss(0, 0.3), 2),
                "Pressure": round(101.3 + 5.0 * math.cos(t * 0.05) + random.gauss(0, 0.1), 3),
                "Vibration": round(abs(0.05 + random.gauss(0, 0.015)), 4),
                "FlowRate": round(120.0 + 20.0 * math.sin(t * 0.07) + random.gauss(0, 1.0), 2),
            }
            for key, val in updates.items():
                await self._nodes[key].write_value(val)

            if t % 50 == 0:
                await self._nodes["MachineStatus"].write_value(random.choice(self.STATUSES))

            await asyncio.sleep(1.0)


if __name__ == "__main__":
    import os

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    port = int(os.getenv("OPCUA_SERVER_PORT", "4840"))
    asyncio.run(MockOpcUaServer(port=port).start())
