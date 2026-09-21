"""Mock OPC UA server for local development and CI.

Moved unchanged (bar a configurable bind host) out of
services/producer_service/app/sources/opcua_source.py, which Phase 1 deletes.
This is dev/test infrastructure, not part of the producer.

    python -m connectors.opcua.mock_server
    OPCUA_SERVER_PORT=4841 python -m connectors.opcua.mock_server
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import random
from typing import Any

logger = logging.getLogger("mock-opcua-server")


class MockOpcUaServer:
    """Exposes 5 nodes under PlantFloor and streams realistic simulated values.

    Nodes are registered in declaration order, so they land on ns=2;i=2 .. ns=2;i=6.
    """

    # key → (display_name, initial_value, unit)
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
        from asyncua import Server

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

            await asyncio.sleep(0.5)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # asyncua logs every standard-address-space node at INFO — hundreds of lines
    # before the server is even up. Useless here, and enough output to fill a
    # pipe buffer and deadlock a caller that is not draining it.
    logging.getLogger("asyncua").setLevel(logging.WARNING)
    asyncio.run(
        MockOpcUaServer(
            host=os.getenv("OPCUA_SERVER_HOST", "0.0.0.0"),  # noqa: S104
            port=int(os.getenv("OPCUA_SERVER_PORT", "4840")),
        ).start()
    )
