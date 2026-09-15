from __future__ import annotations

import asyncio
import random
from datetime import datetime, timezone
from typing import Any, AsyncIterator

from app.sources.base import StreamingSource


class MockStreamingSource(StreamingSource):
    """Simulates an industrial/IoT source such as OPC UA.

    It emits changing sensor values with a realistic delay between events.
    """

    def __init__(self, pipeline_id: str, min_delay_ms: int, max_delay_ms: int) -> None:
        self.pipeline_id = pipeline_id
        self.min_delay_ms = min_delay_ms
        self.max_delay_ms = max_delay_ms
        self.sequence = 0
        self.sensors = ["temperature", "pressure", "vibration", "voltage"]

    async def stream(self) -> AsyncIterator[dict[str, Any]]:
        while True:
            await asyncio.sleep(random.randint(self.min_delay_ms, self.max_delay_ms) / 1000)
            self.sequence += 1
            sensor = random.choice(self.sensors)
            value = self._generate_value(sensor)
            yield {
                "pipeline_id": self.pipeline_id,
                "event_time": datetime.now(timezone.utc).isoformat(),
                "sequence": self.sequence,
                "source": "mock",
                "sensor": sensor,
                "value": value,
                "unit": self._unit(sensor),
                "quality": random.choice(["good", "good", "good", "uncertain"]),
                "asset_id": f"machine-{random.randint(1, 5)}",
            }

    @staticmethod
    def _generate_value(sensor: str) -> float:
        ranges = {
            "temperature": (20, 120),
            "pressure": (1, 10),
            "vibration": (0, 50),
            "voltage": (210, 240),
        }
        low, high = ranges[sensor]
        return round(random.uniform(low, high), 3)

    @staticmethod
    def _unit(sensor: str) -> str:
        return {
            "temperature": "C",
            "pressure": "bar",
            "vibration": "mm/s",
            "voltage": "V",
        }[sensor]
