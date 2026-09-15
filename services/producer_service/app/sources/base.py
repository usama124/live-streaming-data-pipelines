from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, AsyncIterator


class StreamingSource(ABC):
    @abstractmethod
    async def stream(self) -> AsyncIterator[dict[str, Any]]:
        """Yield real-time events."""
