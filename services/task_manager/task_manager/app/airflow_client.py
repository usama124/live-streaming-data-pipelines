from __future__ import annotations
from typing import Any
import httpx
from task_manager.app.config import settings

class AirflowClient:
    def __init__(self) -> None:
        self.base_url = settings.airflow_base_url.rstrip("/")
        self.auth     = (settings.airflow_username, settings.airflow_password)
        self.enabled  = settings.airflow_enabled

    async def trigger_dag(self, dag_id: str) -> dict[str, Any]:
        if not self.enabled:
            return {"mocked": True, "action": "trigger", "dag_id": dag_id}
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns", json={}, auth=self.auth)
            r.raise_for_status(); return r.json()

    async def pause_dag(self, dag_id: str, *, paused: bool) -> dict[str, Any]:
        if not self.enabled:
            return {"mocked": True, "action": "pause", "dag_id": dag_id, "paused": paused}
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.patch(f"{self.base_url}/api/v1/dags/{dag_id}",
                              json={"is_paused": paused}, auth=self.auth)
            r.raise_for_status(); return r.json()

    async def get_dag(self, dag_id: str) -> dict[str, Any]:
        if not self.enabled:
            return {"mocked": True, "dag_id": dag_id, "status": "unknown-local-demo"}
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(f"{self.base_url}/api/v1/dags/{dag_id}", auth=self.auth)
            r.raise_for_status(); return r.json()
