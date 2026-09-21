"""OPC UA source connector for Telegraf's `inputs.execd`.

Its only job: connect to an OPC UA server, subscribe, and emit one JSON record
per line on stdout. Telegraf owns everything downstream — batching, the Kafka
producer, retry/backoff, metrics. Do not add any of that here; see
`docs/ONBOARDING.md` §5.

Reconnect is deliberately *not* implemented as an internal backoff loop. On any
connection or session failure this process logs to stderr and exits non-zero;
Telegraf restarts it after `restart_delay`. That keeps the failure visible in
Telegraf's own logs and guarantees the process never sits alive-but-silent,
which is the one failure mode Telegraf cannot detect (§3.1's known gap).

Config, all from the environment:
    PIPELINE_ID                   pipeline this connector belongs to
    OPCUA_ENDPOINT                opc.tcp://host:4840/path
    OPCUA_NODE_IDS                comma-separated NodeIDs, e.g. "ns=2;i=2,ns=2;i=3"
    OPCUA_NODE_NAMES_JSON         optional {"ns=2;i=2": "Temperature"} mapping
    OPCUA_PUBLISHING_INTERVAL_MS  server publish interval (default 500)
    OPCUA_CONNECTION_CHECK_S      liveness poll interval (default 5)
    OPCUA_CONNECT_TIMEOUT_S       handshake/request timeout (default 4). Raise it
                                  for a slow industrial link — a server that
                                  accepts TCP but answers slowly is common.
    HEALTH_PORT                   serve /healthz on this port (unset = off)
    OPCUA_STALENESS_THRESHOLD_S   /healthz fails after this long with no data
                                  (default 60). See the note on static sensors
                                  in docs/ARCHITECTURE.md §3.4.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

from asyncua import Client, Node

logger = logging.getLogger("opcua-connector")


class _LastRead:
    """When the source last gave us data. Read by the health endpoint.

    Deliberately *not* updated by connection checks: a hung source still answers
    a status read, so a probe driven by connection liveness would call a stalled
    pipeline healthy — which is the exact gap this exists to close.
    """

    def __init__(self) -> None:
        self._at = time.monotonic()
        self._lock = threading.Lock()

    def touch(self) -> None:
        with self._lock:
            self._at = time.monotonic()

    def age_s(self) -> float:
        with self._lock:
            return time.monotonic() - self._at


def serve_health(port: int, last_read: _LastRead, threshold_s: float) -> None:
    """503 once data has not arrived for `threshold_s`, so kubelet recycles us.

    The connector does not exit on staleness: whether a stalled pod is restarted
    is the orchestrator's decision, and Compose-based local dev has no probe.
    """

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            age = last_read.age_s()
            healthy = age <= threshold_s
            body = json.dumps({
                "status": "ok" if healthy else "stale",
                "seconds_since_last_read": round(age, 1),
                "threshold_seconds": threshold_s,
            }).encode()
            self.send_response(200 if healthy else 503)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_: Any) -> None:
            pass  # kubelet probes every few seconds; do not narrate it

    server = HTTPServer(("0.0.0.0", port), Handler)  # noqa: S104
    threading.Thread(target=server.serve_forever, daemon=True, name="health").start()
    logger.info("health endpoint on :%d (staleness threshold %.0fs)", port, threshold_s)


class _Emitter:
    """Writes one JSON record per data-change notification to stdout.

    Printing straight from the asyncua callback is intentional: if the reader
    (Telegraf) is slow, the write blocks and back-pressures the subscription.
    The version this replaces buffered into a bounded queue and dropped records
    on overflow — silent data loss is worse than back-pressure.
    """

    def __init__(self, pipeline_id: str, node_names: dict[str, str], out: Any,
                 last_read: "_LastRead | None" = None) -> None:
        self._pipeline_id = pipeline_id
        self._node_names = node_names
        self._out = out
        self._last_read = last_read
        self._sequence = 0

    def datachange_notification(self, node: Node, val: Any, data: Any) -> None:
        try:
            node_id = node.nodeid.to_string()
            source_ts = data.monitored_item.Value.SourceTimestamp
            status = data.monitored_item.Value.StatusCode

            self._sequence += 1
            record = {
                "pipeline_id": self._pipeline_id,
                "event_time": (
                    source_ts.replace(tzinfo=timezone.utc).isoformat()
                    if source_ts
                    else datetime.now(timezone.utc).isoformat()
                ),
                "sequence": self._sequence,
                "source": "opcua",
                "sensor": self._node_names.get(node_id, node_id).lower(),
                "node_id": node_id,
                "value": val,
                "quality": (
                    "good" if status.is_good else "uncertain" if status.is_uncertain else "bad"
                ),
            }
            print(json.dumps(record), file=self._out, flush=True)
            if self._last_read is not None:
                self._last_read.touch()
        except Exception:
            # Never let a bad notification kill the subscription — one unreadable
            # node must not stop the other four.
            logger.exception("dropping malformed notification from %s", node)


def _config() -> dict[str, Any]:
    endpoint = os.getenv("OPCUA_ENDPOINT", "")
    node_ids = [n.strip() for n in os.getenv("OPCUA_NODE_IDS", "").split(",") if n.strip()]
    if not endpoint:
        raise SystemExit("OPCUA_ENDPOINT is required")
    if not node_ids:
        raise SystemExit("OPCUA_NODE_IDS is required, e.g. OPCUA_NODE_IDS=ns=2;i=2,ns=2;i=3")

    try:
        node_names = json.loads(os.getenv("OPCUA_NODE_NAMES_JSON") or "{}")
    except json.JSONDecodeError as exc:
        raise SystemExit(f"OPCUA_NODE_NAMES_JSON is not valid JSON: {exc}") from exc

    return {
        "pipeline_id": os.getenv("PIPELINE_ID", "unknown"),
        "endpoint": endpoint,
        "node_ids": node_ids,
        "node_names": node_names,
        "publishing_interval_ms": int(os.getenv("OPCUA_PUBLISHING_INTERVAL_MS", "500")),
        "connection_check_s": float(os.getenv("OPCUA_CONNECTION_CHECK_S", "5")),
        "connect_timeout_s": float(os.getenv("OPCUA_CONNECT_TIMEOUT_S", "4")),
        "health_port": int(os.getenv("HEALTH_PORT", "0")),
        "staleness_threshold_s": float(os.getenv("OPCUA_STALENESS_THRESHOLD_S", "60")),
    }


async def run(cfg: dict[str, Any]) -> None:
    # timeout bounds the handshake: without it, an endpoint that accepts TCP and
    # then says nothing leaves this process alive and silent forever.
    client = Client(url=cfg["endpoint"], timeout=cfg["connect_timeout_s"])
    await client.connect()
    logger.info("connected to %s", cfg["endpoint"])

    try:
        last_read = _LastRead()
        if cfg["health_port"]:
            serve_health(cfg["health_port"], last_read, cfg["staleness_threshold_s"])

        emitter = _Emitter(cfg["pipeline_id"], cfg["node_names"], sys.stdout, last_read)
        subscription = await client.create_subscription(
            period=cfg["publishing_interval_ms"], handler=emitter
        )
        await subscription.subscribe_data_change(
            [client.get_node(nid) for nid in cfg["node_ids"]]
        )
        logger.info(
            "subscribed to %d nodes (interval=%dms)",
            len(cfg["node_ids"]), cfg["publishing_interval_ms"],
        )

        # Without this, a dropped session leaves the process alive and silent —
        # exactly what must never happen. check_connection() raises when the
        # session is gone, which exits non-zero and lets Telegraf restart us.
        while True:
            await asyncio.sleep(cfg["connection_check_s"])
            await client.check_connection()
    finally:
        try:
            await client.disconnect()
        except Exception:
            logger.debug("error during disconnect", exc_info=True)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, stream=sys.stderr,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logging.getLogger("asyncua").setLevel(logging.WARNING)

    cfg = _config()
    try:
        asyncio.run(run(cfg))
    except KeyboardInterrupt:
        return 0
    except Exception as exc:
        # The clear log line the operator needs: which endpoint, what failed.
        logger.error("OPC UA connector failed for %s: %s: %s",
                     cfg["endpoint"], type(exc).__name__, exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
