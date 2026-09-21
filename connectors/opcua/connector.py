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
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

from asyncua import Client, Node

logger = logging.getLogger("opcua-connector")


class _Emitter:
    """Writes one JSON record per data-change notification to stdout.

    Printing straight from the asyncua callback is intentional: if the reader
    (Telegraf) is slow, the write blocks and back-pressures the subscription.
    The version this replaces buffered into a bounded queue and dropped records
    on overflow — silent data loss is worse than back-pressure.
    """

    def __init__(self, pipeline_id: str, node_names: dict[str, str], out: Any) -> None:
        self._pipeline_id = pipeline_id
        self._node_names = node_names
        self._out = out
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
    }


async def run(cfg: dict[str, Any]) -> None:
    client = Client(url=cfg["endpoint"])
    await client.connect()
    logger.info("connected to %s", cfg["endpoint"])

    try:
        emitter = _Emitter(cfg["pipeline_id"], cfg["node_names"], sys.stdout)
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
