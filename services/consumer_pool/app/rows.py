from __future__ import annotations

"""Telegraf message -> ClickHouse row, against the pipeline's declared schema.

Telegraf's Kafka output emits {name, tags, fields, timestamp}. The row is built
from `PipelineConfig.table_schema` and nothing else: a key the message happens to
carry but the schema does not declare is dropped, and a column the schema
declares but the message omits is filled. Widening a table to fit whatever
arrived first is the schema-collision bug this phase removes.
"""

from datetime import datetime, timezone
from typing import Any

from common.app_common.models import PipelineConfig


class RecordError(Exception):
    """This one record cannot be turned into a row.

    Caught per record by the write path and routed to dead_letter_events. Any
    other exception escaping the write path would stall every pipeline sharing
    the pool, so mapping failures must all surface as this.
    """


def _event_time(message: dict[str, Any]) -> datetime:
    raw = message.get("timestamp")
    if raw is None:
        raise RecordError("message has no timestamp")
    try:
        # Telegraf's json output is configured for milliseconds.
        return datetime.fromtimestamp(float(raw) / 1000.0, tz=timezone.utc)
    except (TypeError, ValueError, OSError, OverflowError) as exc:
        raise RecordError(f"unusable timestamp {raw!r}: {exc}") from exc


def _split_value(value: Any) -> tuple[float | None, str | None]:
    """Numeric readings to `value`, everything else to `value_text`."""
    if value is None:
        return None, None
    if isinstance(value, bool):
        return None, str(value)
    if isinstance(value, (int, float)):
        return float(value), None
    return None, str(value)


_DEFAULTS: dict[str, Any] = {"Int64": 0, "String": "", "LowCardinality(String)": ""}


def to_row(message: dict[str, Any], config: PipelineConfig) -> dict[str, Any]:
    if not isinstance(message, dict):
        raise RecordError(f"expected a JSON object, got {type(message).__name__}")

    tags = message.get("tags") or {}
    fields = message.get("fields") or {}
    if not isinstance(tags, dict) or not isinstance(fields, dict):
        raise RecordError("tags and fields must both be objects")

    event_time = _event_time(message)
    value, value_text = _split_value(fields.get("value"))
    source: dict[str, Any] = {
        **tags, **fields,
        "event_time": event_time,
        "value": value,
        "value_text": value_text,
        "ingested_at": datetime.now(timezone.utc),
        "pipeline_id": tags.get("pipeline_id", config.pipeline_id),
    }

    row: dict[str, Any] = {}
    for column, ch_type in config.table_schema.items():
        if column in source:
            row[column] = source[column]
        elif ch_type.startswith("Nullable"):
            row[column] = None
        else:
            row[column] = _DEFAULTS.get(ch_type, "")
    return row
