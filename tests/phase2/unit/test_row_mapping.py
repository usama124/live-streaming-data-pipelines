"""Registry #11 (part) — turning a Telegraf message into a row, and what happens
when it cannot be turned into one.

Telegraf's Kafka output emits {name, tags, fields, timestamp}. The row must be
built against the pipeline's *declared* schema — never from whatever keys the
first message happened to carry.
"""

from __future__ import annotations

import pytest

from common.app_common.models import PipelineConfig, PipelineType
from consumer_pool.app.rows import RecordError, to_row

CONFIG = PipelineConfig(
    pipeline_id="p1", pipeline_type=PipelineType.LIVE, topic="pipeline.p1.events",
    user_id="1", collection_number=22, table_name="aveva_iot",
)


def _message(**overrides) -> dict:
    message = {
        "name": "pipeline.p1.events",
        "timestamp": 1758445200123,
        "tags": {"pipeline_id": "p1", "sensor": "temperature", "source": "opcua",
                 "quality": "good", "node_id": "ns=2;i=2"},
        "fields": {"value": 25.125, "sequence": 3},
    }
    message.update(overrides)
    return message


def test_numeric_reading_lands_in_the_numeric_column() -> None:
    row = to_row(_message(), CONFIG)

    assert row["value"] == 25.125
    assert row["value_text"] is None
    assert row["sensor"] == "temperature"
    assert row["pipeline_id"] == "p1"
    assert row["sequence"] == 3


def test_string_reading_lands_in_the_text_column() -> None:
    """A node can read numeric or string; a column has one type."""
    row = to_row(_message(fields={"value": "RUNNING", "sequence": 9}), CONFIG)

    assert row["value"] is None
    assert row["value_text"] == "RUNNING"


def test_row_has_exactly_the_declared_columns() -> None:
    assert set(to_row(_message(), CONFIG)) == set(CONFIG.table_schema)


def test_unexpected_extra_field_does_not_widen_the_row() -> None:
    """An unexpected key must not become a column — that is the collision bug."""
    row = to_row(_message(fields={"value": 1.0, "sequence": 1, "surprise": 7}), CONFIG)

    assert "surprise" not in row
    assert set(row) == set(CONFIG.table_schema)


def test_missing_declared_column_is_filled_not_dropped() -> None:
    row = to_row(_message(tags={"pipeline_id": "p1"}), CONFIG)

    assert set(row) == set(CONFIG.table_schema)
    assert row["sensor"] == ""


def test_timestamp_becomes_event_time() -> None:
    row = to_row(_message(), CONFIG)

    assert row["event_time"].year == 2025 or row["event_time"].year == 2026
    assert row["ingested_at"] is not None


@pytest.mark.parametrize("bad", [
    {"no_timestamp": True},
    {"timestamp": "not-a-number"},
    {"timestamp": 1758445200, "fields": "not-a-dict"},
])
def test_unusable_message_raises_recorderror_for_the_dead_letter_path(bad: dict) -> None:
    """RecordError is what the write path catches per record — anything else
    escaping would stall every pipeline sharing the pool."""
    message = _message()
    message.pop("timestamp", None)
    message.update(bad)

    with pytest.raises(RecordError):
        to_row(message, CONFIG)


def test_seconds_where_milliseconds_are_expected_is_rejected_not_stored() -> None:
    """The exact bug I14 caught: Telegraf emits seconds by default, this expects
    milliseconds, and the result was a valid-looking row timestamped 1970.

    A wrong-unit timestamp must dead-letter loudly rather than quietly poison
    every staleness number downstream.
    """
    import time

    seconds_not_millis = int(time.time())

    with pytest.raises(RecordError, match="milliseconds"):
        to_row(_message(timestamp=seconds_not_millis), CONFIG)


def test_a_normal_millisecond_timestamp_still_works() -> None:
    import time

    row = to_row(_message(timestamp=int(time.time() * 1000)), CONFIG)

    assert row["event_time"].year >= 2025
