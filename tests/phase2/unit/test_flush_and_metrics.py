"""Registry #14 — batch flush thresholds — plus the DLQ metric.

Flush is driven by getmany(timeout_ms=flush_interval, max_records=batch_size).
Both bounds matter: a trickle must still land without waiting for a full batch,
and a burst larger than one batch must not lose its remainder.
"""

from __future__ import annotations

import pytest
import requests

from consumer_pool.app.config import Settings
from tests.phase2.conftest import (
    ch_count,
    create_pipeline,
    dlq_count,
    produce,
    telegraf_message,
    unique_id,
    wait_for,
)

pytestmark = pytest.mark.usefixtures("platform")

METRICS = "http://localhost:9100/metrics"


def test_defaults_are_a_bounded_batch_and_a_bounded_wait() -> None:
    settings = Settings()

    assert 0 < settings.batch_size <= 100_000
    assert 0 < settings.flush_interval_seconds <= 60, (
        "an unbounded flush interval means a low-rate pipeline's data sits in "
        "memory indefinitely"
    )


def test_a_trickle_lands_without_waiting_for_a_full_batch() -> None:
    """Three records, far below batch_size — the time threshold must flush them."""
    pipeline_id = unique_id("p14t")
    table = create_pipeline(pipeline_id, collection_number=14, table_name="trickle")["table"]

    produce(f"pipeline.{pipeline_id}.events",
            [telegraf_message(pipeline_id, sequence=i) for i in range(3)])

    wait_for(lambda: ch_count(table) >= 3, timeout=90,
             what="3 records flushed on the time threshold, not held for a full batch")


def test_a_burst_larger_than_one_batch_keeps_its_remainder() -> None:
    pipeline_id = unique_id("p14b")
    table = create_pipeline(pipeline_id, collection_number=14, table_name="burst")["table"]
    count = Settings().batch_size + 50

    produce(f"pipeline.{pipeline_id}.events",
            [telegraf_message(pipeline_id, sequence=i) for i in range(count)])

    wait_for(lambda: ch_count(table) >= count, timeout=180,
             what=f"all {count} records landed across multiple batches")


def test_dlq_writes_increment_a_prometheus_counter_tagged_by_pipeline() -> None:
    pipeline_id = unique_id("p14m")
    create_pipeline(pipeline_id, collection_number=14, table_name="metric_case")

    produce(f"pipeline.{pipeline_id}.events", [b"not json", b"still not json"])
    wait_for(lambda: dlq_count(pipeline_id) >= 2, timeout=120, what="2 dead letters")

    body = requests.get(METRICS, timeout=30).text
    matching = [
        line for line in body.splitlines()
        if line.startswith("dlq_rows_total") and pipeline_id in line
    ]
    assert matching, f"no dlq_rows_total series for {pipeline_id}\n{body[:1500]}"
    assert float(matching[0].rsplit(" ", 1)[1]) >= 2, matching
