"""Registry #21, #22, #24 — the monitoring numbers, checked against known traffic.

#23 (reconstructable OpenTelemetry trace) is not here: Phase 4 was scoped to the
plan's timestamp-based interim instead of full tracing, because Telegraf sits
mid-path and will not propagate trace context. See the plan's Phase 4 section.
#25 (per-user health API) is not here either — the plan calls for designing it,
not building it, so there is nothing to test yet.
"""

from __future__ import annotations

import time

import pytest

from tests.conftest import unique_table
from tests.phase2.conftest import (
    ch_count,
    create_pipeline,
    produce,
    telegraf_message,
    unique_id,
)
from tests.phase4.conftest import (
    kafka_ui_consumer_group,
    kafka_ui_topic,
    prometheus_targets,
    promql_value,
    wait_for,
)

pytestmark = pytest.mark.usefixtures("monitoring")


def test_21_kafka_ui_reports_the_traffic_that_was_actually_sent() -> None:
    """Not 'the dashboard renders' — the message count has to match what we sent."""
    pipeline_id = unique_id("p21")
    table = create_pipeline(pipeline_id, collection_number=21, table_name=unique_table("kafka_ui"))["table"]
    topic = f"pipeline.{pipeline_id}.events"
    sent = 25

    produce(topic, [telegraf_message(pipeline_id, sequence=i) for i in range(sent)])
    wait_for(lambda: ch_count(table) >= sent, what="records consumed")

    reported = wait_for(lambda: kafka_ui_topic(topic), what=f"{topic} to appear in Kafka UI")
    offsets = sum(p["offsetMax"] - p["offsetMin"] for p in reported["partitions"])
    assert offsets == sent, f"Kafka UI reports {offsets} messages, {sent} were sent"

    group = wait_for(lambda: kafka_ui_consumer_group(), what="the pool's consumer group")
    assert group["state"] in {"STABLE", "PREPARING_REBALANCE", "COMPLETING_REBALANCE"}, group["state"]
    # The pool has consumed everything, so lag on this topic should settle at 0.
    # `topics` on this payload is a count, not a list — per-partition lag lives
    # in `partitions`.
    def _lag_settled() -> bool:
        group = kafka_ui_consumer_group() or {}
        ours = [p for p in group.get("partitions", []) if p.get("topic") == topic]
        return bool(ours) and all(p.get("consumerLag") == 0 for p in ours)

    wait_for(_lag_settled, what="Kafka UI to report zero lag for this topic")


def test_22_prometheus_scrapes_every_expected_target() -> None:
    """A scrape target silently down is itself the bug this catches."""
    pipeline_id = unique_id("p22")
    create_pipeline(pipeline_id, collection_number=22, table_name=unique_table("scrape"))
    produce(f"pipeline.{pipeline_id}.events", [telegraf_message(pipeline_id)])

    targets = wait_for(lambda: prometheus_targets() or None, what="Prometheus targets")

    for job in ("consumer-pool", "clickhouse"):
        assert job in targets, f"no {job} target at all; jobs = {sorted(targets)}"
        unhealthy = [t for t in targets[job] if t["health"] != "up"]
        assert not unhealthy, f"{job} target down: {unhealthy[0].get('lastError')}"


def test_24_stale_data_is_visible_even_though_kafka_lag_is_zero() -> None:
    """The case plain lag-based health checks miss.

    Everything produced has been consumed, so lag is zero and every
    liveness-style check says healthy — while the newest *reading* is an hour
    old because the source stopped publishing.
    """
    pipeline_id = unique_id("p24")
    table = create_pipeline(pipeline_id, collection_number=24, table_name=unique_table("stale_case"))["table"]
    an_hour_ago_ms = int((time.time() - 3600) * 1000)

    produce(f"pipeline.{pipeline_id}.events",
            [telegraf_message(pipeline_id, sequence=i, timestamp_ms=an_hour_ago_ms)
             for i in range(5)])

    wait_for(lambda: ch_count(table) >= 5, what="the stale records to be consumed")

    staleness = wait_for(
        lambda: promql_value(
            f'time() - pipeline_last_event_timestamp_seconds{{pipeline_id="{pipeline_id}"}}'
        ),
        what="the staleness metric to appear in Prometheus",
    )
    assert staleness > 3000, (
        f"staleness reported as {staleness}s for data an hour old — the metric is "
        "not tracking source time"
    )

    # And the thing that would have said "healthy": the pool is fully caught up.
    lag = promql_value(
        f'time() - pipeline_last_write_timestamp_seconds{{pipeline_id="{pipeline_id}"}}')
    assert lag is not None and lag < 300, (
        "the pool itself is up to date, which is exactly why lag-based checks "
        f"miss this (last write {lag}s ago)"
    )


def test_24b_the_staleness_alert_rule_is_loaded() -> None:
    """A metric nobody alerts on is a metric nobody sees."""
    import requests

    from tests.phase4.conftest import PROMETHEUS

    rules = requests.get(f"{PROMETHEUS}/api/v1/rules", timeout=30).json()["data"]["groups"]
    names = {rule["name"] for group in rules for rule in group["rules"]}

    assert "PipelineDataStale" in names, names
    assert "PipelineDeadLettering" in names, names
