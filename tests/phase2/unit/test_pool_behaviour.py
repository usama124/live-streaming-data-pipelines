"""Registry #8, #9, #11, #12, #13 — the shared pool against real Kafka and ClickHouse.

These are the tests the phase exists for. A pool that mocks either end proves
nothing about the two failures this design is most exposed to: one pipeline's
records landing in another's table, and one bad record stalling everyone.
"""

from __future__ import annotations

import pytest

from tests.phase2.conftest import (
    ch_count,
    ch_query,
    create_pipeline,
    dlq_count,
    produce,
    restart_pool,
    telegraf_message,
    unique_id,
    wait_for,
)

pytestmark = pytest.mark.usefixtures("platform")


def test_8_new_pipeline_is_picked_up_with_no_pool_restart() -> None:
    """The core SaaS constraint: customers create pipelines whenever they like,
    and shared infrastructure may not be bounced to notice."""
    pipeline_id = unique_id("p8")
    created = create_pipeline(pipeline_id, collection_number=8, table_name="picked_up")
    table = created["table"]

    # Deliberately no restart of the pool between creation and producing.
    produce(f"pipeline.{pipeline_id}.events",
            [telegraf_message(pipeline_id, sequence=i) for i in range(5)])

    wait_for(lambda: ch_count(table) >= 5, timeout=120,
             what=f"5 rows in {table} without restarting the pool")


def test_9_two_shapes_land_in_their_own_tables() -> None:
    """Direct regression test for the schema-collision bug: two pipelines with
    different columns, each in its own table, neither widening the other."""
    id_a, id_b = unique_id("p9a"), unique_id("p9b")
    schema_b = {
        "event_time": "DateTime64(3)",
        "pipeline_id": "LowCardinality(String)",
        "value": "Nullable(Float64)",
        "value_text": "Nullable(String)",
        "ingested_at": "DateTime64(3)",
    }
    table_a = create_pipeline(id_a, collection_number=9, table_name="shape_a")["table"]
    table_b = create_pipeline(id_b, collection_number=9, table_name="shape_b",
                              table_schema=schema_b)["table"]

    produce(f"pipeline.{id_a}.events", [telegraf_message(id_a, value=1.5, sequence=i)
                                        for i in range(5)])
    produce(f"pipeline.{id_b}.events", [telegraf_message(id_b, value=2.5, sequence=i)
                                        for i in range(5)])

    wait_for(lambda: ch_count(table_a) >= 5 and ch_count(table_b) >= 5, timeout=120,
             what="both tables populated")

    assert table_a != table_b
    # Neither table saw the other's pipeline_id.
    assert ch_count(table_a, f"pipeline_id = '{id_b}'") == 0
    assert ch_count(table_b, f"pipeline_id = '{id_a}'") == 0

    # B declared fewer columns; its table must not have grown A's.
    columns_b = {row[0] for row in ch_query(
        f"SELECT name FROM system.columns WHERE table = '{table_b}'")}
    assert "sensor" not in columns_b, f"table B was widened beyond its declared schema: {columns_b}"


def test_11_bad_record_dead_letters_without_stalling_the_pipeline() -> None:
    """One unusable record must not take the batch, the pipeline, or the pool
    down with it."""
    pipeline_id = unique_id("p11")
    table = create_pipeline(pipeline_id, collection_number=11, table_name="dlq_case")["table"]
    topic = f"pipeline.{pipeline_id}.events"

    good_before = [telegraf_message(pipeline_id, sequence=i) for i in range(3)]
    poison = [
        b"this is not json at all",
        {"no_timestamp": True, "tags": {}, "fields": {}},
        {"timestamp": "not-a-number", "tags": {}, "fields": {}},
    ]
    good_after = [telegraf_message(pipeline_id, sequence=100 + i) for i in range(3)]

    produce(topic, [*good_before, *poison, *good_after])

    wait_for(lambda: ch_count(table) >= 6, timeout=120,
             what="all 6 good rows landed despite the poison between them")
    wait_for(lambda: dlq_count(pipeline_id) >= 3, timeout=120,
             what="3 dead letters recorded")

    dead = ch_query(
        "SELECT topic, error_message, payload FROM dead_letter_events "
        f"WHERE pipeline_id = '{pipeline_id}' LIMIT 3"
    )
    for row_topic, error_message, payload in dead:
        assert row_topic == topic
        assert error_message, "a dead letter with no reason is not diagnosable"
        assert payload, "the original record must be kept for replay"


def test_11b_one_pipelines_poison_does_not_block_another() -> None:
    """The shared-pool risk: a bad record on one topic stalling every other."""
    bad_id, good_id = unique_id("p11bad"), unique_id("p11good")
    create_pipeline(bad_id, collection_number=11, table_name="poisoned")
    good_table = create_pipeline(good_id, collection_number=11, table_name="unaffected")["table"]

    produce(f"pipeline.{bad_id}.events", [b"{{{ not json", b"also not json"])
    produce(f"pipeline.{good_id}.events",
            [telegraf_message(good_id, sequence=i) for i in range(5)])

    wait_for(lambda: ch_count(good_table) >= 5, timeout=120,
             what="the healthy pipeline kept flowing while the other dead-lettered")


def test_12_pool_restart_resumes_from_the_last_committed_offset() -> None:
    """No loss. Duplicates are allowed — this is at-least-once by design — but
    must stay bounded rather than replaying the topic from the start."""
    pipeline_id = unique_id("p12")
    table = create_pipeline(pipeline_id, collection_number=12, table_name="restart_case")["table"]
    topic = f"pipeline.{pipeline_id}.events"

    produce(topic, [telegraf_message(pipeline_id, sequence=i) for i in range(10)])
    wait_for(lambda: ch_count(table) >= 10, timeout=120, what="first 10 rows")

    restart_pool()

    produce(topic, [telegraf_message(pipeline_id, sequence=100 + i) for i in range(10)])
    wait_for(lambda: ch_count(table, "sequence >= 100") >= 10, timeout=180,
             what="10 more rows after the restart")

    total = ch_count(table)
    assert total >= 20, f"rows were lost across the restart: {total}"
    assert total < 40, f"the whole topic was replayed — offsets were not committed: {total}"


def test_13_topic_to_table_mapping_holds_under_load() -> None:
    """Several pipelines producing at once; nothing may cross over."""
    pipelines = {unique_id(f"p13{i}"): f"load_{i}" for i in range(4)}
    tables = {
        pid: create_pipeline(pid, collection_number=13, table_name=name)["table"]
        for pid, name in pipelines.items()
    }

    for pid in pipelines:
        produce(f"pipeline.{pid}.events",
                [telegraf_message(pid, sequence=i) for i in range(25)])

    wait_for(lambda: all(ch_count(t) >= 25 for t in tables.values()), timeout=180,
             what="every pipeline's rows landed")

    for pid, table in tables.items():
        foreign = ch_count(table, f"pipeline_id != '{pid}'")
        assert foreign == 0, f"{table} holds {foreign} rows belonging to another pipeline"
