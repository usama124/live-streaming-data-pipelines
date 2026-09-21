"""ClickHouse table identity — `ch_unique_identifier`.

Mirrors what normal (batch) pipelines build in dataavalanche-be:

    collection_reference = f"collection_{n}"              # data_pipeline_helper.py:52
    prefix               = f"user_{user_id}_{collection_reference}"   # dagUtils.py:211

There is no shared function to call — that expression is inlined 21 times across
three files in that repo — so this is the single implementation on the live side,
and the extraction point when the two codebases merge. These tests pin the
composition against the batch form so the two cannot drift silently.
"""

from __future__ import annotations

import pytest

from common.app_common.ch_naming import ch_unique_identifier


def test_matches_the_documented_example() -> None:
    assert ch_unique_identifier(1, 22, "aveva_iot") == "user_1_collection_22_aveva_iot"


def test_composition_matches_the_batch_expression() -> None:
    """f"user_{user_id}_{collection_reference}_{table_name}", collection_reference
    being f"collection_{n}" — built here the same way batch builds it."""
    user_id, number, table = 7, 3, "sensor_readings"
    collection_reference = f"collection_{number}"

    assert ch_unique_identifier(user_id, number, table) == (
        f"user_{user_id}_{collection_reference}_{table}"
    )


def test_accepts_a_uuid_user_id() -> None:
    """Batch passes user.id, which is a UUID there and an int in the example given.
    Neither may be reformatted — the identifier has to match whatever batch wrote."""
    uid = "3f2b1c4d-0000-4a1b-9c2d-5e6f70819aa2"

    assert ch_unique_identifier(uid, 22, "aveva_iot") == f"user_{uid}_collection_22_aveva_iot"


def test_does_not_change_the_case_of_the_table_name() -> None:
    """Batch does no case folding. Lowercasing here would produce a different
    table than batch for the same input — that is the drift to avoid."""
    assert ch_unique_identifier(1, 2, "AVEVA_IoT") == "user_1_collection_2_AVEVA_IoT"


@pytest.mark.parametrize(
    "table_name",
    ["", "has space", "semi;colon", "quote'", "back`tick", "dash-dash", "dot.dot", "2leading"],
)
def test_rejects_table_names_that_are_not_valid_identifiers(table_name: str) -> None:
    """table_name is customer-influenced and lands in DDL as an identifier. Reject
    rather than rewrite: rewriting would silently produce a different table than
    batch for the same input."""
    with pytest.raises(ValueError):
        ch_unique_identifier(1, 2, table_name)


@pytest.mark.parametrize("collection_number", [0, -1, "x", 1.5])
def test_rejects_a_collection_number_that_is_not_a_positive_integer(collection_number) -> None:
    with pytest.raises(ValueError):
        ch_unique_identifier(1, collection_number, "t")


@pytest.mark.parametrize("user_id", ["", "has space", "quote'"])
def test_rejects_a_user_id_that_would_break_the_identifier(user_id: str) -> None:
    with pytest.raises(ValueError):
        ch_unique_identifier(user_id, 2, "t")
