from __future__ import annotations

"""ClickHouse table identity, shared by every live-pipeline component.

    ch_unique_identifier(1, 22, "aveva_iot") -> "user_1_collection_22_aveva_iot"

Normal (batch) pipelines in `dataavalanche-be` build the same string, but as an
inline f-string repeated 21 times across three files — there is no function there
to call:

    api/helpers/data_pipeline_helper.py:52   collection_reference = f"collection_{n}"
    app/utils/dagUtils.py:211                f"user_{user_id}_{collection_reference}"
    app/scripts/dag_generator.py             PREPEND / TGT_TBL, same expression

This module is therefore the single implementation on the live side, and the
place to extract *both* paths onto when the two codebases merge. Until then this
repo must not reach into the batch path (see CLAUDE.md), so the guard against
drift is the test suite: tests/phase2/unit/test_ch_naming.py pins this
composition against the batch expression character for character.

Validation is deliberately reject-not-rewrite. `table_name` is customer-
influenced and lands in DDL as an identifier, so it needs checking at the
boundary — but silently normalising it (lowercasing, stripping) would make the
live path name a *different* table than batch for the same input, which is
exactly the drift this is supposed to prevent. Anything batch would accept, this
accepts unchanged; anything else raises.
"""

import re

# ClickHouse unquoted identifier: letter or underscore, then alphanumerics and
# underscores. Applied to each part so the composed name is always valid.
_IDENTIFIER_PART = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# ClickHouse stores each table under a path derived from its name; keep the whole
# composed identifier comfortably inside filesystem limits.
MAX_IDENTIFIER_LENGTH = 128


def ch_unique_identifier(user_id: object, collection_number: object, table_name: str) -> str:
    """Compose the ClickHouse table name for a pipeline.

    user_id is not reformatted — batch passes a UUID in some paths and an integer
    in others, and the identifier has to match whatever batch already wrote.
    """
    user_part = str(user_id)
    if not user_part or not _IDENTIFIER_PART.match(f"u{user_part}".replace("-", "_")):
        raise ValueError(
            f"user_id {user_id!r} cannot appear in a ClickHouse identifier; "
            "expected an integer or a UUID"
        )

    if isinstance(collection_number, bool) or not isinstance(collection_number, int):
        raise ValueError(f"collection_number must be an integer, got {collection_number!r}")
    if collection_number < 1:
        raise ValueError(f"collection_number must be positive, got {collection_number!r}")

    if not _IDENTIFIER_PART.match(table_name or ""):
        raise ValueError(
            f"table_name {table_name!r} is not a valid ClickHouse identifier — "
            "expected letters, digits and underscores, not starting with a digit"
        )

    identifier = f"user_{user_part}_collection_{collection_number}_{table_name}"
    if len(identifier) > MAX_IDENTIFIER_LENGTH:
        raise ValueError(
            f"composed table name is {len(identifier)} characters, over the "
            f"{MAX_IDENTIFIER_LENGTH} limit: {identifier}"
        )
    return identifier
