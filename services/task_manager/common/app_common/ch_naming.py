from __future__ import annotations

"""ClickHouse table identity for live pipelines.

    ch_unique_identifier(1, 22, "aveva_iot") -> "user_1_collection_22_aveva_iot"

**This is the live path's own implementation, by decision (Kamran, 2026-09-21).**
The batch path in `dataavalanche-be` produces the same shape, but builds it as an
inline f-string in 21 places rather than exposing anything importable, and this
repo does not reach into that codebase (CLAUDE.md). So the live path generates
its own identifier here, and this module is the single place it happens.

What still matters is the *format*: both paths must name the same table for the
same user, collection and table name, or a merged system would read and write
different places. tests/phase2/unit/test_ch_naming.py pins the composition
against the batch expression for exactly that reason. Change the format here only
with a matching change there.

Validation is reject-not-rewrite. `table_name` is customer-influenced and lands
in DDL as an identifier, so it is checked at the boundary — but silently
normalising it (lowercasing, stripping) would name a *different* table than the
same input names in batch, so anything questionable raises instead.
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
