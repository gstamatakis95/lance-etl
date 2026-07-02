"""Column-role metadata stored in each Lance dataset's own config KV.

Every concrete column the ETL pivot creates originates from one of the three source map
columns, and that origin is the column's role: keys pivoted from ``vectors`` are ``"vector"``
columns, keys from ``texts`` are ``"text"`` columns (the BM25 full-text targets), and keys from
``metadata`` are ``"scalar"`` columns. The roles are persisted under the single config key
:data:`COLUMN_ROLES_KEY` as a JSON object mapping column name to role, using the same
``update_config`` mechanism as the vector artifact config (ADR 0025), so the metadata survives
compaction and version cleanup and costs no extra object-store I/O to read from an open handle.

The mapping is grow-only, mirroring the grow-only dataset schema: merges add newly pivoted
columns and never remove or reassign existing entries, which makes concurrent and replayed
writes idempotent. Downstream consumers use the roles to choose which index each column gets
(vector columns get IVF_RQ, text columns get BM25 INVERTED) and to drive role-aware casts.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable

import lance

from lance_etl.telemetry import commit_with_retries

logger: logging.Logger = logging.getLogger(__name__)

COLUMN_ROLES_KEY: str = "lance-etl.columns"
"""Dataset config key holding the JSON object that maps column name to role."""

VECTOR_ROLE: str = "vector"
"""Role of columns pivoted from the ``vectors`` map, indexed with IVF_RQ."""

TEXT_ROLE: str = "text"
"""Role of columns pivoted from the ``texts`` map, indexed with BM25 INVERTED."""

SCALAR_ROLE: str = "scalar"
"""Role of columns pivoted from the ``metadata`` map."""


def load_column_roles(dataset: lance.LanceDataset) -> dict[str, str]:
    """Read the column-role mapping from an open dataset's config KV.

    The config KV is already in memory from the open manifest, so this performs no additional
    object-store I/O. A missing key or a malformed value yields an empty mapping, with a warning
    for the malformed case, so callers can treat pre-role datasets and healthy datasets
    uniformly.

    Args:
        dataset: The open dataset whose roles to read.

    Returns:
        The stored column-to-role mapping, or an empty dict when absent or unparseable.
    """
    raw: str | None = dataset.config().get(COLUMN_ROLES_KEY)
    if raw is None:
        return {}
    try:
        parsed: dict[str, str] = json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        logger.warning("malformed column roles on %s; treating as absent", dataset.uri)
        return {}
    return {name: role for name, role in parsed.items() if isinstance(name, str) and isinstance(role, str)}


def merge_column_roles(
    uri: str,
    roles: dict[str, str],
    storage_options: dict[str, object] | None,
    retries: int,
    backoff_seconds: float,
    on_conflict: Callable[[], None] | None = None,
) -> None:
    """Merge new column roles into the dataset's stored mapping, grow-only and idempotent.

    Each retry re-opens the dataset at the latest version, unions the stored mapping with the
    supplied roles (stored entries win so a role is never reassigned), and writes back through
    ``update_config`` only when the union adds at least one new column. Replaying the same roles
    therefore converges without extra commits, and concurrent writers adding disjoint columns
    both land after the conflict retry.

    Args:
        uri: Dataset URI.
        roles: Column-to-role entries to add.
        storage_options: Object-store options forwarded to lance.
        retries: Commit-conflict retry budget.
        backoff_seconds: Base backoff between retries.
        on_conflict: Optional callback invoked once per conflict retry.

    Raises:
        OSError | RuntimeError: If commits keep conflicting past the retry budget.
    """
    if not roles:
        return

    def action() -> None:
        """Union the stored roles with the new entries at the latest version."""
        dataset: lance.LanceDataset = lance.dataset(uri, storage_options=storage_options)
        stored: dict[str, str] = load_column_roles(dataset)
        merged: dict[str, str] = {**roles, **stored}
        if merged != stored:
            dataset.update_config({COLUMN_ROLES_KEY: json.dumps(merged, sort_keys=True)})

    commit_with_retries(action, retries, backoff_seconds, on_conflict)
