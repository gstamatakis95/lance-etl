"""The Lance sink: content-routed, idempotent merge of pivoted change rows into per-tenant datasets.

This module is the write seam of the ingestion path, the counterpart of the Iceberg source scan in
:mod:`lance_etl.source`. It deliberately is not a Spark
DataSourceV2 connector: a DSv2 write targets one table per write, while this sink routes each
row group to one of thousands of per-tenant datasets chosen by content, evolves each dataset's
schema independently, and applies last-write-wins merge conditions. Executor closures calling
these functions are the correct pattern for that shape.

Sink contract:

- Idempotent LWW upsert keyed by :data:`~lance_etl.etl.pivot.KEY_COL` with
  ``when_matched_update_all`` guarded by ``source.ts >= target.ts``, plus physical deletes via
  ``when_matched_delete``. Replaying a window converges to the same dataset state.
- Chunked commits (``config.merge_batch_bytes``) that are order-safe because the caller's
  collapse guarantees at most one row per key per increment.
- Grow-only schema evolution through ``add_columns``, with each new column's role (vector, text,
  or scalar) persisted into the dataset's config KV under ``lance-etl.columns``.
- New datasets are bootstrapped with V2 manifest paths and the Lance file format from
  :data:`DATA_STORAGE_VERSION` (``"2.1"``).
- Every object-store behavior (credentials, endpoints, timeouts, retries) flows through
  ``config.storage_options``, which is forwarded verbatim to every ``lance.dataset`` and
  ``lance.write_dataset`` call. Commit conflicts are retried with
  :func:`lance_etl.telemetry.commit_with_retries` on top of lance's own inner retry loop.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import lance
import pyarrow as pa
import pyarrow.compute as pc

from lance_etl.column_roles import merge_column_roles
from lance_etl.etl.pivot import (
    DELETE_OP_VALUES,
    KEY_COL,
    OP_COL,
    ETLConfig,
    pivot_map_columns,
)
from lance_etl.telemetry import DEFAULT_RETRY_TIMEOUT, Telemetry, commit_with_retries

logger: logging.Logger = logging.getLogger(__name__)

DATA_STORAGE_VERSION: str = "2.1"
"""Lance file format version for newly created datasets, never varied.

Adopts the latest stable format with structural encodings. Existing datasets keep the format they
were created with, and lance reads both transparently.
"""


DATASET_NOT_FOUND_MARKER: str = "was not found"
"""Substring lance renders into the load error for a genuinely absent dataset (lance 8.0.0).

pylance maps every dataset-load failure to :class:`ValueError`, so credentials errors, object-store
throttling (S3 503), and corrupt manifests are indistinguishable from a missing dataset by type
alone. The absent-dataset message reads ``Dataset at path <uri> was not found: ...``; matching this
marker lets the sink treat a true absence as a no-op while re-raising every transient or fatal open
error instead of silently skipping a compliance-sensitive delete.
"""


def dataset_absent(error: BaseException) -> bool:
    """Return True only when an open error signals a genuinely absent dataset.

    A :class:`FileNotFoundError` is an unambiguous absence. A :class:`ValueError` is absence only
    when it carries the lance not-found marker (:data:`DATASET_NOT_FOUND_MARKER`); every other
    ``ValueError`` (transient object-store fault, bad credentials, corrupt manifest) is a real
    failure the caller must re-raise rather than treat as a no-op.

    Args:
        error: The exception raised while opening a dataset.

    Returns:
        True when the error means the dataset does not exist, False otherwise.
    """
    if isinstance(error, FileNotFoundError):
        return True
    return isinstance(error, ValueError) and DATASET_NOT_FOUND_MARKER in str(error)


def dataset_uri(config: ETLConfig, *components: str) -> str:
    """Build the validated dataset URI ``base_uri/org_id/tenant_id/namespace.lance``.

    Args:
        config: ETL configuration.
        *components: One routing value per column in :data:`ROUTING_COLS`, in path order.

    Returns:
        The dataset URI for the given routing key.

    Raises:
        ValueError: If any component is not a non-empty string.
    """
    for component in components:
        if not isinstance(component, str) or not component:
            raise ValueError(f"invalid routing component: {component!r}")
    base: str = config.base_uri.rstrip("/")
    return f"{base}/{'/'.join(components)}.lance"


def table_chunks(table: pa.Table, batch_bytes: int | None) -> list[pa.Table]:
    """Slice a PyArrow table into zero-copy chunks whose source byte budget does not exceed ``batch_bytes``.

    The rows-per-chunk is derived from the table's actual mean row width so that any dtype — uint8,
    float32, float64 — stays within the budget without requiring manual tuning. When ``batch_bytes``
    is None or the whole table is already within the budget, returns a single-element list containing
    the original table so the caller can use the same loop for both cases.

    Args:
        table: The table to slice.
        batch_bytes: Source byte budget per chunk. None means no chunking.

    Returns:
        Ordered list of table slices covering all rows.
    """
    if batch_bytes is None or table.nbytes <= batch_bytes:
        return [table]
    bytes_per_row: int = max(1, table.nbytes // table.num_rows)
    rows_per_chunk: int = max(1, batch_bytes // bytes_per_row)
    offsets: list[int] = list(range(0, table.num_rows, rows_per_chunk))
    return [table.slice(offset, min(rows_per_chunk, table.num_rows - offset)) for offset in offsets]


def build_update_condition(ts_col: str) -> str:
    """Build the SQL condition that guards cross-window out-of-order updates.

    The condition ``source.{ts_col} >= target.{ts_col}`` ensures that a source row only overwrites
    a target row when the source timestamp is greater than or equal to the target timestamp,
    enforcing last-write-wins semantics across ETL windows.

    Tie semantics: ties (source.ts == target.ts) apply the update. This preserves idempotency
    when the same ETL window is replayed — applying the same window twice must converge to the
    same result, so equal timestamps must not block the update.

    NULL semantics for target: SQL ``x >= NULL`` evaluates to NULL, treated as FALSE by the Lance
    executor. A target row that carries a NULL ts cannot be updated by any source row. This is an
    accepted constraint: rows written before the ts column was added to the schema retain NULL and
    are skipped by the guard. Such rows can only be overwritten by a schema migration or a direct
    delete-and-reinsert. The alternative (COALESCE with an epoch literal) is not supported in the
    current lance version because the ``target.`` table-qualifier cannot appear inside function
    arguments in the DataFusion condition planner.

    NULL semantics for source: ``NULL >= target.ts`` evaluates to NULL (FALSE), so a source row
    carrying a NULL ts will never overwrite an existing target row. ``collapse`` orders NULL
    timestamps NULLS LAST within a window, treating them as the oldest event — consistent with the
    cross-window guard rejecting NULL-ts source rows. Callers must ensure ``ts_col`` is non-NULL in
    all source rows when the guard is active.

    Delete semantics: ``when_matched_delete`` does not accept a condition parameter in the Lance
    7.x API, so cross-window stale deletes (delete ts older than stored row ts) cannot be blocked
    at the Lance level. Within a window, ``collapse`` selects the terminal op per key, which
    mitigates in-window stale deletes. Cross-window stale deletes remain a known gap pending a
    future Lance API extension.

    Args:
        ts_col: Name of the timestamp column in both source and target.

    Returns:
        An SQL condition string suitable for ``when_matched_update_all(condition=...)``.
    """
    return f"source.{ts_col} >= target.{ts_col}"


def apply_merge(config: ETLConfig, telemetry: Telemetry, key: tuple[str, ...], group: pa.Table) -> tuple[int, int]:
    """Pivot, cast, and merge one dataset group via upsert and physical delete.

    Pivots map columns, bootstraps a new dataset with V2 manifest paths and the
    configured Lance file format when absent (concurrent-bootstrap race caught with OSError
    fallback), evolves schema via ``add_columns`` when new keys appear (idempotent on retry),
    then runs ``merge_insert`` with ``when_matched_update_all(condition)``. Physical deletes use
    ``when_matched_delete()`` on a key-only table. Both paths go through
    :func:`commit_with_retries` with ``on_conflict`` incrementing
    ``dataset.merge_conflict_retries``. After a successful upsert phase the pivoted columns'
    roles are merged into the dataset's ``lance-etl.columns`` config entry, a grow-only,
    idempotent write that no-ops when every column is already recorded.

    Cross-window last-write-wins guard: when ``config.ts_col`` is present in the upsert table, the
    update condition ``source.{ts_col} >= target.{ts_col}`` prevents a later ETL batch carrying an
    older timestamp from silently overwriting a newer stored value. Without this guard, ``collapse``
    enforces last-write-wins only within a single window; across windows, arrival order would
    determine the winner instead of timestamp order.

    Ties (equal timestamps) still apply the update, preserving idempotency: replaying the same
    window twice converges to the same result. NULL source or target timestamps are never updated
    (SQL ``NULL >= x`` and ``x >= NULL`` both evaluate to NULL, treated as FALSE). See
    :func:`build_update_condition` for the full NULL and delete semantics.

    Delete guard: ``when_matched_delete`` does not accept a condition parameter in the Lance API,
    so cross-window stale deletes (delete ts older than stored row ts) are not blocked at the Lance
    level. Within a window, ``collapse`` selects the terminal op per key, which mitigates in-window
    stale deletes. Cross-window stale deletes remain a known gap.

    When ``config.merge_batch_bytes`` is set, the upsert and delete tables are sliced into
    zero-copy chunks via :func:`table_chunks` using the table's actual mean row width to derive a
    rows-per-chunk value, and each chunk is committed independently. Chunking is order-safe because
    the upstream terminal-mutation collapse guarantees at most one row per record id reaches this
    function, so no key appears in more than one chunk.

    Args:
        config: ETL configuration.
        telemetry: Telemetry facade for the current executor.
        key: Routing key values in :data:`ROUTING_COLS` order.
        group: Rows for this routing key carrying the op column.

    Returns:
        Counts of upserted (inserted + updated) and deleted rows.
    """
    uri: str = dataset_uri(config, *key)
    is_delete: pa.Array = pc.is_in(group[OP_COL], value_set=pa.array(DELETE_OP_VALUES))
    payload_cols: list[str] = [c for c in group.column_names if c != OP_COL]

    upserts_pre_pivot: pa.Table = group.filter(pc.invert(is_delete)).select(payload_cols)
    upserts_pivoted, pivot_counts, column_roles = pivot_map_columns(upserts_pre_pivot, config)
    update_condition: str | None = (
        build_update_condition(config.ts_col) if config.ts_col in upserts_pivoted.schema.names else None
    )
    if pivot_counts.get("invalid_map_keys", 0) > 0:
        telemetry.incr("dataset.invalid_map_keys", value=pivot_counts["invalid_map_keys"])
        logger.warning(
            "dataset %s: %d map keys skipped (collision with an existing or reserved column)",
            uri,
            pivot_counts["invalid_map_keys"],
        )
    if pivot_counts.get("invalid_vector_rows", 0) > 0:
        telemetry.incr("dataset.invalid_vector_rows", value=pivot_counts["invalid_vector_rows"])
        logger.warning(
            "dataset %s: %d vector rows nulled out (length did not match the inferred dimension)",
            uri,
            pivot_counts["invalid_vector_rows"],
        )

    upserts: pa.Table = upserts_pivoted

    deletes: pa.Table = group.filter(is_delete).select([KEY_COL])

    upserted: int = 0
    deleted: int = 0

    if upserts.num_rows:
        try:
            with telemetry.timed("dataset.merge_ms"):
                upserted = commit_table_chunks(
                    config,
                    telemetry,
                    upserts,
                    lambda chunk, index, total: run_upsert_chunk(
                        config, uri, upserts.schema, update_condition, chunk, index, total
                    ),
                    lambda stats: stats.get("num_inserted_rows", 0) + stats.get("num_updated_rows", 0),
                )
            telemetry.incr("dataset.merged")
        except Exception:
            telemetry.incr("dataset.merge_error")
            raise

        merge_column_roles(
            uri,
            column_roles,
            config.storage_options,
            config.conflict_retries,
            config.retry_backoff_seconds,
            on_conflict=lambda: telemetry.incr("dataset.merge_conflict_retries"),
        )

    if deletes.num_rows:
        with telemetry.timed("dataset.delete_ms"):
            deleted = commit_table_chunks(
                config,
                telemetry,
                deletes,
                lambda chunk, index, total: run_delete_chunk(config, telemetry, uri, chunk, index, total),
                lambda stats: stats.get("num_deleted_rows", 0),
            )

    telemetry.distribution("dataset.upserted", upserted)
    telemetry.distribution("dataset.deleted", deleted)
    return upserted, deleted


def open_or_bootstrap(uri: str, schema: pa.Schema, config: ETLConfig) -> lance.LanceDataset:
    """Open the dataset, creating it empty when absent.

    New datasets are created with V2 manifest paths and the configured Lance file format. Only a
    genuinely absent dataset (:func:`dataset_absent`) is bootstrapped; a transient or fatal open
    error (bad credentials, object-store throttling, corrupt manifest) is re-raised rather than
    masked by a spurious empty-dataset write. A concurrent-bootstrap race surfaces as ``OSError``
    from the losing writer, which falls back to opening the winner's dataset.

    Args:
        uri: Dataset URI.
        schema: Schema used to bootstrap the empty dataset.
        config: ETL configuration supplying storage options.

    Returns:
        The open dataset handle.
    """
    try:
        return lance.dataset(uri, storage_options=config.storage_options)
    except (FileNotFoundError, ValueError) as error:
        if not dataset_absent(error):
            raise
        try:
            return lance.write_dataset(
                schema.empty_table(),
                uri,
                mode="append",
                storage_options=config.storage_options,
                enable_v2_manifest_paths=True,
                data_storage_version=DATA_STORAGE_VERSION,
            )
        except OSError:
            return lance.dataset(uri, storage_options=config.storage_options)


def run_upsert_chunk(
    config: ETLConfig,
    uri: str,
    schema: pa.Schema,
    update_condition: str | None,
    chunk: pa.Table,
    chunk_index: int,
    num_chunks: int,
) -> dict[str, Any]:
    """Open (or bootstrap) the dataset, evolve schema, and execute the merge upsert for one chunk.

    Re-opens the dataset on every call so retries and sequential chunk commits see the latest
    version. ``add_columns`` schema evolution is idempotent on retry.

    Args:
        config: ETL configuration.
        uri: Dataset URI.
        schema: The full upsert schema, used for bootstrap and schema evolution.
        update_condition: The cross-window LWW guard, or ``None`` when the ts column is absent.
        chunk: The upsert slice to commit.
        chunk_index: Zero-based position in the chunk sequence, used for logging.
        num_chunks: Total chunk count, used for logging.

    Returns:
        The merge statistics dictionary.
    """
    logger.debug("dataset %s: upsert chunk %d/%d (%d rows)", uri, chunk_index + 1, num_chunks, chunk.num_rows)
    dataset_local: lance.LanceDataset = open_or_bootstrap(uri, schema, config)
    missing_fields: list[pa.Field] = [
        schema.field(name) for name in schema.names if name not in dataset_local.schema.names
    ]
    if missing_fields:
        dataset_local.add_columns(pa.schema(missing_fields))
        dataset_local = lance.dataset(uri, storage_options=config.storage_options)
    builder = dataset_local.merge_insert(on=[KEY_COL])
    builder = builder.when_matched_update_all(condition=update_condition)
    return (
        builder.when_not_matched_insert_all()
        .conflict_retries(config.conflict_retries)
        .retry_timeout(DEFAULT_RETRY_TIMEOUT)
        .execute(chunk)
    )


def run_delete_chunk(
    config: ETLConfig,
    telemetry: Telemetry,
    uri: str,
    chunk: pa.Table,
    chunk_index: int,
    num_chunks: int,
) -> dict[str, Any]:
    """Execute ``when_matched_delete`` for one key-only chunk, a no-op only when the dataset is absent.

    Re-opens the dataset on every call so retries and sequential chunk commits see the latest
    version. A genuinely absent dataset (:func:`dataset_absent`) makes the delete a no-op, because
    there is nothing to delete. Any other open error — bad credentials, object-store throttling
    (S3 503), corrupt manifest — is re-raised after incrementing ``dataset.delete_open_failed``,
    so a transient fault never silently skips a compliance-sensitive delete.

    Args:
        config: ETL configuration.
        telemetry: Telemetry facade for the current executor.
        uri: Dataset URI.
        chunk: Key-only delete slice.
        chunk_index: Zero-based position in the chunk sequence, used for logging.
        num_chunks: Total chunk count, used for logging.

    Returns:
        The delete statistics dictionary, empty when the dataset does not exist.
    """
    logger.debug("dataset %s: delete chunk %d/%d (%d rows)", uri, chunk_index + 1, num_chunks, chunk.num_rows)
    try:
        delete_dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
    except (FileNotFoundError, ValueError) as error:
        if dataset_absent(error):
            return {}
        telemetry.incr("dataset.delete_open_failed")
        raise
    return (
        delete_dataset.merge_insert(on=[KEY_COL])
        .when_matched_delete()
        .conflict_retries(config.conflict_retries)
        .retry_timeout(DEFAULT_RETRY_TIMEOUT)
        .execute(chunk)
    )


def commit_table_chunks(
    config: ETLConfig,
    telemetry: Telemetry,
    table: pa.Table,
    run_chunk: Callable[[pa.Table, int, int], dict[str, Any]],
    count_stats: Callable[[dict[str, Any]], int],
) -> int:
    """Slice a table into byte-budgeted chunks and commit each through the shared retry loop.

    The shared shape behind both the upsert and delete paths: split via :func:`table_chunks`,
    commit each chunk with :func:`~lance_etl.telemetry.commit_with_retries` using the config's
    retry budget and backoff, count conflicts on ``dataset.merge_conflict_retries``, and sum the
    per-chunk row counts. Chunking is order-safe because the caller's collapse guarantees at most
    one row per key, so no key appears in more than one chunk.

    Args:
        config: ETL configuration supplying the chunk byte budget and retry knobs.
        telemetry: Telemetry facade for the current executor.
        table: The full table to commit.
        run_chunk: Executes one ``(chunk, index, total)`` commit and returns its statistics.
        count_stats: Extracts the affected-row count from one chunk's statistics.

    Returns:
        The summed affected-row count across all chunks.
    """
    total_rows: int = 0
    chunks: list[pa.Table] = table_chunks(table, config.merge_batch_bytes)
    for index, chunk in enumerate(chunks):
        stats: dict[str, Any] = commit_with_retries(
            lambda chunk=chunk, index=index: run_chunk(chunk, index, len(chunks)),
            retries=config.conflict_retries,
            backoff_seconds=config.retry_backoff_seconds,
            on_conflict=lambda: telemetry.incr("dataset.merge_conflict_retries"),
        )
        total_rows += count_stats(stats)
    return total_rows
