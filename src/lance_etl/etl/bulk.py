"""Bulk-append fast path for backfilling big NEW or empty per-tenant Lance datasets.

The merge path (:func:`lance_etl.etl.sink.apply_merge`) is the correct engine for incremental
change routing: it upserts by key with a last-write-wins guard and physical deletes, one
``merge_insert`` commit per sub-bucket. That per-key, per-commit machinery is pure overhead when
the target dataset is brand new or empty, because there is nothing to match against and no delete
can hit an existing row. A billion-row backfill routed through thousands of merge commits also
piles up commit-conflict retries on each dataset's single manifest.

This module is the fast path for exactly that case. For every big trio whose dataset is absent or
empty it:

1. Derives ONE canonical schema per trio on the driver from the collapsed upsert rows, so that
   every parallel task produces union-compatible fragments even though each task's slice sees only
   a subset of the map keys.
2. Bootstraps the empty dataset at that canonical schema.
3. Fans parallel :func:`lance.fragment.write_fragments` appends across the trio's key-hash
   sub-buckets, each streaming its slice through the same pivot-cast-align pipeline the merge path
   uses, so the written rows are byte-for-byte what the merge path would have produced.
4. Commits every fragment of a trio with ONE :meth:`lance.LanceDataset.commit_batch` append,
   turning thousands of merge commits into a single physical commit and sidestepping the
   merge-commit conflict ceiling entirely.

Delete-op rows are dropped inside the fan-out: against an empty dataset a delete is a no-op, which
is exactly what the merge path yields, so both paths converge to the same dataset state.

The path is guarded end to end: it activates only for trios that are absent or empty at plan time,
re-checks emptiness after bootstrap to demote any trio that gained rows in the race window, and
the caller excludes every committed trio from the merge job so no row is written twice. A replayed
window finds the datasets non-empty and falls back to the idempotent merge path.

Imports only the Spark-side plan helpers, the pure pivot/cast helpers, the sink URI/bootstrap
helpers, the column-role persistence, and telemetry, so :mod:`lance_etl.etl.job` can import it
without a cycle.
"""

from __future__ import annotations

import itertools
import logging
import pickle
from collections.abc import Iterator
from typing import Any

import lance
import pyarrow as pa
import pyarrow.compute as pc
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window, WindowSpec

from lance_etl.column_roles import SCALAR_ROLE, TEXT_ROLE, VECTOR_ROLE, merge_column_roles
from lance_etl.etl.pivot import (
    DELETE_OP_VALUES,
    KEY_COL,
    OP_COL,
    ROUTING_COLS,
    TTL_COL,
    ETLConfig,
    align_to_schema,
    apply_ttl_cast,
    build_stats_batch,
    enforce_map_key_bound,
    pivot_map_columns,
    routing_stats_ddl,
    routing_stats_schema,
    stream_routing_groups,
)
from lance_etl.etl.plan import RoutingPlan, apply_salted_shuffle, bucket_count, collapse
from lance_etl.etl.sink import DATA_STORAGE_VERSION, dataset_absent, dataset_uri, open_or_bootstrap
from lance_etl.telemetry import Telemetry, commit_with_retries

logger: logging.Logger = logging.getLogger(__name__)

MAP_COLUMNS: tuple[str, str, str] = ("vectors", "texts", "metadata")
"""Optional source map columns expanded by the pivot, excluded from the canonical base columns."""


def bulk_stats_schema() -> pa.Schema:
    """Build the per-task bulk-append stats schema.

    Returns:
        A schema of one string column per routing column plus ``appended`` (int64), ``txn``
        (binary, null for a failed trio) carrying the pickled append transaction back to the
        driver, and ``failed`` (int64) marking a trio whose append task failed in isolation.
    """
    return routing_stats_schema([("appended", pa.int64()), ("txn", pa.binary()), ("failed", pa.int64())])


def bulk_stats_spark_ddl() -> str:
    """Return the Spark DDL matching :func:`bulk_stats_schema` for the ``mapInArrow`` output schema.

    Returns:
        A DDL string with routing columns as string, ``appended`` as bigint, ``txn`` as binary,
        and ``failed`` as bigint.
    """
    return routing_stats_ddl([("appended", "bigint"), ("txn", "binary"), ("failed", "bigint")])


def plan_bulk_append(plan: RoutingPlan, config: ETLConfig) -> list[tuple[str, str, str, int]]:
    """Select the big trios whose dataset is absent or empty and therefore bulk-append eligible.

    Driver-only and metadata-only: for each big trio in ``plan`` it opens the dataset URI (treating
    a missing dataset as absent) and keeps it only when the dataset is absent or reports zero rows,
    which :meth:`lance.LanceDataset.count_rows` answers from the manifest without scanning rows. A
    non-empty dataset is left to the merge path, whose idempotent upsert is required to reconcile
    existing rows.

    The returned sub-bucket count is sized for the append fan-out, NOT reused from the plan's merge
    ``K``. The merge ``K`` in ``plan.big_trios`` is capped at ``max_buckets_per_dataset`` (default
    32) to bound per-key commit contention, but an append carries no such contention, so a big
    backfill can fan out far wider. This recomputes ``K_bulk`` from the trio's raw row count in
    ``plan.big_trio_rows`` against the same rows-per-bucket grain, capped at
    ``max_bulk_tasks_per_dataset`` (default 1024). A 1B-row backfill therefore parallelises across
    hundreds of appenders instead of being throttled to the merge cap.

    Args:
        plan: The routing plan carrying the big trios, their raw row counts, and sub-bucket counts.
        config: ETL configuration carrying the kill switch and the bulk task cap.

    Returns:
        One ``(org_id, tenant_id, namespace, K_bulk)`` per bulk-eligible trio, empty when the
        fast path is disabled or no big trio is absent or empty.
    """
    if not config.bulk_append:
        return []
    eligible: list[tuple[str, str, str, int]] = []
    for org, tenant, namespace, sub_buckets in plan.big_trios:
        uri: str = dataset_uri(config, org, tenant, namespace)
        try:
            dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
            empty: bool = dataset.count_rows() == 0
        except (FileNotFoundError, ValueError) as error:
            if not dataset_absent(error):
                raise
            empty = True
        if empty:
            rows: int = plan.big_trio_rows.get((org, tenant, namespace), sub_buckets)
            bulk_buckets: int = bucket_count(rows, config.bucket_rows, config.max_bulk_tasks_per_dataset)
            eligible.append((org, tenant, namespace, bulk_buckets))
    return eligible


def derive_bulk_schemas(
    filtered: DataFrame, trios: list[tuple[str, str, str, int]], config: ETLConfig
) -> dict[tuple[str, str, str], tuple[pa.Schema, dict[str, str], dict[str, int]]]:
    """Derive one canonical schema, role map, and vector-dimension map per bulk-eligible trio.

    Native Spark throughout. The frame is left-semi-joined to the eligible trios, collapsed
    last-write-wins, and reduced to the non-delete terminal rows — exactly the rows the pivot will
    consume, so the derived key set matches the merge path's pivot output. From those rows it
    collects, per trio, the distinct vector/text/metadata map keys and, per vector key, the
    most-frequent value length (count descending, length ascending as the deterministic tiebreak),
    which is strictly more robust against a single short vector than first-non-null inference.

    On the driver it assembles each trio's canonical :class:`pyarrow.Schema` in the exact column
    order :func:`lance_etl.etl.pivot.pivot_map_columns` produces: the base columns in source order
    (the source schema minus the op column and the three map columns, with the TTL column cast to
    ``pa.duration("s")`` when it is an integer, mirroring :func:`~lance_etl.etl.pivot.apply_ttl_cast`),
    then the sorted vector keys as ``fixed_size_list<float32, dim>``, then the sorted text keys as
    string, then the sorted metadata keys as string. Keys colliding with a base column or a reserved
    name are skipped, and each role is claimed in vector-then-text-then-metadata order, matching the
    pivot's own skip rule so the key set cannot diverge.

    A vector key whose value is null in every row of the trio is omitted, because it carries no
    dimension to fix; such a fully-null-everywhere vector key is a degenerate case the merge path
    would materialise as a raw-list all-null column, which is the one shape this fast path does not
    reproduce.

    A trio whose post-collapse increment contains only delete ops is excluded from the returned
    map entirely: it has nothing to append, and bootstrapping it would create a permanently empty
    dataset that the fleet jobs then discover forever. Its rows stay in the merge input, where the
    deletes no-op against the absent dataset.

    Each trio's per-map distinct-key count is checked against ``config.max_keys_per_map`` via
    :func:`~lance_etl.etl.pivot.enforce_map_key_bound` before any schema is assembled, so a source
    emitting near-unique map keys fails loudly on the driver instead of OOMing it or minting a
    runaway grow-only schema.

    Args:
        filtered: The null-routing-filtered increment, before collapse, carrying the op column.
        trios: The bulk-eligible trios from :func:`plan_bulk_append`.
        config: ETL configuration providing the timestamp and window column names and the map-key cap.

    Returns:
        A map from each ``(org_id, tenant_id, namespace)`` trio to ``(schema, roles, vector_dims)``,
        omitting delete-only trios.

    Raises:
        ValueError: When any trio's map column exceeds ``config.max_keys_per_map`` distinct keys.
    """
    if not trios:
        return {}
    spark: SparkSession = filtered.sparkSession
    routing: list[str] = list(ROUTING_COLS)
    eligible_df: DataFrame = spark.createDataFrame(
        [(org, tenant, namespace) for org, tenant, namespace, _ in trios],
        schema="org_id string, tenant_id string, namespace string",
    )
    scoped: DataFrame = filtered.join(F.broadcast(eligible_df), on=routing, how="left_semi")
    collapsed: DataFrame = collapse(scoped, config)
    is_delete: Any = F.col(OP_COL).isin(DELETE_OP_VALUES)
    upserts: DataFrame = collapsed.where(~is_delete)

    appendable: set[tuple[str, str, str]] = {
        (row["org_id"], row["tenant_id"], row["namespace"]) for row in upserts.select(*routing).distinct().collect()
    }
    columns: set[str] = set(filtered.columns)
    vector_dims_rows: list[Any] = []
    if "vectors" in columns:
        exploded: DataFrame = upserts.select(*[F.col(c) for c in routing], F.explode(F.col("vectors")))
        lengths: DataFrame = (
            exploded.where(F.col("value").isNotNull())
            .select(*routing, F.col("key").alias("map_key"), F.size(F.col("value")).alias("dim"))
            .groupBy(*routing, "map_key", "dim")
            .count()
        )
        window: WindowSpec = Window.partitionBy(*routing, "map_key").orderBy(F.col("count").desc(), F.col("dim").asc())
        vector_dims_rows = (
            lengths.withColumn("rank", F.row_number().over(window))
            .where(F.col("rank") == 1)
            .select(*routing, "map_key", "dim")
            .collect()
        )

    text_key_rows: list[Any] = collect_map_keys(upserts, "texts", routing) if "texts" in columns else []
    metadata_key_rows: list[Any] = collect_map_keys(upserts, "metadata", routing) if "metadata" in columns else []

    vector_dims_by_trio: dict[tuple[str, str, str], dict[str, int]] = {}
    for row in vector_dims_rows:
        trio: tuple[str, str, str] = (row["org_id"], row["tenant_id"], row["namespace"])
        vector_dims_by_trio.setdefault(trio, {})[row["map_key"]] = int(row["dim"])
    text_keys_by_trio: dict[tuple[str, str, str], set[str]] = keys_by_trio(text_key_rows)
    metadata_keys_by_trio: dict[tuple[str, str, str], set[str]] = keys_by_trio(metadata_key_rows)

    session_tz: str = spark.conf.get("spark.sql.session.timeZone")
    base_fields: list[pa.Field] = canonical_base_fields(filtered.limit(0).toArrow().schema, session_tz)
    schemas: dict[tuple[str, str, str], tuple[pa.Schema, dict[str, str], dict[str, int]]] = {}
    for org, tenant, namespace, _ in trios:
        trio = (org, tenant, namespace)
        if trio not in appendable:
            logger.info("bulk-append trio %s has only delete ops after collapse; leaving it to the merge path", trio)
            continue
        label: str = f"{org}/{tenant}/{namespace}"
        enforce_map_key_bound(f"vectors[{label}]", len(vector_dims_by_trio.get(trio, {})), config.max_keys_per_map)
        enforce_map_key_bound(f"texts[{label}]", len(text_keys_by_trio.get(trio, set())), config.max_keys_per_map)
        enforce_map_key_bound(
            f"metadata[{label}]", len(metadata_keys_by_trio.get(trio, set())), config.max_keys_per_map
        )
        schema, roles, vector_dims = assemble_canonical_schema(
            base_fields,
            vector_dims_by_trio.get(trio, {}),
            text_keys_by_trio.get(trio, set()),
            metadata_keys_by_trio.get(trio, set()),
            config,
        )
        schemas[trio] = (schema, roles, vector_dims)
    return schemas


def collect_map_keys(frame: DataFrame, map_column: str, routing: list[str]) -> list[Any]:
    """Collect the distinct ``(trio, key)`` pairs of a string map column.

    Args:
        frame: The upsert frame carrying the map column.
        map_column: The map column to enumerate keys from.
        routing: The routing columns naming each trio.

    Returns:
        Distinct ``(org_id, tenant_id, namespace, map_key)`` rows.
    """
    return (
        frame.select(*[F.col(c) for c in routing], F.explode(F.map_keys(F.col(map_column))).alias("map_key"))
        .distinct()
        .collect()
    )


def keys_by_trio(rows: list[Any]) -> dict[tuple[str, str, str], set[str]]:
    """Group collected ``(trio, map_key)`` rows into a per-trio key set.

    Args:
        rows: Rows carrying the routing columns and a ``map_key`` column.

    Returns:
        A map from each trio to its set of map keys.
    """
    grouped: dict[tuple[str, str, str], set[str]] = {}
    for row in rows:
        trio: tuple[str, str, str] = (row["org_id"], row["tenant_id"], row["namespace"])
        grouped.setdefault(trio, set()).add(row["map_key"])
    return grouped


def canonical_base_fields(source_schema: pa.Schema, session_tz: str) -> list[pa.Field]:
    """Build the canonical base columns: the source columns minus op and the map columns.

    Preserves source column order and casts the TTL column to ``pa.duration("s")`` when it is an
    integer, mirroring :func:`lance_etl.etl.pivot.apply_ttl_cast` so the base portion of the
    canonical schema matches the pivoted merge output exactly.

    Timezone-aware timestamp fields are re-stamped with the Spark session timezone. The driver's
    ``DataFrame.toArrow`` normalises every timestamp to UTC, but the executor's ``mapInArrow`` — the
    conversion the merge path actually writes through — stamps timestamps with the session timezone.
    Aligning the canonical schema to the session timezone makes the executor's pivoted timestamps
    conform without a metadata-changing cast, so the bulk and merge datasets carry identical
    timestamp types regardless of the session timezone.

    Each field keeps the source column's nullability. The executor's ``mapInArrow`` conversion the
    merge path writes through preserves Spark's non-nullable flag, so Lance stores those base
    columns non-nullable; a canonical field that dropped the flag would make the bulk dataset's
    schema differ from the merge dataset's.

    Args:
        source_schema: The Arrow schema of the source increment (op and map columns intact).
        session_tz: The Spark session timezone (``spark.sql.session.timeZone``).

    Returns:
        The ordered base fields with the TTL column typed as a duration when applicable, every
        timezone-aware timestamp re-stamped with the session timezone, and each column's source
        nullability preserved.
    """
    excluded: set[str] = {OP_COL, *MAP_COLUMNS}
    fields: list[pa.Field] = []
    for source_field in source_schema:
        if source_field.name in excluded:
            continue
        if source_field.name == TTL_COL and pa.types.is_integer(source_field.type):
            fields.append(pa.field(source_field.name, pa.duration("s"), nullable=source_field.nullable))
        elif pa.types.is_timestamp(source_field.type) and source_field.type.tz is not None:
            restamped: pa.DataType = pa.timestamp(source_field.type.unit, tz=session_tz)
            fields.append(pa.field(source_field.name, restamped, nullable=source_field.nullable))
        else:
            fields.append(source_field)
    return fields


def assemble_canonical_schema(
    base_fields: list[pa.Field],
    vector_dims: dict[str, int],
    text_keys: set[str],
    metadata_keys: set[str],
    config: ETLConfig,
) -> tuple[pa.Schema, dict[str, str], dict[str, int]]:
    """Assemble one trio's canonical schema, role map, and vector-dimension map.

    Claims keys in vector-then-text-then-metadata order, skipping any key that collides with a base
    column or a reserved name or a key already claimed by an earlier role, exactly as
    :func:`lance_etl.etl.pivot.pivot_map_columns` does. Vector keys become
    ``fixed_size_list<float32, dim>``; text and metadata keys become string.

    Args:
        base_fields: The canonical base fields in source order.
        vector_dims: The most-frequent dimension per vector key for this trio.
        text_keys: The text map keys for this trio.
        metadata_keys: The metadata map keys for this trio.
        config: ETL configuration providing the reserved column names.

    Returns:
        ``(schema, roles, kept_vector_dims)`` for this trio.
    """
    reserved: set[str] = {KEY_COL, OP_COL, config.ts_col, config.window_column, *ROUTING_COLS}
    taken: set[str] = {field.name for field in base_fields} | reserved
    fields: list[pa.Field] = list(base_fields)
    roles: dict[str, str] = {}
    kept_vector_dims: dict[str, int] = {}

    for key in sorted(vector_dims):
        if key in taken:
            continue
        fields.append(pa.field(key, pa.list_(pa.float32(), vector_dims[key])))
        roles[key] = VECTOR_ROLE
        kept_vector_dims[key] = vector_dims[key]
        taken.add(key)
    for key in sorted(text_keys):
        if key in taken:
            continue
        fields.append(pa.field(key, pa.string()))
        roles[key] = TEXT_ROLE
        taken.add(key)
    for key in sorted(metadata_keys):
        if key in taken:
            continue
        fields.append(pa.field(key, pa.string()))
        roles[key] = SCALAR_ROLE
        taken.add(key)

    return pa.schema(fields), roles, kept_vector_dims


def bootstrap_bulk_datasets(
    schemas: dict[tuple[str, str, str], tuple[pa.Schema, dict[str, str], dict[str, int]]],
    config: ETLConfig,
    telemetry: Telemetry,
) -> list[tuple[str, str, str]]:
    """Bootstrap each trio's empty dataset at its canonical schema, re-checking emptiness.

    Driver-only. Creates each dataset empty at the canonical schema via
    :func:`lance_etl.etl.sink.open_or_bootstrap`, then re-reads ``count_rows`` and demotes any trio
    that gained rows between planning and bootstrap, so a trio that a concurrent writer filled in
    the race window is left to the merge path instead of double-written.

    Args:
        schemas: The per-trio canonical schemas from :func:`derive_bulk_schemas`.
        config: ETL configuration supplying storage options and the data storage version.
        telemetry: Driver telemetry facade.

    Returns:
        The trios that were created empty and stayed empty, in schema-map order.
    """
    eligible: list[tuple[str, str, str]] = []
    for trio, (schema, _, _) in schemas.items():
        uri: str = dataset_uri(config, *trio)
        dataset: lance.LanceDataset = open_or_bootstrap(uri, schema, config)
        if dataset.count_rows() == 0:
            eligible.append(trio)
        else:
            logger.info("bulk-append trio %s gained rows before bootstrap; deferring to merge path", trio)
    telemetry.gauge("run.bulk_datasets", len(eligible))
    return eligible


def run_bulk_append(
    filtered: DataFrame,
    eligible_trios: list[tuple[str, str, str, int]],
    schemas: dict[tuple[str, str, str], tuple[pa.Schema, dict[str, str], dict[str, int]]],
    config: ETLConfig,
    telemetry: Telemetry,
) -> list[tuple[str, str, str, int, lance.Transaction | None, int]]:
    """Fan parallel ``write_fragments`` appends across the eligible trios' key-hash sub-buckets.

    Collapses and salt-shuffles the eligible-trio slice exactly as the merge path does, then runs
    an Arrow closure per partition. The closure streams routing-key groups
    (:func:`~lance_etl.etl.pivot.stream_routing_groups`) and, per trio, feeds a generator of
    pivoted-cast-aligned flush chunks into ONE :func:`lance.fragment.write_fragments` append.
    Delete-op rows are dropped in the generator, matching the merge path's no-op deletes against an
    empty dataset. Each chunk runs the same pivot (with the canonical vector dimensions), TTL cast,
    and canonical alignment the merge path uses, so the appended rows are byte-for-byte identical.

    Each ``(partition, trio)`` that appended any row emits a stats row carrying the pickled append
    transaction, which is collected to the driver and unpickled for the single per-trio
    :meth:`lance.LanceDataset.commit_batch`.

    Failure isolation: a trio whose append raises on its own already-materialised groups is caught
    per trio, metered as ``dataset.bulk_group_failed``, and reported back as a ``failed`` marker row
    instead of failing the whole run. The caller must drop every transaction of a failed trio, so
    nothing partial is ever committed and the still-empty dataset is retried by a rerun. A failure
    that originates in advancing the shared batch stream (not in one trio's processing) fails the
    whole task instead, so the partition's remaining trios are never silently dropped
    (:func:`append_partition_trios`).

    Args:
        filtered: The null-routing-filtered increment, before collapse, carrying the op column.
        eligible_trios: The trios (with sub-bucket counts) that stayed empty after bootstrap.
        schemas: The per-trio canonical schemas, roles, and vector dimensions.
        config: ETL configuration.
        telemetry: Driver telemetry facade, used to gauge the fan-out width. Each executor task
            builds its own facade for the per-partition spans and distributions.

    Returns:
        One ``(org_id, tenant_id, namespace, appended, transaction, failed)`` per appending or
        failing task, where ``transaction`` is None for a failed trio. Empty when nothing was
        eligible or appended.
    """
    if not eligible_trios:
        return []
    spark: SparkSession = filtered.sparkSession
    routing: list[str] = list(ROUTING_COLS)
    eligible_df: DataFrame = spark.createDataFrame(
        [(org, tenant, namespace) for org, tenant, namespace, _ in eligible_trios],
        schema="org_id string, tenant_id string, namespace string",
    )
    scoped: DataFrame = filtered.join(F.broadcast(eligible_df), on=routing, how="left_semi")
    bulk_plan: RoutingPlan = RoutingPlan(
        total_rows=0,
        trio_count=len(eligible_trios),
        big_trios=list(eligible_trios),
        num_partitions=max(1, sum(sub_buckets for *_, sub_buckets in eligible_trios)),
        null_routing_rows=0,
    )
    routed: DataFrame = apply_salted_shuffle(collapse(scoped, config), bulk_plan)
    telemetry.gauge("run.bulk_partitions", bulk_plan.num_partitions)
    schema_broadcast = spark.sparkContext.broadcast(schemas)
    output_schema: pa.Schema = bulk_stats_schema()
    output_ddl: str = bulk_stats_spark_ddl()

    def append_partition(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        """Append every eligible trio's slice in one Spark partition, one transaction per trio.

        Args:
            batches: Arrow batches for this task, sorted by the routing columns.

        Yields:
            One stats record batch when the partition appended any dataset.
        """
        first: pa.RecordBatch | None = next(batches, None)
        if first is None:
            return
        executor_telemetry: Telemetry = Telemetry.create(config.telemetry)
        chained: Iterator[pa.RecordBatch] = itertools.chain([first], batches)
        broadcast_schemas: dict[tuple[str, str, str], tuple[pa.Schema, dict[str, str], dict[str, int]]] = (
            schema_broadcast.value
        )
        counters: dict[str, int] = {}
        results: list[tuple[Any, ...]] = []
        with executor_telemetry.span("lance.etl.bulk_partition"):
            try:
                groups: Iterator[tuple[tuple[Any, ...], pa.Table]] = stream_routing_groups(
                    chained, routing, config.merge_batch_bytes, counters
                )
                append_partition_trios(groups, broadcast_schemas, config, executor_telemetry, results)
            except Exception:
                executor_telemetry.error("etl bulk partition failed")
                raise
        if results:
            yield build_stats_batch(results, output_schema)

    collected: list[Any] = routed.mapInArrow(append_partition, schema=output_ddl).collect()
    return [
        (
            row["org_id"],
            row["tenant_id"],
            row["namespace"],
            int(row["appended"]),
            pickle.loads(bytes(row["txn"])) if row["txn"] is not None else None,
            int(row["failed"] or 0),
        )
        for row in collected
    ]


def append_partition_trios(
    groups: Iterator[tuple[tuple[Any, ...], pa.Table]],
    schemas: dict[tuple[str, str, str], tuple[pa.Schema, dict[str, str], dict[str, int]]],
    config: ETLConfig,
    telemetry: Telemetry,
    results: list[tuple[Any, ...]],
) -> None:
    """Append each contiguous trio run from one partition's shared routing-group stream.

    Groups the shared stream into contiguous per-trio runs and appends each through
    :func:`append_one_trio`, isolating a failure that is confined to processing one trio's
    already-materialised groups as a ``failed`` marker row while letting a failure that originates in
    advancing the shared stream itself fail the whole task.

    This mirrors the merge path's group-outside-the-try contract (see
    :meth:`lance_etl.etl.job.IcebergToLanceETL.merge_dataframe`). The merge path materialises each
    group fully in the ``for`` header, outside the per-group ``try``, so a shared-stream failure
    escapes the per-group handler. This path cannot, because :func:`append_one_trio` streams the
    trio's groups lazily into ``write_fragments`` inside the ``try``. Instead the shared stream is
    wrapped in a guarded generator that records the exception it raises while advancing before
    re-raising it. The per-trio handler then re-raises a recorded shared-stream failure (failing the
    task, so the partition's remaining trios are never silently dropped and nothing partial commits)
    and marks only the current trio failed for a genuine per-trio processing failure. A post-return
    check re-raises a recorded shared-stream failure even in the unlikely event ``write_fragments``
    consumed the raising reader without surfacing the exception.

    Args:
        groups: The shared ``(key, table)`` routing-group stream for this partition.
        schemas: The per-trio canonical schemas, roles, and vector dimensions.
        config: ETL configuration.
        telemetry: The executor telemetry facade.
        results: Mutable accumulator receiving one stats row per appending or failing trio.
    """
    stream_error: list[BaseException | None] = [None]

    def guarded(source: Iterator[tuple[tuple[Any, ...], pa.Table]]) -> Iterator[tuple[tuple[Any, ...], pa.Table]]:
        """Yield the shared stream's groups, recording an advancement failure before re-raising it.

        Args:
            source: The shared routing-group generator.

        Yields:
            Each ``(key, table)`` group from the shared stream.

        Raises:
            Exception: Whatever the shared stream raised while advancing, after recording it in
                ``stream_error`` so the per-trio handler can tell it apart from a per-trio failure.
        """
        while True:
            try:
                item: tuple[tuple[Any, ...], pa.Table] = next(source)
            except StopIteration:
                return
            except Exception as error:
                stream_error[0] = error
                raise
            yield item

    for trio_key, trio_groups in itertools.groupby(guarded(groups), key=lambda item: item[0]):
        try:
            appended: int = append_one_trio(trio_key, trio_groups, schemas, config, results)
            if stream_error[0] is not None:
                raise stream_error[0]
            if appended:
                telemetry.distribution("dataset.bulk_appended", appended)
        except Exception:
            if stream_error[0] is not None:
                raise
            telemetry.incr("dataset.bulk_group_failed")
            telemetry.error("etl bulk trio failed")
            logger.exception("bulk append failed for trio %s; continuing with remaining trios", trio_key)
            results.append((trio_key[0], trio_key[1], trio_key[2], 0, None, 1))


def append_one_trio(
    trio_key: tuple[Any, ...],
    trio_groups: Iterator[tuple[tuple[Any, ...], pa.Table]],
    schemas: dict[tuple[str, str, str], tuple[pa.Schema, dict[str, str], dict[str, int]]],
    config: ETLConfig,
    results: list[tuple[Any, ...]],
) -> int:
    """Append one trio's contiguous groups as a single streamed ``write_fragments`` transaction.

    Streams the trio's flush chunks through the pivot-cast-align pipeline (delete rows dropped) and
    into one append. The pivoted rows use the canonical vector dimensions and are aligned to the
    canonical schema, so parallel tasks produce union-compatible fragments. Appends nothing and
    emits no stats row when every row of the trio was a delete.

    Args:
        trio_key: The routing key of this contiguous run.
        trio_groups: The ``(key, table)`` groups belonging to this trio.
        schemas: The per-trio canonical schemas, roles, and vector dimensions.
        config: ETL configuration.
        results: Mutable accumulator that receives one ``(org, tenant, namespace, appended, txn, 0)``
            row when the trio appended any row.

    Returns:
        The number of rows appended for this trio.
    """
    trio: tuple[str, str, str] = (trio_key[0], trio_key[1], trio_key[2])
    if trio not in schemas:
        for _ in trio_groups:
            pass
        return 0
    canonical, _, vector_dims = schemas[trio]
    uri: str = dataset_uri(config, *trio)
    appended: list[int] = [0]

    def chunk_batches() -> Iterator[pa.RecordBatch]:
        """Yield canonical-aligned append batches for this trio, dropping delete rows.

        Yields:
            Record batches conforming to the trio's canonical schema.
        """
        for _, table in trio_groups:
            is_delete: pa.Array = pc.is_in(table[OP_COL], value_set=pa.array(DELETE_OP_VALUES))
            payload_cols: list[str] = [column for column in table.column_names if column != OP_COL]
            keep: pa.Table = table.filter(pc.invert(is_delete)).select(payload_cols)
            if keep.num_rows == 0:
                continue
            pivoted, _, _ = pivot_map_columns(keep, config, vector_dims)
            aligned: pa.Table = align_to_schema(apply_ttl_cast(pivoted, TTL_COL), canonical)
            appended[0] += aligned.num_rows
            yield from aligned.to_batches()

    reader: pa.RecordBatchReader = pa.RecordBatchReader.from_batches(canonical, chunk_batches())
    transaction: lance.Transaction = lance.fragment.write_fragments(
        reader,
        uri,
        schema=canonical,
        mode="append",
        return_transaction=True,
        storage_options=config.storage_options,
        data_storage_version=DATA_STORAGE_VERSION,
    )
    if appended[0] == 0:
        return 0
    results.append((*trio, appended[0], pickle.dumps(transaction), 0))
    return appended[0]


def commit_bulk_transactions(
    config: ETLConfig,
    telemetry: Telemetry,
    uri: str,
    transactions: list[lance.Transaction],
    roles: dict[str, str],
) -> int:
    """Commit one trio's append transactions as a single ``commit_batch`` and persist its roles.

    Merges every task's append transaction for the trio into ONE physical append commit through
    :func:`lance_etl.telemetry.commit_with_retries` with a ZERO retry budget, then merges the
    trio's pivoted column roles into its ``lance-etl.columns`` config so the indexer sees the
    backfilled columns. Returns the total rows the merged commit added, read back from the merged
    transaction's fragments.

    Why zero retries: a raw append is not idempotent the way ``merge_insert`` is. If the outer
    wrapper re-ran the action after an ambiguous commit outcome (a CAS whose success the client
    never observed), it would re-commit the same fragments and silently duplicate every row.
    ``commit_batch`` already runs lance's inner rebase retry for ordinary retryable conflicts, so
    the single outer attempt only converts an ambiguous or hard-conflict outcome into a LOUD
    failure. The designed recovery is the rerun: ``plan_bulk_append``'s emptiness check demotes a
    trio whose commit actually landed to the idempotent merge path, and retries the bulk path for
    a trio whose commit truly failed (ADR 0034).

    After the commit, the dataset's ``count_rows`` is asserted equal to the merged transaction's
    fragment row total. The target was bootstrapped empty and no concurrent ETL writer is assumed
    during the append window (ADR 0034), so any excess row is a duplicate append. A mismatch
    increments ``dataset.bulk_rowcount_mismatch`` and raises.

    Args:
        config: ETL configuration supplying storage options and retry knobs.
        telemetry: Driver telemetry facade.
        uri: The trio's dataset URI.
        transactions: The append transactions collected from the fan-out for this trio.
        roles: The trio's pivoted column roles.

    Returns:
        The total rows appended by the merged commit, or zero when there is nothing to commit.

    Raises:
        ValueError: When the post-commit row count differs from the committed fragment total.
    """
    if not transactions:
        return 0

    def action() -> dict[str, Any]:
        """Merge the trio's append transactions into one physical commit."""
        return lance.LanceDataset.commit_batch(uri, transactions, storage_options=config.storage_options)

    with telemetry.timed("dataset.bulk_commit_ms"):
        result: dict[str, Any] = commit_with_retries(
            action,
            0,
            config.retry_backoff_seconds,
            on_conflict=lambda: telemetry.incr("dataset.bulk_commit_conflict_retries"),
        )
    merged: lance.Transaction = result["merged"]
    expected: int = sum(fragment.num_rows for fragment in merged.operation.fragments)
    actual: int = lance.dataset(uri, storage_options=config.storage_options).count_rows()
    if actual != expected:
        telemetry.incr("dataset.bulk_rowcount_mismatch")
        raise ValueError(
            f"bulk append to {uri} committed {expected} rows but the dataset holds {actual}: "
            "a duplicate or concurrent append reached this freshly bootstrapped dataset"
        )
    merge_column_roles(
        uri,
        roles,
        config.storage_options,
        config.conflict_retries,
        config.retry_backoff_seconds,
        on_conflict=lambda: telemetry.incr("dataset.bulk_commit_conflict_retries"),
    )
    return expected
