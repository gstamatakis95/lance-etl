"""End-to-end tests for the Phase 2 bulk-append fast path (:mod:`lance_etl.etl.bulk`).

The bulk-append path replaces thousands of per-key ``merge_insert`` commits with parallel
``write_fragments`` plus one ``commit_batch`` per dataset for big trios whose Lance dataset is
absent or empty. These tests pin its load-bearing guarantees end to end on a four-core local Spark
session: the driver-derived canonical schema equals the executor pivot output byte-compatibly, a
full bulk run reproduces the merge path's per-dataset contents exactly, parallel sub-buckets fan out
without duplicating keys, deletes are no-ops against an empty dataset, replay falls back to the
idempotent merge path, heterogeneous key subsets align into one schema, column roles persist, and a
non-empty big dataset is demoted to the merge path.

The canonical smoke construction lives in the scratchpad ``smoke_bulk.py`` reference. The source
builders here mirror it so the timezone and nullability subtleties stay handled: uniform vector
lengths per key (so most-frequent and first-non-null dimension inference agree), a UTC session (so
driver ``toArrow`` and executor ``mapInArrow`` stamp timestamps identically), and the three map
columns typed exactly as the SQL contract requires.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import lance
import pyarrow as pa
import pytest
from pyspark.sql import DataFrame, SparkSession

from lance_etl.column_roles import load_column_roles
from lance_etl.etl import (
    ETLConfig,
    IcebergToLanceETL,
    apply_ttl_cast,
    dataset_uri,
    derive_bulk_schemas,
    pivot_map_columns,
    plan_bulk_append,
)
from lance_etl.etl.pivot import OP_COL, TTL_COL
from lance_etl.etl.plan import RoutingPlan, compute_routing_plan
from lance_etl.telemetry import TelemetryConfig

pytestmark = pytest.mark.integration

SOURCE_DDL: str = (
    "org_id string, tenant_id string, namespace string, vector_id string, op string, "
    "event_timestamp timestamp, processing_timestamp timestamp, ttl bigint, "
    "vectors map<string, array<float>>, texts map<string, string>, metadata map<string, string>"
)
"""Contract-satisfying Spark DDL shared by every bulk-append source, matching ``smoke_bulk.py``."""

TS: datetime = datetime(2026, 7, 5, 12, 0, 0, tzinfo=UTC)
"""Single UTC timestamp reused for both required timestamp columns."""


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    """Provide a four-core local Spark session pinned to the test interpreter and UTC.

    Yields:
        A four-core local session whose session timezone is forced to UTC at runtime, so the
        canonical-schema timestamp re-stamp matches the driver ``toArrow`` conversion regardless of
        which test module first created the JVM session.
    """
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session: SparkSession = (
        SparkSession.builder.master("local[4]")
        .appName("lance-etl-bulk-append-tests")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.stop()


def bulk_config(base: str, telemetry_config: TelemetryConfig, *, bulk: bool = True, **overrides: object) -> ETLConfig:
    """Build an ETL configuration for the bulk-append tests.

    Args:
        base: Root directory under which per-tenant datasets are written.
        telemetry_config: The test telemetry configuration.
        bulk: Whether the bulk-append fast path is enabled.
        **overrides: Extra :class:`ETLConfig` field overrides (for example ``bucket_rows``).

    Returns:
        An :class:`ETLConfig` with a small merge byte budget and the requested overrides.
    """
    params: dict[str, object] = {
        "base_uri": base,
        "telemetry": telemetry_config,
        "bulk_append": bulk,
        "bucket_rows": 100,
        "max_buckets_per_dataset": 8,
        "merge_batch_bytes": 1_000_000,
    }
    params.update(overrides)
    return ETLConfig(**params)  # type: ignore[arg-type]


def big_trio_rows(count: int, org: str = "orgBig") -> list[tuple]:
    """Build ``count`` insert rows for one big trio with heterogeneous per-row key subsets.

    Every row carries the ``emb`` vector (length 8) and a ``title`` text and ``lang`` metadata. Even
    rows additionally carry an ``emb2`` vector (length 4), every third row a ``body`` text, and every
    fifth row a ``cat`` metadata value, so some pivoted columns are null in some rows.

    Args:
        count: Number of insert rows to generate.
        org: The ``org_id`` literal for every row.

    Returns:
        Rows conforming to :data:`SOURCE_DDL`, all inserts, all in the ``(org, t1, ns1)`` trio.
    """
    rows: list[tuple] = []
    for i in range(count):
        vectors: dict[str, list[float]] = {"emb": [float((i + j) % 11) for j in range(8)]}
        if i % 2 == 0:
            vectors["emb2"] = [float(i), float(-i), float(i + 1), float(i + 2)]
        texts: dict[str, str] = {"title": f"doc-{i}"}
        if i % 3 == 0:
            texts["body"] = f"body text {i}"
        metadata: dict[str, str] = {"lang": "en"}
        if i % 5 == 0:
            metadata["cat"] = f"c{i % 4}"
        rows.append((org, "t1", "ns1", f"big-{i}", "insert", TS, TS, 3600, vectors, texts, metadata))
    return rows


def read_all_datasets(base: str) -> dict[tuple[str, str, str], pa.Table]:
    """Read every Lance dataset under a base directory into a table keyed by its routing trio.

    Args:
        base: Root directory holding ``org/tenant/namespace.lance`` datasets.

    Returns:
        A map from each ``(org_id, tenant_id, namespace)`` trio to its full table.
    """
    out: dict[tuple[str, str, str], pa.Table] = {}
    for path in sorted(Path(base).glob("*/*/*.lance")):
        org: str = path.parent.parent.name
        tenant: str = path.parent.name
        namespace: str = path.name[: -len(".lance")]
        out[(org, tenant, namespace)] = lance.dataset(str(path)).to_table()
    return out


def normalize(table: pa.Table) -> pa.Table:
    """Sort a table by ``vector_id`` and order its columns by name for order-independent comparison.

    Args:
        table: The table to normalize.

    Returns:
        The table sorted by ``vector_id`` with columns in sorted-name order.
    """
    return table.sort_by([("vector_id", "ascending")]).select(sorted(table.column_names))


def assert_tables_equal(left: pa.Table, right: pa.Table, label: str) -> None:
    """Assert two tables are identical in row count, columns, types, and values.

    Args:
        left: The first table.
        right: The second table.
        label: A label naming the compared datasets for assertion messages.
    """
    normalized_left: pa.Table = normalize(left)
    normalized_right: pa.Table = normalize(right)
    assert normalized_left.num_rows == normalized_right.num_rows, (
        f"{label}: row count {normalized_left.num_rows} != {normalized_right.num_rows}"
    )
    assert normalized_left.column_names == normalized_right.column_names, (
        f"{label}: columns {normalized_left.column_names} != {normalized_right.column_names}"
    )
    for name in normalized_left.column_names:
        left_column: pa.ChunkedArray = normalized_left.column(name)
        right_column: pa.ChunkedArray = normalized_right.column(name)
        assert left_column.type.equals(right_column.type), (
            f"{label}.{name}: type {left_column.type} != {right_column.type}"
        )
        assert left_column.to_pylist() == right_column.to_pylist(), f"{label}.{name}: values differ"


def test_canonical_schema_matches_pivot_output(
    spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
) -> None:
    """The driver-derived canonical schema equals the executor pivot output in names, types, and order.

    This pins the crux of the fast path: :func:`derive_bulk_schemas` must produce exactly the schema
    that :func:`pivot_map_columns` plus :func:`apply_ttl_cast` produce on the same rows, so parallel
    tasks that each see only a subset of the map keys still write union-compatible fragments. With
    uniform vector lengths the most-frequent-dimension inference on the driver agrees with the
    first-non-null inference in the pivot, so the two schemas coincide field for field.

    Args:
        spark: The module-scoped four-core UTC session.
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
    """
    config: ETLConfig = bulk_config(str(tmp_path), telemetry_config, bucket_rows=50)
    frame: DataFrame = spark.createDataFrame(big_trio_rows(120), schema=SOURCE_DDL)
    plan: RoutingPlan = compute_routing_plan(frame, config)
    trios: list[tuple[str, str, str, int]] = plan_bulk_append(plan, config)
    schemas = derive_bulk_schemas(frame, trios, config)
    assert len(schemas) == 1
    trio: tuple[str, str, str] = next(iter(schemas))
    canonical: pa.Schema = schemas[trio][0]

    arrow: pa.Table = frame.toArrow()
    without_op: pa.Table = arrow.select([name for name in arrow.column_names if name != OP_COL])
    pivoted, _, _ = pivot_map_columns(without_op, config)
    pivoted = apply_ttl_cast(pivoted, TTL_COL)

    assert canonical.names == pivoted.schema.names
    for canonical_field, pivot_field in zip(canonical, pivoted.schema, strict=True):
        assert canonical_field.type.equals(pivot_field.type), (
            f"{canonical_field.name}: canonical {canonical_field.type} != pivot {pivot_field.type}"
        )


def test_derive_collects_keys_and_most_frequent_dims(
    spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
) -> None:
    """Derivation picks the most-frequent vector dimension (smaller on ties) and collects every role.

    The ``emb`` key appears five times at length 8 and once at length 4, so its canonical dimension
    is the mode 8. The ``tie`` key appears twice at length 4 and twice at length 6, so the documented
    count-descending, size-ascending tiebreak selects the smaller length 4. Every distinct vector,
    text, and metadata key is collected into the matching role.

    Args:
        spark: The module-scoped four-core UTC session.
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
    """
    config: ETLConfig = bulk_config(str(tmp_path), telemetry_config, bucket_rows=5)
    emb_lengths: list[int] = [8, 8, 8, 8, 8, 4]
    tie_lengths: list[int] = [4, 4, 6, 6]
    rows: list[tuple] = []
    for i, length in enumerate(emb_lengths):
        texts: dict[str, str] = {"title": f"t{i}"}
        if i < 2:
            texts["body"] = f"b{i}"
        metadata: dict[str, str] = {"lang": "en"}
        if i == 0:
            metadata["cat"] = "c0"
        rows.append(("orgHet", "t1", "ns1", f"e{i}", "insert", TS, TS, 60, {"emb": [1.0] * length}, texts, metadata))
    for j, length in enumerate(tie_lengths):
        rows.append(
            (
                "orgHet",
                "t1",
                "ns1",
                f"tie{j}",
                "insert",
                TS,
                TS,
                60,
                {"tie": [2.0] * length},
                {"title": "x"},
                {"lang": "en"},
            )
        )
    frame: DataFrame = spark.createDataFrame(rows, schema=SOURCE_DDL)
    plan: RoutingPlan = compute_routing_plan(frame, config)
    trios: list[tuple[str, str, str, int]] = plan_bulk_append(plan, config)
    schemas = derive_bulk_schemas(frame, trios, config)

    trio: tuple[str, str, str] = ("orgHet", "t1", "ns1")
    schema, roles, vector_dims = schemas[trio]
    assert vector_dims == {"emb": 8, "tie": 4}
    assert schema.field("emb").type == pa.list_(pa.float32(), 8)
    assert schema.field("tie").type == pa.list_(pa.float32(), 4)
    assert roles == {
        "emb": "vector",
        "tie": "vector",
        "title": "text",
        "body": "text",
        "lang": "scalar",
        "cat": "scalar",
    }


def test_backfill_matches_merge_path(spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
    """A full bulk-append run reproduces the merge path's per-dataset contents exactly.

    The same heterogeneous-key increment is routed with ``bulk_append=True`` into base A and with
    ``bulk_append=False`` into base B. A big trio with a small ``bucket_rows`` forces ``K > 1``
    sub-buckets so base A exercises real parallel fan-out, and two tiny trios plus a delete op cover
    the merge-path remainder. Both runs pin ``num_partitions=1`` so the merge side of each run
    bootstraps every brand-new dataset serially, removing the pure-merge bootstrap race while base
    A's bulk fan-out still runs across its own ``K`` sub-buckets. Every dataset must be byte-for-byte
    identical between the two bases.

    Args:
        spark: The module-scoped four-core UTC session.
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
    """
    rows: list[tuple] = big_trio_rows(400)
    rows.append(("orgBig", "t1", "ns1", "big-del", "delete", TS, TS, 3600, {}, {}, {}))
    for i in range(3):
        rows.append(
            (
                "orgSmallA",
                "t9",
                "ns9",
                f"a-{i}",
                "insert",
                TS,
                TS,
                100,
                {"emb": [1.0] * 8},
                {"title": "x"},
                {"lang": "fr"},
            )
        )
    for i in range(2):
        rows.append(
            ("orgSmallB", "t8", "ns8", f"b-{i}", "insert", TS, TS, None, {"emb": [9.0] * 8}, {}, {"lang": "de"})
        )
    frame: DataFrame = spark.createDataFrame(rows, schema=SOURCE_DDL)

    base_bulk: str = str(tmp_path / "bulk")
    base_merge: str = str(tmp_path / "merge")
    IcebergToLanceETL(bulk_config(base_bulk, telemetry_config, bulk=True, num_partitions=1)).run_on_dataframe(frame)
    IcebergToLanceETL(bulk_config(base_merge, telemetry_config, bulk=False, num_partitions=1)).run_on_dataframe(frame)

    bulk_tables: dict[tuple[str, str, str], pa.Table] = read_all_datasets(base_bulk)
    merge_tables: dict[tuple[str, str, str], pa.Table] = read_all_datasets(base_merge)
    assert set(bulk_tables) == set(merge_tables), f"dataset sets differ: {set(bulk_tables)} vs {set(merge_tables)}"
    assert ("orgBig", "t1", "ns1") in bulk_tables
    for trio in bulk_tables:
        assert_tables_equal(bulk_tables[trio], merge_tables[trio], str(trio))

    big_ids: list[str] = bulk_tables[("orgBig", "t1", "ns1")].column("vector_id").to_pylist()
    assert "big-del" not in big_ids, "delete-op row leaked into the bulk dataset"
    assert len(big_ids) == 400


def test_bulk_no_duplicate_keys_across_buckets(
    spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
) -> None:
    """A big trio fanned across ``K > 1`` bulk sub-buckets writes each key exactly once.

    The salt is a pure function of the merge key, so no key crosses sub-buckets and the appended
    dataset carries exactly the distinct-key count with no duplicate ``vector_id``.

    Args:
        spark: The module-scoped four-core UTC session.
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
    """
    config: ETLConfig = bulk_config(str(tmp_path), telemetry_config, bulk=True)
    frame: DataFrame = spark.createDataFrame(big_trio_rows(400), schema=SOURCE_DDL)
    plan: RoutingPlan = compute_routing_plan(frame, config)
    eligible: list[tuple[str, str, str, int]] = plan_bulk_append(plan, config)
    assert eligible and eligible[0][3] > 1, "expected the big trio to fan across more than one sub-bucket"

    IcebergToLanceETL(config).run_on_dataframe(frame)
    table: pa.Table = lance.dataset(dataset_uri(config, "orgBig", "t1", "ns1")).to_table()
    vector_ids: list[str] = table.column("vector_id").to_pylist()
    assert len(vector_ids) == 400
    assert len(set(vector_ids)) == 400, "bulk fan-out produced duplicate vector_ids"


def test_replay_falls_back_to_merge_no_duplicates(
    spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
) -> None:
    """Replaying the same window over a now-populated dataset falls back to the idempotent merge path.

    The first run bulk-appends the big trio. The second run finds the dataset non-empty, so the
    ``count_rows == 0`` eligibility guard demotes it to the merge path, whose keyed upsert converges
    without duplicating rows. The row count is therefore unchanged after the replay.

    Args:
        spark: The module-scoped four-core UTC session.
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
    """
    config: ETLConfig = bulk_config(str(tmp_path), telemetry_config, bulk=True)
    frame: DataFrame = spark.createDataFrame(big_trio_rows(400), schema=SOURCE_DDL)
    uri: str = dataset_uri(config, "orgBig", "t1", "ns1")

    IcebergToLanceETL(config).run_on_dataframe(frame)
    before: int = lance.dataset(uri).count_rows()
    assert before == 400

    replay_plan: RoutingPlan = compute_routing_plan(frame, config)
    assert plan_bulk_append(replay_plan, config) == [], "a non-empty dataset must not be bulk-eligible on replay"

    IcebergToLanceETL(config).run_on_dataframe(frame)
    after: int = lance.dataset(uri).count_rows()
    assert after == before, f"replay duplicated rows: {before} -> {after}"


def test_heterogeneous_keys_align_across_buckets(
    spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
) -> None:
    """Keys present in only some rows produce one unified schema with nulls where the key is absent.

    Because parallel sub-buckets see different key subsets, the canonical schema unifies every key
    and :func:`~lance_etl.etl.pivot.align_to_schema` null-fills the columns a slice lacks. Rows that
    do carry a key keep its value, and rows that do not read back null.

    Args:
        spark: The module-scoped four-core UTC session.
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
    """
    config: ETLConfig = bulk_config(str(tmp_path), telemetry_config, bulk=True)
    frame: DataFrame = spark.createDataFrame(big_trio_rows(400), schema=SOURCE_DDL)
    IcebergToLanceETL(config).run_on_dataframe(frame)

    table: pa.Table = lance.dataset(dataset_uri(config, "orgBig", "t1", "ns1")).to_table().sort_by("vector_id")
    assert table.schema.field("emb2").type == pa.list_(pa.float32(), 4)
    assert table.schema.field("body").type == pa.string()
    assert table.schema.field("cat").type == pa.string()

    by_id: dict[str, int] = {vid: i for i, vid in enumerate(table.column("vector_id").to_pylist())}
    emb2_values: list = table.column("emb2").to_pylist()
    body_values: list = table.column("body").to_pylist()
    cat_values: list = table.column("cat").to_pylist()
    assert emb2_values[by_id["big-0"]] == [0.0, 0.0, 1.0, 2.0]
    assert emb2_values[by_id["big-1"]] is None
    assert body_values[by_id["big-0"]] == "body text 0"
    assert body_values[by_id["big-1"]] is None
    assert cat_values[by_id["big-0"]] == "c0"
    assert cat_values[by_id["big-1"]] is None


def test_deletes_in_backfill_are_noops(spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
    """Delete-op rows against a brand-new dataset append nothing, matching the merge path's no-op.

    The bulk fan-out drops delete rows before the append, so a big trio whose window mixes inserts
    with deletes for distinct keys backfills only the inserted keys. The deleted keys are absent
    because there was never a row to remove.

    Args:
        spark: The module-scoped four-core UTC session.
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
    """
    config: ETLConfig = bulk_config(str(tmp_path), telemetry_config, bulk=True)
    rows: list[tuple] = big_trio_rows(300)
    for i in range(20):
        rows.append(
            (
                "orgBig",
                "t1",
                "ns1",
                f"del-{i}",
                "delete",
                TS,
                TS,
                3600,
                {"emb": [0.0] * 8},
                {"title": "x"},
                {"lang": "en"},
            )
        )
    frame: DataFrame = spark.createDataFrame(rows, schema=SOURCE_DDL)
    IcebergToLanceETL(config).run_on_dataframe(frame)

    table: pa.Table = lance.dataset(dataset_uri(config, "orgBig", "t1", "ns1")).to_table()
    vector_ids: set[str] = set(table.column("vector_id").to_pylist())
    assert len(vector_ids) == 300
    assert not any(vid.startswith("del-") for vid in vector_ids), "a delete-op key was appended"


def test_bulk_persists_column_roles(spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
    """A bulk run persists vector, text, and scalar roles for the pivoted columns.

    The single per-trio ``commit_batch`` is followed by a column-role merge, so the indexer can read
    each backfilled column's role back from the dataset config KV.

    Args:
        spark: The module-scoped four-core UTC session.
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
    """
    config: ETLConfig = bulk_config(str(tmp_path), telemetry_config, bulk=True)
    frame: DataFrame = spark.createDataFrame(big_trio_rows(400), schema=SOURCE_DDL)
    IcebergToLanceETL(config).run_on_dataframe(frame)

    roles: dict[str, str] = load_column_roles(lance.dataset(dataset_uri(config, "orgBig", "t1", "ns1")))
    assert roles.get("emb") == "vector"
    assert roles.get("emb2") == "vector"
    assert roles.get("title") == "text"
    assert roles.get("body") == "text"
    assert roles.get("lang") == "scalar"
    assert roles.get("cat") == "scalar"


def test_bulk_append_disabled_uses_merge(
    spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
) -> None:
    """With the fast path disabled, a big new dataset is still written correctly via the merge path.

    The kill switch ``bulk_append=False`` makes :func:`plan_bulk_append` select nothing, so no bulk
    branch is taken and the big new dataset is materialized by the merge path alone.

    Args:
        spark: The module-scoped four-core UTC session.
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
    """
    config: ETLConfig = bulk_config(str(tmp_path), telemetry_config, bulk=False, num_partitions=1)
    frame: DataFrame = spark.createDataFrame(big_trio_rows(400), schema=SOURCE_DDL)
    plan: RoutingPlan = compute_routing_plan(frame, config)
    assert plan_bulk_append(plan, config) == [], "the disabled fast path must select no trio"

    IcebergToLanceETL(config).run_on_dataframe(frame)
    table: pa.Table = lance.dataset(dataset_uri(config, "orgBig", "t1", "ns1")).to_table()
    vector_ids: list[str] = table.column("vector_id").to_pylist()
    assert len(vector_ids) == 400
    assert len(set(vector_ids)) == 400
    assert table.schema.field("emb").type == pa.list_(pa.float32(), 8)


def test_non_empty_big_dataset_skips_bulk(
    spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
) -> None:
    """A big trio whose dataset already carries a row is demoted to the merge path.

    A first window seeds the dataset with one older row. A second big-window run finds the dataset
    non-empty, so :func:`plan_bulk_append` excludes the trio and the merge path applies. The
    pre-existing row plus the new rows are all present, and the seeded key resolves last-write-wins
    to the newer value.

    Args:
        spark: The module-scoped four-core UTC session.
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
    """
    ts_old: datetime = datetime(2026, 1, 1, tzinfo=UTC)
    ts_new: datetime = datetime(2026, 7, 5, tzinfo=UTC)
    config: ETLConfig = bulk_config(str(tmp_path), telemetry_config, bulk=True, bucket_rows=50)
    uri: str = dataset_uri(config, "orgBig", "t1", "ns1")

    seed: list[tuple] = [
        (
            "orgBig",
            "t1",
            "ns1",
            "big-0",
            "insert",
            ts_old,
            ts_old,
            3600,
            {"emb": [1.0] * 8},
            {"title": "old"},
            {"lang": "en"},
        )
    ]
    IcebergToLanceETL(config).run_on_dataframe(spark.createDataFrame(seed, schema=SOURCE_DDL))
    assert lance.dataset(uri).count_rows() == 1

    window: list[tuple] = []
    for i in range(200):
        title: str = "new" if i == 0 else f"doc-{i}"
        timestamp: datetime = ts_new if i == 0 else TS
        window.append(
            (
                "orgBig",
                "t1",
                "ns1",
                f"big-{i}",
                "insert",
                timestamp,
                timestamp,
                3600,
                {"emb": [2.0] * 8},
                {"title": title},
                {"lang": "en"},
            )
        )
    window_frame: DataFrame = spark.createDataFrame(window, schema=SOURCE_DDL)
    plan: RoutingPlan = compute_routing_plan(window_frame, config)
    eligible: list[tuple[str, str, str, int]] = plan_bulk_append(plan, config)
    assert ("orgBig", "t1", "ns1") not in {(o, t, n) for o, t, n, _ in eligible}, "non-empty big trio must skip bulk"

    IcebergToLanceETL(config).run_on_dataframe(window_frame)
    table: pa.Table = lance.dataset(uri).to_table()
    vector_ids: list[str] = table.column("vector_id").to_pylist()
    assert len(vector_ids) == 200
    assert len(set(vector_ids)) == 200
    title_by_id: dict[str, str] = dict(zip(vector_ids, table.column("title").to_pylist(), strict=True))
    assert title_by_id["big-0"] == "new", "the seeded key must resolve last-write-wins to the newer row"
