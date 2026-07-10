"""Tests for the dynamic map-column pivot in the ETL and the collapse last-write-wins guarantee.

Every key of the ``vectors``, ``texts``, and ``metadata`` map columns becomes a concrete column in
the written Lance dataset. The key is the column name and the value is the column value. The pivot
runs per-dataset on the executor so each org's dataset contains only the keys that org uses. These
tests cover the end-to-end Spark pivot and the ``validate_schema`` safety checks (present-but-wrong
map column type, and a map column that is not a map).

The ``TestCollapseGuarantee`` class verifies that ``IcebergToLanceETL.collapse`` enforces at most one
row per ``(routing key, vector_id)`` before rows reach ``apply_merge``. This is the invariant that
makes chunked merge commits order-safe: because collapse runs before partitioning, no key can appear
in more than one chunk when ``merge_batch_bytes`` slices the upsert table.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import lance
import pyarrow as pa
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import lance_etl.etl.pivot as pivot_module
from lance_etl.cliutil import SPARK_CONF_DEFAULTS
from lance_etl.etl import ETLConfig, IcebergToLanceETL, dataset_uri
from lance_etl.etl.pivot import ROUTING_COLS, group_by_routing
from lance_etl.etl.plan import RoutingPlan, compute_routing_plan
from lance_etl.telemetry import TelemetryConfig

DIMENSION: int = 8


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    """Provide a local Spark session pinned to the test interpreter.

    Yields:
        A two-core local session with a UTC timezone and four shuffle partitions.
    """
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session: SparkSession = (
        SparkSession.builder.master("local[2]")
        .appName("lance-etl-pivot-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


TS: datetime = datetime(2024, 1, 1, tzinfo=UTC)


def source_schema() -> StructType:
    """Return the Spark schema of the pivot test source with the three map columns.

    Returns:
        A schema carrying the routing, key, timestamp, window, and op columns plus the vectors,
        texts, and metadata maps, matching the SQL contract column types.
    """
    return StructType(
        [
            StructField("vector_id", StringType(), False),
            StructField("org_id", StringType(), False),
            StructField("tenant_id", StringType(), False),
            StructField("namespace", StringType(), False),
            StructField("event_timestamp", TimestampType(), False),
            StructField("processing_timestamp", TimestampType(), False),
            StructField("op", StringType(), False),
            StructField("vectors", MapType(StringType(), ArrayType(FloatType())), True),
            StructField("texts", MapType(StringType(), StringType()), True),
            StructField("metadata", MapType(StringType(), StringType()), True),
        ]
    )


def pivot_config(tmp_path: Path, telemetry_config: TelemetryConfig) -> ETLConfig:
    """Build an ETL configuration that exercises the dynamic map pivot.

    Args:
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.

    Returns:
        An ETLConfig using all defaults. The ``vectors``, ``texts``, and ``metadata`` map columns are
        pivoted dynamically: every key present in the data becomes a concrete column with its type
        inferred from the Arrow data.
    """
    return ETLConfig(
        base_uri=str(tmp_path),
        telemetry=telemetry_config,
        num_partitions=4,
    )


class TestPivotEndToEnd:
    """The ETL pivots all map keys into concrete indexable columns."""

    def test_pivot_writes_concrete_columns(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """Every distinct map key becomes a concrete typed column in the written dataset.

        Vector keys land as fixed-size-list columns, text keys as string columns, and metadata
        keys as string columns. The map columns themselves are not written to Lance. A key absent
        from a row yields NULL for that column in that row.

        Args:
            spark: The module-scoped local Spark session.
            tmp_path: Pytest-provided temporary directory.
            telemetry_config: The test telemetry configuration.
        """
        config: ETLConfig = pivot_config(tmp_path, telemetry_config)
        vector_a: list[float] = [float(i) for i in range(DIMENSION)]
        vector_b: list[float] = [float(i + 1) for i in range(DIMENSION)]
        rows: list[tuple] = [
            (
                "v1",
                "o1",
                "t1",
                "n1",
                TS,
                TS,
                "insert",
                {"vector": vector_a, "ignored": vector_b},
                {"text": "alpha"},
                {"k": "a"},
            ),
            ("v2", "o1", "t1", "n1", TS, TS, "insert", {"vector": vector_b}, {}, {"k": "b"}),
        ]
        frame = spark.createDataFrame(rows, source_schema())
        IcebergToLanceETL(config).run_on_dataframe(frame)

        table: pa.Table = lance.dataset(dataset_uri(config, "o1", "t1", "n1")).to_table().sort_by("vector_id")
        assert table.schema.field("vector").type == pa.list_(pa.float32(), DIMENSION)
        assert table.schema.field("text").type == pa.string()
        assert table.schema.field("k").type == pa.string()
        assert "vectors" not in table.column_names
        assert "texts" not in table.column_names
        assert "metadata" not in table.column_names
        assert "metadata_keys" not in table.column_names
        assert "metadata_values" not in table.column_names

        by_id: dict[str, int] = {vid: i for i, vid in enumerate(table["vector_id"].to_pylist())}
        assert table["vector"].to_pylist()[by_id["v1"]] == vector_a
        assert table["text"].to_pylist()[by_id["v1"]] == "alpha"
        assert table["text"].to_pylist()[by_id["v2"]] is None
        assert table["k"].to_pylist()[by_id["v1"]] == "a"
        assert table["k"].to_pylist()[by_id["v2"]] == "b"

    def test_pivoted_columns_are_index_eligible(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """The pivoted vector column carries a fixed-size-list type and the text column a string type.

        These are exactly the column types the IVF_RQ and INVERTED index handlers require, so a
        tiny FTS index can be built directly over the pivoted text column.

        Args:
            spark: The module-scoped local Spark session.
            tmp_path: Pytest-provided temporary directory.
            telemetry_config: The test telemetry configuration.
        """
        config: ETLConfig = pivot_config(tmp_path, telemetry_config)
        rows: list[tuple] = [
            (
                str(i),
                "o1",
                "t1",
                "n1",
                TS,
                TS,
                "insert",
                {"vector": [float(i)] * DIMENSION},
                {"text": f"word{i} common"},
                {},
            )
            for i in range(8)
        ]
        frame = spark.createDataFrame(rows, source_schema())
        IcebergToLanceETL(config).run_on_dataframe(frame)

        dataset: lance.LanceDataset = lance.dataset(dataset_uri(config, "o1", "t1", "n1"))
        assert dataset.schema.field("vector").type == pa.list_(pa.float32(), DIMENSION)
        assert dataset.schema.field("text").type == pa.string()
        dataset.create_scalar_index("text", "INVERTED")
        names: set[str] = {description.name for description in dataset.describe_indices()}
        assert "text_idx" in names


def routing_batch(keys: list[tuple[str, str, str]], start: int = 0) -> pa.RecordBatch:
    """Build one record batch with a row per routing key plus a distinct payload id.

    Args:
        keys: One ``(org_id, tenant_id, namespace)`` tuple per row, in row order.
        start: First payload id, so batches concatenate without id collisions.

    Returns:
        A record batch with the three routing columns and a ``vector_id`` payload column.
    """
    return pa.RecordBatch.from_pydict(
        {
            "org_id": [k[0] for k in keys],
            "tenant_id": [k[1] for k in keys],
            "namespace": [k[2] for k in keys],
            "vector_id": [f"v{start + i}" for i in range(len(keys))],
        }
    )


class TestGroupByRouting:
    """group_by_routing splits a routing-sorted partition table into contiguous zero-copy groups."""

    ROUTING: list[str] = list(ROUTING_COLS)

    def test_empty_table_yields_nothing(self) -> None:
        """An empty partition table produces no groups."""
        table: pa.Table = pa.Table.from_batches([], schema=routing_batch([]).schema)
        assert list(group_by_routing(table, self.ROUTING)) == []

    def test_single_group(self) -> None:
        """A partition holding one routing key yields exactly that key with every row."""
        table: pa.Table = pa.Table.from_batches([routing_batch([("o1", "t1", "n1")] * 3)])
        groups: list[tuple[tuple[str, ...], pa.Table]] = list(group_by_routing(table, self.ROUTING))
        assert len(groups) == 1
        key, rows = groups[0]
        assert key == ("o1", "t1", "n1")
        assert rows.num_rows == 3

    def test_sorted_groups_match_keys_and_rows(self) -> None:
        """Sorted input yields one group per distinct key covering every row exactly once."""
        keys: list[tuple[str, str, str]] = (
            [("o1", "t1", "n1")] * 2 + [("o1", "t1", "n2")] + [("o1", "t2", "n1")] * 3 + [("o2", "t1", "n1")]
        )
        table: pa.Table = pa.Table.from_batches([routing_batch(keys)])
        groups: list[tuple[tuple[str, ...], pa.Table]] = list(group_by_routing(table, self.ROUTING))
        assert [key for key, rows in groups] == [
            ("o1", "t1", "n1"),
            ("o1", "t1", "n2"),
            ("o1", "t2", "n1"),
            ("o2", "t1", "n1"),
        ]
        assert [rows.num_rows for key, rows in groups] == [2, 1, 3, 1]
        recovered: set[str] = {vid for key, rows in groups for vid in rows["vector_id"].to_pylist()}
        assert recovered == set(table["vector_id"].to_pylist())

    def test_grouping_cost_is_independent_of_group_count(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The vectorized boundary scan issues a fixed number of compute calls, not one per group.

        The pre-rewrite implementation ran a full-table ``pc.equal`` scan per routing column per
        distinct key (quadratic at long-tail cardinality). The rewrite performs exactly one
        adjacent-inequality comparison per routing column regardless of how many thousands of
        groups the partition holds, which this pins structurally instead of with a flaky timing
        ratio.
        """
        calls: list[str] = []
        real_not_equal = pivot_module.pc.not_equal

        def counting_not_equal(*args: object, **kwargs: object) -> object:
            """Delegate to the real comparison while recording the call."""
            calls.append("not_equal")
            return real_not_equal(*args, **kwargs)

        monkeypatch.setattr(pivot_module.pc, "not_equal", counting_not_equal)
        keys: list[tuple[str, str, str]] = [(f"o{i}", "t1", "n1") for i in range(5000) for _ in range(2)]
        table: pa.Table = pa.Table.from_batches([routing_batch(sorted(keys))])
        groups: list[tuple[tuple[str, ...], pa.Table]] = list(group_by_routing(table, self.ROUTING))
        assert len(groups) == 5000
        assert all(rows.num_rows == 2 for key, rows in groups)
        assert len(calls) == len(self.ROUTING)

    def test_multi_chunk_input_groups_across_chunk_boundaries(self) -> None:
        """A group spanning two Arrow chunks is still yielded as one contiguous run."""
        first: pa.RecordBatch = routing_batch([("o1", "t1", "n1"), ("o1", "t1", "n1")], start=0)
        second: pa.RecordBatch = routing_batch([("o1", "t1", "n1"), ("o2", "t1", "n1")], start=2)
        table: pa.Table = pa.Table.from_batches([first, second])
        assert table.column("org_id").num_chunks == 2
        groups: list[tuple[tuple[str, ...], pa.Table]] = list(group_by_routing(table, self.ROUTING))
        assert [(key, rows.num_rows) for key, rows in groups] == [(("o1", "t1", "n1"), 3), (("o2", "t1", "n1"), 1)]

    def test_unsorted_input_splits_runs_without_losing_rows(self) -> None:
        """Unsorted input degrades to one group per contiguous run, never dropping or mixing rows.

        This pins the documented safety property: a key split across non-adjacent runs is yielded
        once per run with disjoint row sets, so downstream idempotent merges stay correct.
        """
        keys: list[tuple[str, str, str]] = [
            ("o1", "t1", "n1"),
            ("o2", "t1", "n1"),
            ("o1", "t1", "n1"),
        ]
        table: pa.Table = pa.Table.from_batches([routing_batch(keys)])
        groups: list[tuple[tuple[str, ...], pa.Table]] = list(group_by_routing(table, self.ROUTING))
        assert [key for key, rows in groups] == [("o1", "t1", "n1"), ("o2", "t1", "n1"), ("o1", "t1", "n1")]
        assert all(rows.num_rows == 1 for key, rows in groups)
        recovered: list[str] = [vid for key, rows in groups for vid in rows["vector_id"].to_pylist()]
        assert sorted(recovered) == sorted(table["vector_id"].to_pylist())
        for key, rows in groups:
            assert set(zip(*(rows[c].to_pylist() for c in self.ROUTING), strict=True)) == {key}


class TestShuffleWidth:
    """route_increment honors an explicit num_partitions and otherwise follows the planned width.

    Both width assertions toggle AQE off for the measurement because ``DataFrame.rdd`` under
    adaptive execution reports the post-coalesce count, which would make the expected widths
    data-dependent.
    """

    def routed_partitions(self, spark: SparkSession, config: ETLConfig) -> int:
        """Build a small increment, plan and route it, and count its shuffle partitions without AQE.

        Args:
            spark: The module-scoped local Spark session.
            config: The ETL configuration whose partitioning behavior is being measured.

        Returns:
            The routed DataFrame's partition count with adaptive execution disabled.
        """
        frame = spark.createDataFrame(
            [(f"v{i}", "o1", "t1", "n1", TS, TS, "insert", None, None, None) for i in range(8)], source_schema()
        )
        original: str = str(spark.conf.get("spark.sql.adaptive.enabled"))
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        try:
            plan: RoutingPlan = compute_routing_plan(frame, config)
            return IcebergToLanceETL(config).route_increment(frame, plan).rdd.getNumPartitions()
        finally:
            spark.conf.set("spark.sql.adaptive.enabled", original)

    def test_explicit_num_partitions_fixes_width(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """An explicit num_partitions produces exactly that many shuffle partitions."""
        config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config, num_partitions=2)
        assert self.routed_partitions(spark, config) == 2

    def test_default_plan_sizes_a_tiny_increment_to_one_partition(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """num_partitions=None lets the plan size the shuffle; a tiny single-trio increment gets one partition."""
        config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config)
        assert config.num_partitions is None
        assert self.routed_partitions(spark, config) == 1

    def test_conf_defaults_seed_a_wide_initial_partition_count(self) -> None:
        """SPARK_CONF_DEFAULTS carries the high AQE initial partition count coalescing shrinks from."""
        assert SPARK_CONF_DEFAULTS["spark.sql.adaptive.coalescePartitions.initialPartitionNum"] == "8192"
        assert SPARK_CONF_DEFAULTS["spark.sql.adaptive.enabled"] == "true"

    def test_end_to_end_with_default_partitioning(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """A full run with the AQE-sized default writes the same per-dataset contents."""
        config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config)
        rows: list[tuple] = [
            (f"v{i}", f"o{i % 3}", "t1", "n1", TS, TS, "insert", None, {"text": f"w{i}"}, {"k": str(i)})
            for i in range(9)
        ]
        frame = spark.createDataFrame(rows, source_schema())
        IcebergToLanceETL(config).run_on_dataframe(frame)
        for org in ("o0", "o1", "o2"):
            assert lance.dataset(dataset_uri(config, org, "t1", "n1")).count_rows() == 3


class TestValidateSchema:
    """validate_schema rejects a map column that is present but not a MapType."""

    TIMESTAMP_COLS: set[str] = {"event_timestamp", "processing_timestamp"}

    def make_source(self, columns: list[str], map_fields: dict[str, object] | None = None) -> MagicMock:
        """Build a mock DataFrame exposing the given columns and schema field types.

        Timestamp columns (``event_timestamp``, ``processing_timestamp``) default to
        ``TimestampType`` so the schema satisfies the SQL contract. All other columns default to
        ``StringType`` unless overridden via ``map_fields``.

        Args:
            columns: The column names the mock reports.
            map_fields: Mapping of column name to its Spark data type, overriding the defaults.

        Returns:
            A mock with ``columns`` and a ``schema.fields`` list of named typed fields.
        """
        types: dict[str, object] = map_fields or {}
        source: MagicMock = MagicMock()
        source.columns = columns
        source.schema.fields = [
            StructField(
                name,
                types.get(name, TimestampType() if name in self.TIMESTAMP_COLS else StringType()),
                True,
            )
            for name in columns  # type: ignore[arg-type]
        ]
        return source

    def base_columns(self) -> list[str]:
        """Return the always-required non-map columns.

        Returns:
            The key, timestamp, op, window, and routing columns matching the SQL contract defaults.
        """
        return [
            "vector_id",
            "event_timestamp",
            "processing_timestamp",
            "op",
            "org_id",
            "tenant_id",
            "namespace",
        ]

    def test_non_map_vectors_column_raises(self, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
        """A vectors column present in the source but not a MapType fails validation."""
        config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config)
        source: MagicMock = self.make_source([*self.base_columns(), "vectors"], {"vectors": StringType()})
        with pytest.raises(ValueError, match="must be a MapType"):
            IcebergToLanceETL(config).validate_schema(source)

    def test_non_map_metadata_column_raises(self, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
        """A metadata column present but not a MapType fails validation."""
        config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config)
        source: MagicMock = self.make_source([*self.base_columns(), "metadata"], {"metadata": StringType()})
        with pytest.raises(ValueError, match="must be a MapType"):
            IcebergToLanceETL(config).validate_schema(source)

    def test_absent_map_column_passes(self, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
        """A map column absent from the source is allowed; the pivot step skips it gracefully."""
        config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config)
        source: MagicMock = self.make_source(self.base_columns())
        IcebergToLanceETL(config).validate_schema(source)

    def test_valid_map_columns_pass(self, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
        """All three map columns present as MapType passes validation."""
        config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config)
        source: MagicMock = self.make_source(
            [*self.base_columns(), "vectors", "texts", "metadata"],
            {
                "vectors": MapType(StringType(), ArrayType(FloatType())),
                "texts": MapType(StringType(), StringType()),
                "metadata": MapType(StringType(), StringType()),
            },
        )
        IcebergToLanceETL(config).validate_schema(source)


class TestCollapseGuarantee:
    """collapse() enforces last-write-wins uniqueness per (routing key, vector_id) before apply_merge."""

    def collapse_schema(self) -> StructType:
        """Return the Spark schema used by collapse uniqueness tests.

        Returns:
            A schema with routing, key, event and processing timestamps, op, and a texts map.
        """
        return StructType(
            [
                StructField("vector_id", StringType(), False),
                StructField("org_id", StringType(), False),
                StructField("tenant_id", StringType(), False),
                StructField("namespace", StringType(), False),
                StructField("event_timestamp", TimestampType(), False),
                StructField("processing_timestamp", TimestampType(), False),
                StructField("op", StringType(), False),
                StructField("texts", MapType(StringType(), StringType()), True),
            ]
        )

    def test_duplicate_key_collapses_to_most_recent(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """Two rows for the same vector_id collapse to the most-recent event_timestamp row.

        This validates the order-safety invariant: because collapse runs before any partitioning,
        no key can appear in more than one chunk when merge_batch_bytes slices the upsert table.
        The older row is discarded and the newer row's payload is written to the dataset.

        Args:
            spark: The module-scoped local Spark session.
            tmp_path: Pytest-provided temporary directory.
            telemetry_config: The test telemetry configuration.
        """
        ts_old: datetime = datetime(2024, 1, 1, tzinfo=UTC)
        ts_new: datetime = datetime(2024, 6, 1, tzinfo=UTC)
        rows: list[tuple] = [
            ("dup_id", "o1", "t1", "ns1", ts_old, ts_old, "insert", {"txt": "old"}),
            ("dup_id", "o1", "t1", "ns1", ts_new, ts_new, "insert", {"txt": "new"}),
            ("unique_id", "o1", "t1", "ns1", ts_new, ts_new, "insert", {"txt": "only"}),
        ]
        frame = spark.createDataFrame(rows, self.collapse_schema())
        config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config, num_partitions=2)
        IcebergToLanceETL(config).run_on_dataframe(frame)

        table: pa.Table = lance.dataset(dataset_uri(config, "o1", "t1", "ns1")).to_table().sort_by("vector_id")
        assert table.num_rows == 2, f"expected 2 rows after collapse, got {table.num_rows}"
        vids: list[str] = table["vector_id"].to_pylist()
        assert vids == ["dup_id", "unique_id"]
        txt_values: list[str | None] = table["txt"].to_pylist()
        txt_by_id: dict[str, str | None] = dict(zip(vids, txt_values, strict=True))
        assert txt_by_id["dup_id"] == "new", "collapse must keep the most-recent row"

    def test_unique_keys_pass_through_unchanged(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """When all vector_ids are distinct, collapse does not drop any row.

        A group with N unique vector_ids must produce exactly N rows in the written dataset.

        Args:
            spark: The module-scoped local Spark session.
            tmp_path: Pytest-provided temporary directory.
            telemetry_config: The test telemetry configuration.
        """
        ts: datetime = datetime(2024, 3, 15, tzinfo=UTC)
        rows: list[tuple] = [(f"v{i}", "o2", "t2", "ns2", ts, ts, "insert", {"txt": f"val{i}"}) for i in range(10)]
        frame = spark.createDataFrame(rows, self.collapse_schema())
        config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config, num_partitions=2)
        IcebergToLanceETL(config).run_on_dataframe(frame)

        dataset = lance.dataset(dataset_uri(config, "o2", "t2", "ns2"))
        assert dataset.count_rows() == 10


class TestSaltedEquivalence:
    """Salting a big dataset across key-hash sub-buckets yields the same merged result as no salt."""

    def nullable_routing_schema(self) -> StructType:
        """Return the pivot source schema with nullable routing columns.

        Returns:
            The :func:`source_schema` fields with every routing column marked nullable, so a
            null-routing row can flow in and exercise the Spark-level drop filter.
        """
        routing_names: set[str] = {"org_id", "tenant_id", "namespace"}
        return StructType(
            [StructField(f.name, f.dataType, f.nullable or f.name in routing_names) for f in source_schema().fields]
        )

    def make_rows(self) -> list[tuple]:
        """Build an increment with two orgs, duplicate keys, and one null-routing row.

        Returns:
            Rows covering 24 unique o1 keys, 8 unique o2 keys, a stale duplicate for one o1 key,
            and one row with a NULL org_id that must be dropped at the Spark level.
        """
        ts_old: datetime = datetime(2023, 1, 1, tzinfo=UTC)
        rows: list[tuple] = [
            (
                f"v{i:02d}",
                "o1" if i % 4 else "o2",
                "t1",
                "n1",
                TS,
                TS,
                "insert",
                {"vector": [float(i)] * DIMENSION},
                {"text": f"word{i}"},
                {"k": str(i)},
            )
            for i in range(32)
        ]
        rows.append(("v01", "o1", "t1", "n1", ts_old, ts_old, "insert", None, {"text": "stale"}, {"k": "stale"}))
        rows.append(("vnull", None, "t1", "n1", TS, TS, "insert", None, {"text": "dropped"}, {}))
        return rows

    def test_salted_run_matches_unsalted(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """A salted run produces datasets identical to an unsalted run over the same input.

        The salted config picks a ``bucket_rows`` small enough that both orgs exceed it, so the
        plan fans each dataset across up to ``max_buckets_per_dataset`` key-hash sub-buckets. The
        unsalted config keeps the default multi-million ``bucket_rows`` so no dataset is big and
        the shuffle carries no salt. Because the salt is a pure function of the merge key, every
        row of a key lands in one partition, so both runs write the same rows and the same
        last-write-wins winner per key. The null-routing row is dropped by the Spark-level filter
        in both runs.

        Args:
            spark: The module-scoped local Spark session.
            tmp_path: Pytest-provided temporary directory.
            telemetry_config: The test telemetry configuration.
        """
        frame = spark.createDataFrame(self.make_rows(), self.nullable_routing_schema())
        salted_config: ETLConfig = ETLConfig(
            base_uri=str(tmp_path / "salted"),
            telemetry=telemetry_config,
            num_partitions=2,
            bucket_rows=5,
            max_buckets_per_dataset=4,
        )
        unsalted_config: ETLConfig = ETLConfig(
            base_uri=str(tmp_path / "unsalted"),
            telemetry=telemetry_config,
            num_partitions=2,
        )
        IcebergToLanceETL(salted_config).run_on_dataframe(frame)
        IcebergToLanceETL(unsalted_config).run_on_dataframe(frame)

        for org, expected_rows in (("o1", 24), ("o2", 8)):
            salted: pa.Table = (
                lance.dataset(dataset_uri(salted_config, org, "t1", "n1")).to_table().sort_by("vector_id")
            )
            unsalted: pa.Table = (
                lance.dataset(dataset_uri(unsalted_config, org, "t1", "n1")).to_table().sort_by("vector_id")
            )
            assert salted.num_rows == expected_rows
            assert sorted(salted.schema.names) == sorted(unsalted.schema.names)
            ordered_names: list[str] = sorted(salted.schema.names)
            assert salted.select(ordered_names).equals(unsalted.select(ordered_names))

        o1_table: pa.Table = lance.dataset(dataset_uri(salted_config, "o1", "t1", "n1")).to_table()
        text_by_id: dict[str, str | None] = dict(
            zip(o1_table["vector_id"].to_pylist(), o1_table["text"].to_pylist(), strict=True)
        )
        assert text_by_id["v01"] == "word1", "the stale duplicate must lose to the newer event"
        assert "vnull" not in text_by_id

    def test_plan_identifies_the_big_dataset(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """compute_routing_plan flags the org that exceeds bucket_rows and salts it across sub-buckets.

        Args:
            spark: The module-scoped local Spark session.
            tmp_path: Pytest-provided temporary directory.
            telemetry_config: The test telemetry configuration.
        """
        frame = spark.createDataFrame(self.make_rows(), self.nullable_routing_schema())
        config: ETLConfig = ETLConfig(
            base_uri=str(tmp_path),
            telemetry=telemetry_config,
            bucket_rows=5,
            max_buckets_per_dataset=4,
        )
        plan: RoutingPlan = compute_routing_plan(frame, config)

        assert plan.trio_count == 2
        assert plan.total_rows == 33
        assert plan.null_routing_rows == 1
        big: dict[tuple[str, str, str], int] = {(o, t, n): k for (o, t, n, k) in plan.big_trios}
        assert big[("o1", "t1", "n1")] == 4
        assert big[("o2", "t1", "n1")] == 2
