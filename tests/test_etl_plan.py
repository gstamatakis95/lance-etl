"""Tests for the adaptive routing plan and salted shuffle in :mod:`lance_etl.etl.plan`.

The pure-math tests pin the two sizing laws without a Spark session: :func:`bucket_count` (buckets
per big dataset) and :func:`shuffle_partition_count` (routing-shuffle width sized by the larger of a
row floor and a dataset floor). The Spark-backed class exercises :func:`compute_routing_plan` (one
``groupBy`` scan producing counts, big trios, and the null-routing drop count) and
:func:`apply_salted_shuffle` (schema-preserving, key-disjoint fan-out of big datasets), and is marked
``integration`` because it starts a local Spark session.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from lance_etl.etl import ETLConfig
from lance_etl.etl.plan import (
    MAX_SHUFFLE_PARTITIONS,
    SALT_BUCKETS_COL,
    RoutingPlan,
    apply_salted_shuffle,
    bucket_count,
    compute_routing_plan,
    shuffle_partition_count,
)
from lance_etl.telemetry import TelemetryConfig


def plan_config(tmp_path: Path, telemetry_config: TelemetryConfig, **overrides: object) -> ETLConfig:
    """Build an ETLConfig for the plan tests with the sizing tunables under test.

    Args:
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
        **overrides: Sizing-tunable keyword overrides forwarded to :class:`ETLConfig`.

    Returns:
        An ETLConfig rooted at ``tmp_path`` carrying the overrides.
    """
    return ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config, **overrides)


class TestBucketCount:
    """bucket_count scales sub-buckets with rows and caps at max_buckets."""

    def test_bucket_count_small_org_is_one(self) -> None:
        """A dataset smaller than one bucket's rows gets a single sub-bucket."""
        assert bucket_count(rows=1_000, bucket_rows=2_000_000, max_buckets=32) == 1

    def test_bucket_count_scales_with_rows(self) -> None:
        """Ten million rows over a two-million bucket size yields five sub-buckets."""
        assert bucket_count(rows=10_000_000, bucket_rows=2_000_000, max_buckets=32) == 5

    def test_bucket_count_caps_at_max_buckets(self) -> None:
        """A dataset far larger than max_buckets buckets is capped at max_buckets."""
        assert bucket_count(rows=10_000_000_000, bucket_rows=2_000_000, max_buckets=32) == 32


class TestShufflePartitionCount:
    """shuffle_partition_count sizes the shuffle by the larger of the row and dataset floors."""

    def test_partition_count_row_floor_dominates(self, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
        """Many rows over few trios lets the row floor set the width."""
        config: ETLConfig = plan_config(tmp_path, telemetry_config, bucket_rows=2_000_000, datasets_per_task=64)
        assert shuffle_partition_count(total_rows=100_000_000, trio_count=2, config=config) == 50

    def test_partition_count_dataset_floor_dominates(self, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
        """30_000 trios with datasets_per_task=64 pins the width to ceil(30000/64)=469.

        This is the long-tail scaling law: a fleet of tens of thousands of tiny datasets must not
        serialize into a handful of tasks even when the total row count is small.
        """
        config: ETLConfig = plan_config(tmp_path, telemetry_config, bucket_rows=2_000_000, datasets_per_task=64)
        assert shuffle_partition_count(total_rows=100, trio_count=30_000, config=config) == 469

    def test_partition_count_clamps_to_max(self, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
        """A row floor beyond MAX_SHUFFLE_PARTITIONS is clamped to the ceiling."""
        config: ETLConfig = plan_config(tmp_path, telemetry_config, bucket_rows=1)
        assert (
            shuffle_partition_count(total_rows=MAX_SHUFFLE_PARTITIONS + 1_000, trio_count=1, config=config)
            == MAX_SHUFFLE_PARTITIONS
        )

    def test_partition_count_num_partitions_override_wins(
        self, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """An explicit num_partitions is returned verbatim regardless of rows or trios."""
        config: ETLConfig = plan_config(tmp_path, telemetry_config, num_partitions=7)
        assert shuffle_partition_count(total_rows=10_000_000_000, trio_count=99_999, config=config) == 7

    def test_partition_count_empty_increment_is_at_least_one(
        self, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """An increment with no rows and no trios still gets one partition."""
        config: ETLConfig = plan_config(tmp_path, telemetry_config)
        assert shuffle_partition_count(total_rows=0, trio_count=0, config=config) == 1


class TestRoutingPlanTotalBuckets:
    """RoutingPlan.total_buckets sums big-trio buckets plus one per remaining trio."""

    def test_routing_plan_total_buckets(self) -> None:
        """Two big trios (K=4 and K=3) among ten trios yield 4+3+(10-2)=15 total buckets."""
        plan: RoutingPlan = RoutingPlan(
            total_rows=1_000,
            trio_count=10,
            big_trios=[("o1", "t1", "n1", 4), ("o2", "t1", "n1", 3)],
            num_partitions=8,
            null_routing_rows=0,
        )
        assert plan.total_buckets == 15


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
        .appName("lance-etl-plan-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def routing_schema(nullable_routing: bool = False) -> StructType:
    """Return a minimal source schema carrying the routing columns and a key column.

    Args:
        nullable_routing: Whether the routing columns accept NULL, so a null-routing row can be fed
            to :func:`compute_routing_plan` to exercise the null-routing count.

    Returns:
        A schema with ``org_id``, ``tenant_id``, ``namespace``, and ``vector_id``.
    """
    return StructType(
        [
            StructField("org_id", StringType(), nullable_routing),
            StructField("tenant_id", StringType(), nullable_routing),
            StructField("namespace", StringType(), nullable_routing),
            StructField("vector_id", StringType(), False),
        ]
    )


def routing_rows(spec: dict[tuple[str, str, str], int]) -> list[tuple[str, str, str, str]]:
    """Expand a trio-to-count spec into raw source rows with distinct vector ids.

    Args:
        spec: One ``(org_id, tenant_id, namespace) -> row count`` entry per trio.

    Returns:
        The flattened rows, each with a unique vector id.
    """
    rows: list[tuple[str, str, str, str]] = []
    index: int = 0
    for (org, tenant, namespace), count in spec.items():
        for _ in range(count):
            rows.append((org, tenant, namespace, f"v{index}"))
            index += 1
    return rows


@pytest.mark.integration
class TestComputeRoutingPlan:
    """compute_routing_plan derives counts, big trios, and the null-routing drop in one scan."""

    def test_compute_routing_plan_counts_and_finds_big_trios(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """The plan counts every trio and flags only the trio exceeding bucket_rows.

        With bucket_rows=5, the 12-row ``o1`` trio is big with K=ceil(12/5)=3, while the small
        ``o2`` and ``o3`` trios stay unsalted. total_rows and trio_count cover all three trios.
        """
        rows: list[tuple[str, str, str, str]] = routing_rows(
            {("o1", "t1", "n1"): 12, ("o2", "t1", "n1"): 2, ("o3", "t1", "n1"): 3}
        )
        frame: DataFrame = spark.createDataFrame(rows, routing_schema())
        config: ETLConfig = plan_config(tmp_path, telemetry_config, bucket_rows=5, max_buckets_per_dataset=32)
        plan: RoutingPlan = compute_routing_plan(frame, config)

        assert plan.trio_count == 3
        assert plan.total_rows == 17
        assert plan.null_routing_rows == 0
        big: dict[tuple[str, str, str], int] = {(o, t, n): k for (o, t, n, k) in plan.big_trios}
        assert big == {("o1", "t1", "n1"): 3}

    def test_compute_routing_plan_counts_null_routing_rows(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """Null-routing rows are counted separately and excluded from total_rows and trio_count."""
        rows: list[tuple[str | None, str | None, str | None, str]] = [
            ("o1", "t1", "n1", "v0"),
            ("o1", "t1", "n1", "v1"),
            (None, "t1", "n1", "vn0"),
            ("o2", None, "n1", "vn1"),
        ]
        frame: DataFrame = spark.createDataFrame(rows, routing_schema(nullable_routing=True))
        config: ETLConfig = plan_config(tmp_path, telemetry_config)
        plan: RoutingPlan = compute_routing_plan(frame, config)

        assert plan.null_routing_rows == 2
        assert plan.total_rows == 2
        assert plan.trio_count == 1

    def test_compute_routing_plan_empty_source(
        self, spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
    ) -> None:
        """An empty increment yields zero counts and at least one shuffle partition."""
        frame: DataFrame = spark.createDataFrame([], routing_schema()).limit(0)
        config: ETLConfig = plan_config(tmp_path, telemetry_config)
        plan: RoutingPlan = compute_routing_plan(frame, config)

        assert plan.total_rows == 0
        assert plan.trio_count == 0
        assert plan.null_routing_rows == 0
        assert plan.num_partitions >= 1


@pytest.mark.integration
class TestApplySaltedShuffle:
    """apply_salted_shuffle preserves the input schema and keeps every key key-disjoint."""

    def salted_frame(self, spark: SparkSession) -> DataFrame:
        """Build a single-big-trio frame with a payload column beyond the routing key.

        Args:
            spark: The module-scoped local Spark session.

        Returns:
            A frame carrying the routing columns, ``vector_id``, and an extra ``payload`` column.
        """
        schema: StructType = StructType(
            [
                StructField("org_id", StringType(), False),
                StructField("tenant_id", StringType(), False),
                StructField("namespace", StringType(), False),
                StructField("vector_id", StringType(), False),
                StructField("payload", StringType(), False),
            ]
        )
        rows: list[tuple[str, str, str, str, str]] = [("o1", "t1", "n1", f"v{i}", f"p{i}") for i in range(40)]
        return spark.createDataFrame(rows, schema)

    def big_plan(self, num_partitions: int, k: int) -> RoutingPlan:
        """Build a plan whose one big trio matches the salted frame's routing values.

        Args:
            num_partitions: The planned shuffle width.
            k: The big trio's sub-bucket count.

        Returns:
            A RoutingPlan salting ``(o1, t1, n1)`` across ``k`` buckets.
        """
        return RoutingPlan(
            total_rows=40,
            trio_count=1,
            big_trios=[("o1", "t1", "n1", k)],
            num_partitions=num_partitions,
            null_routing_rows=0,
        )

    def test_salted_shuffle_output_schema_equals_input(self, spark: SparkSession) -> None:
        """The salt helper column is dropped so the output schema equals the input schema exactly."""
        frame: DataFrame = self.salted_frame(spark)
        routed: DataFrame = apply_salted_shuffle(frame, self.big_plan(num_partitions=4, k=4))

        assert set(routed.columns) == set(frame.columns)
        assert SALT_BUCKETS_COL not in routed.columns

    def test_same_key_lands_in_one_partition(self, spark: SparkSession) -> None:
        """Every merge key lands in exactly one partition, salting notwithstanding.

        The salt is a pure function of the merge key, so a key never fans across partitions. This
        pins the no-primary-key duplicate-insert defense: concurrent merge writers must be
        key-disjoint because Lance has no insert-side conflict detection. The assertion holds under
        AQE because coalescing merges partitions but never splits a key.
        """
        frame: DataFrame = self.salted_frame(spark)
        routed: DataFrame = apply_salted_shuffle(frame, self.big_plan(num_partitions=8, k=4))

        per_key: DataFrame = (
            routed.withColumn("partition_id", F.spark_partition_id())
            .groupBy("vector_id")
            .agg(F.countDistinct("partition_id").alias("partitions"))
        )
        assert per_key.where(F.col("partitions") > 1).count() == 0
        assert per_key.count() == 40

    def test_no_big_trios_keeps_planned_width(self, spark: SparkSession) -> None:
        """With no big trios the routed frame carries exactly plan.num_partitions partitions.

        AQE is toggled off for the width measurement because ``DataFrame.rdd`` under adaptive
        execution reports the post-coalesce count.
        """
        frame: DataFrame = self.salted_frame(spark)
        plan: RoutingPlan = RoutingPlan(
            total_rows=40, trio_count=1, big_trios=[], num_partitions=3, null_routing_rows=0
        )
        original: str = str(spark.conf.get("spark.sql.adaptive.enabled"))
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        try:
            routed: DataFrame = apply_salted_shuffle(frame, plan)
            assert routed.rdd.getNumPartitions() == 3
        finally:
            spark.conf.set("spark.sql.adaptive.enabled", original)

    def wide_frame(self, spark: SparkSession) -> DataFrame:
        """Build a many-row frame spanning several trios for an AQE width measurement.

        Args:
            spark: The module-scoped local Spark session.

        Returns:
            A frame with the routing columns, ``vector_id``, and a ``payload`` column, carrying a few
            hundred rows spread across a handful of trios so AQE has real bytes to consider coalescing.
        """
        schema: StructType = StructType(
            [
                StructField("org_id", StringType(), False),
                StructField("tenant_id", StringType(), False),
                StructField("namespace", StringType(), False),
                StructField("vector_id", StringType(), False),
                StructField("payload", StringType(), False),
            ]
        )
        rows: list[tuple[str, str, str, str, str]] = [
            (f"o{i % 4}", f"t{i % 4}", f"n{i % 4}", f"v{i}", f"p{i}") for i in range(400)
        ]
        return spark.createDataFrame(rows, schema)

    def test_planned_width_holds_with_aqe_enabled(self, spark: SparkSession) -> None:
        """The planned width survives AQE-on because repartition(N, *cols) is by-number, not coalesced.

        Production runs AQE-on. This pins that the dataset-count floor is honoured under adaptive
        execution: ``repartition(N, *cols)`` is a by-number repartition that Spark AQE does not
        coalesce, unlike by-column, rebalance, or ensure-requirements shuffles. With no big trios the
        routed frame therefore keeps exactly ``plan.num_partitions`` partitions.
        """
        frame: DataFrame = self.wide_frame(spark)
        plan: RoutingPlan = RoutingPlan(
            total_rows=400, trio_count=4, big_trios=[], num_partitions=7, null_routing_rows=0
        )
        original_aqe: str = str(spark.conf.get("spark.sql.adaptive.enabled"))
        original_coalesce: str = str(spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled"))
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        try:
            routed: DataFrame = apply_salted_shuffle(frame, plan)
            assert routed.rdd.getNumPartitions() == plan.num_partitions
        finally:
            spark.conf.set("spark.sql.adaptive.enabled", original_aqe)
            spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", original_coalesce)

    def test_planned_width_holds_with_aqe_enabled_and_big_trios(self, spark: SparkSession) -> None:
        """The salted-join path also keeps the planned width under AQE-on.

        The broadcast salt-join adds a shuffle stage, and this pins that the by-number repartition on
        ``(routing cols, salt)`` still resists AQE coalescing when a big trio is present.
        """
        frame: DataFrame = self.wide_frame(spark)
        plan: RoutingPlan = RoutingPlan(
            total_rows=400,
            trio_count=4,
            big_trios=[("o0", "t0", "n0", 4)],
            num_partitions=7,
            null_routing_rows=0,
        )
        original_aqe: str = str(spark.conf.get("spark.sql.adaptive.enabled"))
        original_coalesce: str = str(spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled"))
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        try:
            routed: DataFrame = apply_salted_shuffle(frame, plan)
            assert routed.rdd.getNumPartitions() == plan.num_partitions
        finally:
            spark.conf.set("spark.sql.adaptive.enabled", original_aqe)
            spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", original_coalesce)
