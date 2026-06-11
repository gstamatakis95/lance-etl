"""ETL merge semantics, chunked-merge correctness, and a concurrent merge-plus-compaction stress test.

These run the executor-task layer directly against local-fs datasets: no Spark is involved. The concurrency test drives
two upserting threads and one compacting thread against the same dataset and asserts no data is lost.
The chunking tests verify that ``merge_batch_rows`` slices the upsert and delete tables without changing the final
dataset state, and that ``merge_batch_rows=None`` preserves the original single-commit behaviour.
"""

from __future__ import annotations

import threading
import time
from datetime import timedelta
from pathlib import Path

import lance
import pyarrow as pa
import pytest

from lance_etl.etl import ETLConfig, apply_merge, dataset_uri
from lance_etl.etl.job import table_chunks
from lance_etl.maintenance import MaintenanceConfig, compact_small_dataset
from lance_etl.telemetry import Telemetry, TelemetryConfig

ROUTING_KEY: tuple[str, str, str] = ("org1", "tenant1", "ns1")


@pytest.fixture
def etl_config(tmp_path: Path, telemetry_config: TelemetryConfig) -> ETLConfig:
    """Build an ETL configuration rooted at a temporary directory.

    Args:
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.

    Returns:
        The ETL configuration.
    """
    return ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config)


def make_group(keys: list[str], op: str = "insert", value: float = 1.0) -> pa.Table:
    """Build one routing key's change rows.

    Args:
        keys: Vector ids for the rows.
        op: Operation value for every row.
        value: Payload value for every row.

    Returns:
        A table shaped like one routed ETL group.
    """
    count: int = len(keys)
    return pa.table(
        {
            "vector_id": pa.array(keys, pa.string()),
            "org_id": pa.array([ROUTING_KEY[0]] * count),
            "tenant_id": pa.array([ROUTING_KEY[1]] * count),
            "namespace": pa.array([ROUTING_KEY[2]] * count),
            "timestamp": pa.array([1] * count, pa.int64()),
            "op": pa.array([op] * count),
            "value": pa.array([value] * count, pa.float64()),
        }
    )


def test_apply_merge_bootstrap_and_counts(etl_config: ETLConfig, telemetry: Telemetry) -> None:
    """The first merge creates the dataset and reports merge-derived counts."""
    upserted, deleted = apply_merge(etl_config, telemetry, ROUTING_KEY, make_group(["a", "b", "c"]))
    assert (upserted, deleted) == (3, 0)
    uri: str = dataset_uri(etl_config, *ROUTING_KEY)
    assert lance.dataset(uri).count_rows() == 3


def test_apply_merge_reupsert_updates_in_place(etl_config: ETLConfig, telemetry: Telemetry) -> None:
    """Re-upserting existing keys updates rows instead of duplicating them."""
    apply_merge(etl_config, telemetry, ROUTING_KEY, make_group(["a", "b", "c"], value=1.0))
    upserted, deleted = apply_merge(etl_config, telemetry, ROUTING_KEY, make_group(["a", "b"], value=2.0))
    assert (upserted, deleted) == (2, 0)
    uri: str = dataset_uri(etl_config, *ROUTING_KEY)
    table: pa.Table = lance.dataset(uri).to_table().sort_by("vector_id")
    assert table.num_rows == 3
    assert table["vector_id"].to_pylist() == ["a", "b", "c"]
    assert table["value"].to_pylist() == [2.0, 2.0, 1.0]


def make_ttl_group(keys: list[str], op: str = "insert", lifetime_days: int = 30) -> pa.Table:
    """Build one routing key's change rows carrying a per-row Duration TTL column.

    Args:
        keys: Vector ids for the rows.
        op: Operation value for every row.
        lifetime_days: The per-row lifetime stored in the ``ttl`` Duration column.

    Returns:
        A routed ETL group with an extra ``ttl`` duration column.
    """

    count: int = len(keys)
    return pa.table(
        {
            "vector_id": pa.array(keys, pa.string()),
            "org_id": pa.array([ROUTING_KEY[0]] * count),
            "tenant_id": pa.array([ROUTING_KEY[1]] * count),
            "namespace": pa.array([ROUTING_KEY[2]] * count),
            "timestamp": pa.array([1] * count, pa.int64()),
            "op": pa.array([op] * count),
            "ttl": pa.array([timedelta(days=lifetime_days)] * count, pa.duration("us")),
        }
    )


def test_apply_merge_passes_ttl_column_through(etl_config: ETLConfig, telemetry: Telemetry) -> None:
    """A per-row Duration TTL column flows through merge_insert and is refreshed on re-upsert."""

    apply_merge(etl_config, telemetry, ROUTING_KEY, make_ttl_group(["a", "b"], lifetime_days=30))
    apply_merge(etl_config, telemetry, ROUTING_KEY, make_ttl_group(["a"], lifetime_days=90))
    uri: str = dataset_uri(etl_config, *ROUTING_KEY)
    table: pa.Table = lance.dataset(uri).to_table().sort_by("vector_id")
    assert "ttl" in table.column_names
    assert table["ttl"].to_pylist() == [timedelta(days=90), timedelta(days=30)]


def test_apply_merge_delete_path(etl_config: ETLConfig, telemetry: Telemetry) -> None:
    """Delete operations physically remove rows and report merge-derived counts."""
    apply_merge(etl_config, telemetry, ROUTING_KEY, make_group(["a", "b", "c"]))
    upserted, deleted = apply_merge(etl_config, telemetry, ROUTING_KEY, make_group(["a", "c"], op="delete"))
    assert (upserted, deleted) == (0, 2)
    uri: str = dataset_uri(etl_config, *ROUTING_KEY)
    table: pa.Table = lance.dataset(uri).to_table()
    assert table["vector_id"].to_pylist() == ["b"]


def test_apply_merge_delete_on_missing_dataset(etl_config: ETLConfig, telemetry: Telemetry) -> None:
    """Deletes against a dataset that never existed report zero deletions."""
    upserted, deleted = apply_merge(etl_config, telemetry, ROUTING_KEY, make_group(["a"], op="delete"))
    assert (upserted, deleted) == (0, 0)


def test_concurrent_merges_and_compaction(
    etl_config: ETLConfig, telemetry_config: TelemetryConfig, telemetry: Telemetry
) -> None:
    """Two merge threads and one compaction thread converge without data loss."""
    iterations: int = 25
    keys_one: list[str] = [f"k{i:04d}" for i in range(0, 200)]
    keys_two: list[str] = [f"k{i:04d}" for i in range(200, 400)]
    apply_merge(etl_config, telemetry, ROUTING_KEY, make_group(keys_one[:1], value=0.0))
    uri: str = dataset_uri(etl_config, *ROUTING_KEY)
    compaction_config: MaintenanceConfig = MaintenanceConfig(
        telemetry=telemetry_config,
        commit_retries=30,
        commit_backoff_seconds=0.05,
    )
    failures: list[BaseException] = []
    compactions: list[dict[str, object]] = []
    stop_compacting: threading.Event = threading.Event()

    def upsert_loop(keys: list[str]) -> None:
        """Repeatedly upsert one key range with increasing values."""
        try:
            thread_telemetry: Telemetry = Telemetry.create(etl_config.telemetry, attach_lance_bridge=False)
            for iteration in range(1, iterations + 1):
                apply_merge(etl_config, thread_telemetry, ROUTING_KEY, make_group(keys, value=float(iteration)))
                time.sleep(0.005)
        except BaseException as exc:
            failures.append(exc)
            raise

    def compact_loop() -> None:
        """Compact the dataset repeatedly while the writers run."""
        try:
            thread_telemetry: Telemetry = Telemetry.create(etl_config.telemetry, attach_lance_bridge=False)
            while not stop_compacting.is_set():
                compactions.append(compact_small_dataset(uri, compaction_config, thread_telemetry))
                time.sleep(0.02)
        except BaseException as exc:
            failures.append(exc)
            raise

    writer_one: threading.Thread = threading.Thread(target=upsert_loop, args=(keys_one,))
    writer_two: threading.Thread = threading.Thread(target=upsert_loop, args=(keys_two,))
    compactor: threading.Thread = threading.Thread(target=compact_loop)
    writer_one.start()
    writer_two.start()
    compactor.start()
    writer_one.join()
    writer_two.join()
    stop_compacting.set()
    compactor.join()

    assert failures == []
    assert len(compactions) >= 2
    assert all(item["tier"] == "small" for item in compactions)
    table: pa.Table = lance.dataset(uri).to_table().sort_by("vector_id")
    assert table.num_rows == 400
    assert table["vector_id"].to_pylist() == sorted(keys_one + keys_two)
    assert table["value"].to_pylist() == [float(iterations)] * 400


def make_large_group(keys: list[str], op: str = "insert", value: float = 1.0) -> pa.Table:
    """Build a routing-key group table for chunked-merge tests.

    Args:
        keys: Vector ids for the rows.
        op: Operation value for every row.
        value: Payload float for every row.

    Returns:
        A table shaped like one routed ETL group with vector_id, routing, timestamp, op, and value.
    """
    count: int = len(keys)
    return pa.table(
        {
            "vector_id": pa.array(keys, pa.string()),
            "org_id": pa.array([ROUTING_KEY[0]] * count),
            "tenant_id": pa.array([ROUTING_KEY[1]] * count),
            "namespace": pa.array([ROUTING_KEY[2]] * count),
            "timestamp": pa.array([1] * count, pa.int64()),
            "op": pa.array([op] * count),
            "value": pa.array([value] * count, pa.float64()),
        }
    )


def test_chunked_upsert_matches_unchunked(
    tmp_path: Path, telemetry_config: TelemetryConfig, telemetry: Telemetry
) -> None:
    """Chunked upsert with merge_batch_rows=small produces the same final table as unchunked.

    Inserts 30 rows in two configs: one with merge_batch_rows=10 (three chunks) and one with
    merge_batch_rows=None (single commit). Both dataset paths must have identical row counts and
    identical values on a key sample.
    """
    keys: list[str] = [f"id{i:03d}" for i in range(30)]
    group: pa.Table = make_large_group(keys, value=7.0)

    chunked_path: Path = tmp_path / "chunked"
    unchunked_path: Path = tmp_path / "unchunked"

    chunked_config: ETLConfig = ETLConfig(
        base_uri=str(chunked_path),
        telemetry=telemetry_config,
        merge_batch_rows=10,
    )
    unchunked_config: ETLConfig = ETLConfig(
        base_uri=str(unchunked_path),
        telemetry=telemetry_config,
        merge_batch_rows=None,
    )

    chunked_upserted, chunked_deleted = apply_merge(chunked_config, telemetry, ROUTING_KEY, group)
    unchunked_upserted, unchunked_deleted = apply_merge(unchunked_config, telemetry, ROUTING_KEY, group)

    assert chunked_upserted == unchunked_upserted == 30
    assert chunked_deleted == unchunked_deleted == 0

    chunked_table: pa.Table = lance.dataset(dataset_uri(chunked_config, *ROUTING_KEY)).to_table().sort_by("vector_id")
    unchunked_table: pa.Table = (
        lance.dataset(dataset_uri(unchunked_config, *ROUTING_KEY)).to_table().sort_by("vector_id")
    )

    assert chunked_table.num_rows == unchunked_table.num_rows == 30
    assert chunked_table["vector_id"].to_pylist() == unchunked_table["vector_id"].to_pylist()
    assert chunked_table["value"].to_pylist() == unchunked_table["value"].to_pylist()


def test_chunked_upsert_counts_aggregate_correctly(
    tmp_path: Path, telemetry_config: TelemetryConfig, telemetry: Telemetry
) -> None:
    """Upserted counts from chunked merges are summed correctly across chunks.

    Bootstraps with 25 rows then re-upserts 15 of them plus 5 new ones. With merge_batch_rows=5
    the result must report 20 total (15 updated + 5 inserted), not just the last chunk's count.
    """
    all_keys: list[str] = [f"k{i:03d}" for i in range(25)]
    apply_merge(
        ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config),
        telemetry,
        ROUTING_KEY,
        make_large_group(all_keys, value=1.0),
    )

    update_keys: list[str] = [f"k{i:03d}" for i in range(15)] + [f"new{i}" for i in range(5)]
    chunked_config: ETLConfig = ETLConfig(
        base_uri=str(tmp_path),
        telemetry=telemetry_config,
        merge_batch_rows=5,
    )
    upserted, deleted = apply_merge(chunked_config, telemetry, ROUTING_KEY, make_large_group(update_keys, value=2.0))

    assert upserted == 20
    assert deleted == 0
    assert lance.dataset(dataset_uri(chunked_config, *ROUTING_KEY)).count_rows() == 30


def test_chunked_upsert_none_disables_chunking(
    tmp_path: Path, telemetry_config: TelemetryConfig, telemetry: Telemetry
) -> None:
    """merge_batch_rows=None commits all rows in one call, behaving identically to the legacy path."""
    keys: list[str] = [f"r{i}" for i in range(20)]
    config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config, merge_batch_rows=None)
    upserted, deleted = apply_merge(config, telemetry, ROUTING_KEY, make_large_group(keys))
    assert upserted == 20
    assert deleted == 0
    assert lance.dataset(dataset_uri(config, *ROUTING_KEY)).count_rows() == 20


def test_chunked_delete_path(tmp_path: Path, telemetry_config: TelemetryConfig, telemetry: Telemetry) -> None:
    """Delete path with merge_batch_rows=small removes exactly the targeted rows.

    Bootstraps 40 rows, then issues 15 deletes chunked at 5 per commit. Final row count must be 25.
    """
    all_keys: list[str] = [f"d{i:03d}" for i in range(40)]
    base_config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config)
    apply_merge(base_config, telemetry, ROUTING_KEY, make_large_group(all_keys))

    delete_keys: list[str] = [f"d{i:03d}" for i in range(15)]
    chunked_config: ETLConfig = ETLConfig(
        base_uri=str(tmp_path),
        telemetry=telemetry_config,
        merge_batch_rows=5,
    )
    upserted, deleted = apply_merge(chunked_config, telemetry, ROUTING_KEY, make_large_group(delete_keys, op="delete"))
    assert upserted == 0
    assert deleted == 15
    assert lance.dataset(dataset_uri(chunked_config, *ROUTING_KEY)).count_rows() == 25


def test_table_chunks_none_returns_single_element() -> None:
    """table_chunks with None returns a single-element list wrapping the original table."""

    table: pa.Table = pa.table({"x": pa.array(list(range(50)), pa.int64())})
    chunks: list[pa.Table] = table_chunks(table, None)
    assert len(chunks) == 1
    assert chunks[0] is table


def test_table_chunks_exact_multiple() -> None:
    """table_chunks slices evenly when row count divides exactly by batch_rows."""

    table: pa.Table = pa.table({"x": pa.array(list(range(30)), pa.int64())})
    chunks: list[pa.Table] = table_chunks(table, 10)
    assert len(chunks) == 3
    for chunk in chunks:
        assert chunk.num_rows == 10
    assert sum(c.num_rows for c in chunks) == 30


def test_table_chunks_remainder() -> None:
    """table_chunks produces a smaller final chunk when rows do not divide evenly."""

    table: pa.Table = pa.table({"x": pa.array(list(range(25)), pa.int64())})
    chunks: list[pa.Table] = table_chunks(table, 10)
    assert len(chunks) == 3
    assert chunks[0].num_rows == 10
    assert chunks[1].num_rows == 10
    assert chunks[2].num_rows == 5
    assert sum(c.num_rows for c in chunks) == 25


def test_table_chunks_larger_than_table() -> None:
    """table_chunks returns the original table when batch_rows exceeds num_rows."""

    table: pa.Table = pa.table({"x": pa.array(list(range(5)), pa.int64())})
    chunks: list[pa.Table] = table_chunks(table, 100)
    assert len(chunks) == 1
    assert chunks[0] is table


TS_TYPE: pa.DataType = pa.timestamp("us", tz=None)
TS_COL: str = "event_timestamp"


def make_ts_group(
    keys: list[str],
    ts_values: list[int],
    op: str = "insert",
    value: float = 1.0,
) -> pa.Table:
    """Build one routing-key group with an ``event_timestamp`` column for the cross-window guard tests.

    Args:
        keys: Vector ids for the rows.
        ts_values: Microsecond-epoch timestamp values, one per row.
        op: Operation value for every row.
        value: Payload float for every row.

    Returns:
        A table shaped like one routed ETL group carrying ``event_timestamp`` as a us-precision timestamp.
    """
    count: int = len(keys)
    return pa.table(
        {
            "vector_id": pa.array(keys, pa.string()),
            "org_id": pa.array([ROUTING_KEY[0]] * count),
            "tenant_id": pa.array([ROUTING_KEY[1]] * count),
            "namespace": pa.array([ROUTING_KEY[2]] * count),
            TS_COL: pa.array(ts_values, TS_TYPE),
            "op": pa.array([op] * count),
            "value": pa.array([value] * count, pa.float64()),
        }
    )


@pytest.fixture
def ts_config(tmp_path: Path, telemetry_config: TelemetryConfig) -> ETLConfig:
    """ETLConfig using the default ts_col (``event_timestamp``) for cross-window guard tests.

    Args:
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.

    Returns:
        ETLConfig with ts_col set to the column present in make_ts_group output.
    """
    return ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config, ts_col=TS_COL)


def test_out_of_order_update_does_not_overwrite(ts_config: ETLConfig, telemetry: Telemetry) -> None:
    """A second merge batch with an older timestamp must not overwrite the stored newer value.

    This is the primary cross-window last-write-wins guard: arrival order after ``collapse`` must
    not matter when the source carries an older timestamp than the already-stored row.
    """
    apply_merge(ts_config, telemetry, ROUTING_KEY, make_ts_group(["k1"], ts_values=[100], value=99.0))
    apply_merge(ts_config, telemetry, ROUTING_KEY, make_ts_group(["k1"], ts_values=[50], value=0.0))
    uri: str = dataset_uri(ts_config, *ROUTING_KEY)
    table: pa.Table = lance.dataset(uri).to_table()
    assert table["value"].to_pylist() == [99.0]
    assert table[TS_COL].to_pylist()[0] == pa.array([100], TS_TYPE)[0].as_py()


def test_equal_ts_replay_is_idempotent(ts_config: ETLConfig, telemetry: Telemetry) -> None:
    """Replaying the same window (same timestamp) must converge: the row must not disappear or corrupt.

    Ties (source.ts == target.ts) must apply the update so that re-running the same ETL window
    produces the same dataset state (idempotent replay).
    """
    apply_merge(ts_config, telemetry, ROUTING_KEY, make_ts_group(["k1"], ts_values=[100], value=7.0))
    apply_merge(ts_config, telemetry, ROUTING_KEY, make_ts_group(["k1"], ts_values=[100], value=7.0))
    uri: str = dataset_uri(ts_config, *ROUTING_KEY)
    table: pa.Table = lance.dataset(uri).to_table()
    assert table.num_rows == 1
    assert table["value"].to_pylist() == [7.0]


def test_newer_update_overwrites_older_stored_row(ts_config: ETLConfig, telemetry: Telemetry) -> None:
    """A source row with a higher timestamp must overwrite the stored row (forward progress).

    Confirms the guard does not block legitimate updates from a later ETL window.
    """
    apply_merge(ts_config, telemetry, ROUTING_KEY, make_ts_group(["k1"], ts_values=[100], value=1.0))
    apply_merge(ts_config, telemetry, ROUTING_KEY, make_ts_group(["k1"], ts_values=[200], value=2.0))
    uri: str = dataset_uri(ts_config, *ROUTING_KEY)
    table: pa.Table = lance.dataset(uri).to_table()
    assert table["value"].to_pylist() == [2.0]


def test_null_target_ts_is_not_overwritten(ts_config: ETLConfig, telemetry: Telemetry) -> None:
    """A row stored with NULL event_timestamp is not overwritten by the cross-window guard.

    The condition ``source.ts >= target.ts`` evaluates to NULL (treated as FALSE) when target.ts
    is NULL, so NULL-timestamp target rows are skipped by the guard. This is the documented
    accepted constraint: rows written before ts_col was added to the schema retain NULL and cannot
    be updated via upsert while the guard is active. Such rows can only be replaced by a
    delete-and-reinsert operation.

    The COALESCE-based alternative (treat NULL target ts as epoch) is not supported in the current
    lance version because the ``target.`` table-qualifier cannot appear inside function arguments
    in the DataFusion condition planner.
    """
    null_ts_group: pa.Table = pa.table(
        {
            "vector_id": pa.array(["k1"], pa.string()),
            "org_id": pa.array([ROUTING_KEY[0]]),
            "tenant_id": pa.array([ROUTING_KEY[1]]),
            "namespace": pa.array([ROUTING_KEY[2]]),
            TS_COL: pa.array([None], TS_TYPE),
            "op": pa.array(["insert"]),
            "value": pa.array([0.0], pa.float64()),
        }
    )
    apply_merge(ts_config, telemetry, ROUTING_KEY, null_ts_group)
    apply_merge(ts_config, telemetry, ROUTING_KEY, make_ts_group(["k1"], ts_values=[100], value=5.0))
    uri: str = dataset_uri(ts_config, *ROUTING_KEY)
    table: pa.Table = lance.dataset(uri).to_table()
    assert table["value"].to_pylist() == [0.0]
