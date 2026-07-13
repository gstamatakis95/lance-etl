"""ETL merge semantics, chunked-merge correctness, and a concurrent merge-plus-compaction stress test.

These run the executor-task layer directly against local-fs datasets: no Spark is involved. The concurrency test drives
two upserting threads and one compacting thread against the same dataset and asserts no data is lost.
The chunking tests verify that ``merge_batch_bytes`` slices the upsert and delete tables without changing the final
dataset state, and that ``merge_batch_bytes=None`` preserves the original single-commit behaviour.
"""

from __future__ import annotations

import threading
import time
from datetime import timedelta
from pathlib import Path
from unittest.mock import MagicMock

import lance
import numpy as np
import pyarrow as pa
import pytest
from conftest import compact_dataset_inline

from lance_etl.etl import ETLConfig, apply_merge, dataset_uri
from lance_etl.etl.sink import dataset_absent, open_or_bootstrap, run_delete_chunk, table_chunks
from lance_etl.maintenance import MaintenanceConfig
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
                compactions.append(compact_dataset_inline(uri, compaction_config, thread_telemetry))
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
    assert all(int(item["tasks"]) >= 1 for item in compactions)
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
    """Chunked upsert with merge_batch_bytes=tiny produces the same final table as unchunked.

    Inserts 30 rows in two configs: one with a byte budget small enough to force multiple chunks
    and one with merge_batch_bytes=None (single commit). Both dataset paths must have identical
    row counts and identical values on a key sample. The budget is derived from the group table's
    actual nbytes so the test is not sensitive to column encoding details.

    Args:
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
        telemetry: The telemetry facade fixture.
    """
    keys: list[str] = [f"id{i:03d}" for i in range(30)]
    group: pa.Table = make_large_group(keys, value=7.0)
    bytes_per_row: int = max(1, group.nbytes // group.num_rows)
    tiny_budget: int = bytes_per_row * 10

    chunked_path: Path = tmp_path / "chunked"
    unchunked_path: Path = tmp_path / "unchunked"

    chunked_config: ETLConfig = ETLConfig(
        base_uri=str(chunked_path),
        telemetry=telemetry_config,
        merge_batch_bytes=tiny_budget,
    )
    unchunked_config: ETLConfig = ETLConfig(
        base_uri=str(unchunked_path),
        telemetry=telemetry_config,
        merge_batch_bytes=None,
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

    Bootstraps with 25 rows then re-upserts 15 of them plus 5 new ones. With a byte budget that
    forces 5-row chunks, the result must report 20 total (15 updated + 5 inserted), not just the
    last chunk's count. The budget is derived from the group table's actual nbytes.

    Args:
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
        telemetry: The telemetry facade fixture.
    """
    all_keys: list[str] = [f"k{i:03d}" for i in range(25)]
    apply_merge(
        ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config),
        telemetry,
        ROUTING_KEY,
        make_large_group(all_keys, value=1.0),
    )

    update_keys: list[str] = [f"k{i:03d}" for i in range(15)] + [f"new{i}" for i in range(5)]
    update_group: pa.Table = make_large_group(update_keys, value=2.0)
    bytes_per_row: int = max(1, update_group.nbytes // update_group.num_rows)
    five_row_budget: int = bytes_per_row * 5
    chunked_config: ETLConfig = ETLConfig(
        base_uri=str(tmp_path),
        telemetry=telemetry_config,
        merge_batch_bytes=five_row_budget,
    )
    upserted, deleted = apply_merge(chunked_config, telemetry, ROUTING_KEY, update_group)

    assert upserted == 20
    assert deleted == 0
    assert lance.dataset(dataset_uri(chunked_config, *ROUTING_KEY)).count_rows() == 30


def test_chunked_upsert_none_disables_chunking(
    tmp_path: Path, telemetry_config: TelemetryConfig, telemetry: Telemetry
) -> None:
    """merge_batch_bytes=None commits all rows in one call, behaving identically to the unchunked path."""
    keys: list[str] = [f"r{i}" for i in range(20)]
    config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config, merge_batch_bytes=None)
    upserted, deleted = apply_merge(config, telemetry, ROUTING_KEY, make_large_group(keys))
    assert upserted == 20
    assert deleted == 0
    assert lance.dataset(dataset_uri(config, *ROUTING_KEY)).count_rows() == 20


def test_chunked_delete_path(tmp_path: Path, telemetry_config: TelemetryConfig, telemetry: Telemetry) -> None:
    """Delete path with merge_batch_bytes=tiny removes exactly the targeted rows.

    Bootstraps 40 rows, then issues 15 deletes with a byte budget that forces ~5-row chunks.
    Final row count must be 25. The budget is derived from the delete group's actual nbytes.

    Args:
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
        telemetry: The telemetry facade fixture.
    """
    all_keys: list[str] = [f"d{i:03d}" for i in range(40)]
    base_config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config)
    apply_merge(base_config, telemetry, ROUTING_KEY, make_large_group(all_keys))

    delete_keys: list[str] = [f"d{i:03d}" for i in range(15)]
    delete_group: pa.Table = make_large_group(delete_keys, op="delete")
    bytes_per_row: int = max(1, delete_group.nbytes // delete_group.num_rows)
    five_row_budget: int = bytes_per_row * 5
    chunked_config: ETLConfig = ETLConfig(
        base_uri=str(tmp_path),
        telemetry=telemetry_config,
        merge_batch_bytes=five_row_budget,
    )
    upserted, deleted = apply_merge(chunked_config, telemetry, ROUTING_KEY, delete_group)
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
    """table_chunks slices evenly when the byte budget divides the table into equal-row chunks.

    Uses a byte budget equal to exactly one third of the table's total bytes so that the derived
    rows-per-chunk is precisely num_rows / 3, producing three equal slices.
    """
    table: pa.Table = pa.table({"x": pa.array(list(range(30)), pa.int64())})
    bytes_per_row: int = max(1, table.nbytes // table.num_rows)
    budget: int = bytes_per_row * 10
    chunks: list[pa.Table] = table_chunks(table, budget)
    assert len(chunks) == 3
    assert sum(c.num_rows for c in chunks) == 30


def test_table_chunks_remainder() -> None:
    """table_chunks produces a smaller final chunk when rows do not divide evenly.

    Uses a byte budget equal to 10 rows worth of bytes on a 25-row table, which yields
    three chunks: 10, 10, and 5 rows.
    """
    table: pa.Table = pa.table({"x": pa.array(list(range(25)), pa.int64())})
    bytes_per_row: int = max(1, table.nbytes // table.num_rows)
    budget: int = bytes_per_row * 10
    chunks: list[pa.Table] = table_chunks(table, budget)
    assert len(chunks) == 3
    assert sum(c.num_rows for c in chunks) == 25
    assert chunks[2].num_rows < chunks[0].num_rows


def test_table_chunks_budget_larger_than_table() -> None:
    """table_chunks returns the original table when the byte budget exceeds the table's total bytes."""
    table: pa.Table = pa.table({"x": pa.array(list(range(5)), pa.int64())})
    chunks: list[pa.Table] = table_chunks(table, table.nbytes * 10)
    assert len(chunks) == 1
    assert chunks[0] is table


def make_wide_float32_group(num_rows: int, dim: int = 128) -> pa.Table:
    """Build a routing-key group with a fixed-size-list float32 vector column.

    Args:
        num_rows: Number of rows to generate.
        dim: Vector dimensionality (number of float32 elements per row).

    Returns:
        A table with a float32 FSL vector column alongside routing and scalar columns.
    """
    vectors: pa.Array = pa.array(
        [list(np.zeros(dim, dtype="float32"))] * num_rows,
        pa.list_(pa.float32(), dim),
    )
    return pa.table(
        {
            "vector_id": pa.array([f"f32_{i}" for i in range(num_rows)], pa.string()),
            "org_id": pa.array([ROUTING_KEY[0]] * num_rows),
            "tenant_id": pa.array([ROUTING_KEY[1]] * num_rows),
            "namespace": pa.array([ROUTING_KEY[2]] * num_rows),
            "op": pa.array(["insert"] * num_rows),
            "vec": vectors,
        }
    )


def test_float32_wide_row_chunks_more_than_uint8_under_same_budget() -> None:
    """A wide float32-vector table produces more chunks than a narrow uint8 table of equal row count.

    This is the sift1m OOM repro: 128-dim float32 rows are 4x wider than uint8 rows of the same
    dimension. Under an identical byte budget both tables must chunk, but the float32 table must
    split into strictly more pieces, confirming that byte-based budgeting adapts to dtype.
    """
    num_rows: int = 400_000
    dim: int = 128
    budget: int = 64 * 1024 * 1024

    float32_table: pa.Table = make_wide_float32_group(num_rows, dim)
    uint8_table: pa.Table = pa.table(
        {
            "vector_id": pa.array([f"u8_{i}" for i in range(num_rows)], pa.string()),
            "org_id": pa.array([ROUTING_KEY[0]] * num_rows),
            "tenant_id": pa.array([ROUTING_KEY[1]] * num_rows),
            "namespace": pa.array([ROUTING_KEY[2]] * num_rows),
            "op": pa.array(["insert"] * num_rows),
            "vec": pa.array(
                [[0] * dim] * num_rows,
                pa.list_(pa.uint8(), dim),
            ),
        }
    )

    float32_chunks: list[pa.Table] = table_chunks(float32_table, budget)
    uint8_chunks: list[pa.Table] = table_chunks(uint8_table, budget)

    assert len(float32_chunks) > 1, "float32 table must be chunked under 64 MiB budget"
    assert len(uint8_chunks) > 1, "uint8 table must be chunked under 64 MiB budget"
    assert len(float32_chunks) > len(uint8_chunks), "float32 rows are wider so more chunks are expected"
    assert sum(c.num_rows for c in float32_chunks) == num_rows
    assert sum(c.num_rows for c in uint8_chunks) == num_rows
    float32_bytes_per_row: int = max(1, float32_table.nbytes // float32_table.num_rows)
    rows_per_chunk: int = max(1, budget // float32_bytes_per_row)
    for chunk in float32_chunks:
        assert chunk.num_rows <= rows_per_chunk, (
            f"chunk has more rows than budget allows: {chunk.num_rows} > {rows_per_chunk}"
        )


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

    Args:
        ts_config: The timestamp-guarded ETL configuration fixture.
        telemetry: The telemetry facade fixture.
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

    Args:
        ts_config: The timestamp-guarded ETL configuration fixture.
        telemetry: The telemetry facade fixture.
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

    Args:
        ts_config: The timestamp-guarded ETL configuration fixture.
        telemetry: The telemetry facade fixture.
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

    Args:
        ts_config: The timestamp-guarded ETL configuration fixture.
        telemetry: The telemetry facade fixture.
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


def test_stale_cross_window_delete_removes_newer_row(ts_config: ETLConfig, telemetry: Telemetry) -> None:
    """A delete carrying an older timestamp still removes a newer stored row.

    This pins the documented cross-window stale-delete gap (ADR 0034, docs/adr/etl-and-data-model.md).
    Upserts are guarded by ``source.ts >= target.ts`` so an older update cannot overwrite a newer
    row, but ``when_matched_delete`` takes no condition parameter in Lance 8.0.0, so a stale delete
    is not guarded and removes the row regardless of its timestamp. This is the accepted current
    behavior, not a bug: the test asserts it so any future change to the delete guard is caught.

    Args:
        ts_config: The timestamp-guarded ETL configuration fixture.
        telemetry: The telemetry facade fixture.
    """
    apply_merge(ts_config, telemetry, ROUTING_KEY, make_ts_group(["k1"], ts_values=[200], value=9.0))
    upserted, deleted = apply_merge(
        ts_config, telemetry, ROUTING_KEY, make_ts_group(["k1"], ts_values=[100], op="delete")
    )
    assert (upserted, deleted) == (0, 1)
    uri: str = dataset_uri(ts_config, *ROUTING_KEY)
    assert lance.dataset(uri).count_rows() == 0


def test_dataset_absent_classifies_only_genuine_absence() -> None:
    """dataset_absent is True only for FileNotFoundError and the lance not-found ValueError rendering.

    pylance maps every dataset-load failure to ValueError, so the sink must distinguish a genuinely
    missing dataset (safe delete no-op) from a transient or fatal open error (must re-raise) by the
    lance not-found message marker.
    """
    assert dataset_absent(FileNotFoundError("no such file"))
    assert dataset_absent(ValueError("Dataset at path /tmp/x.lance was not found: Not found: /tmp/x.lance/_versions"))
    assert not dataset_absent(ValueError("Generic S3 error: 503 Slow Down"))
    assert not dataset_absent(ValueError("Invalid user input: credentials expired"))
    assert not dataset_absent(OSError("connection reset"))


def test_delete_chunk_absent_dataset_is_noop(etl_config: ETLConfig) -> None:
    """A delete against a dataset that never existed returns empty stats without raising."""
    uri: str = dataset_uri(etl_config, *ROUTING_KEY)
    chunk: pa.Table = pa.table({"vector_id": pa.array(["a"], pa.string())})
    stats = run_delete_chunk(etl_config, MagicMock(), uri, chunk, 0, 1)
    assert stats == {}


def test_delete_chunk_transient_open_error_reraises(
    etl_config: ETLConfig, telemetry: Telemetry, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A transient dataset-open ValueError re-raises from the delete path instead of no-opping.

    pylance renders S3 throttling, credential failures, and corrupt manifests as ValueError too, so
    swallowing every ValueError would silently skip compliance-sensitive deletes. Only the genuine
    not-found rendering is a no-op. The failure is metered as ``dataset.delete_open_failed``.
    """
    apply_merge(etl_config, telemetry, ROUTING_KEY, make_group(["a", "b"]))
    uri: str = dataset_uri(etl_config, *ROUTING_KEY)

    def raise_transient(*args: object, **kwargs: object) -> lance.LanceDataset:
        """Simulate a transient object-store failure during dataset open."""
        del args, kwargs
        raise ValueError("Generic S3 error: 503 Slow Down")

    monkeypatch.setattr(lance, "dataset", raise_transient)
    chunk: pa.Table = pa.table({"vector_id": pa.array(["a"], pa.string())})
    telemetry_mock: MagicMock = MagicMock()
    with pytest.raises(ValueError, match="503 Slow Down"):
        run_delete_chunk(etl_config, telemetry_mock, uri, chunk, 0, 1)
    telemetry_mock.incr.assert_called_once_with("dataset.delete_open_failed")


def test_open_or_bootstrap_transient_error_reraises(
    etl_config: ETLConfig, telemetry: Telemetry, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A transient open error re-raises from open_or_bootstrap instead of bootstrapping a spurious empty dataset."""
    apply_merge(etl_config, telemetry, ROUTING_KEY, make_group(["a"]))
    uri: str = dataset_uri(etl_config, *ROUTING_KEY)

    def raise_transient(*args: object, **kwargs: object) -> lance.LanceDataset:
        """Simulate a transient object-store failure during dataset open."""
        del args, kwargs
        raise ValueError("Generic S3 error: 503 Slow Down")

    monkeypatch.setattr(lance, "dataset", raise_transient)
    schema: pa.Schema = pa.schema([("vector_id", pa.string())])
    with pytest.raises(ValueError, match="503 Slow Down"):
        open_or_bootstrap(uri, schema, etl_config)
