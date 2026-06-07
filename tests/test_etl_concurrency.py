"""ETL merge semantics and a concurrent merge-plus-compaction stress test.

These run the executor-task layer directly against local-fs datasets: no Spark is involved. The concurrency test drives
two upserting threads and one compacting thread against the same dataset and asserts no data is lost.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

import lance
import pyarrow as pa
import pytest

from lance_etl.compaction import CompactionConfig, compact_small_dataset
from lance_etl.etl import ETLConfig, apply_merge, build_delete_predicate, dataset_uri
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


def test_build_delete_predicate() -> None:
    """The delete predicate renders a quoted SQL IN list."""
    predicate: str = build_delete_predicate("vector_id", pa.array(["a", "b"]))
    assert predicate == "vector_id IN ('a', 'b')"


def test_build_delete_predicate_escapes_single_quotes() -> None:
    """Embedded single quotes are doubled so the SQL IN list stays well-formed."""
    predicate: str = build_delete_predicate("vector_id", pa.array(["a'b", "o''neil"]))
    assert predicate == "vector_id IN ('a''b', 'o''''neil')"


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
    compaction_config: CompactionConfig = CompactionConfig(
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
