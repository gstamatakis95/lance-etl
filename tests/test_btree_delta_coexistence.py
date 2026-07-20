"""Concurrent replay-merge coexistence with BTREE index deltas.

Recovers the BTREE-delta coexistence regression classifier from the deleted
``tests/test_concurrent_coexistence.py``, ported to the production merge path
(``lance_etl.etl.replay_sink.replay_safe_merge``) rather than the retired ``apply_merge``.

A dataset carries multiple unmerged BTREE delta segments (BTREE segments commit unmerged per hard
rule 6) while several threads run ``replay_safe_merge`` concurrently over disjoint key ranges. This
reproduces the window in which the known pylance 8.0.0 regression can surface:
``RowAddrTreeMap::from_sorted_iter called with non-sorted input`` (see the AGENTS.md API note). The
failure is loud, never silent corruption.

The outcome is classified so the suite stays informative across upstream changes: a failure carrying
the regression signature is turned into a dynamic ``pytest.xfail`` (non-strict, no ``xfail_strict``
is set), any other actor failure fails the test, and a clean run asserts full data integrity —
which is exactly the signal that the upstream regression has been fixed and the marker can be
dropped.
"""

from __future__ import annotations

import threading
from pathlib import Path

import lance
import pyarrow as pa
import pytest
from lance.dataset import Index

from lance_etl.etl.replay_sink import (
    DELETED_COLUMN,
    EVENT_DIGEST_COLUMN,
    SOURCE_SEQUENCE_COLUMN,
    WINDOW_SEQUENCE_COLUMN,
    replay_safe_merge,
)
from lance_etl.indexing import (
    BTreeIndexHandler,
    IndexJobConfig,
    commit_segments,
    index_delta_count,
    scalar_index_name,
    serialize_segment,
    split_evenly,
)
from lance_etl.telemetry import Telemetry, TelemetryConfig

BTREE_DELTA_MERGE_INSERT_REGRESSION_SIGNATURE: str = "RowAddrTreeMap::from_sorted_iter"
"""Substring identifying the known pylance 8.0.0 BTREE-delta merge_insert regression."""

KEY_COLUMN: str = "record_id"
SCALAR_COLUMN: str = "bucket"
INDEX_NAME: str = scalar_index_name(SCALAR_COLUMN)
THREADS: int = 4
KEYS_PER_THREAD: int = 12
ROUNDS: int = 3


def replay_table(record_ids: list[str], sequence: int) -> pa.Table:
    """Build a replay-shaped terminal mutation table over the given keys.

    Args:
        record_ids: Logical record keys to write.
        sequence: The shared source sequence for every row in this table.

    Returns:
        A one-batch replay merge table with a BTREE-indexable ``bucket`` column.
    """
    count: int = len(record_ids)
    return pa.table(
        {
            KEY_COLUMN: pa.array(record_ids, pa.string()),
            SCALAR_COLUMN: pa.array([hash(rid) % 997 for rid in record_ids], pa.int64()),
            WINDOW_SEQUENCE_COLUMN: pa.array([sequence] * count, pa.int64()),
            SOURCE_SEQUENCE_COLUMN: pa.array([sequence] * count, pa.int64()),
            EVENT_DIGEST_COLUMN: pa.array([bytes([sequence % 256]) * 32 for _ in record_ids], pa.binary(32)),
            DELETED_COLUMN: pa.array([False] * count, pa.bool_()),
        }
    )


def make_telemetry() -> Telemetry:
    """Build a telemetry facade for one thread.

    Returns:
        A telemetry facade safe to use offline.
    """
    return Telemetry.create(TelemetryConfig(service="lance-etl-tests", env="test"), False)


def fragment_ids(uri: str) -> list[int]:
    """Return the live fragment ids of a dataset.

    Args:
        uri: Dataset URI.

    Returns:
        The fragment ids in dataset order.
    """
    return [fragment.fragment_id for fragment in lance.dataset(uri).get_fragments()]


def build_btree_deltas(uri: str, config: IndexJobConfig, telemetry: Telemetry, shards: int) -> None:
    """Build and commit BTREE segments unmerged so the index carries multiple deltas.

    Args:
        uri: Dataset URI.
        config: Indexing configuration.
        telemetry: Telemetry facade.
        shards: Number of shards to split the fragments into, one delta segment per shard.
    """
    handler: BTreeIndexHandler = BTreeIndexHandler(config, SCALAR_COLUMN, INDEX_NAME)
    version: int = lance.dataset(uri).version
    documents: list[str] = []
    for group in split_evenly(fragment_ids(uri), shards):
        segment: Index = handler.build_segment(lance.dataset(uri, version=version), group, None)
        documents.append(serialize_segment(segment))
    commit_segments(uri, documents, SCALAR_COLUMN, INDEX_NAME, False, config, telemetry)


def keys_for(thread_index: int) -> list[str]:
    """Return the disjoint key range owned by one thread.

    Args:
        thread_index: The zero-based thread number.

    Returns:
        The record-id keys this thread merges, disjoint across threads.
    """
    start: int = thread_index * KEYS_PER_THREAD
    return [f"key-{identifier:05d}" for identifier in range(start, start + KEYS_PER_THREAD)]


def merge_actor(uri: str, thread_index: int, failures: list[str]) -> None:
    """Run repeated replay merges over one thread's disjoint key range.

    Args:
        uri: Dataset URI.
        thread_index: The zero-based thread number selecting the key range.
        failures: Shared failure sink recording any exception this thread hits.
    """
    telemetry: Telemetry = make_telemetry()
    keys: list[str] = keys_for(thread_index)
    try:
        for round_number in range(ROUNDS):
            replay_safe_merge(
                uri,
                replay_table(keys, sequence=round_number + 2),
                telemetry,
                key_column=KEY_COLUMN,
                conflict_retries=40,
                retry_backoff_seconds=0.0,
            )
    except BaseException as exc:
        failures.append(f"thread-{thread_index}: {type(exc).__name__}: {exc}")


def test_concurrent_replay_merge_coexists_with_btree_deltas(tmp_path: Path) -> None:
    """Concurrent replay merges over a BTREE-delta dataset converge, or hit the known regression."""
    uri: str = str(tmp_path / "btree_delta.lance")
    telemetry: Telemetry = make_telemetry()
    seed_keys: list[str] = [key for index in range(THREADS) for key in keys_for(index)]
    lance.write_dataset(replay_table(seed_keys, sequence=1), uri, max_rows_per_file=THREADS)

    config: IndexJobConfig = IndexJobConfig(
        telemetry=TelemetryConfig(service="lance-etl-tests", env="test"),
        scalar_columns=[SCALAR_COLUMN],
        commit_backoff_seconds=0.0,
    )
    build_btree_deltas(uri, config, telemetry, shards=3)
    assert index_delta_count(lance.dataset(uri), INDEX_NAME) >= 2, "the dataset must carry BTREE deltas"

    failures: list[str] = []
    threads: list[threading.Thread] = [
        threading.Thread(target=merge_actor, args=(uri, index, failures), name=f"merger-{index}")
        for index in range(THREADS)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(120.0)
    assert not any(thread.is_alive() for thread in threads), "a merge actor did not finish in time"

    known_regression: list[str] = [
        failure for failure in failures if BTREE_DELTA_MERGE_INSERT_REGRESSION_SIGNATURE in failure
    ]
    other: list[str] = [failure for failure in failures if BTREE_DELTA_MERGE_INSERT_REGRESSION_SIGNATURE not in failure]
    assert not other, f"actors died for reasons other than the known regression: {other}"
    if known_regression:
        pytest.xfail(
            "hit the known pylance 8.0.0 BTREE-delta merge_insert regression: "
            f"{known_regression}. See BTREE_DELTA_MERGE_INSERT_REGRESSION_SIGNATURE."
        )

    final: lance.LanceDataset = lance.dataset(uri)
    rows: pa.Table = final.to_table(columns=[KEY_COLUMN, SOURCE_SEQUENCE_COLUMN])
    stored: dict[str, int] = dict(
        zip(rows[KEY_COLUMN].to_pylist(), rows[SOURCE_SEQUENCE_COLUMN].to_pylist(), strict=True)
    )
    assert len(stored) == len(seed_keys), "concurrent merges lost or duplicated keys"
    assert all(sequence == ROUNDS + 1 for sequence in stored.values()), "the newest source sequence did not win"
