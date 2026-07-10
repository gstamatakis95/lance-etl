"""Tests for the streaming routing grouper :func:`lance_etl.etl.pivot.stream_routing_groups`.

The streaming grouper is the executor-memory-bounded counterpart of the non-streaming
:func:`group_by_routing`: it consumes an iterator of routing-sorted Arrow batches and yields one
``(key, sub_table)`` per contiguous run, splitting a run further whenever the buffered byte estimate
reaches ``flush_bytes``. These tests pin the equivalence with ``group_by_routing`` on sorted input,
the byte-budget split of a long run, the guarantee that a flush never interleaves keys, empty-batch
skipping, the counters contract, and the documented degradation on unsorted input. All are pure
PyArrow, no Spark.

Because runs are appended one contiguous slice at a time and the byte-budget flush is checked only
after each append, a run wholly contained in a single batch is never split mid-batch. The byte-split
test therefore spreads one key across several small batches so buffered bytes accumulate across batch
boundaries and the flush fires.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
from conftest import group_by_routing

from lance_etl.etl.pivot import ROUTING_COLS, stream_routing_groups

ROUTING: list[str] = list(ROUTING_COLS)


def make_batch(keys: list[tuple[str, str, str]], start: int = 0) -> pa.RecordBatch:
    """Build one record batch with a row per routing key plus a distinct payload id.

    Args:
        keys: One ``(org_id, tenant_id, namespace)`` tuple per row, in row order.
        start: First payload id so batches concatenate without id collisions.

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


def group_ids(groups: list[tuple[tuple[Any, ...], pa.Table]]) -> list[tuple[tuple[Any, ...], list[str]]]:
    """Reduce grouped tables to key and ordered vector-id lists for comparison.

    Args:
        groups: The grouped ``(key, sub_table)`` output.

    Returns:
        One ``(key, [vector_id, ...])`` per group, preserving row order.
    """
    return [(key, rows["vector_id"].to_pylist()) for key, rows in groups]


def test_stream_matches_group_by_routing() -> None:
    """Streaming a sorted multi-trio table matches the non-streaming grouper key-for-key, row-for-row.

    Batch boundaries are placed mid-run so the equivalence holds even when a run spans batches.
    """
    keys: list[tuple[str, str, str]] = (
        [("o1", "t1", "n1")] * 3 + [("o1", "t1", "n2")] * 2 + [("o1", "t2", "n1")] + [("o2", "t1", "n1")] * 2
    )
    batches: list[pa.RecordBatch] = [
        make_batch(keys[0:2], start=0),
        make_batch(keys[2:5], start=2),
        make_batch(keys[5:8], start=5),
    ]
    full: pa.Table = pa.Table.from_batches(batches)

    streamed: list[tuple[tuple[Any, ...], pa.Table]] = list(stream_routing_groups(iter(batches), ROUTING, None))
    reference: list[tuple[tuple[Any, ...], pa.Table]] = list(group_by_routing(full, ROUTING))

    assert group_ids(streamed) == group_ids(reference)


def test_run_spanning_multiple_batches_yields_one_group() -> None:
    """One trio split across three batches is yielded as a single group with every row."""
    batches: list[pa.RecordBatch] = [
        make_batch([("o1", "t1", "n1")] * 2, start=0),
        make_batch([("o1", "t1", "n1")] * 2, start=2),
        make_batch([("o1", "t1", "n1")] * 2, start=4),
    ]
    groups: list[tuple[tuple[Any, ...], pa.Table]] = list(stream_routing_groups(iter(batches), ROUTING, None))

    assert len(groups) == 1
    key, rows = groups[0]
    assert key == ("o1", "t1", "n1")
    assert rows["vector_id"].to_pylist() == [f"v{i}" for i in range(6)]


def test_byte_budget_splits_a_big_run() -> None:
    """A byte budget splits one long run into several same-key groups covering every row once.

    The trio is spread one row per batch so buffered bytes accumulate across batch boundaries.
    A flush_bytes of one row's width forces a flush at each boundary, yielding multiple groups that
    share the key. Their rows must reconstruct the input exactly once, in order, with no dupes.
    """
    batches: list[pa.RecordBatch] = [make_batch([("o1", "t1", "n1")], start=i) for i in range(6)]
    width: int = max(1, batches[0].nbytes // batches[0].num_rows)

    groups: list[tuple[tuple[Any, ...], pa.Table]] = list(stream_routing_groups(iter(batches), ROUTING, width))

    assert len(groups) > 1
    assert all(key == ("o1", "t1", "n1") for key, rows in groups)
    recovered: list[str] = [vid for key, rows in groups for vid in rows["vector_id"].to_pylist()]
    assert recovered == [f"v{i}" for i in range(6)]


def test_flush_never_interleaves_keys() -> None:
    """No yielded group mixes rows from two keys even under an aggressive byte budget."""
    batches: list[pa.RecordBatch] = [
        make_batch([("o1", "t1", "n1")], start=0),
        make_batch([("o1", "t1", "n1")], start=1),
        make_batch([("o2", "t1", "n1")], start=2),
        make_batch([("o2", "t1", "n1")], start=3),
    ]
    width: int = max(1, batches[0].nbytes // batches[0].num_rows)

    groups: list[tuple[tuple[Any, ...], pa.Table]] = list(stream_routing_groups(iter(batches), ROUTING, width))

    for key, rows in groups:
        distinct: set[tuple[Any, ...]] = set(zip(*(rows[c].to_pylist() for c in ROUTING), strict=True))
        assert distinct == {key}


def test_empty_batches_are_skipped() -> None:
    """Zero-row batches interleaved between real batches leave the grouping unchanged."""
    real: list[pa.RecordBatch] = [
        make_batch([("o1", "t1", "n1")] * 2, start=0),
        make_batch([("o2", "t1", "n1")], start=2),
    ]
    empty: pa.RecordBatch = make_batch([])
    with_gaps: list[pa.RecordBatch] = [empty, real[0], empty, real[1], empty]

    baseline: list[tuple[tuple[Any, ...], pa.Table]] = list(stream_routing_groups(iter(real), ROUTING, None))
    padded: list[tuple[tuple[Any, ...], pa.Table]] = list(stream_routing_groups(iter(with_gaps), ROUTING, None))

    assert group_ids(padded) == group_ids(baseline)


def test_counters_report_flushes_and_peak() -> None:
    """The counters dict reports one flush per yielded group and a positive bounded peak estimate."""
    batches: list[pa.RecordBatch] = [
        make_batch([("o1", "t1", "n1")] * 2, start=0),
        make_batch([("o2", "t1", "n1")] * 3, start=2),
    ]
    full: pa.Table = pa.Table.from_batches(batches)
    counters: dict[str, int] = {}

    groups: list[tuple[tuple[Any, ...], pa.Table]] = list(stream_routing_groups(iter(batches), ROUTING, None, counters))

    assert counters["flushes"] == len(groups)
    assert counters["peak_buffered_bytes"] > 0
    assert counters["peak_buffered_bytes"] <= full.nbytes


def test_unsorted_input_degrades_to_run_per_group() -> None:
    """Unsorted input yields a key once per contiguous run without dropping any row.

    This mirrors the documented degradation of :func:`group_by_routing`: a key that reappears after
    another key is emitted once per run over disjoint rows, so downstream idempotent merges stay
    correct.
    """
    batches: list[pa.RecordBatch] = [
        make_batch([("o1", "t1", "n1")], start=0),
        make_batch([("o2", "t1", "n1")], start=1),
        make_batch([("o1", "t1", "n1")], start=2),
    ]
    groups: list[tuple[tuple[Any, ...], pa.Table]] = list(stream_routing_groups(iter(batches), ROUTING, None))

    assert [key for key, rows in groups] == [("o1", "t1", "n1"), ("o2", "t1", "n1"), ("o1", "t1", "n1")]
    recovered: list[str] = sorted(vid for key, rows in groups for vid in rows["vector_id"].to_pylist())
    assert recovered == ["v0", "v1", "v2"]
