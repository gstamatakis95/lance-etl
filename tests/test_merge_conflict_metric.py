"""The commit-conflict metric wiring on the production merge paths.

``commit_with_retries`` invokes its ``on_conflict`` callback once per retried commit conflict, and
this is already covered directly in ``tests/test_telemetry_retries.py``. What those tests do not
cover is that the two production merge paths actually pass an ``on_conflict`` that increments the
``dataset.merge_conflict_retries`` counter:

- ``lance_etl.etl.sink.commit_table_chunks`` (sink.py:441), the chunked idempotent merge sink.
- ``lance_etl.etl.replay_sink.replay_safe_merge`` (replay_sink.py:350), the source-sequenced
  replay-safe merge.

Each test replaces the module-level ``commit_with_retries`` with a seam that fires ``on_conflict``
a fixed number of times and then runs the wrapped action once, so the metric emission is exercised
deterministically without needing to provoke a real Lance commit conflict. A recording DogStatsD
stand-in captures the emitted metric names.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeVar

import lance
import pyarrow as pa
import pytest

import lance_etl.etl.replay_sink as replay_sink
import lance_etl.etl.sink as sink
from lance_etl.etl.pivot import ETLConfig
from lance_etl.etl.replay_sink import (
    DELETED_COLUMN,
    EVENT_DIGEST_COLUMN,
    SOURCE_SEQUENCE_COLUMN,
    WINDOW_SEQUENCE_COLUMN,
    replay_safe_merge,
)
from lance_etl.telemetry import Telemetry, TelemetryConfig

MERGE_CONFLICT_METRIC: str = "dataset.merge_conflict_retries"

ResultT = TypeVar("ResultT")


class RecordingStatsd:
    """A DogStatsD stand-in that records every increment by metric name."""

    def __init__(self) -> None:
        """Initialize the empty increment log."""
        self.increments: list[str] = []

    def increment(self, name: str, value: float = 1, tags: list[str] | None = None) -> None:
        """Record one counter increment.

        Args:
            name: Metric name.
            value: Increment magnitude, ignored beyond recording the name.
            tags: Optional metric tags, ignored.
        """
        del value, tags
        self.increments.append(name)

    def distribution(self, name: str, value: float, tags: list[str] | None = None) -> None:
        """Ignore a distribution sample emitted by timers on the merge path.

        Args:
            name: Metric name, ignored.
            value: Sample value, ignored.
            tags: Optional metric tags, ignored.
        """
        del name, value, tags

    def gauge(self, name: str, value: float, tags: list[str] | None = None) -> None:
        """Ignore a gauge sample.

        Args:
            name: Metric name, ignored.
            value: Sample value, ignored.
            tags: Optional metric tags, ignored.
        """
        del name, value, tags


def recording_telemetry() -> tuple[Telemetry, RecordingStatsd]:
    """Build a telemetry facade whose emissions are captured in memory.

    Returns:
        The telemetry facade and the recorder its increments land in.
    """
    telemetry: Telemetry = Telemetry.create(TelemetryConfig(service="lance-etl-tests", env="test"), False)
    recorder: RecordingStatsd = RecordingStatsd()
    telemetry.statsd = recorder
    return telemetry, recorder


def fire_conflicts_then_run(count: int) -> Callable[..., ResultT]:
    """Build a ``commit_with_retries`` replacement that fires ``on_conflict`` then runs the action.

    Args:
        count: Number of simulated commit conflicts to signal before the successful attempt.

    Returns:
        A drop-in replacement for :func:`lance_etl.telemetry.commit_with_retries`.
    """

    def replacement(
        action: Callable[[], ResultT],
        retries: int = 0,
        backoff_seconds: float = 0.0,
        on_conflict: Callable[[], None] | None = None,
    ) -> ResultT:
        """Signal ``count`` conflicts, then run the wrapped action exactly once.

        Args:
            action: The commit action to run once after the simulated conflicts.
            retries: Ignored retry budget.
            backoff_seconds: Ignored backoff.
            on_conflict: The production conflict callback under test.

        Returns:
            The action's result.
        """
        del retries, backoff_seconds
        if on_conflict is not None:
            for _ in range(count):
                on_conflict()
        return action()

    return replacement


def test_sink_commit_table_chunks_emits_metric_on_conflict(monkeypatch: pytest.MonkeyPatch) -> None:
    """``commit_table_chunks`` increments the conflict metric once per retried conflict."""
    telemetry, recorder = recording_telemetry()
    config: ETLConfig = ETLConfig(
        base_uri="memory://bench",
        telemetry=TelemetryConfig(service="lance-etl-tests", env="test"),
        merge_batch_bytes=None,
        conflict_retries=10,
        retry_backoff_seconds=0.0,
    )
    monkeypatch.setattr(sink, "commit_with_retries", fire_conflicts_then_run(2))
    table: pa.Table = pa.table({"id": pa.array([1, 2, 3], pa.int64())})

    def run_chunk(chunk: pa.Table, index: int, total: int) -> dict[str, Any]:
        """Return a fixed insert-count statistics dictionary for one chunk.

        Args:
            chunk: The chunk being committed.
            index: The chunk index.
            total: The total chunk count.

        Returns:
            A statistics dictionary reporting the chunk's row count as inserts.
        """
        del index, total
        return {"num_inserted_rows": chunk.num_rows}

    total_rows: int = sink.commit_table_chunks(
        config,
        telemetry,
        table,
        run_chunk,
        lambda stats: int(stats.get("num_inserted_rows", 0)),
    )

    assert total_rows == 3
    assert recorder.increments.count(MERGE_CONFLICT_METRIC) == 2


def test_replay_safe_merge_emits_metric_on_conflict(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``replay_safe_merge`` increments the conflict metric once per retried conflict."""
    telemetry, recorder = recording_telemetry()
    uri: str = str(tmp_path / "replay.lance")
    table: pa.Table = pa.table(
        {
            "record_id": pa.array(["a"], pa.string()),
            "text": pa.array(["hello"], pa.string()),
            WINDOW_SEQUENCE_COLUMN: pa.array([1], pa.int64()),
            SOURCE_SEQUENCE_COLUMN: pa.array([1], pa.int64()),
            EVENT_DIGEST_COLUMN: pa.array([b"\x01" * 32], pa.binary(32)),
            DELETED_COLUMN: pa.array([False], pa.bool_()),
        }
    )
    monkeypatch.setattr(replay_sink, "commit_with_retries", fire_conflicts_then_run(3))

    result = replay_safe_merge(uri, table, telemetry, retry_backoff_seconds=0.0)

    assert result.rows == 1
    assert lance.dataset(uri).count_rows() == 1
    assert recorder.increments.count(MERGE_CONFLICT_METRIC) == 3
