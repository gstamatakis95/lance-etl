"""Failure-injection tests for whole-task replay around Lance and object-store boundaries."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import lance
import pyarrow as pa
import pytest

import lance_etl.etl.replay_sink as replay_sink_module
from lance_etl.etl.replay_sink import replay_safe_merge
from lance_etl.telemetry import Telemetry


def mutation_table(source_sequence: int, text: str) -> pa.Table:
    """Build one fixed-schema terminal mutation.

    Args:
        source_sequence: Ordered Iceberg source sequence.
        text: Payload value.

    Returns:
        One-row Arrow table satisfying the replay-safe sink contract.
    """
    return pa.table(
        {
            "vector_id": pa.array(["id"], pa.string()),
            "text": pa.array([text], pa.string()),
            "lance_etl_window_seq": pa.array([source_sequence], pa.int64()),
            "lance_etl_source_sequence": pa.array([source_sequence], pa.int64()),
            "lance_etl_event_digest": pa.array([bytes([source_sequence]) * 32], pa.binary(32)),
            "is_deleted": pa.array([False], pa.bool_()),
        }
    )


def test_retry_reconciles_an_error_returned_after_the_lance_commit(
    tmp_path: Path, telemetry: Telemetry, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An ambiguous durable commit converges when the orchestrator retries the whole task."""
    uri = str(tmp_path / "ambiguous.lance")
    real_commit_with_retries = replay_sink_module.commit_with_retries
    injected: bool = False

    def fail_after_commit(action: Any, retries: int, backoff_seconds: float, on_conflict: Any = None) -> Any:
        """Execute the real commit once and replace its acknowledgement with a transport error."""
        nonlocal injected
        del retries, backoff_seconds, on_conflict
        result = action()
        if not injected:
            injected = True
            raise OSError("injected lost commit acknowledgement")
        return result

    monkeypatch.setattr(replay_sink_module, "commit_with_retries", fail_after_commit)
    with pytest.raises(OSError, match="lost commit acknowledgement"):
        replay_safe_merge(uri, mutation_table(1, "stable"), telemetry, retry_backoff_seconds=0)
    monkeypatch.setattr(replay_sink_module, "commit_with_retries", real_commit_with_retries)
    result = replay_safe_merge(uri, mutation_table(1, "stable"), telemetry, retry_backoff_seconds=0)
    assert result.rows == 1
    assert lance.dataset(uri).count_rows() == 1


def test_retry_converges_after_object_store_open_failure(
    tmp_path: Path, telemetry: Telemetry, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A transient object-store throttle leaves no partial logical mutation and replay succeeds."""
    uri = str(tmp_path / "throttled.lance")
    real_dataset = replay_sink_module.lance.dataset
    injected: bool = False

    def fail_first_open(*args: Any, **kwargs: Any) -> Any:
        """Raise one throttle error before delegating every later dataset open."""
        nonlocal injected
        if not injected:
            injected = True
            raise OSError("injected object-store throttle")
        return real_dataset(*args, **kwargs)

    monkeypatch.setattr(replay_sink_module.lance, "dataset", fail_first_open)
    with pytest.raises(OSError, match="object-store throttle"):
        replay_safe_merge(uri, mutation_table(1, "stable"), telemetry, retry_backoff_seconds=0)
    monkeypatch.setattr(replay_sink_module.lance, "dataset", real_dataset)
    replay_safe_merge(uri, mutation_table(1, "stable"), telemetry, retry_backoff_seconds=0)
    assert lance.dataset(uri).to_table()["text"].to_pylist() == ["stable"]
