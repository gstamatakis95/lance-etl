"""Tests for monotonic and replay-safe Lance completion markers."""

from __future__ import annotations

from pathlib import Path

import lance
import pyarrow as pa
import pytest

from lance_etl.etl.completion import (
    LAST_APPLIED_DIGEST_KEY,
    LAST_APPLIED_WINDOW_KEY,
    CompletionConflict,
    finalize_completion_marker,
    parse_completion_marker,
)
from lance_etl.telemetry import Telemetry


def dataset_uri(tmp_path: Path) -> str:
    """Create a minimal Lance dataset and return its URI.

    Args:
        tmp_path: Temporary directory.

    Returns:
        Dataset URI.
    """
    uri = str(tmp_path / "completion.lance")
    lance.write_dataset(pa.table({"record_id": ["id"]}), uri)
    return uri


def test_completion_marker_replay_is_noop(tmp_path: Path, telemetry: Telemetry) -> None:
    """Repeating exact marker work converges without another Lance version."""
    uri = dataset_uri(tmp_path)
    first = finalize_completion_marker(uri, 7, b"a" * 32, telemetry, retry_backoff_seconds=0)
    second = finalize_completion_marker(uri, 7, b"a" * 32, telemetry, retry_backoff_seconds=0)
    assert second == first
    assert lance.dataset(uri).version == first.lance_version


def test_completion_marker_never_moves_backward(tmp_path: Path, telemetry: Telemetry) -> None:
    """Older work reconciles as superseded without changing the marker."""
    uri = dataset_uri(tmp_path)
    latest = finalize_completion_marker(uri, 9, b"b" * 32, telemetry)
    stale = finalize_completion_marker(uri, 8, b"a" * 32, telemetry)
    assert stale == latest
    assert parse_completion_marker(lance.dataset(uri)) == latest


def test_same_window_different_digest_blocks(tmp_path: Path, telemetry: Telemetry) -> None:
    """One source-window identity cannot complete with two digests."""
    uri = dataset_uri(tmp_path)
    finalize_completion_marker(uri, 2, b"a" * 32, telemetry)
    with pytest.raises(CompletionConflict, match="different source digest"):
        finalize_completion_marker(uri, 2, b"b" * 32, telemetry)


def test_partial_marker_is_corrupt(tmp_path: Path) -> None:
    """A partial marker fails closed instead of releasing source retention."""
    uri = dataset_uri(tmp_path)
    dataset = lance.dataset(uri)
    dataset.update_config({LAST_APPLIED_WINDOW_KEY: "3"})
    with pytest.raises(CompletionConflict, match="partial"):
        parse_completion_marker(lance.dataset(uri))


def test_marker_payload_is_exact(tmp_path: Path, telemetry: Telemetry) -> None:
    """Both durable marker fields are stored in the same Lance config commit."""
    uri = dataset_uri(tmp_path)
    finalize_completion_marker(uri, 4, b"c" * 32, telemetry)
    config = lance.dataset(uri).config()
    assert config[LAST_APPLIED_WINDOW_KEY] == "4"
    assert config[LAST_APPLIED_DIGEST_KEY] == (b"c" * 32).hex()
