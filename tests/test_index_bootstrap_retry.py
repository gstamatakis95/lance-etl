"""Tests for the commit-conflict retry wrapper around the vector index bootstrap build.

Covers the fix to :func:`lance_etl.indexing.runner.bootstrap_vector_index`: it used to publish its
committed ``create_index`` call with no retry wrapper, the only commit path in the repository
without one. A commit conflict raised by a concurrent maintenance, compaction, or ETL commit
against the same dataset therefore aborted the whole fleet build round instead of retrying. The
fix wraps the commit in :func:`lance_etl.indexing.segments.commit_index_with_retries` with an
inner action that re-opens the dataset fresh on every attempt, so a retry rebases on the latest
version.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import lance
import pyarrow as pa
import pytest
from conftest import make_vector_table, write_fragmented_dataset

from lance_etl.indexing import IndexJobConfig, bootstrap_vector_index, load_vector_config
from lance_etl.telemetry import Telemetry, TelemetryConfig

ROWS: int = 512
DIM: int = 8
ROWS_PER_FRAGMENT: int = 128
COMMIT_CONFLICT_MESSAGE: str = "Retryable commit conflict for version 1: retry"


@pytest.fixture
def dataset_uri(tmp_path: Path) -> str:
    """Write a small multi-fragment vector dataset and return its URI.

    Args:
        tmp_path: Pytest-provided temporary directory.

    Returns:
        The dataset URI.
    """
    uri: str = str(tmp_path / "bootstrap_retry.lance")
    write_fragmented_dataset(uri, make_vector_table(rows=ROWS, dim=DIM), max_rows_per_file=ROWS_PER_FRAGMENT)
    return uri


def bootstrap_config() -> IndexJobConfig:
    """Build the indexing configuration used by the bootstrap-retry tests.

    Returns:
        A configuration with a small explicit partition count, no row floor, and a small,
        backoff-free commit-retry budget so a retried test stays fast.
    """
    return IndexJobConfig(
        telemetry=TelemetryConfig(),
        vector_columns=["vector"],
        num_partitions=4,
        vector_min_rows=1,
        commit_retries=3,
        commit_backoff_seconds=0.0,
    )


def counting_create_index(fail_first: bool) -> tuple[Callable[..., Any], list[int]]:
    """Build a ``LanceDataset.create_index`` replacement that counts calls and can fail once.

    The replacement always increments the shared counter first, then either raises a retryable
    commit-conflict error (only on the very first call, when ``fail_first`` is set) or delegates
    to the real, unpatched implementation captured at build time.

    Args:
        fail_first: Whether the first call should raise a retryable commit-conflict error instead
            of building the index.

    Returns:
        The replacement method to install with ``monkeypatch.setattr``, and the call counter it
        appends to on every invocation.
    """
    real_create_index: Callable[..., Any] = lance.LanceDataset.create_index
    calls: list[int] = []

    def create_index(self: lance.LanceDataset, *args: Any, **kwargs: Any) -> Any:
        """Record the call and either raise a commit conflict or delegate to the real method.

        Args:
            self: The dataset instance the call is bound to.
            args: Positional arguments forwarded to the real ``create_index``.
            kwargs: Keyword arguments forwarded to the real ``create_index``.

        Returns:
            Whatever the real ``create_index`` returns.

        Raises:
            OSError: On the first call only, when ``fail_first`` is set.
        """
        calls.append(1)
        if fail_first and len(calls) == 1:
            raise OSError(COMMIT_CONFLICT_MESSAGE)
        return real_create_index(self, *args, **kwargs)

    return create_index, calls


def test_bootstrap_retries_on_commit_conflict(
    dataset_uri: str, telemetry: Telemetry, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A commit conflict on the first create_index attempt is retried against a fresh dataset.

    Reproduces a concurrent commit racing the bootstrap build: the first attempt raises the
    retryable ``OSError`` marker :func:`lance_etl.telemetry.is_commit_conflict_error` matches, and
    the second attempt (against a freshly re-opened dataset, per the ``action`` contract) must
    build and commit the index normally.

    Args:
        dataset_uri: URI of the pre-built test dataset.
        telemetry: The telemetry facade fixture.
        monkeypatch: Pytest monkeypatch used to inject the conflict on the first attempt only.
    """
    config: IndexJobConfig = bootstrap_config()
    replacement, calls = counting_create_index(fail_first=True)
    monkeypatch.setattr(lance.LanceDataset, "create_index", replacement)

    stats: dict[str, object] = bootstrap_vector_index(dataset_uri, "vector", "vector_idx", config, telemetry)

    assert len(calls) == 2, "the commit must retry exactly once after the injected conflict"
    assert stats["column"] == "vector"
    assert stats["index"] == "vector_idx"
    assert stats["segments"] == 1
    assert stats["num_partitions"] == 4
    assert stats["reused_artifacts"] is False

    committed: lance.LanceDataset = lance.dataset(dataset_uri)
    assert stats["fragments"] == len(committed.get_fragments())
    assert "vector_idx" in {item["name"] for item in committed.list_indices()}
    result: object = committed.to_table(nearest={"column": "vector", "q": [0.5] * DIM, "k": 5})
    assert result.num_rows == 5


def test_bootstrap_succeeds_without_conflict(
    dataset_uri: str, telemetry: Telemetry, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With no injected conflict, the commit builds and publishes on the first attempt.

    Pins the non-conflict path: the retry wrapper must not add a spurious retry, and the returned
    stats dict must carry the same shape as before the retry wrapper was introduced.

    Args:
        dataset_uri: URI of the pre-built test dataset.
        telemetry: The telemetry facade fixture.
        monkeypatch: Pytest monkeypatch used to install the counting wrapper.
    """
    config: IndexJobConfig = bootstrap_config()
    replacement, calls = counting_create_index(fail_first=False)
    monkeypatch.setattr(lance.LanceDataset, "create_index", replacement)

    stats: dict[str, object] = bootstrap_vector_index(dataset_uri, "vector", "vector_idx", config, telemetry)

    assert len(calls) == 1
    assert stats["column"] == "vector"
    assert stats["index"] == "vector_idx"
    assert stats["segments"] == 1
    assert stats["num_partitions"] == 4
    assert stats["reused_artifacts"] is False

    committed: lance.LanceDataset = lance.dataset(dataset_uri)
    assert stats["fragments"] == len(committed.get_fragments())
    assert "vector_idx" in {item["name"] for item in committed.list_indices()}


def test_bootstrap_retry_records_latest_training_rows(
    dataset_uri: str, telemetry: Telemetry, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A retry after a concurrent append stores evidence from the generation it actually trained.

    Args:
        dataset_uri: URI of the pre-built test dataset.
        telemetry: The telemetry facade fixture.
        monkeypatch: Pytest monkeypatch used to append rows and inject the first conflict.
    """
    extra_rows: int = 64
    real_create_index: Callable[..., Any] = lance.LanceDataset.create_index
    calls: list[int] = []

    def create_index(self: lance.LanceDataset, *args: Any, **kwargs: Any) -> Any:
        """Append rows before the first conflict, then delegate the retry to pylance.

        Args:
            self: Dataset instance receiving the call.
            args: Positional arguments forwarded to pylance.
            kwargs: Keyword arguments forwarded to pylance.

        Returns:
            The result from pylance's real implementation on the retry.

        Raises:
            OSError: On the first invocation after committing the concurrent append.
        """
        calls.append(1)
        if len(calls) == 1:
            table: pa.Table = make_vector_table(rows=extra_rows, dim=DIM, seed=37)
            appended: pa.Table = table.set_column(0, "id", pa.array(range(ROWS, ROWS + extra_rows), pa.int64()))
            lance.write_dataset(appended, dataset_uri, mode="append")
            raise OSError(COMMIT_CONFLICT_MESSAGE)
        return real_create_index(self, *args, **kwargs)

    monkeypatch.setattr(lance.LanceDataset, "create_index", create_index)
    stats: dict[str, object] = bootstrap_vector_index(
        dataset_uri, "vector", "vector_idx", bootstrap_config(), telemetry
    )

    committed: lance.LanceDataset = lance.dataset(dataset_uri)
    config: dict[str, object] | None = load_vector_config(committed, "vector")
    assert len(calls) == 2
    assert committed.count_rows() == ROWS + extra_rows
    assert stats["fragments"] == len(committed.get_fragments())
    assert config is not None
    assert config["rows_at_train"] == ROWS + extra_rows
