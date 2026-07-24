"""Tests for current Lance storage contracts."""

from __future__ import annotations

from lance_etl.etl.storage import DATA_STORAGE_VERSION, dataset_absent


def test_data_storage_version_is_release_fixed() -> None:
    """All newly materialized datasets use the current structural-encoding format."""
    assert DATA_STORAGE_VERSION == "2.1"


def test_dataset_absent_only_accepts_proven_missing_datasets() -> None:
    """Transient and malformed-dataset failures cannot be mistaken for absence."""
    assert dataset_absent(FileNotFoundError("no such file"))
    assert dataset_absent(ValueError("Dataset at path /tmp/x.lance was not found: missing manifest"))
    assert not dataset_absent(ValueError("Generic S3 error: 503 Slow Down"))
    assert not dataset_absent(ValueError("Invalid user input: credentials expired"))
    assert not dataset_absent(OSError("connection reset"))
