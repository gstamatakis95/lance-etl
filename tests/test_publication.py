"""Tests for the profile-free exact-version publication helpers."""

from __future__ import annotations

import hashlib
import uuid
from pathlib import Path

import lance
import pyarrow as pa
import pytest

from lance_etl.publication import (
    PrewarmResult,
    candidate_pin_name,
    schema_fingerprint,
    tag_version,
    validate_prewarm,
)


def tiny_dataset(tmp_path: Path) -> lance.LanceDataset:
    """Create one tiny local Lance dataset.

    Args:
        tmp_path: Pytest temporary directory.

    Returns:
        Open local dataset.
    """
    table: pa.Table = pa.table({"id": pa.array([1], type=pa.int64())})
    return lance.write_dataset(table, str(tmp_path / "candidate.lance"))


def test_candidate_pin_name_is_stable_and_allowlisted() -> None:
    """One durable work identity maps to one lowercase immutable Lance pin."""
    work_id: uuid.UUID = uuid.UUID("12345678-1234-5678-1234-567812345678")

    assert candidate_pin_name(work_id) == "candidate-12345678123456781234567812345678"


def test_tag_version_normalizes_absence_and_reads_exact_version(tmp_path: Path) -> None:
    """Missing tags become ``None`` while existing tags retain exact versions."""
    dataset: lance.LanceDataset = tiny_dataset(tmp_path)
    pin: str = candidate_pin_name(uuid.uuid4())

    assert tag_version(dataset, pin) is None
    dataset.tags.create(pin, dataset.version)
    assert tag_version(lance.dataset(dataset.uri), pin) == dataset.version


def test_schema_fingerprint_is_stable_and_metadata_sensitive() -> None:
    """The schema digest covers field ordering, types, nullability, and metadata."""
    plain: pa.Schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
    annotated: pa.Schema = plain.with_metadata({b"owner": b"local"})

    assert schema_fingerprint(plain) == hashlib.sha256(plain.serialize().to_pybytes()).hexdigest()
    assert schema_fingerprint(plain) != schema_fingerprint(annotated)


def test_prewarm_requires_unique_exact_replica_resolutions() -> None:
    """Every required replica must resolve the same exact URI and version once."""
    expected_uri: str = "/tmp/dataset.lance"
    valid: tuple[PrewarmResult, PrewarmResult] = (
        PrewarmResult("replica-a", expected_uri, 7),
        PrewarmResult("replica-b", expected_uri, 7),
    )

    assert validate_prewarm(valid, expected_uri, 7) is None
    with pytest.raises(ValueError, match="at least one"):
        validate_prewarm((), expected_uri, 7)
    with pytest.raises(ValueError, match="duplicate"):
        validate_prewarm((valid[0], valid[0]), expected_uri, 7)
    with pytest.raises(ValueError, match="different publication"):
        validate_prewarm((PrewarmResult("replica-a", expected_uri, 8),), expected_uri, 7)
