"""Depth-agnostic dataset discovery under a base URI.

Covers :func:`lance_etl.cloud_storage.discover_datasets` on a local filesystem with datasets at mixed depths,
the empty and missing base cases, and the executor-fanned Spark path returning identical results to the
pure-driver walk.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import lance
import pyarrow as pa
import pytest
from pyspark.sql import SparkSession

from lance_etl.cloud_storage import dataset_paths_under, discover_datasets, resolve_filesystem, validate_gcs_kwargs


@pytest.mark.parametrize(
    "credentials",
    [{"access_token": "token"}, {"credential_token_expiration": 1_800_000_000}],
)
def test_gcs_token_credentials_must_be_paired(credentials: dict[str, object]) -> None:
    """Either half of an explicit GCS token pair fails before filesystem construction.

    Args:
        credentials: Incomplete mapped GCS constructor credentials.
    """
    with pytest.raises(ValueError, match="requires 'access_token'.*together"):
        validate_gcs_kwargs(credentials)


@pytest.mark.parametrize(
    "expiration",
    ["1800000000", "2027-01-15T08:00:00Z"],
)
def test_gcs_token_expiration_reaches_real_constructor_as_datetime(expiration: object) -> None:
    """String expiration forms construct the installed PyArrow filesystem.

    Args:
        expiration: Supported token expiration representation.
    """
    filesystem, path = resolve_filesystem(
        "gs://test-bucket/example",
        {"access_token": "token", "credential_token_expiration": expiration},
    )
    assert isinstance(filesystem, pa.fs.GcsFileSystem)
    assert path == "test-bucket/example"


@pytest.mark.parametrize("expiration", [1_800_000_000, datetime(2027, 1, 15, 8, tzinfo=UTC)])
def test_gcs_token_expiration_rejects_non_string_storage_options(expiration: object) -> None:
    """PyArrow conversion cannot bless a value that pylance will reject later.

    Args:
        expiration: Non-string programmatic expiration.
    """
    with pytest.raises(ValueError, match="must be a string"):
        resolve_filesystem(
            "gs://test-bucket/example",
            {"access_token": "token", "credential_token_expiration": expiration},
        )


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    """Provide a local Spark session for the executor-fanned discovery path.

    Yields:
        A two-core local session pinned to the test interpreter.
    """
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session: SparkSession = (
        SparkSession.builder.master("local[2]")
        .appName("lance-etl-discovery-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def write_tiny_dataset(uri: str) -> None:
    """Write a one-row Lance dataset at the given location.

    Args:
        uri: Destination dataset URI.
    """
    lance.write_dataset(pa.table({"id": pa.array([1], pa.int64())}), uri)


@pytest.fixture
def mixed_depth_base(tmp_path: Path) -> Path:
    """Create datasets at depths one, two, and four under a base directory.

    Args:
        tmp_path: Pytest-provided temporary directory.

    Returns:
        The base directory containing the datasets.
    """
    write_tiny_dataset(str(tmp_path / "x.lance"))
    write_tiny_dataset(str(tmp_path / "a" / "y.lance"))
    write_tiny_dataset(str(tmp_path / "a" / "b" / "c" / "z.lance"))
    return tmp_path


def test_discovery_finds_datasets_at_mixed_depths(mixed_depth_base: Path) -> None:
    """Datasets at any depth are discovered, sorted, and rooted at the base URI."""
    found: list[str] = discover_datasets(str(mixed_depth_base))
    assert found == sorted(
        [
            str(mixed_depth_base / "x.lance"),
            str(mixed_depth_base / "a" / "y.lance"),
            str(mixed_depth_base / "a" / "b" / "c" / "z.lance"),
        ]
    )


def test_discovery_collapses_dataset_internals(mixed_depth_base: Path) -> None:
    """Files inside a dataset directory collapse to one dataset entry."""
    found: list[str] = discover_datasets(str(mixed_depth_base))
    assert len(found) == len(set(found)) == 3


def test_discovery_on_empty_base_returns_nothing(tmp_path: Path) -> None:
    """An existing but empty base yields an empty list."""
    assert discover_datasets(str(tmp_path)) == []


def test_discovery_on_missing_base_returns_nothing(tmp_path: Path) -> None:
    """A missing base yields an empty list instead of raising."""
    assert discover_datasets(str(tmp_path / "missing")) == []


def test_fanned_discovery_matches_driver_walk(mixed_depth_base: Path, spark: SparkSession) -> None:
    """The executor-fanned path returns exactly the pure-driver path's sorted URI list."""
    driver_found: list[str] = discover_datasets(str(mixed_depth_base))
    fanned_found: list[str] = discover_datasets(str(mixed_depth_base), spark=spark)
    assert fanned_found == driver_found
    assert len(fanned_found) == 3


def test_fanned_discovery_ignores_stray_first_level_files(tmp_path: Path, spark: SparkSession) -> None:
    """A plain file at the first level is neither a dataset nor a prefix to descend into."""
    write_tiny_dataset(str(tmp_path / "a" / "y.lance"))
    (tmp_path / "notes.txt").write_text("not a dataset", encoding="utf-8")
    (tmp_path / "misleading.lance").write_text("not a dataset either", encoding="utf-8")
    assert discover_datasets(str(tmp_path), spark=spark) == [str(tmp_path / "a" / "y.lance")]
    assert discover_datasets(str(tmp_path)) == [str(tmp_path / "a" / "y.lance")]


def test_fanned_discovery_on_empty_and_missing_base(tmp_path: Path, spark: SparkSession) -> None:
    """Empty and missing bases yield an empty list on the fanned path, matching the driver path."""
    assert discover_datasets(str(tmp_path), spark=spark) == []
    assert discover_datasets(str(tmp_path / "missing"), spark=spark) == []


def test_dataset_paths_under_subpath_stays_base_relative(mixed_depth_base: Path) -> None:
    """Listing one first-level prefix returns paths relative to the base, not the prefix."""
    found: set[str] = dataset_paths_under(str(mixed_depth_base), None, "a")
    assert found == {"a/y.lance", "a/b/c/z.lance"}
