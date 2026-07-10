"""Depth-agnostic dataset discovery under a base URI.

Covers :func:`lance_etl.cloud_storage.discover_datasets` on a local filesystem with datasets at mixed depths,
the empty and missing base cases, the executor-fanned Spark path returning identical results to the
pure-driver walk, and the ``--base-uri`` wiring through the CLI's ``load_dataset_uris``.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from pathlib import Path

import lance
import pyarrow as pa
import pytest
from pyspark.sql import SparkSession

import lance_etl.indexing.cli as indexing_cli
import lance_etl.maintenance.cli as maintenance_cli
from lance_etl.cliutil import load_dataset_uris
from lance_etl.cloud_storage import dataset_paths_under, discover_datasets


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


def test_cli_base_uri_discovers_datasets(mixed_depth_base: Path) -> None:
    """The maintenance run subcommand's --base-uri flag feeds discovery through load_dataset_uris."""
    args = maintenance_cli.build_parser().parse_args(["run", "--base-uri", str(mixed_depth_base)])
    uris: list[str] = load_dataset_uris(args)
    assert len(uris) == 3
    assert all(uri.endswith(".lance") for uri in uris)


def test_cli_base_uri_combines_with_explicit_uris(mixed_depth_base: Path) -> None:
    """Explicit --dataset-uri values and discovered datasets are combined."""
    args = indexing_cli.build_parser().parse_args(
        ["--dataset-uri", "s3://bucket/explicit.lance", "--base-uri", str(mixed_depth_base)]
    )
    uris: list[str] = load_dataset_uris(args)
    assert uris[0] == "s3://bucket/explicit.lance"
    assert len(uris) == 4


def test_cli_dedupes_repeated_explicit_uris() -> None:
    """Repeating the same --dataset-uri flag contributes exactly one URI, not two."""
    args = indexing_cli.build_parser().parse_args(
        [
            "--dataset-uri",
            "s3://bucket/one.lance",
            "--dataset-uri",
            "s3://bucket/one.lance",
            "--dataset-uri",
            "s3://bucket/two.lance",
        ]
    )
    uris: list[str] = load_dataset_uris(args)
    assert uris == ["s3://bucket/one.lance", "s3://bucket/two.lance"]


def test_cli_dedupes_explicit_uri_also_found_by_discovery(mixed_depth_base: Path) -> None:
    """A URI named both explicitly and via --base-uri discovery contributes exactly one entry."""
    duplicate_uri: str = str(mixed_depth_base / "x.lance")
    args = indexing_cli.build_parser().parse_args(["--dataset-uri", duplicate_uri, "--base-uri", str(mixed_depth_base)])
    uris: list[str] = load_dataset_uris(args)
    assert len(uris) == len(set(uris)) == 3
    assert uris[0] == duplicate_uri
