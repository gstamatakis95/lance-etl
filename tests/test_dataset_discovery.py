"""Depth-agnostic dataset discovery under a base URI.

Covers :func:`lance_etl.cloud_storage.discover_datasets` on a local filesystem with datasets at mixed depths, exclusion
of artifact sidecar directories, the empty and missing base cases, and the ``--base-uri`` wiring through the CLI's
``load_dataset_uris``.
"""

from __future__ import annotations

from pathlib import Path

import lance
import pyarrow as pa
import pytest

from lance_etl.cli import build_parser, load_dataset_uris
from lance_etl.cloud_storage import discover_datasets


def write_tiny_dataset(uri: str) -> None:
    """Write a one-row Lance dataset at the given location.

    Args:
        uri: Destination dataset URI.
    """
    lance.write_dataset(pa.table({"id": pa.array([1], pa.int64())}), uri)


@pytest.fixture
def mixed_depth_base(tmp_path: Path) -> Path:
    """Create datasets at depths one, two, and four under a base directory, plus a sidecar to ignore.

    Args:
        tmp_path: Pytest-provided temporary directory.

    Returns:
        The base directory containing the datasets.
    """
    write_tiny_dataset(str(tmp_path / "x.lance"))
    write_tiny_dataset(str(tmp_path / "a" / "y.lance"))
    write_tiny_dataset(str(tmp_path / "a" / "b" / "c" / "z.lance"))
    sidecar: Path = tmp_path / "a" / "y.lance.artifacts" / "vector"
    sidecar.mkdir(parents=True)
    (sidecar / "manifest.json").write_text("{}", encoding="utf-8")
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


def test_discovery_ignores_artifact_sidecars(mixed_depth_base: Path) -> None:
    """Sidecar directories named {dataset}.lance.artifacts are not reported as datasets."""
    found: list[str] = discover_datasets(str(mixed_depth_base))
    assert all("artifacts" not in uri for uri in found)


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


def test_cli_base_uri_discovers_datasets(mixed_depth_base: Path) -> None:
    """The compact subcommand's --base-uri flag feeds discovery through load_dataset_uris."""
    args = build_parser().parse_args(["compact", "--base-uri", str(mixed_depth_base)])
    uris: list[str] = load_dataset_uris(args)
    assert len(uris) == 3
    assert all(uri.endswith(".lance") for uri in uris)


def test_cli_base_uri_combines_with_explicit_uris(mixed_depth_base: Path) -> None:
    """Explicit --dataset-uri values and discovered datasets are combined."""
    args = build_parser().parse_args(
        ["index", "--dataset-uri", "s3://bucket/explicit.lance", "--base-uri", str(mixed_depth_base)]
    )
    uris: list[str] = load_dataset_uris(args)
    assert uris[0] == "s3://bucket/explicit.lance"
    assert len(uris) == 4
