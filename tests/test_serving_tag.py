"""Tests for the blue-green serving-tag helper and the cleanup-exempts-tagged behavior.

Covers :func:`lance_etl.maintenance.update_serving_tag` creating and moving a serving tag through the Lance tags API,
and :func:`lance_etl.maintenance.cleanup_dataset` skipping tagged versions instead of raising, which is what keeps a
serving layer's pinned version readable across maintenance.
"""

from __future__ import annotations

from pathlib import Path

import lance
import pyarrow as pa
import pytest

from lance_etl.maintenance import MaintenanceConfig, cleanup_dataset, update_serving_tag
from lance_etl.telemetry import Telemetry, TelemetryConfig


def write_versions(uri: str, count: int) -> None:
    """Write a dataset with ``count`` versions by appending one row per version.

    Args:
        uri: Destination dataset URI.
        count: Number of versions (appends) to create.
    """
    for index in range(count):
        lance.write_dataset(pa.table({"a": pa.array([index], pa.int64())}), uri, mode="append")


def test_update_serving_tag_creates_at_latest(tmp_path: Path, telemetry: Telemetry) -> None:
    """With no target version the tag is created at the dataset's latest version."""
    uri: str = str(tmp_path / "ds.lance")
    write_versions(uri, 2)
    result: dict[str, object] = update_serving_tag(uri, None, None, telemetry)
    assert result["created"] == {"HEAD": True}
    assert result["tags"] == ["HEAD"]
    assert result["version"] == 2
    assert lance.dataset(uri).tags.get_version("HEAD") == 2


def test_update_serving_tag_moves_existing(tmp_path: Path, telemetry: Telemetry) -> None:
    """An existing tag is updated in place to an explicit target version."""
    uri: str = str(tmp_path / "ds.lance")
    write_versions(uri, 3)
    update_serving_tag(uri, None, None, telemetry)
    moved: dict[str, object] = update_serving_tag(uri, 1, None, telemetry)
    assert moved["created"] == {"HEAD": False}
    assert moved["version"] == 1
    assert lance.dataset(uri).tags.get_version("HEAD") == 1


def test_update_serving_tag_flips_multiple_tags_at_the_same_version(tmp_path: Path, telemetry: Telemetry) -> None:
    """One call flips several tags against the same resolved version, one dataset open."""
    uri: str = str(tmp_path / "ds.lance")
    write_versions(uri, 2)
    result: dict[str, object] = update_serving_tag(uri, None, None, telemetry, tags=["20260710T000000Z", "HEAD"])
    assert result["tags"] == ["20260710T000000Z", "HEAD"]
    assert result["created"] == {"20260710T000000Z": True, "HEAD": True}
    dataset: lance.LanceDataset = lance.dataset(uri)
    assert dataset.tags.get_version("20260710T000000Z") == 2
    assert dataset.tags.get_version("HEAD") == 2


def test_update_serving_tag_deduplicates_repeated_tag_names(tmp_path: Path, telemetry: Telemetry) -> None:
    """A tag name repeated in the input is flipped once, not twice."""
    uri: str = str(tmp_path / "ds.lance")
    write_versions(uri, 1)
    result: dict[str, object] = update_serving_tag(uri, None, None, telemetry, tags=["HEAD", "HEAD"])
    assert result["tags"] == ["HEAD"]
    assert result["created"] == {"HEAD": True}


def test_cleanup_exempts_tagged_versions(tmp_path: Path, telemetry: Telemetry) -> None:
    """Cleanup keeps a tagged old version readable instead of pruning it, and does not raise."""
    uri: str = str(tmp_path / "ds.lance")
    write_versions(uri, 3)
    update_serving_tag(uri, 1, None, telemetry)
    config: MaintenanceConfig = MaintenanceConfig(telemetry=TelemetryConfig(), retain_versions=1)

    cleanup_dataset(uri, config, telemetry)

    dataset: lance.LanceDataset = lance.dataset(uri)
    surviving: set[int] = {version["version"] for version in dataset.versions()}
    assert 1 in surviving
    assert dataset.tags.get_version("HEAD") == 1


def test_cleanup_without_exemption_would_raise(tmp_path: Path) -> None:
    """The default Lance cleanup raises on a tagged old version, proving the exemption is load-bearing."""
    uri: str = str(tmp_path / "ds.lance")
    write_versions(uri, 3)
    dataset: lance.LanceDataset = lance.dataset(uri)
    dataset.tags.create("HEAD", 1)
    with pytest.raises(OSError, match="tagged version"):
        dataset.cleanup_old_versions(retain_versions=1)
