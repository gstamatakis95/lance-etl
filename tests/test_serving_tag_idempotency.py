"""Tests for idempotent lost-race resolution in the serving-tag and interval-tag-prune helpers.

``dataset.tags.create/update/delete`` are plain object-store put/delete calls, not
optimistic-concurrency manifest commits, so a lost race between a caller's
``dataset.tags.list()`` snapshot and its write surfaces as a ``ValueError`` rather than a
retryable commit conflict. These tests drive :func:`lance_etl.maintenance.tools.update_serving_tag`
and :func:`lance_etl.maintenance.tools.prune_interval_tags` directly against real local Lance
datasets, forcing the stale-snapshot race by monkeypatching the ``lance.dataset.Tags`` class at
the class level (``dataset.tags`` is a read-only property that returns a fresh wrapper on every
access, so the fake behavior must be installed on the class rather than on one instance). The
``lance.dataset`` submodule is fetched through ``importlib.import_module`` rather than a plain
``import lance.dataset`` because ``lance/__init__.py`` rebinds the ``dataset`` attribute on the
``lance`` package to the ``lance.dataset`` factory function, shadowing the submodule reference an
ordinary attribute access would otherwise resolve to.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from pathlib import Path
from types import ModuleType

import lance
import pyarrow as pa
import pytest

from lance_etl.maintenance.tools import prune_interval_tags, update_serving_tag
from lance_etl.telemetry import Telemetry

lance_dataset_module: ModuleType = importlib.import_module("lance.dataset")


def write_versions(uri: str, count: int) -> None:
    """Write a dataset with ``count`` versions by appending one row per version.

    Args:
        uri: Destination dataset URI.
        count: Number of versions (appends) to create.
    """
    for index in range(count):
        lance.write_dataset(pa.table({"a": pa.array([index], pa.int64())}), uri, mode="append")


def test_double_create_resolves_to_update(
    tmp_path: Path, telemetry: Telemetry, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A create that loses a race against an existing tag falls back to an update.

    The tag is created for real up front, then ``Tags.list`` is forced to report the tag as
    absent so the next call re-derives ``created=True`` and calls ``tags.create``, which the
    real dataset rejects because the tag already exists. The resolver must catch that and fall
    back to ``tags.update`` instead of raising, landing the tag on the new target version.
    """
    uri: str = str(tmp_path / "ds.lance")
    write_versions(uri, 2)
    update_serving_tag(uri, 1, None, telemetry)

    def empty_list(self: lance_dataset_module.Tags) -> dict[str, object]:
        """Report no tags at all, forcing the caller to re-derive ``created=True``.

        Args:
            self: The ``Tags`` manager instance.

        Returns:
            An empty dict.
        """
        del self
        return {}

    monkeypatch.setattr(lance_dataset_module.Tags, "list", empty_list)
    result: dict[str, object] = update_serving_tag(uri, 2, None, telemetry)

    assert result["created"] == {"HEAD": False}
    assert result["version"] == 2
    assert lance.dataset(uri).tags.get_version("HEAD") == 2


def test_update_missing_resolves_to_create(
    tmp_path: Path, telemetry: Telemetry, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An update that loses a race against a missing tag falls back to a create.

    No tag exists yet on the real dataset, but ``Tags.list`` is forced to report the tag as
    present so the call derives ``created=False`` and calls ``tags.update``, which the real
    dataset rejects because the tag does not exist. The resolver must catch that and fall back
    to ``tags.create`` instead of raising, landing the tag on the target version.
    """
    uri: str = str(tmp_path / "ds.lance")
    write_versions(uri, 2)

    def present_list(self: lance_dataset_module.Tags) -> dict[str, object]:
        """Report the target tag as present, forcing the caller to re-derive ``created=False``.

        Args:
            self: The ``Tags`` manager instance.

        Returns:
            A dict containing only the ``HEAD`` key.
        """
        del self
        return {"HEAD": object()}

    monkeypatch.setattr(lance_dataset_module.Tags, "list", present_list)
    result: dict[str, object] = update_serving_tag(uri, 1, None, telemetry)

    assert result["created"] == {"HEAD": True}
    assert result["version"] == 1
    assert lance.dataset(uri).tags.get_version("HEAD") == 1


def test_prune_missing_tag_is_noop(tmp_path: Path, telemetry: Telemetry, monkeypatch: pytest.MonkeyPatch) -> None:
    """A delete that races against an already-pruned tag counts as pruned, not an error.

    Three interval tags are created on one dataset version. ``Tags.delete`` is monkeypatched so
    the oldest tag raises the "does not exist" ``ValueError`` (simulating a concurrent pruner or
    a retried Spark task that already deleted it) while the other deletions run for real.
    ``prune_interval_tags`` must complete without raising and report both non-kept tags as
    pruned.
    """
    uri: str = str(tmp_path / "ds.lance")
    write_versions(uri, 1)
    dataset: lance.LanceDataset = lance.dataset(uri)
    version: int = dataset.version
    tag_names: list[str] = ["20260101T000000Z", "20260102T000000Z", "20260103T000000Z"]
    for name in tag_names:
        dataset.tags.create(name, version)

    already_pruned: str = "20260101T000000Z"
    original_delete: Callable[[lance_dataset_module.Tags, str], None] = lance_dataset_module.Tags.delete

    def flaky_delete(self: lance_dataset_module.Tags, tag: str) -> None:
        """Raise a lost-race error for one tag and delete every other tag for real.

        Args:
            self: The ``Tags`` manager instance.
            tag: The tag name to delete.
        """
        if tag == already_pruned:
            raise ValueError(f"Ref not found error: tag {tag} does not exist")
        original_delete(self, tag)

    monkeypatch.setattr(lance_dataset_module.Tags, "delete", flaky_delete)

    result: dict[str, object] = prune_interval_tags(uri, None, 1, telemetry)

    assert result["tags_pruned"] == 2
    assert result["tags_kept"] == 1
    remaining: list[str] = list(lance.dataset(uri).tags.list())
    assert "20260103T000000Z" in remaining
    assert "20260102T000000Z" not in remaining
