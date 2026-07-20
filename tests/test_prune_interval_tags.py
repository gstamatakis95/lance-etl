"""Invariants for interval-tag pruning.

Ports the ``prune_interval_tags`` coverage that lived in the deleted ``tests/test_pipeline.py`` to
the current ``lance_etl.maintenance.tools`` surface. The pruner keeps the newest ``tag_keep_last``
interval tags (names parsable as ``%Y%m%dT%H%M%SZ``), never touches non-interval tags such as
``HEAD`` or ``release-*``, and orders candidates by parsed datetime rather than by the order the
tags happen to be listed in.

Note on datetime-vs-string ordering: for the fixed ``%Y%m%dT%H%M%SZ`` format, lexical string order
is always identical to chronological order (every field is zero-padded and fixed-width), so a case
where a naive string sort and a parsed-datetime sort diverge cannot be constructed with valid
interval-tag names. :meth:`TestPruneIntervalTags.test_newest_kept_regardless_of_creation_order`
instead proves the pruner does not lean on the order the tags were created or listed in.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import lance
import pyarrow as pa

from lance_etl.maintenance.tools import prune_interval_tags
from lance_etl.telemetry import Telemetry


def make_tagged_dataset(tmp_path: Path, tags: list[str]) -> str:
    """Write a one-row dataset and apply a list of tags to it.

    Args:
        tmp_path: Temporary directory.
        tags: Tag names to create, applied in the given order.

    Returns:
        The dataset URI.
    """
    uri: str = str(tmp_path / "tagged.lance")
    dataset: lance.LanceDataset = lance.write_dataset(pa.table({"id": pa.array([1], pa.int64())}), uri)
    for tag in tags:
        dataset.tags.create(tag, dataset.version)
    return uri


class TestPruneIntervalTags:
    """``prune_interval_tags`` keeps the newest N interval tags and deletes the rest."""

    def test_non_interval_tags_never_pruned(self, tmp_path: Path, telemetry: Telemetry) -> None:
        """``HEAD`` and other non-interval tags are never deleted."""
        uri: str = make_tagged_dataset(
            tmp_path,
            ["HEAD", "release-1.0", "20260601T000000Z", "20260602T000000Z"],
        )
        result: dict[str, Any] = prune_interval_tags(uri, None, 1, telemetry)
        remaining: list[str] = list(lance.dataset(uri).tags.list())
        assert "HEAD" in remaining
        assert "release-1.0" in remaining
        assert result["tags_pruned"] == 1
        assert result["tags_kept"] == 1

    def test_keep_last_boundary_keeps_newest_n(self, tmp_path: Path, telemetry: Telemetry) -> None:
        """The newest ``tag_keep_last`` interval tags are kept exactly at the boundary."""
        interval_tags: list[str] = [
            "20260601T000000Z",
            "20260602T000000Z",
            "20260603T000000Z",
            "20260604T000000Z",
            "20260605T000000Z",
        ]
        uri: str = make_tagged_dataset(tmp_path, interval_tags)
        result: dict[str, Any] = prune_interval_tags(uri, None, 3, telemetry)
        remaining: set[str] = set(lance.dataset(uri).tags.list())
        assert result["tags_pruned"] == 2
        assert result["tags_kept"] == 3
        assert {"20260603T000000Z", "20260604T000000Z", "20260605T000000Z"} <= remaining
        assert "20260602T000000Z" not in remaining
        assert "20260601T000000Z" not in remaining

    def test_keep_last_zero_deletes_all_interval_tags(self, tmp_path: Path, telemetry: Telemetry) -> None:
        """``tag_keep_last=0`` removes every interval tag but spares non-interval tags."""
        uri: str = make_tagged_dataset(
            tmp_path,
            ["HEAD", "20260601T000000Z", "20260602T000000Z"],
        )
        result: dict[str, Any] = prune_interval_tags(uri, None, 0, telemetry)
        remaining: set[str] = set(lance.dataset(uri).tags.list())
        assert "HEAD" in remaining
        assert "20260601T000000Z" not in remaining
        assert "20260602T000000Z" not in remaining
        assert result["tags_pruned"] == 2
        assert result["tags_kept"] == 0

    def test_non_matching_names_ignored(self, tmp_path: Path, telemetry: Telemetry) -> None:
        """Tag names that do not match ``%Y%m%dT%H%M%SZ`` are never considered for deletion.

        Lance only permits alphanumerics, ``.``, ``-``, and ``_`` in tag names, so the non-matching
        names here stay within that character set while still failing the ``strptime`` format.
        """
        uri: str = make_tagged_dataset(
            tmp_path,
            ["HEAD", "not-a-date", "release.v1", "20260601T000000Z"],
        )
        result: dict[str, Any] = prune_interval_tags(uri, None, 1, telemetry)
        remaining: set[str] = set(lance.dataset(uri).tags.list())
        assert {"HEAD", "not-a-date", "release.v1", "20260601T000000Z"} <= remaining
        assert result["tags_pruned"] == 0
        assert result["tags_kept"] == 1

    def test_no_interval_tags_is_noop(self, tmp_path: Path, telemetry: Telemetry) -> None:
        """With no interval tags present, pruning is a no-op even when keep_last is small."""
        uri: str = make_tagged_dataset(tmp_path, ["HEAD"])
        result: dict[str, Any] = prune_interval_tags(uri, None, 5, telemetry)
        assert result["tags_pruned"] == 0
        assert result["tags_kept"] == 0

    def test_keep_more_than_present_prunes_nothing(self, tmp_path: Path, telemetry: Telemetry) -> None:
        """When keep_last exceeds the interval-tag count, nothing is pruned."""
        uri: str = make_tagged_dataset(tmp_path, ["20260601T000000Z", "20260602T000000Z"])
        result: dict[str, Any] = prune_interval_tags(uri, None, 5, telemetry)
        assert result["tags_pruned"] == 0
        assert result["tags_kept"] == 2

    def test_newest_kept_regardless_of_creation_order(self, tmp_path: Path, telemetry: Telemetry) -> None:
        """Retention is decided by parsed datetime, not by tag creation or listing order.

        The tags are created newest-first so that any implementation that kept the last N entries of
        the raw tag listing (rather than sorting by parsed datetime) would retain the wrong tags.
        """
        uri: str = make_tagged_dataset(
            tmp_path,
            ["20260605T120000Z", "20260604T120000Z", "20260603T120000Z", "20260602T120000Z", "20260601T120000Z"],
        )
        result: dict[str, Any] = prune_interval_tags(uri, None, 2, telemetry)
        remaining: set[str] = set(lance.dataset(uri).tags.list())
        assert result["tags_pruned"] == 3
        assert result["tags_kept"] == 2
        assert {"20260604T120000Z", "20260605T120000Z"} <= remaining
        assert not ({"20260601T120000Z", "20260602T120000Z", "20260603T120000Z"} & remaining)
