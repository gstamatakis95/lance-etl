"""Tests for the mandatory V2 manifest-path layout on dataset bootstrap.

The ETL always bootstraps datasets with V2 names. The real Lance integration assertion verifies
the ``_versions/{u64::MAX - version}.manifest`` zero-padded layout.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from lance_etl.etl.replay_sink import (
    DELETED_COLUMN,
    EVENT_DIGEST_COLUMN,
    SOURCE_SEQUENCE_COLUMN,
    WINDOW_SEQUENCE_COLUMN,
    replay_safe_merge,
)
from lance_etl.telemetry import Telemetry

V2_VERSION_PART_LEN: int = 20
"""Length of the version-number component of a V2 manifest filename.

V2 names a manifest ``{u64::MAX - version}.manifest`` zero-padded to 20 digits
(``rust/lance-table/src/io/commit.rs:104-109``), so the part before the ``.manifest`` extension is exactly 20 chars.
V1 uses the bare decimal version, which is shorter for any realistic version count.
"""


def has_v2_manifest_paths(dataset_uri: str) -> bool:
    """Return whether every manifest has a V2 inverted-version name.

    Args:
        dataset_uri: Filesystem path to the ``.lance`` dataset.

    Returns:
        Whether every manifest filename uses the 20-digit inverted-version name.

    Raises:
        AssertionError: If the dataset has no manifest files.
    """
    versions: Path = Path(dataset_uri) / "_versions"
    manifests: list[str] = [path.name for path in versions.iterdir() if path.name.endswith(".manifest")]
    assert manifests, f"no manifest files under {versions}"
    return all(len(name.split(".")[0]) == V2_VERSION_PART_LEN for name in manifests)


def terminal_table() -> pa.Table:
    """Return a one-row replay-safe terminal mutation table.

    Returns:
        A release-shaped terminal table.
    """
    return pa.table(
        {
            "record_id": pa.array(["v1"], pa.string()),
            WINDOW_SEQUENCE_COLUMN: pa.array([1], pa.int64()),
            SOURCE_SEQUENCE_COLUMN: pa.array([1], pa.int64()),
            EVENT_DIGEST_COLUMN: pa.array([b"a" * 32], pa.binary(32)),
            DELETED_COLUMN: pa.array([False], pa.bool_()),
        }
    )


@pytest.mark.integration
class TestBootstrapLayout:
    """A bootstrapped dataset carries the configured manifest naming scheme."""

    def test_bootstrap_creates_v2(self, tmp_path: Path, telemetry: Telemetry) -> None:
        """The replay-safe production sink bootstraps with V2 manifest paths."""
        uri: str = str(tmp_path / "candidate.lance")
        replay_safe_merge(uri, terminal_table(), telemetry, retry_backoff_seconds=0.0)
        assert has_v2_manifest_paths(uri)
