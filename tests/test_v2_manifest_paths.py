"""Tests for V2 manifest paths on dataset bootstrap and the one-shot migration entry point.

V2 manifest paths are now baked on with no knob: the ETL always bootstraps datasets with V2 names. Covers the
real-Lance bootstrap layout (a dataset created through :func:`apply_merge` carries V2 manifest names
``_versions/{u64::MAX - version}.manifest`` zero-padded to 20 digits, while a dataset written with the legacy V1
names via a direct Lance call discriminates the detector), and the migration: :func:`migrate_dataset_manifest_paths`
upgrades a V1 dataset to V2 in place and the ``migrate-manifests`` CLI subcommand is wired.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import lance
import pyarrow as pa
import pytest

from lance_etl.cli import build_parser
from lance_etl.etl import ETLConfig, apply_merge
from lance_etl.maintenance import migrate_dataset_manifest_paths
from lance_etl.telemetry import TelemetryConfig

V2_VERSION_PART_LEN: int = 20
"""Length of the version-number component of a V2 manifest filename.

V2 names a manifest ``{u64::MAX - version}.manifest`` zero-padded to 20 digits
(``rust/lance-table/src/io/commit.rs:104-109``), so the part before the ``.manifest`` extension is exactly 20 chars.
V1 uses the bare decimal version, which is shorter for any realistic version count.
"""


def manifest_scheme(dataset_uri: str) -> str:
    """Detect the manifest naming scheme of a dataset by inspecting its versions directory.

    Args:
        dataset_uri: Filesystem path to the ``.lance`` dataset.

    Returns:
        ``"V2"`` when every manifest filename uses the 20-digit inverted-version name, otherwise ``"V1"``.

    Raises:
        AssertionError: If the dataset has no manifest files.
    """
    versions: Path = Path(dataset_uri) / "_versions"
    manifests: list[str] = [path.name for path in versions.iterdir() if path.name.endswith(".manifest")]
    assert manifests, f"no manifest files under {versions}"
    return "V2" if all(len(name.split(".")[0]) == V2_VERSION_PART_LEN for name in manifests) else "V1"


def upsert_group() -> pa.Table:
    """Return a one-row upsert group routed to a single ``o1/t1/n1`` dataset.

    Returns:
        A table with ``vector_id`` and ``op`` columns.
    """
    return pa.table({"vector_id": pa.array(["v1"]), "op": pa.array(["insert"])})


class TestCliWiring:
    """The etl subcommand parses without a V2 flag, and migrate-manifests is wired."""

    def test_etl_parses_without_v2_flag(self, tmp_path: Path) -> None:
        """The opinionated CLI exposes no V2 manifest flag: the ETLConfig default governs the behavior."""
        args = build_parser().parse_args(
            ["etl", "--table", "t", "--start", "0", "--end", "1", "--base-uri", str(tmp_path)]
        )
        assert args.command == "etl"
        assert not hasattr(args, "enable_v2_manifest_paths")

    def test_migrate_subcommand_parses(self, tmp_path: Path) -> None:
        """The migrate-manifests subcommand parses with dataset-selection arguments."""
        args = build_parser().parse_args(["migrate-manifests", "--dataset-uri", str(tmp_path / "x.lance")])
        assert args.command == "migrate-manifests"
        assert args.dataset_uri == [str(tmp_path / "x.lance")]


@pytest.mark.integration
class TestBootstrapLayout:
    """A bootstrapped dataset carries the configured manifest naming scheme."""

    def test_bootstrap_creates_v2(self, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
        """The default config bootstraps a dataset with V2 manifest paths."""
        config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config, retry_backoff_seconds=0.0)
        apply_merge(config, MagicMock(), ("o1", "t1", "n1"), upsert_group())
        assert manifest_scheme(f"{tmp_path}/o1/t1/n1.lance") == "V2"


@pytest.mark.integration
class TestMigration:
    """The one-shot migration upgrades a V1 dataset to V2 in place."""

    def test_migrate_dataset_manifest_paths_upgrades_v1(self, tmp_path: Path) -> None:
        """A V1 dataset becomes V2 after migration, and a second call is an idempotent no-op."""
        uri: str = f"{tmp_path}/x.lance"
        lance.write_dataset(
            pa.table({"a": pa.array([1], pa.int64())}), uri, mode="append", enable_v2_manifest_paths=False
        )
        assert manifest_scheme(uri) == "V1"

        migrate_dataset_manifest_paths(uri, None, MagicMock())
        assert manifest_scheme(uri) == "V2"

        migrate_dataset_manifest_paths(uri, None, MagicMock())
        assert manifest_scheme(uri) == "V2"
