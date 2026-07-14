"""Tests for exact benchmark capacity and reproducibility evidence."""

from __future__ import annotations

from pathlib import Path

from bench.capacity import capacity_artifact
from bench.config import BenchConfig


def test_capacity_artifact_records_commit_versions_hardware_and_cache(tmp_path: Path) -> None:
    """Every benchmark run can be interpreted without undocumented host assumptions."""
    config = BenchConfig(
        command="prepare",
        limit=100,
        workspace=tmp_path / "workspace",
        corpus_root=tmp_path / "corpora",
        results_root=tmp_path / "results",
    )
    artifact = capacity_artifact(config)
    assert artifact["schema_version"] == 1
    assert artifact["git_commit"]
    assert artifact["software"]["pylance"]
    assert artifact["hardware"]["logical_cpu_count"]
    assert artifact["hardware"]["disk_free_bytes_at_start"] > 0
    assert artifact["cache_state"]["workspace_existed_at_start"] is False
    assert artifact["workload"]["limit"] == 100
