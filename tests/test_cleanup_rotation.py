"""Tests for the idle-dataset cleanup rotation that bounds object-store LIST cost.

Covers the deterministic per-dataset rotation slot (:func:`dataset_cleanup_slot`), the wall-clock
active-slot derivation (:func:`active_cleanup_slot`), the rotation gate
(:func:`should_clean_idle`), and the two integration properties that make rotation safe at fleet
scale: every dataset is still cleaned within ``cleanup_rotation_slots`` runs, and a dataset that
did real work (a retention delete) this run is always cleaned regardless of its slot.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from pathlib import Path

import lance
import pyarrow as pa
import pytest

import lance_etl.maintenance.job as maintenance_job
from lance_etl.maintenance import MaintenanceConfig
from lance_etl.maintenance.job import (
    active_cleanup_slot,
    compute_cutoff,
    dataset_cleanup_slot,
    plan_one_dataset,
    should_clean_idle,
)
from lance_etl.telemetry import Telemetry, TelemetryConfig


def test_dataset_cleanup_slot_is_deterministic(telemetry_config: TelemetryConfig) -> None:
    """dataset_cleanup_slot is stable across repeated calls and matches its sha256 definition.

    Args:
        telemetry_config: Unused, present only for fixture-signature parity with the rest of the
            module's tests.
    """
    del telemetry_config
    uri: str = "s3://bucket/org-a.lance"
    slots: int = 8
    first: int = dataset_cleanup_slot(uri, slots)
    second: int = dataset_cleanup_slot(uri, slots)
    assert first == second

    expected: int = int.from_bytes(hashlib.sha256(uri.encode()).digest()[:8], "big") % slots
    assert first == expected

    other_uri: str = "s3://bucket/org-b.lance"
    assert dataset_cleanup_slot(other_uri, slots) != dataset_cleanup_slot(uri, slots)


def test_all_datasets_cleaned_within_n_slots(telemetry_config: TelemetryConfig) -> None:
    """Every dataset is cleaned at least once as the active slot rotates through range(slots).

    Args:
        telemetry_config: The test telemetry configuration.
    """
    slots: int = 8
    uris: list[str] = [f"s3://bucket/org-{index}.lance" for index in range(50)]
    config: MaintenanceConfig = MaintenanceConfig(telemetry=telemetry_config, cleanup_rotation_slots=slots)

    covered: set[str] = set()
    for active_slot in range(slots):
        for uri in uris:
            if should_clean_idle(uri, config, active_slot):
                covered.add(uri)

    assert covered == set(uris)


def test_slots_one_cleans_every_run(telemetry_config: TelemetryConfig) -> None:
    """cleanup_rotation_slots=1 reproduces the pre-rotation always-clean behavior.

    Args:
        telemetry_config: The test telemetry configuration.
    """
    config: MaintenanceConfig = MaintenanceConfig(telemetry=telemetry_config, cleanup_rotation_slots=1)
    uris: list[str] = ["a.lance", "b.lance", "c.lance"]
    for uri in uris:
        for active_slot in range(3):
            assert should_clean_idle(uri, config, active_slot) is True
    assert should_clean_idle(uris[0], config, None) is True


def test_active_cleanup_slot_pins_to_supplied_now(telemetry_config: TelemetryConfig) -> None:
    """active_cleanup_slot derives its result from the supplied now, not wall-clock time.

    Args:
        telemetry_config: The test telemetry configuration.
    """
    config: MaintenanceConfig = MaintenanceConfig(
        telemetry=telemetry_config, cleanup_rotation_slots=8, cleanup_rotation_cadence_hours=1
    )
    now: datetime = datetime.fromtimestamp(0)
    first: int = active_cleanup_slot(config, now)
    second: int = active_cleanup_slot(config, now)
    assert first == second

    later: datetime = now + timedelta(hours=8)
    assert active_cleanup_slot(config, later) == first


def test_did_work_dataset_always_cleaned(
    tmp_path: Path, telemetry_config: TelemetryConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A dataset with retention deletions this run is always cleaned even off its rotation slot.

    Args:
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
        monkeypatch: Pytest monkeypatch fixture.
    """
    uri: str = str(tmp_path / "did_work.lance")
    lance.write_dataset(pa.table({"id": pa.array(range(10), pa.int64())}), uri)

    def fake_retention(
        dataset: lance.LanceDataset,
        uri_arg: str,
        config: MaintenanceConfig,
        cutoff: datetime,
        telemetry: Telemetry,
    ) -> dict[str, object]:
        """Simulate a retention step that deleted rows this run without touching the real column."""
        del dataset, config, cutoff, telemetry
        return {"uri": uri_arg, "retention_rows_deleted": 5, "skipped": ""}

    monkeypatch.setattr(maintenance_job, "run_retention_on_open_dataset", fake_retention)

    cleaned: list[str] = []

    def fake_cleanup(
        uri_arg: str,
        config: MaintenanceConfig,
        telemetry: Telemetry,
        dataset: lance.LanceDataset | None = None,
    ) -> int:
        """Record that cleanup ran for this dataset and return a distinctive sentinel."""
        del config, telemetry, dataset
        cleaned.append(uri_arg)
        return 999

    monkeypatch.setattr(maintenance_job, "cleanup_dataset", fake_cleanup)

    slots: int = 8
    off_slot: int = (dataset_cleanup_slot(uri, slots) + 1) % slots
    config: MaintenanceConfig = MaintenanceConfig(
        telemetry=telemetry_config,
        retention_seconds=10 * 24 * 3600,
        cleanup_rotation_slots=slots,
        commit_backoff_seconds=0.0,
    )
    result: dict[str, object] = plan_one_dataset(
        uri, config, compute_cutoff(config.retention_seconds), Telemetry.create(telemetry_config), off_slot
    )

    assert result["retention_rows_deleted"] == 5
    assert result["bytes_removed"] == 999
    assert cleaned == [uri]


def test_idle_dataset_skipped_off_slot(
    tmp_path: Path, telemetry_config: TelemetryConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An idle single-fragment dataset skips cleanup off its rotation slot and cleans on it.

    Args:
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
        monkeypatch: Pytest monkeypatch fixture, used to lower the fixed cleanup-horizon floor so
            ``cleanup_older_than_seconds=0`` can force cleanup regardless of version age.
    """
    monkeypatch.setattr(maintenance_job, "MIN_CLEANUP_HORIZON_SECONDS", 0)
    uri: str = str(tmp_path / "idle_rotation.lance")
    table: pa.Table = pa.table({"id": pa.array(range(20), pa.int64())})
    lance.write_dataset(table, uri)
    lance.write_dataset(table, uri, mode="overwrite")
    versions_before: int = len(lance.dataset(uri).versions())

    slots: int = 8
    own_slot: int = dataset_cleanup_slot(uri, slots)
    off_slot: int = (own_slot + 1) % slots

    config: MaintenanceConfig = MaintenanceConfig(
        telemetry=telemetry_config,
        cleanup_rotation_slots=slots,
        cleanup_older_than_seconds=0,
        commit_backoff_seconds=0.0,
    )

    skipped_result: dict[str, object] = plan_one_dataset(
        uri, config, None, Telemetry.create(telemetry_config), off_slot
    )
    assert skipped_result["bytes_removed"] == 0
    assert len(lance.dataset(uri).versions()) == versions_before

    cleaned_result: dict[str, object] = plan_one_dataset(
        uri, config, None, Telemetry.create(telemetry_config), own_slot
    )
    assert cleaned_result["bytes_removed"] > 0
    assert len(lance.dataset(uri).versions()) < versions_before
