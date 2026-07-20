"""Direct unit tests for the cleanup-horizon safety floor.

``validate_cleanup_horizon`` rejects a ``cleanup_older_than_seconds`` set below
:data:`~lance_etl.maintenance.job.MIN_CLEANUP_HORIZON_SECONDS`, the floor that keeps version
cleanup from racing a still-running concurrent job. These tests exercise the raise path directly,
without monkeypatching the floor away and without going through ``cleanup_dataset``.
"""

from __future__ import annotations

import pytest

from lance_etl.maintenance import MaintenanceConfig
from lance_etl.maintenance.job import MIN_CLEANUP_HORIZON_SECONDS, validate_cleanup_horizon
from lance_etl.telemetry import TelemetryConfig


def make_config(cleanup_older_than_seconds: int | None) -> MaintenanceConfig:
    """Build a maintenance configuration carrying only the cleanup horizon under test.

    Args:
        cleanup_older_than_seconds: The cleanup horizon to validate, or ``None`` for no override.

    Returns:
        A maintenance configuration with the given cleanup horizon.
    """
    return MaintenanceConfig(
        telemetry=TelemetryConfig(service="lance-etl-tests", env="test"),
        cleanup_older_than_seconds=cleanup_older_than_seconds,
    )


def test_below_floor_raises() -> None:
    """A horizon one second below the floor is rejected."""
    config: MaintenanceConfig = make_config(MIN_CLEANUP_HORIZON_SECONDS - 1)
    with pytest.raises(ValueError, match="cleanup_older_than_seconds"):
        validate_cleanup_horizon(config)


def test_far_below_floor_raises() -> None:
    """A tiny horizon is rejected and the error names the safe floor."""
    config: MaintenanceConfig = make_config(60)
    with pytest.raises(ValueError, match=str(MIN_CLEANUP_HORIZON_SECONDS)):
        validate_cleanup_horizon(config)


def test_at_floor_is_accepted() -> None:
    """A horizon exactly at the floor is accepted, proving the boundary is inclusive."""
    config: MaintenanceConfig = make_config(MIN_CLEANUP_HORIZON_SECONDS)
    validate_cleanup_horizon(config)


def test_above_floor_is_accepted() -> None:
    """A horizon comfortably above the floor is accepted."""
    config: MaintenanceConfig = make_config(MIN_CLEANUP_HORIZON_SECONDS * 2)
    validate_cleanup_horizon(config)


def test_none_horizon_is_accepted() -> None:
    """An unset horizon disables the check and never raises."""
    config: MaintenanceConfig = make_config(None)
    validate_cleanup_horizon(config)
