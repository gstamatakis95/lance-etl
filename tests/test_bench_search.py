"""Unit tests for the bench search phase's headline-status aggregation and CLI exit behavior."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

import bench.cli as cli_module
from bench.search import search_headline_status


def measured_load(concurrency_levels: int = 3) -> dict[str, Any]:
    """Build a load leg result with every concurrency level measured.

    Args:
        concurrency_levels: Number of ``MEASURED`` levels to synthesize.

    Returns:
        A load leg document reporting overall ``MEASURED`` status.
    """
    return {
        "status": "MEASURED",
        "levels": [{"concurrency": 2**index, "status": "MEASURED"} for index in range(concurrency_levels)],
    }


def failed_load() -> dict[str, Any]:
    """Build a load leg result whose every concurrency level was rejected.

    Returns:
        A load leg document reporting overall ``FAILED`` status.
    """
    return {
        "status": "FAILED",
        "levels": [{"concurrency": 1, "status": "FAILED", "reason": "RESOURCE_EXHAUSTED"}],
    }


def measured_sweep(queries: int = 100) -> list[dict[str, Any]]:
    """Build a one-point recall sweep leg with a positive query count.

    Args:
        queries: Query count recorded on the point.

    Returns:
        A single-point sweep list.
    """
    return [{"queries": queries, "recall_at_10": 0.99}]


def empty_sweep() -> list[dict[str, Any]]:
    """Build a one-point recall sweep leg that measured zero queries.

    Returns:
        A single-point sweep list with a zero query count.
    """
    return [{"queries": 0, "recall_at_10": 0.0}]


class TestSearchHeadlineStatus:
    """Pure aggregation of the load and sweep legs into one top-level status."""

    def test_measured_load_and_sweep_is_measured(self) -> None:
        """Every leg succeeding reports the headline as MEASURED."""
        assert search_headline_status(measured_load(), measured_sweep()) == "MEASURED"

    def test_fully_rejected_load_is_failed(self) -> None:
        """A load leg whose every concurrency level was rejected fails the headline."""
        assert search_headline_status(failed_load(), measured_sweep()) == "FAILED"

    def test_zero_measured_queries_in_sweep_is_failed(self) -> None:
        """A sweep leg that measured zero queries fails the headline even if load succeeded."""
        assert search_headline_status(measured_load(), empty_sweep()) == "FAILED"

    def test_both_legs_failing_is_failed(self) -> None:
        """Both legs failing is reported as FAILED, not silently as MEASURED."""
        assert search_headline_status(failed_load(), empty_sweep()) == "FAILED"

    def test_partial_load_rejection_is_still_measured(self) -> None:
        """Load leg status is trusted verbatim rather than re-derived from per-level detail.

        ``run_load_leg`` itself already reports MEASURED when at least one level measured, so the
        headline aggregation must not re-derive a stricter verdict from the per-level detail.
        """
        partial: dict[str, Any] = {
            "status": "MEASURED",
            "levels": [
                {"concurrency": 1, "status": "MEASURED"},
                {"concurrency": 32, "status": "FAILED", "reason": "RESOURCE_EXHAUSTED"},
            ],
        }
        assert search_headline_status(partial, measured_sweep()) == "MEASURED"


def failing_search_phase(config: Any) -> dict[str, Any]:
    """Fake ``search`` phase runner reproducing the FAILED-headline raise contract.

    Args:
        config: Benchmark configuration (unused; this fake never inspects it).

    Returns:
        Never returns; always raises.

    Raises:
        RuntimeError: Unconditionally, mirroring the real phase's failure path.
    """
    del config
    raise RuntimeError("search phase failed: load leg status='FAILED', sweep queries=[0]; inspect search.json")


def test_cli_exits_nonzero_when_search_phase_reports_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The CLI's generic exception boundary turns a FAILED search headline into a nonzero exit.

    ``run_search_against_endpoint`` raises when its aggregated headline status is FAILED (see
    :func:`search_headline_status`), and ``bench.cli.main`` maps any phase exception to exit code
    1. This exercises that boundary end to end through the real CLI argument parsing without
    needing a live gRPC server, by substituting the phase runner with a fake that raises exactly
    the way the fixed phase does.
    """
    monkeypatch.setitem(cli_module.PHASE_RUNNERS, "search", failing_search_phase)
    exit_code: int = cli_module.main(
        [
            "search",
            "--endpoint",
            "127.0.0.1:1",
            "--workspace",
            str(tmp_path / "ws"),
            "--results-root",
            str(tmp_path / "results"),
        ]
    )
    assert exit_code == 1
