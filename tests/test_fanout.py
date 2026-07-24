"""Tests for the shared per-dataset Spark fan-out primitives in :mod:`lance_etl.fanout`.

Covers the partition-count derivation math, per-item failure isolation in
:func:`~lance_etl.fanout.fan_out_per_dataset` and :func:`~lance_etl.fanout.run_flat_tagged_job`,
the :func:`~lance_etl.fanout.dataset_result_failed` / :func:`~lance_etl.fanout.count_failed`
predicates, and the :func:`~lance_etl.fanout.report_fleet_failures` output shape. Reuses the
:class:`FakeSpark` / :class:`FakeSparkContext` doubles already established in ``conftest.py`` for
the fleet-orchestration tests instead of inventing a new fake.
"""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import MagicMock

import pytest
from conftest import FakeSpark

from lance_etl.fanout import (
    FLAT_ERROR,
    FLAT_OK,
    count_failed,
    dataset_result_failed,
    derive_partitions,
    fan_out_per_dataset,
    report_fleet_failures,
    run_flat_tagged_job,
)
from lance_etl.telemetry import TelemetryConfig


def test_derive_partitions_scales_with_multiplier_and_cluster_size() -> None:
    """The partition count is the multiplier times the fake cluster's fixed core count."""
    spark: FakeSpark = FakeSpark()
    assert spark.sparkContext.defaultParallelism == 4
    assert derive_partitions(spark, multiplier=8) == 32
    assert derive_partitions(spark, multiplier=1) == 4


def test_derive_partitions_respects_floor_on_small_clusters() -> None:
    """A floor above the scaled value wins, so a tiny cluster still gets enough partitions."""
    spark: FakeSpark = FakeSpark()
    assert derive_partitions(spark, multiplier=1, floor=100) == 100
    assert derive_partitions(spark, multiplier=50, floor=1) == 200


def test_dataset_result_failed_detects_top_level_and_per_index_errors() -> None:
    """A dataset-level error or any per-index error both mark the result failed."""
    assert dataset_result_failed({"uri": "a"}) is False
    assert dataset_result_failed({"uri": "a", "error": "boom"}) is True
    assert dataset_result_failed({"uri": "a", "indexes": [{"name": "v"}]}) is False
    assert dataset_result_failed({"uri": "a", "indexes": [{"name": "v", "error": "boom"}]}) is True
    assert dataset_result_failed({"uri": "a", "indexes": [{"name": "v"}, {"name": "t", "error": "x"}]}) is True


def test_count_failed_sums_only_failed_results() -> None:
    """The failure count only tallies results that carry an isolation error marker."""
    results: list[dict[str, Any]] = [
        {"uri": "a"},
        {"uri": "b", "error": "boom"},
        {"uri": "c", "indexes": [{"name": "v", "error": "x"}]},
        {"uri": "d", "indexes": [{"name": "v"}]},
    ]
    assert count_failed(results) == 2
    assert count_failed([]) == 0


def test_fan_out_per_dataset_isolates_a_single_failure(telemetry_config: TelemetryConfig) -> None:
    """One pathological URI does not abort the others, and its failure is turned into a marker."""
    spark: FakeSpark = FakeSpark()

    def per_dataset(uri: str, telemetry: Any) -> dict[str, Any]:
        """Fail deterministically for one URI, succeed for the rest.

        Args:
            uri: The dataset URI under test.
            telemetry: Executor-local telemetry facade, unused by the fixture logic.

        Returns:
            A per-dataset success marker.

        Raises:
            ValueError: When ``uri`` is the designated bad dataset.
        """
        del telemetry
        if uri == "bad.lance":
            raise ValueError("simulated failure")
        return {"uri": uri, "ok": True}

    results: list[dict[str, Any]] = fan_out_per_dataset(
        spark, ["good1.lance", "bad.lance", "good2.lance"], telemetry_config, per_dataset, partitions=4, phase="test"
    )

    by_uri: dict[str, dict[str, Any]] = {str(result["uri"]): result for result in results}
    assert by_uri["good1.lance"] == {"uri": "good1.lance", "ok": True}
    assert by_uri["good2.lance"] == {"uri": "good2.lance", "ok": True}
    assert by_uri["bad.lance"]["error"] == "simulated failure"
    assert by_uri["bad.lance"]["phase"] == "test"


def constant_phase_tag(tag: str) -> Any:
    """Build a ``phase_tag`` callable that reports one fixed phase for every failed item.

    Args:
        tag: The phase label every failed result should be tagged with.

    Returns:
        A callable suitable for :func:`~lance_etl.fanout.report_fleet_failures`'s ``phase_tag``.
    """

    def phase_tag(item: dict[str, Any]) -> str:
        """Report the fixed phase, confirming the item is a per-dataset result mapping.

        Args:
            item: One failed per-dataset result.

        Returns:
            The fixed phase label.
        """
        assert "uri" in item
        return tag

    return phase_tag


def unreachable_per_dataset(uri: str, telemetry: Any) -> dict[str, Any]:
    """Fail loudly if the empty-input short circuit ever calls the per-dataset operation.

    Args:
        uri: The dataset URI, never actually supplied.
        telemetry: Executor-local telemetry facade, never actually supplied.

    Returns:
        Never returns; always raises.

    Raises:
        AssertionError: Always, since an empty URI list must short-circuit before this runs.
    """
    raise AssertionError(f"per_dataset should not run for {uri} ({telemetry})")


def test_fan_out_per_dataset_short_circuits_on_empty_input(telemetry_config: TelemetryConfig) -> None:
    """An empty URI list returns immediately without submitting any Spark job."""
    spark: MagicMock = MagicMock()
    spark.sparkContext.parallelize.side_effect = AssertionError("parallelize should not be called")

    results: list[dict[str, Any]] = fan_out_per_dataset(
        spark, [], telemetry_config, unreachable_per_dataset, partitions=4, phase="test"
    )

    assert results == []
    spark.sparkContext.parallelize.assert_not_called()


def test_run_flat_tagged_job_groups_ok_and_records_first_error_per_uri() -> None:
    """Successful task outputs group by owning URI and the first per-URI error is kept."""
    spark: FakeSpark = FakeSpark()
    tasks: list[tuple[str, str]] = [
        ("a.lance", "ok-1"),
        ("a.lance", "ok-2"),
        ("b.lance", "first-error"),
        ("b.lance", "second-error"),
        ("c.lance", "ok-3"),
    ]

    def run_one(task: tuple[str, str]) -> tuple[str, str, Any]:
        """Tag a task as ok or error based on its payload.

        Args:
            task: A ``(uri, payload)`` pair.

        Returns:
            The ok/error tag, owning URI, and payload or error message.
        """
        uri, payload = task
        if "error" in payload:
            return (FLAT_ERROR, uri, payload)
        return (FLAT_OK, uri, payload)

    grouped, errors_by_uri = run_flat_tagged_job(spark, tasks, run_one, partitions=4)

    assert grouped == {"a.lance": ["ok-1", "ok-2"], "c.lance": ["ok-3"]}
    assert errors_by_uri == {"b.lance": "first-error"}


def test_run_flat_tagged_job_keeps_partial_success_uris_in_grouped() -> None:
    """A URI with both a success and a failure keeps its successful value and its error."""
    spark: FakeSpark = FakeSpark()
    tasks: list[tuple[str, str]] = [("a.lance", "ok"), ("a.lance", "error")]

    def run_one(task: tuple[str, str]) -> tuple[str, str, Any]:
        """Tag a task as ok or error based on its payload.

        Args:
            task: A ``(uri, payload)`` pair.

        Returns:
            The ok/error tag, owning URI, and payload or error message.
        """
        uri, payload = task
        if payload == "error":
            return (FLAT_ERROR, uri, "boom")
        return (FLAT_OK, uri, payload)

    grouped, errors_by_uri = run_flat_tagged_job(spark, tasks, run_one, partitions=4)

    assert grouped == {"a.lance": ["ok"]}
    assert errors_by_uri == {"a.lance": "boom"}


def test_run_flat_tagged_job_short_circuits_on_empty_tasks() -> None:
    """An empty task list returns two empty dicts without submitting any Spark job."""
    spark: MagicMock = MagicMock()
    spark.sparkContext.parallelize.side_effect = AssertionError("parallelize should not be called")

    grouped, errors_by_uri = run_flat_tagged_job(spark, [], lambda task: (FLAT_OK, "x", task), partitions=4)

    assert grouped == {}
    assert errors_by_uri == {}
    spark.sparkContext.parallelize.assert_not_called()


def test_report_fleet_failures_tags_meters_logs_and_returns_only_failures(caplog: pytest.LogCaptureFixture) -> None:
    """The helper isolates the failed subset, tags and gauges the count, and warns with the URIs."""
    results: list[dict[str, Any]] = [
        {"uri": "a.lance"},
        {"uri": "b.lance", "error": "boom"},
        {"uri": "c.lance", "indexes": [{"name": "v", "error": "x"}]},
    ]
    run_span: MagicMock = MagicMock()
    driver_telemetry: MagicMock = MagicMock()
    fleet_logger: logging.Logger = logging.getLogger("test_fanout.report_fleet_failures")

    with caplog.at_level(logging.WARNING, logger=fleet_logger.name):
        failed: list[dict[str, Any]] = report_fleet_failures(
            results,
            run_span,
            driver_telemetry,
            job_label="indexing run",
            phase_tag=constant_phase_tag("build"),
            fleet_logger=fleet_logger,
        )

    assert [item["uri"] for item in failed] == ["b.lance", "c.lance"]
    run_span.set_tag.assert_called_once_with("failed_datasets", 2)
    driver_telemetry.gauge.assert_called_once_with("run.datasets_failed", 2)
    assert driver_telemetry.incr.call_args_list == [
        (("dataset.failed",), {"tags": ["phase:build"]}),
        (("dataset.failed",), {"tags": ["phase:build"]}),
    ]
    assert len(caplog.records) == 1
    message: str = caplog.records[0].getMessage()
    assert "indexing run" in message
    assert "b.lance" in message
    assert "c.lance" in message


def test_report_fleet_failures_truncates_the_logged_uri_list() -> None:
    """More than 20 failed URIs are truncated in the warning with a trailing ellipsis marker."""
    results: list[dict[str, Any]] = [{"uri": f"d{i}.lance", "error": "boom"} for i in range(25)]
    run_span: MagicMock = MagicMock()
    driver_telemetry: MagicMock = MagicMock()
    fleet_logger: logging.Logger = logging.getLogger("test_fanout.report_fleet_failures_truncated")
    warnings: list[str] = []
    fleet_logger.warning = lambda message, *args: warnings.append(message % args)

    failed: list[dict[str, Any]] = report_fleet_failures(
        results,
        run_span,
        driver_telemetry,
        job_label="maintenance run",
        phase_tag=constant_phase_tag("compact"),
        fleet_logger=fleet_logger,
    )

    assert len(failed) == 25
    assert len(warnings) == 1
    assert warnings[0].endswith("...")
    assert warnings[0].count("d") >= 20


def test_report_fleet_failures_is_silent_when_nothing_failed() -> None:
    """A fully successful fleet run does not warn and returns an empty failed list."""
    results: list[dict[str, Any]] = [{"uri": "a.lance"}, {"uri": "b.lance"}]
    run_span: MagicMock = MagicMock()
    driver_telemetry: MagicMock = MagicMock()
    fleet_logger: logging.Logger = logging.getLogger("test_fanout.report_fleet_failures_clean")
    fleet_logger.warning = MagicMock(side_effect=AssertionError("should not warn on a clean run"))

    failed: list[dict[str, Any]] = report_fleet_failures(
        results,
        run_span,
        driver_telemetry,
        job_label="indexing run",
        phase_tag=constant_phase_tag("build"),
        fleet_logger=fleet_logger,
    )

    assert failed == []
    run_span.set_tag.assert_called_once_with("failed_datasets", 0)
    driver_telemetry.gauge.assert_called_once_with("run.datasets_failed", 0)
    driver_telemetry.incr.assert_not_called()
