"""Shared per-dataset Spark fan-out used by every fleet job.

One embarrassingly parallel shape covers the plan and commit phases of the maintenance and
indexing jobs plus the operator tools (manifest migration, serving-tag flips): apply an
independent per-dataset callable across executors, one telemetry facade per task.

:func:`run_fleet_fanout` owns the single-span driver shell (span open, dataset-count and
optional extra tags, empty-input short circuit, timed fan-out, gauge, log) shared by the
operator tools' single-phase fleet jobs. :func:`run_flat_tagged_job` owns the
parallelize/map/collect plus ok/error tag protocol shared by the flat (not per-dataset)
fleet jobs that flatten many small tasks across many datasets into one Spark job.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Iterator
from typing import Any

from pyspark.sql import SparkSession

from lance_etl.telemetry import Telemetry, TelemetryConfig

logger: logging.Logger = logging.getLogger(__name__)

REWRITE_PARTITION_FACTOR: int = 4
"""Driver-headroom multiple of ``defaultParallelism`` for the flat rewrite/execute fleet job."""

FANOUT_PARTITION_FACTOR: int = 8
"""Driver-headroom multiple of ``defaultParallelism`` for per-dataset plan/commit fan-outs."""

BUILD_PARTITION_FACTOR: int = 16
"""Driver-headroom multiple of ``defaultParallelism`` for the flat build/artifact fleet jobs."""

TAG_FANOUT_PARTITIONS: int = 512
"""Default Spark partition cap for the cheap per-dataset metadata fan-outs (tag flips, manifest
migration, interval-tag prune, and the ETL/pipeline tag-stamp floors)."""

FLAT_OK: str = "ok"
"""Tag on a flat-job result carrying a successful per-task value."""

FLAT_ERROR: str = "error"
"""Tag on a flat-job result carrying a per-dataset failure message."""


def derive_partitions(spark: SparkSession, multiplier: int, floor: int = 1) -> int:
    """Derive a Spark partition count from the cluster size.

    The partition count is derived from the driver's ``sparkContext.defaultParallelism``, which
    reflects the cluster's currently available cores, scaled by ``multiplier`` to give the
    scheduler headroom beyond the raw core count (finer-grained tasks even out skew and let Spark
    overlap scheduling with execution). ``floor`` sets the minimum partition count regardless of
    cluster size, so a single-core local run still gets at least one partition.

    Args:
        spark: Active Spark session whose driver reports ``defaultParallelism``.
        multiplier: The scaling factor applied to ``defaultParallelism``.
        floor: The minimum partition count returned.

    Returns:
        The derived partition count.
    """
    return max(floor, multiplier * spark.sparkContext.defaultParallelism)


def dataset_result_failed(result: dict[str, Any]) -> bool:
    """Return whether one per-dataset fleet result carries an isolation error marker.

    A dataset can fail at the dataset level (a plan failure sets a top-level ``"error"``) or at
    the per-index level (a build or commit failure appends an error entry to ``"indexes"``);
    either makes the dataset a failure. Results with no ``"indexes"`` key (maintenance results,
    for example) simply never match the per-index clause, so this predicate is safe to use on any
    fleet job's per-dataset result shape.

    Args:
        result: One per-dataset result dictionary returned by a fleet job's ``run()``.

    Returns:
        ``True`` when the result carries a dataset-level or per-index ``"error"``.
    """
    return "error" in result or any("error" in item for item in result.get("indexes", []))


def count_failed(results: list[dict[str, Any]]) -> int:
    """Count the per-dataset results carrying an isolation error marker.

    Args:
        results: The per-dataset result dictionaries returned by a fleet job's ``run()``.

    Returns:
        The number of results for which :func:`dataset_result_failed` is ``True``.
    """
    return sum(1 for result in results if dataset_result_failed(result))


def report_fleet_failures(
    results: list[dict[str, Any]],
    run_span: Any,
    driver_telemetry: Telemetry,
    job_label: str,
    phase_tag: Callable[[dict[str, Any]], str],
    fleet_logger: logging.Logger,
) -> list[dict[str, Any]]:
    """Tag, meter, and log the failed datasets at the end of a fleet job's ``run()``.

    Shared by the maintenance and indexing ``run()`` loops: both collect the failed datasets with
    :func:`dataset_result_failed`, tag the run span and a gauge with the failed count, increment a
    per-dataset ``dataset.failed`` counter tagged with its phase, and warn with the first 20 failed
    URIs when any dataset failed. The warning uses ``fleet_logger`` (the caller's own module
    logger) rather than this module's, so the emitted log record's logger name matches the caller
    exactly as it did before this helper existed.

    Args:
        results: The per-dataset terminal results for this run, in input order.
        run_span: The run's telemetry span; tagged with ``failed_datasets``.
        driver_telemetry: The driver's telemetry facade.
        job_label: Human-readable job name prefixing the deferred-datasets warning (for example
            ``"indexing run"`` or ``"maintenance run"``).
        phase_tag: Maps one failed result to the phase tag used on its ``dataset.failed`` counter
            increment.
        fleet_logger: The calling module's logger, so the warning's logger name is preserved.

    Returns:
        The failed results, in input order.
    """
    failed: list[dict[str, Any]] = [result for result in results if dataset_result_failed(result)]
    run_span.set_tag("failed_datasets", len(failed))
    driver_telemetry.gauge("run.datasets_failed", len(failed))
    for item in failed:
        driver_telemetry.incr("dataset.failed", tags=[f"phase:{phase_tag(item)}"])
    if failed:
        failed_uris: list[str] = [str(item["uri"]) for item in failed]
        fleet_logger.warning(
            "%s: %d datasets failed and are deferred to the next run: %s",
            job_label,
            len(failed),
            ", ".join(failed_uris[:20]) + (" ..." if len(failed_uris) > 20 else ""),
        )
    return failed


def fan_out_per_dataset(
    spark: SparkSession,
    uris: list[str],
    telemetry_config: TelemetryConfig,
    per_dataset: Callable[[str, Telemetry], dict[str, Any]],
    partitions: int,
    phase: str = "fanout",
) -> list[dict[str, Any]]:
    """Run an independent per-dataset operation across executors, one task per partition.

    Each executor task creates its own telemetry facade and applies ``per_dataset`` to every URI
    in its partition. Used by the maintenance and indexing plan and commit fan-outs, the
    manifest migration, the serving-tag flip, and the interval-tag prune, all of which are
    embarrassingly parallel one-call-per-dataset operations that differ only in the per-dataset
    callable.

    Per-dataset failure isolation: a single pathological dataset never aborts the whole fleet
    run. Any exception raised by ``per_dataset`` for one URI is caught at this closure boundary,
    logged, counted under ``dataset.fanout_error`` (tagged with ``phase``), and turned into an
    error marker ``{"uri": uri, "error": str(exc), "phase": phase}`` in the returned list instead
    of propagating. Every other dataset in the partition still completes, and the caller detects
    the failed datasets by scanning for the ``"error"`` key. Business functions keep their own
    raise/return contracts; only this orchestration layer swallows the error.

    An empty ``uris`` list is returned immediately without submitting a Spark job, because
    ``sparkContext.parallelize`` with zero slices raises a Java exception.

    Args:
        spark: Active Spark session.
        uris: Dataset URIs to process.
        telemetry_config: Telemetry configuration created per executor process.
        per_dataset: The operation to apply to one URI with an executor-local telemetry facade.
        partitions: Upper bound on Spark partitions, capped at the URI count.
        phase: Phase label stamped onto error markers and the ``dataset.fanout_error`` metric so
            the operator can tell which fleet phase a failed dataset died in.

    Returns:
        One outcome dictionary per dataset, either the ``per_dataset`` result or an error marker.
    """
    if not uris:
        return []

    def partition(part: Iterable[str]) -> Iterator[dict[str, Any]]:
        """Apply the operation to one partition of dataset URIs on an executor.

        Args:
            part: Dataset URIs assigned to this executor task.

        Yields:
            One outcome dictionary per dataset, an error marker when the operation raised.
        """
        executor_telemetry: Telemetry = Telemetry.create(telemetry_config)
        for uri in part:
            try:
                yield per_dataset(uri, executor_telemetry)
            except Exception as exc:
                logger.warning("fanout: %s failed in phase %s, isolating: %s", uri, phase, exc)
                executor_telemetry.incr("dataset.fanout_error", tags=[f"phase:{phase}"])
                yield {"uri": uri, "error": str(exc), "phase": phase}

    return spark.sparkContext.parallelize(uris, min(len(uris), partitions)).mapPartitions(partition).collect()


def run_fleet_fanout(
    spark: SparkSession,
    dataset_uris: Iterable[str],
    telemetry_config: TelemetryConfig,
    per_dataset: Callable[[str, Telemetry], dict[str, Any]],
    partitions: int,
    span_name: str,
    phase: str,
    timer_metric: str,
    gauge_metric: str,
    gauge_value: Callable[[list[dict[str, Any]]], int],
    log_results: Callable[[list[dict[str, Any]]], None],
    span_tags: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Run one single-phase fleet job's driver shell around :func:`fan_out_per_dataset`.

    Owns the shape shared by the operator tools' single-phase fleet jobs (manifest migration,
    serving-tag flips, interval-tag pruning): open a driver telemetry span, tag it with the
    dataset count plus any caller-supplied ``span_tags``, short-circuit on an empty URI list
    before submitting any Spark job, run the per-dataset fan-out inside a timed block, gauge the
    caller's summary metric, and log the caller's summary line.

    The multi-phase fleet jobs (:meth:`~lance_etl.maintenance.job.MaintenanceJob.run`, the
    indexing job's ``run()``, and :func:`~lance_etl.maintenance.cluster.run_cluster_rewrites`)
    deliberately do NOT share this shell: they interleave several fan-outs and replan rounds
    inside one span, which this single-fan-out-per-span shape cannot express.

    Args:
        spark: Active Spark session.
        dataset_uris: Datasets to process.
        telemetry_config: Telemetry configuration created per executor process.
        per_dataset: The operation to apply to one URI with an executor-local telemetry facade.
        partitions: Upper bound on Spark partitions, capped at the URI count.
        span_name: The driver span's name.
        phase: Phase label forwarded to :func:`fan_out_per_dataset` for error markers and metrics.
        timer_metric: Metric name timing the fan-out call.
        gauge_metric: Metric name for the summary gauge emitted after the fan-out.
        gauge_value: Computes the gauge value from the collected results.
        log_results: Logs the caller's summary line from the collected results.
        span_tags: Extra tags set on the run span beyond ``dataset_count``, in iteration order.

    Returns:
        One outcome dictionary per dataset, or an empty list when ``dataset_uris`` was empty.
    """
    driver_telemetry: Telemetry = Telemetry.create(telemetry_config)
    with driver_telemetry.span(span_name) as run_span:
        uris: list[str] = list(dataset_uris)
        run_span.set_tag("dataset_count", len(uris))
        for tag_name, tag_value in (span_tags or {}).items():
            run_span.set_tag(tag_name, tag_value)
        if not uris:
            return []
        with driver_telemetry.timed(timer_metric):
            results: list[dict[str, Any]] = fan_out_per_dataset(
                spark, uris, telemetry_config, per_dataset, partitions, phase=phase
            )
        driver_telemetry.gauge(gauge_metric, gauge_value(results))
        log_results(results)
        return results


def run_flat_tagged_job[Task](
    spark: SparkSession,
    tasks: list[Task],
    run_one: Callable[[Task], tuple[str, str, Any]],
    partitions: int,
) -> tuple[dict[str, list[Any]], dict[str, str]]:
    """Run one flat parallelize/map/collect job under the ok/error tag protocol.

    Owns the shape shared by the maintenance rewrite phase, the cluster histogram phase, and the
    cluster index-segment rebuild: many small tasks spanning many datasets flatten into one Spark
    job so the scheduler sees the fleet's whole workload at once, and ``run_one`` tags every
    result ``(:data:`FLAT_OK`, uri, value)`` or ``(:data:`FLAT_ERROR`, uri, message)`` so a single
    task's failure never aborts the job and is instead attributed to its owning dataset. An
    empty ``tasks`` list returns immediately without submitting a Spark job, because
    ``sparkContext.parallelize`` with zero slices raises a Java exception.

    Errored URIs are NOT removed from the returned ``grouped`` mapping — a URI can carry both
    successful and failed task outcomes, and only the caller knows whether a partial success is
    usable, so the caller decides whether to drop it.

    Args:
        spark: Active Spark session.
        tasks: The flattened tasks to run, one Spark task per list element.
        run_one: Executes one task, returning its ok/error tag, owning URI, and payload.
        partitions: Upper bound on Spark partitions, capped at the task count.

    Returns:
        A ``(grouped, errors_by_uri)`` pair: successful values grouped by URI in task order, and
        the first error message per URI whose task failed.
    """
    if not tasks:
        return {}, {}
    slices: int = max(1, min(partitions, len(tasks)))
    tagged: list[tuple[str, str, Any]] = spark.sparkContext.parallelize(tasks, slices).map(run_one).collect()
    grouped: dict[str, list[Any]] = {}
    errors_by_uri: dict[str, str] = {}
    for tag, uri, value in tagged:
        if tag == FLAT_OK:
            grouped.setdefault(uri, []).append(value)
        else:
            errors_by_uri.setdefault(uri, str(value))
    return grouped, errors_by_uri
