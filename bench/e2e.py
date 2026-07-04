"""Batch-major end-to-end benchmark orchestration with per-batch tagging and historical-tag verification.

Drives the full pipeline in batch-major order: for each ETL window the orchestrator runs ETL then
the production ``lance_etl.pipeline.PipelineJob`` (compaction, index build, and interval-tag stamp
in one serialized fleet run), then optionally prewarms and queries the gRPC server at the new tag.
Tag pruning is disabled in the bench driver (``tag_keep_last=None``) so every batch's interval tag
is retained for the historical-tag verification pass at the end of the run.  After all batches
complete the verification pass opens every tagged dataset version and asserts the row count matches
what was recorded at tag time.  A final recall measurement runs at the last tag when the gRPC server
is reachable and the adapter supplies official ground truth (or the benchmark brute-forces a
ground-truth subset).

This subcommand is complementary to ``all``: ``all`` is phase-major (full ingest, then full index,
then compact), whereas ``e2e`` is batch-major (ingest batch i, pipeline batch i, repeat). Per-stage
index timings are no longer emitted by the e2e path because all index types now run inside a single
``LanceIndexer.run`` call within the pipeline job.

The ``--no-text`` flag is fully respected: the FTS index column is omitted from the union config
and the gRPC FTS/hybrid legs are skipped, matching the behaviour of the individual phase commands.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import grpc
import lance
import numpy as np

from bench.config import BenchConfig
from bench.groundtruth import recall_at
from bench.grpc_client import (
    generate_stubs,
    load_stubs,
    prewarm_dataset,
    result_vector_ids,
    vector_search_at_tag,
)
from bench.indexes import union_index_config
from bench.ingest import batch_windows, run_etl_window
from bench.results import ensure_dir, save_phase
from bench.spark_session import bench_telemetry_config, build_spark
from bench.telemetry_capture import CaptureConfig, TelemetryCapture
from lance_etl.maintenance import MaintenanceConfig
from lance_etl.pipeline import PipelineConfig, PipelineJob

logger: logging.Logger = logging.getLogger(__name__)

RECALL_CUTOFFS: tuple[int, ...] = (1, 10, 100)
E2E_NPROBES: int = 10


def window_tag_name(window_end: str) -> str:
    """Convert a window-end timestamp string into a Lance tag name.

    Lance tag names allow only ``[A-Za-z0-9._-]``, so colons are replaced. The input
    is a Spark timestamp literal such as ``"2024-01-01 12:00:00"`` which is formatted
    to ``"20240101T120000Z"`` following the ISO-8601 basic format without colons.

    Args:
        window_end: The window-end timestamp literal from :func:`batch_windows`.

    Returns:
        A colon-free tag name suitable for Lance.
    """
    dt: datetime = datetime.strptime(window_end, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
    return dt.strftime("%Y%m%dT%H%M%SZ")


def run_e2e_pipeline_batch(config: BenchConfig, batch_index: int, tag: str) -> dict[str, Any]:
    """Run the production PipelineJob for one ETL batch: compact, index, and stamp in order.

    Builds a :class:`~lance_etl.pipeline.PipelineConfig` with ``tag_keep_last=None`` so every
    batch's interval tag is retained for the historical-tag verification pass.  Pruning is
    disabled in the bench driver intentionally: all interval tags must survive for the
    post-run verification to read their pinned versions.

    The maintenance sub-config mirrors the phase-major ``compact`` command:
    ``defer_index_remap=False`` so covering IVF indexes are remapped inline during the commit.
    The indexing sub-config is the union of all column types produced by
    :func:`~bench.indexes.union_index_config`.

    After ``PipelineJob.run`` returns, each dataset's row count is read via
    ``lance.dataset(uri).count_rows()`` and stored in ``tag_stats`` so
    :func:`verify_historical_tags` can verify the pinned version without arithmetic
    approximation.

    Args:
        config: Benchmark configuration.
        batch_index: Zero-based batch index, used for the Spark application name.
        tag: The colon-free interval tag name to stamp after the pipeline completes.

    Returns:
        A dictionary with keys:

        - ``tag_stats``: list of per-dataset dicts with ``uri``, ``tag``, ``version``,
          ``created``, and ``row_count`` (the actual count at the tagged version).
        - ``pipeline``: lean summary dict with ``seconds`` (total wall time), ``counts``
          (from the pipeline return doc), ``maintenance_datasets`` (count), and
          ``index_datasets`` (count).
    """
    telemetry_cfg = bench_telemetry_config()
    pipeline_cfg = PipelineConfig(
        telemetry=telemetry_cfg,
        maintenance=MaintenanceConfig(
            telemetry=telemetry_cfg,
            target_rows_per_fragment=config.compact_target_rows,
            defer_index_remap=False,
        ),
        indexing=union_index_config(config),
        tag_stamp=tag,
        tag_keep_last=None,
        serve_tag=False,
    )
    uris: list[str] = config.dataset_uris()
    spark = build_spark(config, f"bench-e2e-pipeline-{batch_index}")
    try:
        started: float = time.perf_counter()
        pipeline_result: dict[str, Any] = PipelineJob(pipeline_cfg).run(spark, uris)
        elapsed: float = time.perf_counter() - started
    finally:
        spark.stop()

    stamp_by_uri: dict[str, dict[str, Any]] = {
        r["uri"]: r for r in pipeline_result.get("stamp_results", []) if r.get("tag") == tag
    }
    tag_stats: list[dict[str, Any]] = []
    for uri in uris:
        stamp = stamp_by_uri.get(uri, {"uri": uri, "tag": tag, "version": None, "created": False})
        record: dict[str, Any] = {
            "uri": uri,
            "tag": stamp.get("tag", tag),
            "version": stamp.get("version"),
            "created": stamp.get("created", False),
            "row_count": lance.dataset(uri).count_rows(),
        }
        tag_stats.append(record)

    counts: dict[str, Any] = pipeline_result.get("counts", {})
    pipeline_summary: dict[str, Any] = {
        "seconds": round(elapsed, 3),
        "counts": counts,
        "maintenance_datasets": len(pipeline_result.get("maintenance_results", [])),
        "index_datasets": len(pipeline_result.get("index_results", [])),
    }
    return {"tag_stats": tag_stats, "pipeline": pipeline_summary}


def verify_historical_tags(tag_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Open every tagged version and assert the row count matches the count recorded at tag time.

    For each tag-dataset pair, the dataset is re-opened at the tag's resolved version via
    ``lance.dataset(uri, version=tag)`` and ``count_rows()`` is checked against the
    ``row_count`` recorded in the tag statistics at stamp time. This verifies that the tagged
    version is still accessible and has not been corrupted or pruned.

    Args:
        tag_records: Records produced by :func:`run_e2e_pipeline_batch`, one per (batch, dataset).

    Returns:
        One verification outcome per (tag, URI) pair with ``ok``, ``expected``, and ``actual`` keys.
    """
    outcomes: list[dict[str, Any]] = []
    for record in tag_records:
        tag: str = record["tag"]
        uri: str = record["uri"]
        expected: int = record["row_count"]
        try:
            ds = lance.dataset(uri, version=tag)
            actual: int = ds.count_rows()
            ok: bool = actual == expected
        except Exception as exc:
            outcomes.append({"tag": tag, "uri": uri, "ok": False, "error": str(exc)[:500]})
            continue
        outcomes.append({"tag": tag, "uri": uri, "ok": ok, "expected": expected, "actual": actual})
        if not ok:
            logger.warning("historical tag %r on %s: expected %d rows, got %d", tag, uri, expected, actual)
    return outcomes


def prewarm_orgs_at_tag(stub: Any, pb2: Any, config: BenchConfig, tag: str) -> dict[str, Any]:
    """Prewarm every org's dataset pinned to a serve tag, capturing per-org outcomes.

    Args:
        stub: The connected ``SearchServiceStub``.
        pb2: The generated protobuf module.
        config: Benchmark configuration.
        tag: The serve tag to prewarm at.

    Returns:
        Per-org prewarm reports, with failures recorded as ``error`` strings.
    """
    outcomes: dict[str, Any] = {}
    for org in config.org_ids():
        try:
            outcomes[org] = prewarm_dataset(stub, pb2, org, fts_with_position=False, tag=tag)
        except Exception as exc:
            outcomes[org] = {"error": str(exc)[:500]}
    return outcomes


def org_recall_at_tag(
    stub: Any,
    pb2: Any,
    config: BenchConfig,
    org: str,
    queries: np.ndarray,
    org_gt: np.ndarray,
    tag: str,
) -> dict[str, Any] | None:
    """Run the vector recall sweep for one org pinned to a serve tag.

    Args:
        stub: The connected ``SearchServiceStub``.
        pb2: The generated protobuf module.
        config: Benchmark configuration.
        org: The org to sweep.
        queries: The query matrix.
        org_gt: The org's ground-truth global ids.
        tag: The serve tag to query at.

    Returns:
        The org's recall point with per-cutoff recall and mean latency, or ``None`` when every
        query errored.
    """
    retrieved: list[np.ndarray] = []
    latencies: list[float] = []
    for query in queries:
        try:
            response, elapsed_ms = vector_search_at_tag(stub, pb2, org, query, config.search_k, E2E_NPROBES, tag=tag)
            latencies.append(elapsed_ms)
            retrieved.append(result_vector_ids(response.results))
        except Exception as exc:
            logger.warning("gRPC error during tag recall sweep for org %s: %s", org, exc)
    if not retrieved:
        return None
    expected: np.ndarray = org_gt[: len(retrieved)]
    point: dict[str, Any] = {"org": org, "tag": tag, "queries": len(retrieved)}
    for cutoff in RECALL_CUTOFFS:
        point[f"recall_at_{cutoff}"] = round(float(recall_at(expected, retrieved, cutoff)), 4)
    if latencies:
        point["mean_ms"] = round(float(np.mean(latencies)), 3)
    return point


def run_grpc_legs_at_tag(
    config: BenchConfig,
    tag: str,
    queries: np.ndarray,
    ground_truth: dict[str, np.ndarray],
    grpc_gen_dir: Path,
) -> dict[str, Any]:
    """Run prewarm-at-tag and a vector recall leg pinned to a serve tag via gRPC.

    Probes the server before attempting any RPCs. If the server is unreachable the call
    returns a skipped record with a descriptive reason so callers can treat the absence of a
    running server as a non-fatal condition (used in tests and in the ``all`` chain).

    Args:
        config: Benchmark configuration.
        tag: The serve tag to query at.
        queries: The query matrix (capped by ``--max-queries`` if set).
        ground_truth: Per-org ground-truth global ids.
        grpc_gen_dir: Directory holding the compiled gRPC stubs.

    Returns:
        Prewarm outcome (per org), recall results at the tag, and cold/warm first-query latency.
    """
    try:
        pb2, pb2_grpc = load_stubs(generate_stubs(grpc_gen_dir))
        channel = grpc.insecure_channel(config.endpoint)
        grpc.channel_ready_future(channel).result(timeout=3.0)
        stub = pb2_grpc.SearchServiceStub(channel)
    except Exception as exc:
        return {"skipped": f"gRPC server unreachable at {config.endpoint}: {exc}"}

    prewarm_outcomes: dict[str, Any] = prewarm_orgs_at_tag(stub, pb2, config, tag)

    first_latencies: dict[str, Any] = {}
    for org in config.org_ids():
        try:
            resp, cold_ms = vector_search_at_tag(stub, pb2, org, queries[0], 10, E2E_NPROBES, tag=tag)
            del resp
            _, warm_ms = vector_search_at_tag(stub, pb2, org, queries[0], 10, E2E_NPROBES, tag=tag)
            first_latencies[org] = {"cold_ms": round(cold_ms, 3), "warm_ms": round(warm_ms, 3)}
        except Exception as exc:
            first_latencies[org] = {"error": str(exc)[:500]}

    sweep_recalls: list[dict[str, Any]] = []
    for org in config.org_ids():
        if org not in ground_truth:
            continue
        point: dict[str, Any] | None = org_recall_at_tag(stub, pb2, config, org, queries, ground_truth[org], tag)
        if point is not None:
            sweep_recalls.append(point)

    return {"prewarm": prewarm_outcomes, "first_latencies": first_latencies, "recall": sweep_recalls}


def load_queries_and_gt(config: BenchConfig) -> tuple[np.ndarray, dict[str, np.ndarray]]:
    """Load the query matrix and per-org ground truth from the prepared artifact directory.

    Args:
        config: Benchmark configuration.

    Returns:
        The query matrix (possibly capped by ``--max-queries``) and per-org ground-truth arrays.
    """
    prepared: Path = config.prepared_dir()
    queries: np.ndarray = np.load(prepared / "queries.npy")
    ground_truth_file = np.load(prepared / "ground_truth.npz")
    ground_truth: dict[str, np.ndarray] = {org: ground_truth_file[org] for org in ground_truth_file.files}
    if config.max_queries is not None:
        queries = queries[: config.max_queries]
    return queries, ground_truth


def run_e2e(config: BenchConfig) -> dict[str, Any]:
    """Run the batch-major end-to-end benchmark with per-batch tagging.

    For each batch window: ETL, index, compact, tag. After all batches: historical-tag
    verification, optional gRPC recall legs at each tag, and a final full recall sweep at
    the last tag.

    When ``config.capture_telemetry`` is True, a :class:`~bench.telemetry_capture.TelemetryCapture`
    session is started before the first batch and torn down after the final recall sweep. The
    session binds a DogStatsD UDP listener on ``127.0.0.1:{config.statsd_port}`` and an OTLP
    gRPC receiver on ``127.0.0.1:{config.otlp_port}``, writes all received telemetry to
    ``{config.workspace}/telemetry/``, and injects the required environment variables into
    the current process so all Spark executors and the gRPC client channel can reach the
    listeners. Failure to bind either listener logs a warning and is non-fatal: the emitters
    on both the Python and Rust sides are fire-and-forget UDP/gRPC, so a missing listener
    never fails a pipeline run.

    Args:
        config: Benchmark configuration.

    Returns:
        The phase result document saved as ``e2e.json`` in the run directory.
    """
    if config.capture_telemetry:
        capture_cfg = CaptureConfig(
            telemetry_dir=config.telemetry_dir(),
            statsd_port=config.statsd_port,
            otlp_port=config.otlp_port,
        )
        with TelemetryCapture(capture_cfg) as capture:
            prev_env: dict[str, str | None] = {}
            for key, val in capture.env_overrides.items():
                prev_env[key] = os.environ.get(key)
                os.environ[key] = val
            try:
                return run_e2e_body(config)
            finally:
                for key, old_val in prev_env.items():
                    if old_val is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = old_val
    return run_e2e_body(config)


def run_e2e_body(config: BenchConfig) -> dict[str, Any]:
    """Execute the e2e benchmark body without managing capture lifecycle.

    Called by :func:`run_e2e` after the capture context has been entered (when capture is
    enabled) or directly (when capture is disabled).

    Args:
        config: Benchmark configuration.

    Returns:
        The phase result document saved as ``e2e.json`` in the run directory.
    """
    ensure_dir(config.run_dir())
    windows: list[tuple[str, str]] = batch_windows(config.batches)
    batch_records: list[dict[str, Any]] = []
    all_tag_records: list[dict[str, Any]] = []
    grpc_gen_dir: Path = config.workspace / "grpc_gen"

    for batch_index, window in enumerate(windows):
        window_start: str
        window_end: str
        window_start, window_end = window
        tag: str = window_tag_name(window_end)
        logger.info(
            "e2e batch %d/%d window [%s, %s) tag=%s",
            batch_index + 1,
            len(windows),
            window_start,
            window_end,
            tag,
        )

        etl_start: float = time.perf_counter()
        run_etl_window(config, batch_index, window)
        etl_seconds: float = time.perf_counter() - etl_start

        pipeline_batch_result: dict[str, Any] = run_e2e_pipeline_batch(config, batch_index, tag)
        tag_stats: list[dict[str, Any]] = pipeline_batch_result["tag_stats"]
        all_tag_records.extend(tag_stats)

        batch_records.append(
            {
                "batch": batch_index,
                "window_start": window_start,
                "window_end": window_end,
                "tag": tag,
                "etl_seconds": round(etl_seconds, 3),
                "pipeline": pipeline_batch_result["pipeline"],
                "tag_stats": tag_stats,
            }
        )

    verification: list[dict[str, Any]] = verify_historical_tags(all_tag_records)
    all_ok: bool = all(rec.get("ok", False) for rec in verification)
    if not all_ok:
        logger.warning("historical-tag verification: some row counts did not match (see e2e.json)")

    queries, ground_truth = load_queries_and_gt(config)
    grpc_legs_per_tag: dict[str, Any] = {}
    for batch_record in batch_records:
        current_tag: str = batch_record["tag"]
        grpc_legs_per_tag[current_tag] = run_grpc_legs_at_tag(config, current_tag, queries, ground_truth, grpc_gen_dir)

    last_tag: str = window_tag_name(windows[-1][1])
    final_recall: dict[str, Any] = {}
    last_grpc = grpc_legs_per_tag.get(last_tag, {})
    if "skipped" not in last_grpc:
        final_recall = run_grpc_legs_at_tag(config, last_tag, queries, ground_truth, grpc_gen_dir)

    return save_phase(
        config,
        "e2e",
        {
            "batches": batch_records,
            "tags_created": [r["tag"] for r in all_tag_records if r.get("created")],
            "historical_tag_verification": {"ok": all_ok, "checks": verification},
            "grpc_legs_per_tag": grpc_legs_per_tag,
            "final_recall_at_last_tag": final_recall,
        },
    )
