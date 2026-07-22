"""End-to-end benchmark that qualifies the production PostgreSQL reconciler path.

The benchmark stands up the exact local control plane the release runbook documents: an isolated
migrated PostgreSQL schema, a partitioned Iceberg source table matching the fixed source contract,
source registration through :class:`~lance_etl.state.ControlPlaneRepository`, and a wired
:class:`~lance_etl.reconciler.service.ReconcilerApplication`. Each batch appends one ordinal window
of corpus rows as a new Iceberg snapshot, then reconciliation cycles carry every dataset through
ingest, compaction, indexing, validation, prewarm, and publication. The benchmark reads the
resulting publications back through
:meth:`~lance_etl.state.ControlPlaneRepository.resolve_serving_dataset` and verifies each
organization's published dataset opens at its exact version with the expected terminal row count
and every index the installed specification declares present.

This subcommand replaces the retired batch-major pipeline path. The catalog search leg (recall,
first-query latency, FTS, and hybrid) self-hosts a search-api subprocess inside the PostgreSQL
isolation window whenever ``config.search_api_binary`` exists, and is recorded as ``NOT_RUN``
otherwise so a bare ``e2e`` run never manufactures search coverage. An externally started server
cannot see the ephemeral control-plane schema, so ``e2e`` never dials ``--endpoint``.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

import lance
import numpy as np

from bench.config import RECALL_CUTOFFS, BenchConfig
from bench.groundtruth import recall_at
from bench.grpc_client import (
    generate_and_load_stubs,
    open_stub,
    result_record_ids,
    vector_search,
)
from bench.reconcile import (
    append_production_batch,
    batch_windows_by_ordinal,
    bench_spec_index_names,
    bench_workspace_spark,
    benchmark_adapter,
    build_reconciler_application,
    create_production_source_table,
    drain_reconciler,
    expected_org_rows,
    expected_versions_from_repository,
    isolated_control_plane,
    resolve_database_url,
    resolve_org_serving,
    source_table_identifier,
)
from bench.results import ensure_dir, save_phase, write_json
from bench.search import load_artifacts, run_fts_leg, run_hybrid_leg
from bench.search_server import self_hosted_search_api
from bench.telemetry_capture import CaptureConfig, TelemetryCapture
from lance_etl.state import ControlPlaneRepository, ServingDataset

logger: logging.Logger = logging.getLogger(__name__)


def serving_row_count(serving: ServingDataset) -> int:
    """Count rows of one published dataset at its exact served version.

    Args:
        serving: Resolved active publication.

    Returns:
        The exact row count at the published Lance version.
    """
    return int(lance.dataset(serving.lance_uri, version=serving.lance_version).count_rows())


def serving_index_names(serving: ServingDataset) -> list[str]:
    """List the built index names of one published dataset.

    Args:
        serving: Resolved active publication.

    Returns:
        Sorted index names present at the published Lance version.
    """
    dataset: Any = lance.dataset(serving.lance_uri, version=serving.lance_version)
    return sorted(description.name for description in dataset.describe_indices())


def collect_batch_servings(config: BenchConfig, repository: ControlPlaneRepository) -> list[dict[str, Any]]:
    """Read the active publication of every organization after one batch.

    Args:
        config: Benchmark configuration.
        repository: Migrated PostgreSQL repository.

    Returns:
        One serving record per organization with its published URI, version, and row count.
    """
    servings: list[dict[str, Any]] = []
    for org in config.org_ids():
        serving: ServingDataset | None = resolve_org_serving(repository, org)
        if serving is None:
            servings.append({"org": org, "published": False})
            continue
        servings.append(
            {
                "org": org,
                "published": True,
                "lance_uri": serving.lance_uri,
                "lance_version": serving.lance_version,
                "row_count": serving_row_count(serving),
            }
        )
    return servings


def verify_publications(config: BenchConfig, repository: ControlPlaneRepository) -> list[dict[str, Any]]:
    """Verify every organization's terminal publication cardinality and full index coverage.

    Each organization's published dataset must hold the exact expected row count and carry every
    index name the installed bench specification declares, not merely the vector index. The expected
    index names are derived from the frozen bench spec revision through
    :func:`~bench.reconcile.bench_spec_index_names`, so re-identifying or extending the spec keeps
    the check honest without a hardcoded list.

    Args:
        config: Benchmark configuration.
        repository: Migrated PostgreSQL repository.

    Returns:
        One verification outcome per organization with ``ok``, expected and actual row counts, the
        expected and built index names, any missing indexes, and the published URI and version.
    """
    expected: dict[str, int] = expected_org_rows(config)
    expected_indexes: frozenset[str] = bench_spec_index_names(config)
    checks: list[dict[str, Any]] = []
    for org in config.org_ids():
        want: int = expected[org]
        serving: ServingDataset | None = resolve_org_serving(repository, org)
        if serving is None:
            checks.append({"org": org, "ok": want == 0, "published": False, "expected_rows": want})
            continue
        actual: int = serving_row_count(serving)
        indexes: list[str] = serving_index_names(serving)
        missing: list[str] = sorted(expected_indexes - set(indexes))
        ok: bool = actual == want and not missing
        checks.append(
            {
                "org": org,
                "ok": ok,
                "published": True,
                "expected_rows": want,
                "row_count": actual,
                "expected_indexes": sorted(expected_indexes),
                "indexes": indexes,
                "missing_indexes": missing,
                "lance_uri": serving.lance_uri,
                "lance_version": serving.lance_version,
            }
        )
        if not ok:
            logger.warning(
                "publication verification failed for org %s: expected %d rows, got %d, missing indexes %s",
                org,
                want,
                actual,
                missing,
            )
    return checks


def org_catalog_recall(
    stub: Any,
    pb2: Any,
    config: BenchConfig,
    org: str,
    queries: np.ndarray,
    org_gt: np.ndarray,
    expected_version: int,
) -> dict[str, Any] | None:
    """Measure vector recall for one org through its current catalog publication.

    Args:
        stub: The connected ``SearchServiceStub``.
        pb2: The generated protobuf module.
        config: Benchmark configuration.
        org: The org to sweep.
        queries: The query matrix.
        org_gt: The org's ground-truth global ids.
        expected_version: Operator-approved exact Lance publication.

    Returns:
        The org's recall point with per-cutoff recall and mean latency, or ``None`` when every
        query errored. Queries that error are excluded from scoring by index, so the surviving
        results are always compared against their own ground-truth rows, and the point carries
        ``failed_queries`` so a lossy sweep is visible instead of masquerading as a recall drop.
    """
    retrieved: list[np.ndarray] = []
    latencies: list[float] = []
    kept_indices: list[int] = []
    for index, query in enumerate(queries):
        try:
            response, elapsed_ms = vector_search(stub, pb2, org, query, config.search_k, expected_version)
        except Exception as exc:
            logger.warning("gRPC error during tag recall sweep for org %s: %s", org, exc)
            continue
        latencies.append(elapsed_ms)
        retrieved.append(result_record_ids(response.results))
        kept_indices.append(index)
    if not retrieved:
        return None
    expected: np.ndarray = org_gt[np.asarray(kept_indices, dtype=np.int64)]
    point: dict[str, Any] = {
        "org": org,
        "execution_policy": "catalog_profile",
        "queries": len(retrieved),
        "failed_queries": len(queries) - len(retrieved),
    }
    for cutoff in RECALL_CUTOFFS:
        point[f"recall_at_{cutoff}"] = round(float(recall_at(expected, retrieved, cutoff)), 4)
    if latencies:
        point["mean_ms"] = round(float(np.mean(latencies)), 3)
    return point


def run_catalog_grpc_legs(
    config: BenchConfig,
    expected_versions: dict[str, int],
    queries: np.ndarray,
    ground_truth: dict[str, np.ndarray],
    stub: Any,
    pb2: Any,
) -> dict[str, Any]:
    """Run first-query latency and recall through the final catalog publication.

    Args:
        config: Benchmark configuration.
        expected_versions: Operator-approved exact publication per organization, resolved directly
            from the control-plane repository rather than an external evidence file.
        queries: The query matrix (capped by ``--max-queries`` if set).
        ground_truth: Per-org ground-truth global ids.
        stub: The connected ``SearchServiceStub`` against the self-hosted server.
        pb2: The generated protobuf module.

    Returns:
        Catalog recall and cold plus warm first-query latency.
    """
    first_latencies: dict[str, Any] = {}
    for org in config.org_ids():
        try:
            resp, cold_ms = vector_search(stub, pb2, org, queries[0], 10, expected_versions[org])
            del resp
            _, warm_ms = vector_search(stub, pb2, org, queries[0], 10, expected_versions[org])
            first_latencies[org] = {"cold_ms": round(cold_ms, 3), "warm_ms": round(warm_ms, 3)}
        except Exception as exc:
            first_latencies[org] = {"error": str(exc)[:500]}

    sweep_recalls: list[dict[str, Any]] = []
    recall_failures: list[str] = []
    for org in config.org_ids():
        if org not in ground_truth:
            continue
        point: dict[str, Any] | None = org_catalog_recall(
            stub, pb2, config, org, queries, ground_truth[org], expected_versions[org]
        )
        if point is not None:
            sweep_recalls.append(point)
            if point["failed_queries"]:
                recall_failures.append(org)
        else:
            recall_failures.append(org)

    failed_targets: list[str] = [org for org, timing in first_latencies.items() if "error" in timing]
    failed_targets = sorted(set(failed_targets + recall_failures))
    return {
        "status": "FAILED" if failed_targets else "MEASURED",
        "failed_targets": failed_targets,
        "expected_versions": expected_versions,
        "first_latencies": first_latencies,
        "recall": sweep_recalls,
    }


def catalog_search_leg(
    config: BenchConfig,
    repository: ControlPlaneRepository,
    database_url: str,
    grpc_gen_dir: Path,
) -> dict[str, Any]:
    """Self-host search-api inside the isolation window and measure recall, FTS, and hybrid.

    An externally started server can never see ``e2e``'s ephemeral control-plane schema, so this
    is the only way ``e2e`` can measure a real search leg: the release binary named by
    ``config.search_api_binary`` is spawned as a subprocess from inside the still-open PostgreSQL
    isolation window, pointed at the exact isolated ``database_url``, and torn down before this
    function returns so the caller's ``with isolated_control_plane(...)`` block can safely drop the
    schema afterwards. Must be called from inside that block.

    Args:
        config: Benchmark configuration.
        repository: Migrated PostgreSQL repository, still inside its isolation window.
        database_url: The isolated control-plane URL yielded by ``isolated_control_plane``.
        grpc_gen_dir: Directory holding the compiled gRPC stubs.

    Returns:
        A measured catalog result (recall, first-query latency, FTS, hybrid), a ``NOT_RUN`` record
        when no search-api binary is configured, or a ``FAILED`` record on any startup or RPC
        error. Never fabricates coverage: any failure is reported, not swallowed. The prepared
        query, ground-truth, cluster, and vocabulary artifacts are loaded exactly once through
        :func:`~bench.search.load_artifacts` and shared by the recall, FTS, and hybrid legs.
    """
    binary: Path | None = config.search_api_binary
    if binary is None or not binary.exists():
        return {
            "status": "NOT_RUN",
            "reason": (
                f"no search-api binary at {binary}; build rust/search-api with "
                "`cargo build --release`, or pass --search-api-binary, or an empty string to "
                "disable the leg explicitly"
            ),
        }
    expected_versions: dict[str, int] = expected_versions_from_repository(config, repository)
    if not expected_versions:
        return {"status": "FAILED", "reason": "no organization has an active publication to search"}
    try:
        artifacts: dict[str, Any] = load_artifacts(config)
    except (OSError, ValueError) as exc:
        return {"status": "FAILED", "reason": f"cannot load prepared query artifacts: {exc}"}
    queries: np.ndarray = artifacts["queries"]
    if config.max_queries is not None:
        queries = queries[: config.max_queries]
    ground_truth: dict[str, np.ndarray] = artifacts["ground_truth"]

    log_path: Path = config.telemetry_dir() / "search-api.log"
    try:
        pb2, pb2_grpc = generate_and_load_stubs(grpc_gen_dir)
        with self_hosted_search_api(
            binary=binary,
            database_url=database_url,
            base_uri=config.lance_root(),
            port=config.search_api_port,
            log_path=log_path,
        ) as endpoint:
            hosted_config: BenchConfig = replace(config, endpoint=endpoint)
            channel, stub = open_stub(hosted_config, pb2_grpc, timeout_seconds=5.0)
            try:
                result: dict[str, Any] = run_catalog_grpc_legs(
                    hosted_config, expected_versions, queries, ground_truth, stub, pb2
                )
                if config.no_text:
                    result["fts"] = {"skipped": "no_text mode; FTS leg disabled"}
                    result["hybrid"] = {"skipped": "no_text mode; hybrid leg disabled"}
                else:
                    result["fts"] = run_fts_leg(stub, pb2, hosted_config, artifacts, expected_versions)
                    result["hybrid"] = run_hybrid_leg(stub, pb2, hosted_config, artifacts, expected_versions)
                return result
            finally:
                channel.close()
    except Exception as exc:
        return {"status": "FAILED", "reason": str(exc)}


def run_e2e(config: BenchConfig) -> dict[str, Any]:
    """Run the reconciler-driven end-to-end benchmark, optionally capturing telemetry.

    When ``config.capture_telemetry`` is True, a :class:`~bench.telemetry_capture.TelemetryCapture`
    session is started before the first batch and torn down after verification. The session binds a
    DogStatsD UDP listener on ``127.0.0.1:{config.statsd_port}`` and an OTLP gRPC receiver on
    ``127.0.0.1:{config.otlp_port}``, writes all received telemetry to ``{config.workspace}/telemetry/``,
    and injects the required environment variables so all Spark executors and the reconciler emit to
    the listeners. Failure to bind either listener logs a warning and is non-fatal: the emitters on
    both the Python and Rust sides are fire-and-forget, so a missing listener never fails a run.

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


def run_reconciled_batches(config: BenchConfig, repository: ControlPlaneRepository) -> list[dict[str, Any]]:
    """Append and reconcile every corpus batch through the production control plane.

    Builds a fresh partitioned source table, appends the first ordinal window as the canonical
    baseline, wires the reconciler around the migrated repository, and then drains each batch's
    snapshot to publication. Row generation runs inside Spark executors and the shared session is
    stopped when the batches finish.

    Args:
        config: Benchmark configuration.
        repository: Migrated PostgreSQL repository.

    Returns:
        One record per batch with its ordinal window, reconciliation counts, and per-org servings.
    """
    adapter = benchmark_adapter(config)
    windows: list[tuple[int, int]] = batch_windows_by_ordinal(config)
    spark = bench_workspace_spark(config)
    batch_records: list[dict[str, Any]] = []
    try:
        table: str = source_table_identifier(config)
        create_production_source_table(spark, table)
        baseline_snapshot_id: int = append_production_batch(spark, config, table, adapter, windows[0])
        application = build_reconciler_application(
            spark,
            repository,
            table,
            baseline_snapshot_id,
            config,
        )
        for batch_index, window in enumerate(windows):
            started: float = time.perf_counter()
            if batch_index > 0:
                append_production_batch(spark, config, table, adapter, window)
            logger.info("e2e batch %d/%d ordinals [%d, %d)", batch_index + 1, len(windows), window[0], window[1])
            reconcile_totals: dict[str, int] = drain_reconciler(application)
            elapsed: float = time.perf_counter() - started
            batch_records.append(
                {
                    "batch": batch_index,
                    "first": window[0],
                    "last": window[1],
                    "seconds": round(elapsed, 3),
                    "reconcile": reconcile_totals,
                    "servings": collect_batch_servings(config, repository),
                }
            )
    finally:
        spark.stop()
    return batch_records


def run_e2e_body(config: BenchConfig) -> dict[str, Any]:
    """Execute the reconciler-driven benchmark body without managing capture lifecycle.

    The catalog search leg runs *inside* the PostgreSQL isolation window, immediately after
    publication verification and before the ephemeral schema is dropped: a self-hosted search-api
    subprocess is spawned against the exact isolated schema, queried, and torn down before the
    ``with`` block exits. Passing ``--keep-control-plane`` skips dropping that schema afterwards so
    a later standalone ``search --control-plane-url`` run can self-host against the same published
    catalog; its URL is then written to ``control_plane.json`` in the run directory.

    Args:
        config: Benchmark configuration.

    Returns:
        The phase result document saved as ``e2e.json`` in the run directory.

    Raises:
        RuntimeError: If publication verification fails, or if a configured search-api binary
            could not be self-hosted or measured (an absent binary is not an error: it records
            ``NOT_RUN``).
    """
    ensure_dir(config.run_dir())
    grpc_gen_dir: Path = config.workspace / "grpc_gen"
    base_database_url: str = resolve_database_url()
    with isolated_control_plane(base_database_url, keep=config.keep_control_plane) as (
        repository,
        engine,
        isolated_url,
    ):
        del engine
        batch_records: list[dict[str, Any]] = run_reconciled_batches(config, repository)
        publications: list[dict[str, Any]] = verify_publications(config, repository)
        all_ok: bool = all(check["ok"] for check in publications)
        if not all_ok:
            logger.warning("publication verification: some organizations did not converge (see e2e.json)")
        catalog_grpc: dict[str, Any] = catalog_search_leg(config, repository, isolated_url, grpc_gen_dir)

    if config.keep_control_plane:
        write_json(
            config.run_dir() / "control_plane.json",
            {"database_url": isolated_url, "base_uri": str(config.lance_root())},
        )
        logger.info("control plane kept alive; see %s for --control-plane-url", config.run_dir() / "control_plane.json")

    final_recall: dict[str, Any] = catalog_grpc if catalog_grpc.get("status") == "MEASURED" else {}

    result: dict[str, Any] = save_phase(
        config,
        "e2e",
        {
            "batches": batch_records,
            "publications": publications,
            "publication_verification": {"ok": all_ok, "checks": publications},
            "final_catalog_grpc": catalog_grpc,
            "final_catalog_recall": final_recall,
            "control_plane_kept": config.keep_control_plane,
        },
    )
    if not all_ok:
        raise RuntimeError("benchmark publication verification failed")
    if catalog_grpc.get("status") == "FAILED":
        raise RuntimeError(f"self-hosted search leg failed: {catalog_grpc.get('reason', catalog_grpc)}")
    return result
