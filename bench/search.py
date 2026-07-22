"""Search benchmarks against the Rust gRPC search service.

Four legs run against catalog-resolved targets through the real server:

- recall: the SIFT query vectors measure the catalog-selected publication. Recall@1/@10/@100 is computed against the
  prepared ground truth and latency statistics are recorded.
- fts: deterministic cluster-vocabulary text queries measure BM25 latency and the
  cluster-consistency hit rate (the fraction of hits whose vector belongs to the queried cluster).
- hybrid: vector + text legs fused with reciprocal-rank fusion. Latency and fused recall@10 are recorded.
- load: a fixed in-process profile measures sustained QPS and p50/p95/p99 latency at bounded concurrency levels.
  It applies the same exact-version checks as the recall legs against a plaintext connection.
Every request addresses its dataset through a ``DatasetTarget`` (org, fixed tenant, fixed namespace) matching the
serving catalog identity. Cold-vs-warm first-query latency is recorded per org. Cache warming and index geometry are
operator-only concerns and have no public RPC.

This module needs a running server. There are two ways to get one, both handled by :func:`run_search`:

- ``--endpoint host:port`` dials an already-running server the operator started separately (for example, one
  manually pointed at a control plane kept alive with ``e2e --keep-control-plane``).
- ``--control-plane-url`` self-hosts a fresh ``search-api`` subprocess (see ``bench/search_server.py``) against an
  isolated control-plane URL, typically the one an ``e2e --keep-control-plane`` run wrote to
  ``control_plane.json``. The subprocess is torn down when the command finishes; the control-plane schema itself is
  not touched here (``search`` never creates or drops schemas).

Neither is required for ``e2e``, which self-hosts its own search leg entirely inside its isolation window
(see ``bench/e2e.py:catalog_search_leg``) and never reads ``--endpoint`` or ``--control-plane-url``.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from typing import Any

import grpc
import numpy as np
from sqlalchemy.engine import Engine

from bench.config import RECALL_CUTOFFS, BenchConfig
from bench.groundtruth import recall_at
from bench.grpc_client import (
    dataset_target,
    generate_and_load_stubs,
    load_expected_versions,
    open_ready_stub,
    open_stub,
    result_record_ids,
    text_query,
    timed_call,
    validate_served_version,
    vector_query,
)
from bench.reconcile import expected_versions_from_repository
from bench.results import read_json, save_phase
from bench.search_server import self_hosted_search_api
from lance_etl.state import ControlPlaneRepository, build_control_plane_engine

logger: logging.Logger = logging.getLogger(__name__)

LOAD_CONCURRENCY_LEVELS: tuple[int, ...] = (1, 8, 32)
LOAD_DURATION_SECONDS: float = 15.0


def latency_stats(latencies_ms: list[float]) -> dict[str, float]:
    """Summarize a latency sample.

    Args:
        latencies_ms: Latencies in milliseconds.

    Returns:
        Mean and p50/p95/p99 in milliseconds.
    """
    sample: np.ndarray = np.asarray(latencies_ms, dtype=np.float64)
    return {
        "mean_ms": round(float(sample.mean()), 3),
        "p50_ms": round(float(np.percentile(sample, 50)), 3),
        "p95_ms": round(float(np.percentile(sample, 95)), 3),
        "p99_ms": round(float(np.percentile(sample, 99)), 3),
    }


def load_artifacts(config: BenchConfig) -> dict[str, Any]:
    """Load the prepared artifacts the search legs need.

    When ``config.no_text`` is True the vocab file is absent and cluster/vocab fields are omitted
    because FTS and hybrid legs are skipped.

    Args:
        config: Benchmark configuration.

    Returns:
        Queries, per-org ground truth, cluster assignments, and vocabularies (empty when no_text).

    Raises:
        FileNotFoundError: If the prepare phase has not produced artifacts for this corpus shape.
    """
    prepared: Path = config.prepared_dir()
    if not (prepared / "manifest.json").exists():
        raise FileNotFoundError(f"no prepared artifacts at {prepared}; run 'python -m bench prepare' first")
    ground_truth_file: Any = np.load(prepared / "ground_truth.npz")
    result: dict[str, Any] = {
        "queries": np.load(prepared / "queries.npy"),
        "ground_truth": {org: ground_truth_file[org] for org in ground_truth_file.files},
        "clusters": np.load(prepared / "clusters.npy"),
    }
    if not config.no_text:
        vocab: dict[str, Any] = read_json(prepared / "vocab.json")
        result["cluster_vocab"] = vocab["clusters"]
        result["common_vocab"] = vocab["common"]
    else:
        result["cluster_vocab"] = []
        result["common_vocab"] = []
    return result


def measure_first_queries(
    stub: Any,
    pb2: Any,
    config: BenchConfig,
    queries: np.ndarray,
    expected_versions: dict[str, int],
) -> dict[str, Any]:
    """Record cold and warm first-query latency per org.

    Args:
        stub: The connected service stub.
        pb2: The generated proto module.
        config: Benchmark configuration.
        queries: The query matrix.
        expected_versions: Operator-approved exact publication per organization.

    Returns:
        Per-org cold and warm latencies in milliseconds.
    """
    timings: dict[str, Any] = {}
    org: Any
    for org in config.org_ids():
        request: Any = pb2.VectorSearchRequest(
            target=dataset_target(pb2, org), query=vector_query(pb2, queries[0]), k=10
        )
        cold_response: Any
        cold_ms: Any
        cold_response, cold_ms = timed_call(stub.VectorSearch, request)
        warm_response: Any
        warm_ms: Any
        warm_response, warm_ms = timed_call(stub.VectorSearch, request)
        served_version: int = validate_served_version(cold_response, org, expected_versions[org])
        validate_served_version(warm_response, org, expected_versions[org])
        timings[org] = {
            "cold_ms": round(cold_ms, 3),
            "warm_ms": round(warm_ms, 3),
            "served_version": served_version,
        }
    return timings


def sweep_point(
    stub: Any,
    pb2: Any,
    config: BenchConfig,
    queries: np.ndarray,
    ground_truth: dict[str, np.ndarray],
    expected_versions: dict[str, int],
) -> dict[str, Any]:
    """Measure the catalog-selected publication over every org.

    Args:
        stub: The connected service stub.
        pb2: The generated proto module.
        config: Benchmark configuration.
        queries: The query matrix, already capped by ``--max-queries``.
        ground_truth: Per-org ground-truth global ids.
        expected_versions: Operator-approved exact publication per organization.

    Returns:
        Recall, latency statistics, and single-stream QPS for the point.
    """
    latencies: list[float] = []
    recalls: dict[int, list[float]] = {cutoff: [] for cutoff in RECALL_CUTOFFS}
    org: Any
    for org in config.org_ids():
        retrieved: list[np.ndarray] = []
        query: Any
        for query in queries:
            request: Any = pb2.VectorSearchRequest(
                target=dataset_target(pb2, org), query=vector_query(pb2, query), k=config.search_k
            )
            response: Any
            elapsed_ms: Any
            response, elapsed_ms = timed_call(stub.VectorSearch, request)
            validate_served_version(response, org, expected_versions[org])
            latencies.append(elapsed_ms)
            retrieved.append(result_record_ids(response.results))
        expected: np.ndarray = ground_truth[org][: len(queries)]
        cutoff: Any
        for cutoff in RECALL_CUTOFFS:
            recalls[cutoff].append(recall_at(expected, retrieved, cutoff))
    stats: dict[str, Any] = latency_stats(latencies)
    point: dict[str, Any] = {
        "execution_policy": "catalog_profile",
        "queries": len(queries) * len(config.org_ids()),
        "served_versions": expected_versions,
        "qps_single_stream": round(1000.0 / stats["mean_ms"], 1) if stats["mean_ms"] else 0.0,
        **stats,
    }
    for cutoff in RECALL_CUTOFFS:
        point[f"recall_at_{cutoff}"] = round(float(np.mean(recalls[cutoff])), 4)
    return point


def fts_terms(config: BenchConfig, cluster_vocab: list[list[str]], query_index: int, cluster: int) -> str:
    """Build one deterministic FTS query from a cluster vocabulary.

    Args:
        config: Benchmark configuration.
        cluster_vocab: Per-cluster vocabularies.
        query_index: The query sequence number.
        cluster: The cluster whose vocabulary is sampled.

    Returns:
        The space-joined query terms.
    """
    rng: np.random.Generator = np.random.default_rng([config.seed, 4, query_index])
    terms: np.ndarray = rng.choice(cluster_vocab[cluster], size=min(3, len(cluster_vocab[cluster])), replace=False)
    return " ".join(str(term) for term in terms)


def run_fts_leg(
    stub: Any,
    pb2: Any,
    config: BenchConfig,
    artifacts: dict[str, Any],
    expected_versions: dict[str, int],
) -> dict[str, Any]:
    """Measure full-text latency and the cluster-consistency hit rate.

    Args:
        stub: The connected service stub.
        pb2: The generated proto module.
        config: Benchmark configuration.
        artifacts: The prepared artifacts.
        expected_versions: Operator-approved exact publication per organization.

    Returns:
        Latency statistics and the mean hit rate.
    """
    clusters: np.ndarray = artifacts["clusters"]
    cluster_vocab: list[list[str]] = artifacts["cluster_vocab"]
    orgs: list[str] = config.org_ids()
    latencies: list[float] = []
    hit_rates: list[float] = []
    query_index: Any
    for query_index in range(config.fts_query_count):
        cluster: int = query_index % len(cluster_vocab)
        org: str = orgs[query_index % len(orgs)]
        request: Any = pb2.TextSearchRequest(
            target=dataset_target(pb2, org),
            query=text_query(pb2, fts_terms(config, cluster_vocab, query_index, cluster)),
            k=10,
        )
        response: Any
        elapsed_ms: Any
        response, elapsed_ms = timed_call(stub.TextSearch, request)
        validate_served_version(response, org, expected_versions[org])
        latencies.append(elapsed_ms)
        hit_ids: np.ndarray = result_record_ids(response.results)
        if len(hit_ids):
            hit_rates.append(float(np.mean(clusters[hit_ids] == cluster)))
        else:
            hit_rates.append(0.0)
    return {
        "queries": config.fts_query_count,
        "served_versions": expected_versions,
        "hit_rate": round(float(np.mean(hit_rates)), 4) if hit_rates else 0.0,
        **latency_stats(latencies),
    }


def run_hybrid_leg(
    stub: Any,
    pb2: Any,
    config: BenchConfig,
    artifacts: dict[str, Any],
    expected_versions: dict[str, int],
) -> dict[str, Any]:
    """Measure hybrid (vector + text, RRF) latency and fused recall@10.

    Each query pairs a SIFT query vector with text terms from the cluster of its true nearest neighbor, so the two
    legs are realistically correlated.

    Args:
        stub: The connected service stub.
        pb2: The generated proto module.
        config: Benchmark configuration.
        artifacts: The prepared artifacts.
        expected_versions: Operator-approved exact publication per organization.

    Returns:
        Latency statistics and fused recall@10.
    """
    queries: np.ndarray = artifacts["queries"]
    ground_truth: dict[str, np.ndarray] = artifacts["ground_truth"]
    clusters: np.ndarray = artifacts["clusters"]
    cluster_vocab: list[list[str]] = artifacts["cluster_vocab"]
    orgs: list[str] = config.org_ids()
    latencies: list[float] = []
    recalls: list[float] = []
    count: int = min(config.hybrid_query_count, len(queries))
    query_index: Any
    for query_index in range(count):
        org: str = orgs[query_index % len(orgs)]
        expected: np.ndarray = ground_truth[org][query_index]
        cluster: int = int(clusters[int(expected[0])])
        request: Any = pb2.HybridSearchRequest(
            target=dataset_target(pb2, org),
            vector=vector_query(pb2, queries[query_index]),
            text=text_query(pb2, fts_terms(config, cluster_vocab, query_index, cluster)),
            k=10,
        )
        response: Any
        elapsed_ms: Any
        response, elapsed_ms = timed_call(stub.HybridSearch, request)
        validate_served_version(response, org, expected_versions[org])
        latencies.append(elapsed_ms)
        recalls.append(recall_at(expected[None, :], [result_record_ids(response.results)], 10))
    return {
        "queries": count,
        "served_versions": expected_versions,
        "recall_at_10": round(float(np.mean(recalls)), 4) if recalls else 0.0,
        **latency_stats(latencies),
    }


def run_load_worker(
    stub: Any,
    pb2: Any,
    sample_vector: np.ndarray,
    org_id: str,
    expected_version: int,
    stop_at: float,
) -> list[float]:
    """Issue requests until one fixed load interval ends.

    Args:
        stub: Generated search stub shared by the load workers.
        pb2: Generated protobuf module.
        sample_vector: Query vector replayed by this worker.
        org_id: Exact logical target assigned to this worker.
        expected_version: Operator-approved exact publication.
        stop_at: Monotonic end time for the load interval.

    Returns:
        Successful per-request latencies in milliseconds.

    Raises:
        Exception: Propagates deadline, transport, and version failures. Caught one level up by
            :func:`run_load_level`, which records the failure instead of crashing the whole leg.
    """
    request: Any = pb2.VectorSearchRequest(
        target=dataset_target(pb2, org_id),
        query=vector_query(pb2, sample_vector),
        k=10,
    )
    latencies: list[float] = []
    while not latencies or time.perf_counter() < stop_at:
        response: Any
        elapsed_ms: Any
        response, elapsed_ms = timed_call(stub.VectorSearch, request)
        validate_served_version(response, org_id, expected_version)
        latencies.append(elapsed_ms)
    return latencies


def run_load_level(
    stub: Any,
    pb2: Any,
    config: BenchConfig,
    sample_vector: np.ndarray,
    expected_versions: dict[str, int],
    concurrency: int,
) -> dict[str, Any]:
    """Measure one fixed concurrency level.

    Args:
        stub: Generated search stub shared by the load workers.
        pb2: Generated protobuf module.
        config: Benchmark configuration.
        sample_vector: Query vector replayed by every worker.
        expected_versions: Operator-approved exact publication per organization.
        concurrency: Fixed worker count for this profile level.

    Returns:
        Request count, throughput, and latency distribution when the level completes. A
        ``status: "FAILED"`` record with a bounded ``reason`` when every worker shares one
        organization and the concurrency level exceeds the server's fixed per-tenant admission cap
        (``DEFAULT_PER_TENANT_SEARCH_CONCURRENCY`` in ``rust/search-api/src/config.rs``): this is a
        real, code-owned capacity ceiling, not a transport bug, so one level's rejection is recorded
        rather than crashing the whole load leg (and losing the recall/FTS/hybrid legs already
        measured in the same command). Only a gRPC ``RESOURCE_EXHAUSTED`` status is treated as this
        admission rejection (the server's ``Status::resource_exhausted`` in
        ``rust/search-api/src/grpc/admission.rs``); every other gRPC status (for example
        ``UNAVAILABLE`` or ``INTERNAL`` from a mid-load server crash) is a real failure and is
        re-raised instead of being recorded as a benign capacity rejection. A served-version
        fencing failure (:func:`validate_served_version` raising ``RuntimeError``) is a correctness
        violation, not a capacity limit, and is never caught here: it always propagates and fails
        the benchmark loudly.

    Raises:
        grpc.RpcError: Any gRPC failure other than ``RESOURCE_EXHAUSTED``.
    """
    started: float = time.perf_counter()
    stop_at: float = started + LOAD_DURATION_SECONDS
    orgs: list[str] = config.org_ids()
    try:
        with ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="bench-load") as executor:
            futures: list[Future[list[float]]] = [
                executor.submit(
                    run_load_worker,
                    stub,
                    pb2,
                    sample_vector,
                    orgs[worker % len(orgs)],
                    expected_versions[orgs[worker % len(orgs)]],
                    stop_at,
                )
                for worker in range(concurrency)
            ]
            latencies: list[float] = [latency for future in futures for latency in future.result()]
    except grpc.RpcError as exc:
        if exc.code() != grpc.StatusCode.RESOURCE_EXHAUSTED:
            raise
        return {"concurrency": concurrency, "status": "FAILED", "reason": str(exc)[:500]}
    elapsed_seconds: float = time.perf_counter() - started
    return {
        "concurrency": concurrency,
        "status": "MEASURED",
        "requests": len(latencies),
        "duration_seconds": round(elapsed_seconds, 3),
        "qps": round(len(latencies) / elapsed_seconds, 1),
        **latency_stats(latencies),
    }


def run_load_leg(
    stub: Any,
    pb2: Any,
    config: BenchConfig,
    sample_vector: np.ndarray,
    expected_versions: dict[str, int],
) -> dict[str, Any]:
    """Run the fixed fleet load profile.

    Args:
        stub: Generated search stub shared by load workers.
        pb2: Generated protobuf module.
        config: Benchmark configuration.
        sample_vector: Query vector replayed by every worker.
        expected_versions: Operator-approved exact publication per organization.

    Returns:
        Every requested concurrency level's outcome (``MEASURED`` or ``FAILED``, see
        :func:`run_load_level`) and exact-version evidence. Overall ``status`` is ``MEASURED`` when
        at least one level measured successfully, ``FAILED`` only when every level was rejected.
    """
    levels: list[dict[str, Any]] = [
        run_load_level(stub, pb2, config, sample_vector, expected_versions, concurrency)
        for concurrency in LOAD_CONCURRENCY_LEVELS
    ]
    overall: str = "MEASURED" if any(level["status"] == "MEASURED" for level in levels) else "FAILED"
    return {"status": overall, "served_versions": expected_versions, "levels": levels}


def run_search(config: BenchConfig) -> dict[str, Any]:
    """Run profile-owned recall, FTS, hybrid, and load legs against a resolved server.

    Resolves a server two ways: ``config.endpoint`` dials one already running (started separately
    by the operator), and ``config.control_plane_url`` self-hosts a fresh ``search-api``
    subprocess for the duration of this call (see module docstring). ``endpoint`` wins when both
    are set. Exactly one must be configured. The self-hosting path compiles and imports the proto
    stubs before starting the subprocess rather than after, so stub generation is not sequenced
    behind the server reaching ``SERVING``.

    Args:
        config: Benchmark configuration.

    Returns:
        The phase result document.

    Raises:
        RuntimeError: If neither ``endpoint`` nor ``control_plane_url`` is configured, or the
            configured ``search_api_binary`` self-hosting path is unavailable.
    """
    if config.endpoint:
        return run_search_against_endpoint(config)
    if config.control_plane_url:
        binary: Path | None = config.search_api_binary
        if binary is None or not binary.exists():
            raise RuntimeError(
                f"--control-plane-url was set but no search-api binary is available at {binary}; "
                "pass --search-api-binary or build rust/search-api with `cargo build --release`"
            )
        engine: Engine = build_control_plane_engine(config.control_plane_url)
        try:
            expected_versions: dict[str, int] = expected_versions_from_repository(
                config, ControlPlaneRepository(engine)
            )
        finally:
            engine.dispose()
        pb2: Any
        pb2_grpc: Any
        pb2, pb2_grpc = generate_and_load_stubs(config.workspace / "grpc_gen")
        with self_hosted_search_api(
            binary=binary,
            database_url=config.control_plane_url,
            base_uri=config.lance_root(),
            port=config.search_api_port,
            log_path=config.telemetry_dir() / "search-api.log",
        ) as endpoint:
            return run_search_against_endpoint(
                replace(config, endpoint=endpoint), expected_versions, pb2=pb2, pb2_grpc=pb2_grpc
            )
    raise RuntimeError(
        "search requires either --endpoint host:port (dial an already-running server) or "
        "--control-plane-url (self-host against a control plane kept alive by `e2e "
        "--keep-control-plane`, see control_plane.json in that run's directory)"
    )


def run_search_against_endpoint(
    config: BenchConfig,
    expected_versions: dict[str, int] | None = None,
    pb2: Any | None = None,
    pb2_grpc: Any | None = None,
) -> dict[str, Any]:
    """Run profile-owned recall, FTS, hybrid, and load legs against ``config.endpoint``.

    Args:
        config: Benchmark configuration with a live ``endpoint`` set.
        expected_versions: Operator-approved exact publication per organization. When ``None``
            (the default, matching a dialed ``--endpoint``), loaded from
            ``config.search_expected_versions_path``. The self-hosting path in :func:`run_search`
            passes this explicitly, resolved directly from its own control-plane connection, so it
            never needs that evidence file.
        pb2: Pre-generated proto module. ``None`` (the default, matching a dialed ``--endpoint``)
            compiles and imports it here. The self-hosting path in :func:`run_search` prepares
            stubs before starting the server so stub generation is not sequenced behind server
            boot, and passes the already-compiled modules through instead of regenerating them.
        pb2_grpc: Pre-generated service stub module paired with ``pb2``. Must be given together
            with ``pb2`` or not at all.

    Returns:
        The phase result document.
    """
    if expected_versions is None:
        expected_versions = load_expected_versions(config)
    artifacts: dict[str, Any] = load_artifacts(config)
    stub: Any
    if pb2 is None or pb2_grpc is None:
        pb2, stub = open_ready_stub(config, config.workspace / "grpc_gen")
    else:
        stub = open_stub(config, pb2_grpc)
    queries: np.ndarray = artifacts["queries"]
    if config.max_queries is not None:
        queries = queries[: config.max_queries]

    first_queries: dict[str, Any] = measure_first_queries(stub, pb2, config, queries, expected_versions)
    warmup_count: int = config.warmup_queries
    if warmup_count > 0:
        logger.info("warmup: %d profile-owned queries with results discarded", warmup_count)
        org: Any
        for org in config.org_ids():
            query: Any
            for query in queries[:warmup_count]:
                request: Any = pb2.VectorSearchRequest(
                    target=dataset_target(pb2, org), query=vector_query(pb2, query), k=config.search_k
                )
                response: Any
                unused_ms: Any
                response, unused_ms = timed_call(stub.VectorSearch, request)
                del unused_ms
                validate_served_version(response, org, expected_versions[org])
    sweep: list[dict[str, Any]] = [
        sweep_point(stub, pb2, config, queries, artifacts["ground_truth"], expected_versions)
    ]
    load: dict[str, Any] = run_load_leg(stub, pb2, config, queries[0], expected_versions)
    result: dict[str, Any] = {
        "endpoint": config.endpoint,
        "status": "MEASURED",
        "expected_versions": expected_versions,
        "first_queries": first_queries,
        "sweep": sweep,
        "load": load,
    }
    if config.no_text:
        result["fts"] = {"skipped": "no_text mode; FTS leg disabled"}
        result["hybrid"] = {"skipped": "no_text mode; hybrid leg disabled"}
    else:
        result["fts"] = run_fts_leg(stub, pb2, config, artifacts, expected_versions)
        result["hybrid"] = run_hybrid_leg(stub, pb2, config, artifacts, expected_versions)
    return save_phase(config, "search", result)
