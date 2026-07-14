"""Search benchmarks against the Rust gRPC search service.

Four legs run against the catalog-resolved targets through the real server:

- recall: the SIFT query vectors measure the release-owned target profile. Recall@1/@10/@100 is computed against the
  prepared ground truth and latency statistics are recorded.
- fts: deterministic cluster-vocabulary text queries measure BM25 latency and the
  cluster-consistency hit rate (the fraction of hits whose vector belongs to the queried cluster).
- hybrid: vector + text legs fused with reciprocal-rank fusion. Latency and fused recall@10 are recorded.
- load: when the external ``ghz`` binary is on PATH, sustained QPS and p50/p95/p99 latency are measured per
  ``--concurrency`` level with the raw ghz JSON written into the run directory. Absent ghz the leg is skipped with a
  clear message.
Every request addresses its dataset through a ``DatasetTarget`` (org, fixed tenant, fixed namespace) matching the
serving catalog identity. Cold-vs-warm first-query latency is recorded per org. Cache warming and index geometry are
operator-only concerns and have no public RPC.
"""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
from pathlib import Path
from typing import Any

import numpy as np

from bench.config import NAMESPACE, PROTO_PATH, RECALL_CUTOFFS, TENANT_ID, BenchConfig
from bench.groundtruth import recall_at
from bench.grpc_client import (
    dataset_target,
    generate_stubs,
    load_stubs,
    open_stub,
    result_vector_ids,
    text_query,
    timed_call,
    vector_query,
)
from bench.results import ensure_dir, read_json, save_phase

logger: logging.Logger = logging.getLogger(__name__)

NANOS_PER_MILLI: float = 1_000_000.0


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
    ground_truth_file = np.load(prepared / "ground_truth.npz")
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


def measure_first_queries(stub: Any, pb2: Any, config: BenchConfig, queries: np.ndarray) -> dict[str, Any]:
    """Record cold and warm first-query latency per org.

    Args:
        stub: The connected service stub.
        pb2: The generated proto module.
        config: Benchmark configuration.
        queries: The query matrix.

    Returns:
        Per-org cold and warm latencies in milliseconds.
    """
    timings: dict[str, Any] = {}
    for org in config.org_ids():
        request = pb2.VectorSearchRequest(target=dataset_target(pb2, org), query=vector_query(pb2, queries[0]), k=10)
        cold_ms = timed_call(stub.VectorSearch, request)[1]
        warm_ms = timed_call(stub.VectorSearch, request)[1]
        timings[org] = {"cold_ms": round(cold_ms, 3), "warm_ms": round(warm_ms, 3)}
    return timings


def sweep_point(
    stub: Any,
    pb2: Any,
    config: BenchConfig,
    queries: np.ndarray,
    ground_truth: dict[str, np.ndarray],
) -> dict[str, Any]:
    """Measure the catalog-selected release profile over every org.

    Args:
        stub: The connected service stub.
        pb2: The generated proto module.
        config: Benchmark configuration.
        queries: The query matrix, already capped by ``--max-queries``.
        ground_truth: Per-org ground-truth global ids.

    Returns:
        Recall, latency statistics, and single-stream QPS for the point.
    """
    latencies: list[float] = []
    recalls: dict[int, list[float]] = {cutoff: [] for cutoff in RECALL_CUTOFFS}
    for org in config.org_ids():
        retrieved: list[np.ndarray] = []
        for query in queries:
            request = pb2.VectorSearchRequest(
                target=dataset_target(pb2, org), query=vector_query(pb2, query), k=config.search_k
            )
            response, elapsed_ms = timed_call(stub.VectorSearch, request)
            latencies.append(elapsed_ms)
            retrieved.append(result_vector_ids(response.results))
        expected: np.ndarray = ground_truth[org][: len(queries)]
        for cutoff in RECALL_CUTOFFS:
            recalls[cutoff].append(recall_at(expected, retrieved, cutoff))
    stats: dict[str, Any] = latency_stats(latencies)
    point: dict[str, Any] = {
        "execution_policy": "catalog_profile",
        "queries": len(queries) * len(config.org_ids()),
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


def run_fts_leg(stub: Any, pb2: Any, config: BenchConfig, artifacts: dict[str, Any]) -> dict[str, Any]:
    """Measure full-text latency and the cluster-consistency hit rate.

    Args:
        stub: The connected service stub.
        pb2: The generated proto module.
        config: Benchmark configuration.
        artifacts: The prepared artifacts.

    Returns:
        Latency statistics and the mean hit rate.
    """
    clusters: np.ndarray = artifacts["clusters"]
    cluster_vocab: list[list[str]] = artifacts["cluster_vocab"]
    orgs: list[str] = config.org_ids()
    latencies: list[float] = []
    hit_rates: list[float] = []
    for query_index in range(config.fts_query_count):
        cluster: int = query_index % len(cluster_vocab)
        org: str = orgs[query_index % len(orgs)]
        request = pb2.TextSearchRequest(
            target=dataset_target(pb2, org),
            query=text_query(pb2, fts_terms(config, cluster_vocab, query_index, cluster)),
            k=10,
        )
        response, elapsed_ms = timed_call(stub.TextSearch, request)
        latencies.append(elapsed_ms)
        hit_ids: np.ndarray = result_vector_ids(response.results)
        if len(hit_ids):
            hit_rates.append(float(np.mean(clusters[hit_ids] == cluster)))
        else:
            hit_rates.append(0.0)
    return {
        "queries": config.fts_query_count,
        "hit_rate": round(float(np.mean(hit_rates)), 4) if hit_rates else 0.0,
        **latency_stats(latencies),
    }


def run_hybrid_leg(stub: Any, pb2: Any, config: BenchConfig, artifacts: dict[str, Any]) -> dict[str, Any]:
    """Measure hybrid (vector + text, RRF) latency and fused recall@10.

    Each query pairs a SIFT query vector with text terms from the cluster of its true nearest neighbor, so the two
    legs are realistically correlated.

    Args:
        stub: The connected service stub.
        pb2: The generated proto module.
        config: Benchmark configuration.
        artifacts: The prepared artifacts.

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
    for query_index in range(count):
        org: str = orgs[query_index % len(orgs)]
        expected: np.ndarray = ground_truth[org][query_index]
        cluster: int = int(clusters[int(expected[0])])
        request = pb2.HybridSearchRequest(
            target=dataset_target(pb2, org),
            vector=vector_query(pb2, queries[query_index]),
            text=text_query(pb2, fts_terms(config, cluster_vocab, query_index, cluster)),
            k=10,
        )
        response, elapsed_ms = timed_call(stub.HybridSearch, request)
        latencies.append(elapsed_ms)
        recalls.append(recall_at(expected[None, :], [result_vector_ids(response.results)], 10))
    return {
        "queries": count,
        "recall_at_10": round(float(np.mean(recalls)), 4) if recalls else 0.0,
        **latency_stats(latencies),
    }


def ghz_payload(sample_vector: np.ndarray, org_id: str = "org0") -> dict[str, Any]:
    """Build the JSON body ghz replays against ``VectorSearch``.

    Args:
        sample_vector: The query vector replayed by every request.
        org_id: The targeted org.

    Returns:
        The protojson-compatible request body with the full ``DatasetTarget``.
    """
    return {
        "target": {"org_id": org_id, "tenant_id": TENANT_ID, "namespace": NAMESPACE},
        "query": {"vector": [float(value) for value in sample_vector]},
        "k": 10,
    }


def ghz_summary(raw: dict[str, Any]) -> dict[str, Any]:
    """Extract the headline numbers from a raw ghz JSON report.

    Args:
        raw: The parsed ghz output.

    Returns:
        QPS and latency percentiles in milliseconds.
    """
    percentiles: dict[int, float] = {}
    for entry in raw.get("latencyDistribution") or []:
        percentiles[int(entry["percentage"])] = float(entry["latency"]) / NANOS_PER_MILLI
    return {
        "qps": round(float(raw.get("rps", 0.0)), 1),
        "mean_ms": round(float(raw.get("average", 0.0)) / NANOS_PER_MILLI, 3),
        "p50_ms": round(percentiles.get(50, 0.0), 3),
        "p95_ms": round(percentiles.get(95, 0.0), 3),
        "p99_ms": round(percentiles.get(99, 0.0), 3),
    }


def run_load_leg(config: BenchConfig, sample_vector: np.ndarray) -> dict[str, Any]:
    """Run the sustained-QPS load mode by shelling out to ghz.

    Args:
        config: Benchmark configuration.
        sample_vector: The query vector replayed by every request.

    Returns:
        Per-concurrency summaries, or a skip notice when ghz is absent.
    """
    ghz_binary: str | None = shutil.which("ghz")
    if ghz_binary is None:
        message: str = "ghz not found on PATH; install it (https://ghz.sh) to run the load mode"
        logger.warning(message)
        return {"skipped": message}
    run_directory: Path = ensure_dir(config.run_dir())
    payload: str = json.dumps(ghz_payload(sample_vector))
    levels: list[dict[str, Any]] = []
    for concurrency in config.concurrency:
        output: Path = run_directory / f"ghz_c{concurrency}.json"
        command: list[str] = [
            ghz_binary,
            "--insecure",
            "--proto",
            str(PROTO_PATH),
            "--import-paths",
            str(PROTO_PATH.parents[2]),
            "--call",
            "lance_etl.v1.SearchService.VectorSearch",
            "-d",
            payload,
            "-c",
            str(concurrency),
            "-z",
            config.load_duration,
            "--format",
            "json",
            "--output",
            str(output),
            config.endpoint,
        ]
        logger.info("ghz load at concurrency %d for %s", concurrency, config.load_duration)
        completed = subprocess.run(command, capture_output=True, text=True)
        if completed.returncode != 0:
            levels.append({"concurrency": concurrency, "error": completed.stderr.strip()[:500]})
            continue
        raw: dict[str, Any] = read_json(output)
        levels.append({"concurrency": concurrency, "raw_file": output.name, **ghz_summary(raw)})
    return {"duration": config.load_duration, "execution_policy": "catalog_profile", "levels": levels}


def run_search(config: BenchConfig) -> dict[str, Any]:
    """Run profile-owned recall, FTS, hybrid, and load legs.

    Args:
        config: Benchmark configuration.

    Returns:
        The phase result document.
    """
    artifacts: dict[str, Any] = load_artifacts(config)
    pb2, pb2_grpc = load_stubs(generate_stubs(config.workspace / "grpc_gen"))
    stub: Any = open_stub(config.endpoint, pb2_grpc, str(config.lance_root()))
    queries: np.ndarray = artifacts["queries"]
    if config.max_queries is not None:
        queries = queries[: config.max_queries]

    first_queries: dict[str, Any] = measure_first_queries(stub, pb2, config, queries)
    warmup_count: int = config.warmup_queries
    if warmup_count > 0:
        logger.info("warmup: %d profile-owned queries with results discarded", warmup_count)
        for org in config.org_ids():
            for query in queries[:warmup_count]:
                request = pb2.VectorSearchRequest(
                    target=dataset_target(pb2, org), query=vector_query(pb2, query), k=config.search_k
                )
                timed_call(stub.VectorSearch, request)
    sweep: list[dict[str, Any]] = [sweep_point(stub, pb2, config, queries, artifacts["ground_truth"])]
    load: dict[str, Any] = run_load_leg(config, queries[0])
    result: dict[str, Any] = {
        "endpoint": config.endpoint,
        "first_queries": first_queries,
        "sweep": sweep,
        "load": load,
    }
    if config.no_text:
        result["fts"] = {"skipped": "no_text mode; FTS leg disabled"}
        result["hybrid"] = {"skipped": "no_text mode; hybrid leg disabled"}
    else:
        result["fts"] = run_fts_leg(stub, pb2, config, artifacts)
        result["hybrid"] = run_hybrid_leg(stub, pb2, config, artifacts)
    return save_phase(config, "search", result)
