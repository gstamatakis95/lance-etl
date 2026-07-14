"""One agent-driveable experiment iteration: knobs in, ``metrics.json`` out.

Composes the existing phases into a single command an agent can loop on. Each run: chain
download and prepare when the prepared shape is missing (cached across iterations), wipe the
Lance root so every iteration is a clean build of the configured knobs, run the batch-major
e2e body (real ETL, pipeline compaction, indexing, and hour tags), measure the on-disk
data/index/metadata footprint (:mod:`bench.sizes`), optionally measure an externally managed
authenticated production service, and write one machine-readable ``metrics.json``
plus a one-line summary appended to ``{results_root}/experiments.jsonl``. With ``--baseline
RUN_ID`` the headline delta against a previous iteration is computed and logged.
"""

from __future__ import annotations

import json
import logging
import shutil
from dataclasses import asdict
from pathlib import Path
from typing import Any

from bench.config import BenchConfig
from bench.download import run_download
from bench.e2e import load_queries_and_gt, run_e2e
from bench.grpc_client import generate_stubs, load_stubs, open_stub
from bench.prepare import run_prepare
from bench.results import ensure_dir, read_json, save_phase, utc_now
from bench.search import measure_first_queries, sweep_point
from bench.sizes import measure_dataset_sizes

logger: logging.Logger = logging.getLogger(__name__)

HEADLINE_RECALL_TARGET: float = 0.95

KNOB_FIELDS: tuple[str, ...] = (
    "dataset",
    "limit",
    "tenants",
    "batches",
    "seed",
    "etl_partitions",
    "ivf_partitions",
    "num_shards",
    "vector_row_floor",
    "compact_target_rows",
    "fts_with_position",
    "no_text",
    "search_k",
)


def config_dump(config: BenchConfig) -> dict[str, Any]:
    """Serialize the full configuration with JSON-safe values.

    Args:
        config: Benchmark configuration.

    Returns:
        Every config field with paths rendered as strings.
    """
    dump: dict[str, Any] = asdict(config)
    for key, value in dump.items():
        if isinstance(value, Path):
            dump[key] = str(value)
    return dump


def knob_vector(config: BenchConfig) -> dict[str, Any]:
    """Extract the tradeoff-relevant knobs an agent varies between iterations.

    Args:
        config: Benchmark configuration.

    Returns:
        The knob subset recorded in the experiment history line.
    """
    dump: dict[str, Any] = config_dump(config)
    return {name: dump[name] for name in KNOB_FIELDS}


def ensure_prepared(config: BenchConfig) -> dict[str, Any]:
    """Chain download and prepare when the prepared shape is absent.

    The prepared artifacts are keyed by corpus shape (``prepared_key``), so every iteration
    after the first reuses them at zero cost.

    Args:
        config: Benchmark configuration.

    Returns:
        A record of whether preparation ran or was reused.
    """
    manifest: Path = config.prepared_dir() / "manifest.json"
    if manifest.exists() and not config.force:
        return {"reused": True, "prepared_dir": str(config.prepared_dir())}
    run_download(config)
    run_prepare(config)
    return {"reused": False, "prepared_dir": str(config.prepared_dir())}


def run_sweep_and_first_queries(config: BenchConfig) -> dict[str, Any]:
    """Run first queries plus one release-profile recall point against an external service.

    Args:
        config: Benchmark configuration (endpoint already pointing at the live server).

    Returns:
        First-query timings and one catalog-profile point, an explicit not-run record when
        credentials are absent, or a failed record when verified connectivity fails.
    """
    if not config.search_credentials_configured():
        return {
            "status": "NOT_RUN",
            "reason": "external search requires --endpoint, --search-ca-path, and --search-token-dir",
        }
    grpc_gen_dir: Path = config.workspace / "grpc_gen"
    pb2, pb2_grpc = load_stubs(generate_stubs(grpc_gen_dir))
    try:
        stub = open_stub(config, pb2_grpc)
    except RuntimeError as exc:
        return {"status": "FAILED", "reason": str(exc)}
    queries, ground_truth = load_queries_and_gt(config)
    try:
        first_query: dict[str, Any] = measure_first_queries(stub, pb2, config, queries)
        point: dict[str, Any] = sweep_point(stub, pb2, config, queries, ground_truth)
    except Exception as error:
        return {"status": "FAILED", "reason": str(error)[:500]}
    logger.info("catalog profile recall@10=%.4f p95=%.2fms", point["recall_at_10"], point["p95_ms"])
    return {"status": "MEASURED", "first_query": first_query, "points": [point]}


def headline_numbers(sweep: dict[str, Any], sizes: dict[str, Any], build_seconds: float) -> dict[str, Any]:
    """Distill one iteration into the numbers an agent compares across runs.

    The single catalog-selected operating point records recall and latency. Index execution
    policy is release-owned and therefore cannot be swept by a public request.

    Args:
        sweep: The sweep record from :func:`run_sweep_and_first_queries`.
        sizes: The fleet size record from :func:`bench.sizes.measure_dataset_sizes`.
        build_seconds: Total wall seconds across ETL and pipeline batches.

    Returns:
        The headline record for ``experiments.jsonl`` and baseline deltas.
    """
    headline: dict[str, Any] = {
        "build_seconds": round(build_seconds, 3),
        "total_bytes": sizes["total_bytes"],
        "data_bytes": sizes["data_bytes"],
        "index_bytes": sizes["index_bytes"],
    }
    points: list[dict[str, Any]] = sweep.get("points", [])
    headline["recall_measured"] = bool(points)
    if not points:
        headline["recall_skip_reason"] = str(sweep.get("reason", "sweep produced no recall points"))
    if points:
        best = max(points, key=lambda point: (point["recall_at_10"], -point["p95_ms"]))
        headline["best_recall_at_10"] = best["recall_at_10"]
        headline["best_point"] = {"execution_policy": best["execution_policy"]}
        headline["best_point_p95_ms"] = best["p95_ms"]
        at_target = [point for point in points if point["recall_at_10"] >= HEADLINE_RECALL_TARGET]
        if at_target:
            knee = min(at_target, key=lambda point: point["p95_ms"])
            headline["knee_p95_ms"] = knee["p95_ms"]
            headline["knee_point"] = {"execution_policy": knee["execution_policy"]}
            headline["knee_recall_at_10"] = knee["recall_at_10"]
    cold_values: list[float] = [
        timing["cold_ms"]
        for timing in sweep.get("first_query", {}).values()
        if isinstance(timing, dict) and "cold_ms" in timing
    ]
    if cold_values:
        headline["cold_first_query_ms"] = round(sum(cold_values) / len(cold_values), 3)
    return headline


def baseline_delta(config: BenchConfig, headline: dict[str, Any]) -> dict[str, Any] | None:
    """Compute the headline delta against a previous run's ``metrics.json``.

    Args:
        config: Benchmark configuration carrying ``baseline`` (a previous run id).
        headline: This run's headline record.

    Returns:
        Per-metric ``{baseline, current, delta}`` rows, or ``None`` when no baseline was
        requested or its metrics file is missing.
    """
    if config.baseline is None:
        return None
    baseline_path: Path = config.results_root / config.baseline / "metrics.json"
    if not baseline_path.exists():
        logger.warning("baseline run %s has no metrics.json at %s", config.baseline, baseline_path)
        return None
    baseline_headline: dict[str, Any] = read_json(baseline_path).get("headline", {})
    delta: dict[str, Any] = {"baseline_run_id": config.baseline}
    for metric, current in headline.items():
        previous = baseline_headline.get(metric)
        if isinstance(current, int | float) and isinstance(previous, int | float):
            delta[metric] = {"baseline": previous, "current": current, "delta": round(current - previous, 4)}
    return delta


def append_history(config: BenchConfig, headline: dict[str, Any]) -> Path:
    """Append this iteration's summary line to the experiment history.

    Args:
        config: Benchmark configuration.
        headline: The headline record for this run.

    Returns:
        The history file path (``{results_root}/experiments.jsonl``).
    """
    history_path: Path = config.results_root / "experiments.jsonl"
    line: dict[str, Any] = {
        "run_id": config.run_id,
        "recorded_at": utc_now(),
        "knobs": knob_vector(config),
        "headline": headline,
    }
    ensure_dir(config.results_root)
    with history_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(line) + "\n")
    return history_path


def run_experiment(config: BenchConfig) -> dict[str, Any]:
    """Run one full experiment iteration and write ``metrics.json``.

    Args:
        config: Benchmark configuration.

    Returns:
        The metrics document saved as ``metrics.json`` in the run directory.
    """
    ensure_dir(config.run_dir())
    prepared: dict[str, Any] = ensure_prepared(config)

    lance_root: Path = config.lance_root()
    if lance_root.exists():
        shutil.rmtree(lance_root)

    service_record: dict[str, Any] = {
        "mode": "external_authenticated" if config.search_credentials_configured() else "not_configured",
        "endpoint": config.endpoint or None,
    }
    e2e_doc: dict[str, Any] = run_e2e(config)
    build_seconds: float = sum(batch["etl_seconds"] + batch["pipeline"]["seconds"] for batch in e2e_doc["batches"])
    sizes: dict[str, Any] = measure_dataset_sizes(lance_root)
    sweep: dict[str, Any] = run_sweep_and_first_queries(config)
    service_record["status"] = sweep["status"]

    headline: dict[str, Any] = headline_numbers(sweep, sizes, build_seconds)
    delta: dict[str, Any] | None = baseline_delta(config, headline)
    if delta is not None:
        logger.info("baseline delta vs %s: %s", config.baseline, json.dumps(delta, indent=2))

    metrics: dict[str, Any] = {
        "run_id": config.run_id,
        "knobs": config_dump(config),
        "search_service": service_record,
        "prepared": prepared,
        "build": {
            "total_seconds": round(build_seconds, 3),
            "batches": [
                {
                    "batch": batch["batch"],
                    "tag": batch["tag"],
                    "etl_seconds": batch["etl_seconds"],
                    "pipeline_seconds": batch["pipeline"]["seconds"],
                }
                for batch in e2e_doc["batches"]
            ],
        },
        "sizes": sizes,
        "sweep": sweep,
        "tags": {
            "created": e2e_doc["tags_created"],
            "verified": e2e_doc["historical_tag_verification"]["ok"],
        },
        "headline": headline,
        "baseline_delta": delta,
    }
    doc: dict[str, Any] = save_phase(config, "metrics", metrics)
    history_path: Path = append_history(config, headline)
    logger.info(
        "experiment %s complete: metrics=%s history=%s",
        config.run_id,
        config.run_dir() / "metrics.json",
        history_path,
    )
    return doc
