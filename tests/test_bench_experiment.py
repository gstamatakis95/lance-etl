"""Unit tests for the bench experiment loop pieces that need no Spark and no server.

Covers on-disk size measurement, unrestricted ``--endpoint`` parsing, headline distillation,
experiment history, and baseline delta computation. The full offline experiment integration lives
in ``tests/test_bench_e2e.py`` with the other heavy bench tests.
"""

from __future__ import annotations

import json
from pathlib import Path

import lance
import pyarrow as pa
import pytest

from bench.config import BenchConfig, build_parser
from bench.experiment import append_history, baseline_delta, headline_numbers, knob_vector
from bench.sizes import measure_dataset_sizes


def write_dataset_with_planted_index(root: Path, name: str, rows: int, index_bytes: int) -> Path:
    """Write a small Lance dataset and plant a synthetic index payload under ``_indices/``.

    The size walk is pure filesystem accounting, so a planted index file exercises the
    data/index/meta split without needing a real index build.

    Args:
        root: The Lance base directory.
        name: Dataset directory name (without the ``.lance`` suffix).
        rows: Row count of the written data.
        index_bytes: Size of the planted index file.

    Returns:
        The dataset directory.
    """
    dataset_dir: Path = root / f"{name}.lance"
    table: pa.Table = pa.table({"id": pa.array(range(rows), pa.int64())})
    lance.write_dataset(table, str(dataset_dir))
    index_dir: Path = dataset_dir / "_indices" / "00000000-planted"
    index_dir.mkdir(parents=True)
    (index_dir / "index.idx").write_bytes(b"x" * index_bytes)
    return dataset_dir


def test_measure_dataset_sizes_splits_data_index_meta(tmp_path: Path) -> None:
    """The walk splits bytes by layout dir and aggregates fleet totals."""
    root: Path = tmp_path / "lance"
    write_dataset_with_planted_index(root, "org0/t/a", rows=64, index_bytes=1000)
    write_dataset_with_planted_index(root, "org1/t/b", rows=64, index_bytes=3000)

    sizes = measure_dataset_sizes(root)

    assert sizes["dataset_count"] == 2
    assert sizes["index_bytes"] == 4000
    assert sizes["data_bytes"] > 0
    assert sizes["meta_bytes"] > 0
    assert sizes["total_bytes"] == sizes["data_bytes"] + sizes["index_bytes"] + sizes["meta_bytes"]
    assert sizes["index_to_data_ratio"] == round(4000 / sizes["data_bytes"], 4)
    per_dataset = {record["dataset"]: record for record in sizes["datasets"]}
    assert per_dataset["a.lance"]["index_bytes"] == 1000
    assert per_dataset["b.lance"]["index_bytes"] == 3000


def test_measure_dataset_sizes_empty_root(tmp_path: Path) -> None:
    """An empty or missing root yields zeroed totals instead of failing."""
    sizes = measure_dataset_sizes(tmp_path / "nowhere")
    assert sizes["dataset_count"] == 0
    assert sizes["total_bytes"] == 0
    assert sizes["index_to_data_ratio"] == 0.0


def experiment_config(tmp_path: Path, run_id: str, extra: list[str] | None = None) -> BenchConfig:
    """Parse an experiment configuration rooted in the test tmp dir.

    Args:
        tmp_path: The test workspace root.
        run_id: The run identifier.
        extra: Additional CLI flags.

    Returns:
        The parsed configuration.
    """
    argv: list[str] = [
        "experiment",
        "--workspace",
        str(tmp_path / "workspace"),
        "--results-root",
        str(tmp_path / "results"),
        "--run-id",
        run_id,
        *(extra or []),
    ]
    return BenchConfig.from_args(build_parser().parse_args(argv))


def test_experiment_flags_parse(tmp_path: Path) -> None:
    """The experiment remains offline-only and accepts baseline comparison."""
    config: BenchConfig = experiment_config(tmp_path, "r1", ["--baseline", "r0"])
    assert config.command == "experiment"
    assert config.endpoint == ""
    assert config.baseline == "r0"


def test_endpoint_is_unrestricted_across_subcommands(tmp_path: Path) -> None:
    """--endpoint takes a plaintext host:port and is not gated behind any subcommand or credential."""
    config: BenchConfig = experiment_config(tmp_path, "with-endpoint", ["--endpoint", "127.0.0.1:50051"])
    assert config.endpoint == "127.0.0.1:50051"
    e2e_config: BenchConfig = BenchConfig.from_args(
        build_parser().parse_args(
            [
                "e2e",
                "--workspace",
                str(tmp_path / "workspace"),
                "--results-root",
                str(tmp_path / "results"),
                "--endpoint",
                "127.0.0.1:50051",
                "--search-expected-versions-path",
                str(tmp_path / "expected.json"),
            ]
        )
    )
    assert e2e_config.endpoint == "127.0.0.1:50051"
    assert e2e_config.search_expected_versions_path == (tmp_path / "expected.json").resolve()


def test_obsolete_local_server_flags_removed() -> None:
    """The retired local-server spawn flags remain absent from the experiment parser."""
    with pytest.raises(SystemExit):
        build_parser().parse_args(["experiment", "--build-server"])
    with pytest.raises(SystemExit):
        build_parser().parse_args(["experiment", "--server-env", "SEARCH_API_CACHE_BACKEND=memory"])


def test_headline_numbers_picks_best_and_knee() -> None:
    """The headline carries the best-recall point and the fastest point at the target."""
    sweep = {
        "first_query": {"org0": {"cold_ms": 10.0, "warm_ms": 2.0}, "org1": {"cold_ms": 20.0, "warm_ms": 3.0}},
        "points": [
            {"execution_policy": "catalog_profile", "recall_at_10": 0.99, "p95_ms": 12.0},
        ],
    }
    sizes = {"total_bytes": 1000, "data_bytes": 800, "index_bytes": 150}

    headline = headline_numbers(sweep, sizes, build_seconds=42.5)

    assert headline["best_recall_at_10"] == 0.99
    assert headline["best_point"] == {"execution_policy": "catalog_profile"}
    assert headline["knee_point"] == {"execution_policy": "catalog_profile"}
    assert headline["knee_p95_ms"] == 12.0
    assert headline["cold_first_query_ms"] == 15.0
    assert headline["build_seconds"] == 42.5
    assert headline["total_bytes"] == 1000


def test_headline_numbers_with_skipped_sweep() -> None:
    """A server-less run still yields size and build headline numbers."""
    headline = headline_numbers(
        {"skipped": "no binary"}, {"total_bytes": 10, "data_bytes": 8, "index_bytes": 1}, build_seconds=1.0
    )
    assert headline["total_bytes"] == 10
    assert "best_recall_at_10" not in headline
    assert "cold_first_query_ms" not in headline


def test_history_append_and_baseline_delta(tmp_path: Path) -> None:
    """History lines accumulate per run and the baseline delta compares headline metrics."""
    base_config: BenchConfig = experiment_config(tmp_path, "run-a")
    base_headline = {"best_recall_at_10": 0.90, "total_bytes": 1000, "build_seconds": 10.0}
    append_history(base_config, base_headline)
    base_config.run_dir().mkdir(parents=True, exist_ok=True)
    (base_config.run_dir() / "metrics.json").write_text(json.dumps({"headline": base_headline}))

    current: BenchConfig = experiment_config(tmp_path, "run-b", ["--baseline", "run-a"])
    current_headline = {"best_recall_at_10": 0.95, "total_bytes": 1200, "build_seconds": 9.0}
    history_path: Path = append_history(current, current_headline)

    lines = [json.loads(line) for line in history_path.read_text().splitlines()]
    assert [line["run_id"] for line in lines] == ["run-a", "run-b"]
    assert lines[1]["knobs"]["dataset"] == "sift1m"
    assert set(knob_vector(current)) >= {"ivf_partitions", "compact_target_rows"}
    assert "server_env" not in knob_vector(current)

    delta = baseline_delta(current, current_headline)
    assert delta is not None
    assert delta["baseline_run_id"] == "run-a"
    assert delta["best_recall_at_10"] == {"baseline": 0.90, "current": 0.95, "delta": 0.05}
    assert delta["total_bytes"]["delta"] == 200


def test_baseline_delta_missing_run(tmp_path: Path) -> None:
    """A baseline without metrics.json degrades to None instead of failing the run."""
    config: BenchConfig = experiment_config(tmp_path, "run-x", ["--baseline", "ghost"])
    assert baseline_delta(config, {"total_bytes": 1}) is None
