"""Aggregate per-run phase artifacts into summary tables, CSVs, and the Pareto plot.

Reads every ``<phase>.json`` present in the run directory and writes ``summary.md`` (markdown tables), ``recall.csv``
(the recall/latency sweep), ``results.csv`` (a long-format combination of every phase's headline metrics), and
``pareto.png`` (recall@10 versus QPS for the release profile). :func:`plot_pareto` forces the Agg
backend right before drawing so importing this module never touches a GUI toolkit.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import matplotlib
import matplotlib.pyplot as plt

from bench.config import PHASE_NAMES, BenchConfig
from bench.results import ensure_dir, load_phase, save_phase, utc_now

SWEEP_COLUMNS: tuple[str, ...] = (
    "execution_policy",
    "queries",
    "recall_at_1",
    "recall_at_10",
    "recall_at_100",
    "mean_ms",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "qps_single_stream",
)


def markdown_table(headers: list[str], rows: list[list[Any]]) -> str:
    """Render a markdown table.

    Args:
        headers: Column headers.
        rows: Cell values per row.

    Returns:
        The markdown text.
    """
    lines: list[str] = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    for row in rows:
        lines.append("| " + " | ".join(str(value) for value in row) + " |")
    return "\n".join(lines)


def write_sweep_csv(path: Path, sweep: list[dict[str, Any]]) -> None:
    """Write the recall/latency sweep as a CSV.

    Args:
        path: Destination file.
        sweep: The sweep points from the search phase.
    """
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(SWEEP_COLUMNS), extrasaction="ignore")
        writer.writeheader()
        for point in sweep:
            writer.writerow(point)


def flatten_metrics(phase: str, payload: dict[str, Any], prefix: str = "") -> list[tuple[str, str, Any]]:
    """Flatten numeric metrics of one phase document into long-format rows.

    Args:
        phase: The phase name.
        payload: The phase document.
        prefix: Dotted key prefix accumulated through recursion.

    Returns:
        ``(phase, metric, value)`` rows for every scalar numeric value.
    """
    rows: list[tuple[str, str, Any]] = []
    for key, value in payload.items():
        name: str = f"{prefix}{key}"
        if isinstance(value, bool):
            continue
        if isinstance(value, int | float):
            rows.append((phase, name, value))
        elif isinstance(value, dict):
            rows.extend(flatten_metrics(phase, value, prefix=f"{name}."))
    return rows


def write_results_csv(path: Path, phases: dict[str, dict[str, Any] | None]) -> int:
    """Write the combined long-format metrics CSV across all phases.

    Args:
        path: Destination file.
        phases: Phase documents by name.

    Returns:
        The number of metric rows written.
    """
    rows: list[tuple[str, str, Any]] = []
    for phase, payload in phases.items():
        if payload is not None:
            rows.extend(flatten_metrics(phase, payload))
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["phase", "metric", "value"])
        writer.writerows(rows)
    return len(rows)


def plot_pareto(path: Path, sweep: list[dict[str, Any]], title: str) -> bool:
    """Plot recall@10 versus single-stream QPS for catalog profiles.

    Args:
        path: Destination PNG.
        sweep: The sweep points from the search phase.
        title: The plot title, carrying the dataset name.

    Returns:
        ``True`` when a plot was written.
    """
    if not sweep:
        return False
    matplotlib.use("Agg", force=True)
    figure, axes = plt.subplots(figsize=(8, 6))
    points: list[dict[str, Any]] = sorted(sweep, key=lambda point: point["qps_single_stream"])
    axes.plot(
        [point["qps_single_stream"] for point in points],
        [point["recall_at_10"] for point in points],
        marker="o",
        label="catalog profile",
    )
    axes.set_xlabel("QPS (single stream)")
    axes.set_ylabel("recall@10")
    axes.set_title(title)
    axes.grid(True, alpha=0.3)
    axes.legend()
    figure.tight_layout()
    figure.savefig(path, dpi=150)
    plt.close(figure)
    return True


def summary_sections(config: BenchConfig, phases: dict[str, dict[str, Any] | None]) -> list[str]:
    """Build the markdown sections of the run summary.

    Args:
        config: Benchmark configuration.
        phases: Phase documents by name.

    Returns:
        The markdown section strings.
    """
    sections: list[str] = [
        f"# {config.dataset.upper()} benchmark run `{config.run_id}`",
        f"Generated {utc_now()} | limit={config.limit} tenants={config.tenants} seed={config.seed} "
        f"batches={config.batches}",
    ]
    ingest: dict[str, Any] | None = phases.get("ingest")
    if ingest:
        sections.append("## Ingest (real ETL)")
        sections.append(
            markdown_table(
                ["total rows", "wall (s)", "rows/sec", "batches"],
                [[ingest["total_rows"], ingest["total_seconds"], ingest["rows_per_second"], len(ingest["batches"])]],
            )
        )
    index: dict[str, Any] | None = phases.get("index")
    if index:
        sections.append("## Index builds (LanceIndexer)")
        sections.append(
            markdown_table(["stage", "wall (s)"], [[stage["stage"], stage["seconds"]] for stage in index["stages"]])
        )
    compact: dict[str, Any] | None = phases.get("compact")
    if compact:
        sections.append("## Compaction (MaintenanceJob)")
        rows: list[list[Any]] = [
            [uri.rsplit("/", 3)[-3], compact["fragments_before"][uri], compact["fragments_after"][uri]]
            for uri in compact["fragments_before"]
        ]
        sections.append(markdown_table(["dataset", "fragments before", "fragments after"], rows))
        sections.append(f"Total wall: {compact['total_seconds']}s")
    search: dict[str, Any] | None = phases.get("search")
    sections.append("## Search qualification")
    if search:
        sections.append(f"Status: `{search.get('status', 'UNKNOWN')}`")
        if search.get("reason"):
            sections.append(f"Reason: {search['reason']}")
    else:
        sections.append("Status: `NOT_RUN`")
        sections.append("Reason: no search phase artifact exists")
    if search and "sweep" in search:
        sections.append("## Recall / latency sweep")
        sections.append(
            markdown_table(
                list(SWEEP_COLUMNS), [[point.get(column) for column in SWEEP_COLUMNS] for point in search["sweep"]]
            )
        )
        sections.append("## FTS leg")
        sections.append(markdown_table(list(search["fts"].keys()), [list(search["fts"].values())]))
        sections.append("## Hybrid leg (RRF)")
        sections.append(markdown_table(list(search["hybrid"].keys()), [list(search["hybrid"].values())]))
        load: dict[str, Any] = search.get("load", {})
        sections.append("## Load (ghz)")
        if "levels" in load:
            load_headers: list[str] = ["concurrency", "qps", "mean_ms", "p50_ms", "p95_ms", "p99_ms"]
            sections.append(
                markdown_table(load_headers, [[level.get(h) for h in load_headers] for level in load["levels"]])
            )
        else:
            sections.append(load.get("reason", load.get("skipped", "not run")))
        sections.append("## First and repeated query latency")
        sections.append(
            markdown_table(
                ["org", "cold_ms", "warm_ms"],
                [[org, timing["cold_ms"], timing["warm_ms"]] for org, timing in search["first_queries"].items()],
            )
        )
    return sections


def run_report(config: BenchConfig) -> dict[str, Any]:
    """Aggregate every phase artifact in the run directory into the report files.

    Args:
        config: Benchmark configuration.

    Returns:
        The phase result document listing the written files.
    """
    run_directory: Path = ensure_dir(config.run_dir())
    phases: dict[str, dict[str, Any] | None] = {name: load_phase(config, name) for name in PHASE_NAMES}
    written: list[str] = []

    search: dict[str, Any] | None = phases.get("search")
    sweep: list[dict[str, Any]] = search.get("sweep", []) if search else []
    if sweep:
        write_sweep_csv(run_directory / "recall.csv", sweep)
        written.append("recall.csv")
        title: str = f"{config.dataset.upper()} recall vs QPS (catalog profile)"
        if plot_pareto(run_directory / "pareto.png", sweep, title):
            written.append("pareto.png")
    metric_rows: int = write_results_csv(run_directory / "results.csv", phases)
    written.append("results.csv")
    summary: str = "\n\n".join(summary_sections(config, phases)) + "\n"
    (run_directory / "summary.md").write_text(summary, encoding="utf-8")
    written.append("summary.md")
    return save_phase(config, "report", {"files": written, "metric_rows": metric_rows, "run_dir": str(run_directory)})
