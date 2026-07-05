"""Command-line dispatch for the benchmark phases.

Each subcommand maps to one phase runner imported at module load, per the repository rule that all imports live at the
top of the file. ``all`` chains the full pipeline. ``e2e`` is the batch-major variant that runs ETL, index, compact,
and tagging per batch then does historical-tag verification. The search phase in the ``all`` chain is skipped with a
recorded reason when the gRPC server is unreachable. The standalone ``search`` subcommand fails loudly instead.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from collections.abc import Callable
from typing import Any

import grpc

from bench.compaction import run_compact
from bench.config import BenchConfig, build_parser
from bench.download import run_download
from bench.e2e import run_e2e
from bench.experiment import run_experiment
from bench.indexes import run_index
from bench.ingest import run_ingest
from bench.prepare import run_prepare
from bench.report import run_report
from bench.results import save_phase
from bench.search import run_search

logger: logging.Logger = logging.getLogger(__name__)

PHASE_RUNNERS: dict[str, Callable[[BenchConfig], dict[str, Any]]] = {
    "download": run_download,
    "prepare": run_prepare,
    "ingest": run_ingest,
    "index": run_index,
    "compact": run_compact,
    "search": run_search,
    "report": run_report,
    "e2e": run_e2e,
    "experiment": run_experiment,
}


def run_phase(config: BenchConfig, phase: str) -> dict[str, Any]:
    """Execute one phase.

    Args:
        config: Benchmark configuration.
        phase: The phase name.

    Returns:
        The phase result document.
    """
    return PHASE_RUNNERS[phase](config)


def server_reachable(config: BenchConfig) -> bool:
    """Probe whether the gRPC search server answers on the configured endpoint.

    Args:
        config: Benchmark configuration.

    Returns:
        ``True`` when a channel becomes ready within a short budget.
    """
    channel = grpc.insecure_channel(config.endpoint)
    try:
        grpc.channel_ready_future(channel).result(timeout=3.0)
        return True
    except grpc.FutureTimeoutError:
        return False
    finally:
        channel.close()


def run_all(config: BenchConfig) -> dict[str, Any]:
    """Run the full benchmark chain under one run id.

    Compaction is skipped when ``--batches`` is 1 since single-batch ingest produces nothing to merge. The search
    phase is skipped with a recorded reason when the server is unreachable.

    Args:
        config: Benchmark configuration.

    Returns:
        Phase result documents by name.
    """
    outcomes: dict[str, Any] = {}
    for phase in ("download", "prepare", "ingest", "index"):
        outcomes[phase] = run_phase(config, phase)
    if config.batches > 1:
        outcomes["compact"] = run_phase(config, "compact")
    else:
        outcomes["compact"] = save_phase(config, "compact", {"skipped": "batches=1 leaves nothing to compact"})
    if server_reachable(config):
        outcomes["search"] = run_phase(config, "search")
    else:
        reason: str = f"search server unreachable at {config.endpoint}; start rust/search-api and rerun 'search'"
        logger.warning(reason)
        outcomes["search"] = save_phase(config, "search", {"skipped": reason})
    outcomes["report"] = run_phase(config, "report")
    return outcomes


def main(argv: list[str] | None = None) -> int:
    """Parse arguments and run the selected benchmark subcommand.

    Args:
        argv: Optional argument vector. Defaults to ``sys.argv``.

    Returns:
        A process exit code.
    """
    os.environ.setdefault("DD_TRACE_ENABLED", "false")
    os.environ.setdefault("MPLBACKEND", "Agg")
    args: argparse.Namespace = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.getLevelName(args.log_level.upper()), format="%(asctime)s %(levelname)s %(message)s"
    )
    config: BenchConfig = BenchConfig.from_args(args)
    try:
        if config.command == "all":
            outcome: dict[str, Any] = run_all(config)
        elif config.command == "e2e":
            outcome = run_e2e(config)
        elif config.command == "experiment":
            outcome = run_experiment(config)
        else:
            outcome = run_phase(config, config.command)
    except Exception:
        logger.exception("benchmark phase %s failed", config.command)
        return 1
    print(json.dumps({"run_dir": str(config.run_dir()), "command": config.command, "outcome": outcome}, default=str))
    return 0
