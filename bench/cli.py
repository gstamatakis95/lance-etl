"""Command-line dispatch for the SIFT1M benchmark phases.

Each subcommand maps to one phase module; ``all`` chains the full pipeline. Phase modules are resolved through
``importlib`` at dispatch time so the lightweight subcommands (and the test suite) do not pay for Spark or matplotlib
imports. The search phase in the ``all`` chain is skipped with a recorded reason when the gRPC server is unreachable so
the rest of the report still materializes; the standalone ``search`` subcommand fails loudly instead.
"""

from __future__ import annotations

import argparse
import importlib
import json
import logging
import os
from typing import Any

import grpc

from bench.config import BenchConfig, build_parser
from bench.results import save_phase

logger: logging.Logger = logging.getLogger(__name__)

PHASE_MODULES: dict[str, tuple[str, str]] = {
    "download": ("bench.download", "run_download"),
    "prepare": ("bench.prepare", "run_prepare"),
    "ingest": ("bench.ingest", "run_ingest"),
    "index": ("bench.indexes", "run_index"),
    "compact": ("bench.compaction", "run_compact"),
    "search": ("bench.search", "run_search"),
    "report": ("bench.report", "run_report"),
}


def run_phase(config: BenchConfig, phase: str) -> dict[str, Any]:
    """Import and execute one phase.

    Args:
        config: Benchmark configuration.
        phase: The phase name.

    Returns:
        The phase result document.
    """
    module_name, function_name = PHASE_MODULES[phase]
    module = importlib.import_module(module_name)
    runner = getattr(module, function_name)
    return runner(config)


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

    Compaction is skipped when ``--batches`` is 1 since single-batch ingest produces nothing to merge; the search
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
        argv: Optional argument vector; defaults to ``sys.argv``.

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
        else:
            outcome = run_phase(config, config.command)
    except Exception:
        logger.exception("benchmark phase %s failed", config.command)
        return 1
    print(json.dumps({"run_dir": str(config.run_dir()), "command": config.command, "outcome": outcome}, default=str))
    return 0
