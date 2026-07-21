"""Command-line dispatch for the benchmark phases.

Each subcommand maps to one phase runner imported at module load, per the repository rule that all imports live at the
top of the file. ``e2e`` drives the production PostgreSQL reconciler path per batch then does historical-tag
verification. The standalone ``search`` subcommand fails loudly when external TLS and bearer-token inputs or verified
connectivity are absent.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from collections.abc import Callable
from typing import Any

from bench.capacity import capacity_artifact
from bench.config import BenchConfig, build_parser
from bench.download import run_download
from bench.e2e import run_e2e
from bench.experiment import run_experiment
from bench.fuzz import run_fuzz
from bench.prepare import run_prepare
from bench.qualification import run_qualification
from bench.report import run_report
from bench.results import write_json
from bench.search import run_search

logger: logging.Logger = logging.getLogger(__name__)

PHASE_RUNNERS: dict[str, Callable[[BenchConfig], dict[str, Any]]] = {
    "download": run_download,
    "prepare": run_prepare,
    "search": run_search,
    "report": run_report,
    "e2e": run_e2e,
    "experiment": run_experiment,
    "qualify": run_qualification,
    "fuzz": run_fuzz,
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
    write_json(config.run_dir() / "capacity.json", capacity_artifact(config))
    try:
        outcome: dict[str, Any] = run_phase(config, config.command)
    except Exception:
        logger.exception("benchmark phase %s failed", config.command)
        return 1
    print(json.dumps({"run_dir": str(config.run_dir()), "command": config.command, "outcome": outcome}, default=str))
    return 0
