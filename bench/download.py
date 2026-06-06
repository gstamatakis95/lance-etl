"""Download phase: acquire and verify the selected corpus through its dataset adapter.

All acquisition mechanics live on the adapter (see :mod:`bench.datasets`). This phase resolves the adapter from the
``--dataset`` flag, hands it the workspace and the optional pinned archive digest, and records the returned payload as
the phase document.
"""

from __future__ import annotations

import logging
from typing import Any

from bench.config import BenchConfig
from bench.datasets import adapter_for
from bench.results import save_phase

logger: logging.Logger = logging.getLogger(__name__)


def run_download(config: BenchConfig) -> dict[str, Any]:
    """Fetch, verify, and extract the configured corpus idempotently.

    Args:
        config: Benchmark configuration.

    Returns:
        The phase result document.
    """
    payload: dict[str, Any] = adapter_for(config).download(config.workspace, sha256=config.sha256)
    return save_phase(config, "download", payload)
