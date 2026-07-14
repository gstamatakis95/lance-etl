"""Reproducible hardware, software, workload, and cache evidence for benchmark runs."""

from __future__ import annotations

import importlib.metadata
import os
import platform
import shutil
import subprocess
from dataclasses import asdict
from pathlib import Path
from typing import Any

from bench.config import BenchConfig


def command_output(arguments: list[str], cwd: Path) -> str | None:
    """Return bounded stdout from a local evidence command.

    Args:
        arguments: Executable and arguments without a shell.
        cwd: Command working directory.

    Returns:
        Stripped stdout or null when unavailable.
    """
    try:
        result = subprocess.run(arguments, cwd=cwd, check=True, capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() or None


def memory_bytes() -> int | None:
    """Resolve physical memory without introducing a runtime dependency.

    Returns:
        Physical bytes or null when the platform exposes no supported query.
    """
    if platform.system() == "Darwin":
        value = command_output(["sysctl", "-n", "hw.memsize"], Path.cwd())
        return int(value) if value is not None else None
    page_size = os.sysconf("SC_PAGE_SIZE") if "SC_PAGE_SIZE" in os.sysconf_names else None
    page_count = os.sysconf("SC_PHYS_PAGES") if "SC_PHYS_PAGES" in os.sysconf_names else None
    return int(page_size * page_count) if page_size is not None and page_count is not None else None


def dependency_version(distribution: str) -> str | None:
    """Return an installed distribution version when present.

    Args:
        distribution: Python package distribution name.

    Returns:
        Installed version or null.
    """
    try:
        return importlib.metadata.version(distribution)
    except importlib.metadata.PackageNotFoundError:
        return None


def capacity_artifact(config: BenchConfig) -> dict[str, Any]:
    """Build exact reproducibility evidence for one benchmark invocation.

    Args:
        config: Fully resolved benchmark configuration.

    Returns:
        JSON-compatible capacity and workload evidence.
    """
    disk = shutil.disk_usage(config.workspace.parent if config.workspace.parent.exists() else Path.cwd())
    repository = Path(__file__).resolve().parent.parent
    commit = command_output(["git", "rev-parse", "HEAD"], repository)
    dirty = command_output(["git", "status", "--porcelain", "--untracked-files=no"], repository)
    workload = asdict(config)
    for name in ("workspace", "corpus_root", "results_root"):
        workload[name] = str(workload[name])
    return {
        "schema_version": 1,
        "git_commit": commit,
        "git_tracked_changes_present": bool(dirty),
        "software": {
            "python": platform.python_version(),
            "pylance": dependency_version("pylance"),
            "pyarrow": dependency_version("pyarrow"),
            "pyspark": dependency_version("pyspark"),
            "operating_system": platform.platform(),
            "architecture": platform.machine(),
        },
        "hardware": {
            "logical_cpu_count": os.cpu_count(),
            "physical_memory_bytes": memory_bytes(),
            "disk_total_bytes": disk.total,
            "disk_free_bytes_at_start": disk.free,
        },
        "cache_state": {
            "workspace_existed_at_start": config.workspace.exists(),
            "prepared_corpus_existed_at_start": config.prepared_dir().exists(),
            "lance_root_existed_at_start": config.lance_root().exists(),
        },
        "workload": workload,
    }
