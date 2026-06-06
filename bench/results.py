"""Per-run result directory management and JSON artifact helpers."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from bench.config import BenchConfig


def utc_now() -> str:
    """Return the current UTC instant as an ISO-8601 string.

    Returns:
        The formatted timestamp.
    """
    return datetime.now(UTC).isoformat()


def ensure_dir(path: Path) -> Path:
    """Create a directory if needed and return it.

    Args:
        path: The directory path.

    Returns:
        The same path, guaranteed to exist.
    """
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_json(path: Path, payload: dict[str, Any]) -> None:
    """Write a JSON document, creating parent directories as needed.

    Args:
        path: Destination file.
        payload: The JSON-serializable document.
    """
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def read_json(path: Path) -> dict[str, Any]:
    """Read a JSON document.

    Args:
        path: Source file.

    Returns:
        The parsed document.
    """
    return json.loads(path.read_text(encoding="utf-8"))


def phase_path(config: BenchConfig, phase: str) -> Path:
    """Return the artifact path for one phase in the current run directory.

    Args:
        config: Benchmark configuration.
        phase: The phase name.

    Returns:
        The ``<run-dir>/<phase>.json`` path.
    """
    return config.run_dir() / f"{phase}.json"


def save_phase(config: BenchConfig, phase: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Persist one phase's result document into the run directory.

    Args:
        config: Benchmark configuration.
        phase: The phase name.
        payload: The phase result document.

    Returns:
        The payload, with a recorded timestamp added.
    """
    payload = {"phase": phase, "recorded_at": utc_now(), **payload}
    write_json(phase_path(config, phase), payload)
    return payload


def load_phase(config: BenchConfig, phase: str) -> dict[str, Any] | None:
    """Load one phase's result document if present.

    Args:
        config: Benchmark configuration.
        phase: The phase name.

    Returns:
        The document, or ``None`` when the phase has not run.
    """
    path: Path = phase_path(config, phase)
    if not path.exists():
        return None
    return read_json(path)
