"""Unified pipeline package: prune, compact, index, and stamp in one serialized fleet run.

Re-exports the public API so callers can write
``from lance_etl.pipeline import PipelineConfig, PipelineJob``.
"""

from __future__ import annotations

from lance_etl.maintenance.tools import prune_interval_tags, prune_interval_tags_fleet
from lance_etl.pipeline.job import PipelineConfig, PipelineJob, stamp_eligible

__all__ = [
    "PipelineConfig",
    "PipelineJob",
    "prune_interval_tags",
    "prune_interval_tags_fleet",
    "stamp_eligible",
]
