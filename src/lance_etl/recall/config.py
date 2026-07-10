"""Shared constants and :class:`RecallJobConfig` for the recall audit job.

The identifier and path allowlists, the typed comparison-operator table, the bounded query-type and
distance-type vocabularies, the BM25 parameterization constants, and the scanner batch size are collected here
because every other module in this package (``source``, ``queries``, ``scoring``, ``job``) reads at least one of
them.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from lance_etl.telemetry import TelemetryConfig

FILTER_COLUMN_PATTERN: re.Pattern[str] = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
PATH_COMPONENT_PATTERN: re.Pattern[str] = re.compile(r"^[A-Za-z0-9._-]+$")
COMPARE_OPS: dict[str, str] = {"eq": "=", "ne": "<>", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}
DISTANCE_TYPES: frozenset[str] = frozenset({"l2", "cosine", "dot", "hamming"})
QUERY_TYPES: frozenset[str] = frozenset({"vector", "text", "hybrid"})
TEXT_OPERATORS: frozenset[str] = frozenset({"or", "and"})
SPANS_SEARCH_PATH: str = "api/v2/spans/events/search"
TOKEN_PATTERN: re.Pattern[str] = re.compile(r"\w+", re.UNICODE)
BM25_K1: float = 1.2
BM25_B: float = 0.75
DEFAULT_RRF_K: float = 60.0
BATCH_SIZE: int = 8192


@dataclass
class RecallJobConfig:
    """Configuration for :class:`~lance_etl.recall.job.RecallAuditJob`.

    Attributes:
        base_uri: Root location under which per-tenant datasets live. The dataset URI for a sample is
            ``{base_uri}/{org_id}/{tenant_id}/{namespace}.lance``.
        telemetry: Telemetry configuration, the only telemetry object pickled into executor closures.
        storage_options: Object-store options forwarded to pylance.
        id_column: Name of the unique id column matched against the served result ids.
        vector_column: Name of the fixed-size-list vector column scanned for brute-force distances.
        max_samples: Cap on the number of span records fetched from the source.
        large_group_fragment_threshold: Fragment count above which a ``(uri, version)`` group is scored with the
            per-fragment fan-out instead of one whole-dataset task. Groups at or below it are batched into the packed
            small tier.
        small_tier_slices: Spark partition count for the classification probe job and the packed small-tier scoring
            job. Fewer slices than groups packs many small groups per task, amortizing task scheduling and cold opens.
        large_tier_slices: Spark partition cap for the per-fragment fan-out job. One task scores one fragment up to
            this cap, beyond which fragments share tasks while the driver still reduces them exactly.
    """

    base_uri: str
    telemetry: TelemetryConfig
    storage_options: dict[str, Any] | None = None
    id_column: str = "vector_id"
    vector_column: str = "vector"
    max_samples: int = 10_000
    large_group_fragment_threshold: int = 32
    small_tier_slices: int = 256
    large_tier_slices: int = 512
