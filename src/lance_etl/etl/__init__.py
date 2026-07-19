"""Shared ETL primitives used by the reconciler.

This package is a library of reusable Lance mutation and ingestion building blocks. It is no longer
a standalone job: the production ingestion path is the PostgreSQL-backed reconciler
(:mod:`lance_etl.reconciler`), which composes these primitives directly. The sub-modules cover
canonical digests (:mod:`lance_etl.etl.digest`), operation normalization and terminal mutation
collapse (:mod:`lance_etl.etl.mutation`), the monotonic completion marker
(:mod:`lance_etl.etl.completion`), map projection and Arrow casts (:mod:`lance_etl.etl.pivot`), the
source-sequenced replay-safe merge (:mod:`lance_etl.etl.replay_sink`), and the executor-side Lance
merge sink (:mod:`lance_etl.etl.sink`).

Re-exports the small stable surface a few callers reach for by name, for example
``from lance_etl.etl import ROUTING_COLS, ETLConfig``.
"""

from __future__ import annotations

from lance_etl.etl.pivot import ROUTING_COLS, ETLConfig, pivot_map_columns
from lance_etl.etl.sink import apply_merge, dataset_uri

__all__ = [
    "ROUTING_COLS",
    "ETLConfig",
    "apply_merge",
    "dataset_uri",
    "pivot_map_columns",
]
