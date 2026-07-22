"""Replay-safe ETL primitives composed by the PostgreSQL-backed reconciler.

Modules cover canonical digests, mutation collapse, completion markers, Arrow normalization,
the source-sequenced Lance merge, and current dataset storage contracts. There is no standalone
ETL job or compatibility sink.
"""

from __future__ import annotations
