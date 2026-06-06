"""Standalone ANN benchmark harness for the lance-etl pipeline, SIFT1M by default.

Runs the classic SIFT1M benchmark (or any dataset registered in ``bench.datasets``, selected with ``--dataset``) end to
end through the project's real production classes: the base vectors are written into a local Iceberg table, ingested
into per-tenant Lance datasets by ``IcebergToLanceETL`` (including the production ``updated_at`` window pushdown
filter), indexed with ``LanceIndexer`` (IVF_RQ, BTREE, BITMAP, INVERTED), optionally compacted with ``LanceCompactor``,
and finally searched through the Rust gRPC search service to measure recall, latency, and sustained QPS.

This package is a top-level development tool like ``airflow/``. It is not part of the shipped wheel. Run it with
``python -m bench <subcommand>`` from the repository root.
"""

from __future__ import annotations
