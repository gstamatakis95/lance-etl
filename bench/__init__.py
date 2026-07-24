"""Standalone ANN benchmark harness for the lance-etl reconciler, SIFT1M by default.

Runs the classic SIFT1M benchmark (or any dataset registered in ``bench.datasets``, selected with ``--dataset``) end to
end through the project's real production path: the base vectors are written into a local Iceberg table, registered as a
source and dataset specification in the PostgreSQL control plane, and ingested, indexed, compacted, and published by the
local reconciler (``e2e``). Serving is measured through the Rust gRPC search service to capture recall, latency, and
sustained QPS.

This package is a top-level local development tool. It is not part of the shipped wheel. Run it with
``python -m bench <subcommand>`` from the repository root.
"""

from __future__ import annotations
