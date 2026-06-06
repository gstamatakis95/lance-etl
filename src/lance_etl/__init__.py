"""lance_etl — distributed Iceberg-to-Lance ETL, compaction, and indexing pipeline.

Provides the ETL, compaction, and indexing jobs that move data from Iceberg into per-tenant Lance datasets on a Spark
cluster, together with the shared telemetry and cloud-storage helpers they rely on.
"""

__version__ = "0.1.0"
