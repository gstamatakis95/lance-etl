"""Process bootstrap values and PostgreSQL-backed reconciler policy types."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy.engine import URL, make_url

from lance_etl.state.settings import ReconcilerSettings as ReconcilerSettings
from lance_etl.state.settings import default_reconciler_settings as default_reconciler_settings

DEFAULT_DATABASE_URL: str = "postgresql+psycopg://lance_etl:lance_etl@localhost/lance_etl"
"""Local PostgreSQL control-plane URL used when no environment override exists."""

DEFAULT_ICEBERG_PACKAGE: str = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0"
"""Iceberg runtime compatible with the pinned local PySpark release."""

DEFAULT_LOCAL_SHUFFLE_PARTITIONS: int = 8
"""Small process-wide Spark default. Dataset ingestion fan-out is PostgreSQL-owned."""

SPARK_CATALOG_PATTERN: re.Pattern[str] = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
"""Allowlist for the local Spark catalog identifier."""

LOCAL_SPARK_MASTER_PATTERN: re.Pattern[str] = re.compile(r"local(?:\[(?:\*|[1-9][0-9]*)\])?")
"""Allowlist that prevents the local-only runtime from selecting a remote Spark master."""


def control_plane_database_url() -> str:
    """Load and validate the PostgreSQL control-plane URL alone.

    Returns:
        Psycopg 3 PostgreSQL URL suitable for migrations or runtime use.

    Raises:
        ValueError: If the driver or non-local TLS settings are unsafe.
    """
    database_url_value: str = os.environ.get("LANCE_ETL_DATABASE_URL", DEFAULT_DATABASE_URL).strip()
    database_url: URL = make_url(database_url_value)
    if database_url.get_backend_name() != "postgresql" or database_url.get_driver_name() != "psycopg":
        raise ValueError("reconciler database URL requires PostgreSQL with the psycopg 3 driver")
    local_database: bool = database_url.host in (None, "localhost", "127.0.0.1", "::1")
    ssl_root: str = str(database_url.query.get("sslrootcert", ""))
    if not local_database and (
        database_url.query.get("sslmode") != "verify-full" or not ssl_root or not Path(ssl_root).is_absolute()
    ):
        raise ValueError("remote PostgreSQL requires sslmode=verify-full and an absolute sslrootcert")
    return database_url_value


@dataclass(frozen=True, slots=True)
class RuntimeSettings:
    """Local-first runtime values consumed by the reconciler process."""

    database_url: str
    lance_base_uri: str
    source_table: str
    datadog_service: str
    datadog_env: str
    canonical_baseline_snapshot_id: int | None
    spark_master: str
    spark_catalog: str
    spark_warehouse_path: Path
    spark_iceberg_package: str

    @classmethod
    def from_environment(cls) -> RuntimeSettings:
        """Load local defaults plus optional environment overrides.

        Returns:
            Validated runtime settings.

        Raises:
            ValueError: If PostgreSQL or local Spark settings are invalid.
        """
        local_root: Path = (
            Path(os.environ.get("LANCE_ETL_LOCAL_ROOT", Path.cwd() / ".lance-etl")).expanduser().resolve()
        )
        database_url_value: str = control_plane_database_url()
        lance_base_uri: str = os.environ.get("LANCE_ETL_LANCE_BASE_URI", str(local_root / "lance")).strip()
        spark_catalog: str = os.environ.get("LANCE_ETL_SPARK_CATALOG", "local").strip()
        source_table: str = os.environ.get("LANCE_ETL_SOURCE_TABLE", f"{spark_catalog}.db.events").strip()
        spark_master: str = os.environ.get("LANCE_ETL_SPARK_MASTER", "local[*]").strip()
        spark_warehouse_value: str = os.environ.get("LANCE_ETL_SPARK_WAREHOUSE", str(local_root / "iceberg")).strip()
        spark_iceberg_package: str = os.environ.get(
            "LANCE_ETL_SPARK_ICEBERG_PACKAGE",
            DEFAULT_ICEBERG_PACKAGE,
        ).strip()
        if not lance_base_uri or not source_table or not spark_master or not spark_warehouse_value:
            raise ValueError("local Lance, Iceberg table, Spark master, and warehouse settings must be non-empty")
        if LOCAL_SPARK_MASTER_PATTERN.fullmatch(spark_master) is None:
            raise ValueError("Spark master must be local, local[*], or local[N]")
        if SPARK_CATALOG_PATTERN.fullmatch(spark_catalog) is None:
            raise ValueError("Spark catalog must be a simple identifier")
        if not spark_iceberg_package:
            raise ValueError("Spark Iceberg package must be non-empty")
        baseline_value: str = os.environ.get("LANCE_ETL_CANONICAL_BASELINE_SNAPSHOT_ID", "").strip()
        baseline_snapshot_id: int | None = int(baseline_value) if baseline_value else None
        if baseline_snapshot_id is not None and baseline_snapshot_id < 0:
            raise ValueError("canonical baseline snapshot id must be non-negative")
        return cls(
            database_url=database_url_value,
            lance_base_uri=lance_base_uri,
            source_table=source_table,
            datadog_service=os.environ.get("DD_SERVICE", "lance-etl-local"),
            datadog_env=os.environ.get("DD_ENV", "local"),
            canonical_baseline_snapshot_id=baseline_snapshot_id,
            spark_master=spark_master,
            spark_catalog=spark_catalog,
            spark_warehouse_path=Path(spark_warehouse_value).expanduser().resolve(),
            spark_iceberg_package=spark_iceberg_package,
        )
