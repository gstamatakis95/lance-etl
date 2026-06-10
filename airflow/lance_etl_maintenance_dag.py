"""Airflow DAG: Lance dataset fleet maintenance.

DAG id: ``lance_etl_maintenance``

This DAG runs the maintenance pass over the Lance dataset fleet via
``python -m lance_etl.maintenance run``. Each invocation covers the datasets listed in
the ``lance_etl_datasets_file`` Variable. The maintenance pass opens each dataset once
and applies, in order: optional per-row TTL expiration (when ``lance_etl_ttl_column`` is
set), two-tier distributed compaction, and version cleanup.

TTL deletes expired rows before compaction so the compaction step reclaims the freed
space. When ``lance_etl_ttl_column`` is unset the TTL step is a no-op and the task is
compaction plus cleanup only.

COEXISTENCE
-----------
The three DAGs (``lance_etl_etl``, ``lance_etl_maintenance``, ``lance_etl_index``) run
independently and share no files. Each derives its dataset list from its own inputs.
Overlapping runs across jobs are safe by design: concurrent commits are reconciled by
commit retries, the compaction replan loop, the indexer's stale-segment guards, and the
lazy frag-reuse remap. The one serialization requirement is ``max_active_runs=1`` on the
index DAG (same-name index maintenance races). Staggering the three schedules is
recommended operational practice for cluster contention, not a correctness requirement.

Airflow Variables consumed by this DAG:
    lance_etl_maintenance_schedule
        Airflow schedule expression for this DAG (default ``@daily``). Update via the
        Airflow UI or API to change frequency without touching this file.
    lance_etl_datasets_file
        Path to a file listing every dataset URI (one per line). The maintenance pass
        reads its fleet from this file on every run.
    lance_etl_ttl_column
        Per-row TTL column name forwarded as ``--ttl-column``. Empty (the default) turns
        TTL off so maintenance is compaction plus cleanup only. When set, the column must
        hold each row's lifetime as an Arrow Duration and rows are expired before
        compaction.
    lance_etl_dd_service
        Datadog service tag (default ``lance-pipeline``).
    lance_etl_dd_env
        Datadog env tag (default ``prod``).
    lance_etl_dd_tags
        Comma-separated ``key:value`` pairs forwarded as ``--dd-tag`` flags (default empty).
    lance_etl_executor_instances
        ``spark.executor.instances`` override (default ``8``).
    lance_etl_executor_memory
        ``spark.executor.memory`` override (default ``8g``).
    lance_etl_driver_memory
        ``spark.driver.memory`` override (default ``8g``).
    lance_etl_spark_conf_overrides
        JSON object of extra Spark conf key/value pairs (default ``{}``).
    lance_etl_spark_conn_id
        Airflow Spark connection id (default ``spark_default``).
"""

from __future__ import annotations

from datetime import UTC, datetime

from airflow.models import Variable
from lance_etl_common import (
    APPLICATION_MAINTENANCE,
    build_base_spark_conf,
    build_dd_tag_flags,
    default_args,
    maintenance_dag_params,
    make_lance_operator,
    resolve_variable,
)

from airflow import DAG

DAG_ID = "lance_etl_maintenance"


def build_maintenance_application_args(params: dict[str, str | int]) -> list[str]:
    """Build the CLI argument list for the maintenance module main.

    The maintenance module is invoked as ``python -m lance_etl.maintenance`` and its main
    entry point expects ``run`` as the first positional argument to select the maintenance
    subcommand. The datasets file, Datadog identity flags, and the optional TTL column are
    appended after ``run``.

    When ``lance_etl_ttl_column`` is set in Airflow Variables, ``--ttl-column`` is
    appended so the maintenance job applies per-row TTL expiration before compaction.
    When the Variable is unset or empty the flag is omitted and maintenance is compaction
    plus cleanup only.

    Args:
        params: DAG-run ``params`` dict.

    Returns:
        Argument list starting with ``run`` (the maintenance subcommand).
    """
    args = [
        "run",
        "--datasets-file",
        resolve_variable("datasets_file", params),
        "--dd-service",
        resolve_variable("dd_service", params),
        "--dd-env",
        resolve_variable("dd_env", params),
    ]
    args += build_dd_tag_flags(params)
    ttl_column: str = Variable.get("lance_etl_ttl_column", default_var="").strip()
    if ttl_column:
        args += ["--ttl-column", ttl_column]
    return args


dag_schedule: str = Variable.get("lance_etl_maintenance_schedule", default_var="@daily")

with DAG(
    dag_id=DAG_ID,
    description="Lance fleet maintenance: TTL, compaction, version cleanup (schedule: lance_etl_maintenance_schedule)",
    schedule=dag_schedule,
    start_date=datetime(2026, 6, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params=maintenance_dag_params,
    tags=["lance", "maintenance", "vector-db"],
) as dag:
    spark_conn_id: str = Variable.get("lance_etl_spark_conn_id", default_var="spark_default")
    spark_conf: dict[str, str] = build_base_spark_conf(maintenance_dag_params)

    maintenance_task = make_lance_operator(
        "maintenance",
        spark_conn_id,
        APPLICATION_MAINTENANCE,
        build_maintenance_application_args(maintenance_dag_params),
        spark_conf,
    )
