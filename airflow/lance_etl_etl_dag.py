"""Airflow DAG: Iceberg source ingestion into Lance datasets (ETL only).

DAG id: ``lance_etl_etl``

This DAG handles the data-ingestion leg of the pipeline. It reads a bounded window from
the upstream Iceberg source table and upserts or deletes rows into per-tenant Lance
datasets via ``python -m lance_etl.etl``. Routing uses the fixed trio ``org_id``,
``tenant_id``, ``namespace``.

An optional source-table maintenance task ``optimize-iceberg`` can be enabled via the
Airflow Variable ``lance_etl_optimize_iceberg_enabled`` (default off). When enabled it
runs Iceberg's own ``CALL`` maintenance procedures (``rewrite_data_files``,
``rewrite_manifests``, ``expire_snapshots``, and optionally ``remove_orphan_files``)
before the ETL task. This is distinct from Lance dataset maintenance which lives in the
separate ``lance_etl_maintenance`` DAG.

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
    lance_etl_etl_schedule
        Airflow schedule expression for this DAG (default ``@daily``). Update via the
        Airflow UI or API to change frequency without touching this file.
    lance_etl_iceberg_table
        Fully-qualified Iceberg table name (default ``prod.vectors.events``).
    lance_etl_lance_base_uri
        Base URI under which per-tenant datasets live (default ``s3://my-bucket/lance``).
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
    lance_etl_optimize_iceberg_enabled
        When truthy (``true``/``1``/``yes``), the optional ``optimize-iceberg`` task is
        prepended before ``etl``. Default off.
    lance_etl_optimize_remove_orphan_files
        When truthy, the ``optimize-iceberg`` task also runs ``remove_orphan_files``.
        Default off.

Date-range handling
-------------------
Each run processes a bounded source window derived from the Airflow data interval:

* Scheduled runs use ``data_interval_start`` and ``data_interval_end`` as the natural
  slot boundaries.
* Manual triggers may supply ``{"start": "<ISO-8601>", "end": "<ISO-8601>"}`` in
  ``dag_run.conf`` to override the data interval. These values are forwarded as
  ``--window-start`` / ``--window-end``. The Iceberg snapshot range (``--start`` /
  ``--end``) always tracks the data interval directly for partition pruning.

Backfill usage
--------------
Catchup is disabled (``catchup=False``) so re-parsing the DAG never triggers a surprise
backfill. Explicit Airflow-native backfill still works::

    airflow dags backfill lance_etl_etl --start-date 2024-01-01 --end-date 2024-02-01

The ETL's idempotent ``merge_insert`` ensures replaying a slot converges rather than
duplicating rows.
"""

from __future__ import annotations

from datetime import UTC, datetime

from airflow.models import Variable
from lance_etl_common import (
    APPLICATION_ETL,
    APPLICATION_TOOLS,
    build_base_spark_conf,
    build_dd_tag_flags,
    default_args,
    etl_dag_params,
    make_lance_operator,
    resolve_variable,
    variable_is_truthy,
)

from airflow import DAG

DAG_ID = "lance_etl_etl"


def build_etl_application_args(params: dict[str, str | int]) -> list[str]:
    """Build the CLI argument list for the ETL module main.

    The window bounds are resolved with the following precedence (highest first):

    1. ``dag_run.conf`` keys ``start`` / ``end`` — explicit override supplied when
       triggering the DAG manually.
    2. Airflow data interval — ``{{ data_interval_start }}`` / ``{{ data_interval_end }}``
       provided by the scheduler for every scheduled or backfill run.

    The Jinja expression ``{{ dag_run.conf.get('start', data_interval_start) | string }}``
    evaluates to the ``dag_run.conf['start']`` value when present and non-empty, falling
    back to the templated data interval boundary. Both values are forwarded as
    ``--window-start`` / ``--window-end`` ISO-8601 strings so the ETL CLI can apply a
    pushdown filter on the Iceberg read. The Iceberg snapshot range (``--start`` / ``--end``)
    continues to use the data interval directly so Iceberg partition pruning covers the
    snapshot history regardless of the window filter.

    No leading subcommand token is emitted because the module is invoked as
    ``python -m lance_etl.etl`` and takes only its own flags.

    Args:
        params: DAG-run ``params`` dict.

    Returns:
        Argument list for the ETL module (no leading subcommand token).
    """
    args = [
        "--table",
        resolve_variable("iceberg_table", params),
        "--start",
        "{{ data_interval_start | string }}",
        "--end",
        "{{ data_interval_end | string }}",
        "--base-uri",
        resolve_variable("lance_base_uri", params),
        "--dd-service",
        resolve_variable("dd_service", params),
        "--dd-env",
        resolve_variable("dd_env", params),
        "--window-start",
        "{{ dag_run.conf.get('start', data_interval_start) | string }}",
        "--window-end",
        "{{ dag_run.conf.get('end', data_interval_end) | string }}",
    ]
    args += build_dd_tag_flags(params)
    return args


def build_optimize_iceberg_application_args(params: dict[str, str | int]) -> list[str]:
    """Build the CLI argument list for the optional tools module ``optimize-iceberg`` subcommand.

    The subcommand targets the same source table as the ETL task (the
    ``lance_etl_iceberg_table`` Variable) and carries the shared Datadog identity flags.
    The destructive ``remove_orphan_files`` procedure is appended only when the
    ``lance_etl_optimize_remove_orphan_files`` Variable is truthy. All other step toggles
    and retention values take their opinionated CLI defaults.

    Args:
        params: DAG-run ``params`` dict.

    Returns:
        Argument list starting with the ``optimize-iceberg`` subcommand token.
    """
    args = [
        "optimize-iceberg",
        "--table",
        resolve_variable("iceberg_table", params),
        "--dd-service",
        resolve_variable("dd_service", params),
        "--dd-env",
        resolve_variable("dd_env", params),
    ]
    args += build_dd_tag_flags(params)
    if variable_is_truthy("lance_etl_optimize_remove_orphan_files"):
        args += ["--remove-orphan-files"]
    return args


dag_schedule: str = Variable.get("lance_etl_etl_schedule", default_var="@daily")

with DAG(
    dag_id=DAG_ID,
    description="Iceberg to Lance ETL: optional optimize-iceberg >> etl (schedule: lance_etl_etl_schedule)",
    schedule=dag_schedule,
    start_date=datetime(2026, 6, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params=etl_dag_params,
    tags=["lance", "etl", "vector-db"],
) as dag:
    spark_conn_id: str = Variable.get("lance_etl_spark_conn_id", default_var="spark_default")
    spark_conf: dict[str, str] = build_base_spark_conf(etl_dag_params)

    etl_task = make_lance_operator(
        "etl",
        spark_conn_id,
        APPLICATION_ETL,
        build_etl_application_args(etl_dag_params),
        spark_conf,
    )

    if variable_is_truthy("lance_etl_optimize_iceberg_enabled"):
        optimize_iceberg_task = make_lance_operator(
            "optimize-iceberg",
            spark_conn_id,
            APPLICATION_TOOLS,
            build_optimize_iceberg_application_args(etl_dag_params),
            spark_conf,
        )
        optimize_iceberg_task >> etl_task
