"""Airflow DAG: unified Lance pipeline — prune, maintenance, index, stamp.

DAG id: ``lance_etl_pipeline``

This DAG is the successor to the separate ``lance_etl_maintenance`` and
``lance_etl_index`` DAGs, which are deleted. It runs the full post-ETL pipeline
over the Lance dataset fleet in a single serialized Spark job via
``python -m lance_etl.pipeline run``.

Phase order within each run
---------------------------
1. **Prune**: delete interval tags older than ``lance_etl_tag_keep_last`` (default 48).
   Prune runs first so the same run's cleanup step reclaims the versions that were
   pinned by the dropped tags.
2. **TTL + compact**: apply per-row TTL expiration (when ``lance_etl_ttl_column`` is set),
   run unified distributed compaction, and clean up old versions.
3. **Index**: build or incrementally maintain IVF_RQ vector indices and scalar/FTS indices.
   Column-selection flags come from ``lance_etl_index_flags`` (shell-tokenized).
4. **Stamp**: write an interval tag named from ``data_interval_end`` in colon-free
   ``%Y%m%dT%H%M%SZ`` UTC form.  When ``lance_etl_pipeline_serve_tag`` is truthy, the
   pipeline also advances the ``HEAD`` tag to the same version.

Serialization guarantee
-----------------------
``max_active_runs=1`` ensures that at most one pipeline run is in flight at any time.
This is the same constraint that the old index DAG used, now extended to cover the
full maintenance+index+stamp sequence.  Concurrent ETL runs and the pipeline DAG may
overlap: commit conflicts are resolved by the existing retry loop in
``commit_with_retries``.

Schedule recommendation
-----------------------
Set ``lance_etl_pipeline_schedule`` to a cron offset like ``15 * * * *`` so this DAG
trails the hourly ETL DAG within the same clock hour, e.g.::

    ETL schedules at :00, pipeline schedules at :15.

The default value ``@hourly`` is correct for environments where a single Airflow
worker runs the DAGs sequentially and no clock-offset tuning is needed.

Manual-trigger override
-----------------------
Supply ``dag_run.conf`` with ``{"datasets_file": "/path/to/override.txt"}`` to target a
different fleet file for a one-off run without editing any Variable.

Airflow Variables consumed by this DAG:
    lance_etl_pipeline_schedule
        Airflow schedule expression (default ``@hourly``).  Set to a cron expression
        such as ``15 * * * *`` to trail the ETL DAG within the hour.
    lance_etl_datasets_file
        Path to a newline-delimited file of dataset URIs (one per line).
    lance_etl_dd_service
        Datadog service tag (default ``lance-pipeline``).
    lance_etl_dd_env
        Datadog env tag (default ``prod``).
    lance_etl_dd_tags
        Comma-separated ``key:value`` pairs forwarded as ``--dd-tag`` flags (default empty).
    lance_etl_ttl_column
        Per-row TTL column name forwarded as ``--ttl-column``.  Empty (the default) turns
        TTL off so maintenance is compaction plus cleanup only.
    lance_etl_index_flags
        Shell-tokenized index column-selection flags, e.g.
        ``--vector-column vector --metric cosine --scalar-column updated_at
        --bitmap-column category --text-column body``.  Empty means no index handlers
        are configured and the index step is a no-op.
    lance_etl_tag_keep_last
        Number of most-recent interval tags to retain across the fleet (default ``48``,
        equivalent to two days at hourly cadence).  Set to ``0`` to disable tag pruning
        and retention entirely.
    lance_etl_pipeline_serve_tag
        When truthy (``true``/``1``/``yes``), the pipeline also advances the ``HEAD``
        tag after stamping the interval tag.  Default off.
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

import shlex
from datetime import UTC, datetime

from airflow.models import Variable
from lance_etl_common import (
    APPLICATION_PIPELINE,
    build_base_spark_conf,
    build_dd_tag_flags,
    default_args,
    make_lance_operator,
    pipeline_dag_params,
    resolve_variable,
    variable_is_truthy,
)

from airflow import DAG

DAG_ID = "lance_etl_pipeline"


def build_pipeline_application_args(params: dict[str, str | int]) -> list[str]:
    """Build the CLI argument list for the pipeline module main.

    The pipeline module is invoked as ``python -m lance_etl.pipeline`` with the ``run``
    subcommand.  The datasets file and Datadog identity flags are emitted first, followed
    by an optional ``--ttl-column`` from the ``lance_etl_ttl_column`` Variable, any
    shell-tokenized index flags from ``lance_etl_index_flags``, the ``--tag-stamp``
    derived from the Airflow ``data_interval_end``, the ``--tag-keep-last`` retention
    count, and finally ``--serve-tag`` when ``lance_etl_pipeline_serve_tag`` is truthy.

    Args:
        params: DAG-run ``params`` dict.

    Returns:
        Argument list starting with ``run`` (the pipeline subcommand).
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
    index_flags: str = Variable.get("lance_etl_index_flags", default_var="").strip()
    if index_flags:
        args += shlex.split(index_flags)
    args += [
        "--tag-stamp",
        "{{ data_interval_end | string }}",
        "--tag-keep-last",
        Variable.get("lance_etl_tag_keep_last", default_var="48"),
    ]
    if variable_is_truthy("lance_etl_pipeline_serve_tag"):
        args += ["--serve-tag"]
    return args


dag_schedule: str = Variable.get("lance_etl_pipeline_schedule", default_var="@hourly")

with DAG(
    dag_id=DAG_ID,
    description="Lance unified pipeline: prune >> maintenance >> index >> stamp",
    schedule=dag_schedule,
    start_date=datetime(2026, 6, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params=pipeline_dag_params,
    tags=["lance", "pipeline", "vector-db"],
) as dag:
    spark_conn_id: str = Variable.get("lance_etl_spark_conn_id", default_var="spark_default")
    spark_conf: dict[str, str] = build_base_spark_conf(pipeline_dag_params)

    pipeline_task = make_lance_operator(
        "pipeline",
        spark_conn_id,
        APPLICATION_PIPELINE,
        build_pipeline_application_args(pipeline_dag_params),
        spark_conf,
    )
