"""Airflow DAG: configurable-schedule Iceberg → Lance ETL pipeline.

Pipeline stages (in order):
    1. ``etl``         — reads a bounded source window from the Iceberg source table and
                         upserts/deletes into per-tenant Lance datasets. Routing uses the fixed
                         trio ``org_id``, ``tenant_id``, ``namespace`` (``ROUTING_COLS`` in
                         :mod:`lance_etl.etl`).
    2. ``maintenance`` — per-dataset maintenance over the fleet listed in the datasets file.
                         Per-row TTL expiration (only when a TTL column is configured via the
                         Airflow Variable ``lance_etl_ttl_column``), two-tier distributed
                         compaction, and version cleanup, in that order. TTL deletes expired
                         rows before compaction so the compaction reclaims them. When
                         ``lance_etl_ttl_column`` is unset the TTL step is a no-op and the
                         task is compaction plus cleanup only.
    3. ``index``       — builds or incrementally maintains IVF_RQ vector and
                         btree/bitmap/FTS scalar indices over the same fleet.

Maintenance and index read their dataset list from the static ``lance_etl_datasets_file``.
Per-dataset incremental maintenance is cheap for unchanged datasets: the consolidated
maintenance pass opens each dataset once, and index maintenance no-ops when an index already
covers every fragment.

An optional source-table maintenance stage ``optimize-iceberg`` can be enabled via the Airflow Variable
``lance_etl_optimize_iceberg_enabled`` (default off). When enabled it runs before ``etl`` and optimizes the upstream
Iceberg source table with Iceberg's own ``CALL`` maintenance procedures. It is distinct from the Lance ``maintenance``
stage that optimizes the Lance datasets.

The ``migrate-namespace`` subcommand is a one-off operator tool run manually via the CLI and is not scheduled here.

Compaction runs before indexing on purpose. The compaction planner cannot bin fragments with different
index-coverage sets together, and the distributed commit binding always remaps covering indices inline, so
compacting the fresh still-uncovered merge_insert fragments first merges them into large fragments before any
index covers them. Indexing then covers one large fragment per dataset and the inline remap cost for fresh data
disappears. This order also serializes compaction and index commits per dataset within a run, which the large
tier requires because its commit binding cannot defer index remap. ``max_active_runs=1`` extends that
serialization across runs: overlapping runs would race same-name index maintenance commits (the loser's build is
silently discarded) and let an index build overlap a compaction commit on the same dataset.

Each stage maps to a ``SparkSubmitOperator`` that calls ``python -m lance_etl.cli <subcommand>`` with flags resolved
from Airflow Variables and DAG-run params.

Schedule
--------
The DAG schedule is driven by the Airflow Variable ``lance_etl_schedule`` (default ``@daily``).  Update that Variable
in the Airflow UI or API to change frequency (``@hourly``, ``0 */4 * * *``, etc.) without touching this file.

Date-range handling
-------------------
Each run processes a bounded source window derived from the Airflow data interval:

* **Scheduled runs** — ``data_interval_start`` and ``data_interval_end`` are the natural slot boundaries produced by
  Airflow for the configured schedule.
* **Manual trigger with override** — supply ``{"start": "<ISO-8601>", "end": "<ISO-8601>"}`` in the *Trigger DAG /
  Configuration JSON* dialog (or ``dag_run.conf`` via the REST API).  These values take precedence over the data
  interval and are forwarded directly to the ETL CLI as ``--window-start`` / ``--window-end``.

Both sets of values are resolved at task-execution time via Jinja templates so the ETL ``spark-submit`` invocation
always receives a well-defined window.

Backfill usage
--------------
Scheduled catchup is disabled (``catchup=False`` with a fixed ``start_date``) so re-parsing the DAG never triggers a
surprise backfill. With a real data interval per run, explicit Airflow-native backfill still works out of the box::

    airflow dags backfill lance_etl_pipeline --start-date 2024-01-01 --end-date 2024-02-01

Each interval slot is submitted as an independent DAG run. The ETL's idempotent ``merge_insert`` ensures that
replaying a slot converges rather than duplicating rows.  Parallelism is controlled by ``max_active_runs`` on the DAG
(set to 1) to prevent overlapping runs from racing concurrent index maintenance commits on the same dataset.  For
backfills that need higher throughput, set ``max_active_runs`` via a DAG code change and ensure no two simultaneous
runs target the same dataset.

Deployment notes
----------------
The ``lance-etl`` wheel **must** be installed on every Spark executor before the DAG runs.  Two approaches work in
practice:

1. **Bake it into the Docker image / conda env** used by your Spark cluster. Set ``spark_binary`` and the ``SPARK_HOME``
   connection accordingly.

2. **Ship it at submission time** via ``--py-files`` or ``--packages``::

       spark_conf_overrides = {
           "spark.submit.pyFiles": "s3://my-bucket/wheels/lance_etl-0.1.0-py3-none-any.whl",
       }

The Airflow Connection ``spark_default`` (type: Apache Spark) must point at your cluster master / YARN resource manager
/ Kubernetes API server.  Override the connection id via the Airflow Variable ``lance_etl_spark_conn_id``.

Airflow Variables (all optional — defaults are listed in ``dag_params`` below):
    lance_etl_schedule               Airflow schedule expression (default: ``@daily``).
    lance_etl_iceberg_table          Fully-qualified Iceberg table name.
    lance_etl_lance_base_uri         Base URI under which per-tenant datasets live.
    lance_etl_datasets_file          Path to a file listing every dataset URI (one per line).
                                     The maintenance and index stages read their fleet from
                                     this file on every run.
    lance_etl_index_flags            Shell-tokenized index column-selection flags appended verbatim to the
                                     ``index`` subcommand, e.g.
                                     ``--vector-column vector --metric cosine --scalar-column updated_at
                                     --bitmap-column category --text-column text``. Empty (the default) means the
                                     indexer configures no handlers and the ``index`` step is a no-op.
    lance_etl_spark_conf_overrides   JSON object of extra Spark conf key/value pairs,
                                     e.g. {"spark.executor.instances": "16"}.
    lance_etl_spark_conn_id          Airflow Spark connection id (default: spark_default).
    lance_etl_dd_service             Datadog service tag (default: lance-pipeline).
    lance_etl_dd_env                 Datadog env tag (default: prod).
    lance_etl_dd_tags                Comma-separated ``key:value`` pairs forwarded as
                                     ``--dd-tag`` flags (default: empty).
    lance_etl_executor_instances     spark.executor.instances override (default: 8).
    lance_etl_executor_memory        spark.executor.memory override (default: 8g).
    lance_etl_driver_memory          spark.driver.memory override (default: 8g). The index stage
                                     trains IVF centroids on the driver and needs at least 8g for
                                     the largest datasets.
    lance_etl_ttl_column             Per-row TTL column name forwarded to the ``maintenance`` step as
                                     ``--ttl-column``. Empty (the default) turns TTL off so maintenance is
                                     compaction plus cleanup only. When set, the column must hold each row's
                                     lifetime as an Arrow Duration and rows are expired before compaction.
    lance_etl_optimize_iceberg_enabled
                                     When truthy (``true``/``1``/``yes``), an optional ``optimize-iceberg`` task is
                                     added before ``etl`` that runs Iceberg's own source-table maintenance
                                     procedures (rewrite_data_files, rewrite_manifests, expire_snapshots) on the
                                     source table. Default off, so the chain stays ``etl >> maintenance >> index``.
                                     This is source-table maintenance and is distinct from the Lance ``maintenance``
                                     task that optimizes the Lance datasets.
    lance_etl_optimize_remove_orphan_files
                                     When truthy, the optional ``optimize-iceberg`` task also runs the destructive
                                     opt-in ``remove_orphan_files`` procedure (only files older than Iceberg's safety
                                     horizon are removed). Default off.
"""

from __future__ import annotations

import json
import logging
import shlex
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from airflow import DAG

logger: logging.Logger = logging.getLogger(__name__)

DAG_ID = "lance_etl_pipeline"

LANCE_ETL_CLI = "/opt/lance-etl/src/lance_etl/cli.py"
"""Absolute path to ``lance_etl/cli.py`` on the Spark driver and executors.

``spark-submit`` requires a ``.py`` file as the ``application`` argument. The path must match where the ``lance-etl``
wheel is installed (or unpacked) on the cluster.  Override it by baking a different path into the image or by setting
``spark.submit.pyFiles`` to ship the wheel and adjusting this constant.
"""

dag_params: dict[str, str | int] = {
    "iceberg_table": "prod.vectors.events",
    "lance_base_uri": "s3://my-bucket/lance",
    "datasets_file": "/opt/lance/datasets.txt",
    "dd_service": "lance-pipeline",
    "dd_env": "prod",
    "dd_tags": "",
    "executor_instances": 8,
    "executor_memory": "8g",
    "driver_memory": "8g",
    "spark_conf_overrides": "{}",
}


def resolve_variable(key: str, params: dict[str, str | int], param_key: str | None = None) -> str:
    """Return the Airflow Variable value if set, else fall back to the DAG-run param.

    The Variable is looked up under the name ``lance_etl_<key>``.  This lets operators override defaults without editing
    the DAG file.  When the Variable key and the params key are identical (the common case) the ``param_key`` argument
    may be omitted.

    Args:
        key: Short key used to build the Variable name ``lance_etl_<key>``.
        params: DAG-run ``params`` dict (from the context or defaults).
        param_key: Key in ``params`` to use as the fallback.  Defaults to ``key`` when not supplied.

    Returns:
        The resolved string value.
    """
    return Variable.get(f"lance_etl_{key}", default_var=str(params[param_key if param_key is not None else key]))


def build_base_spark_conf(params: dict[str, str | int]) -> dict[str, str]:
    """Build the Spark configuration dict from params and Variable overrides.

    Executor instance count and memory are taken from Variables / params first, then any ``spark_conf_overrides`` JSON
    is merged on top (overrides win).

    Args:
        params: DAG-run ``params`` dict.

    Returns:
        A flat ``{spark_key: value}`` dict suitable for ``SparkSubmitOperator.conf``.
    """
    conf: dict[str, str] = {
        "spark.executor.instances": str(resolve_variable("executor_instances", params)),
        "spark.executor.memory": resolve_variable("executor_memory", params),
        "spark.driver.memory": resolve_variable("driver_memory", params),
    }
    raw_overrides = resolve_variable("spark_conf_overrides", params)
    try:
        extra: dict[str, str] = json.loads(raw_overrides)
    except (json.JSONDecodeError, TypeError):
        extra = {}
    conf.update({str(k): str(v) for k, v in extra.items()})
    return conf


def build_dd_tag_flags(params: dict[str, str | int]) -> list[str]:
    """Return a flat list of ``--dd-tag key:value`` CLI tokens.

    Tags are read from the ``lance_etl_dd_tags`` Variable (comma-separated ``key:value`` pairs) or the ``dd_tags`` DAG
    param.

    Args:
        params: DAG-run ``params`` dict.

    Returns:
        A list of alternating ``--dd-tag`` and ``key:value`` strings.
    """
    raw = resolve_variable("dd_tags", params).strip()
    if not raw:
        return []
    tokens: list[str] = []
    for raw_tag in raw.split(","):
        tag: str = raw_tag.strip()
        if tag:
            tokens += ["--dd-tag", tag]
    return tokens


def build_etl_application_args(params: dict[str, str | int]) -> list[str]:
    """Build the CLI argument list for the ``etl`` subcommand.

    The window bounds are resolved with the following precedence (highest first):

    1. ``dag_run.conf`` keys ``start`` / ``end`` — explicit override supplied when triggering the DAG manually.
    2. Airflow data interval — ``{{ data_interval_start }}`` / ``{{ data_interval_end }}`` provided by the scheduler
       for every scheduled or backfill run.

    The Jinja expression ``{{ dag_run.conf.get('start', data_interval_start) | string }}`` evaluates to the
    ``dag_run.conf['start']`` value when present and non-empty, falling back to the templated data interval boundary.
    Both values are forwarded as ``--window-start`` / ``--window-end`` ISO-8601 strings so the ETL CLI can apply a
    pushdown filter on the Iceberg read.  The Iceberg snapshot range (``--start`` / ``--end``) continues to use the
    data interval directly so Iceberg partition pruning covers the snapshot history regardless of the window filter.

    Routing always uses the fixed ``org_id``, ``tenant_id``, ``namespace`` trio and no routing flag is emitted.

    Args:
        params: DAG-run ``params`` dict.

    Returns:
        Argument list starting with the ``etl`` subcommand token.
    """
    args = [
        "etl",
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


def build_datasets_subcommand_args(subcommand: str, params: dict[str, str | int]) -> list[str]:
    """Build the CLI argument list for a datasets-file subcommand (``index`` or ``maintenance``).

    Both subcommands share the same required flags: the datasets file, the Datadog service/env tags, and any
    user-supplied tag pairs.  The leading subcommand token differs.

    The ``index`` subcommand additionally needs column-selection flags: with no ``--vector-column`` /
    ``--scalar-column`` / ``--bitmap-column`` / ``--text-column`` the indexer configures zero handlers and the
    Spark job is a silent no-op.  Those flags are read verbatim from the ``lance_etl_index_flags`` Airflow
    Variable (shell-tokenized) so operators control exactly which index types are maintained without editing this
    file.

    The ``maintenance`` subcommand additionally appends ``--ttl-column`` when the ``lance_etl_ttl_column``
    Variable is set (turns on per-row TTL expiration before compaction).

    Args:
        subcommand: The CLI subcommand token, either ``"index"`` or ``"maintenance"``.
        params: DAG-run ``params`` dict.

    Returns:
        Argument list starting with ``subcommand``.
    """
    args = [
        subcommand,
        "--datasets-file",
        resolve_variable("datasets_file", params),
        "--dd-service",
        resolve_variable("dd_service", params),
        "--dd-env",
        resolve_variable("dd_env", params),
    ]
    args += build_dd_tag_flags(params)
    if subcommand == "index":
        index_flags: str = Variable.get("lance_etl_index_flags", default_var="").strip()
        if index_flags:
            args += shlex.split(index_flags)
    if subcommand == "maintenance":
        ttl_column: str = Variable.get("lance_etl_ttl_column", default_var="").strip()
        if ttl_column:
            args += ["--ttl-column", ttl_column]
    return args


def variable_is_truthy(name: str) -> bool:
    """Return whether an Airflow Variable holds a truthy gate value.

    Args:
        name: The full Airflow Variable name to read.

    Returns:
        ``True`` when the value is one of ``true``, ``1``, or ``yes`` (case-insensitive).
    """
    return Variable.get(name, default_var="false").strip().lower() in ("true", "1", "yes")


def build_optimize_iceberg_application_args(params: dict[str, str | int]) -> list[str]:
    """Build the CLI argument list for the optional ``optimize-iceberg`` subcommand.

    The subcommand targets the same source table as ``etl`` (the ``lance_etl_iceberg_table`` Variable) and carries the
    shared Datadog identity flags. The destructive ``remove_orphan_files`` procedure is appended only when the
    ``lance_etl_optimize_remove_orphan_files`` Variable is truthy. The step toggles and retention otherwise take their
    opinionated CLI defaults.

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


def make_lance_operator(
    task_id: str,
    conn_id: str,
    application_args: list[str],
    conf: dict[str, str],
) -> SparkSubmitOperator:
    """Construct a ``SparkSubmitOperator`` with the shared lance-etl defaults.

    All three pipeline tasks use the same ``LANCE_ETL_CLI`` application, the same ``PYTHONPATH`` environment variable,
    and the same empty-string sentinels for optional Spark submit options.  This factory captures those constants in one
    place.

    Args:
        task_id: Airflow task identifier and the ``name`` suffix for the Spark application.
        conn_id: Airflow Spark connection id.
        application_args: CLI arguments forwarded after the application path.
        conf: Spark configuration key/value pairs.

    Returns:
        The configured ``SparkSubmitOperator``.
    """
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id=conn_id,
        application=LANCE_ETL_CLI,
        application_args=application_args,
        name=f"lance-etl-{task_id}",
        conf=conf,
        py_files="",
        verbose=False,
        do_xcom_push=False,
        env_vars={"PYTHONPATH": "/opt/lance-etl/src"},
        spark_binary="spark-submit",
        driver_class_path="",
        jars="",
        packages="",
        exclude_packages="",
        keytab="",
        principal="",
        proxy_user="",
    )


default_args: dict[str, Any] = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
    "email_on_failure": False,
    "email_on_retry": False,
}

dag_schedule: str = Variable.get("lance_etl_schedule", default_var="@daily")

with DAG(
    dag_id=DAG_ID,
    description="Iceberg → Lance ETL: etl → maintenance → index (schedule driven by lance_etl_schedule Variable)",
    schedule=dag_schedule,
    start_date=datetime(2026, 6, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params=dag_params,
    tags=["lance", "etl", "vector-db"],
) as dag:
    spark_conn_id: str = Variable.get("lance_etl_spark_conn_id", default_var="spark_default")
    spark_conf: dict[str, str] = build_base_spark_conf(dag_params)

    etl_task = make_lance_operator("etl", spark_conn_id, build_etl_application_args(dag_params), spark_conf)
    maintenance_task = make_lance_operator(
        "maintenance", spark_conn_id, build_datasets_subcommand_args("maintenance", dag_params), spark_conf
    )
    index_task = make_lance_operator(
        "index", spark_conn_id, build_datasets_subcommand_args("index", dag_params), spark_conf
    )

    if variable_is_truthy("lance_etl_optimize_iceberg_enabled"):
        optimize_iceberg_task = make_lance_operator(
            "optimize-iceberg",
            spark_conn_id,
            build_optimize_iceberg_application_args(dag_params),
            spark_conf,
        )
        optimize_iceberg_task >> etl_task

    etl_task >> maintenance_task >> index_task
