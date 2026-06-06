"""Airflow DAG: configurable-schedule Iceberg → Lance ETL pipeline.

Pipeline stages (in order):
    1. ``etl``      — reads a bounded source window from the Iceberg source table and
                      upserts/deletes into per-tenant Lance datasets.
    2. ``index``    — builds IVF_RQ vector and btree/bitmap/FTS scalar indices
                      over the updated datasets.
    3. ``compact``  — runs distributed compaction and version cleanup over the
                      same datasets.

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
With ``catchup=True`` and a real data interval per run, Airflow-native backfill works out of the box::

    airflow dags backfill lance_etl_pipeline --start-date 2024-01-01 --end-date 2024-02-01

Each interval slot is submitted as an independent DAG run; the ETL's idempotent ``merge_insert`` ensures that
replaying a slot converges rather than duplicating rows.  Parallelism is controlled by ``max_active_runs`` on the DAG
(set to 3 here) so backfills do not overwhelm the cluster while keeping throughput reasonable.

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
    lance_etl_datasets_file          Path to a file with one dataset URI per line
                                     (required by the ``index`` and ``compact`` steps).
    lance_etl_spark_conf_overrides   JSON object of extra Spark conf key/value pairs,
                                     e.g. {"spark.executor.instances": "16"}.
    lance_etl_spark_conn_id          Airflow Spark connection id (default: spark_default).
    lance_etl_dd_service             Datadog service tag (default: lance-pipeline).
    lance_etl_dd_env                 Datadog env tag (default: prod).
    lance_etl_dd_tags                Comma-separated ``key:value`` pairs forwarded as
                                     ``--dd-tag`` flags (default: empty).
    lance_etl_num_partitions         Spark partition count for the ETL step (default: 512).
    lance_etl_executor_instances     spark.executor.instances override (default: 8).
    lance_etl_executor_memory        spark.executor.memory override (default: 8g).
    lance_etl_driver_memory          spark.driver.memory override (default: 4g).
    lance_etl_window_column          Iceberg timestamp column used for the window pushdown
                                     filter (default: updated_at).
"""

from __future__ import annotations

import json
import logging
from datetime import timedelta

from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

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
    "num_partitions": 512,
    "executor_instances": 8,
    "executor_memory": "8g",
    "driver_memory": "4g",
    "spark_conf_overrides": "{}",
}


def resolve_variable(key: str, param_key: str, params: dict) -> str:
    """Return the Airflow Variable value if set, else fall back to the DAG-run param.

    The Variable is looked up under the name ``lance_etl_<key>``.  This lets operators override defaults without editing
    the DAG file.

    Args:
        key: Short key used to build the Variable name.
        param_key: Key in the ``params`` dict to use as the fallback.
        params: DAG-run ``params`` dict (from the context or defaults).

    Returns:
        The resolved string value.
    """
    return Variable.get(f"lance_etl_{key}", default_var=str(params[param_key]))


def build_base_spark_conf(params: dict) -> dict[str, str]:
    """Build the Spark configuration dict from params and Variable overrides.

    Executor instance count and memory are taken from Variables / params first, then any ``spark_conf_overrides`` JSON
    is merged on top (overrides win).

    Args:
        params: DAG-run ``params`` dict.

    Returns:
        A flat ``{spark_key: value}`` dict suitable for ``SparkSubmitOperator.conf``.
    """
    executor_instances = resolve_variable("executor_instances", "executor_instances", params)
    executor_memory = resolve_variable("executor_memory", "executor_memory", params)
    driver_memory = resolve_variable("driver_memory", "driver_memory", params)

    conf: dict[str, str] = {
        "spark.executor.instances": str(executor_instances),
        "spark.executor.memory": executor_memory,
        "spark.driver.memory": driver_memory,
    }

    raw_overrides = resolve_variable("spark_conf_overrides", "spark_conf_overrides", params)
    try:
        extra: dict[str, str] = json.loads(raw_overrides)
    except (json.JSONDecodeError, TypeError):
        extra = {}
    conf.update({str(k): str(v) for k, v in extra.items()})
    return conf


def build_dd_tag_flags(params: dict) -> list[str]:
    """Return a flat list of ``--dd-tag key:value`` CLI tokens.

    Tags are read from the ``lance_etl_dd_tags`` Variable (comma-separated ``key:value`` pairs) or the ``dd_tags`` DAG
    param.

    Args:
        params: DAG-run ``params`` dict.

    Returns:
        A list of alternating ``--dd-tag`` and ``key:value`` strings.
    """
    raw = resolve_variable("dd_tags", "dd_tags", params).strip()
    if not raw:
        return []
    tokens: list[str] = []
    for tag in raw.split(","):
        tag = tag.strip()
        if tag:
            tokens += ["--dd-tag", tag]
    return tokens


def build_etl_application_args(params: dict) -> list[str]:
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

    Args:
        params: DAG-run ``params`` dict.

    Returns:
        Argument list starting with the ``etl`` subcommand token.
    """
    iceberg_table = resolve_variable("iceberg_table", "iceberg_table", params)
    lance_base_uri = resolve_variable("lance_base_uri", "lance_base_uri", params)
    dd_service = resolve_variable("dd_service", "dd_service", params)
    dd_env = resolve_variable("dd_env", "dd_env", params)
    num_partitions = resolve_variable("num_partitions", "num_partitions", params)
    window_column = Variable.get("lance_etl_window_column", default_var="updated_at")

    args = [
        "etl",
        "--table",
        iceberg_table,
        "--start",
        "{{ data_interval_start | string }}",
        "--end",
        "{{ data_interval_end | string }}",
        "--base-uri",
        lance_base_uri,
        "--num-partitions",
        str(num_partitions),
        "--dd-service",
        dd_service,
        "--dd-env",
        dd_env,
        "--window-start",
        "{{ dag_run.conf.get('start', data_interval_start) | string }}",
        "--window-end",
        "{{ dag_run.conf.get('end', data_interval_end) | string }}",
        "--window-column",
        window_column,
    ]
    args += build_dd_tag_flags(params)
    return args


def build_index_application_args(params: dict) -> list[str]:
    """Build the CLI argument list for the ``index`` subcommand.

    Args:
        params: DAG-run ``params`` dict.

    Returns:
        Argument list starting with the ``index`` subcommand token.
    """
    datasets_file = resolve_variable("datasets_file", "datasets_file", params)
    dd_service = resolve_variable("dd_service", "dd_service", params)
    dd_env = resolve_variable("dd_env", "dd_env", params)

    args = [
        "index",
        "--datasets-file",
        datasets_file,
        "--dd-service",
        dd_service,
        "--dd-env",
        dd_env,
    ]
    args += build_dd_tag_flags(params)
    return args


def build_compact_application_args(params: dict) -> list[str]:
    """Build the CLI argument list for the ``compact`` subcommand.

    Compaction includes version cleanup (``run_cleanup`` is True by default in ``LanceCompactor``; no ``--no-cleanup``
    flag is passed here).

    Args:
        params: DAG-run ``params`` dict.

    Returns:
        Argument list starting with the ``compact`` subcommand token.
    """
    datasets_file = resolve_variable("datasets_file", "datasets_file", params)
    dd_service = resolve_variable("dd_service", "dd_service", params)
    dd_env = resolve_variable("dd_env", "dd_env", params)

    args = [
        "compact",
        "--datasets-file",
        datasets_file,
        "--dd-service",
        dd_service,
        "--dd-env",
        dd_env,
    ]
    args += build_dd_tag_flags(params)
    return args


default_args: dict = {
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
    description="Iceberg → Lance ETL: etl → index → compact (schedule driven by lance_etl_schedule Variable)",
    schedule=dag_schedule,
    start_date=days_ago(1),
    catchup=True,
    max_active_runs=3,
    default_args=default_args,
    params=dag_params,
    tags=["lance", "etl", "vector-db"],
) as dag:
    spark_conn_id: str = Variable.get("lance_etl_spark_conn_id", default_var="spark_default")

    etl_task = SparkSubmitOperator(
        task_id="etl",
        conn_id=spark_conn_id,
        application=LANCE_ETL_CLI,
        application_args=build_etl_application_args(dag_params),
        name="lance-etl-etl",
        conf=build_base_spark_conf(dag_params),
        py_files="",
        verbose=False,
        do_xcom_push=False,
        env_vars={
            "PYTHONPATH": "/opt/lance-etl/src",
        },
        spark_binary="spark-submit",
        driver_class_path="",
        jars="",
        packages="",
        exclude_packages="",
        keytab="",
        principal="",
        proxy_user="",
    )

    index_task = SparkSubmitOperator(
        task_id="index",
        conn_id=spark_conn_id,
        application=LANCE_ETL_CLI,
        application_args=build_index_application_args(dag_params),
        name="lance-etl-index",
        conf=build_base_spark_conf(dag_params),
        py_files="",
        verbose=False,
        do_xcom_push=False,
        env_vars={
            "PYTHONPATH": "/opt/lance-etl/src",
        },
        spark_binary="spark-submit",
        driver_class_path="",
        jars="",
        packages="",
        exclude_packages="",
        keytab="",
        principal="",
        proxy_user="",
    )

    compact_task = SparkSubmitOperator(
        task_id="compact",
        conn_id=spark_conn_id,
        application=LANCE_ETL_CLI,
        application_args=build_compact_application_args(dag_params),
        name="lance-etl-compact",
        conf=build_base_spark_conf(dag_params),
        py_files="",
        verbose=False,
        do_xcom_push=False,
        env_vars={
            "PYTHONPATH": "/opt/lance-etl/src",
        },
        spark_binary="spark-submit",
        driver_class_path="",
        jars="",
        packages="",
        exclude_packages="",
        keytab="",
        principal="",
        proxy_user="",
    )

    etl_task >> index_task >> compact_task
