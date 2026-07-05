"""Shared helpers for the lance-etl Airflow DAG suite.

This module is imported by the two DAG modules (``lance_etl_etl_dag``,
``lance_etl_pipeline_dag``) and provides all reusable building blocks so each DAG file
stays thin and declarative.

Shared responsibilities:

* ``resolve_variable`` — reads an Airflow Variable with a DAG-run-param fallback.
* ``build_base_spark_conf`` — assembles the Spark conf dict from Variables / params.
* ``build_dd_tag_flags`` — converts the comma-separated tag Variable into CLI tokens.
* ``variable_is_truthy`` — gates optional features on a truthy Variable value.
* ``make_lance_operator`` — SparkSubmitOperator factory parameterised by application path.
* ``default_args`` — shared Airflow task-level defaults (retries, backoff, ownership).
"""

from __future__ import annotations

import json
import logging
from datetime import timedelta
from typing import Any

from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

logger: logging.Logger = logging.getLogger(__name__)

PYTHONPATH_LANCE_ETL = "/opt/lance-etl/src"
"""PYTHONPATH injected into every Spark application so ``lance_etl`` is importable."""

APPLICATION_ETL = "/opt/lance-etl/src/lance_etl/etl/__main__.py"
"""Spark application file for the ETL job (``python -m lance_etl.etl``)."""

APPLICATION_PIPELINE = "/opt/lance-etl/src/lance_etl/pipeline/__main__.py"
"""Spark application file for the unified pipeline job (``python -m lance_etl.pipeline``)."""

APPLICATION_TOOLS = "/opt/lance-etl/src/lance_etl/tools/__main__.py"
"""Spark application file for the tools job (``python -m lance_etl.tools``)."""

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
"""Airflow task-level defaults shared by all DAGs."""

etl_dag_params: dict[str, str | int] = {
    "iceberg_table": "prod.vectors.events",
    "lance_base_uri": "s3://my-bucket/lance",
    "dd_service": "lance-pipeline",
    "dd_env": "prod",
    "dd_tags": "",
    "executor_instances": 8,
    "executor_memory": "8g",
    "driver_memory": "8g",
    "spark_conf_overrides": "{}",
}
"""Default DAG-run params for the ETL DAG."""

pipeline_dag_params: dict[str, str | int] = {
    "datasets_file": "/opt/lance/datasets.txt",
    "dd_service": "lance-pipeline",
    "dd_env": "prod",
    "dd_tags": "",
    "executor_instances": 8,
    "executor_memory": "8g",
    "driver_memory": "8g",
    "spark_conf_overrides": "{}",
}
"""Default DAG-run params for the unified pipeline DAG."""


def resolve_variable(key: str, params: dict[str, str | int], param_key: str | None = None) -> str:
    """Return the Airflow Variable value if set, else fall back to the DAG-run param.

    The Variable is looked up under the name ``lance_etl_<key>``. This lets operators override
    defaults without editing any DAG file. When the Variable key and the params key are identical
    (the common case) the ``param_key`` argument may be omitted.

    Args:
        key: Short key used to build the Variable name ``lance_etl_<key>``.
        params: DAG-run ``params`` dict (from the context or defaults).
        param_key: Key in ``params`` to use as the fallback. Defaults to ``key`` when not supplied.

    Returns:
        The resolved string value.
    """
    return Variable.get(f"lance_etl_{key}", default_var=str(params[param_key if param_key is not None else key]))


def build_base_spark_conf(params: dict[str, str | int]) -> dict[str, str]:
    """Build the Spark configuration dict from params and Variable overrides.

    Executor instance count and memory are taken from Variables / params first, then any
    ``spark_conf_overrides`` JSON is merged on top (overrides win).

    ``spark.executor.memoryOverheadFactor`` defaults to ``0.3`` because lance-etl executors run
    substantial native and Python-worker memory outside the JVM heap: Lance dataset reads and
    writes happen in native code, and the pivot plus merge closures live in PySpark workers. The
    Spark default of 0.1 under-provisions that off-heap footprint and shows up as executors
    killed by the resource manager rather than JVM OOMs. Override through
    ``spark_conf_overrides`` when a workload needs a different split.

    Args:
        params: DAG-run ``params`` dict.

    Returns:
        A flat ``{spark_key: value}`` dict suitable for ``SparkSubmitOperator.conf``.
    """
    conf: dict[str, str] = {
        "spark.executor.instances": str(resolve_variable("executor_instances", params)),
        "spark.executor.memory": resolve_variable("executor_memory", params),
        "spark.driver.memory": resolve_variable("driver_memory", params),
        "spark.executor.memoryOverheadFactor": "0.3",
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

    Tags are read from the ``lance_etl_dd_tags`` Variable (comma-separated ``key:value`` pairs) or
    the ``dd_tags`` DAG param.

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


def variable_is_truthy(name: str) -> bool:
    """Return whether an Airflow Variable holds a truthy gate value.

    Args:
        name: The full Airflow Variable name to read.

    Returns:
        ``True`` when the value is one of ``true``, ``1``, or ``yes`` (case-insensitive).
    """
    return Variable.get(name, default_var="false").strip().lower() in ("true", "1", "yes")


def make_lance_operator(
    task_id: str,
    conn_id: str,
    application: str,
    application_args: list[str],
    conf: dict[str, str],
) -> SparkSubmitOperator:
    """Construct a ``SparkSubmitOperator`` with shared lance-etl defaults.

    Every pipeline task uses the same ``PYTHONPATH`` environment variable and the same
    empty-string sentinels for optional Spark submit options. The ``application`` argument
    selects which per-job ``__main__.py`` file is submitted so each DAG can invoke a
    different module without duplicating the boilerplate.

    Args:
        task_id: Airflow task identifier and the ``name`` suffix for the Spark application.
        conn_id: Airflow Spark connection id.
        application: Absolute path to the per-job ``__main__.py`` on the cluster.
        application_args: CLI arguments forwarded after the application path.
        conf: Spark configuration key/value pairs.

    Returns:
        The configured ``SparkSubmitOperator``.
    """
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id=conn_id,
        application=application,
        application_args=application_args,
        name=f"lance-etl-{task_id}",
        conf=conf,
        py_files="",
        verbose=False,
        do_xcom_push=False,
        env_vars={"PYTHONPATH": PYTHONPATH_LANCE_ETL},
        spark_binary="spark-submit",
        driver_class_path="",
        jars="",
        packages="",
        exclude_packages="",
        keytab="",
        principal="",
        proxy_user="",
    )
