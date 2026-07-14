"""Release-owned helpers for the single durable reconciler DAG."""

from __future__ import annotations

import os
from datetime import timedelta
from typing import Any

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from lance_etl.reconciler import SYSTEMIC_RETRIES, production_profile

PYTHONPATH_LANCE_ETL: str = "/opt/lance-etl/src"
"""PYTHONPATH injected into reconciler Spark applications."""

APPLICATION_RECONCILER: str = "/opt/lance-etl/src/lance_etl/reconciler/__main__.py"
"""Single application submitted by every reconciler task."""

SPARK_CONN_ID: str = os.environ.get("LANCE_ETL_SPARK_CONN_ID", "spark_default")
"""Deployment-owned Airflow Spark connection, never a DAG-run parameter."""

default_args: dict[str, Any] = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": SYSTEMIC_RETRIES,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
    "email_on_failure": False,
    "email_on_retry": False,
}
"""Fixed systemic retry policy for scheduler and cluster failures."""


def make_reconciler_operator(task_id: str) -> SparkSubmitOperator:
    """Build one parameter-free reconciler phase submission.

    Args:
        task_id: One of the five closed scheduled phase names.

    Returns:
        Spark submission carrying only its closed phase token.
    """
    profile = production_profile()
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id=SPARK_CONN_ID,
        application=APPLICATION_RECONCILER,
        application_args=[task_id],
        name=f"lance-etl-{task_id}",
        conf=profile.spark_configuration(),
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
