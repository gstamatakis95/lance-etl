"""Airflow DAG: Lance dataset fleet index build and maintenance.

DAG id: ``lance_etl_index``

This DAG builds and incrementally maintains IVF_RQ vector indices and
btree/bitmap/FTS scalar indices over the Lance dataset fleet via
``python -m lance_etl.indexing``. The dataset fleet is read from the
``lance_etl_datasets_file`` Variable on every run. Index maintenance is a no-op for
datasets where every fragment is already covered, so the task is cheap for unchanged
datasets.

Column-selection flags are passed verbatim from the ``lance_etl_index_flags`` Variable
(shell-tokenized). With no ``--vector-column`` / ``--scalar-column`` / ``--bitmap-column``
/ ``--text-column`` flags the indexer configures zero handlers and the Spark job is a
silent no-op. Operators control exactly which index types are maintained by setting this
Variable without editing the DAG file.

IVF centroid training runs on the Spark driver. The driver memory must be at least 8g
for the largest datasets. The default ``lance_etl_driver_memory`` value of ``8g`` satisfies
this requirement. Raise it via the Variable if your fleet contains datasets larger than
the default training budget.

COEXISTENCE
-----------
The three DAGs (``lance_etl_etl``, ``lance_etl_maintenance``, ``lance_etl_index``) run
independently and share no files. Each derives its dataset list from its own inputs.
Overlapping runs across jobs are safe by design: concurrent commits are reconciled by
commit retries, the compaction replan loop, the indexer's stale-segment guards, and the
lazy frag-reuse remap. The one serialization requirement is ``max_active_runs=1`` on this
DAG: two concurrent index runs targeting the same dataset race same-name index
maintenance commits and the loser's build is silently discarded. Staggering the three
schedules is recommended operational practice for cluster contention, not a correctness
requirement.

Airflow Variables consumed by this DAG:
    lance_etl_index_schedule
        Airflow schedule expression for this DAG (default ``@daily``). Update via the
        Airflow UI or API to change frequency without touching this file.
    lance_etl_datasets_file
        Path to a file listing every dataset URI (one per line). The index job reads its
        fleet from this file on every run.
    lance_etl_index_flags
        Shell-tokenized index column-selection flags appended verbatim to the indexing
        module invocation, e.g.
        ``--vector-column vector --metric cosine --scalar-column updated_at
        --bitmap-column category --text-column text``. Empty (the default) means the
        indexer configures no handlers and the index step is a no-op.
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
        ``spark.driver.memory`` override (default ``8g``). The index job trains IVF
        centroids on the driver and needs at least 8g for the largest datasets.
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
    APPLICATION_INDEXING,
    build_base_spark_conf,
    build_dd_tag_flags,
    default_args,
    index_dag_params,
    make_lance_operator,
    resolve_variable,
)

from airflow import DAG

DAG_ID = "lance_etl_index"


def build_index_application_args(params: dict[str, str | int]) -> list[str]:
    """Build the CLI argument list for the indexing module main.

    The indexing module is invoked as ``python -m lance_etl.indexing`` and takes only its
    own flags (no leading subcommand token). The datasets file and Datadog identity flags
    are emitted first, followed by any shell-tokenized passthrough flags from the
    ``lance_etl_index_flags`` Airflow Variable.

    With no ``--vector-column``, ``--scalar-column``, ``--bitmap-column``, or
    ``--text-column`` flags in ``lance_etl_index_flags`` the indexer configures zero
    handlers and the Spark job is a silent no-op.

    Args:
        params: DAG-run ``params`` dict.

    Returns:
        Argument list for the indexing module (no leading subcommand token).
    """
    args = [
        "--datasets-file",
        resolve_variable("datasets_file", params),
        "--dd-service",
        resolve_variable("dd_service", params),
        "--dd-env",
        resolve_variable("dd_env", params),
    ]
    args += build_dd_tag_flags(params)
    index_flags: str = Variable.get("lance_etl_index_flags", default_var="").strip()
    if index_flags:
        args += shlex.split(index_flags)
    return args


dag_schedule: str = Variable.get("lance_etl_index_schedule", default_var="@daily")

with DAG(
    dag_id=DAG_ID,
    description="Lance dataset fleet index build and maintenance (schedule: lance_etl_index_schedule)",
    schedule=dag_schedule,
    start_date=datetime(2026, 6, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params=index_dag_params,
    tags=["lance", "index", "vector-db"],
) as dag:
    spark_conn_id: str = Variable.get("lance_etl_spark_conn_id", default_var="spark_default")
    spark_conf: dict[str, str] = build_base_spark_conf(index_dag_params)

    index_task = make_lance_operator(
        "index",
        spark_conn_id,
        APPLICATION_INDEXING,
        build_index_application_args(index_dag_params),
        spark_conf,
    )
