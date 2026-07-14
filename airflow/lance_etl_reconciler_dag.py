"""The sole scheduled production DAG for durable Iceberg-to-Lance reconciliation."""

from __future__ import annotations

from datetime import UTC, datetime

from lance_etl_common import default_args, make_reconciler_operator

from airflow import DAG
from lance_etl.reconciler import production_profile

DAG_ID: str = "lance_etl_reconciler"
"""Stable identifier of the only scheduled production workflow."""

RECONCILER_TASKS: tuple[str, ...] = (
    "plan_and_enqueue_window",
    "run_due_target_work",
    "reconcile_results",
    "gate_source_retention",
    "emit_slo_status",
)
"""Closed serialized control loop with no routine user parameters."""

with DAG(
    dag_id=DAG_ID,
    description="Durable Iceberg-to-Lance source and target reconciliation",
    schedule=production_profile().schedule,
    start_date=datetime(2026, 7, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={},
    tags=["lance", "reconciler", "vector-db"],
) as dag:
    tasks = [make_reconciler_operator(task_id) for task_id in RECONCILER_TASKS]
    for upstream, downstream in zip(tasks, tasks[1:], strict=False):
        upstream >> downstream
