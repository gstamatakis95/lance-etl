"""Parse and contract tests for the single parameter-free reconciler DAG."""

from __future__ import annotations

import importlib
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from types import ModuleType

import pytest

pytest.importorskip("airflow.models", reason="apache-airflow is not installed in the test environment")
pytest.importorskip(
    "airflow.providers.apache.spark.operators.spark_submit",
    reason="the apache-airflow-providers-apache-spark provider is not installed",
)

from lance_etl.reconciler import SYSTEMIC_RETRIES, production_profile

AIRFLOW_DAG_DIR: Path = Path(__file__).resolve().parent.parent / "airflow"
MODULE_NAMES: tuple[str, str] = ("lance_etl_common", "lance_etl_reconciler_dag")
EXPECTED_TASKS: tuple[str, ...] = (
    "plan_and_enqueue_window",
    "run_due_target_work",
    "reconcile_results",
    "gate_source_retention",
    "emit_slo_status",
)


@pytest.fixture
def import_dag_module() -> Iterator[Callable[[str], ModuleType]]:
    """Provide fresh Airflow-DAG-folder imports.

    Yields:
        Callable importing one freshly evicted DAG module.
    """
    sys.path.insert(0, str(AIRFLOW_DAG_DIR))
    for name in MODULE_NAMES:
        sys.modules.pop(name, None)

    def import_fresh(name: str) -> ModuleType:
        """Import one module after evicting its previous scheduler parse.

        Args:
            name: DAG-folder module name.

        Returns:
            Freshly imported module.
        """
        sys.modules.pop(name, None)
        return importlib.import_module(name)

    yield import_fresh
    for name in MODULE_NAMES:
        sys.modules.pop(name, None)
    sys.path.remove(str(AIRFLOW_DAG_DIR))


def test_only_one_dag_module_exists() -> None:
    """Obsolete ETL and pipeline DAG modules are physically absent."""
    dag_files = sorted(path.name for path in AIRFLOW_DAG_DIR.glob("*_dag.py"))
    assert dag_files == ["lance_etl_reconciler_dag.py"]


def test_common_helper_has_fixed_release_policy(import_dag_module: Callable[[str], ModuleType]) -> None:
    """The shared helper exposes no Variable, JSON, dataset, window, or index configuration surface.

    Args:
        import_dag_module: Fresh DAG-folder importer.
    """
    common = import_dag_module("lance_etl_common")
    assert common.default_args["retries"] == SYSTEMIC_RETRIES == 24
    assert callable(common.make_reconciler_operator)
    assert not hasattr(common, "resolve_variable")
    assert not hasattr(common, "build_base_spark_conf")
    assert not hasattr(common, "build_dd_tag_flags")
    assert not hasattr(common, "variable_is_truthy")
    assert common.make_reconciler_operator("emit_slo_status").conf == production_profile().spark_configuration()


def test_reconciler_dag_has_exact_serial_shape(import_dag_module: Callable[[str], ModuleType]) -> None:
    """Five closed tasks form one serial, non-overlapping, non-catchup workflow.

    Args:
        import_dag_module: Fresh DAG-folder importer.
    """
    module = import_dag_module("lance_etl_reconciler_dag")
    dag = module.dag
    assert dag.dag_id == "lance_etl_reconciler"
    assert dag.max_active_runs == 1
    assert not dag.catchup
    assert tuple(module.RECONCILER_TASKS) == EXPECTED_TASKS
    assert set(dag.task_dict) == set(EXPECTED_TASKS)
    for index, task_id in enumerate(EXPECTED_TASKS):
        task = dag.task_dict[task_id]
        expected_upstream = {EXPECTED_TASKS[index - 1]} if index else set()
        expected_downstream = {EXPECTED_TASKS[index + 1]} if index + 1 < len(EXPECTED_TASKS) else set()
        assert task.upstream_task_ids == expected_upstream
        assert task.downstream_task_ids == expected_downstream


def test_scheduled_tasks_have_no_routine_parameters(import_dag_module: Callable[[str], ModuleType]) -> None:
    """Every task forwards only its closed action token and receives the fixed retry budget.

    Args:
        import_dag_module: Fresh DAG-folder importer.
    """
    dag = import_dag_module("lance_etl_reconciler_dag").dag
    assert dict(dag.params) == {}
    for task_id, task in dag.task_dict.items():
        assert task.application_args == [task_id]
        assert task.retries == 24
        serialized = " ".join(task.application_args)
        for forbidden in (
            "--dataset-uri",
            "--window-start",
            "--window-end",
            "--tag",
            "--scalar-column",
            "--ttl-column",
            "--cache-bytes",
            "--search-api",
            "--spark-conf",
        ):
            assert forbidden not in serialized


def test_legacy_airflow_variables_cannot_change_dag(
    import_dag_module: Callable[[str], ModuleType], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Former tuning names have no effect on schedule, tasks, arguments, or Spark policy.

    Args:
        import_dag_module: Fresh DAG-folder importer.
        monkeypatch: Scoped environment mutation fixture.
    """
    for name in (
        "LANCE_ETL_DATASETS_FILE",
        "LANCE_ETL_WINDOW_START",
        "LANCE_ETL_INDEX_FLAGS",
        "LANCE_ETL_TTL_COLUMN",
        "LANCE_ETL_SPARK_CONF_OVERRIDES",
        "LANCE_ETL_CACHE_BYTES",
    ):
        monkeypatch.setenv(name, "adversarial-value")
    dag = import_dag_module("lance_etl_reconciler_dag").dag
    assert dag.schedule == production_profile().schedule
    assert tuple(dag.task_dict) == EXPECTED_TASKS
    assert all(task.conf == production_profile().spark_configuration() for task in dag.task_dict.values())
