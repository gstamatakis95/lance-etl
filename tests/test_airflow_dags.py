"""DAG-parse smoke tests for the Airflow modules under ``airflow/``.

Imports each DAG module the way the Airflow scheduler would (with the DAG folder on ``sys.path``)
and asserts the DAG objects construct with the expected identity, task ids, and task ordering.
``Variable.get`` is stubbed to return its ``default_var`` (with per-test overrides) so no Airflow
metadata database is required. The whole module self-skips when ``apache-airflow`` or its Spark
provider is not installed in the test environment. The bare ``import airflow`` probe is not enough
for that check because the repository's own ``airflow/`` DAG directory is importable as a namespace
package, so the skip probes ``airflow.models`` and the Spark provider module instead.
"""

from __future__ import annotations

import importlib
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from types import ModuleType

import pytest

airflow_models: ModuleType = pytest.importorskip(
    "airflow.models", reason="apache-airflow is not installed in the test environment"
)
pytest.importorskip(
    "airflow.providers.apache.spark.operators.spark_submit",
    reason="the apache-airflow-providers-apache-spark provider is not installed",
)

AIRFLOW_DAG_DIR: Path = Path(__file__).resolve().parent.parent / "airflow"
DAG_MODULE_NAMES: tuple[str, str, str] = ("lance_etl_common", "lance_etl_etl_dag", "lance_etl_pipeline_dag")


@pytest.fixture
def dag_harness(monkeypatch: pytest.MonkeyPatch) -> Iterator[tuple[dict[str, str], Callable[[str], ModuleType]]]:
    """Provide a Variable override map and a fresh-importer for the DAG modules.

    Stubs ``Variable.get`` to serve its ``default_var`` unless the returned override map carries
    an entry for the full Variable name, so no Airflow metadata database is required. Puts the
    DAG directory on ``sys.path`` (mirroring the scheduler's DAG-folder import), evicts any
    previously imported copies of the DAG modules so each test observes its own Variable
    overrides at module-import time, and restores ``sys.path`` and ``sys.modules`` afterwards.

    Args:
        monkeypatch: Pytest monkeypatch fixture scoping the Variable stub to one test.

    Yields:
        A pair of the mutable override map (keys are full Airflow Variable names such as
        ``lance_etl_optimize_iceberg_enabled``) and a callable mapping a DAG module name to the
        freshly imported module object.
    """
    overrides: dict[str, str] = {}

    def fake_get(key: str, default_var: str | None = None) -> str | None:
        """Return the override for a Variable name, else its ``default_var``.

        Args:
            key: Full Airflow Variable name.
            default_var: Fallback value supplied by the caller.

        Returns:
            The overridden or default value.
        """
        return overrides.get(key, default_var)

    monkeypatch.setattr(airflow_models.Variable, "get", fake_get)
    sys.path.insert(0, str(AIRFLOW_DAG_DIR))
    for name in DAG_MODULE_NAMES:
        sys.modules.pop(name, None)

    def import_fresh(name: str) -> ModuleType:
        """Import one DAG module by name from the DAG directory.

        Args:
            name: Module name, for example ``lance_etl_etl_dag``.

        Returns:
            The imported module object.
        """
        return importlib.import_module(name)

    yield overrides, import_fresh
    for name in DAG_MODULE_NAMES:
        sys.modules.pop(name, None)
    sys.path.remove(str(AIRFLOW_DAG_DIR))


def test_common_module_parses(dag_harness: tuple[dict[str, str], Callable[[str], ModuleType]]) -> None:
    """The shared helper module imports cleanly and exposes the expected building blocks.

    Args:
        dag_harness: Variable override map plus fresh-import helper.
    """
    overrides, import_dag_module = dag_harness
    common: ModuleType = import_dag_module("lance_etl_common")
    assert callable(common.make_lance_operator)
    assert callable(common.build_base_spark_conf)
    assert callable(common.build_dd_tag_flags)
    assert callable(common.resolve_variable)
    assert callable(common.variable_is_truthy)
    assert common.default_args["retries"] == 2
    conf: dict[str, str] = common.build_base_spark_conf(common.etl_dag_params)
    assert conf["spark.executor.memoryOverheadFactor"] == "0.3"
    assert conf["spark.executor.instances"] == "8"


def test_etl_dag_default_shape(dag_harness: tuple[dict[str, str], Callable[[str], ModuleType]]) -> None:
    """The ETL DAG parses with only the ``etl`` task when the optimize gate is off.

    Args:
        dag_harness: Variable override map plus fresh-import helper.
    """
    overrides, import_dag_module = dag_harness
    module: ModuleType = import_dag_module("lance_etl_etl_dag")
    dag = module.dag
    assert dag.dag_id == "lance_etl_etl"
    assert dag.max_active_runs == 1
    assert not dag.catchup
    assert set(dag.task_dict) == {"etl"}
    assert dag.task_dict["etl"].upstream_task_ids == set()


def test_etl_dag_with_optimize_iceberg_enabled(
    dag_harness: tuple[dict[str, str], Callable[[str], ModuleType]],
) -> None:
    """Enabling the optimize gate prepends ``optimize-iceberg`` upstream of ``etl``.

    Args:
        dag_harness: Variable override map plus fresh-import helper.
    """
    overrides, import_dag_module = dag_harness
    overrides["lance_etl_optimize_iceberg_enabled"] = "true"
    module: ModuleType = import_dag_module("lance_etl_etl_dag")
    dag = module.dag
    assert set(dag.task_dict) == {"optimize-iceberg", "etl"}
    assert dag.task_dict["etl"].upstream_task_ids == {"optimize-iceberg"}
    assert dag.task_dict["optimize-iceberg"].downstream_task_ids == {"etl"}


def test_etl_application_args_shape(dag_harness: tuple[dict[str, str], Callable[[str], ModuleType]]) -> None:
    """The ETL application args carry the window, tag-stamp, and identity flags.

    Args:
        dag_harness: Variable override map plus fresh-import helper.
    """
    overrides, import_dag_module = dag_harness
    module: ModuleType = import_dag_module("lance_etl_etl_dag")
    args: list[str] = module.build_etl_application_args(module.etl_dag_params)
    for flag in ("--table", "--start", "--end", "--base-uri", "--window-start", "--window-end", "--tag-stamp"):
        assert flag in args, f"missing {flag} in ETL application args"
    assert args[args.index("--table") + 1] == "prod.vectors.events"


def test_pipeline_dag_shape(dag_harness: tuple[dict[str, str], Callable[[str], ModuleType]]) -> None:
    """The pipeline DAG parses with the single serialized ``pipeline`` task.

    Args:
        dag_harness: Variable override map plus fresh-import helper.
    """
    overrides, import_dag_module = dag_harness
    module: ModuleType = import_dag_module("lance_etl_pipeline_dag")
    dag = module.dag
    assert dag.dag_id == "lance_etl_pipeline"
    assert dag.max_active_runs == 1
    assert not dag.catchup
    assert set(dag.task_dict) == {"pipeline"}


def test_pipeline_application_args_shape(
    dag_harness: tuple[dict[str, str], Callable[[str], ModuleType]],
) -> None:
    """The pipeline application args start with ``run`` and honor the Variable-driven flags.

    Args:
        dag_harness: Variable override map plus fresh-import helper.
    """
    overrides, import_dag_module = dag_harness
    overrides["lance_etl_ttl_column"] = "expires_at"
    module: ModuleType = import_dag_module("lance_etl_pipeline_dag")
    args: list[str] = module.build_pipeline_application_args(module.pipeline_dag_params)
    assert args[0] == "run"
    assert "--datasets-file" in args
    assert args[args.index("--ttl-column") + 1] == "expires_at"
    assert "--tag-stamp" in args
    assert args[args.index("--tag-keep-last") + 1] == "48"
    assert "--serve-tag" not in args
