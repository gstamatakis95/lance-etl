"""Unit tests for the e2e search-leg orchestration fix, entirely offline (no Spark or PostgreSQL).

The prior architecture called ``catalog_search_leg`` *after* ``with isolated_control_plane(...)``
had already exited and dropped the ephemeral schema, so the leg could never succeed under any
server configuration. These tests fake ``bench.e2e``'s module-level collaborators (the same
monkeypatch pattern ``tests/test_bench_recall_alignment.py`` uses for ``vector_search``) to assert
the ordering contract directly: the search leg must run while the isolation window is still open,
with access to the live repository and the exact isolated database URL, and only after that leg
returns may the window's cleanup (schema drop) happen.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

import bench.e2e as bench_e2e
from bench.config import BenchConfig, build_parser


def fake_config(tmp_path: Path, keep_control_plane: bool = False) -> BenchConfig:
    """Build a minimal e2e configuration pointed at a scratch workspace.

    Args:
        tmp_path: Pytest temporary directory.
        keep_control_plane: Value forwarded to ``--keep-control-plane``.

    Returns:
        A parsed configuration with the search leg explicitly disabled by default, since these
        tests fake ``catalog_search_leg`` directly and never spawn a real subprocess.
    """
    argv: list[str] = [
        "e2e",
        "--workspace",
        str(tmp_path / "workspace"),
        "--results-root",
        str(tmp_path / "results"),
        "--run-id",
        "orchestration-test",
        "--search-api-binary",
        "",
    ]
    if keep_control_plane:
        argv.append("--keep-control-plane")
    return BenchConfig.from_args(build_parser().parse_args(argv))


class TestSearchLegRunsInsideTheIsolationWindow:
    """catalog_search_leg is called before the isolated schema is dropped, not after."""

    def test_ordering_and_arguments(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """The search leg observes the live repository and isolated URL before schema drop."""
        events: list[str] = []
        isolated_repository: object = object()
        isolated_database_url: str = "postgresql://fake-host/fake-db?options=-csearch_path%3Dlance_bench_e2e_fake"

        @contextmanager
        def fake_isolated_control_plane(database_url: str, keep: bool = False) -> Iterator[tuple[Any, Any, str]]:
            del database_url
            events.append("schema-created")
            try:
                yield isolated_repository, object(), isolated_database_url
            finally:
                events.append(f"schema-dropped(keep={keep})")

        def fake_run_reconciled_batches(config: BenchConfig, repository: Any) -> list[dict[str, Any]]:
            del config
            assert repository is isolated_repository
            events.append("batches-run")
            return [{"batch": 0, "first": 0, "last": 1, "seconds": 0.1, "reconcile": {}, "servings": []}]

        def fake_verify_publications(config: BenchConfig, repository: Any) -> list[dict[str, Any]]:
            del config
            assert repository is isolated_repository
            events.append("publications-verified")
            return [{"org": "org0", "ok": True}]

        def fake_catalog_search_leg(
            config: BenchConfig, repository: Any, database_url: str, grpc_gen_dir: Path
        ) -> dict[str, Any]:
            del config, grpc_gen_dir
            assert repository is isolated_repository, "search leg must see the live isolated repository"
            assert database_url == isolated_database_url, "search leg must self-host against the isolated URL"
            assert "schema-dropped(keep=False)" not in events, "search leg ran after the schema was already dropped"
            events.append("search-leg-called")
            return {"status": "MEASURED", "recall": []}

        monkeypatch.setattr(bench_e2e, "isolated_control_plane", fake_isolated_control_plane)
        monkeypatch.setattr(bench_e2e, "run_reconciled_batches", fake_run_reconciled_batches)
        monkeypatch.setattr(bench_e2e, "verify_publications", fake_verify_publications)
        monkeypatch.setattr(bench_e2e, "catalog_search_leg", fake_catalog_search_leg)
        monkeypatch.setattr(bench_e2e, "resolve_database_url", lambda: "postgresql://base-host/base-db")

        config: BenchConfig = fake_config(tmp_path)
        result: dict[str, Any] = bench_e2e.run_e2e_body(config)

        assert events == [
            "schema-created",
            "batches-run",
            "publications-verified",
            "search-leg-called",
            "schema-dropped(keep=False)",
        ], "search leg must run strictly between publication verification and schema drop"
        assert result["final_catalog_grpc"]["status"] == "MEASURED"
        assert result["final_catalog_recall"]["status"] == "MEASURED"
        assert result["control_plane_kept"] is False

    def test_keep_control_plane_writes_its_url_after_the_window_closes(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """--keep-control-plane threads through to isolated_control_plane and is recorded."""
        isolated_database_url: str = "postgresql://fake-host/fake-db?options=-csearch_path%3Dlance_bench_e2e_kept"
        observed_keep: list[bool] = []

        @contextmanager
        def fake_isolated_control_plane(database_url: str, keep: bool = False) -> Iterator[tuple[Any, Any, str]]:
            del database_url
            observed_keep.append(keep)
            yield object(), object(), isolated_database_url

        def empty_batches(*args: Any) -> list[dict[str, Any]]:
            """Return no batch records, ignoring every positional argument."""
            del args
            return []

        def empty_publications(*args: Any) -> list[dict[str, Any]]:
            """Return no publication checks, ignoring every positional argument."""
            del args
            return []

        def not_run_leg(*args: Any) -> dict[str, Any]:
            """Return a NOT_RUN search-leg result, ignoring every positional argument."""
            del args
            return {"status": "NOT_RUN"}

        monkeypatch.setattr(bench_e2e, "isolated_control_plane", fake_isolated_control_plane)
        monkeypatch.setattr(bench_e2e, "run_reconciled_batches", empty_batches)
        monkeypatch.setattr(bench_e2e, "verify_publications", empty_publications)
        monkeypatch.setattr(bench_e2e, "catalog_search_leg", not_run_leg)
        monkeypatch.setattr(bench_e2e, "resolve_database_url", lambda: "postgresql://base-host/base-db")

        config: BenchConfig = fake_config(tmp_path, keep_control_plane=True)
        bench_e2e.run_e2e_body(config)

        assert observed_keep == [True]
        control_plane_path: Path = config.run_dir() / "control_plane.json"
        assert control_plane_path.exists(), "control_plane.json must be written when --keep-control-plane is set"


class TestCatalogSearchLegBinaryGate:
    """catalog_search_leg records NOT_RUN, never fabricated coverage, when self-hosting is off."""

    def test_none_binary_is_not_run(self, tmp_path: Path) -> None:
        """An explicitly disabled binary (--search-api-binary "") short-circuits to NOT_RUN."""
        config: BenchConfig = fake_config(tmp_path)
        assert config.search_api_binary is None
        result: dict[str, Any] = bench_e2e.catalog_search_leg(config, object(), "postgresql://unused", tmp_path)
        assert result["status"] == "NOT_RUN"
        assert "reason" in result

    def test_missing_binary_path_is_not_run(self, tmp_path: Path) -> None:
        """A configured but nonexistent binary path also short-circuits to NOT_RUN, not FAILED."""
        argv: list[str] = [
            "e2e",
            "--workspace",
            str(tmp_path / "workspace"),
            "--results-root",
            str(tmp_path / "results"),
            "--search-api-binary",
            str(tmp_path / "does-not-exist" / "search-api"),
        ]
        config: BenchConfig = BenchConfig.from_args(build_parser().parse_args(argv))
        result: dict[str, Any] = bench_e2e.catalog_search_leg(config, object(), "postgresql://unused", tmp_path)
        assert result["status"] == "NOT_RUN"
