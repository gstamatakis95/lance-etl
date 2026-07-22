"""Tests for locked release inputs and the local runtime contract."""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

import pytest
from alembic import command as alembic_command
from alembic.config import Config
from alembic.script import Script, ScriptDirectory

from lance_etl.reconciler.migrations import AlembicMigrationRunner

REPOSITORY_ROOT: Path = Path(__file__).resolve().parent.parent


def read_repository_file(relative_path: str) -> str:
    """Read one UTF-8 repository file.

    Args:
        relative_path: Path relative to the repository root.

    Returns:
        File contents.
    """
    return (REPOSITORY_ROOT / relative_path).read_text(encoding="utf-8")


def test_python_and_rust_release_versions_are_exact() -> None:
    """Production language and Lance inputs are exact and represented in lockfiles."""
    project: dict[str, Any] = tomllib.loads(read_repository_file("pyproject.toml"))
    assert "pylance==8.0.0" in project["project"]["dependencies"]
    assert "pyspark==4.0.1" in project["project"]["dependencies"]
    assert 'specifier = "==8.0.0"' in read_repository_file("uv.lock")
    assert 'name = "pyspark"\nversion = "4.0.1"' in read_repository_file("uv.lock")
    toolchain: dict[str, Any] = tomllib.loads(read_repository_file("rust-toolchain.toml"))
    assert toolchain["toolchain"] == {
        "channel": "1.91.0",
        "components": ["clippy", "rustfmt"],
        "profile": "minimal",
    }


def test_catalog_rebuild_uses_fenced_reconciler_work() -> None:
    """Rebuild documentation enqueues deterministic work through the restricted repair surface."""
    runbook: str = read_repository_file("docs/production-release.md")
    assert "lance-etl-reconcile repair" in runbook
    assert "--action rebuild" in runbook
    assert "--request-id" in runbook
    assert "--dry-run" in runbook
    assert "Repair never edits the active" in runbook
    assert "--action rollback" not in runbook


def test_reconciler_release_contract_is_local_first() -> None:
    """The local process requires no service images, manifests, or remote prewarm."""
    assert not (REPOSITORY_ROOT / "containers").exists()
    assert not (REPOSITORY_ROOT / "deploy").exists()
    runbook: str = read_repository_file("docs/production-release.md")
    assert "lance-etl-reconcile run-once" in runbook
    assert "local Spark" in runbook
    assert "Search is optional" in runbook
    assert "SEARCH_API_LOCAL_MODE=true" in runbook
    assert "cargo run --locked" in runbook


def test_control_plane_migration_uses_the_configured_postgres_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """The local migration command gives Alembic the exact PostgreSQL URL.

    Args:
        monkeypatch: Scoped Alembic replacement fixture.
    """
    production_url: str = (
        "postgresql+psycopg://migrator:p%25word@postgres.example.com/lance?"
        "sslmode=verify-full&sslrootcert=/var/run/secrets/lance-etl-migrator/database-ca.pem"
    )
    captured: dict[str, str] = {}

    def capture_upgrade(config: Config, revision: str) -> None:
        """Capture the URL that the runner passes to Alembic.

        Args:
            config: Prepared Alembic configuration.
            revision: Requested migration revision.
        """
        captured["url"] = config.get_main_option("sqlalchemy.url")
        captured["script_location"] = config.get_main_option("script_location")
        captured["revision"] = revision

    monkeypatch.setattr(alembic_command, "upgrade", capture_upgrade)
    AlembicMigrationRunner(production_url, REPOSITORY_ROOT).migrate()
    assert captured == {
        "url": production_url,
        "script_location": str(REPOSITORY_ROOT / "migrations"),
        "revision": "head",
    }
    runbook: str = read_repository_file("docs/production-release.md")
    assert "lance-etl-reconcile migrate" in runbook


def test_control_plane_migration_is_one_resettable_baseline() -> None:
    """The control plane has one self-contained baseline and no forward revisions."""
    configuration: Config = Config(str(REPOSITORY_ROOT / "alembic.ini"))
    configuration.set_main_option("script_location", str(REPOSITORY_ROOT / "migrations"))
    scripts: ScriptDirectory = ScriptDirectory.from_config(configuration)
    revisions: list[Script] = list(scripts.walk_revisions())

    assert [path.name for path in (REPOSITORY_ROOT / "migrations" / "versions").glob("*.py")] == [
        "0001_control_plane.py"
    ]
    assert len(revisions) == 1
    assert revisions[0].revision == "0001_control_plane"
    assert revisions[0].down_revision is None
    assert scripts.get_heads() == ["0001_control_plane"]
    assert "must be dropped and recreated" in read_repository_file("migrations/README.md")
