"""Local Alembic migration wiring for the PostgreSQL control plane."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from alembic import command
from alembic.config import Config

from lance_etl.reconciler.config import control_plane_database_url


@dataclass(frozen=True, slots=True)
class AlembicMigrationRunner:
    """Upgrade one configured PostgreSQL control plane from a source checkout."""

    database_url: str
    repository_root: Path

    def migrate(self) -> None:
        """Upgrade the configured database to the current Alembic head."""
        configuration: Config = Config(str(self.repository_root / "alembic.ini"))
        configuration.set_main_option("script_location", str(self.repository_root / "migrations"))
        configuration.set_main_option("sqlalchemy.url", self.database_url.replace("%", "%%"))
        command.upgrade(configuration, "head")


def build_runtime_migrator() -> AlembicMigrationRunner:
    """Build a migration runner without starting Spark or the reconciler runtime.

    Returns:
        Migration runner using the local environment and checked-in revisions.
    """
    repository_root: Path = Path(__file__).resolve().parents[3]
    return AlembicMigrationRunner(control_plane_database_url(), repository_root)
