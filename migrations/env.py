"""Alembic environment for the durable PostgreSQL control plane."""

from __future__ import annotations

from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from lance_etl.state.tables import metadata

config = context.config
"""Active Alembic configuration."""

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = metadata
"""SQLAlchemy Core metadata used by migration autogeneration."""


def run_migrations_offline() -> None:
    """Run migrations without creating a live database connection."""
    url: str = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations against a live psycopg connection."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
