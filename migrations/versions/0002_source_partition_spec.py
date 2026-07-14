"""Persist exact source partition specifications and rejected audit tips.

Revision ID: 0002_source_partition_spec
Revises: 0001_control_plane
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002_source_partition_spec"
down_revision: str | None = "0001_control_plane"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add non-fabricated spec identity and allow durable rejected source rows.

    Raises:
        RuntimeError: If source audit rows need an operator-supplied spec backfill.
    """
    op.add_column("source_windows", sa.Column("partition_spec_id", sa.Integer(), nullable=True))
    connection = op.get_bind()
    existing = int(connection.scalar(sa.text("SELECT count(*) FROM source_windows")) or 0)
    if existing:
        raise RuntimeError(
            "source_windows contains rows without partition_spec_id. Backfill exact Iceberg spec IDs before upgrade"
        )
    op.alter_column("source_windows", "partition_spec_id", nullable=False)
    op.create_check_constraint("ck_source_windows_spec_nonnegative", "source_windows", "partition_spec_id >= 0")
    op.drop_constraint("ck_source_windows_kind", "source_windows", type_="check")
    op.create_check_constraint(
        "ck_source_windows_kind",
        "source_windows",
        "kind IN ('BASELINE', 'APPEND', 'TRUSTED_MAINTENANCE', 'REJECTED')",
    )


def downgrade() -> None:
    """Remove rejected-kind support and exact partition-spec persistence."""
    op.drop_constraint("ck_source_windows_kind", "source_windows", type_="check")
    op.create_check_constraint(
        "ck_source_windows_kind",
        "source_windows",
        "kind IN ('BASELINE', 'APPEND', 'TRUSTED_MAINTENANCE')",
    )
    op.drop_constraint("ck_source_windows_spec_nonnegative", "source_windows", type_="check")
    op.drop_column("source_windows", "partition_spec_id")
