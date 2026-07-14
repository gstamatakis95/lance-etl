"""SQLAlchemy Core definitions for the three application tables."""

from __future__ import annotations

import sqlalchemy as sa

metadata: sa.MetaData = sa.MetaData()
"""Control-plane metadata containing exactly three application tables."""

SOURCE_KINDS: tuple[str, ...] = ("BASELINE", "APPEND", "TRUSTED_MAINTENANCE")
SOURCE_STATES: tuple[str, ...] = ("SEALED", "COMPLETE", "BLOCKED")
WORK_KINDS: tuple[str, ...] = ("INGEST", "SERVE", "REBUILD")
WORK_STATES: tuple[str, ...] = ("PENDING", "RUNNING", "RETRY_WAIT", "SUCCEEDED", "BLOCKED")
WORK_PHASES: tuple[str, ...] = ("INGEST", "MAINTAIN", "INDEX", "VALIDATE", "PREWARM", "PUBLISH")


def quoted_values(values: tuple[str, ...]) -> str:
    """Render closed string values for a SQL CHECK expression.

    Args:
        values: Trusted code-owned values.

    Returns:
        Comma-separated single-quoted literals.
    """
    return ", ".join(f"'{value}'" for value in values)


source_windows: sa.Table = sa.Table(
    "source_windows",
    metadata,
    sa.Column("window_seq", sa.BigInteger(), sa.Identity(), primary_key=True),
    sa.Column("table_uuid", sa.Uuid(as_uuid=True), nullable=False),
    sa.Column("snapshot_id", sa.BigInteger(), nullable=False),
    sa.Column("parent_snapshot_id", sa.BigInteger(), nullable=True),
    sa.Column("iceberg_sequence_number", sa.BigInteger(), nullable=False),
    sa.Column("kind", sa.String(32), nullable=False),
    sa.Column("state", sa.String(16), nullable=False, server_default="SEALED"),
    sa.Column("error_code", sa.String(128), nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    sa.UniqueConstraint("table_uuid", "snapshot_id", name="uq_source_windows_table_snapshot"),
    sa.UniqueConstraint("table_uuid", "iceberg_sequence_number", name="uq_source_windows_table_sequence"),
    sa.CheckConstraint(f"kind IN ({quoted_values(SOURCE_KINDS)})", name="ck_source_windows_kind"),
    sa.CheckConstraint(f"state IN ({quoted_values(SOURCE_STATES)})", name="ck_source_windows_state"),
    sa.CheckConstraint("snapshot_id >= 0", name="ck_source_windows_snapshot_nonnegative"),
    sa.CheckConstraint("iceberg_sequence_number >= 0", name="ck_source_windows_sequence_nonnegative"),
    sa.CheckConstraint(
        "kind = 'BASELINE' OR parent_snapshot_id IS NOT NULL",
        name="ck_source_windows_parent_required",
    ),
)

targets: sa.Table = sa.Table(
    "targets",
    metadata,
    sa.Column("target_id", sa.Uuid(as_uuid=True), primary_key=True),
    sa.Column("tenant_id", sa.String(128), nullable=False),
    sa.Column("namespace", sa.String(128), nullable=False),
    sa.Column("org_id", sa.String(128), nullable=False),
    sa.Column("ingest_lance_uri", sa.Text(), nullable=False),
    sa.Column("served_lance_uri", sa.Text(), nullable=True),
    sa.Column("profile_id", sa.String(128), nullable=False),
    sa.Column("last_applied_window_seq", sa.BigInteger(), nullable=True),
    sa.Column("last_applied_lance_version", sa.BigInteger(), nullable=True),
    sa.Column("served_lance_version", sa.BigInteger(), nullable=True),
    sa.Column("fence_epoch", sa.BigInteger(), nullable=False, server_default="0"),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    sa.UniqueConstraint("tenant_id", "namespace", "org_id", name="uq_targets_identity"),
    sa.CheckConstraint("tenant_id ~ '^[A-Za-z0-9_-]{1,128}$'", name="ck_targets_tenant_id"),
    sa.CheckConstraint("namespace ~ '^[A-Za-z0-9_-]{1,128}$'", name="ck_targets_namespace"),
    sa.CheckConstraint("org_id ~ '^[A-Za-z0-9_-]{1,128}$'", name="ck_targets_org_id"),
    sa.CheckConstraint("profile_id ~ '^[A-Za-z0-9_-]{1,128}$'", name="ck_targets_profile_id"),
    sa.CheckConstraint("fence_epoch >= 0", name="ck_targets_fence_nonnegative"),
    sa.CheckConstraint(
        "(served_lance_uri IS NULL) = (served_lance_version IS NULL)",
        name="ck_targets_served_tuple",
    ),
)

target_work: sa.Table = sa.Table(
    "target_work",
    metadata,
    sa.Column("work_id", sa.Uuid(as_uuid=True), primary_key=True),
    sa.Column("target_id", sa.Uuid(as_uuid=True), sa.ForeignKey("targets.target_id"), nullable=False),
    sa.Column(
        "source_window_seq",
        sa.BigInteger(),
        sa.ForeignKey("source_windows.window_seq"),
        nullable=True,
    ),
    sa.Column("kind", sa.String(16), nullable=False),
    sa.Column("state", sa.String(16), nullable=False, server_default="PENDING"),
    sa.Column("phase", sa.String(16), nullable=False),
    sa.Column("lease_token", sa.Uuid(as_uuid=True), nullable=True),
    sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("attempt_count", sa.BigInteger(), nullable=False, server_default="0"),
    sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    sa.Column("data_lance_version", sa.BigInteger(), nullable=True),
    sa.Column("indexed_lance_version", sa.BigInteger(), nullable=True),
    sa.Column("candidate_lance_uri", sa.Text(), nullable=True),
    sa.Column("expected_ingest_lance_uri", sa.Text(), nullable=False),
    sa.Column("expected_served_lance_uri", sa.Text(), nullable=True),
    sa.Column("expected_served_lance_version", sa.BigInteger(), nullable=True),
    sa.Column("source_applied_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("source_row_count", sa.BigInteger(), nullable=True),
    sa.Column("source_digest", sa.LargeBinary(32), nullable=True),
    sa.Column("artifact_manifest_uri", sa.Text(), nullable=True),
    sa.Column("artifact_digest", sa.LargeBinary(32), nullable=True),
    sa.Column("error_code", sa.String(128), nullable=True),
    sa.Column("error_message", sa.String(2000), nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    sa.UniqueConstraint("target_id", "source_window_seq", "kind", name="uq_target_work_source_kind"),
    sa.CheckConstraint(f"kind IN ({quoted_values(WORK_KINDS)})", name="ck_target_work_kind"),
    sa.CheckConstraint(f"state IN ({quoted_values(WORK_STATES)})", name="ck_target_work_state"),
    sa.CheckConstraint(f"phase IN ({quoted_values(WORK_PHASES)})", name="ck_target_work_phase"),
    sa.CheckConstraint("kind != 'INGEST' OR source_window_seq IS NOT NULL", name="ck_target_work_ingest_source"),
    sa.CheckConstraint(
        "(state = 'RUNNING') = (lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)",
        name="ck_target_work_lease_state",
    ),
    sa.CheckConstraint("attempt_count >= 0", name="ck_target_work_attempt_nonnegative"),
    sa.CheckConstraint("source_row_count IS NULL OR source_row_count >= 0", name="ck_target_work_rows_nonnegative"),
    sa.CheckConstraint(
        "source_digest IS NULL OR octet_length(source_digest) = 32",
        name="ck_target_work_source_digest",
    ),
    sa.CheckConstraint(
        "artifact_digest IS NULL OR octet_length(artifact_digest) = 32",
        name="ck_target_work_artifact_digest",
    ),
    sa.CheckConstraint(
        "(expected_served_lance_uri IS NULL) = (expected_served_lance_version IS NULL)",
        name="ck_target_work_expected_served_tuple",
    ),
)

sa.Index(
    "uq_target_work_running_target",
    target_work.c.target_id,
    unique=True,
    postgresql_where=target_work.c.state == "RUNNING",
)
sa.Index(
    "uq_target_work_open_serve",
    target_work.c.target_id,
    unique=True,
    postgresql_where=sa.and_(
        target_work.c.kind == "SERVE",
        target_work.c.state.in_(("PENDING", "RUNNING", "RETRY_WAIT")),
    ),
)
sa.Index(
    "ix_target_work_claim",
    target_work.c.state,
    target_work.c.next_attempt_at,
    target_work.c.target_id,
)
sa.Index(
    "ix_target_work_ingest_order",
    target_work.c.target_id,
    target_work.c.source_window_seq,
    postgresql_where=sa.and_(
        target_work.c.kind == "INGEST",
        target_work.c.state != "SUCCEEDED",
    ),
)
