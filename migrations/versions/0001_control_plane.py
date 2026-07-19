"""Create the PostgreSQL dataset control plane.

Revision ID: 0001_control_plane
Revises:
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql.selectable import TableClause

revision: str = "0001_control_plane"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

DEFAULT_SPEC_ID: uuid.UUID = uuid.UUID("00000000-0000-0000-0000-000000000001")
DEFAULT_SPEC_REVISION_ID: uuid.UUID = uuid.UUID("00000000-0000-0000-0000-000000000101")
DEFAULT_CONFIGURATION_DIGEST_HEX: str = "7f22b7fd26dac4aa832138372f9a6ea1e7c0d751d2509f463d6b7aaca0224bbe"
DEFAULT_FIELD_IDS: dict[str, uuid.UUID] = {
    "vector_id": uuid.UUID("00000000-0000-0000-0000-000000001001"),
    "event_timestamp": uuid.UUID("00000000-0000-0000-0000-000000001002"),
    "vector": uuid.UUID("00000000-0000-0000-0000-000000001003"),
    "text": uuid.UUID("00000000-0000-0000-0000-000000001004"),
    "cluster": uuid.UUID("00000000-0000-0000-0000-000000001005"),
    "ttl": uuid.UUID("00000000-0000-0000-0000-000000001006"),
    "lance_etl_window_seq": uuid.UUID("00000000-0000-0000-0000-000000001007"),
    "lance_etl_source_sequence": uuid.UUID("00000000-0000-0000-0000-000000001008"),
    "lance_etl_event_digest": uuid.UUID("00000000-0000-0000-0000-000000001009"),
    "is_deleted": uuid.UUID("00000000-0000-0000-0000-000000001010"),
}
DEFAULT_INDEX_IDS: dict[str, uuid.UUID] = {
    "vector_idx": uuid.UUID("00000000-0000-0000-0000-000000002001"),
    "text_fts_idx": uuid.UUID("00000000-0000-0000-0000-000000002002"),
    "cluster_idx": uuid.UUID("00000000-0000-0000-0000-000000002003"),
    "event_timestamp_idx": uuid.UUID("00000000-0000-0000-0000-000000002004"),
    "event_timestamp_zonemap_idx": uuid.UUID("00000000-0000-0000-0000-000000002005"),
    "is_deleted_bitmap_idx": uuid.UUID("00000000-0000-0000-0000-000000002006"),
}


def upgrade() -> None:
    """Create the relational control plane and deterministic default specification."""
    create_settings_and_spec_tables()
    create_field_and_index_tables()
    create_source_and_dataset_tables()
    create_work_and_publication_tables()
    seed_defaults()
    create_lifecycle_triggers()


def create_settings_and_spec_tables() -> None:
    """Create process settings and immutable dataset specification tables."""
    op.create_table(
        "reconciler_settings",
        sa.Column("singleton_id", sa.SmallInteger(), primary_key=True, server_default="1"),
        sa.Column("poll_interval_seconds", sa.Integer(), nullable=False, server_default="30"),
        sa.Column("claim_batch_size", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("max_drain_batches", sa.Integer(), nullable=False, server_default="64"),
        sa.Column("max_snapshots_per_plan", sa.Integer(), nullable=False, server_default="32"),
        sa.Column("lease_duration_seconds", sa.Integer(), nullable=False, server_default="900"),
        sa.Column("lease_heartbeat_seconds", sa.Integer(), nullable=False, server_default="300"),
        sa.Column("retry_base_delay_seconds", sa.Integer(), nullable=False, server_default="30"),
        sa.Column("retry_max_delay_seconds", sa.Integer(), nullable=False, server_default="1800"),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="10"),
        sa.Column("max_due_work", sa.Integer(), nullable=False, server_default="10000"),
        sa.Column("max_open_work_age_seconds", sa.BigInteger(), nullable=False, server_default="3600"),
        sa.Column("max_retention_age_seconds", sa.BigInteger(), nullable=False, server_default="86400"),
        sa.Column("audit_retention_seconds", sa.BigInteger(), nullable=False, server_default="2592000"),
        sa.Column("cleanup_batch_size", sa.Integer(), nullable=False, server_default="128"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("singleton_id = 1", name="ck_reconciler_settings_singleton"),
        sa.CheckConstraint("poll_interval_seconds > 0", name="ck_reconciler_settings_poll_positive"),
        sa.CheckConstraint("claim_batch_size > 0", name="ck_reconciler_settings_claim_positive"),
        sa.CheckConstraint("max_drain_batches > 0", name="ck_reconciler_settings_drain_positive"),
        sa.CheckConstraint("max_snapshots_per_plan > 0", name="ck_reconciler_settings_plan_positive"),
        sa.CheckConstraint("lease_duration_seconds > 0", name="ck_reconciler_settings_lease_positive"),
        sa.CheckConstraint(
            "lease_heartbeat_seconds > 0 AND lease_heartbeat_seconds < lease_duration_seconds",
            name="ck_reconciler_settings_heartbeat",
        ),
        sa.CheckConstraint("retry_base_delay_seconds > 0", name="ck_reconciler_settings_retry_base_positive"),
        sa.CheckConstraint(
            "retry_max_delay_seconds >= retry_base_delay_seconds",
            name="ck_reconciler_settings_retry_range",
        ),
        sa.CheckConstraint("max_attempts > 0", name="ck_reconciler_settings_attempts_positive"),
        sa.CheckConstraint("max_due_work > 0", name="ck_reconciler_settings_due_positive"),
        sa.CheckConstraint("max_open_work_age_seconds > 0", name="ck_reconciler_settings_open_age_positive"),
        sa.CheckConstraint("max_retention_age_seconds > 0", name="ck_reconciler_settings_retention_age_positive"),
        sa.CheckConstraint("audit_retention_seconds > 0", name="ck_reconciler_settings_audit_positive"),
        sa.CheckConstraint("cleanup_batch_size > 0", name="ck_reconciler_settings_cleanup_positive"),
    )
    op.create_table(
        "dataset_specs",
        sa.Column("spec_id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(128), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("name", name="uq_dataset_specs_name"),
        sa.CheckConstraint("name ~ '^[A-Za-z][A-Za-z0-9_-]{0,127}$'", name="ck_dataset_specs_name"),
    )
    op.create_table(
        "dataset_spec_revisions",
        sa.Column("spec_revision_id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("spec_id", sa.Uuid(as_uuid=True), sa.ForeignKey("dataset_specs.spec_id"), nullable=False),
        sa.Column("revision_number", sa.BigInteger(), nullable=False),
        sa.Column("state", sa.String(16), nullable=False, server_default="DRAFT"),
        sa.Column(
            "supersedes_revision_id",
            sa.Uuid(as_uuid=True),
            nullable=True,
        ),
        sa.Column("configuration_digest", sa.LargeBinary(32), nullable=False),
        sa.Column("ingest_shuffle_partitions", sa.Integer(), nullable=False),
        sa.Column("merge_rows_per_chunk", sa.BigInteger(), nullable=False),
        sa.Column("merge_batch_bytes", sa.BigInteger(), nullable=False),
        sa.Column("write_rows_per_fragment", sa.BigInteger(), nullable=False),
        sa.Column("compaction_enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("compaction_mode", sa.String(32), nullable=False, server_default="try_binary_copy"),
        sa.Column("target_rows_per_fragment", sa.BigInteger(), nullable=False),
        sa.Column("max_source_fragments", sa.Integer(), nullable=True),
        sa.Column("compaction_threads", sa.Integer(), nullable=True),
        sa.Column("defer_index_remap", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("materialize_deletions", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("materialize_deletions_threshold", sa.Numeric(5, 4), nullable=False),
        sa.Column("cleanup_older_than_seconds", sa.BigInteger(), nullable=True),
        sa.Column("retain_versions", sa.Integer(), nullable=True),
        sa.Column("fragments_per_index_task", sa.Integer(), nullable=False),
        sa.Column("max_index_deltas", sa.Integer(), nullable=False),
        sa.Column("max_stale_replans", sa.Integer(), nullable=False),
        sa.Column("prewarm_required", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("retained_publications", sa.Integer(), nullable=False),
        sa.Column("artifact_retention_seconds", sa.BigInteger(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("activated_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("spec_id", "revision_number", name="uq_dataset_spec_revisions_number"),
        sa.UniqueConstraint("spec_id", "configuration_digest", name="uq_dataset_spec_revisions_digest"),
        sa.UniqueConstraint("spec_id", "spec_revision_id", name="uq_dataset_spec_revisions_spec_revision"),
        sa.ForeignKeyConstraint(
            ["spec_id", "supersedes_revision_id"],
            ["dataset_spec_revisions.spec_id", "dataset_spec_revisions.spec_revision_id"],
            name="fk_dataset_spec_revisions_supersedes_same_spec",
        ),
        sa.CheckConstraint("state IN ('DRAFT', 'ACTIVE', 'RETIRED')", name="ck_dataset_spec_revisions_state"),
        sa.CheckConstraint("revision_number > 0", name="ck_dataset_spec_revisions_number_positive"),
        sa.CheckConstraint("octet_length(configuration_digest) = 32", name="ck_dataset_spec_revisions_digest"),
        sa.CheckConstraint(
            "supersedes_revision_id IS NULL OR supersedes_revision_id != spec_revision_id",
            name="ck_dataset_spec_revisions_supersedes_other",
        ),
        sa.CheckConstraint(
            "(state = 'DRAFT') = (activated_at IS NULL)",
            name="ck_dataset_spec_revisions_activation",
        ),
        sa.CheckConstraint("ingest_shuffle_partitions > 0", name="ck_dataset_spec_revisions_shuffle_positive"),
        sa.CheckConstraint("merge_rows_per_chunk > 0", name="ck_dataset_spec_revisions_merge_rows_positive"),
        sa.CheckConstraint("merge_batch_bytes > 0", name="ck_dataset_spec_revisions_merge_bytes_positive"),
        sa.CheckConstraint("write_rows_per_fragment > 0", name="ck_dataset_spec_revisions_write_rows_positive"),
        sa.CheckConstraint(
            "compaction_mode IN ('try_binary_copy', 'reencode')",
            name="ck_dataset_spec_revisions_compaction_mode",
        ),
        sa.CheckConstraint("target_rows_per_fragment > 0", name="ck_dataset_spec_revisions_target_rows_positive"),
        sa.CheckConstraint(
            "max_source_fragments IS NULL OR max_source_fragments > 0",
            name="ck_dataset_spec_revisions_source_fragments_positive",
        ),
        sa.CheckConstraint(
            "compaction_threads IS NULL OR compaction_threads > 0",
            name="ck_dataset_spec_revisions_threads_positive",
        ),
        sa.CheckConstraint(
            "materialize_deletions_threshold >= 0 AND materialize_deletions_threshold <= 1",
            name="ck_dataset_spec_revisions_deletion_threshold",
        ),
        sa.CheckConstraint(
            "cleanup_older_than_seconds IS NULL OR cleanup_older_than_seconds >= 21600",
            name="ck_dataset_spec_revisions_cleanup_horizon",
        ),
        sa.CheckConstraint(
            "retain_versions IS NULL OR retain_versions > 0",
            name="ck_dataset_spec_revisions_retain_versions_positive",
        ),
        sa.CheckConstraint("fragments_per_index_task > 0", name="ck_dataset_spec_revisions_index_task_positive"),
        sa.CheckConstraint("max_index_deltas > 0", name="ck_dataset_spec_revisions_index_deltas_positive"),
        sa.CheckConstraint("max_stale_replans >= 0", name="ck_dataset_spec_revisions_replans_nonnegative"),
        sa.CheckConstraint("retained_publications > 0", name="ck_dataset_spec_revisions_publications_positive"),
        sa.CheckConstraint(
            "artifact_retention_seconds > 0",
            name="ck_dataset_spec_revisions_artifact_retention_positive",
        ),
    )
    op.create_index(
        "uq_dataset_spec_revisions_active",
        "dataset_spec_revisions",
        ["spec_id"],
        unique=True,
        postgresql_where=sa.text("state = 'ACTIVE'"),
    )


def create_field_and_index_tables() -> None:
    """Create typed field contracts and per-index option tables."""
    op.create_table(
        "dataset_fields",
        sa.Column("field_id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column(
            "spec_revision_id",
            sa.Uuid(as_uuid=True),
            sa.ForeignKey("dataset_spec_revisions.spec_revision_id"),
            nullable=False,
        ),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("target_name", sa.String(128), nullable=False),
        sa.Column("role", sa.String(16), nullable=False),
        sa.Column("source_kind", sa.String(16), nullable=False),
        sa.Column("source_column", sa.String(128), nullable=False),
        sa.Column("source_key", sa.String(128), nullable=True),
        sa.Column("data_type", sa.String(128), nullable=False),
        sa.Column("nullable", sa.Boolean(), nullable=False),
        sa.Column("required_on_upsert", sa.Boolean(), nullable=False),
        sa.Column("vector_dimension", sa.Integer(), nullable=True),
        sa.UniqueConstraint("spec_revision_id", "ordinal", name="uq_dataset_fields_ordinal"),
        sa.UniqueConstraint("spec_revision_id", "target_name", name="uq_dataset_fields_target_name"),
        sa.UniqueConstraint("spec_revision_id", "field_id", name="uq_dataset_fields_revision_field"),
        sa.CheckConstraint("ordinal >= 0", name="ck_dataset_fields_ordinal_nonnegative"),
        sa.CheckConstraint(
            "target_name ~ '^[A-Za-z_][A-Za-z0-9_]{0,127}$'",
            name="ck_dataset_fields_target_name",
        ),
        sa.CheckConstraint(
            "role IN ('KEY', 'EVENT_TIME', 'VECTOR', 'TEXT', 'METADATA', 'TTL', 'TOMBSTONE', 'LINEAGE')",
            name="ck_dataset_fields_role",
        ),
        sa.CheckConstraint(
            "source_kind IN ('DIRECT', 'MAP_KEY', 'DERIVED')",
            name="ck_dataset_fields_source_kind",
        ),
        sa.CheckConstraint(
            "source_column ~ '^[A-Za-z_][A-Za-z0-9_]{0,127}$'",
            name="ck_dataset_fields_source_column",
        ),
        sa.CheckConstraint(
            "source_key IS NULL OR source_key ~ '^[A-Za-z_][A-Za-z0-9_]{0,127}$'",
            name="ck_dataset_fields_source_key",
        ),
        sa.CheckConstraint(
            "(source_kind = 'MAP_KEY') = (source_key IS NOT NULL)",
            name="ck_dataset_fields_map_key",
        ),
        sa.CheckConstraint(
            "source_kind <> 'MAP_KEY' OR source_key = target_name",
            name="ck_dataset_fields_map_key_target",
        ),
        sa.CheckConstraint(
            "(role = 'KEY' AND target_name = 'vector_id' AND source_kind = 'DIRECT' "
            "AND source_column = 'vector_id') OR "
            "(role = 'EVENT_TIME' AND target_name = 'event_timestamp' AND source_kind = 'DIRECT' "
            "AND source_column = 'event_timestamp') OR "
            "(role = 'VECTOR' AND source_kind = 'MAP_KEY' AND source_column = 'vectors') OR "
            "(role = 'TEXT' AND source_kind = 'MAP_KEY' AND source_column = 'texts') OR "
            "(role = 'METADATA' AND source_kind = 'MAP_KEY' AND source_column = 'metadata') OR "
            "(role = 'TTL' AND target_name = 'ttl' AND source_kind = 'DIRECT' AND source_column = 'ttl') OR "
            "(role = 'TOMBSTONE' AND target_name = 'is_deleted' AND source_kind = 'DERIVED' "
            "AND source_column = target_name) OR "
            "(role = 'LINEAGE' AND target_name IN "
            "('lance_etl_window_seq', 'lance_etl_source_sequence', 'lance_etl_event_digest') "
            "AND source_kind = 'DERIVED' AND source_column = target_name)",
            name="ck_dataset_fields_source_contract",
        ),
        sa.CheckConstraint("length(data_type) > 0", name="ck_dataset_fields_data_type"),
        sa.CheckConstraint(
            "role = 'VECTOR' OR "
            "(role = 'KEY' AND data_type = 'string') OR "
            "(role = 'EVENT_TIME' AND data_type = 'timestamp[us,UTC]') OR "
            "(role = 'TEXT' AND data_type = 'string') OR "
            "(role = 'METADATA' AND data_type = 'string') OR "
            "(role = 'TTL' AND data_type = 'duration[s]') OR "
            "(role = 'TOMBSTONE' AND data_type = 'bool') OR "
            "(role = 'LINEAGE' AND target_name IN ('lance_etl_window_seq', 'lance_etl_source_sequence') "
            "AND data_type = 'int64') OR "
            "(role = 'LINEAGE' AND target_name = 'lance_etl_event_digest' AND data_type = 'binary[32]')",
            name="ck_dataset_fields_type_contract",
        ),
        sa.CheckConstraint(
            "(role <> 'VECTOR' AND vector_dimension IS NULL) OR "
            "(role = 'VECTOR' AND vector_dimension >= 8 AND vector_dimension % 8 = 0 "
            "AND data_type = 'fixed_size_list<float32,' || vector_dimension::text || '>')",
            name="ck_dataset_fields_vector_shape",
        ),
        sa.CheckConstraint(
            "(role = 'KEY' AND NOT nullable AND required_on_upsert) OR "
            "(role IN ('EVENT_TIME', 'VECTOR') AND nullable AND required_on_upsert) OR "
            "(role IN ('TEXT', 'METADATA', 'TTL') AND nullable AND NOT required_on_upsert) OR "
            "(role IN ('TOMBSTONE', 'LINEAGE') AND NOT nullable AND NOT required_on_upsert)",
            name="ck_dataset_fields_flags_contract",
        ),
    )
    op.create_index(
        "uq_dataset_fields_singleton_role",
        "dataset_fields",
        ["spec_revision_id", "role"],
        unique=True,
        postgresql_where=sa.text("role IN ('KEY', 'EVENT_TIME', 'TTL', 'TOMBSTONE')"),
    )
    op.create_table(
        "index_definitions",
        sa.Column("index_definition_id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column(
            "spec_revision_id",
            sa.Uuid(as_uuid=True),
            sa.ForeignKey("dataset_spec_revisions.spec_revision_id"),
            nullable=False,
        ),
        sa.Column("field_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("index_name", sa.String(128), nullable=False),
        sa.Column("index_type", sa.String(16), nullable=False),
        sa.ForeignKeyConstraint(
            ["spec_revision_id", "field_id"],
            ["dataset_fields.spec_revision_id", "dataset_fields.field_id"],
            name="fk_index_definitions_revision_field",
        ),
        sa.UniqueConstraint("spec_revision_id", "ordinal", name="uq_index_definitions_ordinal"),
        sa.UniqueConstraint("spec_revision_id", "index_name", name="uq_index_definitions_name"),
        sa.UniqueConstraint(
            "spec_revision_id",
            "field_id",
            "index_type",
            name="uq_index_definitions_field_type",
        ),
        sa.UniqueConstraint("index_definition_id", "index_type", name="uq_index_definitions_id_type"),
        sa.UniqueConstraint(
            "spec_revision_id",
            "index_definition_id",
            name="uq_index_definitions_revision_id",
        ),
        sa.UniqueConstraint(
            "spec_revision_id",
            "index_definition_id",
            "index_type",
            name="uq_index_definitions_revision_id_type",
        ),
        sa.CheckConstraint("ordinal >= 0", name="ck_index_definitions_ordinal_nonnegative"),
        sa.CheckConstraint(
            "index_name ~ '^[A-Za-z_][A-Za-z0-9_]{0,127}$'",
            name="ck_index_definitions_name",
        ),
        sa.CheckConstraint(
            "index_type IN ('IVF_RQ', 'BTREE', 'BITMAP', 'ZONEMAP', 'INVERTED')",
            name="ck_index_definitions_type",
        ),
    )
    op.create_table(
        "vector_index_options",
        sa.Column("index_definition_id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("index_type", sa.String(16), nullable=False, server_default="IVF_RQ"),
        sa.Column("metric", sa.String(16), nullable=False),
        sa.Column("num_partitions", sa.Integer(), nullable=True),
        sa.Column("minimum_partitions", sa.Integer(), nullable=False),
        sa.Column("maximum_partitions", sa.Integer(), nullable=False),
        sa.Column("target_rows_per_partition", sa.Integer(), nullable=False),
        sa.Column("minimum_rows", sa.BigInteger(), nullable=False),
        sa.Column("num_bits", sa.Integer(), nullable=False),
        sa.Column("streaming_sample_rate", sa.Integer(), nullable=False),
        sa.Column("streaming_refine_passes", sa.Integer(), nullable=False),
        sa.Column("retrain_growth_factor", sa.Numeric(8, 4), nullable=False),
        sa.ForeignKeyConstraint(
            ["index_definition_id", "index_type"],
            ["index_definitions.index_definition_id", "index_definitions.index_type"],
            name="fk_vector_index_options_definition_type",
        ),
        sa.CheckConstraint("index_type = 'IVF_RQ'", name="ck_vector_index_options_type"),
        sa.CheckConstraint("metric IN ('l2', 'cosine', 'dot')", name="ck_vector_index_options_metric"),
        sa.CheckConstraint(
            "num_partitions IS NULL OR num_partitions > 0",
            name="ck_vector_index_options_partitions",
        ),
        sa.CheckConstraint("minimum_partitions > 0", name="ck_vector_index_options_min_partitions_positive"),
        sa.CheckConstraint(
            "maximum_partitions >= minimum_partitions",
            name="ck_vector_index_options_partition_range",
        ),
        sa.CheckConstraint(
            "num_partitions IS NULL OR (num_partitions >= minimum_partitions AND num_partitions <= maximum_partitions)",
            name="ck_vector_index_options_explicit_partition_range",
        ),
        sa.CheckConstraint(
            "target_rows_per_partition > 0",
            name="ck_vector_index_options_target_rows_positive",
        ),
        sa.CheckConstraint("minimum_rows >= 0", name="ck_vector_index_options_minimum_rows_nonnegative"),
        sa.CheckConstraint("num_bits > 0", name="ck_vector_index_options_num_bits_positive"),
        sa.CheckConstraint(
            "streaming_sample_rate > 0",
            name="ck_vector_index_options_sample_rate_positive",
        ),
        sa.CheckConstraint(
            "streaming_refine_passes >= 0",
            name="ck_vector_index_options_refine_passes_nonnegative",
        ),
        sa.CheckConstraint("retrain_growth_factor > 1", name="ck_vector_index_options_growth_factor"),
    )
    op.create_table(
        "fts_index_options",
        sa.Column("index_definition_id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("index_type", sa.String(16), nullable=False, server_default="INVERTED"),
        sa.Column("with_position", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("base_tokenizer", sa.String(64), nullable=True),
        sa.Column("language", sa.String(32), nullable=True),
        sa.Column("max_unindexed_fragments", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["index_definition_id", "index_type"],
            ["index_definitions.index_definition_id", "index_definitions.index_type"],
            name="fk_fts_index_options_definition_type",
        ),
        sa.CheckConstraint("index_type = 'INVERTED'", name="ck_fts_index_options_type"),
        sa.CheckConstraint(
            "base_tokenizer IS NULL OR length(base_tokenizer) > 0",
            name="ck_fts_index_options_tokenizer",
        ),
        sa.CheckConstraint("language IS NULL OR length(language) > 0", name="ck_fts_index_options_language"),
        sa.CheckConstraint(
            "max_unindexed_fragments >= 0",
            name="ck_fts_index_options_unindexed_nonnegative",
        ),
    )


def create_source_and_dataset_tables() -> None:
    """Create registered Iceberg sources, logical datasets, and source snapshots."""
    op.create_table(
        "iceberg_sources",
        sa.Column("source_id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("source_name", sa.String(128), nullable=False),
        sa.Column("spark_catalog", sa.String(128), nullable=False),
        sa.Column("table_namespace", sa.String(256), nullable=False),
        sa.Column("table_name", sa.String(128), nullable=False),
        sa.Column("table_uuid", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("lance_base_uri", sa.Text(), nullable=False),
        sa.Column("lifecycle_state", sa.String(16), nullable=False, server_default="DRAFT"),
        sa.Column("default_spec_id", sa.Uuid(as_uuid=True), sa.ForeignKey("dataset_specs.spec_id"), nullable=False),
        sa.Column("canonical_baseline_snapshot_id", sa.BigInteger(), nullable=True),
        sa.Column("replay_horizon_seconds", sa.BigInteger(), nullable=False, server_default="2592000"),
        sa.Column("tenant_column", sa.String(128), nullable=False, server_default="tenant_id"),
        sa.Column("namespace_column", sa.String(128), nullable=False, server_default="namespace"),
        sa.Column("org_column", sa.String(128), nullable=False, server_default="org_id"),
        sa.Column("record_id_column", sa.String(128), nullable=False, server_default="vector_id"),
        sa.Column("operation_column", sa.String(128), nullable=False, server_default="op"),
        sa.Column("event_time_column", sa.String(128), nullable=False, server_default="event_timestamp"),
        sa.Column("vectors_column", sa.String(128), nullable=False, server_default="vectors"),
        sa.Column("texts_column", sa.String(128), nullable=False, server_default="texts"),
        sa.Column("metadata_column", sa.String(128), nullable=False, server_default="metadata"),
        sa.Column("ttl_column", sa.String(128), nullable=True, server_default="ttl"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("source_name", name="uq_iceberg_sources_name"),
        sa.UniqueConstraint("table_uuid", name="uq_iceberg_sources_table_uuid"),
        sa.CheckConstraint(
            "source_name ~ '^[A-Za-z][A-Za-z0-9_-]{0,127}$'",
            name="ck_iceberg_sources_name",
        ),
        sa.CheckConstraint(
            "spark_catalog ~ '^[A-Za-z_][A-Za-z0-9_]{0,127}$'",
            name="ck_iceberg_sources_catalog",
        ),
        sa.CheckConstraint(
            "table_namespace ~ '^[A-Za-z_][A-Za-z0-9_.]{0,255}$'",
            name="ck_iceberg_sources_namespace",
        ),
        sa.CheckConstraint(
            "table_name ~ '^[A-Za-z_][A-Za-z0-9_]{0,127}$'",
            name="ck_iceberg_sources_table_name",
        ),
        sa.CheckConstraint(
            "lifecycle_state IN ('DRAFT', 'ACTIVE', 'PAUSED')",
            name="ck_iceberg_sources_lifecycle",
        ),
        sa.CheckConstraint("length(lance_base_uri) > 0", name="ck_iceberg_sources_lance_base_uri"),
        sa.CheckConstraint(
            "canonical_baseline_snapshot_id IS NULL OR canonical_baseline_snapshot_id >= 0",
            name="ck_iceberg_sources_baseline_nonnegative",
        ),
        sa.CheckConstraint("replay_horizon_seconds > 0", name="ck_iceberg_sources_replay_positive"),
        sa.CheckConstraint(
            "tenant_column ~ '^[A-Za-z_][A-Za-z0-9_]{0,127}$'",
            name="ck_iceberg_sources_tenant_column",
        ),
        sa.CheckConstraint(
            "namespace_column ~ '^[A-Za-z_][A-Za-z0-9_]{0,127}$'",
            name="ck_iceberg_sources_namespace_column",
        ),
        sa.CheckConstraint(
            "org_column ~ '^[A-Za-z_][A-Za-z0-9_]{0,127}$'",
            name="ck_iceberg_sources_org_column",
        ),
        sa.CheckConstraint(
            "record_id_column ~ '^[A-Za-z_][A-Za-z0-9_]{0,127}$'",
            name="ck_iceberg_sources_record_column",
        ),
        sa.CheckConstraint(
            "operation_column ~ '^[A-Za-z_][A-Za-z0-9_]{0,127}$'",
            name="ck_iceberg_sources_operation_column",
        ),
        sa.CheckConstraint(
            "event_time_column ~ '^[A-Za-z_][A-Za-z0-9_]{0,127}$'",
            name="ck_iceberg_sources_event_time_column",
        ),
        sa.CheckConstraint(
            "vectors_column ~ '^[A-Za-z_][A-Za-z0-9_]{0,127}$'",
            name="ck_iceberg_sources_vectors_column",
        ),
        sa.CheckConstraint(
            "texts_column ~ '^[A-Za-z_][A-Za-z0-9_]{0,127}$'",
            name="ck_iceberg_sources_texts_column",
        ),
        sa.CheckConstraint(
            "metadata_column ~ '^[A-Za-z_][A-Za-z0-9_]{0,127}$'",
            name="ck_iceberg_sources_metadata_column",
        ),
        sa.CheckConstraint(
            "ttl_column IS NULL OR ttl_column ~ '^[A-Za-z_][A-Za-z0-9_]{0,127}$'",
            name="ck_iceberg_sources_ttl_column",
        ),
    )
    op.create_table(
        "datasets",
        sa.Column("dataset_id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("source_id", sa.Uuid(as_uuid=True), sa.ForeignKey("iceberg_sources.source_id"), nullable=False),
        sa.Column("tenant_id", sa.String(128), nullable=False),
        sa.Column("namespace", sa.String(128), nullable=False),
        sa.Column("org_id", sa.String(128), nullable=False),
        sa.Column("lifecycle_state", sa.String(16), nullable=False, server_default="ACTIVE"),
        sa.Column(
            "desired_spec_revision_id",
            sa.Uuid(as_uuid=True),
            sa.ForeignKey("dataset_spec_revisions.spec_revision_id"),
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("dataset_id", "source_id", name="uq_datasets_dataset_source"),
        sa.UniqueConstraint("tenant_id", "namespace", "org_id", name="uq_datasets_routing_identity"),
        sa.CheckConstraint("tenant_id ~ '^[A-Za-z0-9_-]{1,128}$'", name="ck_datasets_tenant_id"),
        sa.CheckConstraint("namespace ~ '^[A-Za-z0-9_-]{1,128}$'", name="ck_datasets_namespace"),
        sa.CheckConstraint("org_id ~ '^[A-Za-z0-9_-]{1,128}$'", name="ck_datasets_org_id"),
        sa.CheckConstraint(
            "lifecycle_state IN ('ACTIVE', 'PAUSED', 'BLOCKED')",
            name="ck_datasets_lifecycle",
        ),
    )
    op.create_table(
        "source_snapshots",
        sa.Column("source_snapshot_seq", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("source_id", sa.Uuid(as_uuid=True), sa.ForeignKey("iceberg_sources.source_id"), nullable=False),
        sa.Column("snapshot_id", sa.BigInteger(), nullable=False),
        sa.Column("parent_snapshot_id", sa.BigInteger(), nullable=True),
        sa.Column("iceberg_sequence_number", sa.BigInteger(), nullable=False),
        sa.Column("partition_spec_id", sa.Integer(), nullable=False),
        sa.Column("committed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("iceberg_operation", sa.String(32), nullable=False),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("state", sa.String(16), nullable=False, server_default="SEALED"),
        sa.Column("error_code", sa.String(128), nullable=True),
        sa.Column("error_message", sa.String(2000), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("source_snapshot_seq", "source_id", name="uq_source_snapshots_sequence_source"),
        sa.UniqueConstraint("source_id", "snapshot_id", name="uq_source_snapshots_source_snapshot"),
        sa.UniqueConstraint("source_id", "iceberg_sequence_number", name="uq_source_snapshots_source_sequence"),
        sa.CheckConstraint(
            "kind IN ('BASELINE', 'APPEND', 'TRUSTED_MAINTENANCE', 'REJECTED')",
            name="ck_source_snapshots_kind",
        ),
        sa.CheckConstraint("state IN ('SEALED', 'COMPLETE', 'BLOCKED')", name="ck_source_snapshots_state"),
        sa.CheckConstraint("snapshot_id >= 0", name="ck_source_snapshots_snapshot_nonnegative"),
        sa.CheckConstraint(
            "parent_snapshot_id IS NULL OR parent_snapshot_id >= 0",
            name="ck_source_snapshots_parent_nonnegative",
        ),
        sa.CheckConstraint("iceberg_sequence_number >= 0", name="ck_source_snapshots_sequence_nonnegative"),
        sa.CheckConstraint("partition_spec_id >= 0", name="ck_source_snapshots_spec_nonnegative"),
        sa.CheckConstraint("length(iceberg_operation) > 0", name="ck_source_snapshots_operation"),
        sa.CheckConstraint(
            "kind IN ('BASELINE', 'REJECTED') OR parent_snapshot_id IS NOT NULL",
            name="ck_source_snapshots_parent_required",
        ),
    )


def create_work_and_publication_tables() -> None:
    """Create fenced work, immutable publications, qualification, and mutable dataset state."""
    op.create_table(
        "dataset_work",
        sa.Column("work_id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("dataset_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("source_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("source_snapshot_seq", sa.BigInteger(), nullable=False),
        sa.Column(
            "spec_revision_id",
            sa.Uuid(as_uuid=True),
            sa.ForeignKey("dataset_spec_revisions.spec_revision_id"),
            nullable=False,
        ),
        sa.Column("kind", sa.String(16), nullable=False),
        sa.Column("state", sa.String(16), nullable=False, server_default="PENDING"),
        sa.Column("phase", sa.String(16), nullable=False),
        sa.Column("lease_token", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempt_count", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("launcher_kind", sa.String(16), nullable=False, server_default="LOCAL"),
        sa.Column("airflow_ctx_dag_id", sa.String(250), nullable=True),
        sa.Column("airflow_ctx_dag_run_id", sa.String(512), nullable=True),
        sa.Column("airflow_ctx_task_id", sa.String(250), nullable=True),
        sa.Column("airflow_ctx_map_index", sa.Integer(), nullable=True),
        sa.Column("airflow_ctx_try_number", sa.Integer(), nullable=True),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("expected_ingest_lance_uri", sa.Text(), nullable=False),
        sa.Column("expected_ingest_lance_version", sa.BigInteger(), nullable=True),
        sa.Column("expected_active_publication_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("candidate_lance_uri", sa.Text(), nullable=True),
        sa.Column("candidate_lance_version", sa.BigInteger(), nullable=True),
        sa.Column("source_applied_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_row_count", sa.BigInteger(), nullable=True),
        sa.Column("source_digest", sa.LargeBinary(32), nullable=True),
        sa.Column("artifact_manifest_uri", sa.Text(), nullable=True),
        sa.Column("artifact_digest", sa.LargeBinary(32), nullable=True),
        sa.Column("error_code", sa.String(128), nullable=True),
        sa.Column("error_message", sa.String(2000), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(
            ["dataset_id", "source_id"],
            ["datasets.dataset_id", "datasets.source_id"],
            name="fk_dataset_work_dataset_source",
        ),
        sa.ForeignKeyConstraint(
            ["source_snapshot_seq", "source_id"],
            ["source_snapshots.source_snapshot_seq", "source_snapshots.source_id"],
            name="fk_dataset_work_snapshot_source",
        ),
        sa.UniqueConstraint("dataset_id", "work_id", name="uq_dataset_work_dataset_work"),
        sa.UniqueConstraint(
            "dataset_id",
            "work_id",
            "spec_revision_id",
            "source_snapshot_seq",
            name="uq_dataset_work_publication_identity",
        ),
        sa.CheckConstraint("kind IN ('INGEST', 'PUBLISH', 'REBUILD')", name="ck_dataset_work_kind"),
        sa.CheckConstraint(
            "state IN ('PENDING', 'RUNNING', 'RETRY_WAIT', 'SUCCEEDED', 'BLOCKED')",
            name="ck_dataset_work_state",
        ),
        sa.CheckConstraint(
            "phase IN ('INGEST', 'COMPACT', 'INDEX', 'VALIDATE', 'PREWARM', 'PUBLISH')",
            name="ck_dataset_work_phase",
        ),
        sa.CheckConstraint(
            "(state = 'RUNNING') = (lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="ck_dataset_work_lease_state",
        ),
        sa.CheckConstraint("attempt_count >= 0", name="ck_dataset_work_attempt_nonnegative"),
        sa.CheckConstraint(
            "launcher_kind IN ('LOCAL', 'AIRFLOW')",
            name="ck_dataset_work_launcher_kind",
        ),
        sa.CheckConstraint(
            "(launcher_kind = 'LOCAL' AND airflow_ctx_dag_id IS NULL "
            "AND airflow_ctx_dag_run_id IS NULL AND airflow_ctx_task_id IS NULL "
            "AND airflow_ctx_map_index IS NULL AND airflow_ctx_try_number IS NULL) OR "
            "(launcher_kind = 'AIRFLOW' AND airflow_ctx_dag_id IS NOT NULL "
            "AND airflow_ctx_dag_run_id IS NOT NULL AND airflow_ctx_task_id IS NOT NULL "
            "AND length(airflow_ctx_dag_id) > 0 AND length(airflow_ctx_dag_run_id) > 0 "
            "AND length(airflow_ctx_task_id) > 0 "
            "AND (airflow_ctx_map_index IS NULL OR airflow_ctx_map_index >= -1) "
            "AND (airflow_ctx_try_number IS NULL OR airflow_ctx_try_number > 0))",
            name="ck_dataset_work_launcher_context",
        ),
        sa.CheckConstraint("length(expected_ingest_lance_uri) > 0", name="ck_dataset_work_expected_ingest_uri"),
        sa.CheckConstraint(
            "expected_ingest_lance_version IS NULL OR expected_ingest_lance_version >= 0",
            name="ck_dataset_work_expected_ingest_version",
        ),
        sa.CheckConstraint(
            "(candidate_lance_uri IS NULL) = (candidate_lance_version IS NULL)",
            name="ck_dataset_work_candidate_tuple",
        ),
        sa.CheckConstraint(
            "candidate_lance_version IS NULL OR candidate_lance_version >= 0",
            name="ck_dataset_work_candidate_version",
        ),
        sa.CheckConstraint(
            "source_row_count IS NULL OR source_row_count >= 0",
            name="ck_dataset_work_rows_nonnegative",
        ),
        sa.CheckConstraint(
            "source_digest IS NULL OR octet_length(source_digest) = 32",
            name="ck_dataset_work_source_digest",
        ),
        sa.CheckConstraint(
            "artifact_digest IS NULL OR octet_length(artifact_digest) = 32",
            name="ck_dataset_work_artifact_digest",
        ),
        sa.CheckConstraint(
            "artifact_manifest_uri IS NOT NULL OR artifact_digest IS NULL",
            name="ck_dataset_work_artifact_uri",
        ),
    )
    op.create_index(
        "uq_dataset_work_source_kind",
        "dataset_work",
        ["dataset_id", "source_snapshot_seq", "kind"],
        unique=True,
        postgresql_where=sa.text("kind != 'REBUILD'"),
    )
    op.create_index(
        "uq_dataset_work_running_dataset",
        "dataset_work",
        ["dataset_id"],
        unique=True,
        postgresql_where=sa.text("state = 'RUNNING'"),
    )
    op.create_index(
        "uq_dataset_work_open_publish",
        "dataset_work",
        ["dataset_id"],
        unique=True,
        postgresql_where=sa.text("kind = 'PUBLISH' AND state IN ('PENDING', 'RUNNING', 'RETRY_WAIT')"),
    )
    op.create_index(
        "ix_dataset_work_claim",
        "dataset_work",
        ["state", "next_attempt_at", "dataset_id"],
    )
    op.create_index(
        "ix_dataset_work_ingest_order",
        "dataset_work",
        ["dataset_id", "source_snapshot_seq"],
        postgresql_where=sa.text("kind = 'INGEST' AND state != 'SUCCEEDED'"),
    )
    op.create_table(
        "dataset_publications",
        sa.Column("publication_id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("dataset_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("work_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column(
            "spec_revision_id",
            sa.Uuid(as_uuid=True),
            sa.ForeignKey("dataset_spec_revisions.spec_revision_id"),
            nullable=False,
        ),
        sa.Column(
            "source_snapshot_seq",
            sa.BigInteger(),
            sa.ForeignKey("source_snapshots.source_snapshot_seq"),
            nullable=False,
        ),
        sa.Column("lance_uri", sa.Text(), nullable=False),
        sa.Column("lance_version", sa.BigInteger(), nullable=False),
        sa.Column("schema_digest", sa.LargeBinary(32), nullable=False),
        sa.Column("total_row_count", sa.BigInteger(), nullable=False),
        sa.Column("distinct_row_count", sa.BigInteger(), nullable=False),
        sa.Column("live_row_count", sa.BigInteger(), nullable=False),
        sa.Column("distinct_live_row_count", sa.BigInteger(), nullable=False),
        sa.Column("fragment_count", sa.BigInteger(), nullable=False),
        sa.Column("manifest_uri", sa.Text(), nullable=False),
        sa.Column("manifest_digest", sa.LargeBinary(32), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("retired_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["dataset_id", "work_id", "spec_revision_id", "source_snapshot_seq"],
            [
                "dataset_work.dataset_id",
                "dataset_work.work_id",
                "dataset_work.spec_revision_id",
                "dataset_work.source_snapshot_seq",
            ],
            name="fk_dataset_publications_dataset_work_identity",
        ),
        sa.UniqueConstraint("work_id", name="uq_dataset_publications_work"),
        sa.UniqueConstraint("dataset_id", "publication_id", name="uq_dataset_publications_dataset_publication"),
        sa.UniqueConstraint(
            "publication_id",
            "spec_revision_id",
            name="uq_dataset_publications_publication_revision",
        ),
        sa.UniqueConstraint("dataset_id", "lance_uri", "lance_version", name="uq_dataset_publications_version"),
        sa.CheckConstraint("length(lance_uri) > 0", name="ck_dataset_publications_lance_uri"),
        sa.CheckConstraint("lance_version >= 0", name="ck_dataset_publications_lance_version"),
        sa.CheckConstraint("octet_length(schema_digest) = 32", name="ck_dataset_publications_schema_digest"),
        sa.CheckConstraint("total_row_count >= 0", name="ck_dataset_publications_total_rows"),
        sa.CheckConstraint(
            "distinct_row_count = total_row_count",
            name="ck_dataset_publications_distinct_rows",
        ),
        sa.CheckConstraint(
            "live_row_count >= 0 AND live_row_count <= total_row_count",
            name="ck_dataset_publications_live_rows",
        ),
        sa.CheckConstraint(
            "distinct_live_row_count = live_row_count",
            name="ck_dataset_publications_distinct_live_rows",
        ),
        sa.CheckConstraint("fragment_count >= 0", name="ck_dataset_publications_fragments"),
        sa.CheckConstraint("length(manifest_uri) > 0", name="ck_dataset_publications_manifest_uri"),
        sa.CheckConstraint("octet_length(manifest_digest) = 32", name="ck_dataset_publications_manifest_digest"),
        sa.CheckConstraint(
            "retired_at IS NULL OR retired_at >= published_at",
            name="ck_dataset_publications_retired_after_publish",
        ),
    )
    op.create_table(
        "publication_indexes",
        sa.Column("publication_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("spec_revision_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("index_definition_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("actual_index_type", sa.String(16), nullable=False),
        sa.Column("indexed_fragment_count", sa.BigInteger(), nullable=False),
        sa.Column("unindexed_fragment_count", sa.BigInteger(), nullable=False),
        sa.Column("artifact_generation_digest", sa.LargeBinary(32), nullable=True),
        sa.ForeignKeyConstraint(
            ["publication_id", "spec_revision_id"],
            ["dataset_publications.publication_id", "dataset_publications.spec_revision_id"],
            name="fk_publication_indexes_publication_revision",
        ),
        sa.ForeignKeyConstraint(
            ["spec_revision_id", "index_definition_id", "actual_index_type"],
            [
                "index_definitions.spec_revision_id",
                "index_definitions.index_definition_id",
                "index_definitions.index_type",
            ],
            name="fk_publication_indexes_definition_type",
        ),
        sa.PrimaryKeyConstraint("publication_id", "index_definition_id", name="pk_publication_indexes"),
        sa.CheckConstraint(
            "actual_index_type IN ('IVF_RQ', 'BTREE', 'BITMAP', 'ZONEMAP', 'INVERTED')",
            name="ck_publication_indexes_actual_type",
        ),
        sa.CheckConstraint("indexed_fragment_count >= 0", name="ck_publication_indexes_indexed_nonnegative"),
        sa.CheckConstraint(
            "unindexed_fragment_count = 0",
            name="ck_publication_indexes_complete_coverage",
        ),
        sa.CheckConstraint(
            "artifact_generation_digest IS NULL OR octet_length(artifact_generation_digest) = 32",
            name="ck_publication_indexes_artifact_digest",
        ),
    )
    op.create_table(
        "dataset_state",
        sa.Column(
            "dataset_id",
            sa.Uuid(as_uuid=True),
            sa.ForeignKey("datasets.dataset_id"),
            primary_key=True,
        ),
        sa.Column(
            "materialized_spec_revision_id",
            sa.Uuid(as_uuid=True),
            sa.ForeignKey("dataset_spec_revisions.spec_revision_id"),
            nullable=True,
        ),
        sa.Column(
            "last_applied_source_snapshot_seq",
            sa.BigInteger(),
            sa.ForeignKey("source_snapshots.source_snapshot_seq"),
            nullable=True,
        ),
        sa.Column("ingest_lance_uri", sa.Text(), nullable=False),
        sa.Column("ingest_lance_version", sa.BigInteger(), nullable=True),
        sa.Column("active_publication_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("fence_epoch", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(
            ["dataset_id", "active_publication_id"],
            ["dataset_publications.dataset_id", "dataset_publications.publication_id"],
            name="fk_dataset_state_active_publication",
        ),
        sa.CheckConstraint("length(ingest_lance_uri) > 0", name="ck_dataset_state_ingest_uri"),
        sa.CheckConstraint(
            "ingest_lance_version IS NULL OR ingest_lance_version >= 0",
            name="ck_dataset_state_ingest_version",
        ),
        sa.CheckConstraint("fence_epoch >= 0", name="ck_dataset_state_fence_nonnegative"),
    )


def seed_defaults() -> None:
    """Seed local reconciler settings and the deterministic production dataset specification."""
    settings_seed: TableClause = sa.table("reconciler_settings", sa.column("singleton_id", sa.SmallInteger()))
    op.bulk_insert(settings_seed, [{"singleton_id": 1}])
    specs_seed: TableClause = sa.table(
        "dataset_specs",
        sa.column("spec_id", sa.Uuid(as_uuid=True)),
        sa.column("name", sa.String(128)),
        sa.column("description", sa.Text()),
    )
    op.bulk_insert(
        specs_seed,
        [
            {
                "spec_id": DEFAULT_SPEC_ID,
                "name": "default-v1",
                "description": (
                    "Local Iceberg to Lance schema, ingestion, compaction, indexing, and publication policy."
                ),
            }
        ],
    )
    revisions_seed: TableClause = sa.table(
        "dataset_spec_revisions",
        sa.column("spec_revision_id", sa.Uuid(as_uuid=True)),
        sa.column("spec_id", sa.Uuid(as_uuid=True)),
        sa.column("revision_number", sa.BigInteger()),
        sa.column("state", sa.String(16)),
        sa.column("configuration_digest", sa.LargeBinary(32)),
        sa.column("ingest_shuffle_partitions", sa.Integer()),
        sa.column("merge_rows_per_chunk", sa.BigInteger()),
        sa.column("merge_batch_bytes", sa.BigInteger()),
        sa.column("write_rows_per_fragment", sa.BigInteger()),
        sa.column("compaction_enabled", sa.Boolean()),
        sa.column("compaction_mode", sa.String(32)),
        sa.column("target_rows_per_fragment", sa.BigInteger()),
        sa.column("max_source_fragments", sa.Integer()),
        sa.column("compaction_threads", sa.Integer()),
        sa.column("defer_index_remap", sa.Boolean()),
        sa.column("materialize_deletions", sa.Boolean()),
        sa.column("materialize_deletions_threshold", sa.Numeric(5, 4)),
        sa.column("cleanup_older_than_seconds", sa.BigInteger()),
        sa.column("retain_versions", sa.Integer()),
        sa.column("fragments_per_index_task", sa.Integer()),
        sa.column("max_index_deltas", sa.Integer()),
        sa.column("max_stale_replans", sa.Integer()),
        sa.column("prewarm_required", sa.Boolean()),
        sa.column("retained_publications", sa.Integer()),
        sa.column("artifact_retention_seconds", sa.BigInteger()),
        sa.column("activated_at", sa.DateTime(timezone=True)),
    )
    op.execute(
        revisions_seed.insert().values(
            spec_revision_id=DEFAULT_SPEC_REVISION_ID,
            spec_id=DEFAULT_SPEC_ID,
            revision_number=1,
            state="ACTIVE",
            configuration_digest=sa.func.decode(DEFAULT_CONFIGURATION_DIGEST_HEX, "hex"),
            ingest_shuffle_partitions=256,
            merge_rows_per_chunk=250_000,
            merge_batch_bytes=268_435_456,
            write_rows_per_fragment=1_000_000,
            compaction_enabled=True,
            compaction_mode="try_binary_copy",
            target_rows_per_fragment=1_048_576,
            max_source_fragments=256,
            compaction_threads=None,
            defer_index_remap=False,
            materialize_deletions=True,
            materialize_deletions_threshold=0.1,
            cleanup_older_than_seconds=216_000,
            retain_versions=2,
            fragments_per_index_task=8,
            max_index_deltas=4,
            max_stale_replans=3,
            prewarm_required=True,
            retained_publications=2,
            artifact_retention_seconds=2_592_000,
            activated_at=sa.func.now(),
        )
    )
    fields_seed: TableClause = sa.table(
        "dataset_fields",
        sa.column("field_id", sa.Uuid(as_uuid=True)),
        sa.column("spec_revision_id", sa.Uuid(as_uuid=True)),
        sa.column("ordinal", sa.Integer()),
        sa.column("target_name", sa.String(128)),
        sa.column("role", sa.String(16)),
        sa.column("source_kind", sa.String(16)),
        sa.column("source_column", sa.String(128)),
        sa.column("source_key", sa.String(128)),
        sa.column("data_type", sa.String(128)),
        sa.column("nullable", sa.Boolean()),
        sa.column("required_on_upsert", sa.Boolean()),
        sa.column("vector_dimension", sa.Integer()),
    )
    field_rows: list[dict[str, object]] = [
        default_field_row("vector_id", 0, "KEY", "DIRECT", "vector_id", None, "string", False, True),
        default_field_row(
            "event_timestamp",
            1,
            "EVENT_TIME",
            "DIRECT",
            "event_timestamp",
            None,
            "timestamp[us,UTC]",
            True,
            True,
        ),
        default_field_row(
            "vector",
            2,
            "VECTOR",
            "MAP_KEY",
            "vectors",
            "vector",
            "fixed_size_list<float32,128>",
            True,
            True,
            128,
        ),
        default_field_row("text", 3, "TEXT", "MAP_KEY", "texts", "text", "string", True, False),
        default_field_row(
            "cluster",
            4,
            "METADATA",
            "MAP_KEY",
            "metadata",
            "cluster",
            "string",
            True,
            False,
        ),
        default_field_row("ttl", 5, "TTL", "DIRECT", "ttl", None, "duration[s]", True, False),
        default_field_row(
            "lance_etl_window_seq",
            6,
            "LINEAGE",
            "DERIVED",
            "lance_etl_window_seq",
            None,
            "int64",
            False,
            False,
        ),
        default_field_row(
            "lance_etl_source_sequence",
            7,
            "LINEAGE",
            "DERIVED",
            "lance_etl_source_sequence",
            None,
            "int64",
            False,
            False,
        ),
        default_field_row(
            "lance_etl_event_digest",
            8,
            "LINEAGE",
            "DERIVED",
            "lance_etl_event_digest",
            None,
            "binary[32]",
            False,
            False,
        ),
        default_field_row(
            "is_deleted",
            9,
            "TOMBSTONE",
            "DERIVED",
            "is_deleted",
            None,
            "bool",
            False,
            False,
        ),
    ]
    op.bulk_insert(fields_seed, field_rows)
    indexes_seed: TableClause = sa.table(
        "index_definitions",
        sa.column("index_definition_id", sa.Uuid(as_uuid=True)),
        sa.column("spec_revision_id", sa.Uuid(as_uuid=True)),
        sa.column("field_id", sa.Uuid(as_uuid=True)),
        sa.column("ordinal", sa.Integer()),
        sa.column("index_name", sa.String(128)),
        sa.column("index_type", sa.String(16)),
    )
    index_rows: list[dict[str, object]] = [
        default_index_row("vector_idx", "vector", 0, "IVF_RQ"),
        default_index_row("text_fts_idx", "text", 1, "INVERTED"),
        default_index_row("cluster_idx", "cluster", 2, "BTREE"),
        default_index_row("event_timestamp_idx", "event_timestamp", 3, "BTREE"),
        default_index_row("event_timestamp_zonemap_idx", "event_timestamp", 4, "ZONEMAP"),
        default_index_row("is_deleted_bitmap_idx", "is_deleted", 5, "BITMAP"),
    ]
    op.bulk_insert(indexes_seed, index_rows)
    vector_seed: TableClause = sa.table(
        "vector_index_options",
        sa.column("index_definition_id", sa.Uuid(as_uuid=True)),
        sa.column("index_type", sa.String(16)),
        sa.column("metric", sa.String(16)),
        sa.column("num_partitions", sa.Integer()),
        sa.column("minimum_partitions", sa.Integer()),
        sa.column("maximum_partitions", sa.Integer()),
        sa.column("target_rows_per_partition", sa.Integer()),
        sa.column("minimum_rows", sa.BigInteger()),
        sa.column("num_bits", sa.Integer()),
        sa.column("streaming_sample_rate", sa.Integer()),
        sa.column("streaming_refine_passes", sa.Integer()),
        sa.column("retrain_growth_factor", sa.Numeric(8, 4)),
    )
    op.bulk_insert(
        vector_seed,
        [
            {
                "index_definition_id": DEFAULT_INDEX_IDS["vector_idx"],
                "index_type": "IVF_RQ",
                "metric": "cosine",
                "num_partitions": None,
                "minimum_partitions": 16,
                "maximum_partitions": 32_768,
                "target_rows_per_partition": 8_192,
                "minimum_rows": 1,
                "num_bits": 1,
                "streaming_sample_rate": 32,
                "streaming_refine_passes": 1,
                "retrain_growth_factor": 4.0,
            }
        ],
    )
    fts_seed: TableClause = sa.table(
        "fts_index_options",
        sa.column("index_definition_id", sa.Uuid(as_uuid=True)),
        sa.column("index_type", sa.String(16)),
        sa.column("with_position", sa.Boolean()),
        sa.column("base_tokenizer", sa.String(64)),
        sa.column("language", sa.String(32)),
        sa.column("max_unindexed_fragments", sa.Integer()),
    )
    op.bulk_insert(
        fts_seed,
        [
            {
                "index_definition_id": DEFAULT_INDEX_IDS["text_fts_idx"],
                "index_type": "INVERTED",
                "with_position": False,
                "base_tokenizer": None,
                "language": None,
                "max_unindexed_fragments": 32,
            }
        ],
    )


def default_field_row(
    target_name: str,
    ordinal: int,
    role: str,
    source_kind: str,
    source_column: str,
    source_key: str | None,
    data_type: str,
    nullable: bool,
    required_on_upsert: bool,
    vector_dimension: int | None = None,
) -> dict[str, object]:
    """Build one deterministic default dataset-field seed row.

    Args:
        target_name: Persisted Lance field name.
        ordinal: Stable schema order.
        role: Processing role.
        source_kind: Source projection kind.
        source_column: Physical Iceberg source column.
        source_key: Map key when the source kind is MAP_KEY.
        data_type: Canonical physical type descriptor.
        nullable: Whether the persisted field accepts nulls.
        required_on_upsert: Whether an upsert source row must provide the field.
        vector_dimension: Fixed vector width for vector fields.

    Returns:
        Seed row for ``dataset_fields``.
    """
    return {
        "field_id": DEFAULT_FIELD_IDS[target_name],
        "spec_revision_id": DEFAULT_SPEC_REVISION_ID,
        "ordinal": ordinal,
        "target_name": target_name,
        "role": role,
        "source_kind": source_kind,
        "source_column": source_column,
        "source_key": source_key,
        "data_type": data_type,
        "nullable": nullable,
        "required_on_upsert": required_on_upsert,
        "vector_dimension": vector_dimension,
    }


def default_index_row(index_name: str, field_name: str, ordinal: int, index_type: str) -> dict[str, object]:
    """Build one deterministic default index-definition seed row.

    Args:
        index_name: Stable Lance index name.
        field_name: Persisted field covered by the index.
        ordinal: Stable execution order.
        index_type: Lance index family.

    Returns:
        Seed row for ``index_definitions``.
    """
    return {
        "index_definition_id": DEFAULT_INDEX_IDS[index_name],
        "spec_revision_id": DEFAULT_SPEC_REVISION_ID,
        "field_id": DEFAULT_FIELD_IDS[field_name],
        "ordinal": ordinal,
        "index_name": index_name,
        "index_type": index_type,
    }


def create_lifecycle_triggers() -> None:
    """Install PostgreSQL guards for frozen revisions and active assignments."""
    op.execute(
        sa.text(
            """
            CREATE FUNCTION require_draft_spec_revision(candidate_revision_id uuid)
            RETURNS void
            LANGUAGE plpgsql
            AS $$
            DECLARE
                revision_state text;
            BEGIN
                SELECT state
                INTO revision_state
                FROM dataset_spec_revisions
                WHERE spec_revision_id = candidate_revision_id
                FOR SHARE;
                IF revision_state IS DISTINCT FROM 'DRAFT' THEN
                    RAISE EXCEPTION 'specification children may change only while their revision is DRAFT'
                        USING ERRCODE = '23514', CONSTRAINT = 'ck_specification_children_draft';
                END IF;
            END;
            $$;

            CREATE FUNCTION enforce_spec_revision_lifecycle()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $$
            BEGIN
                IF TG_OP = 'INSERT' THEN
                    IF NEW.state <> 'DRAFT' THEN
                        RAISE EXCEPTION 'new specification revisions must be DRAFT'
                            USING ERRCODE = '23514', CONSTRAINT = 'ck_spec_revision_insert_draft';
                    END IF;
                    RETURN NEW;
                END IF;
                IF TG_OP = 'DELETE' THEN
                    IF OLD.state <> 'DRAFT' THEN
                        RAISE EXCEPTION 'active and retired specification revisions are immutable'
                            USING ERRCODE = '23514', CONSTRAINT = 'ck_spec_revision_frozen';
                    END IF;
                    RETURN OLD;
                END IF;
                IF OLD.state = 'DRAFT' THEN
                    IF NEW.state = 'DRAFT' THEN
                        RETURN NEW;
                    END IF;
                    IF NEW.state = 'ACTIVE'
                       AND (to_jsonb(NEW) - 'state' - 'activated_at') =
                           (to_jsonb(OLD) - 'state' - 'activated_at') THEN
                        RETURN NEW;
                    END IF;
                    RAISE EXCEPTION 'DRAFT revisions may transition only to ACTIVE without content changes'
                        USING ERRCODE = '23514', CONSTRAINT = 'ck_spec_revision_draft_transition';
                END IF;
                IF OLD.state = 'ACTIVE'
                   AND NEW.state = 'RETIRED'
                   AND (to_jsonb(NEW) - 'state') = (to_jsonb(OLD) - 'state') THEN
                    RETURN NEW;
                END IF;
                RAISE EXCEPTION 'active and retired specification revisions are immutable'
                    USING ERRCODE = '23514', CONSTRAINT = 'ck_spec_revision_frozen';
            END;
            $$;

            CREATE TRIGGER enforce_spec_revision_lifecycle_trigger
            BEFORE INSERT OR UPDATE OR DELETE ON dataset_spec_revisions
            FOR EACH ROW EXECUTE FUNCTION enforce_spec_revision_lifecycle();

            CREATE FUNCTION enforce_direct_spec_child_lifecycle()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $$
            BEGIN
                IF TG_OP = 'INSERT' THEN
                    PERFORM require_draft_spec_revision(NEW.spec_revision_id);
                    RETURN NEW;
                END IF;
                IF TG_OP = 'DELETE' THEN
                    PERFORM require_draft_spec_revision(OLD.spec_revision_id);
                    RETURN OLD;
                END IF;
                PERFORM require_draft_spec_revision(OLD.spec_revision_id);
                PERFORM require_draft_spec_revision(NEW.spec_revision_id);
                RETURN NEW;
            END;
            $$;

            CREATE TRIGGER enforce_dataset_fields_lifecycle_trigger
            BEFORE INSERT OR UPDATE OR DELETE ON dataset_fields
            FOR EACH ROW EXECUTE FUNCTION enforce_direct_spec_child_lifecycle();

            CREATE TRIGGER enforce_index_definitions_lifecycle_trigger
            BEFORE INSERT OR UPDATE OR DELETE ON index_definitions
            FOR EACH ROW EXECUTE FUNCTION enforce_direct_spec_child_lifecycle();

            CREATE FUNCTION require_draft_index(candidate_index_id uuid)
            RETURNS void
            LANGUAGE plpgsql
            AS $$
            DECLARE
                owner_revision_id uuid;
            BEGIN
                SELECT spec_revision_id
                INTO owner_revision_id
                FROM index_definitions
                WHERE index_definition_id = candidate_index_id;
                IF owner_revision_id IS NULL THEN
                    RAISE EXCEPTION 'index option owner is missing'
                        USING ERRCODE = '23514', CONSTRAINT = 'ck_specification_option_owner';
                END IF;
                PERFORM require_draft_spec_revision(owner_revision_id);
            END;
            $$;

            CREATE FUNCTION enforce_spec_option_lifecycle()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $$
            BEGIN
                IF TG_OP = 'INSERT' THEN
                    PERFORM require_draft_index(NEW.index_definition_id);
                    RETURN NEW;
                END IF;
                IF TG_OP = 'DELETE' THEN
                    PERFORM require_draft_index(OLD.index_definition_id);
                    RETURN OLD;
                END IF;
                PERFORM require_draft_index(OLD.index_definition_id);
                PERFORM require_draft_index(NEW.index_definition_id);
                RETURN NEW;
            END;
            $$;

            CREATE TRIGGER enforce_vector_options_lifecycle_trigger
            BEFORE INSERT OR UPDATE OR DELETE ON vector_index_options
            FOR EACH ROW EXECUTE FUNCTION enforce_spec_option_lifecycle();

            CREATE TRIGGER enforce_fts_options_lifecycle_trigger
            BEFORE INSERT OR UPDATE OR DELETE ON fts_index_options
            FOR EACH ROW EXECUTE FUNCTION enforce_spec_option_lifecycle();

            CREATE FUNCTION enforce_active_dataset_revision()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $$
            DECLARE
                revision_state text;
            BEGIN
                SELECT state
                INTO revision_state
                FROM dataset_spec_revisions
                WHERE spec_revision_id = NEW.desired_spec_revision_id
                FOR SHARE;
                IF revision_state IS DISTINCT FROM 'ACTIVE' THEN
                    RAISE EXCEPTION 'dataset desired specification revision must be ACTIVE'
                        USING ERRCODE = '23514', CONSTRAINT = 'ck_datasets_desired_revision_active';
                END IF;
                RETURN NEW;
            END;
            $$;

            CREATE TRIGGER enforce_active_dataset_revision_trigger
            BEFORE INSERT OR UPDATE OF desired_spec_revision_id ON datasets
            FOR EACH ROW EXECUTE FUNCTION enforce_active_dataset_revision();

            CREATE FUNCTION enforce_source_default_active()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM iceberg_sources source
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM dataset_spec_revisions revision
                        WHERE revision.spec_id = source.default_spec_id
                          AND revision.state = 'ACTIVE'
                    )
                ) THEN
                    RAISE EXCEPTION 'source default specification must have an ACTIVE revision'
                        USING ERRCODE = '23514', CONSTRAINT = 'ck_iceberg_sources_default_active';
                END IF;
                RETURN NULL;
            END;
            $$;

            CREATE CONSTRAINT TRIGGER enforce_source_default_on_source_trigger
            AFTER INSERT OR UPDATE OF default_spec_id ON iceberg_sources
            DEFERRABLE INITIALLY DEFERRED
            FOR EACH ROW EXECUTE FUNCTION enforce_source_default_active();

            CREATE CONSTRAINT TRIGGER enforce_source_default_on_revision_trigger
            AFTER INSERT OR UPDATE OR DELETE ON dataset_spec_revisions
            DEFERRABLE INITIALLY DEFERRED
            FOR EACH ROW EXECUTE FUNCTION enforce_source_default_active();
            """
        )
    )


def downgrade() -> None:
    """Drop the control plane in reverse dependency order."""
    op.drop_table("dataset_state")
    op.drop_table("publication_indexes")
    op.drop_table("dataset_publications")
    op.drop_table("dataset_work")
    op.drop_table("source_snapshots")
    op.drop_table("datasets")
    op.drop_table("iceberg_sources")
    op.drop_table("fts_index_options")
    op.drop_table("vector_index_options")
    op.drop_table("index_definitions")
    op.drop_table("dataset_fields")
    op.drop_table("dataset_spec_revisions")
    op.drop_table("dataset_specs")
    op.drop_table("reconciler_settings")
    op.execute(sa.text("DROP FUNCTION IF EXISTS enforce_source_default_active()"))
    op.execute(sa.text("DROP FUNCTION IF EXISTS enforce_active_dataset_revision()"))
    op.execute(sa.text("DROP FUNCTION IF EXISTS enforce_spec_option_lifecycle()"))
    op.execute(sa.text("DROP FUNCTION IF EXISTS require_draft_index(uuid)"))
    op.execute(sa.text("DROP FUNCTION IF EXISTS enforce_direct_spec_child_lifecycle()"))
    op.execute(sa.text("DROP FUNCTION IF EXISTS enforce_spec_revision_lifecycle()"))
    op.execute(sa.text("DROP FUNCTION IF EXISTS require_draft_spec_revision(uuid)"))
