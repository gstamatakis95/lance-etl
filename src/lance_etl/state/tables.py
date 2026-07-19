"""SQLAlchemy Core definitions for the PostgreSQL dataset control plane."""

from __future__ import annotations

import sqlalchemy as sa

metadata: sa.MetaData = sa.MetaData()
"""Control-plane metadata containing the application tables."""

SPEC_STATES: tuple[str, ...] = ("DRAFT", "ACTIVE", "RETIRED")
FIELD_ROLES: tuple[str, ...] = (
    "KEY",
    "EVENT_TIME",
    "VECTOR",
    "TEXT",
    "METADATA",
    "TTL",
    "TOMBSTONE",
    "LINEAGE",
)
FIELD_SOURCE_KINDS: tuple[str, ...] = ("DIRECT", "MAP_KEY", "DERIVED")
INDEX_TYPES: tuple[str, ...] = ("IVF_RQ", "BTREE", "BITMAP", "ZONEMAP", "INVERTED")
SOURCE_LIFECYCLE_STATES: tuple[str, ...] = ("DRAFT", "ACTIVE", "PAUSED")
DATASET_LIFECYCLE_STATES: tuple[str, ...] = ("ACTIVE", "PAUSED", "BLOCKED")
SNAPSHOT_KINDS: tuple[str, ...] = ("BASELINE", "APPEND", "TRUSTED_MAINTENANCE", "REJECTED")
SNAPSHOT_STATES: tuple[str, ...] = ("SEALED", "COMPLETE", "BLOCKED")
WORK_KINDS: tuple[str, ...] = ("INGEST", "PUBLISH", "REBUILD")
WORK_STATES: tuple[str, ...] = ("PENDING", "RUNNING", "RETRY_WAIT", "SUCCEEDED", "BLOCKED")
WORK_PHASES: tuple[str, ...] = ("INGEST", "COMPACT", "INDEX", "VALIDATE", "PREWARM", "PUBLISH")
WORK_LAUNCHER_KINDS: tuple[str, ...] = ("LOCAL", "AIRFLOW")
VECTOR_METRICS: tuple[str, ...] = ("l2", "cosine", "dot")


def quoted_values(values: tuple[str, ...]) -> str:
    """Render closed string values for a SQL CHECK expression.

    Args:
        values: Trusted code-owned values.

    Returns:
        Comma-separated single-quoted literals.
    """
    return ", ".join(f"'{value}'" for value in values)


reconciler_settings: sa.Table = sa.Table(
    "reconciler_settings",
    metadata,
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

dataset_specs: sa.Table = sa.Table(
    "dataset_specs",
    metadata,
    sa.Column("spec_id", sa.Uuid(as_uuid=True), primary_key=True),
    sa.Column("name", sa.String(128), nullable=False),
    sa.Column("description", sa.Text(), nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    sa.UniqueConstraint("name", name="uq_dataset_specs_name"),
    sa.CheckConstraint("name ~ '^[A-Za-z][A-Za-z0-9_-]{0,127}$'", name="ck_dataset_specs_name"),
)

dataset_spec_revisions: sa.Table = sa.Table(
    "dataset_spec_revisions",
    metadata,
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
    sa.CheckConstraint(f"state IN ({quoted_values(SPEC_STATES)})", name="ck_dataset_spec_revisions_state"),
    sa.CheckConstraint("revision_number > 0", name="ck_dataset_spec_revisions_number_positive"),
    sa.CheckConstraint(
        "octet_length(configuration_digest) = 32",
        name="ck_dataset_spec_revisions_digest",
    ),
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
    sa.CheckConstraint(
        "fragments_per_index_task > 0",
        name="ck_dataset_spec_revisions_index_task_positive",
    ),
    sa.CheckConstraint("max_index_deltas > 0", name="ck_dataset_spec_revisions_index_deltas_positive"),
    sa.CheckConstraint("max_stale_replans >= 0", name="ck_dataset_spec_revisions_replans_nonnegative"),
    sa.CheckConstraint(
        "retained_publications > 0",
        name="ck_dataset_spec_revisions_publications_positive",
    ),
    sa.CheckConstraint(
        "artifact_retention_seconds > 0",
        name="ck_dataset_spec_revisions_artifact_retention_positive",
    ),
)

sa.Index(
    "uq_dataset_spec_revisions_active",
    dataset_spec_revisions.c.spec_id,
    unique=True,
    postgresql_where=dataset_spec_revisions.c.state == "ACTIVE",
)

dataset_fields: sa.Table = sa.Table(
    "dataset_fields",
    metadata,
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
    sa.CheckConstraint(f"role IN ({quoted_values(FIELD_ROLES)})", name="ck_dataset_fields_role"),
    sa.CheckConstraint(
        f"source_kind IN ({quoted_values(FIELD_SOURCE_KINDS)})",
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

sa.Index(
    "uq_dataset_fields_singleton_role",
    dataset_fields.c.spec_revision_id,
    dataset_fields.c.role,
    unique=True,
    postgresql_where=dataset_fields.c.role.in_(("KEY", "EVENT_TIME", "TTL", "TOMBSTONE")),
)

index_definitions: sa.Table = sa.Table(
    "index_definitions",
    metadata,
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
    sa.UniqueConstraint("spec_revision_id", "field_id", "index_type", name="uq_index_definitions_field_type"),
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
    sa.CheckConstraint(f"index_type IN ({quoted_values(INDEX_TYPES)})", name="ck_index_definitions_type"),
)

vector_index_options: sa.Table = sa.Table(
    "vector_index_options",
    metadata,
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
    sa.CheckConstraint(f"metric IN ({quoted_values(VECTOR_METRICS)})", name="ck_vector_index_options_metric"),
    sa.CheckConstraint("num_partitions IS NULL OR num_partitions > 0", name="ck_vector_index_options_partitions"),
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

fts_index_options: sa.Table = sa.Table(
    "fts_index_options",
    metadata,
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

iceberg_sources: sa.Table = sa.Table(
    "iceberg_sources",
    metadata,
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
        f"lifecycle_state IN ({quoted_values(SOURCE_LIFECYCLE_STATES)})",
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

datasets: sa.Table = sa.Table(
    "datasets",
    metadata,
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
        f"lifecycle_state IN ({quoted_values(DATASET_LIFECYCLE_STATES)})",
        name="ck_datasets_lifecycle",
    ),
)

source_snapshots: sa.Table = sa.Table(
    "source_snapshots",
    metadata,
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
    sa.CheckConstraint(f"kind IN ({quoted_values(SNAPSHOT_KINDS)})", name="ck_source_snapshots_kind"),
    sa.CheckConstraint(f"state IN ({quoted_values(SNAPSHOT_STATES)})", name="ck_source_snapshots_state"),
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

dataset_work: sa.Table = sa.Table(
    "dataset_work",
    metadata,
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
    sa.CheckConstraint(f"kind IN ({quoted_values(WORK_KINDS)})", name="ck_dataset_work_kind"),
    sa.CheckConstraint(f"state IN ({quoted_values(WORK_STATES)})", name="ck_dataset_work_state"),
    sa.CheckConstraint(f"phase IN ({quoted_values(WORK_PHASES)})", name="ck_dataset_work_phase"),
    sa.CheckConstraint(
        "(state = 'RUNNING') = (lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)",
        name="ck_dataset_work_lease_state",
    ),
    sa.CheckConstraint("attempt_count >= 0", name="ck_dataset_work_attempt_nonnegative"),
    sa.CheckConstraint(
        f"launcher_kind IN ({quoted_values(WORK_LAUNCHER_KINDS)})",
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
    sa.CheckConstraint("source_row_count IS NULL OR source_row_count >= 0", name="ck_dataset_work_rows_nonnegative"),
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

sa.Index(
    "uq_dataset_work_source_kind",
    dataset_work.c.dataset_id,
    dataset_work.c.source_snapshot_seq,
    dataset_work.c.kind,
    unique=True,
    postgresql_where=dataset_work.c.kind != "REBUILD",
)
sa.Index(
    "uq_dataset_work_running_dataset",
    dataset_work.c.dataset_id,
    unique=True,
    postgresql_where=dataset_work.c.state == "RUNNING",
)
sa.Index(
    "uq_dataset_work_open_publish",
    dataset_work.c.dataset_id,
    unique=True,
    postgresql_where=sa.and_(
        dataset_work.c.kind == "PUBLISH",
        dataset_work.c.state.in_(("PENDING", "RUNNING", "RETRY_WAIT")),
    ),
)
sa.Index(
    "ix_dataset_work_claim",
    dataset_work.c.state,
    dataset_work.c.next_attempt_at,
    dataset_work.c.dataset_id,
)
sa.Index(
    "ix_dataset_work_ingest_order",
    dataset_work.c.dataset_id,
    dataset_work.c.source_snapshot_seq,
    postgresql_where=sa.and_(
        dataset_work.c.kind == "INGEST",
        dataset_work.c.state != "SUCCEEDED",
    ),
)

dataset_publications: sa.Table = sa.Table(
    "dataset_publications",
    metadata,
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

publication_indexes: sa.Table = sa.Table(
    "publication_indexes",
    metadata,
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
        f"actual_index_type IN ({quoted_values(INDEX_TYPES)})",
        name="ck_publication_indexes_actual_type",
    ),
    sa.CheckConstraint("indexed_fragment_count >= 0", name="ck_publication_indexes_indexed_nonnegative"),
    sa.CheckConstraint("unindexed_fragment_count = 0", name="ck_publication_indexes_complete_coverage"),
    sa.CheckConstraint(
        "artifact_generation_digest IS NULL OR octet_length(artifact_generation_digest) = 32",
        name="ck_publication_indexes_artifact_digest",
    ),
)

dataset_state: sa.Table = sa.Table(
    "dataset_state",
    metadata,
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
