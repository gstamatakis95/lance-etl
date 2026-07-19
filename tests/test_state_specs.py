"""Tests for immutable PostgreSQL-owned dataset specification revisions."""

from __future__ import annotations

import uuid
from dataclasses import FrozenInstanceError, dataclass, replace

import pytest

from lance_etl.state.specs import (
    DEFAULT_CONFIGURATION_DIGEST_HEX,
    DEFAULT_FIELD_IDS,
    DEFAULT_INDEX_IDS,
    DEFAULT_SPEC_ID,
    DEFAULT_SPEC_REVISION_ID,
    EMPTY_UUID,
    CompactionMode,
    DatasetField,
    DatasetSpecRevision,
    FieldRole,
    FtsIndexOptions,
    IndexDefinition,
    IndexType,
    SourceKind,
    SpecRevisionState,
    VectorIndexOptions,
    VectorMetric,
    decode_dataset_spec_revision,
    production_default_spec_revision,
)


@dataclass(frozen=True, slots=True)
class SpecRows:
    """Normalized PostgreSQL rows for one complete specification graph."""

    revision: dict[str, object]
    fields: tuple[dict[str, object], ...]
    indexes: tuple[dict[str, object], ...]


def redigest(revision: DatasetSpecRevision) -> DatasetSpecRevision:
    """Return a revision carrying the digest for its semantic configuration.

    Args:
        revision: Revision whose semantic values may have changed.

    Returns:
        Revision with a matching raw SHA-256 digest.
    """
    return replace(revision, configuration_digest=revision.expected_configuration_digest())


def replace_field(revision: DatasetSpecRevision, field_name: str, **changes: object) -> DatasetSpecRevision:
    """Replace one named field in a revision fixture.

    Args:
        revision: Base revision.
        field_name: Target field to replace.
        changes: Dataclass field replacements.

    Returns:
        Revision carrying the changed field and its original digest.
    """
    replacement: DatasetField = replace(revision.field_by_name(field_name), **changes)
    fields: tuple[DatasetField, ...] = tuple(
        replacement if item.target_name == field_name else item for item in revision.fields
    )
    return replace(revision, fields=fields)


def replace_index(revision: DatasetSpecRevision, current_index_name: str, **changes: object) -> DatasetSpecRevision:
    """Replace one named index in a revision fixture.

    Args:
        revision: Base revision.
        current_index_name: Lance index to replace.
        changes: Dataclass index replacements.

    Returns:
        Revision carrying the changed index and its original digest.
    """
    replacement: IndexDefinition = replace(revision.index_by_name(current_index_name), **changes)
    indexes: tuple[IndexDefinition, ...] = tuple(
        replacement if item.index_name == current_index_name else item for item in revision.indexes
    )
    return replace(revision, indexes=indexes)


def normalized_rows(revision: DatasetSpecRevision) -> SpecRows:
    """Convert one typed revision to its normalized PostgreSQL row graph.

    Args:
        revision: Validated revision.

    Returns:
        Parent, field, index, and subtype rows.
    """
    revision_row: dict[str, object] = {
        "spec_revision_id": revision.spec_revision_id,
        "spec_id": revision.spec_id,
        "name": "production",
        "description": "Default local processing contract",
        "revision_number": revision.revision_number,
        "state": revision.state.value,
        "supersedes_revision_id": revision.supersedes_revision_id,
        "configuration_digest": revision.configuration_digest,
        "ingest_shuffle_partitions": revision.ingest_shuffle_partitions,
        "merge_rows_per_chunk": revision.merge_rows_per_chunk,
        "merge_batch_bytes": revision.merge_batch_bytes,
        "write_rows_per_fragment": revision.write_rows_per_fragment,
        "compaction_enabled": revision.compaction_enabled,
        "compaction_mode": revision.compaction_mode.value,
        "target_rows_per_fragment": revision.target_rows_per_fragment,
        "max_source_fragments": revision.max_source_fragments,
        "compaction_threads": revision.compaction_threads,
        "defer_index_remap": revision.defer_index_remap,
        "materialize_deletions": revision.materialize_deletions,
        "materialize_deletions_threshold": revision.materialize_deletions_threshold,
        "cleanup_older_than_seconds": revision.cleanup_older_than_seconds,
        "retain_versions": revision.retain_versions,
        "fragments_per_index_task": revision.fragments_per_index_task,
        "max_index_deltas": revision.max_index_deltas,
        "max_stale_replans": revision.max_stale_replans,
        "prewarm_required": revision.prewarm_required,
        "retained_publications": revision.retained_publications,
        "artifact_retention_seconds": revision.artifact_retention_seconds,
        "record_retention_seconds": revision.record_retention_seconds,
    }
    field_rows: tuple[dict[str, object], ...] = tuple(
        {
            "field_id": field_value.field_id,
            "spec_revision_id": field_value.spec_revision_id,
            "ordinal": field_value.ordinal,
            "target_name": field_value.target_name,
            "role": field_value.role.value,
            "source_kind": field_value.source_kind.value,
            "source_column": field_value.source_column,
            "source_key": field_value.source_key,
            "data_type": field_value.data_type,
            "nullable": field_value.nullable,
            "required_on_upsert": field_value.required_on_upsert,
            "vector_dimension": field_value.vector_dimension,
        }
        for field_value in revision.fields
    )
    index_rows: tuple[dict[str, object], ...] = tuple(normalized_index_row(index) for index in revision.indexes)
    return SpecRows(revision_row, field_rows, index_rows)


def normalized_index_row(index: IndexDefinition) -> dict[str, object]:
    """Build one ``index_definitions`` row carrying inline subtype option columns.

    Args:
        index: Typed index definition.

    Returns:
        PostgreSQL-shaped index row with every option column populated or null.
    """
    row: dict[str, object] = {
        "index_definition_id": index.index_definition_id,
        "spec_revision_id": index.spec_revision_id,
        "field_id": index.field_id,
        "ordinal": index.ordinal,
        "index_name": index.index_name,
        "index_type": index.index_type.value,
        "metric": None,
        "num_partitions": None,
        "minimum_partitions": None,
        "maximum_partitions": None,
        "target_rows_per_partition": None,
        "minimum_rows": None,
        "num_bits": None,
        "streaming_sample_rate": None,
        "streaming_refine_passes": None,
        "retrain_growth_factor": None,
        "with_position": None,
        "base_tokenizer": None,
        "language": None,
        "max_unindexed_fragments": None,
    }
    if index.vector_options is not None:
        row.update(
            {
                "metric": index.vector_options.metric.value,
                "num_partitions": index.vector_options.num_partitions,
                "minimum_partitions": index.vector_options.minimum_partitions,
                "maximum_partitions": index.vector_options.maximum_partitions,
                "target_rows_per_partition": index.vector_options.target_rows_per_partition,
                "minimum_rows": index.vector_options.minimum_rows,
                "num_bits": index.vector_options.num_bits,
                "streaming_sample_rate": index.vector_options.streaming_sample_rate,
                "streaming_refine_passes": index.vector_options.streaming_refine_passes,
                "retrain_growth_factor": index.vector_options.retrain_growth_factor,
            }
        )
    if index.fts_options is not None:
        row.update(
            {
                "with_position": index.fts_options.with_position,
                "base_tokenizer": index.fts_options.base_tokenizer,
                "language": index.fts_options.language,
                "max_unindexed_fragments": index.fts_options.max_unindexed_fragments,
            }
        )
    return row


def decode_rows(rows: SpecRows) -> DatasetSpecRevision:
    """Decode one normalized test row graph.

    Args:
        rows: Normalized specification row graph.

    Returns:
        Validated typed revision.
    """
    return decode_dataset_spec_revision(
        rows.revision,
        rows.fields,
        rows.indexes,
    )


def test_production_default_is_deterministic_complete_and_valid() -> None:
    """The seed revision freezes the complete current processing configuration."""
    first: DatasetSpecRevision = production_default_spec_revision()
    second: DatasetSpecRevision = production_default_spec_revision()

    assert first == second
    assert first.validate() is first
    assert first.spec_id == DEFAULT_SPEC_ID
    assert first.spec_revision_id == DEFAULT_SPEC_REVISION_ID
    assert first.revision_number == 1
    assert first.state is SpecRevisionState.ACTIVE
    assert first.configuration_digest == first.expected_configuration_digest()
    assert first.configuration_digest.hex() == DEFAULT_CONFIGURATION_DIGEST_HEX
    assert first.vector_fields == (("vector", 128),)
    assert first.text_fields == ("text",)
    assert first.metadata_fields == ("cluster",)
    assert first.record_retention_seconds is None
    assert first.ingest_shuffle_partitions == 256
    assert first.merge_rows_per_chunk == 250_000
    assert first.merge_batch_bytes == 268_435_456
    assert first.compaction_mode is CompactionMode.TRY_BINARY_COPY
    assert first.materialize_deletions
    assert first.field_names_for_index_type(IndexType.BTREE) == ("cluster", "ts")
    assert tuple(index.index_type for index in first.index_definitions) == (
        IndexType.IVF_RQ,
        IndexType.INVERTED,
        IndexType.BTREE,
        IndexType.BTREE,
        IndexType.ZONEMAP,
        IndexType.BITMAP,
    )
    vector_options: VectorIndexOptions = first.vector_options_for_field("vector")
    assert vector_options.minimum_partitions == 16
    assert vector_options.maximum_partitions == 32_768
    assert vector_options.metric is VectorMetric.COSINE
    assert first.fts_options_for_field("text").max_unindexed_fragments == 32


def test_default_ids_match_the_alembic_seed_contract() -> None:
    """The Python seed uses the simple durable identities copied by Alembic."""
    revision: DatasetSpecRevision = production_default_spec_revision()

    assert {field_value.target_name: field_value.field_id for field_value in revision.fields} == DEFAULT_FIELD_IDS
    assert {index.index_name: index.index_definition_id for index in revision.indexes} == DEFAULT_INDEX_IDS


def test_normalized_decoder_round_trips_unordered_rows() -> None:
    """Database row ordering does not affect the immutable decoded value or digest."""
    expected: DatasetSpecRevision = production_default_spec_revision()
    rows: SpecRows = normalized_rows(expected)
    reordered: SpecRows = replace(rows, fields=tuple(reversed(rows.fields)), indexes=tuple(reversed(rows.indexes)))

    assert decode_rows(reordered) == expected


def test_domain_values_are_frozen() -> None:
    """A loaded revision cannot mutate underneath claimed work."""
    revision: DatasetSpecRevision = production_default_spec_revision()

    with pytest.raises(FrozenInstanceError):
        revision.revision_number = 2
    with pytest.raises(FrozenInstanceError):
        revision.fields[0].target_name = "changed"
    with pytest.raises(FrozenInstanceError):
        revision.indexes[0].ordinal = 10


def test_digest_is_canonical_across_child_tuple_order() -> None:
    """Canonical ordinals make digesting independent from SQL result ordering."""
    revision: DatasetSpecRevision = production_default_spec_revision()
    reordered: DatasetSpecRevision = replace(
        revision, fields=tuple(reversed(revision.fields)), indexes=tuple(reversed(revision.indexes))
    )

    assert reordered.expected_configuration_digest() == revision.configuration_digest
    assert reordered.validate() is reordered


def test_digest_rejects_semantic_changes_and_wrong_lengths() -> None:
    """Every processing change requires a newly computed configuration digest."""
    revision: DatasetSpecRevision = production_default_spec_revision()

    with pytest.raises(ValueError, match="does not match"):
        replace(revision, merge_rows_per_chunk=revision.merge_rows_per_chunk + 1).validate()
    with pytest.raises(ValueError, match="exactly 32 bytes"):
        replace(revision, configuration_digest=b"short").validate()
    with pytest.raises(ValueError, match="exactly 32 bytes"):
        replace(revision, configuration_digest="not-bytes").validate()


@pytest.mark.parametrize(
    ("field_name", "value"),
    (
        ("ingest_shuffle_partitions", 0),
        ("merge_rows_per_chunk", 0),
        ("merge_batch_bytes", 0),
        ("write_rows_per_fragment", -1),
        ("target_rows_per_fragment", 0),
        ("fragments_per_index_task", 0),
        ("max_index_deltas", 0),
        ("retained_publications", 0),
        ("artifact_retention_seconds", 0),
        ("max_source_fragments", 0),
        ("compaction_threads", -1),
        ("retain_versions", 0),
    ),
)
def test_positive_operational_settings_reject_zero_and_negatives(field_name: str, value: int) -> None:
    """Required and present optional operational bounds match database checks."""
    revision: DatasetSpecRevision = production_default_spec_revision()

    with pytest.raises(ValueError, match=field_name):
        replace(revision, **{field_name: value}).validate()


def test_nonnegative_and_cleanup_operational_bounds_match_database() -> None:
    """Zero replans are valid while cleanup retains its six-hour safety floor."""
    revision: DatasetSpecRevision = production_default_spec_revision()
    valid: DatasetSpecRevision = redigest(
        replace(revision, max_stale_replans=0, materialize_deletions_threshold=0.0)
    ).validate()

    assert valid.max_stale_replans == 0
    with pytest.raises(ValueError, match="max_stale_replans"):
        replace(revision, max_stale_replans=-1).validate()
    with pytest.raises(ValueError, match="at least 21600"):
        replace(revision, cleanup_older_than_seconds=21_599).validate()


@pytest.mark.parametrize("threshold", (-0.1, 1.01, float("inf"), True))
def test_materialize_deletion_threshold_is_a_finite_fraction(threshold: float) -> None:
    """Compaction deletion materialization accepts only finite fractions in range."""
    revision: DatasetSpecRevision = production_default_spec_revision()

    with pytest.raises(ValueError, match="materialize_deletions_threshold"):
        replace(revision, materialize_deletions_threshold=threshold).validate()


def test_revision_identity_lifecycle_and_supersession_are_validated() -> None:
    """Persisted revision identities and lifecycle values reject placeholders and cycles."""
    revision: DatasetSpecRevision = production_default_spec_revision()

    with pytest.raises(ValueError, match="spec_id"):
        replace(revision, spec_id=EMPTY_UUID).validate()
    with pytest.raises(ValueError, match="revision_number"):
        replace(revision, revision_number=0).validate()
    with pytest.raises(ValueError, match="state"):
        replace(revision, state="ACTIVE").validate()
    with pytest.raises(ValueError, match="another revision"):
        replace(revision, supersedes_revision_id=revision.spec_revision_id).validate()


@pytest.mark.parametrize(
    ("field_name", "changes", "message"),
    (
        ("vector", {"source_kind": SourceKind.DIRECT, "source_column": "vector", "source_key": None}, "MAP_KEY"),
        ("vector", {"source_column": "metadata"}, "vectors"),
        ("record_id", {"source_key": "id"}, "only MAP_KEY"),
        ("is_deleted", {"source_kind": SourceKind.DIRECT}, "DERIVED"),
        (
            "cluster",
            {"role": FieldRole.KEY, "source_kind": SourceKind.MAP_KEY},
            "only VECTOR, TEXT, and METADATA",
        ),
    ),
)
def test_source_mapping_must_match_field_role(field_name: str, changes: dict[str, object], message: str) -> None:
    """Direct, map-key, and derived projections cannot be combined inconsistently."""
    revision: DatasetSpecRevision = replace_field(production_default_spec_revision(), field_name, **changes)

    with pytest.raises(ValueError, match=message):
        revision.validate()


def test_map_key_must_equal_persisted_target_name() -> None:
    """Map projections cannot silently rename a source payload key."""
    revision: DatasetSpecRevision = replace_field(production_default_spec_revision(), "vector", source_key="embedding")

    with pytest.raises(ValueError, match="must equal"):
        revision.validate()


def test_field_flags_are_fixed_by_the_implemented_local_data_path() -> None:
    """PostgreSQL cannot advertise nullability or upsert semantics the writer does not implement."""
    revision: DatasetSpecRevision = replace_field(production_default_spec_revision(), "text", required_on_upsert=True)

    with pytest.raises(ValueError, match="fixed by the local data path"):
        revision.validate()


@pytest.mark.parametrize("dimension", (None, 7, 130))
def test_vector_dimension_must_be_present_and_divisible_by_eight(dimension: int | None) -> None:
    """RaBitQ-compatible vectors reject absent or non-byte-aligned dimensions."""
    revision: DatasetSpecRevision = replace_field(
        production_default_spec_revision(), "vector", vector_dimension=dimension
    )

    with pytest.raises(ValueError, match="divisible by 8"):
        revision.validate()


def test_vector_physical_type_must_encode_its_dimension() -> None:
    """The target type descriptor and vector dimension cannot disagree."""
    revision: DatasetSpecRevision = production_default_spec_revision()

    with pytest.raises(ValueError, match="exact vector_dimension"):
        replace_field(revision, "vector", data_type="fixed_size_list<float32,256>").validate()
    with pytest.raises(ValueError, match="TEXT fields require string"):
        replace_field(revision, "text", data_type="fixed_size_list<float32,128>").validate()
    with pytest.raises(ValueError, match="KEY fields require string"):
        replace_field(revision, "record_id", data_type="int64").validate()


def test_revision_requires_singleton_key_and_event_time() -> None:
    """KEY and EVENT_TIME are required singletons in the record contract."""
    revision: DatasetSpecRevision = production_default_spec_revision()
    without_key: DatasetSpecRevision = replace(
        revision, fields=tuple(item for item in revision.fields if item.role is not FieldRole.KEY)
    )
    duplicate_event_time: DatasetSpecRevision = replace_field(
        revision,
        "cluster",
        role=FieldRole.EVENT_TIME,
        source_kind=SourceKind.DIRECT,
        source_column="secondary_ts",
        source_key=None,
        data_type="timestamp[us,UTC]",
    )

    with pytest.raises(ValueError, match="exactly one KEY"):
        without_key.validate()
    with pytest.raises(ValueError, match="canonical name"):
        duplicate_event_time.validate()


def test_revision_requires_canonical_derived_fields_and_execution_order() -> None:
    """The field graph exactly describes the stored lineage, tombstone, and physical column order."""
    revision: DatasetSpecRevision = production_default_spec_revision()
    missing_lineage: DatasetSpecRevision = replace(
        revision,
        fields=tuple(item for item in revision.fields if item.target_name != "lance_etl_event_digest"),
    )
    renamed_tombstone: DatasetSpecRevision = replace_field(
        revision,
        "is_deleted",
        target_name="deleted",
        source_column="deleted",
    )
    reordered: DatasetSpecRevision = replace_field(
        replace_field(revision, "text", ordinal=revision.field_by_name("cluster").ordinal),
        "cluster",
        ordinal=revision.field_by_name("text").ordinal,
    )

    with pytest.raises(ValueError, match="three canonical LINEAGE"):
        missing_lineage.validate()
    with pytest.raises(ValueError, match="canonical name 'is_deleted'"):
        renamed_tombstone.validate()
    with pytest.raises(ValueError, match="canonical execution order"):
        reordered.validate()


def test_field_and_index_names_ordinals_ids_and_roles_are_unique() -> None:
    """Relational child keys and index roles cannot alias within one revision."""
    revision: DatasetSpecRevision = production_default_spec_revision()
    vector_field: DatasetField = revision.field_by_name("vector")
    vector_index: IndexDefinition = revision.index_by_name("vector_idx")

    with pytest.raises(ValueError, match="field names"):
        replace_field(revision, "text", target_name="vector").validate()
    with pytest.raises(ValueError, match="field ordinals"):
        replace_field(revision, "text", ordinal=vector_field.ordinal).validate()
    with pytest.raises(ValueError, match="field IDs"):
        replace_field(revision, "text", field_id=vector_field.field_id).validate()
    with pytest.raises(ValueError, match="index names"):
        replace_index(revision, "text_fts_idx", index_name="vector_idx").validate()
    with pytest.raises(ValueError, match="index ordinals"):
        replace_index(revision, "text_fts_idx", ordinal=vector_index.ordinal).validate()
    with pytest.raises(ValueError, match="index IDs"):
        replace_index(revision, "text_fts_idx", index_definition_id=vector_index.index_definition_id).validate()


def test_children_and_index_fields_must_belong_to_revision() -> None:
    """Cross-revision fields and indexes cannot enter a frozen work configuration."""
    revision: DatasetSpecRevision = production_default_spec_revision()

    with pytest.raises(ValueError, match="field must belong"):
        replace_field(revision, "text", spec_revision_id=uuid.uuid4()).validate()
    with pytest.raises(ValueError, match="index must belong"):
        replace_index(revision, "text_fts_idx", spec_revision_id=uuid.uuid4()).validate()
    with pytest.raises(ValueError, match="index field must belong"):
        replace_index(revision, "text_fts_idx", field_id=uuid.uuid4()).validate()


def test_index_subtype_options_are_exclusive_required_and_role_safe() -> None:
    """Typed index families own only their subtype options and compatible fields."""
    revision: DatasetSpecRevision = production_default_spec_revision()
    vector_options: VectorIndexOptions = revision.vector_options_for_field("vector")
    fts_options: FtsIndexOptions = revision.fts_options_for_field("text")
    text_id: uuid.UUID = revision.field_by_name("text").field_id

    with pytest.raises(ValueError, match="require vector options"):
        replace_index(revision, "vector_idx", vector_options=None).validate()
    with pytest.raises(ValueError, match="only for IVF_RQ"):
        replace_index(revision, "cluster_idx", vector_options=vector_options).validate()
    with pytest.raises(ValueError, match="require FTS options"):
        replace_index(revision, "text_fts_idx", fts_options=None).validate()
    with pytest.raises(ValueError, match="only for INVERTED"):
        replace_index(revision, "cluster_idx", fts_options=fts_options).validate()
    with pytest.raises(ValueError, match="IVF_RQ cannot index a TEXT"):
        replace_index(revision, "vector_idx", field_id=text_id).validate()


def test_vector_partition_policy_and_training_bounds_are_validated() -> None:
    """Automatic and explicit IVF partitions remain within configured sizing bounds."""
    revision: DatasetSpecRevision = production_default_spec_revision()
    options: VectorIndexOptions = revision.vector_options_for_field("vector")

    with pytest.raises(ValueError, match="maximum_partitions"):
        replace_index(
            revision,
            "vector_idx",
            vector_options=replace(options, maximum_partitions=options.minimum_partitions - 1),
        ).validate()
    with pytest.raises(ValueError, match="partition range"):
        replace_index(revision, "vector_idx", vector_options=replace(options, num_partitions=8)).validate()
    with pytest.raises(ValueError, match="num_bits"):
        replace_index(revision, "vector_idx", vector_options=replace(options, num_bits=9)).validate()
    with pytest.raises(ValueError, match="retrain_growth_factor"):
        replace_index(revision, "vector_idx", vector_options=replace(options, retrain_growth_factor=1.0)).validate()

    valid_options: VectorIndexOptions = replace(options, minimum_rows=0, streaming_refine_passes=0, num_partitions=16)
    changed: DatasetSpecRevision = replace_index(revision, "vector_idx", vector_options=valid_options)
    assert redigest(changed).validate().vector_options_for_field("vector") == valid_options


def test_fts_options_allow_zero_backlog_but_reject_bad_strings() -> None:
    """The FTS subtype follows its database bounds and bounded optional strings."""
    revision: DatasetSpecRevision = production_default_spec_revision()
    options: FtsIndexOptions = revision.fts_options_for_field("text")
    changed: DatasetSpecRevision = replace_index(
        revision, "text_fts_idx", fts_options=replace(options, max_unindexed_fragments=0)
    )

    assert redigest(changed).validate().fts_options_for_field("text").max_unindexed_fragments == 0
    with pytest.raises(ValueError, match="base_tokenizer"):
        replace_index(revision, "text_fts_idx", fts_options=replace(options, base_tokenizer=" ")).validate()


def test_decoder_rejects_cross_revision_children() -> None:
    """A normalized row graph cannot cross revision boundaries between parent and children."""
    rows: SpecRows = normalized_rows(production_default_spec_revision())
    bad_field: dict[str, object] = dict(rows.fields[0])
    bad_field["spec_revision_id"] = uuid.uuid4()
    mismatched: SpecRows = replace(rows, fields=(bad_field, *rows.fields[1:]))

    with pytest.raises(ValueError, match="another spec revision"):
        decode_rows(mismatched)

    bad_index: dict[str, object] = dict(rows.indexes[0])
    bad_index["spec_revision_id"] = uuid.uuid4()
    with pytest.raises(ValueError, match="another spec revision"):
        decode_rows(replace(rows, indexes=(bad_index, *rows.indexes[1:])))


def test_lookup_helpers_fail_closed_for_unknown_names() -> None:
    """Worker-facing lookup helpers never silently omit requested schema or index values."""
    revision: DatasetSpecRevision = production_default_spec_revision()

    with pytest.raises(KeyError, match="no field"):
        revision.field_by_name("unknown")
    with pytest.raises(KeyError, match="no index"):
        revision.index_by_name("unknown_idx")
    with pytest.raises(KeyError, match="no IVF_RQ"):
        revision.vector_options_for_field("text")
    with pytest.raises(KeyError, match="no INVERTED"):
        revision.fts_options_for_field("vector")


def test_individual_domain_values_validate_their_own_contracts() -> None:
    """Standalone values catch nil identities and unsupported raw enum values."""
    revision: DatasetSpecRevision = production_default_spec_revision()
    field_value: DatasetField = revision.field_by_name("text")
    index: IndexDefinition = revision.index_by_name("text_fts_idx")
    vector_options: VectorIndexOptions = revision.vector_options_for_field("vector")
    fts_options: FtsIndexOptions = revision.fts_options_for_field("text")

    with pytest.raises(ValueError, match="field_id"):
        replace(field_value, field_id=EMPTY_UUID).validate()
    with pytest.raises(ValueError, match="role"):
        replace(field_value, role="TEXT").validate()
    with pytest.raises(ValueError, match="index_type"):
        replace(index, index_type="INVERTED").validate()
    assert vector_options.validate() is vector_options
    assert fts_options.validate() is fts_options
