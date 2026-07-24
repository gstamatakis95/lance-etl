"""Immutable, validated dataset processing specifications owned by PostgreSQL."""

from __future__ import annotations

import hashlib
import json
import math
import re
import uuid
from collections.abc import Hashable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from enum import StrEnum

IDENTIFIER_PATTERN: re.Pattern[str] = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
"""Bounded identifier contract shared with the relational schema."""

VECTOR_TYPE_PATTERN: re.Pattern[str] = re.compile(r"^fixed_size_list<float32,([1-9][0-9]*)>$")
"""Canonical physical type descriptor for one fixed-size float vector."""

SCALAR_DATA_TYPES: frozenset[str] = frozenset(
    {
        "string",
        "bool",
        "int32",
        "int64",
        "float32",
        "float64",
        "date32",
        "timestamp[us,UTC]",
        "duration[s]",
        "binary[32]",
    }
)
"""Closed physical scalar types supported by the local target-schema builder."""

EMPTY_UUID: uuid.UUID = uuid.UUID(int=0)
"""Nil UUID rejected for persisted specification identities."""

DEFAULT_SPEC_ID: uuid.UUID = uuid.UUID("00000000-0000-0000-0000-000000000001")
"""Stable identity of the bundled default dataset specification."""

DEFAULT_SPEC_REVISION_ID: uuid.UUID = uuid.UUID("00000000-0000-0000-0000-000000000101")
"""Stable identity of revision one of the bundled default specification."""

DEFAULT_FIELD_IDS: Mapping[str, uuid.UUID] = {
    "record_id": uuid.UUID("00000000-0000-0000-0000-000000001001"),
    "ts": uuid.UUID("00000000-0000-0000-0000-000000001002"),
    "vector": uuid.UUID("00000000-0000-0000-0000-000000001003"),
    "text": uuid.UUID("00000000-0000-0000-0000-000000001004"),
    "cluster": uuid.UUID("00000000-0000-0000-0000-000000001005"),
    "lance_etl_window_seq": uuid.UUID("00000000-0000-0000-0000-000000001007"),
    "lance_etl_source_sequence": uuid.UUID("00000000-0000-0000-0000-000000001008"),
    "lance_etl_event_digest": uuid.UUID("00000000-0000-0000-0000-000000001009"),
    "is_deleted": uuid.UUID("00000000-0000-0000-0000-000000001010"),
}
"""Stable identities of fields in the bundled default specification."""

DEFAULT_INDEX_IDS: Mapping[str, uuid.UUID] = {
    "vector_idx": uuid.UUID("00000000-0000-0000-0000-000000002001"),
    "text_fts_idx": uuid.UUID("00000000-0000-0000-0000-000000002002"),
    "cluster_idx": uuid.UUID("00000000-0000-0000-0000-000000002003"),
    "ts_idx": uuid.UUID("00000000-0000-0000-0000-000000002004"),
    "ts_zonemap_idx": uuid.UUID("00000000-0000-0000-0000-000000002005"),
    "is_deleted_bitmap_idx": uuid.UUID("00000000-0000-0000-0000-000000002006"),
}
"""Stable identities of indexes in the bundled default specification."""


class SpecRevisionState(StrEnum):
    """Lifecycle states of an immutable dataset specification revision."""

    DRAFT = "DRAFT"
    ACTIVE = "ACTIVE"
    RETIRED = "RETIRED"


class FieldRole(StrEnum):
    """Semantic roles supported by the persisted Lance schema."""

    KEY = "KEY"
    EVENT_TIME = "EVENT_TIME"
    VECTOR = "VECTOR"
    TEXT = "TEXT"
    METADATA = "METADATA"
    TOMBSTONE = "TOMBSTONE"
    LINEAGE = "LINEAGE"


class SourceKind(StrEnum):
    """Ways a target field is obtained from the fixed Iceberg source contract."""

    DIRECT = "DIRECT"
    MAP_KEY = "MAP_KEY"
    DERIVED = "DERIVED"


class IndexType(StrEnum):
    """Lance index families supported by distributed index construction."""

    IVF_RQ = "IVF_RQ"
    BTREE = "BTREE"
    BITMAP = "BITMAP"
    ZONEMAP = "ZONEMAP"
    INVERTED = "INVERTED"


class VectorMetric(StrEnum):
    """Distance metrics accepted by Lance IVF_RQ indexes."""

    L2 = "l2"
    COSINE = "cosine"
    DOT = "dot"


class CompactionMode(StrEnum):
    """Lance compaction execution modes owned by a specification revision."""

    TRY_BINARY_COPY = "try_binary_copy"
    REENCODE = "reencode"


@dataclass(frozen=True, slots=True)
class VectorIndexOptions:
    """Typed IVF_RQ training, sizing, and rotation settings."""

    metric: VectorMetric
    num_partitions: int | None
    minimum_partitions: int
    maximum_partitions: int
    target_rows_per_partition: int
    minimum_rows: int
    num_bits: int
    streaming_sample_rate: int
    streaming_refine_passes: int
    retrain_growth_factor: float

    def validate(self) -> VectorIndexOptions:
        """Validate IVF_RQ option bounds.

        Returns:
            This immutable option value after validation.

        Raises:
            ValueError: If a metric or numeric bound is unsupported.
        """
        if not isinstance(self.metric, VectorMetric):
            raise ValueError("vector metric must be one of l2, cosine, or dot")
        validate_positive_integers(
            {
                "minimum_partitions": self.minimum_partitions,
                "maximum_partitions": self.maximum_partitions,
                "target_rows_per_partition": self.target_rows_per_partition,
                "num_bits": self.num_bits,
                "streaming_sample_rate": self.streaming_sample_rate,
            }
        )
        validate_optional_positive(self.num_partitions, "num_partitions")
        if self.maximum_partitions < self.minimum_partitions:
            raise ValueError("maximum_partitions must be at least minimum_partitions")
        if (
            self.num_partitions is not None
            and not self.minimum_partitions <= self.num_partitions <= self.maximum_partitions
        ):
            raise ValueError("num_partitions must fall within the configured partition range")
        if type(self.minimum_rows) is not int or self.minimum_rows < 0:
            raise ValueError("minimum_rows must be a non-negative integer")
        if not 1 <= self.num_bits <= 8:
            raise ValueError("num_bits must be between 1 and 8")
        if type(self.streaming_refine_passes) is not int or self.streaming_refine_passes < 0:
            raise ValueError("streaming_refine_passes must be a non-negative integer")
        if (
            isinstance(self.retrain_growth_factor, bool)
            or not isinstance(self.retrain_growth_factor, (int, float))
            or not math.isfinite(self.retrain_growth_factor)
            or self.retrain_growth_factor <= 1.0
        ):
            raise ValueError("retrain_growth_factor must be finite and greater than one")
        return self

    def semantic_document(self) -> dict[str, object]:
        """Return canonical semantic option values for digesting.

        Returns:
            JSON-compatible vector option values.
        """
        return {
            "metric": self.metric.value,
            "num_partitions": self.num_partitions,
            "minimum_partitions": self.minimum_partitions,
            "maximum_partitions": self.maximum_partitions,
            "target_rows_per_partition": self.target_rows_per_partition,
            "minimum_rows": self.minimum_rows,
            "num_bits": self.num_bits,
            "streaming_sample_rate": self.streaming_sample_rate,
            "streaming_refine_passes": self.streaming_refine_passes,
            "retrain_growth_factor": self.retrain_growth_factor,
        }


@dataclass(frozen=True, slots=True)
class FtsIndexOptions:
    """Typed full-text construction and incremental-maintenance settings."""

    with_position: bool
    base_tokenizer: str | None
    language: str | None
    max_unindexed_fragments: int

    def validate(self) -> FtsIndexOptions:
        """Validate full-text option values.

        Returns:
            This immutable option value after validation.

        Raises:
            ValueError: If a string option is blank or the backlog bound is negative.
        """
        if type(self.with_position) is not bool:
            raise ValueError("with_position must be a boolean")
        validate_optional_text(self.base_tokenizer, "base_tokenizer", 64)
        validate_optional_text(self.language, "language", 32)
        if type(self.max_unindexed_fragments) is not int or self.max_unindexed_fragments < 0:
            raise ValueError("max_unindexed_fragments must be a non-negative integer")
        return self

    def semantic_document(self) -> dict[str, object]:
        """Return canonical semantic option values for digesting.

        Returns:
            JSON-compatible full-text option values.
        """
        return {
            "with_position": self.with_position,
            "base_tokenizer": self.base_tokenizer,
            "language": self.language,
            "max_unindexed_fragments": self.max_unindexed_fragments,
        }


@dataclass(frozen=True, slots=True)
class DatasetField:
    """One immutable target field and its source projection contract."""

    field_id: uuid.UUID
    spec_revision_id: uuid.UUID
    ordinal: int
    target_name: str
    role: FieldRole
    source_kind: SourceKind
    source_column: str
    source_key: str | None
    data_type: str
    nullable: bool
    required_on_upsert: bool
    vector_dimension: int | None = None

    def validate(self) -> DatasetField:
        """Validate field identity, source mapping, type, and vector shape.

        Returns:
            This immutable field after validation.

        Raises:
            ValueError: If the field cannot be represented by the source or Lance contracts.
        """
        validate_uuid(self.field_id, "field_id")
        validate_uuid(self.spec_revision_id, "field spec_revision_id")
        if type(self.ordinal) is not int or self.ordinal < 0:
            raise ValueError("field ordinal must be a non-negative integer")
        validate_identifier(self.target_name, "target_name")
        validate_identifier(self.source_column, "source_column")
        if not isinstance(self.role, FieldRole):
            raise ValueError("field role is unsupported")
        if not isinstance(self.source_kind, SourceKind):
            raise ValueError("field source_kind is unsupported")
        if type(self.nullable) is not bool or type(self.required_on_upsert) is not bool:
            raise ValueError("nullable and required_on_upsert must be booleans")
        self.validate_source_mapping()
        self.validate_role_type()
        return self

    def validate_source_mapping(self) -> None:
        """Validate source-column and map-key consistency for this role.

        Raises:
            ValueError: If the source mapping contradicts its projection kind or role.
        """
        if self.source_kind is SourceKind.MAP_KEY:
            if self.source_key is None:
                raise ValueError("MAP_KEY fields require source_key")
            validate_identifier(self.source_key, "source_key")
        elif self.source_key is not None:
            raise ValueError("only MAP_KEY fields may declare source_key")
        map_columns: dict[FieldRole, str] = {
            FieldRole.VECTOR: "vectors",
            FieldRole.TEXT: "texts",
            FieldRole.METADATA: "metadata",
        }
        expected_map: str | None = map_columns.get(self.role)
        if expected_map is not None and (
            self.source_kind is not SourceKind.MAP_KEY or self.source_column != expected_map
        ):
            raise ValueError(f"{self.role.value} fields must use MAP_KEY from {expected_map!r}")
        if self.source_kind is SourceKind.MAP_KEY and expected_map is None:
            raise ValueError("only VECTOR, TEXT, and METADATA fields may use MAP_KEY")
        if self.source_kind is SourceKind.MAP_KEY and self.source_key != self.target_name:
            raise ValueError("MAP_KEY source_key must equal the persisted target_name")
        if self.role in (FieldRole.TOMBSTONE, FieldRole.LINEAGE) and self.source_kind is not SourceKind.DERIVED:
            raise ValueError(f"{self.role.value} fields must be DERIVED")
        if self.role in (FieldRole.KEY, FieldRole.EVENT_TIME) and self.source_kind is not SourceKind.DIRECT:
            raise ValueError(f"{self.role.value} fields must be DIRECT")
        canonical_direct_columns: dict[FieldRole, str] = {
            FieldRole.KEY: "record_id",
            FieldRole.EVENT_TIME: "ts",
        }
        expected_direct: str | None = canonical_direct_columns.get(self.role)
        if expected_direct is not None and (
            self.target_name != expected_direct or self.source_column != expected_direct
        ):
            raise ValueError(f"{self.role.value} field must use canonical name {expected_direct!r}")
        if self.source_kind is SourceKind.DERIVED and self.source_column != self.target_name:
            raise ValueError("DERIVED source_column must equal target_name")

    def validate_role_type(self) -> None:
        """Validate semantic-role data types and vector dimensions.

        Raises:
            ValueError: If a role has an incompatible type, nullability, or vector shape.
        """
        required_types: dict[FieldRole, str] = {
            FieldRole.KEY: "string",
            FieldRole.EVENT_TIME: "timestamp[us,UTC]",
            FieldRole.TEXT: "string",
            FieldRole.METADATA: "string",
            FieldRole.TOMBSTONE: "bool",
        }
        required_type: str | None = required_types.get(self.role)
        if required_type is not None and self.data_type != required_type:
            raise ValueError(f"{self.role.value} fields require {required_type}")
        vector_match: re.Match[str] | None = VECTOR_TYPE_PATTERN.fullmatch(self.data_type)
        if self.role is FieldRole.VECTOR:
            if self.vector_dimension is None or self.vector_dimension < 8 or self.vector_dimension % 8 != 0:
                raise ValueError("vector_dimension must be positive and divisible by 8")
            if vector_match is None or int(vector_match.group(1)) != self.vector_dimension:
                raise ValueError("VECTOR data_type must encode its exact vector_dimension")
            if not self.required_on_upsert:
                raise ValueError("VECTOR fields must be required_on_upsert")
        elif self.vector_dimension is not None or vector_match is not None:
            raise ValueError("only VECTOR fields may declare vector dimensions or fixed-size vector types")
        if self.role is not FieldRole.VECTOR and self.data_type not in SCALAR_DATA_TYPES:
            raise ValueError("field data_type is unsupported")
        if self.role in (FieldRole.KEY, FieldRole.EVENT_TIME) and not self.required_on_upsert:
            raise ValueError(f"{self.role.value} fields must be required_on_upsert")
        if self.role is FieldRole.KEY and self.nullable:
            raise ValueError("KEY fields cannot be nullable")
        if self.role in (FieldRole.TOMBSTONE, FieldRole.LINEAGE) and self.nullable:
            raise ValueError(f"{self.role.value} fields cannot be nullable")
        expected_flags: dict[FieldRole, tuple[bool, bool]] = {
            FieldRole.KEY: (False, True),
            FieldRole.EVENT_TIME: (True, True),
            FieldRole.VECTOR: (True, True),
            FieldRole.TEXT: (True, False),
            FieldRole.METADATA: (True, False),
            FieldRole.TOMBSTONE: (False, False),
            FieldRole.LINEAGE: (False, False),
        }
        if (self.nullable, self.required_on_upsert) != expected_flags[self.role]:
            raise ValueError(f"{self.role.value} nullability and upsert requirement are fixed by the local data path")

    def semantic_document(self) -> dict[str, object]:
        """Return canonical semantic field values for digesting.

        Returns:
            JSON-compatible field values without database identities.
        """
        return {
            "ordinal": self.ordinal,
            "target_name": self.target_name,
            "role": self.role.value,
            "source_kind": self.source_kind.value,
            "source_column": self.source_column,
            "source_key": self.source_key,
            "data_type": self.data_type,
            "nullable": self.nullable,
            "required_on_upsert": self.required_on_upsert,
            "vector_dimension": self.vector_dimension,
        }


@dataclass(frozen=True, slots=True)
class IndexDefinition:
    """One immutable required Lance index and its typed family options."""

    index_definition_id: uuid.UUID
    spec_revision_id: uuid.UUID
    field_id: uuid.UUID
    ordinal: int
    index_name: str
    index_type: IndexType
    vector_options: VectorIndexOptions | None = None
    fts_options: FtsIndexOptions | None = None

    def validate(self) -> IndexDefinition:
        """Validate identity, family, and subtype option ownership.

        Returns:
            This immutable index definition after validation.

        Raises:
            ValueError: If identifiers, type, or subtype options are invalid.
        """
        validate_uuid(self.index_definition_id, "index_definition_id")
        validate_uuid(self.spec_revision_id, "index spec_revision_id")
        validate_uuid(self.field_id, "index field_id")
        if type(self.ordinal) is not int or self.ordinal < 0:
            raise ValueError("index ordinal must be a non-negative integer")
        validate_identifier(self.index_name, "index_name")
        if not isinstance(self.index_type, IndexType):
            raise ValueError("index_type is unsupported")
        if self.index_type is IndexType.IVF_RQ:
            if self.vector_options is None:
                raise ValueError("IVF_RQ indexes require vector options")
            self.vector_options.validate()
        elif self.vector_options is not None:
            raise ValueError("vector options are valid only for IVF_RQ indexes")
        if self.index_type is IndexType.INVERTED:
            if self.fts_options is None:
                raise ValueError("INVERTED indexes require FTS options")
            self.fts_options.validate()
        elif self.fts_options is not None:
            raise ValueError("FTS options are valid only for INVERTED indexes")
        return self

    def semantic_document(self, field_name: str) -> dict[str, object]:
        """Return canonical semantic index values for digesting.

        Args:
            field_name: Stable target field name replacing its database identity.

        Returns:
            JSON-compatible index values.
        """
        return {
            "field_name": field_name,
            "ordinal": self.ordinal,
            "index_name": self.index_name,
            "index_type": self.index_type.value,
            "vector_options": self.vector_options.semantic_document() if self.vector_options is not None else None,
            "fts_options": self.fts_options.semantic_document() if self.fts_options is not None else None,
        }


@dataclass(frozen=True, slots=True)
class DatasetSpecRevision:
    """Complete immutable target schema and execution contract for one revision."""

    spec_id: uuid.UUID
    spec_revision_id: uuid.UUID
    revision_number: int
    state: SpecRevisionState
    supersedes_revision_id: uuid.UUID | None
    configuration_digest: bytes
    fields: tuple[DatasetField, ...]
    indexes: tuple[IndexDefinition, ...]
    ingest_shuffle_partitions: int
    merge_rows_per_chunk: int
    merge_batch_bytes: int
    write_rows_per_fragment: int
    compaction_enabled: bool
    compaction_mode: CompactionMode
    target_rows_per_fragment: int
    max_source_fragments: int | None
    compaction_threads: int | None
    defer_index_remap: bool
    materialize_deletions: bool
    materialize_deletions_threshold: float
    cleanup_older_than_seconds: int | None
    retain_versions: int | None
    fragments_per_index_task: int
    max_index_deltas: int
    max_stale_replans: int
    prewarm_required: bool
    retained_publications: int
    artifact_retention_seconds: int
    record_retention_seconds: int | None

    @property
    def vector_fields(self) -> tuple[tuple[str, int], ...]:
        """Return vector field names and dimensions in schema order.

        Returns:
            Ordered vector field contracts for ingestion and indexing.
        """
        ordered: tuple[DatasetField, ...] = self.fields_for_role(FieldRole.VECTOR)
        return tuple((field_value.target_name, int(field_value.vector_dimension or 0)) for field_value in ordered)

    @property
    def text_fields(self) -> tuple[str, ...]:
        """Return text field names in schema order.

        Returns:
            Ordered text fields.
        """
        return tuple(field_value.target_name for field_value in self.fields_for_role(FieldRole.TEXT))

    @property
    def metadata_fields(self) -> tuple[str, ...]:
        """Return metadata field names in schema order.

        Returns:
            Ordered metadata fields.
        """
        return tuple(field_value.target_name for field_value in self.fields_for_role(FieldRole.METADATA))

    @property
    def index_definitions(self) -> tuple[IndexDefinition, ...]:
        """Return index definitions in their persisted order.

        Returns:
            Deterministically ordered index definitions.
        """
        return tuple(sorted(self.indexes, key=lambda item: (item.ordinal, item.index_name)))

    def fields_for_role(self, role: FieldRole) -> tuple[DatasetField, ...]:
        """Return all fields carrying one semantic role.

        Args:
            role: Semantic field role.

        Returns:
            Matching fields in schema order.
        """
        return tuple(sorted((item for item in self.fields if item.role is role), key=lambda item: item.ordinal))

    def field_by_name(self, target_name: str) -> DatasetField:
        """Resolve one field by its persisted target name.

        Args:
            target_name: Exact target field name.

        Returns:
            Matching field definition.

        Raises:
            KeyError: If this revision has no such field.
        """
        field_value: DatasetField
        for field_value in self.fields:
            if field_value.target_name == target_name:
                return field_value
        raise KeyError(f"dataset spec has no field named {target_name!r}")

    def index_by_name(self, index_name: str) -> IndexDefinition:
        """Resolve one index by its persisted Lance name.

        Args:
            index_name: Exact index name.

        Returns:
            Matching index definition.

        Raises:
            KeyError: If this revision has no such index.
        """
        index: IndexDefinition
        for index in self.indexes:
            if index.index_name == index_name:
                return index
        raise KeyError(f"dataset spec has no index named {index_name!r}")

    def indexes_for_field(self, target_name: str) -> tuple[IndexDefinition, ...]:
        """Return every required index for one target field.

        Args:
            target_name: Exact target field name.

        Returns:
            Matching definitions in index order.
        """
        field_value: DatasetField = self.field_by_name(target_name)
        return tuple(index for index in self.index_definitions if index.field_id == field_value.field_id)

    def field_names_for_index_type(self, index_type: IndexType) -> tuple[str, ...]:
        """Return fields indexed by one Lance family in index order.

        Args:
            index_type: Required index family.

        Returns:
            Ordered target field names.
        """
        fields_by_id: dict[uuid.UUID, str] = {
            field_value.field_id: field_value.target_name for field_value in self.fields
        }
        return tuple(fields_by_id[index.field_id] for index in self.index_definitions if index.index_type is index_type)

    def vector_options_for_field(self, target_name: str) -> VectorIndexOptions:
        """Resolve IVF_RQ options for one vector field.

        Args:
            target_name: Exact vector target field name.

        Returns:
            Typed IVF_RQ options.

        Raises:
            KeyError: If the field has no IVF_RQ definition.
        """
        index: IndexDefinition
        for index in self.indexes_for_field(target_name):
            if index.index_type is IndexType.IVF_RQ and index.vector_options is not None:
                return index.vector_options
        raise KeyError(f"dataset spec field {target_name!r} has no IVF_RQ index")

    def fts_options_for_field(self, target_name: str) -> FtsIndexOptions:
        """Resolve INVERTED options for one text field.

        Args:
            target_name: Exact text target field name.

        Returns:
            Typed full-text options.

        Raises:
            KeyError: If the field has no INVERTED definition.
        """
        index: IndexDefinition
        for index in self.indexes_for_field(target_name):
            if index.index_type is IndexType.INVERTED and index.fts_options is not None:
                return index.fts_options
        raise KeyError(f"dataset spec field {target_name!r} has no INVERTED index")

    def validate(self) -> DatasetSpecRevision:
        """Validate the revision, its schema, indexes, policies, and digest.

        Returns:
            This immutable revision after all invariants pass.

        Raises:
            ValueError: If any identity, configuration, child, or digest invariant fails.
        """
        validate_uuid(self.spec_id, "spec_id")
        validate_uuid(self.spec_revision_id, "spec_revision_id")
        if self.supersedes_revision_id is not None:
            validate_uuid(self.supersedes_revision_id, "supersedes_revision_id")
            if self.supersedes_revision_id == self.spec_revision_id:
                raise ValueError("supersedes_revision_id must name another revision")
        if type(self.revision_number) is not int or self.revision_number < 1:
            raise ValueError("revision_number must be a positive integer")
        if not isinstance(self.state, SpecRevisionState):
            raise ValueError("spec revision state is unsupported")
        self.validate_operational_settings()
        self.validate_fields()
        self.validate_indexes()
        if not isinstance(self.configuration_digest, bytes) or len(self.configuration_digest) != 32:
            raise ValueError("configuration_digest must contain exactly 32 bytes")
        if self.configuration_digest != self.expected_configuration_digest():
            raise ValueError("configuration_digest does not match the revision configuration")
        return self

    def validate_operational_settings(self) -> None:
        """Validate ingestion, compaction, indexing, and publication settings.

        Raises:
            ValueError: If an operational setting falls outside the database contract.
        """
        validate_positive_integers(
            {
                "ingest_shuffle_partitions": self.ingest_shuffle_partitions,
                "merge_rows_per_chunk": self.merge_rows_per_chunk,
                "merge_batch_bytes": self.merge_batch_bytes,
                "write_rows_per_fragment": self.write_rows_per_fragment,
                "target_rows_per_fragment": self.target_rows_per_fragment,
                "fragments_per_index_task": self.fragments_per_index_task,
                "max_index_deltas": self.max_index_deltas,
                "retained_publications": self.retained_publications,
                "artifact_retention_seconds": self.artifact_retention_seconds,
            }
        )
        validate_optional_positive(self.max_source_fragments, "max_source_fragments")
        validate_optional_positive(self.compaction_threads, "compaction_threads")
        validate_optional_positive(self.retain_versions, "retain_versions")
        validate_optional_positive(self.record_retention_seconds, "record_retention_seconds")
        if self.cleanup_older_than_seconds is not None and (
            type(self.cleanup_older_than_seconds) is not int or self.cleanup_older_than_seconds < 21_600
        ):
            raise ValueError("cleanup_older_than_seconds must be at least 21600 when set")
        if type(self.max_stale_replans) is not int or self.max_stale_replans < 0:
            raise ValueError("max_stale_replans must be a non-negative integer")
        boolean_values: tuple[bool, ...] = (
            self.compaction_enabled,
            self.defer_index_remap,
            self.materialize_deletions,
            self.prewarm_required,
        )
        if any(type(value) is not bool for value in boolean_values):
            raise ValueError("compaction and publication switches must be booleans")
        if not isinstance(self.compaction_mode, CompactionMode):
            raise ValueError("compaction_mode is unsupported")
        threshold: float = self.materialize_deletions_threshold
        if (
            isinstance(threshold, bool)
            or not isinstance(threshold, (int, float))
            or not math.isfinite(threshold)
            or not 0.0 <= threshold <= 1.0
        ):
            raise ValueError("materialize_deletions_threshold must be finite and between zero and one")

    def validate_fields(self) -> None:
        """Validate the complete field collection and singleton roles.

        Raises:
            ValueError: If fields do not form one unambiguous target schema.
        """
        if not self.fields:
            raise ValueError("a dataset specification must contain fields")
        validate_unique((field_value.field_id for field_value in self.fields), "field IDs")
        validate_unique((field_value.target_name for field_value in self.fields), "field names")
        validate_unique((field_value.ordinal for field_value in self.fields), "field ordinals")
        field_value: DatasetField
        for field_value in self.fields:
            field_value.validate()
            if field_value.spec_revision_id != self.spec_revision_id:
                raise ValueError("every field must belong to this spec revision")
        source_mappings: Iterable[tuple[SourceKind, str, str | None]] = (
            (field_value.source_kind, field_value.source_column, field_value.source_key) for field_value in self.fields
        )
        validate_unique(source_mappings, "field source mappings")
        role_counts: dict[FieldRole, int] = {
            role: sum(field_value.role is role for field_value in self.fields) for role in FieldRole
        }
        if role_counts[FieldRole.KEY] != 1:
            raise ValueError("a dataset specification requires exactly one KEY field")
        if role_counts[FieldRole.EVENT_TIME] != 1:
            raise ValueError("a dataset specification requires exactly one EVENT_TIME field")
        if role_counts[FieldRole.TOMBSTONE] != 1:
            raise ValueError("a dataset specification requires exactly one TOMBSTONE field")
        expected_lineage: dict[str, str] = {
            "lance_etl_window_seq": "int64",
            "lance_etl_source_sequence": "int64",
            "lance_etl_event_digest": "binary[32]",
        }
        lineage: dict[str, str] = {
            field_value.target_name: field_value.data_type for field_value in self.fields_for_role(FieldRole.LINEAGE)
        }
        if lineage != expected_lineage:
            raise ValueError("a dataset specification requires the three canonical LINEAGE fields")
        tombstone: DatasetField = self.fields_for_role(FieldRole.TOMBSTONE)[0]
        if tombstone.target_name != "is_deleted":
            raise ValueError("the TOMBSTONE field must use canonical name 'is_deleted'")
        ordered: tuple[str, ...] = tuple(
            field_value.target_name for field_value in sorted(self.fields, key=lambda item: item.ordinal)
        )
        expected_order: tuple[str, ...] = (
            "record_id",
            "ts",
            *(field_value.target_name for field_value in self.fields_for_role(FieldRole.VECTOR)),
            *(field_value.target_name for field_value in self.fields_for_role(FieldRole.TEXT)),
            *(field_value.target_name for field_value in self.fields_for_role(FieldRole.METADATA)),
            "lance_etl_window_seq",
            "lance_etl_source_sequence",
            "lance_etl_event_digest",
            "is_deleted",
        )
        if ordered != expected_order or tuple(sorted(field_value.ordinal for field_value in self.fields)) != tuple(
            range(len(self.fields))
        ):
            raise ValueError("dataset fields must use contiguous canonical execution order")

    def validate_indexes(self) -> None:
        """Validate index identities, ownership, and field-family compatibility.

        Raises:
            ValueError: If an index is duplicated or cannot index its referenced field.
        """
        fields_by_id: dict[uuid.UUID, DatasetField] = {field_value.field_id: field_value for field_value in self.fields}
        index: IndexDefinition
        for index in self.indexes:
            index.validate()
            if index.spec_revision_id != self.spec_revision_id:
                raise ValueError("every index must belong to this spec revision")
            field_value: DatasetField | None = fields_by_id.get(index.field_id)
            if field_value is None:
                raise ValueError("every index field must belong to this spec revision")
            validate_index_field_compatibility(index, field_value)
        validate_unique((index.index_definition_id for index in self.indexes), "index IDs")
        validate_unique((index.index_name for index in self.indexes), "index names")
        validate_unique((index.ordinal for index in self.indexes), "index ordinals")
        validate_unique(((index.field_id, index.index_type) for index in self.indexes), "index field and type pairs")

    def configuration_document(self) -> dict[str, object]:
        """Return canonical semantic configuration covered by the digest.

        Database identities, lifecycle state, and lineage between revisions are excluded. Stable
        field names replace child UUID references, making the digest easy to reproduce in Alembic.

        Returns:
            JSON-compatible configuration with deterministically ordered children.
        """
        ordered_fields: list[DatasetField] = sorted(self.fields, key=lambda item: (item.ordinal, item.target_name))
        field_names: dict[uuid.UUID, str] = {
            field_value.field_id: field_value.target_name for field_value in self.fields
        }
        ordered_indexes: list[IndexDefinition] = sorted(
            self.indexes,
            key=lambda item: (item.ordinal, item.index_name),
        )
        return {
            "fields": [field_value.semantic_document() for field_value in ordered_fields],
            "indexes": [index.semantic_document(field_names[index.field_id]) for index in ordered_indexes],
            "ingestion": {
                "ingest_shuffle_partitions": self.ingest_shuffle_partitions,
                "merge_rows_per_chunk": self.merge_rows_per_chunk,
                "merge_batch_bytes": self.merge_batch_bytes,
                "write_rows_per_fragment": self.write_rows_per_fragment,
            },
            "compaction": {
                "compaction_enabled": self.compaction_enabled,
                "compaction_mode": self.compaction_mode.value,
                "target_rows_per_fragment": self.target_rows_per_fragment,
                "max_source_fragments": self.max_source_fragments,
                "compaction_threads": self.compaction_threads,
                "defer_index_remap": self.defer_index_remap,
                "materialize_deletions": self.materialize_deletions,
                "materialize_deletions_threshold": self.materialize_deletions_threshold,
                "cleanup_older_than_seconds": self.cleanup_older_than_seconds,
                "retain_versions": self.retain_versions,
                "record_retention_seconds": self.record_retention_seconds,
            },
            "indexing": {
                "fragments_per_index_task": self.fragments_per_index_task,
                "max_index_deltas": self.max_index_deltas,
                "max_stale_replans": self.max_stale_replans,
            },
            "publication": {
                "prewarm_required": self.prewarm_required,
                "retained_publications": self.retained_publications,
                "artifact_retention_seconds": self.artifact_retention_seconds,
            },
        }

    def expected_configuration_digest(self) -> bytes:
        """Compute SHA-256 over the canonical semantic configuration.

        Returns:
            Raw 32-byte digest suitable for PostgreSQL ``bytea``.
        """
        document: bytes = json.dumps(
            self.configuration_document(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
        return hashlib.sha256(document).digest()


def validate_uuid(value: uuid.UUID, field_name: str) -> None:
    """Validate one non-nil UUID identity.

    Args:
        value: Candidate UUID.
        field_name: Diagnostic field name.

    Raises:
        ValueError: If the identity is not a UUID or is nil.
    """
    if not isinstance(value, uuid.UUID) or value == EMPTY_UUID:
        raise ValueError(f"{field_name} must be a non-nil UUID")


def validate_identifier(value: str, field_name: str) -> None:
    """Validate one bounded database and Lance identifier.

    Args:
        value: Candidate identifier.
        field_name: Diagnostic field name.

    Raises:
        ValueError: If the identifier does not match the shared allowlist.
    """
    if not isinstance(value, str) or IDENTIFIER_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{field_name} must match [A-Za-z_][A-Za-z0-9_]{{0,127}}")


def validate_optional_text(value: str | None, field_name: str, maximum_length: int) -> None:
    """Validate an optional nonblank bounded text setting.

    Args:
        value: Optional setting.
        field_name: Diagnostic field name.
        maximum_length: Maximum accepted character count.

    Raises:
        ValueError: If a present value is blank or too long.
    """
    if value is not None and (not isinstance(value, str) or not value.strip() or len(value) > maximum_length):
        raise ValueError(f"{field_name} must be nonblank and at most {maximum_length} characters when set")


def validate_positive_integers(values: Mapping[str, int]) -> None:
    """Validate required positive integer settings.

    Args:
        values: Setting names and candidate values.

    Raises:
        ValueError: If a setting is a boolean, non-integer, or not positive.
    """
    name: str
    value: int
    for name, value in values.items():
        if type(value) is not int or value < 1:
            raise ValueError(f"{name} must be a positive integer")


def validate_optional_positive(value: int | None, field_name: str) -> None:
    """Validate one optional positive integer setting.

    Args:
        value: Optional integer.
        field_name: Diagnostic field name.

    Raises:
        ValueError: If a present value is a boolean, non-integer, or not positive.
    """
    if value is not None and (type(value) is not int or value < 1):
        raise ValueError(f"{field_name} must be a positive integer when set")


def validate_unique(values: Iterable[Hashable], description: str) -> None:
    """Validate uniqueness of one finite iterable.

    Args:
        values: Finite iterable of hashable values.
        description: Diagnostic description.

    Raises:
        ValueError: If duplicate values exist.
    """
    materialized: tuple[Hashable, ...] = tuple(values)
    if len(materialized) != len(set(materialized)):
        raise ValueError(f"{description} must be unique within a spec revision")


def validate_index_field_compatibility(index: IndexDefinition, field_value: DatasetField) -> None:
    """Validate that an index family supports the referenced field role.

    Args:
        index: Required index definition.
        field_value: Referenced field from the same revision.

    Raises:
        ValueError: If the index family cannot index the field role.
    """
    allowed_roles: dict[IndexType, frozenset[FieldRole]] = {
        IndexType.IVF_RQ: frozenset({FieldRole.VECTOR}),
        IndexType.INVERTED: frozenset({FieldRole.TEXT}),
        IndexType.BTREE: frozenset(
            {
                FieldRole.KEY,
                FieldRole.EVENT_TIME,
                FieldRole.METADATA,
                FieldRole.LINEAGE,
            }
        ),
        IndexType.BITMAP: frozenset({FieldRole.KEY, FieldRole.METADATA, FieldRole.TOMBSTONE}),
        IndexType.ZONEMAP: frozenset({FieldRole.EVENT_TIME, FieldRole.LINEAGE}),
    }
    if field_value.role not in allowed_roles[index.index_type]:
        raise ValueError(f"{index.index_type.value} cannot index a {field_value.role.value} field")


def production_default_spec_revision() -> DatasetSpecRevision:
    """Build the deterministic PostgreSQL seed revision for current processing defaults.

    Returns:
        Validated immutable default revision with reproducible identities and digest.
    """
    fields: tuple[DatasetField, ...] = production_default_fields()
    fields_by_name: dict[str, DatasetField] = {field_value.target_name: field_value for field_value in fields}
    vector_options: VectorIndexOptions = VectorIndexOptions(
        metric=VectorMetric.COSINE,
        num_partitions=None,
        minimum_partitions=16,
        maximum_partitions=32_768,
        target_rows_per_partition=8_192,
        minimum_rows=1,
        num_bits=1,
        streaming_sample_rate=32,
        streaming_refine_passes=1,
        retrain_growth_factor=4.0,
    )
    fts_options: FtsIndexOptions = FtsIndexOptions(False, None, None, 32)
    indexes: tuple[IndexDefinition, ...] = (
        production_index(fields_by_name, "vector_idx", "vector", 0, IndexType.IVF_RQ, vector_options, None),
        production_index(fields_by_name, "text_fts_idx", "text", 1, IndexType.INVERTED, None, fts_options),
        production_index(fields_by_name, "cluster_idx", "cluster", 2, IndexType.BTREE, None, None),
        production_index(
            fields_by_name,
            "ts_idx",
            "ts",
            3,
            IndexType.BTREE,
            None,
            None,
        ),
        production_index(
            fields_by_name,
            "ts_zonemap_idx",
            "ts",
            4,
            IndexType.ZONEMAP,
            None,
            None,
        ),
        production_index(
            fields_by_name,
            "is_deleted_bitmap_idx",
            "is_deleted",
            5,
            IndexType.BITMAP,
            None,
            None,
        ),
    )
    candidate: DatasetSpecRevision = DatasetSpecRevision(
        spec_id=DEFAULT_SPEC_ID,
        spec_revision_id=DEFAULT_SPEC_REVISION_ID,
        revision_number=1,
        state=SpecRevisionState.ACTIVE,
        supersedes_revision_id=None,
        configuration_digest=b"",
        fields=fields,
        indexes=indexes,
        ingest_shuffle_partitions=256,
        merge_rows_per_chunk=250_000,
        merge_batch_bytes=268_435_456,
        write_rows_per_fragment=1_000_000,
        compaction_enabled=True,
        compaction_mode=CompactionMode.TRY_BINARY_COPY,
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
        record_retention_seconds=None,
    )
    return replace(candidate, configuration_digest=candidate.expected_configuration_digest()).validate()


def production_default_fields() -> tuple[DatasetField, ...]:
    """Build deterministic fields for the bundled default specification.

    Returns:
        Ordered immutable field definitions.
    """
    definitions: tuple[tuple[object, ...], ...] = (
        ("record_id", FieldRole.KEY, SourceKind.DIRECT, "record_id", None, "string", False, True, None),
        (
            "ts",
            FieldRole.EVENT_TIME,
            SourceKind.DIRECT,
            "ts",
            None,
            "timestamp[us,UTC]",
            True,
            True,
            None,
        ),
        (
            "vector",
            FieldRole.VECTOR,
            SourceKind.MAP_KEY,
            "vectors",
            "vector",
            "fixed_size_list<float32,128>",
            True,
            True,
            128,
        ),
        ("text", FieldRole.TEXT, SourceKind.MAP_KEY, "texts", "text", "string", True, False, None),
        (
            "cluster",
            FieldRole.METADATA,
            SourceKind.MAP_KEY,
            "metadata",
            "cluster",
            "string",
            True,
            False,
            None,
        ),
        (
            "lance_etl_window_seq",
            FieldRole.LINEAGE,
            SourceKind.DERIVED,
            "lance_etl_window_seq",
            None,
            "int64",
            False,
            False,
            None,
        ),
        (
            "lance_etl_source_sequence",
            FieldRole.LINEAGE,
            SourceKind.DERIVED,
            "lance_etl_source_sequence",
            None,
            "int64",
            False,
            False,
            None,
        ),
        (
            "lance_etl_event_digest",
            FieldRole.LINEAGE,
            SourceKind.DERIVED,
            "lance_etl_event_digest",
            None,
            "binary[32]",
            False,
            False,
            None,
        ),
        (
            "is_deleted",
            FieldRole.TOMBSTONE,
            SourceKind.DERIVED,
            "is_deleted",
            None,
            "bool",
            False,
            False,
            None,
        ),
    )
    return tuple(
        DatasetField(
            field_id=DEFAULT_FIELD_IDS[target_name],
            spec_revision_id=DEFAULT_SPEC_REVISION_ID,
            ordinal=ordinal,
            target_name=target_name,
            role=role,
            source_kind=source_kind,
            source_column=source_column,
            source_key=source_key,
            data_type=data_type,
            nullable=nullable,
            required_on_upsert=required_on_upsert,
            vector_dimension=vector_dimension,
        )
        for ordinal, (
            target_name,
            role,
            source_kind,
            source_column,
            source_key,
            data_type,
            nullable,
            required_on_upsert,
            vector_dimension,
        ) in enumerate(definitions)
    )


def production_index(
    fields_by_name: Mapping[str, DatasetField],
    index_name: str,
    field_name: str,
    ordinal: int,
    index_type: IndexType,
    vector_options: VectorIndexOptions | None,
    fts_options: FtsIndexOptions | None,
) -> IndexDefinition:
    """Build one deterministic default index definition.

    Args:
        fields_by_name: Default fields keyed by target name.
        index_name: Stable Lance index name.
        field_name: Persisted target field name.
        ordinal: Stable index order.
        index_type: Lance index family.
        vector_options: IVF_RQ subtype options.
        fts_options: INVERTED subtype options.

    Returns:
        Immutable index definition.
    """
    return IndexDefinition(
        index_definition_id=DEFAULT_INDEX_IDS[index_name],
        spec_revision_id=DEFAULT_SPEC_REVISION_ID,
        field_id=fields_by_name[field_name].field_id,
        ordinal=ordinal,
        index_name=index_name,
        index_type=index_type,
        vector_options=vector_options,
        fts_options=fts_options,
    )


def decode_dataset_spec_revision(
    revision_row: Mapping[str, object],
    field_rows: Sequence[Mapping[str, object]],
    index_rows: Sequence[Mapping[str, object]],
) -> DatasetSpecRevision:
    """Decode one normalized PostgreSQL specification graph.

    Args:
        revision_row: One ``dataset_spec_revisions`` row carrying its own ``spec_id``.
        field_rows: Child ``dataset_fields`` rows in any order.
        index_rows: Child ``index_definitions`` rows carrying inline option columns in any order.

    Returns:
        Fully decoded and validated immutable revision.

    Raises:
        ValueError: If rows are incomplete, cross revision boundaries, or violate the domain contract.
    """
    spec_id: uuid.UUID = row_uuid(revision_row, "spec_id")
    spec_revision_id: uuid.UUID = row_uuid(revision_row, "spec_revision_id")
    row: Mapping[str, object]
    for row in field_rows:
        validate_child_revision(row, spec_revision_id, "dataset field")
    for row in index_rows:
        validate_child_revision(row, spec_revision_id, "index definition")
    fields: tuple[DatasetField, ...] = tuple(
        sorted((decode_dataset_field(row) for row in field_rows), key=lambda item: item.ordinal)
    )
    indexes: tuple[IndexDefinition, ...] = tuple(
        sorted((decode_index_definition(row) for row in index_rows), key=lambda item: item.ordinal)
    )
    revision: DatasetSpecRevision = DatasetSpecRevision(
        spec_id=spec_id,
        spec_revision_id=spec_revision_id,
        revision_number=row_int(revision_row, "revision_number"),
        state=row_enum(revision_row, "state", SpecRevisionState),
        supersedes_revision_id=row_optional_uuid(revision_row, "supersedes_revision_id"),
        configuration_digest=row_bytes(revision_row, "configuration_digest"),
        fields=fields,
        indexes=indexes,
        ingest_shuffle_partitions=row_int(revision_row, "ingest_shuffle_partitions"),
        merge_rows_per_chunk=row_int(revision_row, "merge_rows_per_chunk"),
        merge_batch_bytes=row_int(revision_row, "merge_batch_bytes"),
        write_rows_per_fragment=row_int(revision_row, "write_rows_per_fragment"),
        compaction_enabled=row_bool(revision_row, "compaction_enabled"),
        compaction_mode=row_enum(revision_row, "compaction_mode", CompactionMode),
        target_rows_per_fragment=row_int(revision_row, "target_rows_per_fragment"),
        max_source_fragments=row_optional_int(revision_row, "max_source_fragments"),
        compaction_threads=row_optional_int(revision_row, "compaction_threads"),
        defer_index_remap=row_bool(revision_row, "defer_index_remap"),
        materialize_deletions=row_bool(revision_row, "materialize_deletions"),
        materialize_deletions_threshold=row_float(revision_row, "materialize_deletions_threshold"),
        cleanup_older_than_seconds=row_optional_int(revision_row, "cleanup_older_than_seconds"),
        retain_versions=row_optional_int(revision_row, "retain_versions"),
        fragments_per_index_task=row_int(revision_row, "fragments_per_index_task"),
        max_index_deltas=row_int(revision_row, "max_index_deltas"),
        max_stale_replans=row_int(revision_row, "max_stale_replans"),
        prewarm_required=row_bool(revision_row, "prewarm_required"),
        retained_publications=row_int(revision_row, "retained_publications"),
        artifact_retention_seconds=row_int(revision_row, "artifact_retention_seconds"),
        record_retention_seconds=row_optional_int(revision_row, "record_retention_seconds"),
    )
    return revision.validate()


def decode_dataset_field(row: Mapping[str, object]) -> DatasetField:
    """Decode one ``dataset_fields`` row.

    Args:
        row: PostgreSQL field row.

    Returns:
        Typed immutable field.
    """
    return DatasetField(
        field_id=row_uuid(row, "field_id"),
        spec_revision_id=row_uuid(row, "spec_revision_id"),
        ordinal=row_int(row, "ordinal"),
        target_name=row_string(row, "target_name"),
        role=row_enum(row, "role", FieldRole),
        source_kind=row_enum(row, "source_kind", SourceKind),
        source_column=row_string(row, "source_column"),
        source_key=row_optional_string(row, "source_key"),
        data_type=row_string(row, "data_type"),
        nullable=row_bool(row, "nullable"),
        required_on_upsert=row_bool(row, "required_on_upsert"),
        vector_dimension=row_optional_int(row, "vector_dimension"),
    )


def decode_index_definition(row: Mapping[str, object]) -> IndexDefinition:
    """Decode one ``index_definitions`` row and its inline subtype option columns.

    Args:
        row: PostgreSQL index-definition row carrying inline vector and full-text columns.

    Returns:
        Typed immutable index definition.
    """
    index_type: IndexType = IndexType(row_enum(row, "index_type", IndexType))
    return IndexDefinition(
        index_definition_id=row_uuid(row, "index_definition_id"),
        spec_revision_id=row_uuid(row, "spec_revision_id"),
        field_id=row_uuid(row, "field_id"),
        ordinal=row_int(row, "ordinal"),
        index_name=row_string(row, "index_name"),
        index_type=index_type,
        vector_options=decode_vector_options(row) if index_type is IndexType.IVF_RQ else None,
        fts_options=decode_fts_options(row) if index_type is IndexType.INVERTED else None,
    )


def decode_vector_options(row: Mapping[str, object]) -> VectorIndexOptions:
    """Decode the inline IVF_RQ option columns from one ``index_definitions`` row.

    Args:
        row: PostgreSQL index-definition row of an IVF_RQ index.

    Returns:
        Typed IVF_RQ options.
    """
    return VectorIndexOptions(
        metric=row_enum(row, "metric", VectorMetric),
        num_partitions=row_optional_int(row, "num_partitions"),
        minimum_partitions=row_int(row, "minimum_partitions"),
        maximum_partitions=row_int(row, "maximum_partitions"),
        target_rows_per_partition=row_int(row, "target_rows_per_partition"),
        minimum_rows=row_int(row, "minimum_rows"),
        num_bits=row_int(row, "num_bits"),
        streaming_sample_rate=row_int(row, "streaming_sample_rate"),
        streaming_refine_passes=row_int(row, "streaming_refine_passes"),
        retrain_growth_factor=row_float(row, "retrain_growth_factor"),
    )


def decode_fts_options(row: Mapping[str, object]) -> FtsIndexOptions:
    """Decode the inline INVERTED option columns from one ``index_definitions`` row.

    Args:
        row: PostgreSQL index-definition row of an INVERTED index.

    Returns:
        Typed full-text options.
    """
    return FtsIndexOptions(
        with_position=row_bool(row, "with_position"),
        base_tokenizer=row_optional_string(row, "base_tokenizer"),
        language=row_optional_string(row, "language"),
        max_unindexed_fragments=row_int(row, "max_unindexed_fragments"),
    )


def validate_child_revision(row: Mapping[str, object], revision_id: uuid.UUID, description: str) -> None:
    """Validate that one child row belongs to the decoded revision.

    Args:
        row: Child row carrying ``spec_revision_id``.
        revision_id: Expected parent revision identity.
        description: Diagnostic child description.

    Raises:
        ValueError: If the child belongs to another revision.
    """
    if row_uuid(row, "spec_revision_id") != revision_id:
        raise ValueError(f"{description} belongs to another spec revision")


def row_value(row: Mapping[str, object], key: str) -> object:
    """Read one required row value.

    Args:
        row: PostgreSQL result mapping.
        key: Required column.

    Returns:
        Stored value, including ``None`` for nullable columns.

    Raises:
        ValueError: If the column is absent.
    """
    if key not in row:
        raise ValueError(f"database row is missing required column {key!r}")
    return row[key]


def row_uuid(row: Mapping[str, object], key: str) -> uuid.UUID:
    """Read one required UUID column.

    Args:
        row: PostgreSQL result mapping.
        key: Required UUID column.

    Returns:
        UUID value.

    Raises:
        ValueError: If the value is not a UUID.
    """
    value: object = row_value(row, key)
    if not isinstance(value, uuid.UUID):
        raise ValueError(f"database column {key!r} must be a UUID")
    return value


def row_optional_uuid(row: Mapping[str, object], key: str) -> uuid.UUID | None:
    """Read one nullable UUID column.

    Args:
        row: PostgreSQL result mapping.
        key: Nullable UUID column.

    Returns:
        UUID value or ``None``.
    """
    value: object = row_value(row, key)
    if value is None:
        return None
    if not isinstance(value, uuid.UUID):
        raise ValueError(f"database column {key!r} must be a UUID or null")
    return value


def row_string(row: Mapping[str, object], key: str) -> str:
    """Read one required string column.

    Args:
        row: PostgreSQL result mapping.
        key: Required string column.

    Returns:
        String value.

    Raises:
        ValueError: If the value is not a string.
    """
    value: object = row_value(row, key)
    if not isinstance(value, str):
        raise ValueError(f"database column {key!r} must be a string")
    return value


def row_optional_string(row: Mapping[str, object], key: str) -> str | None:
    """Read one nullable string column.

    Args:
        row: PostgreSQL result mapping.
        key: Nullable string column.

    Returns:
        String value or ``None``.
    """
    value: object = row_value(row, key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"database column {key!r} must be a string or null")
    return value


def row_int(row: Mapping[str, object], key: str) -> int:
    """Read one required integer column.

    Args:
        row: PostgreSQL result mapping.
        key: Required integer column.

    Returns:
        Integer value.

    Raises:
        ValueError: If the value is not an integer.
    """
    value: object = row_value(row, key)
    if type(value) is not int:
        raise ValueError(f"database column {key!r} must be an integer")
    return value


def row_optional_int(row: Mapping[str, object], key: str) -> int | None:
    """Read one nullable integer column.

    Args:
        row: PostgreSQL result mapping.
        key: Nullable integer column.

    Returns:
        Integer value or ``None``.
    """
    value: object = row_value(row, key)
    if value is None:
        return None
    if type(value) is not int:
        raise ValueError(f"database column {key!r} must be an integer or null")
    return value


def row_bool(row: Mapping[str, object], key: str) -> bool:
    """Read one required boolean column.

    Args:
        row: PostgreSQL result mapping.
        key: Required boolean column.

    Returns:
        Boolean value.

    Raises:
        ValueError: If the value is not a boolean.
    """
    value: object = row_value(row, key)
    if type(value) is not bool:
        raise ValueError(f"database column {key!r} must be a boolean")
    return value


def row_float(row: Mapping[str, object], key: str) -> float:
    """Read one required numeric column.

    Args:
        row: PostgreSQL result mapping.
        key: Required numeric column.

    Returns:
        Float value.

    Raises:
        ValueError: If the value cannot be represented as a float.
    """
    value: object = row_value(row, key)
    if isinstance(value, bool):
        raise ValueError(f"database column {key!r} must be numeric")
    try:
        return float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"database column {key!r} must be numeric") from error


def row_bytes(row: Mapping[str, object], key: str) -> bytes:
    """Read one required binary column.

    Args:
        row: PostgreSQL result mapping.
        key: Required binary column.

    Returns:
        Immutable bytes.

    Raises:
        ValueError: If the value is not binary.
    """
    value: object = row_value(row, key)
    if not isinstance(value, (bytes, bytearray, memoryview)):
        raise ValueError(f"database column {key!r} must be binary")
    return bytes(value)


def row_enum(row: Mapping[str, object], key: str, enum_type: type[StrEnum]) -> StrEnum:
    """Read one required string-backed enum column.

    Args:
        row: PostgreSQL result mapping.
        key: Required enum column.
        enum_type: Expected ``StrEnum`` class.

    Returns:
        Typed enum value.

    Raises:
        ValueError: If the value is not supported by the enum.
    """
    value: str = row_string(row, key)
    try:
        return enum_type(value)
    except ValueError as error:
        raise ValueError(f"database column {key!r} has unsupported value {value!r}") from error


DEFAULT_CONFIGURATION_DIGEST_HEX: str = production_default_spec_revision().configuration_digest.hex()
"""Single source of truth for the seeded default-revision configuration digest, in lowercase hex."""
