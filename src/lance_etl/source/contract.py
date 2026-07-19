"""Validation for the code-owned Iceberg table and partition contract."""

from __future__ import annotations

from lance_etl.source.errors import SourceContractError
from lance_etl.source.models import PartitionField, PartitionSpec, SourceCheckpoint, TableMetadata

REQUIRED_PARTITION_FIELDS: tuple[PartitionField, ...] = (
    PartitionField("tenant_id", "tenant_id", "identity"),
    PartitionField("namespace", "namespace", "identity"),
    PartitionField("org_id", "org_id", "identity"),
    PartitionField("processing_timestamp_hour", "processing_timestamp", "hour"),
)


def required_partition_fields(
    tenant_column: str,
    namespace_column: str,
    org_column: str,
) -> tuple[PartitionField, ...]:
    """Build the expected partition contract from PostgreSQL source mappings.

    Args:
        tenant_column: Physical tenant source column.
        namespace_column: Physical namespace source column.
        org_column: Physical organization source column.

    Returns:
        Canonically named routing partitions plus the fixed processing-time hour.
    """
    return (
        PartitionField("tenant_id", tenant_column, "identity"),
        PartitionField("namespace", namespace_column, "identity"),
        PartitionField("org_id", org_column, "identity"),
        PartitionField("processing_timestamp_hour", "processing_timestamp", "hour"),
    )


def validate_partition_contract(
    spec: PartitionSpec,
    required_fields: tuple[PartitionField, ...] = REQUIRED_PARTITION_FIELDS,
) -> None:
    """Require the exact code-owned source partition specification.

    Args:
        spec: Active Iceberg partition specification.
        required_fields: Expected field names, source columns, transforms, and order.

    Raises:
        SourceContractError: If names, sources, transforms, or order differ.
    """
    if spec.fields != required_fields:
        raise SourceContractError(
            "active partition spec must be tenant_id, namespace, org_id, and hour(processing_timestamp)"
        )


def validate_table_contract(
    metadata: TableMetadata,
    checkpoint: SourceCheckpoint | None,
    required_fields: tuple[PartitionField, ...] = REQUIRED_PARTITION_FIELDS,
) -> None:
    """Validate active metadata against the fixed contract and recorded table identity.

    Args:
        metadata: Metadata pinned at the start of a planner run.
        checkpoint: Previously recorded audit tip, when one exists.
        required_fields: Expected active partition fields.

    Raises:
        SourceContractError: If table UUID or active partition spec changed.
    """
    validate_partition_contract(metadata.active_partition_spec, required_fields)
    if checkpoint is None:
        return
    if checkpoint.table_uuid != metadata.table_uuid:
        raise SourceContractError("Iceberg table UUID changed since the recorded source window")
    if checkpoint.partition_spec_id != metadata.active_partition_spec.spec_id:
        raise SourceContractError("Iceberg active partition specification changed since the recorded source window")
