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


def validate_partition_contract(spec: PartitionSpec) -> None:
    """Require the exact code-owned source partition specification.

    Args:
        spec: Active Iceberg partition specification.

    Raises:
        SourceContractError: If names, sources, transforms, or order differ.
    """
    if spec.fields != REQUIRED_PARTITION_FIELDS:
        raise SourceContractError(
            "active partition spec must be tenant_id, namespace, org_id, and hour(processing_timestamp)"
        )


def validate_table_contract(metadata: TableMetadata, checkpoint: SourceCheckpoint | None) -> None:
    """Validate active metadata against the fixed contract and recorded table identity.

    Args:
        metadata: Metadata pinned at the start of a planner run.
        checkpoint: Previously recorded audit tip, when one exists.

    Raises:
        SourceContractError: If table UUID or active partition spec changed.
    """
    validate_partition_contract(metadata.active_partition_spec)
    if checkpoint is None:
        return
    if checkpoint.table_uuid != metadata.table_uuid:
        raise SourceContractError("Iceberg table UUID changed since the recorded source window")
    if checkpoint.partition_spec_id != metadata.active_partition_spec.spec_id:
        raise SourceContractError("Iceberg active partition specification changed since the recorded source window")
