//! Conversions between intake protobuf messages and domain types.
//!
//! Validation lives here: targets are checked against the path-segment allowlist, every record id
//! must be non-empty, an upsert must carry at least one of metadata, vectors, or texts, and every
//! named vector it carries must be non-empty. Ids, metadata, and texts are transported verbatim
//! and never interpreted as expressions, so no raw SQL ever crosses the seam.

use std::collections::BTreeMap;

use crate::domain::{DatasetTarget, IntakeError, IntakeReport, Record, RecordWrite};
use crate::pb;

/// Converts an optional proto dataset target into the validated domain target.
pub fn target_from_proto(target: Option<pb::DatasetTarget>) -> Result<DatasetTarget, IntakeError> {
    let target = target.ok_or_else(|| IntakeError::invalid_argument("target is required"))?;
    let target = DatasetTarget {
        org_id: target.org_id,
        tenant_id: target.tenant_id,
        namespace: target.namespace,
    };
    target
        .validate()
        .map_err(|err| IntakeError::invalid_argument(err.message().to_string()))?;
    Ok(target)
}

/// Validates and converts one proto record write into a domain record write.
///
/// An upsert keeps the full record. It must carry a non-empty id and at least one of metadata,
/// vectors, or texts, and every named vector it carries must be non-empty. A delete keeps only the
/// id. An unspecified or unknown op, a missing record, or an empty id is rejected.
pub fn record_write_from_proto(write: pb::RecordWrite) -> Result<RecordWrite, IntakeError> {
    let op = pb::WriteOp::try_from(write.op).map_err(|_| IntakeError::invalid_argument("write op is unknown"))?;
    let record = write
        .record
        .ok_or_else(|| IntakeError::invalid_argument("record write requires a record"))?;
    validate_id(&record.id)?;
    match op {
        pb::WriteOp::Upsert => {
            let vectors = vectors_from_proto(record.vectors)?;
            if record.metadata.is_empty() && vectors.is_empty() && record.texts.is_empty() {
                return Err(IntakeError::invalid_argument(
                    "upsert requires at least one of metadata, vectors, or texts",
                ));
            }
            Ok(RecordWrite::Upsert(Record {
                id: record.id,
                event_timestamp_ms: record.event_timestamp_ms,
                metadata: record.metadata.into_iter().collect(),
                vectors,
                texts: record.texts.into_iter().collect(),
            }))
        }
        pb::WriteOp::Delete => Ok(RecordWrite::Delete { id: record.id }),
        pb::WriteOp::Unspecified => Err(IntakeError::invalid_argument("write op is unspecified")),
    }
}

/// Converts the proto named-vector map into the domain map, rejecting any empty named vector.
fn vectors_from_proto(
    vectors: std::collections::HashMap<String, pb::FloatVector>,
) -> Result<BTreeMap<String, Vec<f32>>, IntakeError> {
    let mut out = BTreeMap::new();
    for (name, vector) in vectors {
        if vector.values.is_empty() {
            return Err(IntakeError::invalid_argument(format!(
                "vector '{name}' must be non-empty"
            )));
        }
        out.insert(name, vector.values);
    }
    Ok(out)
}

/// The record id a proto record write refers to, used to label a per-item failure. Empty when the
/// write carries no record.
pub fn write_id(write: &pb::RecordWrite) -> String {
    write
        .record
        .as_ref()
        .map(|record| record.id.clone())
        .unwrap_or_default()
}

/// Converts a domain intake report into the proto response, carrying only record ids.
pub fn report_to_proto(report: IntakeReport) -> pb::WriteRecordsResponse {
    pb::WriteRecordsResponse {
        succeeded_ids: report.succeeded_ids,
        failed_ids: report.failed_ids,
    }
}

/// Rejects empty record ids. Ids are otherwise opaque and transported verbatim.
fn validate_id(id: &str) -> Result<(), IntakeError> {
    if id.is_empty() {
        return Err(IntakeError::invalid_argument("record id must be non-empty"));
    }
    Ok(())
}
